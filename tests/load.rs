//! Concurrency & performance hardening.
//!
//! Drives the gateway over a real TCP socket against a MiniDFSCluster, then hammers
//! `GetObject` with concurrent requests. To make the test DETERMINISTIC (and immune to CI
//! runner load), the *client* deliberately throttles its read rate to a known value. Because
//! the client is the bottleneck, the transfer time is governed by our throttle -- not by
//! network or disk speed -- so we assert timing bounds instead of fragile throughput floors.
//!
//! The key invariant: the shared `Arc<Client>` architecture must PARALLELIZE. If reads were
//! serialized behind a shared reader, N concurrent reads would take ~Nx the single-read time.
//! Under true parallelism they take ~the single-read time. We assert the concurrent time stays
//! far below the serialized bound.
//!
//! Requires `mvn` + JDK on PATH (MiniDfs harness). Run serially with a clean `target/test`.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use hdfs_native::minidfs::MiniDfs;
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use hdfs_s3_gateway::config::Config;
use hdfs_s3_gateway::s3::server::{build_service, serve};
use hdfs_s3_gateway::s3::HdfsGateway;

use bytes::Bytes;
use futures::StreamExt;
use reqwest::Client as HttpClient;

/// Client read rate we throttle to (bytes/sec). The client is the bottleneck, so transfer time
/// is deterministic regardless of CI runner speed.
const THROTTLE_BYTES_PER_SEC: f64 = 1.0 * 1024.0 * 1024.0; // 1 MiB/s

/// Start the gateway on an ephemeral port and return the bound address + a shutdown trigger.
async fn start_gateway(
    dfs: &MiniDfs,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let config = Config {
        namenode_uri: dfs.url.clone(),
        hdfs_root: "/".to_string(),
        bucket_name: "hdfs".to_string(),
        listen_addr: "127.0.0.1:0".to_string(),
        max_concurrent_requests: 2048,
        expose_upstream_errors: false,
        hdfs_options: Default::default(),
        hdfs_config_dir: None,
        hdfs_user: None,
    };
    let client = ClientBuilder::new().with_url(&dfs.url).build().unwrap();
    let gateway = HdfsGateway::new(client, config.clone());
    let service = build_service(gateway, &config);

    // Bind to port 0 to get an ephemeral port, then hand the listener to `server::serve`.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // `std::future::pending()` never resolves, so the server runs until the task is aborted.
    let handle = tokio::spawn(serve(listener, service, std::future::pending()));
    (addr, handle)
}

async fn write_file(client: &Client, path: &str, data: &[u8]) {
    let mut writer = client
        .create(path, &WriteOptions::default().overwrite(true))
        .await
        .unwrap();
    writer
        .write_bytes(Bytes::copy_from_slice(data))
        .await
        .unwrap();
    writer.close().await.unwrap();
}

/// Read `url` fully, consuming bytes at `THROTTLE_BYTES_PER_SEC` by sleeping proportionally to
/// each received chunk. Returns the elapsed wall-clock time. Because the client paces itself,
/// the transfer duration depends only on the file size and our throttle -- not on network/disk
/// speed -- making the test deterministic across environments.
async fn throttled_get(http: &HttpClient, url: &str, expected_len: usize) -> Duration {
    let start = Instant::now();
    let resp = http.get(url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "GET {url}");
    let mut stream = resp.bytes_stream();
    let mut got = 0usize;
    while let Some(chunk) = stream.next().await {
        let c = chunk.expect("body chunk");
        got += c.len();
        // Sleep proportional to bytes received => exact average consume rate.
        let delay = Duration::from_secs_f64(c.len() as f64 / THROTTLE_BYTES_PER_SEC);
        tokio::time::sleep(delay).await;
    }
    assert_eq!(got, expected_len, "truncated body for {url}");
    start.elapsed()
}

/// Spawn `concurrency` throttled GETs of `key` and return total elapsed wall-clock time (from
/// first spawn to last completion).
async fn hammer_throttled(
    http: &HttpClient,
    base: &str,
    key: &str,
    concurrency: usize,
    expected_len: usize,
) -> Duration {
    let start = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let http = http.clone();
        let url = format!("{base}/{key}");
        tasks.push(tokio::spawn(async move {
            throttled_get(&http, &url, expected_len).await
        }));
    }
    for t in tasks {
        let _ = t.await.unwrap();
    }
    start.elapsed()
}

#[tokio::test]
async fn concurrent_reads_scale() {
    let _ = env_logger::builder().is_test(true).try_init();
    let dfs = MiniDfs::with_features(&HashSet::new());
    let hdfs = ClientBuilder::new().with_url(&dfs.url).build().unwrap();

    // A 1 MiB file. At our 1 MiB/s throttle a single read should take ~1s.
    let medium: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
    write_file(&hdfs, "/data/medium.bin", &medium).await;

    let (addr, _handle) = start_gateway(&dfs).await;
    let base = format!("http://{addr}/hdfs");
    let http = HttpClient::new();

    // Baseline: a single throttled read of the 1 MiB file. The measured time must be close to
    // the throttle-predicted time -- this proves the body is actually streamed at our controlled
    // rate (not delivered instantly, not absurdly slow).
    let expected_single = Duration::from_secs_f64(medium.len() as f64 / THROTTLE_BYTES_PER_SEC);
    let t_single = throttled_get(&http, &format!("{base}/data/medium.bin"), medium.len()).await;
    assert!(
        t_single >= expected_single.mul_f64(0.5),
        "single read too fast ({t_single:?} < {expected_single:?}*0.5) -- body may be truncated/instant"
    );
    assert!(
        t_single <= expected_single.mul_f64(3.0),
        "single read too slow ({t_single:?} > {expected_single:?}*3) -- environment bottleneck"
    );

    // Same-file concurrency: 64 concurrent throttled reads of the SAME 1 MiB file. Under true
    // parallelism each read takes ~1s and they overlap => ~1s total. Under serialization they'd
    // take ~64s. We assert we're far below the serialized bound.
    let t_same = hammer_throttled(&http, &base, "data/medium.bin", 64, medium.len()).await;

    // Parallelism bound: well below even a partially-serialized time.
    let parallel_bound = expected_single.mul_f64(8.0);
    assert!(
        t_same <= parallel_bound,
        "same-file concurrency looks serialized: {t_same:?} > {parallel_bound:?} (single={expected_single:?})"
    );
    // And it should still be at least roughly the single-read time (work actually happened).
    assert!(
        t_same >= expected_single.mul_f64(0.5),
        "same-file concurrent read implausibly fast ({t_same:?})"
    );

    println!(
        "single: {:.2}s (expected ~{:.2}s), 64-way same-file: {:.2}s (serialized would be ~{:.1}s)",
        t_single.as_secs_f64(),
        expected_single.as_secs_f64(),
        t_same.as_secs_f64(),
        expected_single.as_secs_f64() * 64.0,
    );
}
