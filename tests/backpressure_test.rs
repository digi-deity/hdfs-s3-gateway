//! Backpressure HTTP-level test. This test drives the gateway over a real
//! TCP socket and pushes concurrency past `max_concurrent_requests`, asserting excess requests
//! get a clean `503 SlowDown` (a real S3 error code) rather than unbounded degradation, hangs,
//! or OOM.

use std::collections::HashSet;
use std::time::Duration;

use hdfs_native::minidfs::MiniDfs;
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use hdfs_s3_gateway::config::Config;
use hdfs_s3_gateway::s3::server::{build_service, serve};
use hdfs_s3_gateway::s3::HdfsGateway;
use reqwest::Client as HttpClient;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::Mutex;

/// Serializes the MiniDfs-backed tests in this file.
///
/// Each test spins up its own `MiniDfs`, which writes to the *same* hardcoded
/// `target/test/data/dfs` directory. If two run concurrently (Cargo runs test
/// functions in parallel by default), their NameNode/DataNode storage dirs collide
/// and the cluster fails to start (`renameTo ... seen_txid.tmp failed`,
/// `namespaceID is incompatible`). Holding this guard for the duration of each test
/// ensures only one MiniDfs is alive at a time.
static MINIDFS_GUARD: Mutex<()> = Mutex::const_new(());

async fn start_gateway(
    dfs: &MiniDfs,
    max_concurrent: usize,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let config = Config {
        namenode_uri: dfs.url.clone(),
        hdfs_root: "/".to_string(),
        bucket_name: "hdfs".to_string(),
        listen_addr: "127.0.0.1:0".to_string(),
        max_concurrent_requests: max_concurrent,
        expose_upstream_errors: false,
        hdfs_options: Default::default(),
        hdfs_config_dir: None,
        hdfs_user: None,
    };
    let client = ClientBuilder::new().with_url(&dfs.url).build().unwrap();
    let gateway = HdfsGateway::new(client, config.clone());
    let service = build_service(gateway, &config);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
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

#[tokio::test]
async fn backpressure_503_slowdown() {
    let _guard = MINIDFS_GUARD.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();
    let dfs = MiniDfs::with_features(&HashSet::new());
    let hdfs = ClientBuilder::new().with_url(&dfs.url).build().unwrap();
    write_file(&hdfs, "/data/obj.txt", b"hello").await;

    // Cap at 2 in-flight requests; fire 50 concurrent GETs.
    let (addr, _handle) = start_gateway(&dfs, 2).await;
    let base = format!("http://{addr}/hdfs");
    let http = HttpClient::builder()
        .pool_idle_timeout(Duration::from_secs(1))
        .build()
        .unwrap();

    let mut tasks = Vec::new();
    for _ in 0..50 {
        let http = http.clone();
        let url = format!("{base}/data/obj.txt");
        tasks.push(tokio::spawn(async move {
            let resp = http.get(&url).send().await.unwrap();
            (resp.status().as_u16(), resp.headers().clone())
        }));
    }

    let mut ok = 0usize;
    let mut slowdown = 0usize;
    for t in tasks {
        let (status, _headers) = t.await.unwrap();
        match status {
            200 => ok += 1,
            503 => slowdown += 1,
            other => panic!("unexpected status {other} under backpressure"),
        }
    }

    // Every request must be cleanly resolved: either 200 or 503. No hangs, no 5xx other
    // than 503, no connection aborts (which would surface as a reqwest error, not a status).
    assert_eq!(ok + slowdown, 50, "every request must be cleanly resolved");
    assert!(
        slowdown > 0,
        "excess concurrency must be rejected with 503 SlowDown"
    );
    // The in-flight cap is 2, so at most a handful of requests should have been admitted
    // before the rest were rejected — but because requests complete fast, some 200s are
    // expected. The key invariant is that *none* were dropped/aborted and *some* were 503'd.
    println!("backpressure: {ok} ok, {slowdown} 503 SlowDown");
}

/// Verify the backpressure permit is held until the response body is fully streamed, not
/// merely until the handler builds the response headers.
///
/// Cap at 1 in-flight request. Open a GET of a large object and read it *slowly* (one chunk
/// at a time with a delay between reads). While that first request is still streaming its
/// body, fire a second GET. Because the first request still holds the single slot, the second
/// must be rejected with `503 SlowDown`. If the permit were released at header-build time, the
/// first request would free its slot immediately and the second would be admitted (200), which
/// this test would catch.
#[tokio::test]
async fn backpressure_holds_permit_during_body_streaming() {
    let _guard = MINIDFS_GUARD.lock().await;
    let _ = env_logger::builder().is_test(true).try_init();
    let dfs = MiniDfs::with_features(&HashSet::new());
    let hdfs = ClientBuilder::new().with_url(&dfs.url).build().unwrap();

    // A large object so the body takes many reads to stream.
    let payload = vec![b'x'; 8 * 1024 * 1024];
    write_file(&hdfs, "/data/big.txt", &payload).await;

    // Cap at exactly 1 in-flight request.
    let (addr, _handle) = start_gateway(&dfs, 1).await;
    let base = format!("http://{addr}/hdfs");
    let http = HttpClient::builder()
        .pool_idle_timeout(Duration::from_secs(1))
        .build()
        .unwrap();

    // First request: open the GET but read the body slowly so it stays in-flight.
    let first_url = format!("{base}/data/big.txt");
    let first = http.get(&first_url).send().await.unwrap();
    assert_eq!(first.status().as_u16(), 200, "first request admitted");
    let mut first_stream = first.bytes_stream();

    // Read a few chunks slowly to keep the first request occupying the single slot.
    for _ in 0..4 {
        let chunk = tokio::time::timeout(Duration::from_secs(5), first_stream.next())
            .await
            .expect("first body chunk within timeout")
            .expect("first body chunk present");
        assert!(!chunk.unwrap().is_empty());
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // While the first request is still streaming, the second must be rejected: the single
    // slot is still held by the first request's body.
    let second_url = format!("{base}/data/obj.txt");
    write_file(&hdfs, "/data/obj.txt", b"hello").await;
    let second = http.get(&second_url).send().await.unwrap();
    assert_eq!(
        second.status().as_u16(),
        503,
        "second request must be rejected with 503 SlowDown while first body is still streaming"
    );

    // Drain the rest of the first body so the test cleans up promptly.
    while first_stream.next().await.is_some() {}
}
