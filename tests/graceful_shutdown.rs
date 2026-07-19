//! Graceful shutdown integration test (operational readiness).
//!
//! Sends SIGTERM-equivalent (a resolved shutdown future) to a running gateway *while* a
//! large streamed `GetObject` download is in flight, and asserts the client observes
//! EITHER a complete file OR a clearly-failed connection — never a silently-truncated
//! "successful" response. This is the streaming-architecture risk, validated at
//! the operational boundary: in-flight responses must be allowed to finish (or fail
//! cleanly) on shutdown rather than being cut off mid-stream by process exit.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use hdfs_native::minidfs::MiniDfs;
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use hdfs_s3_gateway::config::Config;
use hdfs_s3_gateway::s3::server::{build_service, serve};
use hdfs_s3_gateway::s3::HdfsGateway;
use reqwest::Client as HttpClient;
use tokio::sync::Notify;

/// Write a file directly via the HDFS client (we are not testing gateway writes — just
/// seeding data for the read-under-shutdown test).
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
async fn graceful_shutdown_drains_inflight_get() {
    let _ = env_logger::builder().is_test(true).try_init();
    let dfs = MiniDfs::with_features(&HashSet::new());
    let hdfs = ClientBuilder::new().with_url(&dfs.url).build().unwrap();

    // A large file so the streamed GET takes a meaningful amount of time to deliver,
    // giving us a window in which to trigger shutdown mid-stream.
    let size: usize = 20 * 1024 * 1024; // 20 MiB
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    write_file(&hdfs, "/data/big.bin", &data).await;

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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Shutdown is driven by a `Notify` we control (stands in for SIGTERM/SIGINT).
    let shutdown = Arc::new(Notify::new());
    let shutdown_trigger = shutdown.clone();
    let shutdown_future = async move { shutdown_trigger.notified().await };
    let server_handle = tokio::spawn(serve(listener, service, shutdown_future));

    let base = format!("http://{addr}/hdfs");
    let http = HttpClient::builder().build().unwrap();

    // Start a streamed GET and read exactly ONE chunk, then STOP reading. Over loopback a
    // 20 MiB body would otherwise be delivered in milliseconds, so the GET could finish
    // *before* shutdown is triggered and the test would pass without ever exercising the
    // drain path. By pausing here, the gateway's writer fills the TCP send buffer and then
    // blocks on socket backpressure — the GET handler is genuinely suspended mid-stream,
    // i.e. a real in-flight request — regardless of how fast reads would otherwise be.
    let resp = http
        .get(format!("{base}/data/big.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET must start successfully");
    let mut stream = resp.bytes_stream();

    let first = stream
        .next()
        .await
        .expect("first chunk")
        .expect("first chunk ok");
    assert!(!first.is_empty());
    let mut received: usize = first.len();

    // Hold the connection open without reading. The GET is now in flight on the server.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Trigger graceful shutdown WHILE the GET is still in flight.
    shutdown.notify_one();
    // Let the server observe the signal and enter its drain-wait.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The server must NOT have exited: it is waiting for the in-flight GET to finish. This
    // is the load-bearing assertion — it proves the drain actually waits, rather than the
    // GET having already completed before shutdown (which would make the test meaningless).
    assert!(
        !server_handle.is_finished(),
        "server must remain running while an in-flight GET is being drained"
    );

    // Now resume reading to the end. The gateway must deliver the COMPLETE file (never a
    // silently-truncated "success") and only then let the server drain and exit.
    let mut clean_failure = false;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => received += bytes.len(),
            Err(_) => {
                clean_failure = true;
                break;
            }
        }
    }

    // Forbidden outcome: shutdown during the stream yields a "success" with fewer bytes
    // than the full object and no error. Either a complete file or a clean failure is OK.
    assert!(
        received == size || clean_failure,
        "shutdown during stream must yield complete file or clean failure, got {received}/{size} with no error"
    );

    // After the in-flight GET completes, the server should drain and return.
    let drained = tokio::time::timeout(Duration::from_secs(20), server_handle).await;
    assert!(
        drained.is_ok(),
        "server should shut down gracefully within 20s after the in-flight GET finished"
    );
}

/// Structured logging correlation test.
use std::sync::Mutex;

use tracing::field::Visit;
use tracing::span::{Attributes, Id};
use tracing::{Event, Metadata, Subscriber};

/// A minimal `tracing` subscriber that records every event's formatted fields as a line.
#[derive(Clone)]
struct CaptureSubscriber {
    lines: Arc<Mutex<Vec<String>>>,
}

impl CaptureSubscriber {
    fn new() -> Self {
        CaptureSubscriber {
            lines: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Subscriber for CaptureSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _span: &Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut msg = String::new();
        event.record(&mut StringVisitor(&mut msg));
        self.lines.lock().unwrap().push(msg);
    }
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
}

/// Collects formatted fields of a `tracing` event into a single string.
struct StringVisitor<'a>(&'a mut String);
impl<'a> Visit for StringVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, "{}:{:?} ", field.name(), value);
    }
}

#[tokio::test]
async fn logging_request_id_correlated() {
    let _ = env_logger::builder().is_test(true).try_init();
    let dfs = MiniDfs::with_features(&HashSet::new());
    let hdfs = ClientBuilder::new().with_url(&dfs.url).build().unwrap();
    write_file(&hdfs, "/data/obj.txt", b"logging-body").await;

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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(serve(listener, service, std::future::pending()));

    let base = format!("http://{addr}/hdfs");
    let http = HttpClient::builder().build().unwrap();

    // Capture tracing output (set as the thread-local default) while issuing a HEAD request.
    let capture = CaptureSubscriber::new();
    let lines = capture.lines.clone();
    let _guard = tracing::subscriber::set_default(capture);
    let resp = http
        .head(format!("{base}/data/obj.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Give the completion log line a moment to be emitted.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(_guard);

    let logged = lines.lock().unwrap().clone();
    // Find the request-id from the "request started" line for HeadObject.
    let started = logged
        .iter()
        .find(|l| l.contains("request started") && l.contains("HeadObject"))
        .expect("a 'request started' log line for HeadObject must exist");
    let rid = started
        .split("request_id:")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .expect("request_id field must be present on the start line");

    // The same request id must appear on a "request completed" line.
    let completed = logged
        .iter()
        .any(|l| l.contains("request completed") && l.contains("HeadObject") && l.contains(rid));
    assert!(
        completed,
        "request id {rid} must appear on both start and completion log lines; logged lines: {logged:?}"
    );

    server_handle.abort();
}
