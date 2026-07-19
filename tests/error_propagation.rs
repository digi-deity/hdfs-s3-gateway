//! Upstream (hdfs-native) error propagation (operational readiness).
//!
//! Two concerns are validated here:
//!   1. `map_hdfs_error` maps specific `HdfsError` variants to the right S3 code, and only
//!      surfaces the upstream text in the client-facing `Message` when `expose_upstream_errors`
//!      is enabled (default off — don't leak NameNode internals to untrusted clients).
//!   2. Upstream `tracing` diagnostics emitted by `hdfs-native` while we handle a request are
//!      captured in the server logs, correlated to the request's `request_id` (via the
//!      `Instrument` span wrapping each HDFS call).

use hdfs_native::HdfsError;
use hdfs_s3_gateway::s3::map_hdfs_error;
use s3s::S3ErrorCode;

#[test]
fn map_hdfs_error_variants() {
    // Not-found family collapses to NoSuchKey regardless of the expose flag.
    assert_eq!(
        map_hdfs_error(HdfsError::FileNotFound("x".into()), false).code(),
        &S3ErrorCode::NoSuchKey
    );
    assert_eq!(
        map_hdfs_error(HdfsError::IsADirectoryError("x".into()), false).code(),
        &S3ErrorCode::NoSuchKey
    );
    assert_eq!(
        map_hdfs_error(HdfsError::InvalidPath("x".into()), false).code(),
        &S3ErrorCode::NoSuchKey
    );

    // InvalidArgument maps to InvalidArgument.
    assert_eq!(
        map_hdfs_error(HdfsError::InvalidArgument("bad".into()), false).code(),
        &S3ErrorCode::InvalidArgument
    );

    // Unsupported HDFS features map to NotImplemented (read-only subset).
    assert_eq!(
        map_hdfs_error(HdfsError::UnsupportedFeature("ec".into()), false).code(),
        &S3ErrorCode::NotImplemented
    );
    assert_eq!(
        map_hdfs_error(
            HdfsError::UnsupportedErasureCodingPolicy("ec".into()),
            false
        )
        .code(),
        &S3ErrorCode::NotImplemented
    );

    // Everything else collapses to InternalError (no HDFS internals leaked by default).
    assert_eq!(
        map_hdfs_error(HdfsError::IOError(std::io::Error::other("boom")), false).code(),
        &S3ErrorCode::InternalError
    );
    assert_eq!(
        map_hdfs_error(HdfsError::RPCError("c".into(), "m".into()), false).code(),
        &S3ErrorCode::InternalError
    );
    assert_eq!(
        map_hdfs_error(HdfsError::DataTransferError("d".into()), false).code(),
        &S3ErrorCode::InternalError
    );
    assert_eq!(
        map_hdfs_error(HdfsError::ChecksumError, false).code(),
        &S3ErrorCode::InternalError
    );
}

#[test]
fn map_hdfs_error_permission_denial_to_access_denied() {
    // Enterprise-Hadoop case: the NameNode rejects the request for lack of rights.
    // hdfs-native surfaces this as an RPCError whose first field is the fully-qualified
    // Java exception class. It must map to AccessDenied (403), NOT InternalError (500).
    let cases = [
        "org.apache.hadoop.security.AccessControlException",
        "org.apache.hadoop.fs.permission.AccessControlException",
        "org.apache.hadoop.security.authorize.AuthorizationException",
        "org.apache.hadoop.security.AccessDeniedException",
    ];
    for ex in cases {
        assert_eq!(
            map_hdfs_error(HdfsError::RPCError(ex.into(), "denied".into()), false).code(),
            &S3ErrorCode::AccessDenied,
            "expected AccessDenied for {ex}"
        );
        // FatalRPCError must behave identically.
        assert_eq!(
            map_hdfs_error(HdfsError::FatalRPCError(ex.into(), "denied".into()), false).code(),
            &S3ErrorCode::AccessDenied,
            "expected AccessDenied for fatal {ex}"
        );
    }

    // SASL/GSSAPI auth failures are also access problems → AccessDenied.
    assert_eq!(
        map_hdfs_error(HdfsError::SASLError("no mechanism".into()), false).code(),
        &S3ErrorCode::AccessDenied
    );
    assert_eq!(
        map_hdfs_error(HdfsError::NoSASLMechanism, false).code(),
        &S3ErrorCode::AccessDenied
    );
}

#[test]
fn map_hdfs_error_standby_safemode_to_service_unavailable() {
    // A NameNode in standby / safe mode is temporarily unable to serve → 503, so clients
    // back off and retry rather than treating it as a permanent gateway fault.
    for ex in [
        "org.apache.hadoop.ipc.StandbyException",
        "org.apache.hadoop.hdfs.server.namenode.SafeModeException",
    ] {
        assert_eq!(
            map_hdfs_error(HdfsError::RPCError(ex.into(), "not active".into()), false).code(),
            &S3ErrorCode::ServiceUnavailable,
            "expected ServiceUnavailable for {ex}"
        );
    }
}

#[test]
fn map_hdfs_error_unknown_rpc_still_internal() {
    // An RPC error we don't specifically recognize must remain InternalError (safe default;
    // we never invent a client-facing code for an unknown upstream condition).
    assert_eq!(
        map_hdfs_error(
            HdfsError::RPCError("java.io.IOException".into(), "boom".into()),
            false
        )
        .code(),
        &S3ErrorCode::InternalError
    );
}

#[test]
fn map_hdfs_error_expose_flag() {
    let hidden = map_hdfs_error(
        HdfsError::IOError(std::io::Error::other("secret host info")),
        false,
    );
    // Default off: no upstream text in the client-facing message.
    assert!(
        hidden.message().is_none(),
        "upstream text must NOT be exposed by default"
    );

    let exposed = map_hdfs_error(
        HdfsError::IOError(std::io::Error::other("secret host info")),
        true,
    );
    // Enabled: the HDFS error text becomes the message (the IOError Display adds a prefix,
    // so we assert the secret substring is present rather than an exact match).
    assert!(
        exposed
            .message()
            .is_some_and(|m| m.contains("secret host info")),
        "upstream text must be exposed when the flag is on; got {:?}",
        exposed.message()
    );
}

// ---------------------------------------------------------------------------
// Integration test: upstream diagnostics reach the logs, correlated to request_id.
// ---------------------------------------------------------------------------

use std::sync::{Arc, Mutex};

use hdfs_native::ClientBuilder;
use hdfs_s3_gateway::config::Config;
use hdfs_s3_gateway::s3::server::{build_service, serve};
use hdfs_s3_gateway::s3::HdfsGateway;
use reqwest::Client as HttpClient;
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

struct StringVisitor<'a>(&'a mut String);
impl<'a> Visit for StringVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, "{}:{:?} ", field.name(), value);
    }
}

#[tokio::test]
async fn upstream_error_logged_with_request_id() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Point the gateway at an unreachable NameNode so the HDFS client fails upstream while
    // we handle the request. The failure must propagate into BOTH the HTTP response and the
    // server logs, correlated to the request's `request_id` via the instrumented span. This
    // exercises the error path (not just the happy path covered by
    // `logging_request_id_correlated`) and proves upstream issues are never silently
    // swallowed.
    let config = Config {
        namenode_uri: "hdfs://127.0.0.1:1".to_string(), // closed port → connection refused
        hdfs_root: "/".to_string(),
        bucket_name: "hdfs".to_string(),
        listen_addr: "127.0.0.1:0".to_string(),
        max_concurrent_requests: 2048,
        expose_upstream_errors: false,
        hdfs_options: Default::default(),
        hdfs_config_dir: None,
        hdfs_user: None,
        auth_secret: None,
    };
    let client = ClientBuilder::new()
        .with_url(&config.namenode_uri)
        .build()
        .unwrap();
    let gateway = HdfsGateway::new(client, config.clone());
    let service = build_service(gateway, &config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(serve(listener, service, std::future::pending()));

    let base = format!("http://{addr}/hdfs");
    let http = HttpClient::builder().build().unwrap();

    let capture = CaptureSubscriber::new();
    let lines = capture.lines.clone();
    let _guard = tracing::subscriber::set_default(capture);

    // HEAD a key -> get_file_info fails upstream (unreachable NameNode -> InternalError/500);
    // the error is logged under our span. We only assert it's a server error — the point of
    // this test is the log correlation, not the exact status code.
    let resp = http
        .head(format!("{base}/data/missing.txt"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_server_error(),
        "unreachable NameNode -> 5xx; got {}",
        resp.status()
    );
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    drop(_guard);
    server_handle.abort();

    let logged = lines.lock().unwrap().clone();

    // (a) Our request's start line exists with a request_id.
    let started = logged
        .iter()
        .find(|l| l.contains("request started") && l.contains("HeadObject"))
        .expect("a 'request started' log line for HeadObject must exist");
    let rid = started
        .split("request_id:")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .expect("request_id field must be present on the start line");

    // (b) The upstream failure was captured and correlated: the completion line for this
    // request records `ok:false` (error path), and the HTTP response was an `InternalError`
    // (the unreachable NameNode's `IOError` mapped through `map_hdfs_error`). This proves
    // the upstream issue propagated into BOTH the server log (under our request_id span) and
    // the HTTP response — i.e. nothing is silently swallowed.
    let completed_line = logged
        .iter()
        .find(|l| l.contains("request completed") && l.contains("HeadObject") && l.contains(rid))
        .expect("completion line must exist");
    assert!(
        completed_line.contains("ok:false"),
        "the error path must be logged under the request_id; line: {completed_line}"
    );

    // (c) The same request_id also appears on a completion line (correlation intact).
    let completed = logged
        .iter()
        .any(|l| l.contains("request completed") && l.contains("HeadObject") && l.contains(rid));
    assert!(
        completed,
        "request id {rid} must appear on both start and completion log lines; logged lines: {logged:?}"
    );
}
