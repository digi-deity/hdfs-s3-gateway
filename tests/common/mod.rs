//! Shared test harness for the integration tests.
//!
//! These tests require a *pre-started* HDFS cluster (e.g. a `MiniDFSCluster` launched in
//! bash before `cargo test`, mirroring the Python CI flow). The cluster endpoint is read
//! from `HDFS_NAMENODE_URI` and defaults to `hdfs://127.0.0.1:9000`. We deliberately do
//! NOT spawn a cluster from within Rust — that was the old design and it forced every test
//! binary to (re)start MiniDFS and clobber the shared `target/test/data/dfs` work dir.
//!
//! Each test gets its own unique HDFS root directory (`/__t_<pid>_<n>`). All fixtures are
//! written under that root, and the root is recursively deleted when the `TestScope` is
//! dropped — so tests never collide with each other and always clean up after themselves,
//! even on panic.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use hdfs_s3_gateway::config::Config;
use hdfs_s3_gateway::s3::HdfsGateway;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Endpoint of the pre-started HDFS cluster.
pub fn minidfs_url() -> String {
    std::env::var("HDFS_NAMENODE_URI").unwrap_or_else(|_| "hdfs://127.0.0.1:9000".to_string())
}

/// A per-test isolated HDFS namespace. Created with a unique root; deleted on drop.
pub struct TestScope {
    pub root: String,
    pub client: Client,
}

impl TestScope {
    /// Connect to the shared cluster and create a fresh, unique root directory.
    #[allow(dead_code)]
    pub async fn new() -> Self {
        let url = minidfs_url();
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let root = format!("/__t_{}_{}", std::process::id(), n);

        let client = ClientBuilder::new().with_url(&url).build().unwrap();
        // Create the isolated root so fixtures have a home. Ignore if it already exists.
        let _ = client.mkdirs(&root, 0o755, true).await;

        TestScope { root, client }
    }

    /// Default gateway `Config` rooted at this scope's directory.
    #[allow(dead_code)]
    pub fn config(&self) -> Config {
        self.config_with(2048, None)
    }

    /// Gateway `Config` with a custom in-flight cap and optional auth secret.
    #[allow(dead_code)]
    pub fn config_with(&self, max_concurrent: usize, auth_secret: Option<String>) -> Config {
        Config {
            namenode_uri: minidfs_url(),
            hdfs_root: self.root.clone(),
            bucket_name: "hdfs".to_string(),
            listen_addr: "127.0.0.1:0".to_string(),
            max_concurrent_requests: max_concurrent,
            expose_upstream_errors: false,
            hdfs_options: Default::default(),
            hdfs_config_dir: None,
            hdfs_user: None,
            auth_secret,
        }
    }

    /// Build a gateway whose HDFS client acts as `user` (an unprivileged principal).
    #[allow(dead_code)]
    pub fn gateway_as(&self, user: &str) -> HdfsGateway {
        let config = self.config();
        let client = ClientBuilder::new()
            .with_url(minidfs_url())
            .with_user(user.to_string())
            .build()
            .unwrap();
        HdfsGateway::new(client, config)
    }

    /// Write `data` to `rel` (a path relative to this scope's root; a leading `/` is
    /// stripped). Fixtures always land under the isolated root, so they are cleaned up
    /// with the scope.
    #[allow(dead_code)]
    pub async fn write_file(&self, rel: &str, data: &[u8]) {
        let path = format!("{}/{}", self.root, rel.trim_start_matches('/'));
        let mut writer = self
            .client
            .create(&path, &WriteOptions::default().overwrite(true))
            .await
            .unwrap();
        writer
            .write_bytes(Bytes::copy_from_slice(data))
            .await
            .unwrap();
        writer.close().await.unwrap();
    }
}

impl Drop for TestScope {
    fn drop(&mut self) {
        // Best-effort cleanup: remove the whole isolated namespace on a fresh OS thread
        // with its own blocking HDFS client (we may be inside a tokio runtime, where a
        // sync client cannot be constructed). Failures are non-fatal and must not panic
        // in a destructor (e.g. cluster already torn down).
        let root = self.root.clone();
        let url = minidfs_url();
        let _ = std::thread::spawn(move || {
            if let Ok(client) = hdfs_native::sync::ClientBuilder::new()
                .with_url(&url)
                .build()
            {
                let _ = client.delete(&root, true);
            }
        })
        .join();
    }
}
