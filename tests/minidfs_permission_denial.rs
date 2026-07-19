//! Permission-denial scenario against a real MiniDFSCluster.
//!
//! This is the enterprise-Hadoop case from the error-mapping discussion: the caller
//! reaches the NameNode but lacks POSIX/ACL rights to the file. We seed a file owned by
//! the superuser with mode `0600`, then read it through a *second* client acting as a
//! different (unprivileged) user. The NameNode rejects the read with an
//! `AccessControlException`, which `hdfs-native` surfaces as an `RPCError`, and our
//! gateway must translate that into S3 `AccessDenied` (HTTP 403) — not a generic
//! `InternalError` (500).
//!
//! Requires `mvn` + JDK on PATH (MiniDfs shells out to Maven). The `integration-test`
//! feature on `hdfs-native` enables `hdfs_native::minidfs::MiniDfs`.

use std::collections::HashSet;

use bytes::Bytes;
use hdfs_native::minidfs::MiniDfs;
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use hdfs_s3_gateway::config::Config;
use hdfs_s3_gateway::s3::HdfsGateway;
use s3s::dto::*;
use s3s::{S3Request, S3};

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
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

/// Build a gateway whose HDFS client acts as `user` (an unprivileged principal).
fn gateway_as(dfs: &MiniDfs, user: &str) -> HdfsGateway {
    let config = Config {
        namenode_uri: dfs.url.clone(),
        hdfs_root: "/".to_string(),
        bucket_name: "hdfs".to_string(),
        listen_addr: "0.0.0.0:0".to_string(),
        max_concurrent_requests: 64,
        expose_upstream_errors: false,
        hdfs_options: Default::default(),
        hdfs_config_dir: None,
        hdfs_user: None,
    };
    let client = ClientBuilder::new()
        .with_url(&dfs.url)
        .with_user(user.to_string())
        .build()
        .unwrap();
    HdfsGateway::new(client, config)
}

#[tokio::test]
async fn permission_denied_maps_to_access_denied() {
    let _ = env_logger::builder().is_test(true).try_init();
    let dfs = MiniDfs::with_features(&HashSet::new());

    // Seed a secret file as the superuser (default client identity) with mode 0600 so no
    // other user can read it.
    let secret = "/data/secret.txt";
    write_file(
        &ClientBuilder::new().with_url(&dfs.url).build().unwrap(),
        secret,
        b"topsecret",
    )
    .await;
    let super_client = ClientBuilder::new().with_url(&dfs.url).build().unwrap();
    super_client.set_permission(secret, 0o600).await.unwrap();
    super_client
        .set_owner(secret, Some("root"), Some("supergroup"))
        .await
        .unwrap();

    // A different (unprivileged) user reads through the gateway → AccessDenied (403).
    //
    // NOTE: HDFS `getFileInfo` (HEAD) does NOT enforce read permission on the file
    // itself — only the actual block-read path (`getBlockLocations`, used by GET) does.
    // So we assert on `get_object`, which is where the NameNode returns
    // `AccessControlException` and `hdfs-native` surfaces it as an `RPCError`.
    let gateway = gateway_as(&dfs, "nobody");

    let err = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/secret.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    let dbg = format!("{err:?}");
    assert!(
        dbg.contains("AccessDenied"),
        "permission denial must map to AccessDenied, got: {dbg}"
    );
}

#[tokio::test]
async fn owner_can_still_read_after_chmod() {
    // Sanity: the owner (superuser) is unaffected by the restrictive mode and reads fine.
    let _ = env_logger::builder().is_test(true).try_init();
    let dfs = MiniDfs::with_features(&HashSet::new());

    let path = "/data/owned.txt";
    write_file(
        &ClientBuilder::new().with_url(&dfs.url).build().unwrap(),
        path,
        b"mine",
    )
    .await;
    let super_client = ClientBuilder::new().with_url(&dfs.url).build().unwrap();
    super_client.set_permission(path, 0o600).await.unwrap();

    let gateway = gateway_as(&dfs, "root");
    let resp = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/owned.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(resp.output.content_length, Some(4));
}
