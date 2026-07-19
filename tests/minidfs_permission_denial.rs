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
//! Requires a pre-started HDFS cluster (see `tests/common/mod.rs` and the CI workflow).

use hdfs_native::ClientBuilder;
use hdfs_s3_gateway::s3::HdfsGateway;
use s3s::dto::*;
use s3s::{S3Request, S3};

mod common;
use common::TestScope;

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

/// Build a gateway whose HDFS client acts as `user` (an unprivileged principal).
fn gateway_as(scope: &TestScope, user: &str) -> HdfsGateway {
    scope.gateway_as(user)
}

#[tokio::test]
async fn permission_denied_maps_to_access_denied() {
    let _ = env_logger::builder().is_test(true).try_init();
    let scope = TestScope::new().await;

    // Seed a secret file as the superuser (default client identity) with mode 0600 so no
    // other user can read it.
    let secret = format!("{}/secret.txt", scope.root);
    scope.write_file("secret.txt", b"topsecret").await;
    let super_client = ClientBuilder::new()
        .with_url(&scope.config().namenode_uri)
        .build()
        .unwrap();
    super_client.set_permission(&secret, 0o600).await.unwrap();
    super_client
        .set_owner(&secret, Some("root"), Some("supergroup"))
        .await
        .unwrap();

    // A different (unprivileged) user reads through the gateway → AccessDenied (403).
    //
    // NOTE: HDFS `getFileInfo` (HEAD) does NOT enforce read permission on the file
    // itself — only the actual block-read path (`getBlockLocations`, used by GET) does.
    // So we assert on `get_object`, which is where the NameNode returns
    // `AccessControlException` and `hdfs-native` surfaces it as an `RPCError`.
    let gateway = gateway_as(&scope, "nobody");

    let err = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "secret.txt".into(),
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
    let scope = TestScope::new().await;

    let path = format!("{}/owned.txt", scope.root);
    scope.write_file("owned.txt", b"mine").await;
    let super_client = ClientBuilder::new()
        .with_url(&scope.config().namenode_uri)
        .build()
        .unwrap();
    super_client.set_permission(&path, 0o600).await.unwrap();

    let gateway = gateway_as(&scope, "root");
    let resp = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "owned.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(resp.output.content_length, Some(4));
}
