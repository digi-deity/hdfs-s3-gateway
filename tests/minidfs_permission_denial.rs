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

use bytes::Bytes;
use hdfs_native::{ClientBuilder, WriteOptions};
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

#[tokio::test]
async fn deeper_path_under_unreadable_ancestor_is_denied_by_traverse_check() {
    // Boundary of the "list what you know" capability: the gateway resolves a
    // prefix's own directory and RPCs it DIRECTLY — it never requires the parent to
    // be readable, so a known deeper path is always attempted as-is. But HDFS POSIX
    // semantics enforce EXECUTE permission on every ancestor for any operation on a
    // path, so a directory under an unreadable ancestor is unreachable even when the
    // deeper directory itself grants full rights to the user. The NameNode answers
    // AccessControlException, and the gateway must surface it as 403 AccessDenied —
    // not as an empty listing (which would lie) and not as an InternalError.
    //
    // This is HDFS's rule, not the gateway's: there is no "no rights on the parent,
    // rights deeper" case in real HDFS that a fallback could serve. Pin it so a
    // future "walk ancestors to discover the prefix" change cannot silently break it.
    let _ = env_logger::builder().is_test(true).try_init();
    let scope = TestScope::new().await;

    // Seed `forbidden/inner` as the superuser: the parent is 000, but the inner
    // directory is owned by `nobody` with full rights (0777) — the maximal version
    // of the "I know this deeper path and it is mine" scenario.
    let forbidden = format!("{}/forbidden", scope.root);
    let inner = format!("{forbidden}/inner");
    let super_client = ClientBuilder::new()
        .with_url(&scope.config().namenode_uri)
        .build()
        .unwrap();
    super_client.mkdirs(&forbidden, 0o000, true).await.unwrap();
    super_client.mkdirs(&inner, 0o777, true).await.unwrap();
    super_client
        .set_owner(&inner, Some("nobody"), Some("nobody"))
        .await
        .unwrap();
    // A leaf file inside the user-owned directory, created by the superuser.
    let leaf = format!("{inner}/leaf.txt");
    let mut writer = super_client
        .create(&leaf, &WriteOptions::default().overwrite(true))
        .await
        .unwrap();
    writer
        .write_bytes(Bytes::copy_from_slice(b"deep"))
        .await
        .unwrap();
    writer.close().await.unwrap();

    // Sanity via the HDFS client itself: even `getFileInfo` on the deeper path is
    // denied for `nobody` — the NameNode's traverse check fires before anything
    // else. (This is the crux: no gateway fallback could do better.)
    let nobody_client = ClientBuilder::new()
        .with_url(&scope.config().namenode_uri)
        .with_user("nobody".to_string())
        .build()
        .unwrap();
    let err = nobody_client.get_file_info(&inner).await.unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessControlException"),
        "HDFS must deny the deeper path via the ancestor traverse check: {err:?}"
    );

    // Through the gateway: listing the KNOWN deeper prefix must be 403 AccessDenied
    // (the NameNode's answer, mapped faithfully) — not an empty page.
    let gateway = gateway_as(&scope, "nobody");
    for prefix in [
        Some("forbidden/inner/".to_string()),
        Some("forbidden/inner/leaf.txt".to_string()),
    ] {
        let err = gateway
            .list_objects_v2(req(ListObjectsV2Input {
                bucket: "hdfs".into(),
                prefix,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        let dbg = format!("{err:?}");
        assert!(
            dbg.contains("AccessDenied"),
            "known deeper prefix under an unreadable ancestor must be 403, got: {dbg}"
        );
    }
}
