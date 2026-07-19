//! Integration tests against a real HDFS cluster (MiniDFSCluster, the same Java/Maven
//! harness `hdfs-native` uses for its own tests). These exercise OUR usage of the
//! client — not HDFS itself.
//!
//! Requires `mvn` and a JDK on PATH (the MiniDfs harness shells out to Maven). The
//! `integration-test` feature on `hdfs-native` enables `hdfs_native::minidfs::MiniDfs`.

use std::collections::HashSet;

use bytes::Bytes;
use hdfs_native::minidfs::MiniDfs;
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use hdfs_s3_gateway::config::Config;
use hdfs_s3_gateway::s3::HdfsGateway;
use s3s::dto::*;
use s3s::{S3Request, S3Response, S3};

/// Build an `S3Request` carrying only the operation input (no HTTP context needed for
/// direct trait calls in tests).
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

/// Stand up a MiniDFSCluster and return it plus a gateway wired to it. The same
/// `Client` is returned so tests write through the exact instance the gateway uses.
fn setup() -> (MiniDfs, HdfsGateway, Client) {
    let _ = env_logger::builder().is_test(true).try_init();
    let dfs = MiniDfs::with_features(&HashSet::new());

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

    let client = ClientBuilder::new().with_url(&dfs.url).build().unwrap();
    let gateway = HdfsGateway::new(client.clone(), config);
    (dfs, gateway, client)
}

/// Write a file directly via the HDFS client (we are not testing writes through the
/// gateway — it's read-only — just seeding data for read tests).
async fn write_file(client: &hdfs_native::Client, path: &str, data: &[u8]) {
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
async fn hello_world_minidfs() {
    // Prove we can talk to a real cluster and read a file back.
    let dfs = MiniDfs::with_features(&HashSet::new());
    let client = ClientBuilder::new().with_url(&dfs.url).build().unwrap();

    write_file(&client, "/hello.txt", b"hello hdfs").await;

    let status = client.get_file_info("/hello.txt").await.unwrap();
    assert!(!status.isdir);
    assert_eq!(status.length, 10);

    let mut reader = client.read("/hello.txt").await.unwrap();
    let buf = reader.read_bytes(1024).await.unwrap();
    assert_eq!(&buf[..], b"hello hdfs");
}

#[tokio::test]
async fn head_object_and_bucket() {
    let (_dfs, gateway, client) = setup();
    write_file(&client, "/data/a/b.txt", b"contents").await;

    // head_bucket for the configured bucket → 200
    let resp = gateway
        .head_bucket(req(HeadBucketInput {
            bucket: "hdfs".into(),
            ..Default::default()
        }))
        .await;
    assert!(
        resp.is_ok(),
        "head_bucket should succeed for configured bucket"
    );

    // head_bucket for a different bucket → NoSuchBucket
    let err = gateway
        .head_bucket(req(HeadBucketInput {
            bucket: "other".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("NoSuchBucket"));

    // head_object on existing file → 200 with correct length
    let resp = gateway
        .head_object(req(HeadObjectInput {
            bucket: "hdfs".into(),
            key: "data/a/b.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let out: S3Response<HeadObjectOutput> = resp;
    assert_eq!(out.output.content_length, Some(8));
    assert!(out.output.e_tag.is_some());

    // head_object on a directory → NoSuchKey
    write_file(&client, "/data/dir/file.txt", b"x").await;
    let err = gateway
        .head_object(req(HeadObjectInput {
            bucket: "hdfs".into(),
            key: "data/dir".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("NoSuchKey"));

    // head_object on missing path → NoSuchKey
    let err = gateway
        .head_object(req(HeadObjectInput {
            bucket: "hdfs".into(),
            key: "data/missing.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("NoSuchKey"));
}

#[tokio::test]
async fn path_traversal_blocked() {
    let (_dfs, gateway, client) = setup();
    write_file(&client, "/secret.txt", b"topsecret").await;

    // A traversal key must not resolve to /secret.txt
    let err = gateway
        .head_object(req(HeadObjectInput {
            bucket: "hdfs".into(),
            key: "../../secret.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("NoSuchKey"));
}

#[tokio::test]
async fn list_objects_v2() {
    let (_dfs, gateway, client) = setup();
    write_file(&client, "/data/a/b/c.txt", b"1").await;
    write_file(&client, "/data/a/b/d.txt", b"2").await;
    write_file(&client, "/data/a/e.txt", b"3").await;
    write_file(&client, "/data/f.txt", b"4").await;

    // No prefix/delimiter → all objects recursively
    let resp = gateway
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "hdfs".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let out = resp.output;
    let keys: Vec<String> = out
        .contents
        .unwrap_or_default()
        .into_iter()
        .map(|o| o.key.unwrap())
        .collect();
    assert_eq!(
        keys,
        vec![
            "data/a/b/c.txt".to_string(),
            "data/a/b/d.txt".to_string(),
            "data/a/e.txt".to_string(),
            "data/f.txt".to_string(),
        ]
    );

    // delimiter=/ → subdirectories collapse to CommonPrefixes
    let resp = gateway
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "hdfs".into(),
            delimiter: Some("/".into()),
            ..Default::default()
        }))
        .await
        .unwrap();
    let out = resp.output;
    let prefixes: Vec<String> = out
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .map(|p| p.prefix.unwrap())
        .collect();
    assert_eq!(prefixes, vec!["data/".to_string()]);
}

#[tokio::test]
async fn list_objects_v2_prefix_pagination_and_order() {
    // Prefix filtering, max-keys pagination with continuation-token
    // round-trip, deeply-nested single CommonPrefix collapse, and strict binary-lexicographic
    // ordering — all driven through the real `s3s` trait (HTTP-shaped, no HTTP server needed).
    let (_dfs, gateway, client) = setup();

    // A small tree under /data/list/.
    write_file(&client, "/data/list/a.txt", b"a").await;
    write_file(&client, "/data/list/b.txt", b"b").await;
    write_file(&client, "/data/list/sub/c.txt", b"c").await;
    write_file(&client, "/data/list/sub/d.txt", b"d").await;
    write_file(&client, "/data/list/sub/deep/e.txt", b"e").await;
    write_file(&client, "/data/list2/x.txt", b"x").await;

    // --- prefix filtering -------------------------------------------------------
    let resp = gateway
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "hdfs".into(),
            prefix: Some("data/list/sub/".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let keys: Vec<String> = resp
        .contents
        .unwrap_or_default()
        .into_iter()
        .map(|o| o.key.unwrap())
        .collect();
    assert_eq!(
        keys,
        vec![
            "data/list/sub/c.txt".to_string(),
            "data/list/sub/d.txt".to_string(),
            "data/list/sub/deep/e.txt".to_string(),
        ]
    );

    // --- deeply nested collapses to ONE CommonPrefix at the requested level --------
    let resp = gateway
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "hdfs".into(),
            prefix: Some("data/list/".into()),
            delimiter: Some("/".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let contents: Vec<String> = resp
        .contents
        .unwrap_or_default()
        .into_iter()
        .map(|o| o.key.unwrap())
        .collect();
    let prefixes: Vec<String> = resp
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .map(|p| p.prefix.unwrap())
        .collect();
    // "data/list/a.txt", "data/list/b.txt" are direct Contents; "data/list/sub/" collapses
    // to a single CommonPrefix (NOT one per nesting level).
    assert_eq!(
        contents,
        vec!["data/list/a.txt".to_string(), "data/list/b.txt".to_string()]
    );
    assert_eq!(prefixes, vec!["data/list/sub/".to_string()]);

    // --- max-keys pagination with continuation-token round-trip ------------------
    // List the whole bucket with max-keys=2 and walk pages.
    let mut all: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    let expected = vec![
        "data/list/a.txt".to_string(),
        "data/list/b.txt".to_string(),
        "data/list/sub/c.txt".to_string(),
        "data/list/sub/d.txt".to_string(),
        "data/list/sub/deep/e.txt".to_string(),
        "data/list2/x.txt".to_string(),
    ];
    loop {
        let resp = gateway
            .list_objects_v2(req(ListObjectsV2Input {
                bucket: "hdfs".into(),
                max_keys: Some(2),
                continuation_token: token.clone(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let page: Vec<String> = resp
            .contents
            .unwrap_or_default()
            .into_iter()
            .map(|o| o.key.unwrap())
            .collect();
        all.extend(page);
        match resp.next_continuation_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    assert_eq!(
        all, expected,
        "paginated listing must equal the full recursive listing"
    );

    // --- strict binary-lexicographic ordering ------------------------------------
    // Keys must be sorted by raw byte order, not locale/collation. Insert a key with a
    // byte that sorts after lowercase letters (e.g. '~') to confirm byte order.
    write_file(&client, "/data/list/~z.txt", b"z").await;
    let resp = gateway
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "hdfs".into(),
            prefix: Some("data/list/".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let keys: Vec<String> = resp
        .contents
        .unwrap_or_default()
        .into_iter()
        .map(|o| o.key.unwrap())
        .collect();
    let mut sorted = keys.clone();
    sorted.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    assert_eq!(
        keys, sorted,
        "listing must be in strict binary-lexicographic order"
    );
    // '~' (0x7E) sorts after 'a'/'b'/'s' (0x61-0x73), so it must be last.
    assert_eq!(keys.last().unwrap(), "data/list/~z.txt");
}

#[tokio::test]
async fn list_objects_v2_large_dir() {
    // A directory with many thousands of entries must list completely
    // and return every key exactly once. This also surfaces whether `hdfs-native`'s listing
    // returns everything in one shot (it does per docs) rather than paginating internally.
    let (_dfs, gateway, client) = setup();
    let n: usize = 3000;
    for i in 0..n {
        write_file(&client, &format!("/data/bigdir/file_{i:05}.bin"), b"x").await;
    }

    let start = std::time::Instant::now();
    // Walk all pages (default max-keys is 1000, so we must paginate to collect everything).
    let mut keys: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let resp = gateway
            .list_objects_v2(req(ListObjectsV2Input {
                bucket: "hdfs".into(),
                prefix: Some("data/bigdir/".into()),
                continuation_token: token.clone(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        keys.extend(
            resp.contents
                .unwrap_or_default()
                .into_iter()
                .map(|o| o.key.unwrap()),
        );
        match resp.next_continuation_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    let elapsed = start.elapsed();

    assert_eq!(keys.len(), n, "every entry must be listed exactly once");
    // No duplicates.
    let mut uniq = keys.clone();
    uniq.sort();
    uniq.dedup();
    assert_eq!(uniq.len(), n, "no duplicate keys in listing");
    // Loose latency gate: 3000 entries should list well under a few seconds against MiniDFS.
    assert!(
        elapsed.as_secs() < 30,
        "listing 3000 entries took too long: {elapsed:?}"
    );
}

#[tokio::test]
async fn get_object_full_and_ranged() {
    let (_dfs, gateway, client) = setup();

    // A 1 MiB file of a repeating pattern.
    let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
    write_file(&client, "/data/big.bin", &data).await;

    // Full GET → body bytes match exactly, correct Content-Length.
    let resp = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/big.bin".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let out = resp.output;
    assert_eq!(out.content_length, Some((1024 * 1024) as i64));
    let body = out.body.unwrap();
    let bytes = collect_body(body).await;
    assert_eq!(&bytes[..], &data[..]);

    // Range bytes=0-99 → first 100 bytes.
    let resp = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/big.bin".into(),
            range: Some(Range::Int {
                first: 0,
                last: Some(99),
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    let out = resp.output;
    assert_eq!(out.content_length, Some(100));
    assert_eq!(out.content_range.as_deref(), Some("bytes 0-99/1048576"));
    let bytes = collect_body(out.body.unwrap()).await;
    assert_eq!(&bytes[..], &data[0..100]);

    // Range bytes=100- → from offset 100 to end.
    let resp = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/big.bin".into(),
            range: Some(Range::Int {
                first: 100,
                last: None,
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    let out = resp.output;
    assert_eq!(out.content_length, Some((1024 * 1024 - 100) as i64));
    let bytes = collect_body(out.body.unwrap()).await;
    assert_eq!(&bytes[..], &data[100..]);

    // Range bytes=-500 → last 500 bytes.
    let resp = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/big.bin".into(),
            range: Some(Range::Suffix { length: 500 }),
            ..Default::default()
        }))
        .await
        .unwrap();
    let out = resp.output;
    assert_eq!(out.content_length, Some(500));
    let bytes = collect_body(out.body.unwrap()).await;
    assert_eq!(&bytes[..], &data[1024 * 1024 - 500..]);

    // Unsatisfiable range → InvalidRange (416).
    let err = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/big.bin".into(),
            range: Some(Range::Int {
                first: 1024 * 1024,
                last: None,
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("InvalidRange"));

    // get_object on a directory → NoSuchKey.
    write_file(&client, "/data/dir/file.txt", b"x").await;
    let err = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/dir".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("NoSuchKey"));
}

#[tokio::test]
async fn get_object_conditionals() {
    let (_dfs, gateway, client) = setup();
    write_file(&client, "/data/c.txt", b"conditional").await;

    // Capture the ETag from head_object.
    let head = gateway
        .head_object(req(HeadObjectInput {
            bucket: "hdfs".into(),
            key: "data/c.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let etag = head.output.e_tag.clone().unwrap();

    // If-None-Match: <etag> → 304 NotModified.
    let err = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/c.txt".into(),
            if_none_match: Some(ETagCondition::ETag(etag.clone())),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("NotModified"));

    // If-Match: <etag> → 200 (matches).
    let resp = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/c.txt".into(),
            if_match: Some(ETagCondition::ETag(etag.clone())),
            ..Default::default()
        }))
        .await
        .unwrap();
    let bytes = collect_body(resp.output.body.unwrap()).await;
    assert_eq!(&bytes[..], b"conditional");

    // If-Match: wrong etag → 412 PreconditionFailed.
    let wrong = ETag::Strong("deadbeef".to_string());
    let err = gateway
        .get_object(req(GetObjectInput {
            bucket: "hdfs".into(),
            key: "data/c.txt".into(),
            if_match: Some(ETagCondition::ETag(wrong)),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("PreconditionFailed"));
}

/// Collect a streaming body into a single `Vec<u8>`.
async fn collect_body(body: s3s::dto::StreamingBlob) -> Vec<u8> {
    use futures::StreamExt;
    let mut out = Vec::new();
    let mut stream = body;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("body chunk should not error in tests");
        out.extend_from_slice(&chunk);
    }
    out
}

/// Assert that an `S3Result` is an `AccessDenied` error (write-op policy).
fn assert_access_denied<T: std::fmt::Debug>(res: s3s::S3Result<T>) {
    let err = res.unwrap_err();
    let dbg = format!("{err:?}");
    assert!(
        dbg.contains("AccessDenied"),
        "expected AccessDenied, got: {dbg}"
    );
}

#[tokio::test]
async fn write_ops_uniformly_denied() {
    // Every write-shaped operation returns a uniform AccessDenied, regardless of
    // whether the target exists. We seed a file so existence is not the deciding factor.
    let (_dfs, gateway, client) = setup();
    write_file(&client, "/data/w.txt", b"x").await;

    // Object-level writes.
    assert_access_denied(
        gateway
            .put_object(req(PutObjectInput {
                bucket: "hdfs".into(),
                key: "data/w.txt".into(),
                ..Default::default()
            }))
            .await,
    );
    assert_access_denied(
        gateway
            .delete_object(req(DeleteObjectInput {
                bucket: "hdfs".into(),
                key: "data/w.txt".into(),
                ..Default::default()
            }))
            .await,
    );
    assert_access_denied(
        gateway
            .delete_objects(req(DeleteObjectsInput {
                bucket: "hdfs".into(),
                bypass_governance_retention: None,
                checksum_algorithm: None,
                delete: Delete {
                    objects: ObjectIdentifierList::default(),
                    quiet: None,
                },
                expected_bucket_owner: None,
                mfa: None,
                request_payer: None,
            }))
            .await,
    );
    assert_access_denied(
        gateway
            .copy_object(req(CopyObjectInput::builder()
                .bucket("hdfs".to_string())
                .key("data/w2.txt".to_string())
                .copy_source(CopySource::Bucket {
                    bucket: "hdfs".into(),
                    key: "data/w.txt".into(),
                    version_id: None,
                })
                .build()
                .unwrap()))
            .await,
    );

    // Bucket-level writes.
    assert_access_denied(
        gateway
            .create_bucket(req(CreateBucketInput {
                bucket: "hdfs".into(),
                ..Default::default()
            }))
            .await,
    );
    assert_access_denied(
        gateway
            .delete_bucket(req(DeleteBucketInput {
                bucket: "hdfs".into(),
                ..Default::default()
            }))
            .await,
    );

    // Multipart upload lifecycle.
    assert_access_denied(
        gateway
            .create_multipart_upload(req(CreateMultipartUploadInput {
                bucket: "hdfs".into(),
                key: "data/w.txt".into(),
                ..Default::default()
            }))
            .await,
    );
    assert_access_denied(
        gateway
            .upload_part(req(UploadPartInput {
                bucket: "hdfs".into(),
                key: "data/w.txt".into(),
                ..Default::default()
            }))
            .await,
    );
    assert_access_denied(
        gateway
            .complete_multipart_upload(req(CompleteMultipartUploadInput {
                bucket: "hdfs".into(),
                key: "data/w.txt".into(),
                ..Default::default()
            }))
            .await,
    );
    assert_access_denied(
        gateway
            .abort_multipart_upload(req(AbortMultipartUploadInput {
                bucket: "hdfs".into(),
                key: "data/w.txt".into(),
                ..Default::default()
            }))
            .await,
    );
    assert_access_denied(
        gateway
            .list_multipart_uploads(req(ListMultipartUploadsInput {
                bucket: "hdfs".into(),
                ..Default::default()
            }))
            .await,
    );
    assert_access_denied(
        gateway
            .list_parts(req(ListPartsInput {
                bucket: "hdfs".into(),
                key: "data/w.txt".into(),
                ..Default::default()
            }))
            .await,
    );

    // Restore (object-level lifecycle write).
    assert_access_denied(
        gateway
            .restore_object(req(RestoreObjectInput {
                bucket: "hdfs".into(),
                key: "data/w.txt".into(),
                ..Default::default()
            }))
            .await,
    );
}

#[tokio::test]
async fn bucket_config_probes_not_configured() {
    // Bucket-config probes answer the way real S3 would on a fresh bucket.
    let (_dfs, gateway, _client) = setup();

    // Versioning → disabled (empty status).
    let out = gateway
        .get_bucket_versioning(req(GetBucketVersioningInput {
            bucket: "hdfs".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(
        out.status.is_none(),
        "versioning should be 'not configured'"
    );

    // Tagging → empty tag set.
    let out = gateway
        .get_bucket_tagging(req(GetBucketTaggingInput {
            bucket: "hdfs".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(out.tag_set.is_empty(), "tagging should be empty");

    // ACL → no owner/grants.
    let out = gateway
        .get_bucket_acl(req(GetBucketAclInput {
            bucket: "hdfs".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(
        out.owner.is_none() && out.grants.is_none(),
        "acl should be 'not configured'"
    );

    // CORS → no rules.
    let out = gateway
        .get_bucket_cors(req(GetBucketCorsInput {
            bucket: "hdfs".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(out.cors_rules.is_none(), "cors should be 'not configured'");
}
