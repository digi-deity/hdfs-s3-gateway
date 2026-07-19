//! The `impl S3 for HdfsGateway` — the thin mechanical layer connecting the `core`
//! module to `s3s`. Each method parses the trait input, calls `core`
//! / the HDFS client, and maps the result into an `s3s` output or error type.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::core::{
    decode_token, encode_token, list_to_contents, ListEntry, ObjectMetadata, PathMapper,
};
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use hdfs_native::Client;
use hdfs_native::ClientBuilder;
use s3s::dto::*;
use s3s::header::X_AMZ_REQUEST_ID;
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use tokio_util::io::ReaderStream;
use tracing::Instrument;

/// Per-process monotonic counter used to make request ids unique within a process.
static REQUEST_SEQ: AtomicU64 = AtomicU64::new(0);

/// Generate a per-request id. We are not AWS, so we mint our own: a base36 timestamp
/// plus a process-local sequence number. This is surfaced in the `x-amz-request-id`
/// header and in logs so clients/operators can correlate requests.
fn new_request_id() -> String {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    let seq = REQUEST_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{:x}-{:x}", micros, seq)
}

/// Per-request logging context (operational readiness).
///
/// Created at the top of each S3 operation. Records the op name and a freshly-minted
/// `request_id` into a span so any nested log lines are correlated, and on drop emits a
/// single structured completion event carrying the request id, op, resolved HDFS path,
/// latency, and bytes served. Uses `tracing`'s native structured fields (not a hand-rolled
/// correlation map). A start event is also emitted so the request id appears on more than
/// one line, demonstrating cross-line correlation.
///
/// The `span` is wrapped around each upstream `hdfs-native` call via `.instrument(..)` (see
/// the call sites). `Instrument` enters the span for the duration of each poll, so any
/// `tracing` diagnostics emitted by `hdfs-native` while we are handling the request — e.g.
/// `warn!("Error occurred while reading from DataNode: ...")` or `warn!("IO error on RPC
/// call, retrying: ...")` — are recorded as children of this span and therefore
/// automatically tagged with our `request_id`. That is how upstream HDFS issues become
/// visible in the logs, correlated to the request that triggered them, without any per-call
/// wiring. We deliberately do NOT hold an `EnteredSpan` guard (it is `!Send` and would break
/// the `Send` bound on the `S3` async trait methods); `Instrument` is the Send-safe way to
/// keep a span active across an `.await`.
struct RequestLog {
    op: &'static str,
    request_id: String,
    hdfs_path: Option<String>,
    bytes_served: u64,
    ok: bool,
    start: std::time::Instant,
    span: tracing::Span,
}

impl RequestLog {
    fn new(op: &'static str) -> Self {
        let request_id = new_request_id();
        let span = tracing::info_span!(
            "s3_request",
            op,
            request_id = %request_id,
            hdfs_path = tracing::field::Empty,
            latency_ms = tracing::field::Empty,
            bytes_served = tracing::field::Empty,
        );
        // Start event: the request id appears here and again on the completion event,
        // so a single request's log lines share a consistent id (correlation).
        tracing::info!(parent: &span, request_id = %request_id, op = %op, "request started");
        RequestLog {
            op,
            request_id,
            hdfs_path: None,
            bytes_served: 0,
            ok: false,
            start: std::time::Instant::now(),
            span,
        }
    }

    /// Record the resolved HDFS path for this request (once known).
    fn set_path(&mut self, path: impl Into<String>) {
        self.hdfs_path = Some(path.into());
    }

    /// Record the number of bytes served (for GET / object responses).
    fn set_bytes(&mut self, n: u64) {
        self.bytes_served = n;
    }

    /// The request id minted for this request (also attached to the HTTP response header).
    fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Mark the request as having produced a successful response (vs an error).
    fn mark_ok(&mut self) {
        self.ok = true;
    }

    /// Consume the log (emitting the completion line on drop) and return `result`
    /// unchanged. Used by error-returning methods so the request is still logged.
    fn finish<T>(self, result: S3Result<T>) -> S3Result<T> {
        result
    }
}

impl Drop for RequestLog {
    fn drop(&mut self) {
        let latency_ms = self.start.elapsed().as_millis() as u64;
        let path = self.hdfs_path.clone().unwrap_or_default();
        // Single completion log line with all structured fields.
        tracing::info!(
            parent: &self.span,
            request_id = %self.request_id,
            op = %self.op,
            hdfs_path = %path,
            latency_ms = latency_ms,
            bytes_served = self.bytes_served,
            ok = self.ok,
            "request completed"
        );
    }
}

/// Attach the request id (minted in `RequestLog`) to an `S3Response` as `x-amz-request-id`.
///
/// s3s serializes this via `resp.headers.extend(s3_resp.headers)`, which *appends* our
/// header without clobbering any headers s3s already set — so this is safe to call on
/// every response. This is the supported public extension point (the `S3Response.headers`
/// field), not a hand-rolled HTTP layer.
fn with_request_id<T>(mut resp: S3Response<T>, log: &mut RequestLog) -> S3Response<T> {
    if let Ok(val) = http::HeaderValue::from_str(log.request_id()) {
        resp.headers.insert(X_AMZ_REQUEST_ID, val);
    }
    log.mark_ok();
    resp
}

mod error;
pub use error::map_hdfs_error;
pub mod backpressure;
pub mod server;
mod write_policy;

/// Enforce an exact content length on a byte stream. If the upstream stream yields more
/// bytes than `content_length`, the excess is truncated; if it yields fewer (e.g. a
/// mid-stream DataNode failure), the stream ends early and the consumer observes a
/// short read rather than a silently-truncated "successful" response.
fn bytes_stream<S, E>(
    stream: S,
    content_length: usize,
) -> impl Stream<Item = Result<Bytes, E>> + Send + 'static
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Send + 'static,
{
    futures::stream::unfold(
        (stream, content_length),
        |(mut stream, mut remaining)| async move {
            if remaining == 0 {
                return None;
            }
            match stream.next().await {
                Some(Ok(mut bytes)) => {
                    if bytes.len() > remaining {
                        bytes.truncate(remaining);
                    }
                    remaining -= bytes.len();
                    Some((Ok(bytes), (stream, remaining)))
                }
                Some(Err(e)) => Some((Err(e), (stream, remaining))),
                None => None,
            }
        },
    )
}

/// The gateway: holds a shared HDFS client and the path mapper.
#[derive(Clone)]
pub struct HdfsGateway {
    client: Arc<Client>,
    mapper: PathMapper,
    config: Arc<Config>,
}

impl HdfsGateway {
    pub fn new(client: Client, config: Config) -> Self {
        let mapper = PathMapper::new(&config);
        HdfsGateway {
            client: Arc::new(client),
            mapper,
            config: Arc::new(config),
        }
    }

    /// Build a `HdfsGateway` from a validated `Config`, constructing the shared HDFS
    /// client exactly as the binary does. Shared by `main.rs` and the Python bindings so
    /// the client-construction logic lives in one place.
    pub fn from_config(config: &Config) -> Result<Self, String> {
        let mut builder = ClientBuilder::new().with_url(&config.namenode_uri);

        if !config.hdfs_options.is_empty() {
            builder = builder.with_config(config.hdfs_options.clone());
        }
        if let Some(dir) = &config.hdfs_config_dir {
            builder = builder.with_config_dir(dir.clone());
        }
        if let Some(user) = &config.hdfs_user {
            builder = builder.with_user(user.clone());
        }

        let client = builder
            .build()
            .map_err(|e| format!("failed to build HDFS client: {e}"))?;
        Ok(HdfsGateway::new(client, config.clone()))
    }

    /// The address the gateway will bind to (from config). Exposed so callers (e.g. the
    /// Python bindings) can report the bound address after `serve`.
    pub fn listen_addr(&self) -> &str {
        &self.config.listen_addr
    }
}

#[async_trait::async_trait]
impl S3 for HdfsGateway {
    #[tracing::instrument(skip(self))]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let mut log = RequestLog::new("HeadBucket");
        let bucket = req.input.bucket.as_str();
        if bucket != self.mapper.bucket() {
            return Err(s3_error!(NoSuchBucket));
        }
        Ok(with_request_id(
            S3Response::new(HeadBucketOutput::default()),
            &mut log,
        ))
    }

    #[tracing::instrument(skip(self))]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let mut log = RequestLog::new("HeadObject");
        let input = req.input;
        if input.bucket.as_str() != self.mapper.bucket() {
            return Err(s3_error!(NoSuchBucket));
        }

        let key = input.key.as_str();
        let hdfs_path = self
            .mapper
            .key_to_hdfs_path(key)
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        log.set_path(&hdfs_path);

        let status = self
            .client
            .get_file_info(&hdfs_path)
            .instrument(log.span.clone())
            .await
            .map_err(|e| map_hdfs_error(e, self.config.expose_upstream_errors))?;

        // Directories are never surfaced as objects.
        if status.isdir {
            return Err(s3_error!(NoSuchKey));
        }

        let meta = ObjectMetadata::from_hdfs(
            status.path,
            status.length as u64,
            status.isdir,
            status.modification_time,
            None, // hdfs-native does not expose getFileChecksum (not supported upstream)
        );

        let last_modified = millis_to_timestamp(status.modification_time);

        let output = HeadObjectOutput {
            content_length: Some(status.length as i64),
            content_type: Some(meta.content_type()),
            last_modified: Some(last_modified),
            e_tag: Some(ETag::Strong(meta.etag())),
            ..Default::default()
        };
        Ok(with_request_id(S3Response::new(output), &mut log))
    }

    #[tracing::instrument(skip(self))]
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let mut log = RequestLog::new("ListBuckets");
        let bucket = Bucket {
            name: Some(self.mapper.bucket().to_string()),
            creation_date: Some(Timestamp::from(SystemTime::now())),
            bucket_region: None,
        };
        let output = ListBucketsOutput {
            buckets: Some(vec![bucket]),
            owner: None,
            ..Default::default()
        };
        Ok(with_request_id(S3Response::new(output), &mut log))
    }

    #[tracing::instrument(skip(self))]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let mut log = RequestLog::new("ListObjectsV2");
        let input = req.input;
        if input.bucket.as_str() != self.mapper.bucket() {
            return Err(s3_error!(NoSuchBucket));
        }

        let prefix = input.prefix.as_deref().unwrap_or("").to_string();
        let delimiter = input.delimiter.as_deref();
        let max_keys = input.max_keys.unwrap_or(1000) as usize;

        // List everything under the bucket root (recursive), then translate.
        let root = self.mapper.root().to_string();
        let statuses = self
            .client
            .list_status(&root, true)
            .instrument(log.span.clone())
            .await
            .map_err(|e| map_hdfs_error(e, self.config.expose_upstream_errors))?;

        let mut entries: Vec<ListEntry> = Vec::new();
        for s in statuses {
            if s.isdir {
                continue; // directories are not objects
            }
            let Some(key) = self.mapper.hdfs_path_to_key(&s.path) else {
                continue;
            };
            if key.is_empty() {
                continue;
            }
            entries.push(ListEntry {
                key,
                size: s.length as u64,
                modification_time: s.modification_time,
            });
        }

        // Strict lexicographic order (S3 guarantee).
        entries.sort_by(|a, b| a.key.as_bytes().cmp(b.key.as_bytes()));

        let (mut contents, common) = list_to_contents(&entries, &prefix, delimiter);
        let common_vec = common.into_vec();

        // Pagination: resume after the decoded continuation token.
        let start_after = input
            .continuation_token
            .as_deref()
            .and_then(decode_token)
            .or_else(|| input.start_after.clone());

        if let Some(marker) = &start_after {
            contents.retain(|e| &e.key > marker);
        }

        // Interleave contents and common prefixes in key order for pagination.
        let mut all_keys: Vec<String> = contents.iter().map(|e| e.key.clone()).collect();
        all_keys.extend(common_vec.iter().cloned());
        all_keys.sort();

        let total: Vec<String> = all_keys;
        let take = max_keys.min(total.len());
        let page: Vec<String> = total[..take].to_vec();
        let is_truncated = take < total.len();

        let page_set: std::collections::HashSet<&String> = page.iter().collect();

        let result_contents: Vec<Object> = contents
            .iter()
            .filter(|e| page_set.contains(&e.key))
            .map(|e| Object {
                key: Some(e.key.clone()),
                size: Some(e.size as i64),
                last_modified: Some(millis_to_timestamp(e.modification_time)),
                e_tag: Some(ETag::Strong(crate::core::fallback_etag(
                    e.size,
                    e.modification_time,
                ))), // hdfs-native does not expose getFileChecksum (not supported upstream)
                ..Default::default()
            })
            .collect();

        let result_prefixes: Vec<CommonPrefix> = common_vec
            .iter()
            .filter(|p| page_set.contains(p))
            .map(|p| CommonPrefix {
                prefix: Some(p.clone()),
            })
            .collect();

        let next_token = if is_truncated {
            page.last().map(|k| encode_token(k))
        } else {
            None
        };

        let output = ListObjectsV2Output {
            name: Some(self.mapper.bucket().to_string()),
            prefix: input.prefix,
            delimiter: input.delimiter,
            max_keys: Some(max_keys as i32),
            is_truncated: Some(is_truncated),
            contents: if result_contents.is_empty() {
                None
            } else {
                Some(result_contents)
            },
            common_prefixes: if result_prefixes.is_empty() {
                None
            } else {
                Some(result_prefixes)
            },
            continuation_token: input.continuation_token,
            next_continuation_token: next_token,
            key_count: Some((page.len()) as i32),
            ..Default::default()
        };
        Ok(with_request_id(S3Response::new(output), &mut log))
    }

    #[tracing::instrument(skip(self))]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let mut log = RequestLog::new("GetObject");
        let input = req.input;
        if input.bucket.as_str() != self.mapper.bucket() {
            return Err(s3_error!(NoSuchBucket));
        }

        let key = input.key.as_str();
        let hdfs_path = self
            .mapper
            .key_to_hdfs_path(key)
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        log.set_path(&hdfs_path);

        let status = self
            .client
            .get_file_info(&hdfs_path)
            .instrument(log.span.clone())
            .await
            .map_err(|e| map_hdfs_error(e, self.config.expose_upstream_errors))?;

        // Directories are never surfaced as objects.
        if status.isdir {
            return Err(s3_error!(NoSuchKey));
        }

        let file_len = status.length as u64;
        let last_modified = millis_to_timestamp(status.modification_time);

        let meta = ObjectMetadata::from_hdfs(
            status.path,
            file_len,
            status.isdir,
            status.modification_time,
            None, // hdfs-native does not expose getFileChecksum (not supported upstream)
        );
        let etag = ETag::Strong(meta.etag());
        let content_type = meta.content_type();

        // --- Conditional headers (RFC 7232) -------------------------------------
        // If-Match / If-Unmodified-Since → 412 PreconditionFailed when not satisfied.
        // If-None-Match / If-Modified-Since → 304 NotModified when satisfied.
        if let Some(cond) = &input.if_match {
            let matched = match cond {
                ETagCondition::Any => true,
                ETagCondition::ETag(other) => etag.strong_cmp(other),
            };
            if !matched {
                return Err(s3_error!(PreconditionFailed));
            }
        }
        if let Some(since) = &input.if_unmodified_since {
            if last_modified > *since {
                return Err(s3_error!(PreconditionFailed));
            }
        }
        if let Some(cond) = &input.if_none_match {
            let not_modified = match cond {
                ETagCondition::Any => true, // resource exists → not modified
                ETagCondition::ETag(other) => etag.weak_cmp(other),
            };
            if not_modified {
                return Err(s3_error!(NotModified));
            }
        }
        if let Some(since) = &input.if_modified_since {
            if last_modified <= *since {
                return Err(s3_error!(NotModified));
            }
        }

        // --- Range resolution ---------------------------------------------------
        let (start, end_exclusive, content_length, content_range) = match input.range {
            None => (0u64, file_len, file_len, None),
            Some(range) => {
                let resolved = match range {
                    Range::Int { first, last } => match last {
                        Some(last) => crate::core::ByteRange::Inclusive { first, last },
                        None => crate::core::ByteRange::From { first },
                    },
                    Range::Suffix { length } => crate::core::ByteRange::Suffix { length },
                };
                let (s, e) = crate::core::resolve_range(file_len, resolved)
                    .ok_or_else(|| s3_error!(InvalidRange))?;
                let len = e - s;
                let cr = fmt_content_range(s, e - 1, file_len);
                (s, e, len, Some(cr))
            }
        };

        // --- Streaming body (never buffer the whole object) ----------------
        let mut reader = self
            .client
            .read(&hdfs_path)
            .instrument(log.span.clone())
            .await
            .map_err(|e| map_hdfs_error(e, self.config.expose_upstream_errors))?;
        reader.set_position(start as usize);
        let remaining = (end_exclusive - start) as usize;

        let stream = ReaderStream::with_capacity(reader, 64 * 1024);
        // `bytes_stream` enforces the exact content length so a truncated read surfaces as
        // an error rather than a silently-short "successful" response.
        let body = bytes_stream(stream, remaining);

        let output = GetObjectOutput {
            body: Some(StreamingBlob::wrap(body)),
            content_length: Some(content_length as i64),
            content_range,
            last_modified: Some(last_modified),
            e_tag: Some(etag),
            content_type: Some(content_type),
            ..Default::default()
        };
        log.set_bytes(content_length);
        Ok(with_request_id(S3Response::new(output), &mut log))
    }

    // -----------------------------------------------------------------------------------------
    // Write-shaped operations → uniform AccessDenied (read-only gateway).
    // -----------------------------------------------------------------------------------------

    #[tracing::instrument(skip(self))]
    async fn put_object(
        &self,
        _req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let log = RequestLog::new("PutObject");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object(
        &self,
        _req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let log = RequestLog::new("DeleteObject");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn delete_objects(
        &self,
        _req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let log = RequestLog::new("DeleteObjects");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let log = RequestLog::new("CreateBucket");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn delete_bucket(
        &self,
        _req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let log = RequestLog::new("DeleteBucket");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn copy_object(
        &self,
        _req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let log = RequestLog::new("CopyObject");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn create_multipart_upload(
        &self,
        _req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let log = RequestLog::new("CreateMultipartUpload");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn upload_part(
        &self,
        _req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let log = RequestLog::new("UploadPart");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn complete_multipart_upload(
        &self,
        _req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let log = RequestLog::new("CompleteMultipartUpload");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn abort_multipart_upload(
        &self,
        _req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let log = RequestLog::new("AbortMultipartUpload");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn list_multipart_uploads(
        &self,
        _req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let log = RequestLog::new("ListMultipartUploads");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn list_parts(
        &self,
        _req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let log = RequestLog::new("ListParts");
        log.finish(Err(write_policy::write_denied()))
    }

    #[tracing::instrument(skip(self))]
    async fn restore_object(
        &self,
        _req: S3Request<RestoreObjectInput>,
    ) -> S3Result<S3Response<RestoreObjectOutput>> {
        let log = RequestLog::new("RestoreObject");
        log.finish(Err(write_policy::write_denied()))
    }

    // -----------------------------------------------------------------------------------------
    // Bucket-configuration probes → "not configured" (real S3 answers these).
    // -----------------------------------------------------------------------------------------

    #[tracing::instrument(skip(self))]
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let mut log = RequestLog::new("GetBucketVersioning");
        let resp = write_policy::bucket_versioning_not_configured(req)?;
        Ok(with_request_id(resp, &mut log))
    }

    #[tracing::instrument(skip(self))]
    async fn get_bucket_tagging(
        &self,
        req: S3Request<GetBucketTaggingInput>,
    ) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        let mut log = RequestLog::new("GetBucketTagging");
        let resp = write_policy::bucket_tagging_not_configured(req)?;
        Ok(with_request_id(resp, &mut log))
    }

    #[tracing::instrument(skip(self))]
    async fn get_bucket_acl(
        &self,
        req: S3Request<GetBucketAclInput>,
    ) -> S3Result<S3Response<GetBucketAclOutput>> {
        let mut log = RequestLog::new("GetBucketAcl");
        let resp = write_policy::bucket_acl_not_configured(req)?;
        Ok(with_request_id(resp, &mut log))
    }

    #[tracing::instrument(skip(self))]
    async fn get_bucket_cors(
        &self,
        req: S3Request<GetBucketCorsInput>,
    ) -> S3Result<S3Response<GetBucketCorsOutput>> {
        let mut log = RequestLog::new("GetBucketCors");
        let resp = write_policy::bucket_cors_not_configured(req)?;
        Ok(with_request_id(resp, &mut log))
    }
}

/// <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range>
fn fmt_content_range(start: u64, end_inclusive: u64, size: u64) -> String {
    format!("bytes {start}-{end_inclusive}/{size}")
}

/// Convert HDFS modification_time (millis since epoch) to an `s3s` `Timestamp`.
fn millis_to_timestamp(millis: u64) -> Timestamp {
    let secs = millis / 1000;
    let nanos = ((millis % 1000) * 1_000_000) as u32;
    let st = UNIX_EPOCH + std::time::Duration::new(secs, nanos);
    Timestamp::from(st)
}
