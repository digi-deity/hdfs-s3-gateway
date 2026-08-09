//! The `impl S3 for HdfsGateway` — the thin mechanical layer connecting the `core`
//! module to `s3s`. Each method parses the trait input, calls `core`
//! / the HDFS client, and maps the result into an `s3s` output or error type.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::core::{decode_token, encode_token, paginate, ListEntry, ObjectMetadata, PathMapper};
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use hdfs_native::Client;
use hdfs_native::ClientBuilder;
use s3s::dto::*;
use s3s::header::X_AMZ_REQUEST_ID;
use s3s::{s3_error, S3Error, S3Request, S3Response, S3Result, S3};
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

    /// Attach this request's id to an `S3Error` so the HTTP error response carries
    /// the same `x-amz-request-id` (header + XML `<RequestId>`) as success responses
    /// and the server logs — clients/operators correlate on it. Used on every error
    /// path so no error response is missing the correlation id.
    fn attach(&self, err: S3Error) -> S3Error {
        attach_error_request_id(self.request_id(), err)
    }

    /// Consume the log (emitting the completion line on drop) and return `result`
    /// unchanged. Used by error-returning methods so the request is still logged;
    /// the request id is attached to any error result.
    fn finish<T>(self, result: S3Result<T>) -> S3Result<T> {
        result.map_err(|e| self.attach(e))
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

/// Attach a request id to an `S3Error`: both the XML `<RequestId>` element and the
/// `x-amz-request-id` HTTP header, so error responses correlate with the server logs
/// exactly like success responses.
///
/// Note: s3s's `serialize_error` applies `S3Error::headers` by *replacing* the response
/// header map, so we must re-include the XML `Content-Type` header that it sets before
/// the replacement — otherwise the error body would lose its content type.
fn attach_error_request_id(request_id: &str, mut err: S3Error) -> S3Error {
    err.set_request_id(request_id.to_string());
    let mut headers = http::HeaderMap::new();
    if let Ok(val) = http::HeaderValue::from_str(request_id) {
        headers.insert(X_AMZ_REQUEST_ID, val);
    }
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/xml"),
    );
    err.set_headers(headers);
    err
}

mod auth;
pub use auth::SharedSecretAuth;
mod error;
pub use error::map_hdfs_error;
pub mod backpressure;
pub mod server;
mod sorted_listing;
use self::sorted_listing::{DirLister, HdfsDirLister, SortedListing};
mod write_policy;

/// Error produced while streaming a GET response body.
#[derive(Debug, thiserror::Error)]
enum GetBodyError {
    /// The upstream HDFS reader failed mid-stream.
    #[error("HDFS read failed: {0}")]
    Upstream(#[from] std::io::Error),
    /// The upstream stream ended before the declared content length was reached
    /// (e.g. the file shrank between stat and read, or a DataNode failure that
    /// surfaced as EOF). An explicit error beats a silently-short "successful"
    /// response.
    #[error("HDFS stream ended after {got} of {expected} declared bytes")]
    ShortRead { expected: usize, got: usize },
}

/// Enforce an exact content length on a byte stream.
///
/// The HDFS reader streams from `set_position` to EOF in large chunks (the 64 KiB
/// `ReaderStream` buffer), so a ranged GET's final chunk routinely extends past the
/// requested window — the excess is truncated here. That is the normal contract, NOT
/// a bug: the window's end is rarely chunk-aligned. What this wrapper detects is the
/// opposite failure: the stream ending BEFORE `content_length` bytes were delivered
/// (a mid-stream DataNode failure surfacing as EOF, or the file shrinking between
/// `get_file_info` and `read`), which fails the stream explicitly rather than
/// producing a silently-truncated "successful" response.
fn bytes_stream(
    stream: impl Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
    content_length: usize,
) -> impl Stream<Item = Result<Bytes, GetBodyError>> + Send + 'static {
    futures::stream::unfold(
        (stream, content_length, content_length),
        |(mut stream, limit, mut remaining)| async move {
            if remaining == 0 {
                return None;
            }
            match stream.next().await {
                Some(Ok(mut bytes)) => {
                    if bytes.len() > remaining {
                        bytes.truncate(remaining);
                    }
                    remaining -= bytes.len();
                    Some((Ok(bytes), (stream, limit, remaining)))
                }
                Some(Err(e)) => Some((Err(GetBodyError::Upstream(e)), (stream, limit, remaining))),
                None => {
                    if remaining > 0 {
                        let got = limit - remaining;
                        Some((
                            Err(GetBodyError::ShortRead {
                                expected: limit,
                                got,
                            }),
                            (stream, limit, 0),
                        ))
                    } else {
                        None
                    }
                }
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
            return Err(log.attach(s3_error!(NoSuchBucket)));
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
            return Err(log.attach(s3_error!(NoSuchBucket)));
        }

        let key = input.key.as_str();
        let hdfs_path = self
            .mapper
            .key_to_hdfs_path(key)
            .ok_or_else(|| log.attach(s3_error!(NoSuchKey)))?;
        log.set_path(&hdfs_path);

        let status = self
            .client
            .get_file_info(&hdfs_path)
            .instrument(log.span.clone())
            .await
            .map_err(|e| log.attach(map_hdfs_error(e, self.config.expose_upstream_errors)))?;

        // Directories are never surfaced as objects.
        if status.isdir {
            return Err(log.attach(s3_error!(NoSuchKey)));
        }

        let meta = ObjectMetadata::from_hdfs(
            status.path,
            status.length as u64,
            status.isdir,
            status.modification_time,
            None, // hdfs-native does not expose getFileChecksum (not supported upstream)
        );
        let etag = ETag::Strong(meta.etag());
        let last_modified = millis_to_timestamp(status.modification_time);

        // --- Conditional headers (RFC 7232) — identical semantics to GetObject. ---
        // HEAD is subject to the same preconditions; clients use it for cheap
        // freshness checks (e.g. cache revalidation) and expect 304/412 here too.
        // If-Match / If-Unmodified-Since → 412 PreconditionFailed when not satisfied.
        // If-None-Match / If-Modified-Since → 304 NotModified when satisfied.
        if let Some(cond) = &input.if_match {
            let matched = match cond {
                ETagCondition::Any => true,
                ETagCondition::ETag(other) => etag.strong_cmp(other),
            };
            if !matched {
                return Err(log.attach(s3_error!(PreconditionFailed)));
            }
        }
        if let Some(since) = &input.if_unmodified_since {
            if last_modified > *since {
                return Err(log.attach(s3_error!(PreconditionFailed)));
            }
        }
        if let Some(cond) = &input.if_none_match {
            let not_modified = match cond {
                ETagCondition::Any => true, // resource exists → not modified
                ETagCondition::ETag(other) => etag.weak_cmp(other),
            };
            if not_modified {
                return Err(log.attach(s3_error!(NotModified)));
            }
        }
        if let Some(since) = &input.if_modified_since {
            if last_modified <= *since {
                return Err(log.attach(s3_error!(NotModified)));
            }
        }

        let output = HeadObjectOutput {
            // RFC 9110: advertise that byte ranges are supported, so clients that
            // probe (e.g. pyarrow/DuckDB) decide to issue ranged GETs.
            accept_ranges: Some("bytes".to_string()),
            content_length: Some(status.length as i64),
            content_type: Some(meta.content_type()),
            last_modified: Some(last_modified),
            e_tag: Some(etag),
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
            return Err(log.attach(s3_error!(NoSuchBucket)));
        }

        let prefix = input.prefix.as_deref().unwrap_or("").to_string();
        // S3 semantics: an empty delimiter means "no grouping" — treat it as None so
        // keys are returned as Contents rather than collapsing every key into the
        // prefix itself (which `rest.find("")` would otherwise do at index 0).
        let delimiter = input.delimiter.as_deref().filter(|d| !d.is_empty());
        let max_keys = input.max_keys.unwrap_or(1000) as usize;

        // Pagination: resume after the decoded continuation token. A token that does
        // not decode is a client error (`InvalidToken`, matching AWS S3) — silently
        // falling back to the first page would make a client with a corrupted or
        // truncated token poll page 1 forever. Decoded up front so both listing
        // strategies below can skip already-served keys without materializing them.
        let start_after = match input.continuation_token.as_deref() {
            Some(token) => {
                Some(decode_token(token).ok_or_else(|| log.attach(s3_error!(InvalidToken)))?)
            }
            None => input.start_after.clone(),
        };

        // Contents and CommonPrefixes of the requested page, in key order, plus the raw
        // last key of the page (the next continuation token) when it is truncated.
        let (page_entries, page_prefixes, is_truncated, next_token) = if delimiter.is_some() {
            // --- Delimiter path: ONE non-recursive listing of the prefix directory. ---
            // With a delimiter, a listing's output is fully determined by the direct
            // children of the directory part of the prefix: matching files become
            // Contents, matching subdirectories collapse into CommonPrefixes. A
            // recursive walk (one `getListing` RPC per directory of the subtree) would
            // be pure waste, so list exactly the one directory (`recursive = false`).
            //
            // Semantic note: an EMPTY HDFS directory is a real directory, so it now
            // appears as a CommonPrefix. A recursive walk used to hide empty
            // directories (only files became entries), but detecting emptiness would
            // cost one extra RPC per directory — the very amplification this
            // single-listing path exists to avoid. The namespace is surfaced as-is.
            let statuses = match self.mapper.list_start(&prefix) {
                // The prefix cannot match any key under the root (e.g. it escapes it):
                // S3 semantics — an empty listing, not an error.
                None => Vec::new(),
                Some(hdfs_start) => match self
                    .client
                    .list_status(&hdfs_start, false)
                    .instrument(log.span.clone())
                    .await
                {
                    Ok(statuses) => statuses,
                    // The prefix's directory does not exist (or the prefix points at a
                    // file, which the NameNode `getListing` reports the same way):
                    // nothing can match — an empty listing, not an error. Clients probe
                    // non-existent prefixes (e.g. `table/partition=x/` before it exists)
                    // constantly, and real S3 answers those with an empty page.
                    Err(hdfs_native::HdfsError::FileNotFound(_)) => Vec::new(),
                    Err(e) => {
                        return Err(
                            log.attach(map_hdfs_error(e, self.config.expose_upstream_errors))
                        )
                    }
                },
            };

            let mut files: Vec<ListEntry> = Vec::new();
            let mut dirs: Vec<String> = Vec::new();
            for s in statuses {
                let Some(key) = self.mapper.hdfs_path_to_key(&s.path) else {
                    continue;
                };
                if key.is_empty() {
                    continue;
                }
                if s.isdir {
                    dirs.push(key);
                } else {
                    files.push(ListEntry {
                        key,
                        size: s.length as u64,
                        modification_time: s.modification_time,
                    });
                }
            }

            let page = paginate(
                &files,
                &dirs,
                &prefix,
                delimiter,
                start_after.as_deref(),
                max_keys,
            );
            (
                page.contents,
                page.common_prefixes,
                page.is_truncated,
                page.next_token,
            )
        } else {
            // --- No-delimiter path (flat listing): lazy sorted k-way merge ---
            // The flat listing used to walk the whole bounded subtree per
            // request (one `getListing` RPC per directory) and materialize it
            // before paging — re-paying the full walk on every continuation
            // page. Instead, per-directory listings are merged lazily in key
            // order (see `sorted_listing`): only the directories that sort
            // before the page boundary are opened, memory is bounded to the
            // merge frontier, and page 1 of a huge subtree costs only what
            // `max_keys` actually needs.
            let (page_entries, is_truncated) = match self.mapper.list_start(&prefix) {
                // The prefix cannot match any key under the root (e.g. it escapes
                // it): S3 semantics — an empty listing, not an error.
                None => (Vec::new(), false),
                Some(hdfs_start) => {
                    let lister: Arc<dyn DirLister> =
                        Arc::new(HdfsDirLister::new(self.client.clone(), log.span.clone()));
                    let mut listing = SortedListing::new(
                        lister,
                        hdfs_start,
                        prefix.clone(),
                        start_after.clone(),
                        self.mapper.clone(),
                    );
                    listing.collect_page(max_keys).await.map_err(|e| {
                        log.attach(map_hdfs_error(e, self.config.expose_upstream_errors))
                    })?
                }
            };
            let next_token = if is_truncated {
                page_entries.last().map(|e| e.key.clone())
            } else {
                None
            };
            (page_entries, Vec::new(), is_truncated, next_token)
        };

        let result_contents: Vec<Object> = page_entries
            .iter()
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

        let result_prefixes: Vec<CommonPrefix> = page_prefixes
            .iter()
            .map(|p| CommonPrefix {
                prefix: Some(p.clone()),
            })
            .collect();

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
            next_continuation_token: next_token.as_ref().map(|k| encode_token(k)),
            key_count: Some((page_entries.len() + page_prefixes.len()) as i32),
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
            return Err(log.attach(s3_error!(NoSuchBucket)));
        }

        let key = input.key.as_str();
        let hdfs_path = self
            .mapper
            .key_to_hdfs_path(key)
            .ok_or_else(|| log.attach(s3_error!(NoSuchKey)))?;
        log.set_path(&hdfs_path);

        let status = self
            .client
            .get_file_info(&hdfs_path)
            .instrument(log.span.clone())
            .await
            .map_err(|e| log.attach(map_hdfs_error(e, self.config.expose_upstream_errors)))?;

        // Directories are never surfaced as objects.
        if status.isdir {
            return Err(log.attach(s3_error!(NoSuchKey)));
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
                return Err(log.attach(s3_error!(PreconditionFailed)));
            }
        }
        if let Some(since) = &input.if_unmodified_since {
            if last_modified > *since {
                return Err(log.attach(s3_error!(PreconditionFailed)));
            }
        }
        if let Some(cond) = &input.if_none_match {
            let not_modified = match cond {
                ETagCondition::Any => true, // resource exists → not modified
                ETagCondition::ETag(other) => etag.weak_cmp(other),
            };
            if not_modified {
                return Err(log.attach(s3_error!(NotModified)));
            }
        }
        if let Some(since) = &input.if_modified_since {
            if last_modified <= *since {
                return Err(log.attach(s3_error!(NotModified)));
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
                    .ok_or_else(|| log.attach(s3_error!(InvalidRange)))?;
                let len = e - s;
                let cr = fmt_content_range(s, e - 1, file_len);
                (s, e, len, Some(cr))
            }
        };

        // hdfs-native addresses readers with `usize`, so on 32-bit targets a window
        // beyond 4 GiB cannot be addressed — silently truncating the offset would
        // serve the wrong bytes. Reject it explicitly (416) instead; on 64-bit
        // targets this never triggers.
        let Ok(window_start) = usize::try_from(start) else {
            return Err(log.attach(s3_error!(InvalidRange)));
        };
        let Ok(window_end) = usize::try_from(end_exclusive) else {
            return Err(log.attach(s3_error!(InvalidRange)));
        };

        // --- Streaming body (never buffer the whole object) -------------
        let mut reader = self
            .client
            .read(&hdfs_path)
            .instrument(log.span.clone())
            .await
            .map_err(|e| log.attach(map_hdfs_error(e, self.config.expose_upstream_errors)))?;
        reader.set_position(window_start);
        let remaining = window_end - window_start;

        let stream = ReaderStream::with_capacity(reader, 64 * 1024);
        // `bytes_stream` enforces the exact content length: the final chunk of a
        // ranged read is truncated to the window (the HDFS reader streams in 64 KiB
        // chunks past the range end — normal), and an upstream stream that ends
        // early fails with a short-read error instead of a silently-short
        // "successful" response.
        let body = bytes_stream(stream, remaining);

        let output = GetObjectOutput {
            // RFC 9110: advertise byte-range support on GET responses too.
            accept_ranges: Some("bytes".to_string()),
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
