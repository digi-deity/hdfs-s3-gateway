//! The `impl S3 for HdfsGateway` — the thin mechanical layer connecting the `core`
//! module to `s3s`. Each method parses the trait input, calls `core`
//! / the HDFS client, and maps the result into an `s3s` output or error type.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::core::{decode_token, encode_token, paginate, DirListing, ObjectMetadata, PathMapper};
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use hdfs_native::Client;
use hdfs_native::ClientBuilder;
use s3s::dto::*;
use s3s::header::X_AMZ_REQUEST_ID;
use s3s::{s3_error, S3Error, S3Request, S3Response, S3Result, S3};
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
mod cache;
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
/// The HDFS range stream already stops at the end of the requested window, so the
/// truncation below is defensive; what this wrapper really enforces is the exact
/// content length: a stream that ends BEFORE `content_length` bytes were delivered
/// (a mid-stream DataNode failure surfacing as EOF, or the file shrinking between
/// `get_file_info` and `read`) fails explicitly rather than producing a silently
/// truncated "successful" response.
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

/// Read a GET window in bounded chunks instead of handing the whole window to
/// `FileReader::read_range_stream` at once.
///
/// hdfs-native constructs one block stream (and its packet channel) for EVERY block
/// intersecting the requested range *eagerly*, before any data is returned. Asking it
/// for a whole multi-TB object would therefore allocate a stream per block for every
/// concurrent GET, which can be hundreds of MiB across connections. Reading in bounded
/// windows keeps per-request memory proportional to the window size (a few block
/// streams), while still amortizing the DataNode `ReadBlock` setup over megabytes —
/// 64 KiB reads cost one DataNode op each (~8 MB/s), this costs one per window.
const READ_WINDOW_BYTES: usize = 8 * 1024 * 1024;

/// State for [`range_stream`]: the next window to request plus the stream currently
/// being drained. `next_offset` always points at the first byte after the open window.
struct ReadWindows {
    reader: Arc<hdfs_native::file::FileReader>,
    end: usize,
    next_offset: usize,
    window_remaining: usize,
    current: Option<futures::stream::BoxStream<'static, Result<Bytes, hdfs_native::HdfsError>>>,
}

impl ReadWindows {
    fn new(reader: Arc<hdfs_native::file::FileReader>, start: usize, len: usize) -> Self {
        ReadWindows {
            reader,
            end: start + len,
            next_offset: start,
            window_remaining: 0,
            current: None,
        }
    }

    /// Open the next bounded window. The caller guarantees `next_offset < end`, and the
    /// whole GET window was already checked against `reader.file_length()`, so the
    /// upstream range is in bounds.
    fn open_window(&mut self) {
        let len = (self.end - self.next_offset).min(READ_WINDOW_BYTES);
        let reader = Arc::clone(&self.reader);
        self.current = Some(reader.read_range_stream(self.next_offset, len).boxed());
        self.next_offset += len;
        self.window_remaining = len;
    }
}

/// Stream exactly `len` bytes from `start`, reading at most [`READ_WINDOW_BYTES`] from
/// hdfs-native at a time. A window that ends early (without an upstream error) is
/// reported as `UnexpectedEof` rather than silently skipping to the next window.
fn range_stream(
    reader: Arc<hdfs_native::file::FileReader>,
    start: usize,
    len: usize,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    futures::stream::unfold(
        ReadWindows::new(reader, start, len),
        |mut windows| async move {
            loop {
                if windows.current.is_none() {
                    if windows.next_offset >= windows.end {
                        return None;
                    }
                    windows.open_window();
                }

                let next = {
                    let stream = windows.current.as_mut().expect("a window was just opened");
                    stream.next().await
                };

                match next {
                    Some(Ok(mut bytes)) => {
                        // Defensive: hdfs-native should never return more than requested.
                        if bytes.len() > windows.window_remaining {
                            bytes.truncate(windows.window_remaining);
                        }
                        windows.window_remaining -= bytes.len();
                        return Some((Ok(bytes), windows));
                    }
                    Some(Err(e)) => {
                        // Stop after the first error; the next poll returns None. The S3
                        // layer evicts the cached block map when it sees this error.
                        windows.current = None;
                        windows.next_offset = windows.end;
                        windows.window_remaining = 0;
                        return Some((Err(std::io::Error::other(e)), windows));
                    }
                    None => {
                        if windows.window_remaining > 0 {
                            let missing = windows.window_remaining;
                            windows.current = None;
                            windows.next_offset = windows.end;
                            windows.window_remaining = 0;
                            let error = std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "HDFS range stream ended {missing} bytes before the end of its read window"
                                ),
                            );
                            return Some((Err(error), windows));
                        }
                        // Window complete; loop to open the next one (or finish).
                        windows.current = None;
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
    cache: Arc<cache::Caches>,
}

impl HdfsGateway {
    pub fn new(client: Client, config: Config) -> Self {
        let mapper = PathMapper::new(&config);
        let cache = Arc::new(cache::Caches::new(&config.cache));
        HdfsGateway {
            client: Arc::new(client),
            mapper,
            config: Arc::new(config),
            cache,
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

    /// Resolve an object's HDFS metadata (length, mtime, isdir), through the bounded TTL
    /// cache unless caching is disabled.
    ///
    /// A missing path is remembered for the (short) negative TTL: clients probe for
    /// `_SUCCESS`/`_common_metadata` on every scan, and each probe is otherwise a
    /// NameNode round trip.
    async fn file_meta(&self, hdfs_path: &str, log: &RequestLog) -> S3Result<cache::FileMeta> {
        if let Some(meta) = self.cache.metadata.get(hdfs_path) {
            return Ok(meta);
        }
        if self.cache.negative.get(hdfs_path).is_some() {
            return Err(log.attach(s3_error!(NoSuchKey)));
        }

        match self
            .client
            .get_file_info(hdfs_path)
            .instrument(log.span.clone())
            .await
        {
            Ok(status) => {
                let meta = cache::FileMeta {
                    length: status.length as u64,
                    isdir: status.isdir,
                    modification_time: status.modification_time,
                };
                self.cache.metadata.insert(hdfs_path.to_string(), meta, 1);
                Ok(meta)
            }
            Err(hdfs_native::HdfsError::FileNotFound(_)) => {
                self.cache.negative.insert(hdfs_path.to_string(), (), 1);
                Err(log.attach(s3_error!(NoSuchKey)))
            }
            Err(e) => Err(log.attach(map_hdfs_error(e, self.config.expose_upstream_errors))),
        }
    }

    /// Open (or reuse) the object's block map.
    ///
    /// `client.read()` fetches `getBlockLocations(path, 0, i64::MAX)` — the whole block
    /// list, for every GET. For a multi-TB object with thousands of blocks that is the
    /// dominant NameNode cost of a small ranged read, and parquet-style clients issue
    /// many such reads per object. Cache the `FileReader` and reuse it only while the
    /// file's freshly read identity (length, mtime) still matches: a replaced object can
    /// never be served from the previous block map. A failed open/read evicts both the
    /// block map and the metadata, so a deleted/replaced object or an all-replicas-stale
    /// block map costs one failed request, not the rest of its TTL.
    async fn open_reader(
        &self,
        hdfs_path: &str,
        meta: &cache::FileMeta,
        log: &RequestLog,
    ) -> S3Result<Arc<hdfs_native::file::FileReader>> {
        if let Some(cached) = self.cache.blocks.get(hdfs_path) {
            if cached.length == meta.length && cached.modification_time == meta.modification_time {
                return Ok(cached.reader);
            }
            self.cache.blocks.remove(hdfs_path);
        }

        let reader = match self
            .client
            .read(hdfs_path)
            .instrument(log.span.clone())
            .await
        {
            Ok(reader) => Arc::new(reader),
            Err(e) => {
                // The path may have been replaced/deleted since the (possibly cached)
                // metadata was read. Drop that metadata so the next request re-stats
                // instead of repeatedly failing on the same stale entry.
                self.cache.metadata.remove(hdfs_path);
                return Err(log.attach(map_hdfs_error(e, self.config.expose_upstream_errors)));
            }
        };
        self.cache.blocks.insert(
            hdfs_path.to_string(),
            cache::CachedReader {
                reader: Arc::clone(&reader),
                length: meta.length,
                modification_time: meta.modification_time,
            },
            1,
        );
        Ok(reader)
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

        let status = self.file_meta(&hdfs_path, &log).await?;

        // Directories are never surfaced as objects.
        if status.isdir {
            return Err(log.attach(s3_error!(NoSuchKey)));
        }

        let meta = ObjectMetadata::from_hdfs(
            hdfs_path.clone(),
            status.length,
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
        let max_keys = match input.max_keys {
            None => 1000,
            // S3 rejects a negative max-keys with InvalidArgument. The previous
            // `as usize` cast wrapped a negative value to usize::MAX, i.e. a
            // client could turn "max-keys=-1" into an unbounded listing.
            Some(n) if n < 0 => {
                return Err(log.attach(s3_error!(InvalidArgument, "max-keys must not be negative")));
            }
            Some(n) => n as usize,
        };

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
        let (page_entries, page_prefixes, is_truncated, next_token) = if delimiter == Some("/") {
            // --- Canonical delimiter path: ONE non-recursive listing of the prefix dir. ---
            // With delimiter "/", a listing's output is fully determined by the direct
            // children of the directory part of the prefix: matching files become
            // Contents, matching subdirectories collapse into CommonPrefixes. A
            // recursive walk (one `getListing` RPC per directory of the subtree) would
            // be pure waste, so list exactly the one directory (`recursive = false`).
            //
            // Permission semantics: subdirectories are never entered, and the parent
            // listing carries a child's name without needing access to the child — so
            // an unpermissioned subdirectory still surfaces as a CommonPrefix and is
            // simply never opened. Only the prefix's own directory being unreadable
            // fails the request (403 AccessDenied, matching S3).
            //
            // Semantic note: an EMPTY HDFS directory is a real directory, so it now
            // appears as a CommonPrefix. A recursive walk used to hide empty
            // directories (only files became entries), but detecting emptiness would
            // cost one extra RPC per directory — the very amplification this
            // single-listing path exists to avoid. The namespace is surfaced as-is.
            // Both listing strategies use the same cached, key-mapped lister, so a
            // repeated page (or a flat listing of the same directory) costs no NameNode
            // round trip, no path normalization and no re-sort — only the page slice.
            let lister: Arc<dyn DirLister> = Arc::new(HdfsDirLister::new(
                self.client.clone(),
                self.mapper.clone(),
                Arc::clone(&self.cache),
                log.span.clone(),
            ));
            let listing = match self.mapper.list_start(&prefix) {
                // The prefix cannot match any key under the root (e.g. it escapes it):
                // S3 semantics — an empty listing, not an error.
                None => Arc::new(DirListing::default()),
                Some(hdfs_start) => match lister.list_dir(hdfs_start).await {
                    Ok(listing) => listing,
                    // The prefix's directory does not exist (or the prefix points at a
                    // file, which the NameNode `getListing` reports the same way):
                    // nothing can match — an empty listing, not an error. Clients probe
                    // non-existent prefixes (e.g. `table/partition=x/` before it exists)
                    // constantly, and real S3 answers those with an empty page.
                    Err(hdfs_native::HdfsError::FileNotFound(_)) => Arc::new(DirListing::default()),
                    Err(e) => {
                        return Err(
                            log.attach(map_hdfs_error(e, self.config.expose_upstream_errors))
                        )
                    }
                },
            };

            // Canonical delimiter: collapse is always `dir + "/"`, so each kind's
            // key order is monotone and resumption is a binary search (O(log n)).
            let page = crate::core::paginate_slash(
                &listing.files,
                &listing.dirs,
                &prefix,
                start_after.as_deref(),
                max_keys,
            );
            (
                page.contents,
                page.common_prefixes,
                page.is_truncated,
                page.next_token,
            )
        } else if delimiter.is_some() {
            // --- Rare non-"/" delimiter: recurse over the bounded subtree. ---
            // The one-directory shortcut above is only valid for "/": any other
            // delimiter can occur arbitrarily deep in a key (`a/b.txt` with delimiter
            // "."), so the collapse depends on every matching key in the subtree, not
            // just the prefix directory's direct children. Use the same lazy, sorted
            // subtree merge as the flat path, then let the general paginator compute
            // Contents/CommonPrefixes. This path is uncommon (S3 clients overwhelmingly
            // use "/"), so materializing its matching keys is an acceptable trade for
            // correctness; the "/" path stays O(log n + page) per request.
            let (entries, _) = if max_keys == 0 {
                (Vec::new(), false)
            } else {
                match self.mapper.list_start(&prefix) {
                    None => (Vec::new(), false),
                    Some(hdfs_start) => {
                        let lister: Arc<dyn DirLister> = Arc::new(HdfsDirLister::new(
                            self.client.clone(),
                            self.mapper.clone(),
                            Arc::clone(&self.cache),
                            log.span.clone(),
                        ));
                        let mut listing = SortedListing::new(
                            lister,
                            hdfs_start,
                            prefix.clone(),
                            start_after.clone(),
                            self.mapper.clone(),
                        );
                        // `usize::MAX` requests every remaining key, not a page: the
                        // delimiter collapse needs the whole matching subtree. The
                        // continuation marker is pushed down, so a later page never
                        // re-reads keys that sort strictly before it.
                        listing.collect_page(usize::MAX).await.map_err(|e| {
                            log.attach(map_hdfs_error(e, self.config.expose_upstream_errors))
                        })?
                    }
                }
            };
            let page = paginate(
                &entries,
                &[],
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
                    let lister: Arc<dyn DirLister> = Arc::new(HdfsDirLister::new(
                        self.client.clone(),
                        self.mapper.clone(),
                        Arc::clone(&self.cache),
                        log.span.clone(),
                    ));
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

        // S3 contract: `IsTruncated=true` must come with a token to advance with, and
        // `max-keys=0` must not report truncation at all (the reference S3 tests assert
        // `IsTruncated=false` for `MaxKeys=0`, which is also the only self-consistent
        // answer when no token can be produced). Both listing strategies already return
        // an empty, non-truncated page for max-keys=0; this is the layer-level guarantee.
        debug_assert!(
            !is_truncated || next_token.is_some(),
            "truncated page without a continuation token"
        );
        let is_truncated = is_truncated && next_token.is_some();

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

        let status = self.file_meta(&hdfs_path, &log).await?;

        // Directories are never surfaced as objects.
        if status.isdir {
            return Err(log.attach(s3_error!(NoSuchKey)));
        }

        let file_len = status.length;
        let last_modified = millis_to_timestamp(status.modification_time);

        let meta = ObjectMetadata::from_hdfs(
            hdfs_path.clone(),
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
        let reader = self.open_reader(&hdfs_path, &status, &log).await?;
        // Read in bounded windows (not one ReadBlock per 64 KiB buffer, and not one
        // eagerly-built stream per block of a multi-TB object). See `range_stream`.
        if window_end > reader.file_length() {
            // File shrank since the metadata was read; read_range_stream would panic.
            // Both caches are stale: drop the block map (so the next request re-opens
            // it) and the metadata (so the next request re-reads the new length instead
            // of failing this check for the rest of the metadata TTL).
            self.cache.blocks.remove(&hdfs_path);
            self.cache.metadata.remove(&hdfs_path);
            return Err(log.attach(s3_error!(InternalError)));
        }
        let remaining = window_end - window_start;
        let cache = Arc::clone(&self.cache);
        let cached_path = hdfs_path.clone();
        let stream = range_stream(reader, window_start, remaining)
            .map(move |r| {
                if r.is_err() {
                    // Stale block locations (or a DataNode failure): stop reusing this
                    // block map for the rest of its TTL. Also drop the metadata: if the
                    // file was deleted/replaced, re-stat on the next request instead of
                    // re-opening a doomed block map until the metadata TTL expires.
                    cache.blocks.remove(&cached_path);
                    cache.metadata.remove(&cached_path);
                }
                r
            })
            .boxed();
        // `hdfs-native`'s range stream is `Send` but not `Sync` (it is built on
        // `BoxStream`, which erases `Sync`), while `s3s` requires the response body to be
        // both (`StreamingBlob::wrap`). The mutex restores `Sync`; it is never contended
        // (the body is polled by one task) and is held for a single `poll_next` only.
        let stream = std::sync::Mutex::new(stream);
        let stream = futures::stream::poll_fn(move |cx| {
            let mut stream = stream.lock().unwrap_or_else(|e| e.into_inner());
            std::pin::Pin::new(&mut *stream).poll_next(cx)
        });
        // `bytes_stream` enforces the exact content length: an upstream stream that
        // ends early fails with a short-read error instead of a silently-short
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
