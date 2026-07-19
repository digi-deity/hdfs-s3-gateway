//! Concurrency backpressure around the `s3s` service.
//!
//! `s3s` explicitly does **not** provide body-size limits, rate limiting, or backpressure —
//! that's the integrator's responsibility. This module caps the number of in-flight requests
//! at `config.max_concurrent_requests`. When the cap is exceeded, the request is rejected with
//! a clean `503 SlowDown` (a real S3 error code well-behaved clients already know to back off
//! on) rather than being allowed to degrade into unbounded concurrency, hangs, or OOM.
//!
//! The cap is enforced as a `hyper::service::Service` wrapper around `S3Service` (the natural
//! integration point, since `s3s` is built on `hyper`/`tower`). `poll_ready` always reports
//! ready so the connection keeps dispatching; the actual admission decision happens in `call`,
//! where we `try_acquire` a permit from a shared `Semaphore`.
//!
//! Crucially, the permit is held until the **entire response body has been streamed** to the
//! client (or the connection drops), not merely until the handler builds the response headers.
//! This makes `max_concurrent_requests` bound *end-to-end* in-flight requests — including GET
//! body egress to slow clients and large objects — rather than only the handler's work up to
//! response-header construction. Without this, a slow client or a large object would release its
//! slot the instant the headers are built, letting unbounded GET bodies stream concurrently and
//! overload the NameNode/DataNodes and network. Excess requests (those that cannot acquire a
//! permit) get a clean, signaled `503 SlowDown` rejection.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::Request as HttpRequest;
use http_body::{Body as HttpBody, Frame, SizeHint};
use s3s::service::S3Service;
use s3s::{s3_error, Body, HttpError, HttpResponse};
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// A `hyper::service::Service` wrapper that caps concurrency.
#[derive(Clone)]
pub struct BackpressureService {
    inner: S3Service,
    sem: Arc<Semaphore>,
}

impl BackpressureService {
    /// Wrap an `S3Service`, allowing at most `max_concurrent` in-flight requests.
    pub fn new(inner: S3Service, max_concurrent: usize) -> Self {
        Self {
            inner,
            sem: Arc::new(Semaphore::new(max_concurrent.max(1))),
        }
    }
}

/// Build a `503 SlowDown` response. We render it through `s3s`'s own error serializer so the
/// XML shape matches real S3 (and what `s3s` would produce for any other error).
fn slow_down_response() -> HttpResponse {
    s3_error!(SlowDown, "too many requests in flight; please retry later")
        .to_http_response()
        .unwrap_or_else(|_| {
            // Fallback that should never trigger: a bare 503 with no body.
            HttpResponse::from(
                http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::empty())
                    .unwrap(),
            )
        })
}

impl hyper::service::Service<HttpRequest<hyper::body::Incoming>> for BackpressureService {
    type Response = HttpResponse;
    type Error = HttpError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn call(&self, req: HttpRequest<hyper::body::Incoming>) -> Self::Future {
        match self.sem.clone().try_acquire_owned() {
            Ok(permit) => {
                let inner = self.inner.clone();
                // `S3Service::call` expects `Body`; convert the incoming hyper body.
                let req = req.map(s3s::Body::from);
                Box::pin(async move {
                    let mut resp = inner.call(req).await?;
                    // Hold the backpressure permit until the response body is fully
                    // streamed to the client (or the connection is dropped). This makes
                    // `max_concurrent_requests` bound *end-to-end* in-flight requests —
                    // including GET body egress — rather than only the handler's work up
                    // to response-header construction. Without this, a slow client or a
                    // large object would release its slot the instant the headers are
                    // built, letting unbounded GET bodies stream concurrently and
                    // overload the NameNode/DataNodes and network.
                    let body = std::mem::replace(resp.body_mut(), Body::empty());
                    *resp.body_mut() = Body::http_body(PermitBody::new(body, permit));
                    Ok(resp)
                })
            }
            Err(_) => Box::pin(async move { Ok(slow_down_response()) }),
        }
    }
}

/// A response body wrapper that holds a backpressure permit until the body is fully
/// streamed (or dropped).
///
/// `s3s::Body` is `Unpin`, so we can poll the inner body via `get_mut` without pin
/// projection. The permit is released as soon as `poll_frame` returns `None` (end of
/// stream) or when the body is dropped (e.g. the client disconnects mid-stream), so the
/// concurrency slot is freed promptly once the request is truly finished.
struct PermitBody {
    inner: Body,
    permit: Option<OwnedSemaphorePermit>,
}

impl PermitBody {
    fn new(inner: Body, permit: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            permit: Some(permit),
        }
    }

    /// Drop the permit, freeing the concurrency slot. Idempotent.
    fn release(&mut self) {
        self.permit = None;
    }
}

impl HttpBody for PermitBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_frame(cx);
        if matches!(result, Poll::Ready(None)) {
            // End of stream reached: the response is fully sent, release the slot.
            this.release();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for PermitBody {
    fn drop(&mut self) {
        // Released here if the body was dropped before reaching end-of-stream
        // (e.g. client disconnects mid-stream). Frees the slot promptly.
        self.release();
    }
}
