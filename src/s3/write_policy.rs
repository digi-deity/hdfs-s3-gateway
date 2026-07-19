//! Minimum-contract handling for unimplemented operations.
//!
//! This module centralizes the policy for operations the read-only gateway does not
//! implement, so the behavior is uniform and easy to audit:
//!
//! - **Write-shaped operations** (`PutObject`, `DeleteObject`, multipart upload, `CopyObject`,
//!   bucket create/delete, etc.) all return a single, uniform error: `AccessDenied`. A
//!   read-only-mode message is more informative to an operator than a generic `NotImplemented`.
//! - **Read-only bucket-configuration probes** that real S3 answers even on a fresh, feature-less
//!   bucket (`GetBucketVersioning`, `GetBucketTagging`, `GetBucketAcl`, `GetBucketCors`) answer
//!   the way real S3 would ("not configured") rather than erroring, so well-behaved clients that
//!   probe these don't break.

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

/// The uniform error returned for every write-shaped operation.
///
/// We deliberately use `AccessDenied` (with a read-only-mode message) rather than
/// `NotImplemented`: it tells an operator exactly why the write failed (this is a
/// read-only gateway), and it's a real S3 error code well-behaved clients already
/// understand.
pub fn write_denied() -> s3s::S3Error {
    s3_error!(
        AccessDenied,
        "this gateway is read-only; write operations are not supported"
    )
}

/// `GetBucketVersioning` on a fresh bucket → versioning disabled (empty status).
pub fn bucket_versioning_not_configured(
    req: S3Request<GetBucketVersioningInput>,
) -> S3Result<S3Response<GetBucketVersioningOutput>> {
    let _ = req;
    Ok(S3Response::new(GetBucketVersioningOutput::default()))
}

/// `GetBucketTagging` on a fresh bucket → empty tag set.
pub fn bucket_tagging_not_configured(
    req: S3Request<GetBucketTaggingInput>,
) -> S3Result<S3Response<GetBucketTaggingOutput>> {
    let _ = req;
    Ok(S3Response::new(GetBucketTaggingOutput {
        tag_set: TagSet::default(),
    }))
}

/// `GetBucketAcl` on a fresh bucket → empty owner/grants (S3 returns an owner with no grants).
pub fn bucket_acl_not_configured(
    req: S3Request<GetBucketAclInput>,
) -> S3Result<S3Response<GetBucketAclOutput>> {
    let _ = req;
    Ok(S3Response::new(GetBucketAclOutput::default()))
}

/// `GetBucketCors` on a fresh bucket → no CORS rules.
pub fn bucket_cors_not_configured(
    req: S3Request<GetBucketCorsInput>,
) -> S3Result<S3Response<GetBucketCorsOutput>> {
    let _ = req;
    Ok(S3Response::new(GetBucketCorsOutput::default()))
}
