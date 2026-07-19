//! Mapping from `hdfs_native::HdfsError` to `s3s` error variants.

use hdfs_native::HdfsError;
use s3s::{s3_error, S3Error, S3ErrorCode};

/// Map an HDFS error into the appropriate `s3s` error variant.
///
/// The mapping is deliberately specific where S3 has a matching code, and collapses the
/// rest to `InternalError` (we don't want to leak HDFS internals as S3 errors):
///
/// - `FileNotFound` / `IsADirectoryError` / `InvalidPath` → `NoSuchKey`
///   (directories are not objects; a bad key is just "not found").
/// - `InvalidArgument` → `InvalidArgument` (e.g. malformed HDFS path).
/// - `UnsupportedFeature` / `UnsupportedErasureCodingPolicy` → `NotImplemented`
///   (we are a read-only subset; some HDFS files can't be served).
/// - `RPCError` / `FatalRPCError` whose upstream exception class is an access-control
///   problem — `AccessControlException`, `AuthorizationException`, `AccessDeniedException`,
///   or `PermissionDenied(Exception)` — → `AccessDenied` (HTTP 403). This is the common
///   enterprise-Hadoop case: a caller reaches the NameNode but lacks POSIX/ACL rights to
///   the file. It must NOT collapse to `InternalError` (500), or S3 clients misread it as a
///   gateway fault and retry. The match is on the fully-qualified Java class name suffix
///   (see `is_access_denied`).
/// - `RPCError` / `FatalRPCError` for `StandbyException` / `SafeModeException` →
///   `ServiceUnavailable` (HTTP 503): the cluster is temporarily unable to serve, which is
///   the correct signal for a client to back off and retry.
/// - `SASLError` / `GSSAPIError` / `NoSASLMechanism` → `AccessDenied` (HTTP 403): the
///   client could not authenticate to the secure cluster, which is an access problem.
/// - `AlreadyExists` → `InternalError` (shouldn't happen on a read path, but if it does
///   it's not a client fault).
/// - Everything else (`IOError`, `DataTransferError`, `ChecksumError`, `OperationFailed`,
///   `BlocksNotFound`, `TrashNotEnabled`, `ErasureCodingError`, `InvalidRPCResponse`,
///   `XmlParseError`, `UrlParseError`, and the `InternalError` variant itself) →
///   `InternalError`.
///
/// In all non-`NoSuchKey` cases the original `HdfsError` is attached as the error `source`
/// and (when `expose_upstream_errors` is set) its text becomes the S3 `Message`. The error
/// is always logged server-side with the request id (see `RequestLog`), so operators can
/// correlate a client's `x-amz-request-id` to the full upstream detail without ever
/// exposing it to untrusted clients.
/// Upstream Hadoop exception class names that denote an authorization/permission denial.
/// When the NameNode rejects a request for lack of rights, `hdfs-native` surfaces it as an
/// `RPCError`/`FatalRPCError` whose first field is the fully-qualified Java exception class.
/// Matching on the class name lets us translate it to S3 `AccessDenied` (403) rather than a
/// misleading `InternalError` (500).
fn is_access_denied(exception: &str) -> bool {
    exception.ends_with("AccessControlException")
        || exception.ends_with("AuthorizationException")
        || exception.ends_with("AccessDeniedException")
        || exception.ends_with("PermissionDeniedException")
        || exception.ends_with("PermissionDenied")
}

/// Upstream Hadoop exception class names that denote the cluster is temporarily unable to
/// serve the request (active NameNode is in standby / safe mode). These map to S3
/// `ServiceUnavailable` (503) so clients back off and retry.
fn is_unavailable(exception: &str) -> bool {
    exception.ends_with("StandbyException") || exception.ends_with("SafeModeException")
}

pub fn map_hdfs_error(err: HdfsError, expose: bool) -> S3Error {
    // Not-found family: no upstream detail worth surfacing.
    match err {
        HdfsError::FileNotFound(_)
        | HdfsError::IsADirectoryError(_)
        | HdfsError::InvalidPath(_) => {
            return s3_error!(NoSuchKey);
        }
        _ => {}
    }

    let code: S3ErrorCode = match &err {
        HdfsError::InvalidArgument(_) => S3ErrorCode::InvalidArgument,
        HdfsError::UnsupportedFeature(_) | HdfsError::UnsupportedErasureCodingPolicy(_) => {
            S3ErrorCode::NotImplemented
        }
        // Auth/authorization failures: the caller is not allowed to do this.
        HdfsError::SASLError(_) | HdfsError::GSSAPIError(..) | HdfsError::NoSASLMechanism => {
            S3ErrorCode::AccessDenied
        }
        // RPC-level errors carry the upstream Hadoop exception class name as the first
        // string. Match known access-control / availability exceptions by class name so we
        // return the correct S3 status code instead of a generic 500.
        HdfsError::RPCError(exception, _) | HdfsError::FatalRPCError(exception, _) => {
            if is_access_denied(exception) {
                S3ErrorCode::AccessDenied
            } else if is_unavailable(exception) {
                S3ErrorCode::ServiceUnavailable
            } else {
                S3ErrorCode::InternalError
            }
        }
        _ => S3ErrorCode::InternalError,
    };

    // Build the error. When `expose` is on, the HDFS error text becomes the client-facing
    // message; otherwise we keep the generic S3 code message and only retain the source
    // for server-side logging.
    let mut e = if expose {
        S3Error::with_message(code, err.to_string())
    } else {
        S3Error::new(code)
    };
    e.set_source(Box::new(err));
    e
}
