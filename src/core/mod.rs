//! Core translation logic between S3 keys and HDFS paths, and HDFS metadata to
//! S3-shaped metadata. This crate is deliberately free of `s3s` and HTTP-framework
//! types so that every path-mapping and metadata-translation decision is unit-testable
//! without touching the S3 protocol layer.

pub mod listing;
pub mod metadata;
pub mod path;
pub mod range;

pub use listing::{
    cmp_dir_keys, cmp_dir_output, cmp_file_dir, decode_token, encode_token, list_to_contents,
    paginate, paginate_slash, CommonPrefixSet, DirListing, ListEntry,
};
pub use metadata::{fallback_etag, ObjectMetadata};
pub use path::PathMapper;
pub use range::{resolve_range, ByteRange};
