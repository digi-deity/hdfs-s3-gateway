//! Translation of HDFS `FileStatus` into an internal metadata struct that the `s3` module
//! maps into `s3s` output types. No `s3s` types appear here.

/// Internal representation of an object's metadata, derived from HDFS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// Absolute HDFS path of the object.
    pub path: String,
    /// Object length in bytes.
    pub length: u64,
    /// Whether the path is a directory. Directories are never surfaced as objects.
    pub is_dir: bool,
    /// Last-modified time, milliseconds since the Unix epoch (HDFS `modification_time`).
    pub modification_time: u64,
    /// Raw HDFS checksum bytes (CRC32C-based), if available. Used as the ETag source.
    ///
    /// NOTE: `hdfs-native` does not currently expose the NameNode `getFileChecksum`
    /// RPC (nor does `FileStatus` carry a checksum), so this is always `None` in
    /// practice. The field is retained so the ETag logic is correct the moment
    /// upstream support lands; until then the ETag falls back to a deterministic
    /// length+mtime-derived value (see [`ObjectMetadata::etag`]).
    pub checksum: Option<Vec<u8>>,
}

impl ObjectMetadata {
    /// Build S3 metadata from HDFS fields. `checksum` is the raw bytes returned by the
    /// NameNode's `getFileChecksum` RPC (when available). Currently always `None`
    /// because `hdfs-native` does not expose that RPC — see the `checksum` field docs.
    pub fn from_hdfs(
        path: String,
        length: u64,
        is_dir: bool,
        modification_time: u64,
        checksum: Option<Vec<u8>>,
    ) -> Self {
        ObjectMetadata {
            path,
            length,
            is_dir,
            modification_time,
            checksum,
        }
    }

    /// Compute the ETag string for this object.
    ///
    /// The ETag is the HDFS native checksum, NOT MD5. We hex-encode the raw
    /// checksum bytes. When no checksum is available we fall back to a stable value
    /// derived from length + mtime so that the ETag is still deterministic (never an
    /// error, never real MD5).
    pub fn etag(&self) -> String {
        match &self.checksum {
            Some(bytes) => hex_encode(bytes),
            None => fallback_etag(self.length, self.modification_time),
        }
    }

    /// Best-effort content type from the file extension, with `application/octet-stream`
    /// fallback.
    pub fn content_type(&self) -> String {
        let name = self.path.rsplit('/').next().unwrap_or(&self.path);
        mime_guess::from_path(name)
            .first()
            .map(|m| m.to_string())
            .unwrap_or_else(|| "application/octet-stream".to_string())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
    }
    s
}

/// Deterministic ETag used when no HDFS checksum is available (the current state, since
/// `hdfs-native` does not expose `getFileChecksum`). Derived from length + modification
/// time so it is stable for an unchanged object. Shared by `HeadObject`/`GetObject` and
/// `ListObjectsV2` so the same key always yields the same ETag regardless of which
/// operation produced it.
pub fn fallback_etag(length: u64, modification_time: u64) -> String {
    format!("hdfs-{:x}-{:x}", length, modification_time)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(checksum: Option<Vec<u8>>) -> ObjectMetadata {
        ObjectMetadata::from_hdfs(
            "/data/a/b.txt".into(),
            1024,
            false,
            1_700_000_000_000,
            checksum,
        )
    }

    #[test]
    fn etag_from_checksum() {
        let m = meta(Some(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(m.etag(), "deadbeef");
    }

    #[test]
    fn etag_fallback_when_no_checksum() {
        let _m = meta(None);
        let e = _m.etag();
        assert!(e.starts_with("hdfs-"));
    }

    #[test]
    fn fallback_etag_matches_head_and_listing() {
        // The listing path and the head/get path must produce identical ETags for the
        // same object, otherwise a client that caches the listing ETag and later HEADs
        // the key sees a spurious mismatch.
        let m = meta(None);
        let head_etag = m.etag();
        let list_etag = fallback_etag(m.length, m.modification_time);
        assert_eq!(head_etag, list_etag);
        assert_eq!(head_etag, "hdfs-400-18bcfe56800");
    }

    #[test]
    fn content_type_parquet() {
        let _m = meta(None);
        let m = ObjectMetadata::from_hdfs("/data/x.parquet".into(), 1, false, 0, None);
        assert_eq!(m.content_type(), "application/vnd.apache.parquet");
    }

    #[test]
    fn content_type_unknown_falls_back() {
        let m = ObjectMetadata::from_hdfs("/data/x.unknownext".into(), 1, false, 0, None);
        assert_eq!(m.content_type(), "application/octet-stream");
    }
}
