//! S3 key ↔ HDFS path translation.
//!
//! The mapping is intentionally simple: one configured bucket name and one configured
//! HDFS root. An S3 key `foo/bar.parquet` maps to `{root}/foo/bar.parquet`.
//!
//! Security note: because we do NOT verify auth, containment within `hdfs_root` is
//! the main safety boundary this service offers. Path-traversal attempts
//! (`../../etc/passwd`-shaped keys) MUST be rejected/normalized so they cannot escape the
//! configured root.
//!
//! Platform note: HDFS paths are POSIX-shaped, so all normalization here is hand-rolled
//! over `/` separators and deliberately never goes through `std::path::Path`. On Windows
//! the standard library parses a leading `//` (or `\\`) as a UNC prefix, backslashes as
//! separators, and `C:`-style components as drive prefixes — all of which silently corrupt
//! HDFS paths. A `split('/')` loop behaves identically on every platform.

use tracing::debug;

use crate::config::Config;

/// Translates S3 keys to absolute HDFS paths under a configured root, and validates
/// that resolved paths stay within the root.
#[derive(Debug, Clone)]
pub struct PathMapper {
    root: String,
    bucket: String,
}

impl PathMapper {
    pub fn new(config: &Config) -> Self {
        // Normalize root to an absolute path with no trailing slash.
        let root = normalize_root(&config.hdfs_root);
        PathMapper {
            root,
            bucket: config.bucket_name.clone(),
        }
    }

    /// The single configured bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The configured HDFS root (normalized, no trailing slash).
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Resolve an S3 key into an absolute HDFS path string.
    ///
    /// Returns `None` if the key is empty or would escape the configured root
    /// (path traversal). The key is expected to already be URL-decoded by `s3s`
    /// before reaching us (we assume decoded input).
    pub fn key_to_hdfs_path(&self, key: &str) -> Option<String> {
        let key = key.trim_start_matches('/');
        if key.is_empty() {
            debug!(%key, "rejecting S3 key: empty after stripping leading slashes");
            return None;
        }

        // Join root + key and canonicalize to detect traversal. Avoid producing a
        // double-slash boundary when the root is `/` — the normalizer below handles
        // `//` correctly on every platform, but the combined string should not look
        // like a UNC path in the first place.
        let combined = join_root(&self.root, key);
        let Some(normalized) = normalize_abs(&combined) else {
            debug!(%key, %combined, "rejecting S3 key: normalization failed (path escapes its root)");
            return None;
        };

        // Ensure the normalized path is still rooted at `self.root`.
        // The root `/` is a special case: every absolute path is under it.
        let under_root = if self.root == "/" {
            normalized.starts_with('/')
        } else {
            normalized == self.root || normalized.starts_with(&format!("{}/", self.root))
        };
        if under_root {
            Some(normalized)
        } else {
            debug!(%key, %normalized, root = %self.root, "rejecting S3 key: resolves outside configured hdfs_root");
            None
        }
    }

    /// Inverse of [`key_to_hdfs_path`]: given an absolute HDFS path, return the S3 key
    /// relative to the root. Returns `None` if the path is not under the root.
    pub fn hdfs_path_to_key(&self, hdfs_path: &str) -> Option<String> {
        let normalized = normalize_abs(hdfs_path)?;
        if normalized == self.root {
            return Some(String::new());
        }
        // Root `/` is special: strip a single leading slash instead of `//`.
        let stripped = if self.root == "/" {
            normalized.strip_prefix('/')
        } else {
            normalized.strip_prefix(&format!("{}/", self.root))
        };
        stripped.map(|s| s.to_string())
    }
}

/// Join a normalized root and a key without producing a `//` boundary when the
/// root is `/`.
fn join_root(root: &str, key: &str) -> String {
    if root == "/" {
        format!("/{key}")
    } else {
        format!("{root}/{key}")
    }
}

/// Normalize a root path: make absolute (relative to `/` if needed), collapse `.`/`..`,
/// and strip a trailing slash. Returns an empty-ish absolute root for `/`.
fn normalize_root(root: &str) -> String {
    let rooted = if root.starts_with('/') {
        root.to_string()
    } else {
        format!("/{}", root)
    };
    normalize_abs(&rooted).unwrap_or_else(|| "/".to_string())
}

/// Collapse `.` and `..` components of a POSIX-style path without touching the
/// filesystem. Returns `None` if `..` would escape the root of an absolute path.
///
/// This is deliberately a hand-rolled `split('/')` loop rather than
/// `std::path::Path::components()`: HDFS paths are POSIX-shaped, and on Windows
/// `std::path` parses a leading `//` (or `\\`) as a UNC prefix, backslashes as
/// separators, and drive-letter components — all of which would corrupt or reject
/// valid HDFS paths. A `split('/')` loop is byte-identical on every platform.
///
/// Backslashes are *literal filename characters* here, exactly like an undecoded
/// `%2F`: only `/` is a separator, so a backslash-shaped segment (`a\..\b`) can never
/// escape the configured root. The input is expected to be URL-decoded already (s3s
/// decodes the key before we see it); a raw `%2F` is therefore a literal filename, not
/// a separator — the safe choice.
fn normalize_abs(path: &str) -> Option<String> {
    let absolute = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                // Pop the last real segment; never pop past root.
                if parts.pop().is_none() && absolute {
                    return None;
                }
            }
            other => parts.push(other),
        }
    }

    Some(if absolute {
        format!("/{}", parts.join("/"))
    } else {
        parts.join("/")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapper() -> PathMapper {
        let config = Config {
            namenode_uri: "hdfs://localhost:8020".into(),
            hdfs_root: "/data".into(),
            bucket_name: "hdfs".into(),
            listen_addr: "0.0.0.0:8080".into(),
            max_concurrent_requests: 2048,
            expose_upstream_errors: false,
            hdfs_options: Default::default(),
            hdfs_config_dir: None,
            hdfs_user: None,
            auth_secret: None,
        };
        PathMapper::new(&config)
    }

    /// Mapper with `hdfs_root = "/"` — the configuration that triggered the
    /// Windows-only `NoSuchKey` bug (see [`root_slash_maps_deep_key`]).
    fn root_mapper() -> PathMapper {
        let config = Config {
            namenode_uri: "hdfs://localhost:8020".into(),
            hdfs_root: "/".into(),
            bucket_name: "hdfs".into(),
            listen_addr: "0.0.0.0:8080".into(),
            max_concurrent_requests: 2048,
            expose_upstream_errors: false,
            hdfs_options: Default::default(),
            hdfs_config_dir: None,
            hdfs_user: None,
            auth_secret: None,
        };
        PathMapper::new(&config)
    }

    #[test]
    fn simple_key() {
        let m = mapper();
        assert_eq!(
            m.key_to_hdfs_path("a/b/c.txt"),
            Some("/data/a/b/c.txt".into())
        );
    }

    #[test]
    fn leading_slash_normalized() {
        let m = mapper();
        assert_eq!(m.key_to_hdfs_path("/a/b.txt"), Some("/data/a/b.txt".into()));
    }

    #[test]
    fn trailing_slash_normalized() {
        let m = mapper();
        assert_eq!(m.key_to_hdfs_path("a/"), Some("/data/a".into()));
    }

    #[test]
    fn empty_key_rejected() {
        let m = mapper();
        assert_eq!(m.key_to_hdfs_path(""), None);
        assert_eq!(m.key_to_hdfs_path("/"), None);
    }

    #[test]
    fn path_traversal_rejected() {
        let m = mapper();
        // Decoded keys (s3s decodes %2F -> '/' before we see them).
        assert_eq!(m.key_to_hdfs_path("../../etc/passwd"), None);
        assert_eq!(m.key_to_hdfs_path("a/../../etc/passwd"), None);
        // A literal '%2F' (not decoded) is a filename, not a separator, and stays
        // under root — safe, never escapes.
        assert!(m.key_to_hdfs_path("..%2F..%2Fetc%2Fpasswd").is_some());
    }

    #[test]
    fn non_ascii_key() {
        let m = mapper();
        // Non-ASCII keys are allowed; they map directly.
        assert_eq!(
            m.key_to_hdfs_path("café/naïve.txt"),
            Some("/data/café/naïve.txt".into())
        );
    }

    #[test]
    fn round_trip() {
        let m = mapper();
        let key = "foo/bar/baz.parquet";
        let p = m.key_to_hdfs_path(key).unwrap();
        assert_eq!(m.hdfs_path_to_key(&p), Some(key.into()));
    }

    #[test]
    fn hdfs_path_to_key_root() {
        let m = mapper();
        assert_eq!(m.hdfs_path_to_key("/data"), Some(String::new()));
    }

    #[test]
    fn hdfs_path_outside_root() {
        let m = mapper();
        assert_eq!(m.hdfs_path_to_key("/etc/passwd"), None);
    }

    // --- hdfs_root = "/" (regression: Windows parsed `//key` as a UNC prefix) ---

    #[test]
    fn root_slash_maps_deep_key() {
        // Regression test for the reported bug: with hdfs_root = "/", the combined
        // string used to start with "//" (e.g. "//appdata/foo/bar.parquet"). On
        // Windows, std::path::Path parsed that as a UNC prefix and normalize_abs
        // returned None, so every HeadObject/GetObject 404'd with NoSuchKey while
        // ListObjectsV2 (which bypasses key_to_hdfs_path) worked fine.
        let m = root_mapper();
        assert_eq!(
            m.key_to_hdfs_path("appdata/foo/bar.parquet"),
            Some("/appdata/foo/bar.parquet".into())
        );
    }

    #[test]
    fn root_slash_maps_with_leading_slash_key() {
        let m = root_mapper();
        assert_eq!(
            m.key_to_hdfs_path("/appdata/foo"),
            Some("/appdata/foo".into())
        );
    }

    #[test]
    fn root_slash_collapses_dot_components() {
        let m = root_mapper();
        assert_eq!(m.key_to_hdfs_path("a/./b.txt"), Some("/a/b.txt".into()));
        assert_eq!(m.key_to_hdfs_path("a/../b.txt"), Some("/b.txt".into()));
    }

    #[test]
    fn root_slash_rejects_traversal() {
        let m = root_mapper();
        assert_eq!(m.key_to_hdfs_path("../etc/passwd"), None);
        assert_eq!(m.key_to_hdfs_path("a/../../etc/passwd"), None);
        assert_eq!(m.key_to_hdfs_path("/.."), None);
    }

    #[test]
    fn root_slash_rejects_empty_key() {
        let m = root_mapper();
        assert_eq!(m.key_to_hdfs_path(""), None);
        assert_eq!(m.key_to_hdfs_path("/"), None);
        assert_eq!(m.key_to_hdfs_path("///"), None);
    }

    #[test]
    fn root_slash_round_trip() {
        let m = root_mapper();
        let key = "appdata/foo/bar.parquet";
        let p = m.key_to_hdfs_path(key).unwrap();
        assert_eq!(p, "/appdata/foo/bar.parquet");
        assert_eq!(m.hdfs_path_to_key(&p), Some(key.into()));
    }

    #[test]
    fn root_slash_hdfs_path_to_key() {
        let m = root_mapper();
        assert_eq!(m.hdfs_path_to_key("/"), Some(String::new()));
        assert_eq!(m.hdfs_path_to_key("/data/x.txt"), Some("data/x.txt".into()));
        // Relative paths are never under the absolute root.
        assert_eq!(m.hdfs_path_to_key("data/x.txt"), None);
    }

    #[test]
    fn root_slash_normalizes_root() {
        let m = root_mapper();
        assert_eq!(m.root(), "/");
    }

    // --- normalize_abs platform independence (the actual root-cause fix) ---

    #[test]
    fn double_slash_prefix_normalized() {
        // `//a/b` used to be rejected on Windows, where std::path parsed the
        // leading `//` as a UNC prefix. The POSIX normalizer treats it as an
        // absolute path with an empty first segment — identically on all platforms.
        assert_eq!(normalize_abs("//a/b"), Some("/a/b".into()));
        assert_eq!(normalize_abs("///a//b/"), Some("/a/b".into()));
        assert_eq!(normalize_abs("//"), Some("/".into()));
    }

    #[test]
    fn backslash_is_literal_filename_char() {
        // Backslash must never act as a separator: HDFS paths are POSIX-shaped and
        // this normalizer is platform-independent. A backslash-shaped segment cannot
        // escape the root (only '/' separates), mirroring the %2F literal policy.
        assert_eq!(
            normalize_abs("/data/a\\..\\b"),
            Some("/data/a\\..\\b".into())
        );
        let m = mapper();
        assert!(m.key_to_hdfs_path("a\\..\\b").is_some());
        assert!(m.key_to_hdfs_path("..\\..\\etc").is_some());
    }

    #[test]
    fn normalize_abs_absolute_forms() {
        assert_eq!(normalize_abs("/a/b"), Some("/a/b".into()));
        assert_eq!(normalize_abs("/a/./b"), Some("/a/b".into()));
        assert_eq!(normalize_abs("/a/b/"), Some("/a/b".into()));
        assert_eq!(normalize_abs("/"), Some("/".into()));
        // `..` must not escape past the root of an absolute path.
        assert_eq!(normalize_abs("/.."), None);
        assert_eq!(normalize_abs("/a/.."), Some("/".into()));
        assert_eq!(normalize_abs("/a/../.."), None);
    }

    #[test]
    fn normalize_abs_relative_forms() {
        assert_eq!(normalize_abs("a/b"), Some("a/b".into()));
        assert_eq!(normalize_abs("../a"), Some("a".into()));
        assert_eq!(normalize_abs("a/.."), Some(String::new()));
        assert_eq!(normalize_abs("a/../b"), Some("b".into()));
        assert_eq!(normalize_abs(""), Some(String::new()));
    }
}
