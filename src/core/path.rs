//! S3 key ↔ HDFS path translation.
//!
//! The mapping is intentionally simple: one configured bucket name and one configured
//! HDFS root. An S3 key `foo/bar.parquet` maps to `{root}/foo/bar.parquet`.
//!
//! Security note: because we do NOT verify auth, containment within `hdfs_root` is
//! the main safety boundary this service offers. Path-traversal attempts
//! (`../../etc/passwd`-shaped keys) MUST be rejected/normalized so they cannot escape the
//! configured root.

use std::path::Path;

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
            return None;
        }

        // Join root + key and canonicalize to detect traversal.
        let combined = format!("{}/{}", self.root, key);
        let normalized = normalize_abs(&combined)?;

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

/// Collapse `.` and `..` components of an absolute-ish path without touching the
/// filesystem. Returns `None` if `..` would escape the root of the path.
///
/// The input is expected to be URL-decoded already (s3s decodes the key before we
/// see it). A raw `%2F` is therefore treated as a literal filename, not a separator —
/// which is the safe choice: it cannot be used to escape the root.
fn normalize_abs(path: &str) -> Option<String> {
    let mut absolute = false;
    let mut parts: Vec<&str> = Vec::new();
    for component in Path::new(path).components() {
        use std::path::Component::*;
        match component {
            Prefix(_) => return None,
            RootDir => {
                absolute = true;
            }
            CurDir => {}
            ParentDir => {
                // Pop the last real segment; never pop past root.
                match parts.last() {
                    Some(&"") => return None, // already at root
                    Some(_) => {
                        parts.pop();
                    }
                    None => {
                        if absolute {
                            return None;
                        }
                    }
                }
            }
            Normal(name) => {
                parts.push(name.to_str()?);
            }
        }
    }

    if absolute {
        Some(format!("/{}", parts.join("/")))
    } else {
        Some(parts.join("/"))
    }
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
}
