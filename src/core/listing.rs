//! Listing translation logic: turning a flat list of HDFS paths into S3 `Contents` and
//! `CommonPrefixes`, plus continuation-token pagination. Pure logic, no `s3s` types.

/// A single object entry produced by listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
    pub modification_time: u64,
}

/// A set of common prefixes (subdirectory collapses) keyed by the prefix string.
#[derive(Debug, Clone, Default)]
pub struct CommonPrefixSet {
    prefixes: std::collections::BTreeSet<String>,
}

impl CommonPrefixSet {
    pub fn insert(&mut self, prefix: String) {
        self.prefixes.insert(prefix);
    }

    pub fn into_vec(self) -> Vec<String> {
        self.prefixes.into_iter().collect()
    }
}

/// Given a flat list of object keys (relative to the bucket root) and a delimiter,
/// compute which keys become `Contents` and which collapse into `CommonPrefixes`.
///
/// A key `a/b/c.txt` with delimiter `/` and prefix `a/` collapses to a single
/// `CommonPrefix` `a/b/` — NOT one per nesting level (the classic "fake folder" bug).
pub fn list_to_contents(
    entries: &[ListEntry],
    prefix: &str,
    delimiter: Option<&str>,
) -> (Vec<ListEntry>, CommonPrefixSet) {
    let mut contents = Vec::new();
    let mut common = CommonPrefixSet::default();

    for entry in entries {
        if !entry.key.starts_with(prefix) {
            continue;
        }
        let rest = &entry.key[prefix.len()..];

        if let Some(delim) = delimiter {
            if let Some(idx) = rest.find(delim) {
                // Collapse everything up to and including the first delimiter.
                let common_prefix = format!("{}{}{}", prefix, &rest[..idx + delim.len()], "");
                common.insert(common_prefix);
                continue;
            }
        }
        contents.push(entry.clone());
    }

    (contents, common)
}

/// Encode a continuation token. We use base64 of the last returned key.
pub fn encode_token(last_key: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(last_key)
}

/// Decode a continuation token back into the resume key.
pub fn decode_token(token: &str) -> Option<String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(token)
        .ok()?;
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str) -> ListEntry {
        ListEntry {
            key: key.into(),
            size: 10,
            modification_time: 0,
        }
    }

    #[test]
    fn delimiter_collapses_one_level() {
        let entries = vec![
            entry("a/b/c.txt"),
            entry("a/b/d.txt"),
            entry("a/e.txt"),
            entry("f.txt"),
        ];
        let (contents, common) = list_to_contents(&entries, "", Some("/"));
        let common = common.into_vec();
        // Any key containing a '/' collapses to its first-level prefix "a/".
        // Only "f.txt" (no delimiter) remains a Content.
        assert_eq!(contents, vec![entry("f.txt")]);
        assert_eq!(common, vec!["a/".to_string()]);
    }

    #[test]
    fn deeply_nested_collapses_to_one() {
        let entries = vec![entry("x/y/z/w/file.txt")];
        let (contents, common) = list_to_contents(&entries, "", Some("/"));
        let common = common.into_vec();
        assert!(contents.is_empty());
        // One CommonPrefix at the requested level, not one per nesting level.
        assert_eq!(common, vec!["x/".to_string()]);
    }

    #[test]
    fn prefix_filter() {
        let entries = vec![entry("foo/a.txt"), entry("bar/b.txt")];
        let (contents, _common) = list_to_contents(&entries, "foo/", None);
        assert_eq!(contents, vec![entry("foo/a.txt")]);
    }

    #[test]
    fn token_round_trip() {
        let key = "some/key/with/slashes";
        let tok = encode_token(key);
        assert_eq!(decode_token(&tok), Some(key.into()));
    }
}
