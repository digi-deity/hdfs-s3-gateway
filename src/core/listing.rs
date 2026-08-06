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
    // S3 semantics: an empty delimiter means "no grouping" — treat it as `None`
    // so keys are returned as Contents rather than collapsing every key into the
    // prefix itself (which `rest.find("")` would otherwise do at index 0). This
    // is defense in depth: the S3 layer also filters empty delimiters.
    let delimiter = delimiter.filter(|d| !d.is_empty());

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
                let common_prefix = format!("{prefix}{}", &rest[..idx + delim.len()]);
                common.insert(common_prefix);
                continue;
            }
        }
        contents.push(entry.clone());
    }

    (contents, common)
}

/// Encode a continuation token: URL-safe base64 (no padding) of the last returned key.
///
/// NOTE: this is **not an authenticated token**. It is trivially decodable (it
/// reveals the last returned key to anyone) and forgeable (any string encodes to
/// a valid token, letting a client jump anywhere in the key space). It exists
/// purely for pagination continuity — nothing security-sensitive may ever rely
/// on "the client must have seen page N-1 to see page N".
pub fn encode_token(last_key: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(last_key)
}

/// Decode a continuation token back into the resume key.
///
/// Accepts both the current URL-safe unpadded form and legacy standard base64
/// (padded) tokens, so clients that captured a token before a format change keep
/// working. Returns `None` for anything that is not valid base64 (or does not
/// decode to UTF-8); callers map that to an S3 `InvalidToken` error rather than
/// silently restarting pagination.
pub fn decode_token(token: &str) -> Option<String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(token))
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
    fn prefix_with_delimiter() {
        // The most common real-world shape: `prefix=a/&delimiter=/`. Keys under
        // the prefix split into direct Contents and one-level CommonPrefixes;
        // keys outside the prefix are ignored entirely.
        let entries = vec![
            entry("a/b.txt"),
            entry("a/c/d.txt"),
            entry("a/c/e.txt"),
            entry("ab/x.txt"), // shares the leading 'a' but is NOT under 'a/'
        ];
        let (contents, common) = list_to_contents(&entries, "a/", Some("/"));
        assert_eq!(contents, vec![entry("a/b.txt")]);
        assert_eq!(common.into_vec(), vec!["a/c/".to_string()]);
    }

    #[test]
    fn empty_delimiter_is_no_grouping() {
        // `delimiter=""` must behave exactly like no delimiter: every key under
        // the prefix is a Content, nothing collapses into a CommonPrefix.
        let entries = vec![entry("a/b.txt"), entry("a/c/d.txt")];
        let (contents, common) = list_to_contents(&entries, "a/", Some(""));
        assert_eq!(contents, vec![entry("a/b.txt"), entry("a/c/d.txt")]);
        assert!(common.into_vec().is_empty());
    }

    #[test]
    fn token_round_trip() {
        let key = "some/key/with/slashes";
        let tok = encode_token(key);
        assert_eq!(decode_token(&tok), Some(key.into()));
        // URL-safe unpadded: never contains `+`, `/`, or `=` (transport-safe in
        // URLs, query strings, and XML).
        assert!(
            !tok.contains(['+', '/', '=']),
            "token must be URL-safe: {tok}"
        );
    }

    #[test]
    fn legacy_standard_token_still_decodes() {
        // Tokens minted before the switch to URL-safe base64 decode too.
        use base64::Engine as _;
        let key = "some/key/with/slashes";
        let legacy = base64::engine::general_purpose::STANDARD.encode(key);
        assert_eq!(decode_token(&legacy), Some(key.into()));
    }

    #[test]
    fn invalid_token_is_none() {
        // Corrupted / truncated / garbage tokens must not silently decode to a
        // key (the S3 layer maps `None` to `InvalidToken`, never to "first page").
        assert_eq!(decode_token("%%%not-base64%%%"), None);
        assert_eq!(decode_token("a"), None); // 1-char final quantum: invalid base64
        assert_eq!(decode_token(""), Some(String::new())); // empty key encodes to empty
    }
}
