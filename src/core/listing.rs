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

/// The result of paginating a listing: the page's Contents and CommonPrefixes (both in
/// strict key order), whether more keys remain, and the raw last key of the page to resume
/// from (the S3 layer base64-encodes it into the continuation token).
pub struct Page {
    /// Contents that fall on this page (already key-sorted).
    pub contents: Vec<ListEntry>,
    /// CommonPrefixes that fall on this page (already key-sorted).
    pub common_prefixes: Vec<String>,
    /// Whether more keys exist beyond this page.
    pub is_truncated: bool,
    /// The last key of this page (a Content or a CommonPrefix), used to resume. `Some`
    /// exactly when `is_truncated`.
    pub next_token: Option<String>,
}

/// Compute one page of a delimiter'd listing from the DIRECT CHILDREN of the prefix's
/// directory: `files` (file keys under it) and `dirs` (directory keys under it).
///
/// This is the pure counterpart of the S3 layer's single non-recursive `getListing`:
/// with a delimiter, a listing's output is fully determined by one directory level, so
/// nothing deeper is ever needed (a recursive walk would cost one RPC per directory of
/// the subtree for zero extra information).
///
/// Semantics:
/// - Contents are the file keys whose remainder after `prefix` contains no delimiter;
///   everything else collapses to a CommonPrefix (files included — e.g. prefix `t` and
///   key `t/ca` collapse to `t/`, because the remainder starts with `/`).
/// - Directories always collapse to the CommonPrefix that would contain their keys;
///   an EMPTY directory is still a real CommonPrefix here, because the caller lists one
///   directory non-recursively and cannot know emptiness without an extra RPC per
///   directory — the very amplification this one-level path exists to avoid.
/// - Directories whose key does not start with `prefix` can never contain a matching
///   key and are ignored.
/// - Pagination interleaves Contents and CommonPrefixes in strict key order; the
///   `marker` (decoded continuation token or `start_after`) filters BOTH (strictly
///   greater). Filtering only Contents would re-serve CommonPrefixes ≤ the marker when
///   a page is cut inside a run of them, looping the client on the same page forever —
///   each iteration re-triggering the HDFS listing.
pub fn paginate(
    files: &[ListEntry],
    dirs: &[String],
    prefix: &str,
    delimiter: Option<&str>,
    marker: Option<&str>,
    max_keys: usize,
) -> Page {
    // max-keys=0 asks for no keys: the page is empty and NOT truncated. Reporting
    // truncation would be self-contradictory (there is no last key to mint a token
    // from) and contradicts S3, whose reference tests assert IsTruncated=false for
    // MaxKeys=0.
    if max_keys == 0 {
        return Page {
            contents: Vec::new(),
            common_prefixes: Vec::new(),
            is_truncated: false,
            next_token: None,
        };
    }

    let (mut contents, common) = list_to_contents(files, prefix, delimiter);
    let mut common_vec = common.into_vec();

    // Directories collapse to the CommonPrefix that would contain all of their keys,
    // mirroring `list_to_contents`' rule for file keys: everything up to and including
    // the first delimiter after `prefix`; when the prefix is the directory's own name
    // (no trailing slash, e.g. prefix `t` and directory `ta`), that is `ta/`.
    for dir in dirs {
        if !dir.starts_with(prefix) {
            continue; // nothing under a non-matching directory can match the prefix
        }
        let rest = &dir[prefix.len()..];
        let common_prefix = match rest.find('/') {
            Some(i) => format!("{prefix}{}", &rest[..i + 1]),
            None => format!("{dir}/"),
        };
        common_vec.push(common_prefix);
    }
    common_vec.sort();
    common_vec.dedup();

    // Strict key order (S3 guarantee).
    contents.sort_by(|a, b| a.key.as_bytes().cmp(b.key.as_bytes()));

    if let Some(marker) = marker {
        contents.retain(|e| e.key.as_str() > marker);
        common_vec.retain(|p| p.as_str() > marker);
    }

    // Interleave Contents and CommonPrefixes in key order for pagination.
    let mut all_keys: Vec<String> = contents.iter().map(|e| e.key.clone()).collect();
    all_keys.extend(common_vec.iter().cloned());
    all_keys.sort();

    let take = max_keys.min(all_keys.len());
    let page: Vec<String> = all_keys[..take].to_vec();
    let is_truncated = take < all_keys.len();

    let page_set: std::collections::HashSet<&String> = page.iter().collect();
    let contents = contents
        .into_iter()
        .filter(|e| page_set.contains(&e.key))
        .collect();
    let common_prefixes = common_vec
        .into_iter()
        .filter(|p| page_set.contains(p))
        .collect();
    let next_token = if is_truncated {
        page.last().cloned()
    } else {
        None
    };

    Page {
        contents,
        common_prefixes,
        is_truncated,
        next_token,
    }
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

    // --- paginate: one-level delimiter pages ------------------------------------------

    /// Walk every page of a delimiter'd listing the way a client paginator does,
    /// asserting that pagination terminates and yields no duplicate prefixes.
    fn walk_pages(
        files: &[ListEntry],
        dirs: &[String],
        prefix: &str,
        max_keys: usize,
    ) -> Vec<String> {
        let mut all: Vec<String> = Vec::new();
        let mut marker: Option<String> = None;
        let mut pages = 0;
        loop {
            pages += 1;
            assert!(pages <= 16, "pagination must terminate (infinite loop?)");
            let page = paginate(files, dirs, prefix, Some("/"), marker.as_deref(), max_keys);
            for c in &page.common_prefixes {
                assert!(
                    !all.contains(c),
                    "duplicate common prefix on page {pages}: {c}"
                );
            }
            all.extend(page.common_prefixes.iter().cloned());
            match page.next_token {
                Some(t) => marker = Some(t),
                None => break,
            }
        }
        all
    }

    #[test]
    fn paginate_more_prefixes_than_max_keys_terminates() {
        // THE regression: with a delimiter and more CommonPrefixes than max_keys, the
        // continuation marker used to be applied to Contents only, so every page
        // re-served the same prefixes and a client paginator looped forever (each
        // iteration re-triggering the HDFS listing). Filtering CommonPrefixes by the
        // marker too makes the walk advance page by page.
        let dirs: Vec<String> = (1..=5).map(|i| format!("a/part{i}")).collect();
        let all = walk_pages(&[], &dirs, "a/", 2);
        assert_eq!(
            all,
            vec![
                "a/part1/".to_string(),
                "a/part2/".to_string(),
                "a/part3/".to_string(),
                "a/part4/".to_string(),
                "a/part5/".to_string(),
            ]
        );
    }

    #[test]
    fn paginate_mixed_contents_and_prefixes_page_advances() {
        let files = vec![entry("a/x.txt"), entry("a/z.txt")];
        let dirs = vec!["a/m".to_string(), "a/y".to_string()];
        // Sorted interleave of everything under `a/`: a/m/, a/x.txt, a/y/, a/z.txt.
        let page1 = paginate(&files, &dirs, "a/", Some("/"), None, 2);
        assert_eq!(page1.contents, vec![entry("a/x.txt")]);
        assert_eq!(page1.common_prefixes, vec!["a/m/".to_string()]);
        assert!(page1.is_truncated);
        let marker = page1.next_token.clone().unwrap();
        assert_eq!(marker, "a/x.txt");
        // Resuming must skip BOTH the old contents and the old prefixes.
        let page2 = paginate(&files, &dirs, "a/", Some("/"), Some(&marker), 2);
        assert_eq!(page2.contents, vec![entry("a/z.txt")]);
        assert_eq!(page2.common_prefixes, vec!["a/y/".to_string()]);
        assert!(!page2.is_truncated);
        assert!(page2.next_token.is_none());
    }

    #[test]
    fn paginate_empty_dir_is_a_common_prefix() {
        // An empty HDFS directory is surfaced as a CommonPrefix: the one-level listing
        // cannot know it is empty without an extra RPC per directory.
        let files: Vec<ListEntry> = Vec::new();
        let dirs = vec!["a/empty".to_string()];
        let page = paginate(&files, &dirs, "a/", Some("/"), None, 100);
        assert!(page.contents.is_empty());
        assert_eq!(page.common_prefixes, vec!["a/empty/".to_string()]);
        assert!(!page.is_truncated);
    }

    #[test]
    fn paginate_ignores_dirs_outside_prefix() {
        let files: Vec<ListEntry> = vec![entry("a/x.txt")];
        let dirs = vec!["a/keep".to_string(), "b/other".to_string()];
        let page = paginate(&files, &dirs, "a/", Some("/"), None, 100);
        assert_eq!(page.contents, vec![entry("a/x.txt")]);
        assert_eq!(page.common_prefixes, vec!["a/keep/".to_string()]);
    }

    #[test]
    fn paginate_slashless_prefix_collapses() {
        // Prefix without '/': matching keys may live at any depth, so the collapse rule
        // applies to the remainder after the prefix. A file `t/ca` collapses into `t/`
        // (its remainder starts with '/'), a directory `ta` collapses into `ta/`, and
        // `t.txt` (remainder `.txt`) is a Content.
        let files = vec![entry("t/ca"), entry("t.txt")];
        let dirs = vec!["ta".to_string()];
        let page = paginate(&files, &dirs, "t", Some("/"), None, 100);
        assert_eq!(page.contents, vec![entry("t.txt")]);
        assert_eq!(
            page.common_prefixes,
            vec!["t/".to_string(), "ta/".to_string()]
        );
    }

    #[test]
    fn paginate_marker_is_strictly_greater() {
        // '.' (0x2E) sorts before '/' (0x2F): a file `a/sub.txt` sorts BEFORE the
        // CommonPrefix `a/sub/`. A marker of `a/sub/` must therefore drop `a/sub.txt`.
        let files = vec![entry("a/sub.txt"), entry("a/sub/x.txt")];
        let dirs = vec!["a/sub".to_string()];
        let page = paginate(&files, &dirs, "a/", Some("/"), Some("a/sub/"), 100);
        assert!(
            page.contents.is_empty(),
            "contents must be empty: {:?}",
            page.contents
        );
        assert!(
            page.common_prefixes.is_empty(),
            "prefixes must be empty: {:?}",
            page.common_prefixes
        );
    }

    /// Deterministic adversarial namespace: names deliberately use bytes that sort
    /// immediately around the delimiter (`!`, `.`, `/`, `0`, space, UTF-8), so any
    /// algorithm that assumes "the collapsed CommonPrefix of a directory sorts next to
    /// the directory's own key" or "a key <= marker implies its children are too" fails
    /// here — e.g. directory `a/b` (CommonPrefix `a/b/`) next to file `a/b.txt`
    /// (`.` 0x2E < `/` 0x2F), or file `a/b!x` between them.
    fn adversarial_namespace(seed: u64, n: usize) -> (Vec<ListEntry>, Vec<String>) {
        let alphabet = ["!", ".", "0", "a", "b", "e", "_", "~", " "];
        let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let mut rnd = |m: usize| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 33) as usize) % m
        };
        let mut keys = std::collections::BTreeSet::new();
        while keys.len() < n {
            let segments = 1 + rnd(3);
            let mut key = String::new();
            for i in 0..segments {
                if i > 0 {
                    key.push('/');
                }
                let len = 1 + rnd(3);
                for _ in 0..len {
                    key.push_str(alphabet[rnd(alphabet.len())]);
                }
            }
            // HDFS forbids "." and ".." as path components (and `hdfs_path_to_key`
            // would normalize them away, collapsing two distinct fake entries).
            if key.split('/').any(|s| s == "." || s == "..") {
                continue;
            }
            keys.insert(key);
        }
        // A real namespace cannot contain both file `k` and a directory `k`: drop
        // candidate keys that are extended by another key at a `/` boundary.
        let key_list: Vec<String> = keys.iter().cloned().collect();
        let is_dir_key: std::collections::HashSet<&str> = key_list
            .iter()
            .filter(|k| {
                key_list.iter().any(|other| {
                    other.len() > k.len()
                        && other.starts_with(k.as_str())
                        && other.as_bytes()[k.len()] == b'/'
                })
            })
            .map(String::as_str)
            .collect();
        keys.retain(|k| !is_dir_key.contains(k.as_str()));

        let files: Vec<ListEntry> = keys.iter().map(|k| entry(k)).collect();
        let mut dirs: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for key in &keys {
            let mut parts: Vec<&str> = key.split('/').collect();
            while parts.len() > 1 {
                parts.pop();
                dirs.insert(parts.join("/"));
            }
        }
        // A couple of empty directories: they must surface as CommonPrefixes.
        dirs.insert(format!("{}/empty", keys.iter().next().unwrap()));
        (files, dirs.into_iter().collect())
    }

    /// The keys of one page in output order: Contents and CommonPrefixes are each
    /// sorted, so merging them by key recovers the page's global order.
    fn page_keys(page: &Page) -> Vec<String> {
        let mut keys: Vec<String> = page
            .contents
            .iter()
            .map(|e| e.key.clone())
            .chain(page.common_prefixes.iter().cloned())
            .collect();
        keys.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        keys
    }

    /// Walk every page the way a client paginator does, asserting the S3 guarantees
    /// that do not depend on concurrent writers: strictly increasing keys, each key
    /// exactly once, a token on every truncated page, and termination.
    fn walk_all_pages(
        files: &[ListEntry],
        dirs: &[String],
        prefix: &str,
        delimiter: Option<&str>,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let mut marker = start_after.map(str::to_string);
        let mut pages = 0usize;
        loop {
            pages += 1;
            assert!(
                pages <= files.len() + dirs.len() + 2,
                "pagination must terminate (prefix={prefix:?} delimiter={delimiter:?} max_keys={max_keys} start={start_after:?})"
            );
            let page = paginate(files, dirs, prefix, delimiter, marker.as_deref(), max_keys);
            let keys = page_keys(&page);
            if page.is_truncated {
                assert!(
                    page.next_token.is_some(),
                    "a truncated page must carry a continuation token"
                );
                assert!(!keys.is_empty(), "a truncated page must not be empty");
            }
            for key in &keys {
                if let Some(prev) = out.last() {
                    assert!(
                        prev.as_str() < key.as_str(),
                        "keys must be strictly increasing: {prev:?} then {key:?}"
                    );
                }
                out.push(key.clone());
            }
            match page.next_token {
                Some(token) => marker = Some(token),
                None => break,
            }
        }
        out
    }

    #[test]
    fn paginate_page_size_invariance() {
        // The whole listing must be identical whether it is served in one page or
        // walked with any page size, from any start_after marker. This is the safety
        // net for any "resume by binary search" optimisation: a page boundary must
        // never skip or duplicate a key that the single-page listing contains.
        let (files, dirs) = adversarial_namespace(7, 120);
        for prefix in ["", "a", "a/", "a.b", "!", "\u{e9}"] {
            for delimiter in [Some("/"), Some(".")] {
                let full = page_keys(&paginate(
                    &files,
                    &dirs,
                    prefix,
                    delimiter,
                    None,
                    usize::MAX,
                ));
                for max_keys in [1, 2, 3, 5, 7] {
                    let walked = walk_all_pages(&files, &dirs, prefix, delimiter, None, max_keys);
                    assert_eq!(
                        walked, full,
                        "prefix={prefix:?} delimiter={delimiter:?} max_keys={max_keys}"
                    );
                }
                // Resuming from an arbitrary client-supplied start_after must yield
                // exactly the suffix of the full listing that sorts after it.
                for marker in ["a", "a/", "a/b", "a/b.", "a/0", "!"] {
                    let expected: Vec<String> = full
                        .iter()
                        .filter(|k| k.as_str() > marker)
                        .cloned()
                        .collect();
                    let walked = walk_all_pages(&files, &dirs, prefix, delimiter, Some(marker), 2);
                    assert_eq!(walked, expected, "prefix={prefix:?} marker={marker:?}");
                }
            }
        }
    }

    #[test]
    fn paginate_max_keys_zero_is_not_truncated() {
        // max-keys=0 asks for no keys: an empty page that is NOT truncated (S3's
        // reference tests assert IsTruncated=false for MaxKeys=0). Reporting truncation
        // here used to produce `IsTruncated=true` with no continuation token, a page a
        // client cannot advance past.
        let files = vec![entry("a/x.txt")];
        let dirs = vec!["a/d".to_string()];
        let page = paginate(&files, &dirs, "a/", Some("/"), None, 0);
        assert!(page.contents.is_empty());
        assert!(page.common_prefixes.is_empty());
        assert!(!page.is_truncated);
        assert!(page.next_token.is_none());
    }
}
