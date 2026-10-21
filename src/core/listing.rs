//! Listing translation logic: turning a flat list of HDFS paths into S3 `Contents` and
//! `CommonPrefixes`, plus continuation-token pagination. Pure logic, no `s3s` types.

use std::cmp::Ordering;
use std::sync::Arc;

/// A single object entry produced by listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
    pub modification_time: u64,
}

/// One directory's direct children, mapped to S3 keys, split by kind, and each array in
/// the order the listing emits them: `files` by key bytes, `dirs` by their output key
/// (`dir + "/"`, see [`cmp_dir_keys`]).
///
/// Splitting by kind is what makes paginated resumption exact: a file's output key is
/// its key, while a directory's output key (`dir + "/"`) can sort *after* sibling files
/// whose names extend the directory's (`a/b` vs `a/b.txt`: `/` 0x2F > `.` 0x2E). One
/// array ordered by raw keys therefore has no monotone "resume here" predicate; one
/// array per kind does.
#[derive(Debug, Clone, Default)]
pub struct DirListing {
    pub files: Arc<[ListEntry]>,
    pub dirs: Arc<[String]>,
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

/// Compute one page of a delimiter'd listing from file keys and, for the canonical
/// `/` delimiter, the DIRECT CHILDREN of the prefix's directory: `files` (file keys
/// under it) and `dirs` (directory keys under it).
///
/// For `/`, a listing's output is fully determined by one directory level, so the
/// non-recursive `dirs` are enough (and an EMPTY directory can surface as a
/// CommonPrefix without an extra RPC). For any other delimiter the first occurrence
/// can be arbitrarily deep in a key (`a/b.txt` with delimiter `.`), so `dirs` cannot
/// describe the collapse: callers pass every matching file key in the subtree and an
/// empty `dirs` slice. The S3 layer does exactly that.
///
/// Semantics:
/// - Contents are the file keys whose remainder after `prefix` contains no delimiter;
///   everything else collapses to a CommonPrefix (files included — e.g. prefix `t` and
///   key `t/ca` collapse to `t/`, because the remainder starts with `/`).
/// - With `/`, directories additionally collapse to the CommonPrefix that would contain
///   their keys; an EMPTY directory is a real CommonPrefix because the caller lists one
///   directory non-recursively and cannot know emptiness without an extra RPC per
///   directory — the very amplification that one-level path exists to avoid.
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

    // Directory keys only describe direct children, so they can be collapsed exactly
    // for `/` (where a direct child's key has no further `/` after the prefix). For
    // any other delimiter the first occurrence can be deeper than the directory name,
    // and callers pass the matching file keys instead (see the function docs).
    if delimiter == Some("/") {
        // Directories collapse to the CommonPrefix that would contain all of their
        // keys, mirroring `list_to_contents`' rule for file keys: everything up to and
        // including the first `/` after `prefix`; when the prefix is the directory's own
        // name (no trailing slash, e.g. prefix `t` and directory `ta`), that is `ta/`.
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

/// Compare a directory's output key (`dir + "/"`) with `other`, without allocating.
pub fn cmp_dir_output(dir: &str, other: &str) -> Ordering {
    let d = dir.as_bytes();
    let o = other.as_bytes();
    let common = d.len().min(o.len());
    match d[..common].cmp(&o[..common]) {
        // `d + "/"` is greater whenever `d` already exhausts `other`.
        Ordering::Equal if d.len() >= o.len() => Ordering::Greater,
        // `d` is a strict prefix of `other`: compare the appended `/` with its next byte.
        // If they are equal, `d + "/"` is a prefix of `other`; it is equal only when
        // `other` is exactly `d + "/"`, and otherwise sorts before it. Returning
        // `Equal` unconditionally here would compare `a/` as equal to `a/b`, breaking
        // the ordering assumptions of the `partition_point` bounds above.
        Ordering::Equal => match b'/'.cmp(&o[d.len()]) {
            Ordering::Equal if o.len() == d.len() + 1 => Ordering::Equal,
            Ordering::Equal => Ordering::Less,
            other => other,
        },
        other => other,
    }
}

/// Compare two directories' output keys (`a + "/"` vs `b + "/"`), without allocating.
///
/// This is NOT the same order as comparing `a` with `b`: sibling names can be string
/// prefixes of each other (`0` vs `0 `), and appending `/` then compares the delimiter
/// against the longer name's next byte, which can flip their order. Listing directories
/// must be stored in THIS order so that "first entry after the marker" is a monotone
/// predicate (see [`paginate_slash`]).
pub fn cmp_dir_keys(a: &str, b: &str) -> Ordering {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let common = a.len().min(b.len());
    match a[..common].cmp(&b[..common]) {
        Ordering::Equal if a.len() == b.len() => Ordering::Equal,
        // When the appended `/` matches the longer key's next byte, the shorter
        // output (`a + "/"`) is a strict prefix of the longer (`b + "/"`), so it
        // sorts first. The old code returned `Equal`, making this helper claim two
        // distinct directory outputs were equal.
        Ordering::Equal if a.len() < b.len() => match b'/'.cmp(&b[a.len()]) {
            Ordering::Equal => Ordering::Less,
            other => other,
        },
        Ordering::Equal => match a[b.len()].cmp(&b'/') {
            Ordering::Equal => Ordering::Greater,
            other => other,
        },
        other => other,
    }
}

/// Compare a file key with a directory's output key (`dir + "/"`), without allocating.
pub fn cmp_file_dir(file: &str, dir: &str) -> Ordering {
    cmp_dir_output(dir, file).reverse()
}

/// True when a directory's output key (`dir + "/"`) starts with `prefix`, without
/// allocating.
fn dir_output_starts_with(dir: &str, prefix: &str) -> bool {
    let d = dir.as_bytes();
    let p = prefix.as_bytes();
    if p.len() <= d.len() {
        d.starts_with(p)
    } else {
        p.len() == d.len() + 1 && p.starts_with(d) && p[d.len()] == b'/'
    }
}

/// Paginate a delimiter'd listing for the canonical `/` delimiter from per-kind arrays
/// already sorted by key, in `O(log n + page)` instead of `O(n log n)`.
///
/// Equivalent to [`paginate`] with `delimiter = Some("/")` **for the inputs the S3 layer
/// passes**: the direct children of the prefix's directory. That restriction is what
/// makes the collapse exactly `dir + "/"` (a direct child's key has no `/` in its
/// remainder, so `rest.find(delimiter)` can never fire) and therefore what makes the two
/// per-kind key orders monotone:
///
/// - files stream: output key == `entry.key`, sorted by key;
/// - directories stream: output key == `dir + "/"`, sorted by [`cmp_dir_keys`] (which is
///   not raw key order: sibling names can be prefixes of each other);
/// - the prefix range and the `marker` bound are contiguous lower bounds in each stream,
///   found with `partition_point`, and the two streams are merged as they are emitted.
///
/// Non-`/` delimiters keep using [`paginate`], whose collapse depends on the prefix.
pub fn paginate_slash(
    files: &[ListEntry],
    dirs: &[String],
    prefix: &str,
    marker: Option<&str>,
    max_keys: usize,
) -> Page {
    if max_keys == 0 {
        return Page {
            contents: Vec::new(),
            common_prefixes: Vec::new(),
            is_truncated: false,
            next_token: None,
        };
    }

    // Lower bounds: the prefix range start, then the continuation marker (strictly
    // greater, matching S3).
    let mut fi = files.partition_point(|f| f.key.as_str() < prefix);
    // Lower bound for the prefix range: FIRST output >= prefix. `!starts_with` is not
    // monotone (entries after the range do not start with it either), so compare
    // against the prefix itself.
    let mut di = dirs.partition_point(|d| cmp_dir_output(d, prefix) == Ordering::Less);
    if let Some(marker) = marker {
        fi = fi.max(files.partition_point(|f| f.key.as_bytes() <= marker.as_bytes()));
        di = di.max(dirs.partition_point(|d| cmp_dir_output(d, marker) != Ordering::Greater));
    }

    let mut contents: Vec<ListEntry> = Vec::new();
    let mut common_prefixes: Vec<String> = Vec::new();
    let mut last_key: Option<String> = None;
    let mut is_truncated = false;

    loop {
        // Drop candidates outside the prefix range. The range is contiguous in each
        // sorted stream: below it we skip, above it the stream is finished.
        while fi < files.len() && !files[fi].key.starts_with(prefix) {
            if files[fi].key.as_str() > prefix {
                fi = files.len();
            } else {
                fi += 1;
            }
        }
        while di < dirs.len() && !dir_output_starts_with(&dirs[di], prefix) {
            if cmp_dir_output(&dirs[di], prefix) == Ordering::Greater {
                di = dirs.len();
            } else {
                di += 1;
            }
        }

        let next_file = files.get(fi);
        let next_dir = dirs.get(di);
        if next_file.is_none() && next_dir.is_none() {
            break;
        }
        if contents.len() + common_prefixes.len() == max_keys {
            is_truncated = true;
            break;
        }

        // Merge the two sorted streams by output key.
        let take_file = match (next_file, next_dir) {
            (Some(file), Some(dir)) => cmp_dir_output(dir, &file.key) == Ordering::Greater,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => unreachable!(),
        };
        if take_file {
            let file = files[fi].clone();
            fi += 1;
            last_key = Some(file.key.clone());
            contents.push(file);
        } else {
            let output = format!("{}/", dirs[di]);
            di += 1;
            last_key = Some(output.clone());
            common_prefixes.push(output);
        }
    }

    Page {
        contents,
        common_prefixes,
        is_truncated,
        next_token: if is_truncated { last_key } else { None },
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
    fn directory_output_comparators_handle_prefix_boundaries() {
        // `dir + "/"` vs an arbitrary key: `a/` is a prefix of `a/b`, so it sorts
        // before it (not equal). It is equal only to the exact output `a/`.
        assert_eq!(cmp_dir_output("a", "a/"), Ordering::Equal);
        assert_eq!(cmp_dir_output("a", "a/b"), Ordering::Less);
        assert_eq!(cmp_dir_output("a", "a!"), Ordering::Greater); // '/' > '!'
        assert_eq!(cmp_dir_output("a", "a0"), Ordering::Less); // '/' < '0'

        // Two directory outputs: `a/` is a prefix of `a/b/`, so `a` sorts first.
        assert_eq!(cmp_dir_keys("a", "a/b"), Ordering::Less);
        assert_eq!(cmp_dir_keys("a/b", "a"), Ordering::Greater);
        assert_eq!(cmp_dir_keys("a", "a!"), Ordering::Greater); // "a/" > "a!/"
        assert_eq!(cmp_dir_keys("a", "a"), Ordering::Equal);
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
    fn non_slash_delimiter_uses_file_keys_not_directories() {
        // A directory's direct-child key is not enough to collapse for a non-"/"
        // delimiter: the delimiter can occur deeper (`a/b.txt` with "."). The caller
        // supplies the matching file keys and no directories; the directory entry must
        // not leak a bogus `/`-collapsed prefix.
        let files = vec![entry("a/b.txt")];
        let dirs = vec!["a".to_string()];
        let page = paginate(&files, &dirs, "", Some("."), None, 100);
        assert!(page.contents.is_empty());
        assert_eq!(page.common_prefixes, vec!["a/b.".to_string()]);
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

    /// The S3 directory part of a prefix (`""` for the bucket root).
    fn dir_part(prefix: &str) -> &str {
        prefix.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("")
    }

    /// Keep only the entries that are DIRECT children of the prefix's directory — the
    /// exact shape the S3 layer feeds both paginators.
    fn direct_children(
        files: &[ListEntry],
        dirs: &[String],
        prefix: &str,
    ) -> (Vec<ListEntry>, Vec<String>) {
        let parent = dir_part(prefix);
        let keep = |key: &str| match key.rsplit_once('/') {
            Some((dir, _)) => dir == parent,
            None => parent.is_empty(),
        };
        let files: Vec<ListEntry> = files.iter().filter(|f| keep(&f.key)).cloned().collect();
        let mut dirs: Vec<String> = dirs.iter().filter(|d| keep(d)).cloned().collect();
        // The paginators require their documented input order.
        dirs.sort_by(|a, b| cmp_dir_keys(a, b));
        (files, dirs)
    }

    /// Walk a delimiter'd listing with the production paginator (`paginate_slash` for
    /// `/`, the general `paginate` otherwise), asserting order/uniqueness/termination.
    fn walk_slash_pages(
        files: &[ListEntry],
        dirs: &[String],
        prefix: &str,
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
                "pagination must terminate"
            );
            let page = paginate_slash(files, dirs, prefix, marker.as_deref(), max_keys);
            let keys = page_keys(&page);
            if page.is_truncated {
                assert!(page.next_token.is_some());
                assert!(!keys.is_empty());
            }
            for key in &keys {
                if let Some(prev) = out.last() {
                    assert!(prev.as_str() < key.as_str(), "{prev:?} then {key:?}");
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
    fn paginate_slash_matches_general_paginate() {
        // `paginate_slash` is the O(log n + page) fast path used for the canonical `/`
        // delimiter. It must agree page-for-page with the general paginator on the
        // inputs the S3 layer actually produces (the direct children of the prefix's
        // directory) — including sibling names that are prefixes of each other
        // (`0`, `0 `), where `dir + "/"` does not preserve raw-key order.
        let (files, dirs) = adversarial_namespace(3, 150);
        for prefix in ["", "a", "a/", "a.b", "!", "0"] {
            let (files, dirs) = direct_children(&files, &dirs, prefix);
            let one = page_keys(&paginate_slash(&files, &dirs, prefix, None, usize::MAX));
            for max_keys in [1, 2, 3, 5, 7] {
                let walked = walk_slash_pages(&files, &dirs, prefix, None, max_keys);
                assert_eq!(walked, one, "prefix={prefix:?} max_keys={max_keys}");
            }
            for marker in [
                None,
                Some("a"),
                Some("a/"),
                Some("0"),
                Some("0 "),
                Some("0."),
            ] {
                let expected: Vec<String> = one
                    .iter()
                    .filter(|k| marker.map_or(true, |m: &str| k.as_str() > m))
                    .cloned()
                    .collect();
                for max_keys in [1, 2, 4] {
                    let walked = walk_slash_pages(&files, &dirs, prefix, marker, max_keys);
                    assert_eq!(
                        walked, expected,
                        "prefix={prefix:?} marker={marker:?} max_keys={max_keys}"
                    );
                }

                // Agree with the general paginator page by page, including which keys
                // land on which page and the continuation token.
                let mut marker = marker.map(str::to_string);
                loop {
                    let fast = paginate_slash(&files, &dirs, prefix, marker.as_deref(), 3);
                    let general = paginate(&files, &dirs, prefix, Some("/"), marker.as_deref(), 3);
                    assert_eq!(page_keys(&fast), page_keys(&general), "prefix={prefix:?}");
                    assert_eq!(fast.is_truncated, general.is_truncated);
                    assert_eq!(fast.next_token, general.next_token);
                    match fast.next_token {
                        Some(token) => marker = Some(token),
                        None => break,
                    }
                }
            }
        }
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
