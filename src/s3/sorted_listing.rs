//! Lazy, sorted, k-way merge over per-directory HDFS listings.
//!
//! This is the no-delimiter (flat) listing engine: instead of recursively walking
//! the whole requested subtree (one `getListing` RPC per directory, materializing
//! every entry in memory) and *then* sorting and paging, it merges per-directory
//! listings **lazily in key order**. Only the directories that sort before the
//! current page boundary are ever opened, and the memory footprint is the merge
//! frontier (one cursor per opened directory), never the whole subtree.
//!
//! Correctness contract (equivalences with the old walk-everything behaviour):
//!
//! - The emitted keys are exactly the files under the start directory whose key
//!   matches `prefix`, in strict UTF-8 binary order, each exactly once — except
//!   subtrees that cannot be listed: a directory that vanished mid-walk or that
//!   the gateway's HDFS user has no permission to read is skipped (best effort;
//!   the rest of the listing is still served, see `next_entry`).
//! - A continuation `after` marker skips every key `<= after` (byte order), the
//!   same rule the old code applied via `contents.retain(|e| &e.key > marker)`.
//! - Directories are never emitted as objects; they are only descended into.
//! - On a resumed page, a directory whose entire key range sorts before the
//!   `after` marker is never opened: every child of `dir` has key
//!   `dir + "/" + name`, so `dir + "/" < after` (with `after` not living under
//!   `dir/`) proves no child can exceed the marker. The marker's own directory
//!   and its ancestors are still re-walked — their children straddle the marker.
//! - `max_keys` bounds the returned page; `is_truncated` reports whether at least
//!   one more key exists after the page.
//!
//! Why a merge and not a bounded walk? hdfs-native's recursive `list_status`
//! yields directories in depth-first pre-order, which is *unsorted* globally, so a
//! walk cannot stop early without missing keys that sort before what was collected.
//! The NameNode *does* return each directory's entries sorted by name, so a lazy
//! k-way merge over per-directory listings can stop exactly at the page boundary.
//!
//! Sorting detail: HDFS sorts directory entries by Java `String` order, which
//! differs from UTF-8 byte order for astral-plane characters (e.g. emoji keys:
//! Java compares UTF-16 code units, S3 compares UTF-8 bytes). S3 keys must be in
//! strict UTF-8 binary order, so every directory's entries are re-sorted by raw
//! path bytes when the cursor is pushed; the heap then merges those sorted runs.
//! This reproduces the old implementation's global `as_bytes()` sort exactly.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

use futures::future::BoxFuture;
use hdfs_native::client::FileStatus;
use hdfs_native::{Client, HdfsError};
use tracing::Instrument;

use crate::core::{cmp_dir_keys, cmp_dir_output, cmp_file_dir, DirListing, ListEntry, PathMapper};
use crate::s3::error::is_access_denied;

/// True when `err` means "the gateway's HDFS user has no permission to read this
/// specific directory": the NameNode rejected the `getListing` RPC with an
/// access-control exception class (POSIX/ACL denial on the directory).
///
/// Only RPC-level access-control denials qualify. SASL/GSSAPI failures are NOT
/// included: they mean the gateway cannot authenticate to the cluster at all —
/// that is not per-directory, and swallowing it would turn a broken-auth cluster
/// into silently truncated listings.
fn is_dir_denied(err: &HdfsError) -> bool {
    matches!(
        err,
        HdfsError::RPCError(exception, _) | HdfsError::FatalRPCError(exception, _)
            if is_access_denied(exception)
    )
}

/// Abstraction over "list one directory, non-recursively", so the merge can be
/// unit-tested against an in-memory fake instead of a live cluster.
pub trait DirLister: Send + Sync {
    /// Non-recursive listing of `dir` (its direct children), already mapped to S3 keys,
    /// split by kind and sorted by key bytes ([`DirListing`]). Mapping and sorting here,
    /// once per directory listing, is what lets pagination resume with a binary search
    /// instead of re-scanning the directory on every page.
    fn list_dir(&self, dir: String) -> BoxFuture<'static, Result<Arc<DirListing>, HdfsError>>;
}

/// The production lister: one non-recursive `getListing` RPC per directory.
///
/// Unlike `list_status(.., true)` (one RPC per directory of the whole subtree),
/// this bounds the work to the directories the merge actually opens.
pub struct HdfsDirLister {
    client: Arc<Client>,
    mapper: PathMapper,
    cache: Arc<crate::s3::cache::Caches>,
    span: tracing::Span,
}

impl HdfsDirLister {
    /// `span` is the request log span; upstream diagnostics stay correlated to the
    /// request that triggered them (same pattern as the other gateway call sites).
    pub fn new(
        client: Arc<Client>,
        mapper: PathMapper,
        cache: Arc<crate::s3::cache::Caches>,
        span: tracing::Span,
    ) -> Self {
        HdfsDirLister {
            client,
            mapper,
            cache,
            span,
        }
    }
}

impl DirLister for HdfsDirLister {
    fn list_dir(&self, dir: String) -> BoxFuture<'static, Result<Arc<DirListing>, HdfsError>> {
        let client = self.client.clone();
        let mapper = self.mapper.clone();
        let cache = Arc::clone(&self.cache);
        let span = self.span.clone();
        Box::pin(async move {
            if let Some(listing) = cache.dirs.get(&dir) {
                return Ok(listing);
            }
            let statuses = client.list_status(&dir, false).instrument(span).await?;
            let listing = Arc::new(listing_from_statuses(statuses, &mapper));
            // Remember the listing for a few seconds. Oversized directories are served
            // but never cached: one such listing would evict everything else and still
            // exceed the budget (a scan must not be able to flush the cache).
            let weight = listing.files.len() + listing.dirs.len();
            if weight <= cache.max_dir_entries {
                cache.dirs.insert(dir, Arc::clone(&listing), weight);
            }
            Ok(listing)
        })
    }
}

/// Map one directory's raw `FileStatus` children into a [`DirListing`] (S3-key space,
/// split by kind, sorted by key bytes). Entries outside the configured root are dropped:
/// they cannot be addressed by any S3 key.
pub(crate) fn listing_from_statuses(statuses: Vec<FileStatus>, mapper: &PathMapper) -> DirListing {
    let mut files = Vec::with_capacity(statuses.len());
    let mut dirs = Vec::new();
    for status in statuses {
        let Some(key) = mapper.hdfs_path_to_key(&status.path) else {
            continue;
        };
        // The root itself (a listing of `/`) has the empty key: not an object.
        if key.is_empty() {
            continue;
        }
        if status.isdir {
            dirs.push(key);
        } else {
            files.push(ListEntry {
                key,
                size: status.length as u64,
                modification_time: status.modification_time,
            });
        }
    }
    files.sort_by(|a, b| a.key.as_bytes().cmp(b.key.as_bytes()));
    // Directories are ordered by their OUTPUT key (`dir + "/"`), not their raw key:
    // sibling names can be prefixes of each other (`0` vs `0 `), which flips the
    // comparison once the delimiter is appended. Output order is what makes the
    // pagination bounds below monotone.
    dirs.sort_by(|a, b| cmp_dir_keys(a, b));
    DirListing {
        files: files.into(),
        dirs: dirs.into(),
    }
}

/// A cursor over one directory's files or directories, positioned at its next
/// un-emitted entry.
///
/// The two kinds get separate cursors because their output keys order differently: a
/// file's key is its own, while a directory's emitted key is `dir + "/"`, which sorts
/// *after* sibling files that extend the directory's name (`a/b` vs `a/b.txt`). Resuming
/// a single raw-key-ordered cursor by binary search would therefore skip directories
/// whose children sort after a sibling file; per-kind cursors have monotone predicates
/// (see `dir_children_all_before`).
struct Cursor {
    listing: Arc<DirListing>,
    files: bool,
    idx: usize,
}

impl Cursor {
    fn len(&self) -> usize {
        if self.files {
            self.listing.files.len()
        } else {
            self.listing.dirs.len()
        }
    }

    fn front(&self) -> &str {
        // Invariant: a cursor is only pushed with `idx < len` and dropped when exhausted.
        if self.files {
            &self.listing.files[self.idx].key
        } else {
            &self.listing.dirs[self.idx]
        }
    }

    /// Advance to the next entry; returns whether the cursor is still non-empty.
    fn advance(&mut self) -> bool {
        self.idx += 1;
        self.idx < self.len()
    }
}

impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        self.front() == other.front()
    }
}

impl Eq for Cursor {}

impl PartialOrd for Cursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cursor {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare each cursor's OUTPUT key: a file's key is its own; a directory's
        // smallest descendant key is `dir + "/"`. Sibling directory names can be
        // prefixes of each other, so this is not raw byte order (see `cmp_dir_keys`).
        // Keys are unique within one listing, so this is a total order.
        match (self.files, other.files) {
            (true, true) => self.front().as_bytes().cmp(other.front().as_bytes()),
            (false, false) => cmp_dir_keys(self.front(), other.front()),
            (true, false) => cmp_file_dir(self.front(), other.front()),
            (false, true) => cmp_file_dir(other.front(), self.front()).reverse(),
        }
    }
}

/// True when every possible child key of `dir_key` (keys of the form
/// `dir_key + "/" + name`) sorts strictly before `after`, so the directory can
/// contribute no emittable key and may be skipped without listing it.
///
/// This is exactly `dir_key + "/" < after && !after.starts_with(dir_key + "/")`
/// (the smallest possible child is `dir_key + "/"`), computed without
/// allocating. The `starts_with` half matters: marker `ab` with directory `a` —
/// children `a/...` sort before `ab` because `/` (0x2F) < `b`, so the directory
/// is skippable even though the marker extends its key.
///
/// NOTE: this predicate is exact per directory but NOT monotone across a listing's
/// sibling directories: sibling names can be prefixes of each other (`0` vs `0 `), so
/// appending the delimiter can flip their order. Resumption therefore bounds the cursor
/// with a binary search over the directories' OUTPUT order (`cmp_dir_keys`) plus an
/// explicit straddler adjustment (see [`SortedListing::push_listing`]), not by binary
/// searching this predicate.
fn dir_children_all_before(dir_key: &str, after: &str) -> bool {
    let key = dir_key.as_bytes();
    let marker = after.as_bytes();
    let common = key.len().min(marker.len());
    match key[..common].cmp(&marker[..common]) {
        // Differ within the directory's own key: every child compares the same
        // way as `dir_key + "/"` vs `after`.
        Ordering::Less => true,
        Ordering::Greater => false,
        // Equal prefixes: the decision is at `dir_key.len()` — the child's `/`
        // vs the marker's byte. A byte greater than `/` (e.g. `ab`, `aX`) puts
        // every child before the marker; `/` itself means the marker lives
        // under the directory (children straddle it); a marker that is equal to
        // the directory key or a strict prefix of it sorts before its children.
        Ordering::Equal if key.len() == marker.len() => false,
        Ordering::Equal if key.len() < marker.len() => marker[key.len()] > b'/',
        Ordering::Equal => false,
    }
}

/// First index in an output-ordered directory array that can still contain a key
/// greater than `marker`.
///
/// Every directory whose output key (`dir + "/"`) sorts after the marker is kept — as a
/// suffix of the output-ordered array, so a plain `partition_point` finds its start.
/// Exactly one extra directory can also matter: the one whose output key is a *prefix* of
/// the marker (`marker` starts with `dir + "/"`), because its children straddle the
/// marker. That directory, if present, is the last entry at or before the boundary
/// (nothing else can sort between `dir + "/"` and a marker inside that subtree), so the
/// bound is simply adjusted by one.
fn dirs_start_after(dirs: &[String], marker: &str) -> usize {
    let mut start = dirs.partition_point(|d| cmp_dir_output(d, marker) != Ordering::Greater);
    if start > 0 {
        let candidate = dirs[start - 1].as_bytes();
        let marker = marker.as_bytes();
        if marker.len() > candidate.len()
            && marker.starts_with(candidate)
            && marker[candidate.len()] == b'/'
        {
            start -= 1;
        }
    }
    start
}

/// A lazy, sorted k-way merge over per-directory listings.
///
/// The start directory's listing is fetched on the first pull; every directory is
/// opened only when it is popped as the smallest entry in the frontier (i.e. only
/// when its key range is actually reached), and directories whose key cannot match
/// `prefix` are never opened at all (nothing under them can match either).
pub struct SortedListing {
    lister: Arc<dyn DirLister>,
    mapper: PathMapper,
    prefix: String,
    after: Option<String>,
    start: String,
    heap: BinaryHeap<Reverse<Cursor>>,
    start_seeded: bool,
    exhausted: bool,
}

impl SortedListing {
    /// `start` is the HDFS directory the walk is bounded to (from
    /// `PathMapper::list_start`); `prefix` is the S3 prefix; `after` is the decoded
    /// continuation marker (every key `<= after` is skipped); `mapper` converts S3
    /// keys back to HDFS paths when a directory is descended into.
    pub fn new(
        lister: Arc<dyn DirLister>,
        start: String,
        prefix: String,
        after: Option<String>,
        mapper: PathMapper,
    ) -> Self {
        SortedListing {
            lister,
            mapper,
            prefix,
            after,
            start,
            heap: BinaryHeap::new(),
            start_seeded: false,
            exhausted: false,
        }
    }

    /// Yield the next matching key in byte order, or `None` when exhausted.
    ///
    /// Error semantics (best effort, never silently incomplete):
    /// - the start directory missing (`FileNotFound`) is an empty listing, not an
    ///   error — S3 answers probes of non-existent prefixes with an empty page;
    /// - a subdirectory that vanished mid-walk (`FileNotFound` between the parent
    ///   listing and ours) is skipped — the old recursive walk surfaced that as an
    ///   error which the s3 layer mapped to a silently *empty* listing, dropping
    ///   everything else; skipping only the vanished directory is strictly better;
    /// - a subdirectory the gateway's HDFS user cannot read (NameNode
    ///   access-control exception) is skipped the same way — the merge continues
    ///   and every readable key is still served, with a `warn` logged server-side.
    ///   Directories are never objects, so there is nothing to surface for the
    ///   denied subtree itself; it is simply absent from the listing;
    /// - the start directory itself failing with anything but `FileNotFound`
    ///   (including access-denied) propagates: if the requested prefix's own
    ///   directory cannot be listed there is nothing to serve, and S3 semantics
    ///   say answer with 403 `AccessDenied`, not a misleading empty page;
    /// - any other error (e.g. transient IO failure on a popped directory)
    ///   propagates to the caller — a subtree must not silently disappear from a
    ///   listing the client will believe is complete.
    async fn next_entry(&mut self) -> Result<Option<ListEntry>, HdfsError> {
        if self.exhausted {
            return Ok(None);
        }
        if !self.start_seeded {
            self.start_seeded = true;
            match self.lister.list_dir(self.start.clone()).await {
                Ok(listing) => self.push_listing(listing),
                Err(HdfsError::FileNotFound(_)) => {
                    self.exhausted = true;
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }

        loop {
            let Some(Reverse(mut cursor)) = self.heap.pop() else {
                self.exhausted = true;
                return Ok(None);
            };
            if cursor.files {
                let entry = cursor.listing.files[cursor.idx].clone();
                if cursor.advance() {
                    self.heap.push(Reverse(cursor));
                }
                // Defence in depth: file cursors are positioned past the marker, so
                // these checks cannot fire; they keep the emitted contract local.
                if !entry.key.starts_with(&self.prefix) {
                    continue;
                }
                if let Some(after) = &self.after {
                    if &entry.key <= after {
                        continue;
                    }
                }
                return Ok(Some(entry));
            }

            // Directories are never objects; descend into them (lazily, one RPC).
            let key = cursor.listing.dirs[cursor.idx].clone();
            if cursor.advance() {
                self.heap.push(Reverse(cursor));
            }
            if !key.starts_with(&self.prefix) {
                continue;
            }
            if let Some(after) = &self.after {
                if dir_children_all_before(&key, after) {
                    continue;
                }
            }
            let Some(hdfs_path) = self.mapper.key_to_hdfs_path(&key) else {
                continue;
            };
            match self.lister.list_dir(hdfs_path.clone()).await {
                Ok(listing) => self.push_listing(listing),
                // Unreadable directory: skip it and keep merging. Best effort —
                // the client gets every key it can see; the denied subtree is
                // absent (and visible to operators via the warn log).
                Err(e) if is_dir_denied(&e) => {
                    tracing::warn!(
                        dir = %hdfs_path,
                        error = %e,
                        "skipping directory without read permission during flat listing"
                    );
                    continue;
                }
                Err(HdfsError::FileNotFound(_)) => continue, // vanished mid-walk
                Err(e) => return Err(e),
            }
        }
    }

    /// Push one listing's two cursors, positioned at the first entry that can still be
    /// emitted after the continuation marker.
    ///
    /// - files: first key strictly greater than the marker (`partition_point` over a
    ///   key-sorted array — monotone: a file's output key IS its key);
    /// - directories: first directory that is not provably entirely before the marker.
    ///   This is what keeps a directory whose own key sorts before the marker but whose
    ///   children sort after it (`a/b` vs marker `a/b.txt`) from being skipped; the
    ///   predicate is monotone over siblings, so no compression happens.
    fn push_listing(&mut self, listing: Arc<DirListing>) {
        let (files_start, dirs_start) = match &self.after {
            Some(marker) => (
                listing
                    .files
                    .partition_point(|f| f.key.as_bytes() <= marker.as_bytes()),
                dirs_start_after(&listing.dirs, marker),
            ),
            None => (0, 0),
        };
        if files_start < listing.files.len() {
            self.heap.push(Reverse(Cursor {
                listing: Arc::clone(&listing),
                files: true,
                idx: files_start,
            }));
        }
        if dirs_start < listing.dirs.len() {
            self.heap.push(Reverse(Cursor {
                listing,
                files: false,
                idx: dirs_start,
            }));
        }
    }

    /// Collect up to `max_keys` entries (the page), returning them with a flag
    /// telling whether at least one more entry exists after the page (the S3
    /// `IsTruncated` contract). The extra entry is probed lazily — one pull beyond
    /// `max_keys` — so truncation is exact.
    ///
    /// `max_keys = 0` is the exception: the page is empty and NOT truncated. Probing
    /// for an extra entry there would report truncation with no token to advance with
    /// (and S3's reference tests assert `IsTruncated=false` for `MaxKeys=0`).
    pub async fn collect_page(
        &mut self,
        max_keys: usize,
    ) -> Result<(Vec<ListEntry>, bool), HdfsError> {
        if max_keys == 0 {
            return Ok((Vec::new(), false));
        }
        let mut page = Vec::with_capacity(max_keys.min(1024));
        while page.len() < max_keys {
            match self.next_entry().await? {
                Some(entry) => page.push(entry),
                None => return Ok((page, false)),
            }
        }
        let is_truncated = self.next_entry().await?.is_some();
        Ok((page, is_truncated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// How a fake directory listing should fail (HdfsError is not Clone).
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum FakeErr {
        NotFound,
        /// An upstream NameNode access-control denial (e.g. `AccessControlException`).
        Denied,
        Other,
    }

    /// An in-memory namespace for unit tests. Records every directory it was asked
    /// to list so tests can assert the pruning contract (dirs that cannot match the
    /// prefix must never be opened).
    #[derive(Default)]
    struct FakeFs {
        children: HashMap<String, Vec<FileStatus>>,
        errors: HashMap<String, FakeErr>,
        listed: Arc<Mutex<Vec<String>>>,
    }

    struct FakeLister(Arc<FakeFs>);

    impl DirLister for FakeLister {
        fn list_dir(&self, dir: String) -> BoxFuture<'static, Result<Arc<DirListing>, HdfsError>> {
            let fs = Arc::clone(&self.0);
            Box::pin(async move {
                fs.listed.lock().unwrap().push(dir.clone());
                match fs.errors.get(&dir).copied() {
                    Some(FakeErr::NotFound) => Err(HdfsError::FileNotFound(dir)),
                    Some(FakeErr::Denied) => Err(HdfsError::RPCError(
                        "org.apache.hadoop.security.AccessControlException".into(),
                        format!("Permission denied: user=gw, path={dir}"),
                    )),
                    Some(FakeErr::Other) => Err(HdfsError::InternalError("boom".into())),
                    None => Ok(Arc::new(listing_from_statuses(
                        fs.children.get(&dir).cloned().unwrap_or_default(),
                        &mapper(),
                    ))),
                }
            })
        }
    }

    fn file(path: &str) -> FileStatus {
        FileStatus {
            path: path.into(),
            length: 7,
            isdir: false,
            permission: 0o644,
            owner: "u".into(),
            group: "g".into(),
            modification_time: 42,
            access_time: 42,
            replication: Some(1),
            blocksize: Some(1 << 27),
        }
    }

    fn dir_(path: &str) -> FileStatus {
        dir(path)
    }

    fn dir(path: &str) -> FileStatus {
        FileStatus {
            isdir: true,
            ..file(path)
        }
    }

    fn mapper() -> PathMapper {
        let config = crate::config::Config {
            namenode_uri: "hdfs://localhost:8020".into(),
            hdfs_root: "/data".into(),
            bucket_name: "hdfs".into(),
            listen_addr: "0.0.0.0:8080".into(),
            max_concurrent_requests: 2048,
            expose_upstream_errors: false,
            hdfs_options: Default::default(),
            hdfs_config_dir: None,
            hdfs_user: None,
            cache: Default::default(),
            auth_secret: None,
        };
        PathMapper::new(&config)
    }

    fn new_listing(fs: Arc<FakeFs>, prefix: &str, after: Option<String>) -> SortedListing {
        // Mirror the production entry point: the walk starts at the prefix's directory
        // (`PathMapper::list_start`), never at the root — a prefix like "a/" is served by
        // listing /data/a directly, not by listing /data and filtering its children.
        let mapper = mapper();
        let start = mapper
            .list_start(prefix)
            .unwrap_or_else(|| "/data".to_string());
        SortedListing::new(
            Arc::new(FakeLister(fs)),
            start,
            prefix.into(),
            after,
            mapper,
        )
    }

    fn keys(page: &[ListEntry]) -> Vec<String> {
        page.iter().map(|e| e.key.clone()).collect()
    }

    /// A small tree under the root `/data`:
    /// `a.txt`, `b/1.txt`, `b/2.txt`, `b2.txt`, `c/1.txt`, `d.txt`, and an empty
    /// directory `e/` (which must contribute nothing to a flat listing).
    fn tree() -> Arc<FakeFs> {
        Arc::new(FakeFs {
            children: HashMap::from([
                (
                    "/data".into(),
                    vec![
                        file("/data/a.txt"),
                        dir("/data/b"),
                        file("/data/b2.txt"),
                        dir("/data/c"),
                        file("/data/d.txt"),
                        dir("/data/e"),
                    ],
                ),
                (
                    "/data/b".into(),
                    vec![file("/data/b/1.txt"), file("/data/b/2.txt")],
                ),
                ("/data/c".into(), vec![file("/data/c/1.txt")]),
            ]),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn merges_directories_in_global_byte_order() {
        let fs = tree();
        let mut listing = new_listing(fs.clone(), "", None);
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(!truncated);
        assert_eq!(
            keys(&page),
            vec![
                "a.txt", "b/1.txt", "b/2.txt",
                "b2.txt", // 'b/' (0x2F) sorts before "b2" (0x32)
                "c/1.txt", "d.txt",
            ]
        );
        // The empty directory was opened but yielded nothing.
        assert!(fs.listed.lock().unwrap().contains(&"/data/e".to_string()));
    }

    #[tokio::test]
    async fn emits_utf8_byte_order_not_java_string_order() {
        // HDFS returns names in Java String order (UTF-16 code units): U+10000
        // (surrogate D800) sorts BEFORE U+E000. UTF-8 bytes put U+E000 (EE...) first.
        // The per-directory re-sort must restore S3's UTF-8 binary order.
        let fs = Arc::new(FakeFs {
            children: HashMap::from([(
                "/data".into(),
                vec![file("/data/\u{10000}.txt"), file("/data/\u{e000}.txt")],
            )]),
            ..Default::default()
        });
        let mut listing = new_listing(fs, "", None);
        let (page, _truncated) = listing.collect_page(10).await.unwrap();
        assert_eq!(keys(&page), vec!["\u{e000}.txt", "\u{10000}.txt"]);
    }

    #[tokio::test]
    async fn prefix_filters_and_prunes_unmatchable_directories() {
        let fs = tree();
        let mut listing = new_listing(fs.clone(), "b", None);
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(!truncated);
        assert_eq!(keys(&page), vec!["b/1.txt", "b/2.txt", "b2.txt"]);

        // Pruning contract: directories whose key cannot match the prefix must
        // never be opened (nothing under them can match either).
        let listed = fs.listed.lock().unwrap();
        assert!(listed.contains(&"/data".to_string()));
        assert!(listed.contains(&"/data/b".to_string()));
        assert!(
            !listed.iter().any(|d| d == "/data/c" || d == "/data/e"),
            "unmatchable directories must not be listed, got: {listed:?}"
        );
    }

    #[tokio::test]
    async fn continuation_marker_skips_keys_in_byte_order() {
        let fs = tree();
        let mut listing = new_listing(fs, "", Some("b/1.txt".into()));
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(!truncated);
        // a.txt and b/1.txt itself are <= the marker; everything after survives.
        assert_eq!(keys(&page), vec!["b/2.txt", "b2.txt", "c/1.txt", "d.txt"]);
    }

    #[tokio::test]
    async fn pages_concatenate_exactly_to_the_full_sorted_listing() {
        // The pagination contract: walking pages with continuation tokens must
        // yield every key exactly once, in order, and terminate. (This is the
        // regression guard for the flat-listing pagination path.)
        let full: Vec<String> = vec!["a.txt", "b/1.txt", "b/2.txt", "b2.txt", "c/1.txt", "d.txt"]
            .into_iter()
            .map(String::from)
            .collect();

        let mut token: Option<String> = None;
        let mut collected: Vec<String> = Vec::new();
        loop {
            let mut listing = new_listing(tree(), "", token.clone());
            let (page, truncated) = listing.collect_page(2).await.unwrap();
            collected.extend(keys(&page));
            match truncated {
                true => token = page.last().map(|e| e.key.clone()),
                false => break,
            }
        }
        assert_eq!(
            collected, full,
            "pages must tile the sorted listing exactly"
        );
    }

    #[tokio::test]
    async fn resume_skips_directories_entirely_before_marker() {
        // Page 1 served a/1.txt, b/1.txt, c/1.txt (marker `c/1.txt`). On resume,
        // a/ and b/ provably contain only keys <= the marker, so they must not be
        // opened: the only listings are the root and the marker's own directory.
        let fs = Arc::new(FakeFs {
            children: HashMap::from([
                (
                    "/data".into(),
                    vec![dir("/data/a"), dir("/data/b"), dir("/data/c")],
                ),
                ("/data/a".into(), vec![file("/data/a/1.txt")]),
                ("/data/b".into(), vec![file("/data/b/1.txt")]),
                (
                    "/data/c".into(),
                    vec![file("/data/c/1.txt"), file("/data/c/2.txt")],
                ),
            ]),
            ..Default::default()
        });
        let mut listing = new_listing(fs.clone(), "", Some("c/1.txt".into()));
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(!truncated);
        assert_eq!(keys(&page), vec!["c/2.txt"]);
        let listed = fs.listed.lock().unwrap();
        assert!(listed.contains(&"/data".to_string()));
        assert!(listed.contains(&"/data/c".to_string()));
        assert!(
            !listed.iter().any(|d| d == "/data/a" || d == "/data/b"),
            "directories wholly before the marker must not be listed, got: {listed:?}"
        );
    }

    #[tokio::test]
    async fn marker_inside_directory_still_lists_it() {
        // Marker `b/1.txt` lies inside b/'s key range: b's children straddle it
        // (b/2.txt must be emitted), so /data/b must still be opened. a/ is
        // wholly before the marker and stays skipped.
        let fs = Arc::new(FakeFs {
            children: HashMap::from([
                (
                    "/data".into(),
                    vec![dir("/data/a"), dir("/data/b"), dir("/data/c")],
                ),
                ("/data/a".into(), vec![file("/data/a/1.txt")]),
                (
                    "/data/b".into(),
                    vec![file("/data/b/1.txt"), file("/data/b/2.txt")],
                ),
                ("/data/c".into(), vec![file("/data/c/1.txt")]),
            ]),
            ..Default::default()
        });
        let mut listing = new_listing(fs.clone(), "", Some("b/1.txt".into()));
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(!truncated);
        assert_eq!(keys(&page), vec!["b/2.txt", "c/1.txt"]);
        let listed = fs.listed.lock().unwrap();
        assert!(listed.contains(&"/data/b".to_string()));
        assert!(
            !listed.iter().any(|d| d == "/data/a"),
            "a/ is wholly before the marker, got: {listed:?}"
        );
    }

    #[tokio::test]
    async fn marker_under_sibling_file_still_skips_directory() {
        // The subtle case: marker `ab` (a root file) extends the directory key
        // `a`, yet every child `a/...` sorts before `ab` ('/' 0x2F < 'b' 0x62).
        // The directory must be skipped, not listed — only the root is opened.
        let fs = Arc::new(FakeFs {
            children: HashMap::from([
                ("/data".into(), vec![dir("/data/a"), file("/data/ab")]),
                ("/data/a".into(), vec![file("/data/a/1.txt")]),
            ]),
            ..Default::default()
        });
        let mut listing = new_listing(fs.clone(), "", Some("ab".into()));
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(!truncated);
        assert!(page.is_empty()); // a/1.txt and ab itself are <= the marker
        let listed = fs.listed.lock().unwrap();
        assert_eq!(
            listed.as_slice(),
            &["/data".to_string()],
            "only the root may be listed"
        );
    }

    /// Deterministic adversarial tree: names use bytes around '/' in UTF-8 order
    /// (`.`, `!`, space, `0`), so directories are string-prefixes of sibling files
    /// (`a/b` next to `a/b.txt`) and files sit between a directory key and the keys of
    /// its children (`a/b!x`). Returns the tree and its sorted flat listing.
    fn adversarial_fs(seed: u64, n: usize) -> (Arc<FakeFs>, Vec<String>) {
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
                for _ in 0..=rnd(2) {
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

        let mut children: HashMap<String, Vec<FileStatus>> = HashMap::new();
        let mut seen_dirs: std::collections::BTreeSet<(String, String)> =
            std::collections::BTreeSet::new();
        for key in &keys {
            let path = format!("/data/{key}");
            let (parent, _) = path.rsplit_once('/').unwrap();
            children
                .entry(parent.to_string())
                .or_default()
                .push(file(&path));
            // Register every ancestor directory with its own parent.
            let mut dir = parent.to_string();
            while dir != "/data" {
                let (up, _) = dir.rsplit_once('/').unwrap();
                if seen_dirs.insert((up.to_string(), dir.clone())) {
                    children.entry(up.to_string()).or_default().push(dir_(&dir));
                }
                dir = up.to_string();
            }
        }
        // An empty directory: contributes no keys but must be openable.
        children
            .entry("/data".into())
            .or_default()
            .push(dir_("/data/emptydir"));

        let full = keys.into_iter().collect::<Vec<String>>();
        (
            Arc::new(FakeFs {
                children,
                ..Default::default()
            }),
            full,
        )
    }

    /// Walk the flat listing with a client-style paginator, asserting strict ordering,
    /// no duplicates, a token on every truncated page, and termination.
    async fn walk_flat(
        fs: Arc<FakeFs>,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Vec<String> {
        let mut collected: Vec<String> = Vec::new();
        let mut marker = start_after.map(str::to_string);
        let mut pages = 0usize;
        loop {
            pages += 1;
            assert!(pages <= 10_000, "pagination must terminate");
            let mut listing = new_listing(fs.clone(), prefix, marker.clone());
            let (page, truncated) = listing.collect_page(max_keys).await.unwrap();
            let page_keys = keys(&page);
            if truncated {
                assert!(!page_keys.is_empty(), "a truncated page must not be empty");
                marker = page_keys.last().cloned();
                assert!(marker.is_some(), "a truncated page must carry a token");
            } else {
                marker = None;
            }
            for key in page_keys {
                if let Some(prev) = collected.last() {
                    assert!(
                        prev.as_str() < key.as_str(),
                        "keys must be strictly increasing (prefix={prefix:?} max_keys={max_keys} start={start_after:?}): {prev:?} then {key:?}"
                    );
                }
                collected.push(key);
            }
            if marker.is_none() {
                break;
            }
        }
        collected
    }

    #[tokio::test]
    async fn flat_listing_page_size_invariance() {
        // Served in one page or walked with any page size, from any marker, the flat
        // listing must be identical: no key skipped at a page boundary, none twice,
        // strictly ascending throughout. This is the safety net for resume-by-binary-
        // search: a directory whose own key sorts before the marker can still have
        // children after it (`a/b` vs marker `a/b.txt`), and files with bytes before
        // '/' (`a/b!x`) sit between them.
        let (fs, full) = adversarial_fs(11, 120);
        for prefix in ["", "a", "a/", "a.b", "!"] {
            let expected: Vec<String> = full
                .iter()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            // One page is the reference.
            let one = walk_flat(fs.clone(), prefix, None, usize::MAX).await;
            assert_eq!(one, expected, "prefix={prefix:?} (single page)");
            for max_keys in [1, 2, 3, 5, 7] {
                let walked = walk_flat(fs.clone(), prefix, None, max_keys).await;
                assert_eq!(walked, expected, "prefix={prefix:?} max_keys={max_keys}");
            }
            // Resuming from an arbitrary client marker yields the exact suffix.
            let markers: Vec<Option<String>> = vec![
                None,
                Some("a".to_string()),
                Some("a/".to_string()),
                Some("a/b".to_string()),
                Some("a/b.".to_string()),
                Some("a/b!".to_string()),
                Some("a/0".to_string()),
                Some("!".to_string()),
            ];
            for marker in markers {
                let expected_suffix: Vec<String> = expected
                    .iter()
                    .filter(|k| marker.as_deref().map_or(true, |m| k.as_str() > m))
                    .cloned()
                    .collect();
                let walked = walk_flat(fs.clone(), prefix, marker.as_deref(), 2).await;
                assert_eq!(
                    walked, expected_suffix,
                    "prefix={prefix:?} marker={marker:?}"
                );
            }
        }
    }

    #[test]
    fn dir_children_all_before_cases() {
        // The predicate is exact: skip only when every child (`key + "/" + name`)
        // sorts before the marker.
        assert!(dir_children_all_before("a", "b/1.txt")); // decided within the key
        assert!(dir_children_all_before("a", "ab")); // '/' < 'b' at the boundary
        assert!(dir_children_all_before("a", "aX")); // '/' < 'X'
        assert!(dir_children_all_before("a", "a0")); // '/' < '0' (0x30)
        assert!(!dir_children_all_before("a", "a.")); // '.' (0x2E) < '/': children sort after
        assert!(!dir_children_all_before("a", "a")); // marker == dir key
        assert!(!dir_children_all_before("a", "a/")); // marker under the dir: straddle
        assert!(!dir_children_all_before("a", "a/1.txt")); // straddle
        assert!(!dir_children_all_before("ab", "a")); // marker prefix of dir key
        assert!(!dir_children_all_before("b", "aZ")); // dir sorts after the marker
        assert!(!dir_children_all_before("a", "")); // empty marker
        assert!(dir_children_all_before("ab", "ac")); // decided within the key
    }

    #[tokio::test]
    async fn empty_directories_yield_nothing_but_are_still_openable() {
        // Covered implicitly by `merges_directories_in_global_byte_order`; keep an
        // explicit, focused assertion for the flat path: an empty dir contributes
        // no keys even when it is the only content of its parent.
        let fs = Arc::new(FakeFs {
            children: HashMap::from([("/data".into(), vec![dir("/data/e")])]),
            ..Default::default()
        });
        let mut listing = new_listing(fs, "", None);
        let (page, truncated) = listing.collect_page(10).await.unwrap();
        assert!(page.is_empty());
        assert!(!truncated);
    }

    #[tokio::test]
    async fn vanished_subdirectory_is_skipped_not_fatal() {
        // The parent listing contains `b/`, but `b/` is gone by the time we list
        // it (concurrent deletion). The merge must skip it and still return the
        // rest — not fail the whole listing (and certainly not silently empty it,
        // which was the old recursive walk's behaviour).
        let fs = Arc::new(FakeFs {
            children: HashMap::from([
                (
                    "/data".into(),
                    vec![file("/data/a.txt"), dir("/data/b"), file("/data/d.txt")],
                ),
                ("/data/b".into(), vec![file("/data/b/1.txt")]),
            ]),
            errors: HashMap::from([("/data/b".into(), FakeErr::NotFound)]),
            ..Default::default()
        });
        let mut listing = new_listing(fs, "", None);
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(!truncated);
        assert_eq!(keys(&page), vec!["a.txt", "d.txt"]);
    }

    #[tokio::test]
    async fn unreadable_subdirectory_is_skipped_not_fatal() {
        // THE permission story: the gateway's HDFS user cannot read `b/` (the
        // NameNode answers `getListing(b)` with AccessControlException). The flat
        // listing must still serve everything else — a single unreadable subtree
        // must not fail the whole ListObjectsV2 (and the directory itself is
        // never an object, so there is nothing to surface for it).
        let fs = Arc::new(FakeFs {
            children: HashMap::from([
                (
                    "/data".into(),
                    vec![file("/data/a.txt"), dir("/data/b"), file("/data/d.txt")],
                ),
                ("/data/b".into(), vec![file("/data/b/1.txt")]),
            ]),
            errors: HashMap::from([("/data/b".into(), FakeErr::Denied)]),
            ..Default::default()
        });
        let mut listing = new_listing(fs, "", None);
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(!truncated);
        assert_eq!(keys(&page), vec!["a.txt", "d.txt"]);
    }

    #[tokio::test]
    async fn unreadable_subdirectory_after_skipped_files_still_continues() {
        // The denied directory sorts between two readable files: the merge pops
        // it mid-page, skips it, and still yields the files after it.
        let fs = Arc::new(FakeFs {
            children: HashMap::from([
                (
                    "/data".into(),
                    vec![file("/data/a.txt"), dir("/data/b"), file("/data/c.txt")],
                ),
                ("/data/b".into(), vec![file("/data/b/1.txt")]),
            ]),
            errors: HashMap::from([("/data/b".into(), FakeErr::Denied)]),
            ..Default::default()
        });
        let mut listing = new_listing(fs, "", None);
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(!truncated);
        assert_eq!(keys(&page), vec!["a.txt", "c.txt"]);
    }

    #[tokio::test]
    async fn unreadable_start_directory_still_propagates() {
        // The start directory is the prefix's own directory. If IT cannot be
        // listed there is nothing to serve, and S3 semantics say answer with 403
        // AccessDenied — not a misleading empty page that hides the problem.
        let fs = Arc::new(FakeFs {
            children: HashMap::from([("/data".into(), vec![file("/data/a.txt")])]),
            errors: HashMap::from([("/data".into(), FakeErr::Denied)]),
            ..Default::default()
        });
        let mut listing = new_listing(fs, "", None);
        let err = listing.collect_page(100).await.unwrap_err();
        assert!(
            matches!(err, HdfsError::RPCError(exception, _) if exception.ends_with("AccessControlException"))
        );
    }

    #[tokio::test]
    async fn missing_start_directory_is_an_empty_listing() {
        let fs = Arc::new(FakeFs {
            errors: HashMap::from([("/data".into(), FakeErr::NotFound)]),
            ..Default::default()
        });
        let mut listing = new_listing(fs, "", None);
        let (page, truncated) = listing.collect_page(100).await.unwrap();
        assert!(page.is_empty());
        assert!(!truncated);
    }

    #[tokio::test]
    async fn other_errors_propagate_when_reached() {
        // `/data/c` fails with a non-FileNotFound error. A full pull must surface
        // it; a page that ends before `c` is popped must succeed lazily (the error
        // only bites the page that actually reaches it).
        let fs = Arc::new(FakeFs {
            children: HashMap::from([
                ("/data".into(), vec![file("/data/a.txt"), dir("/data/c")]),
                ("/data/c".into(), vec![file("/data/c/1.txt")]),
            ]),
            errors: HashMap::from([("/data/c".into(), FakeErr::Other)]),
            ..Default::default()
        });

        // Page 1 (max_keys=2) emits a.txt, then the truncation probe pulls the next
        // entry — which is the directory /data/c → the error surfaces.
        let mut listing = new_listing(fs.clone(), "", None);
        let err = listing.collect_page(2).await.unwrap_err();
        assert!(matches!(err, HdfsError::InternalError(_)));
    }

    #[tokio::test]
    async fn max_keys_zero_is_empty_and_not_truncated() {
        // max-keys=0 asks for no keys and must NOT report truncation: an empty page has
        // no last key to mint a continuation token from, so "truncated" would be a page
        // the client cannot advance past (and S3's reference tests assert
        // IsTruncated=false for MaxKeys=0).
        let fs = tree();
        let mut listing = new_listing(fs.clone(), "", None);
        let (page, truncated) = listing.collect_page(0).await.unwrap();
        assert!(page.is_empty());
        assert!(!truncated);

        let empty = Arc::new(FakeFs::default());
        let mut listing = new_listing(empty, "", None);
        let (page, truncated) = listing.collect_page(0).await.unwrap();
        assert!(page.is_empty());
        assert!(!truncated);
    }
}
