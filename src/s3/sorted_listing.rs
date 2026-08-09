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
//!   matches `prefix`, in strict UTF-8 binary order, each exactly once.
//! - A continuation `after` marker skips every key `<= after` (byte order), the
//!   same rule the old code applied via `contents.retain(|e| &e.key > marker)`.
//! - Directories are never emitted as objects; they are only descended into.
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
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;

use futures::future::BoxFuture;
use hdfs_native::client::FileStatus;
use hdfs_native::{Client, HdfsError};
use tracing::Instrument;

use crate::core::{ListEntry, PathMapper};

/// Abstraction over "list one directory, non-recursively", so the merge can be
/// unit-tested against an in-memory fake instead of a live cluster.
pub trait DirLister: Send + Sync {
    /// Non-recursive listing of `dir` (its direct children). The result may be in
    /// any order; [`SortedListing`] re-sorts each directory's entries by raw path
    /// bytes before merging.
    fn list_dir(&self, dir: String) -> BoxFuture<'static, Result<Vec<FileStatus>, HdfsError>>;
}

/// The production lister: one non-recursive `getListing` RPC per directory.
///
/// Unlike `list_status(.., true)` (one RPC per directory of the whole subtree),
/// this bounds the work to the directories the merge actually opens.
pub struct HdfsDirLister {
    client: Arc<Client>,
    span: tracing::Span,
}

impl HdfsDirLister {
    /// `span` is the request log span; upstream diagnostics stay correlated to the
    /// request that triggered them (same pattern as the other gateway call sites).
    pub fn new(client: Arc<Client>, span: tracing::Span) -> Self {
        HdfsDirLister { client, span }
    }
}

impl DirLister for HdfsDirLister {
    fn list_dir(&self, dir: String) -> BoxFuture<'static, Result<Vec<FileStatus>, HdfsError>> {
        let client = self.client.clone();
        let span = self.span.clone();
        Box::pin(async move { client.list_status(&dir, false).instrument(span).await })
    }
}

/// One opened directory listing, positioned at its next entry.
///
/// Cursors in the heap always have a non-empty front; the heap orders them by the
/// front entry's raw path bytes, so the minimum cursor holds the smallest
/// un-emitted key in the whole frontier.
struct Cursor {
    entries: VecDeque<FileStatus>,
}

impl Cursor {
    fn front_path(&self) -> &str {
        // Invariant: only non-empty cursors are ever pushed/reinserted.
        &self
            .entries
            .front()
            .expect("heap cursors are never empty")
            .path
    }
}

impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        self.front_path() == other.front_path()
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
        // Raw byte order, matching S3's UTF-8 binary key ordering. Paths in a real
        // tree are unique, so ties cannot occur; comparing the front path is a
        // total order.
        self.front_path()
            .as_bytes()
            .cmp(other.front_path().as_bytes())
    }
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
    /// continuation marker (every key `<= after` is skipped); `mapper` converts
    /// HDFS paths back to S3 keys.
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
    /// Error semantics:
    /// - the start directory missing (`FileNotFound`) is an empty listing, not an
    ///   error — S3 answers probes of non-existent prefixes with an empty page;
    /// - a subdirectory that vanished mid-walk (`FileNotFound` between the parent
    ///   listing and ours) is skipped — the old recursive walk surfaced that as an
    ///   error which the s3 layer mapped to a silently *empty* listing, dropping
    ///   everything else; skipping only the vanished directory is strictly better;
    /// - any other error (e.g. `AccessControlException` on a popped directory)
    ///   propagates to the caller, matching the old behaviour.
    async fn next_entry(&mut self) -> Result<Option<ListEntry>, HdfsError> {
        if self.exhausted {
            return Ok(None);
        }
        if !self.start_seeded {
            self.start_seeded = true;
            match self.lister.list_dir(self.start.clone()).await {
                Ok(statuses) => self.push_dir(statuses),
                Err(HdfsError::FileNotFound(_)) => {
                    self.exhausted = true;
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }

        loop {
            let Some(Reverse(mut cur)) = self.heap.pop() else {
                self.exhausted = true;
                return Ok(None);
            };
            let entry = cur
                .entries
                .pop_front()
                .expect("heap cursors are never empty");
            if !cur.entries.is_empty() {
                self.heap.push(Reverse(cur));
            }

            if entry.isdir {
                // Directories are never objects; descend into them (lazily, one
                // RPC) unless no key under them can match the prefix.
                let Some(key) = self.mapper.hdfs_path_to_key(&entry.path) else {
                    continue;
                };
                if !key.starts_with(&self.prefix) {
                    continue;
                }
                match self.lister.list_dir(entry.path.clone()).await {
                    Ok(statuses) => self.push_dir(statuses),
                    Err(HdfsError::FileNotFound(_)) => continue, // vanished mid-walk
                    Err(e) => return Err(e),
                }
                continue;
            }

            let Some(key) = self.mapper.hdfs_path_to_key(&entry.path) else {
                continue;
            };
            if !key.starts_with(&self.prefix) {
                continue;
            }
            if let Some(after) = &self.after {
                if &key <= after {
                    continue;
                }
            }
            return Ok(Some(ListEntry {
                key,
                size: entry.length as u64,
                modification_time: entry.modification_time,
            }));
        }
    }

    /// Push a directory's children as a fresh cursor, sorted by raw path bytes so
    /// the heap merge yields strict UTF-8 binary order (see module docs).
    fn push_dir(&mut self, mut statuses: Vec<FileStatus>) {
        statuses.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        if !statuses.is_empty() {
            self.heap.push(Reverse(Cursor {
                entries: VecDeque::from(statuses),
            }));
        }
    }

    /// Collect up to `max_keys` entries (the page), returning them with a flag
    /// telling whether at least one more entry exists after the page (the S3
    /// `IsTruncated` contract). The extra entry is probed lazily — one pull beyond
    /// `max_keys` — so `max_keys = 0` still reports truncation correctly.
    pub async fn collect_page(
        &mut self,
        max_keys: usize,
    ) -> Result<(Vec<ListEntry>, bool), HdfsError> {
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
        fn list_dir(&self, dir: String) -> BoxFuture<'static, Result<Vec<FileStatus>, HdfsError>> {
            let fs = Arc::clone(&self.0);
            Box::pin(async move {
                fs.listed.lock().unwrap().push(dir.clone());
                match fs.errors.get(&dir).copied() {
                    Some(FakeErr::NotFound) => Err(HdfsError::FileNotFound(dir)),
                    Some(FakeErr::Other) => Err(HdfsError::InternalError("boom".into())),
                    None => Ok(fs.children.get(&dir).cloned().unwrap_or_default()),
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
            auth_secret: None,
        };
        PathMapper::new(&config)
    }

    fn new_listing(fs: Arc<FakeFs>, prefix: &str, after: Option<String>) -> SortedListing {
        SortedListing::new(
            Arc::new(FakeLister(fs)),
            "/data".into(),
            prefix.into(),
            after,
            mapper(),
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
    async fn max_keys_zero_probes_truncation_only() {
        let fs = tree();
        let mut listing = new_listing(fs.clone(), "", None);
        let (page, truncated) = listing.collect_page(0).await.unwrap();
        assert!(page.is_empty());
        assert!(
            truncated,
            "keys exist, so max_keys=0 must still report truncation"
        );

        let empty = Arc::new(FakeFs::default());
        let mut listing = new_listing(empty, "", None);
        let (page, truncated) = listing.collect_page(0).await.unwrap();
        assert!(page.is_empty());
        assert!(!truncated);
    }
}
