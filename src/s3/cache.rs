//! Per-gateway caches: HDFS file metadata, block maps and directory listings.
//!
//! All three are bounded TTL caches (see [`crate::core::TtlCache`]); this module only
//! defines their keys/values and the freshness rules. The gateway is read-only, so a
//! cached value can only go stale because an external writer changed HDFS; the TTLs in
//! `[cache]` bound that window, and nothing here tries to track writers.

use std::num::NonZeroUsize;
use std::time::Duration;

use std::sync::Arc;

use hdfs_native::file::FileReader;

use crate::config::CacheConfig;
use crate::core::{DirListing, TtlCache};

/// The HDFS metadata the S3 layer needs, cached instead of the full `FileStatus` (which
/// carries a path `String` and listing-only fields). `Copy` so every hit is free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileMeta {
    pub length: u64,
    pub isdir: bool,
    pub modification_time: u64,
}

/// A cached block map (`FileReader`) plus the identity it was opened for. A GET reuses it
/// only while the freshly read (length, mtime) still match, so a replaced file with the
/// same name can never be served from the old block map.
#[derive(Clone)]
pub struct CachedReader {
    pub reader: Arc<FileReader>,
    pub length: u64,
    pub modification_time: u64,
}

fn capacity(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n.max(1)).expect("max(1) is non-zero")
}

fn ttl(config: &CacheConfig, millis: u64) -> Duration {
    if config.enabled {
        Duration::from_millis(millis)
    } else {
        Duration::ZERO
    }
}

/// The gateway's caches. Built once per gateway and shared by all requests.
pub struct Caches {
    /// `getFileInfo` results, keyed by absolute HDFS path.
    pub metadata: TtlCache<String, FileMeta>,
    /// Known-missing paths, so clients probing `_SUCCESS`/`_common_metadata` do not
    /// hammer the NameNode with lookups that just fail.
    pub negative: TtlCache<String, ()>,
    /// Block maps keyed by absolute HDFS path.
    pub blocks: TtlCache<String, CachedReader>,
    /// Non-recursive directory listings keyed by absolute HDFS directory path.
    pub dirs: TtlCache<String, Arc<DirListing>>,
    /// Directories larger than this are served but never cached (scan resistance).
    pub max_dir_entries: usize,
}

impl Caches {
    pub fn new(config: &CacheConfig) -> Self {
        Caches {
            metadata: TtlCache::new(
                capacity(config.metadata_capacity),
                ttl(config, config.metadata_ttl_ms),
                config.metadata_capacity,
            ),
            negative: TtlCache::new(
                capacity(config.metadata_capacity),
                ttl(config, config.negative_ttl_ms),
                config.metadata_capacity,
            ),
            blocks: TtlCache::new(
                capacity(config.blocks_capacity),
                ttl(config, config.blocks_ttl_ms),
                config.blocks_capacity,
            ),
            dirs: TtlCache::new(
                capacity(config.listing_capacity_entries),
                ttl(config, config.listing_ttl_ms),
                config.listing_capacity_entries,
            ),
            max_dir_entries: config.listing_max_dir_entries,
        }
    }
}
