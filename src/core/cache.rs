//! Bounded, weighted TTL cache used by the gateway's metadata, block-map and directory
//! listing caches.
//!
//! The gateway is read-only, so the only source of change is a writer *outside* the
//! gateway (another HDFS client). There is no invalidation channel from the NameNode, so
//! freshness is bounded purely by a per-entry TTL, and the entry budget keeps memory
//! finite. Eviction is LRU (the least recently used entry is dropped first) and
//! **weighted**, because one cached directory listing is orders of magnitude larger than
//! one cached file stat; callers pass the weight (e.g. number of entries) at insertion.
//!
//! Entries whose weight exceeds the whole budget are not cached at all: caching one
//! million-entry directory listing would evict everything else and still blow the budget,
//! so those are streamed and forgotten (also makes the cache resistant to scans).

use std::borrow::Borrow;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use lru::LruCache;

struct Entry<V> {
    value: V,
    inserted: Instant,
    weight: usize,
}

struct Inner<K, V> {
    entries: LruCache<K, Entry<V>>,
    weight: usize,
}

/// A bounded, weighted, TTL'd LRU cache. Cheap to share behind an `Arc`; all operations
/// take a short mutex (no `.await` is ever held across it).
pub struct TtlCache<K, V> {
    inner: Mutex<Inner<K, V>>,
    ttl: Duration,
    max_weight: usize,
}

impl<K: Hash + Eq, V: Clone> TtlCache<K, V> {
    /// Build a cache holding up to `capacity` entries and `max_weight` total weight.
    /// `ttl == 0` disables the cache: every lookup misses and every insert is a no-op.
    pub fn new(capacity: NonZeroUsize, ttl: Duration, max_weight: usize) -> Self {
        TtlCache {
            inner: Mutex::new(Inner {
                entries: LruCache::new(capacity),
                weight: 0,
            }),
            ttl,
            max_weight,
        }
    }

    /// True when this cache does anything at all (non-zero TTL).
    pub fn is_enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    /// Fetch a live entry. Expired entries are dropped on access; a hit refreshes the
    /// entry's LRU position.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if !self.is_enabled() {
            return None;
        }
        let mut inner = self.lock();
        let expired = inner
            .entries
            .peek(key)
            .is_some_and(|entry| entry.inserted.elapsed() >= self.ttl);
        if expired {
            if let Some(entry) = inner.entries.pop(key) {
                inner.weight -= entry.weight;
            }
            return None;
        }
        inner.entries.get(key).map(|entry| entry.value.clone())
    }

    /// Store `value` with the given weight. A value heavier than the whole budget is not
    /// cached (it would evict everything and still not fit). Inserting an existing key
    /// replaces it and its weight.
    pub fn insert(&self, key: K, value: V, weight: usize) {
        if !self.is_enabled() || weight > self.max_weight {
            return;
        }
        let mut inner = self.lock();
        if let Some(old) = inner.entries.pop(&key) {
            inner.weight -= old.weight;
        }
        inner.entries.put(
            key,
            Entry {
                value,
                inserted: Instant::now(),
                weight,
            },
        );
        inner.weight += weight;
        while inner.weight > self.max_weight {
            match inner.entries.pop_lru() {
                Some((_, entry)) => inner.weight -= entry.weight,
                None => break,
            }
        }
    }

    /// Drop an entry (used when an upstream read reveals a cached value is stale/bad).
    pub fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut inner = self.lock();
        if let Some(entry) = inner.entries.pop(key) {
            inner.weight -= entry.weight;
        }
    }

    /// Total weight currently held (for tests and diagnostics).
    pub fn weight(&self) -> usize {
        self.lock().weight
    }

    /// Number of entries currently held, including any not yet lazily expired.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<K, V>> {
        // A poisoned lock only means some other request panicked while holding it; the
        // cached data is plain values, so recovering is safe and keeps the gateway
        // serving instead of cascading the panic.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(ttl_ms: u64, max_weight: usize) -> TtlCache<&'static str, &'static str> {
        TtlCache::new(
            NonZeroUsize::new(16).unwrap(),
            Duration::from_millis(ttl_ms),
            max_weight,
        )
    }

    #[test]
    fn hit_and_miss() {
        let c = cache(60_000, 10);
        assert_eq!(c.get(&"a"), None);
        c.insert("a", "A", 1);
        assert_eq!(c.get(&"a"), Some("A"));
        assert_eq!(c.get(&"b"), None);
    }

    #[test]
    fn ttl_expires_entries() {
        let c = cache(20, 10);
        c.insert("a", "A", 1);
        assert_eq!(c.get(&"a"), Some("A"));
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(c.get(&"a"), None, "entry must expire after its TTL");
        assert_eq!(c.weight(), 0, "expired entries release their weight");
    }

    #[test]
    fn lru_evicts_least_recently_used_first() {
        let c = cache(60_000, 2);
        c.insert("a", "A", 1);
        c.insert("b", "B", 1);
        // Touch `a` so `b` becomes the LRU entry.
        assert_eq!(c.get(&"a"), Some("A"));
        c.insert("c", "C", 1);
        assert_eq!(c.get(&"b"), None, "b was least recently used");
        assert_eq!(c.get(&"a"), Some("A"));
        assert_eq!(c.get(&"c"), Some("C"));
        assert_eq!(c.weight(), 2);
    }

    #[test]
    fn weighted_entries_evict_until_they_fit() {
        // One 4-weight listing forces two 1-weight entries out.
        let c = cache(60_000, 4);
        c.insert("a", "A", 1);
        c.insert("b", "B", 1);
        c.insert("big", "BIG", 4);
        assert_eq!(c.get(&"big"), Some("BIG"));
        assert_eq!(c.get(&"a"), None);
        assert_eq!(c.get(&"b"), None);
        assert_eq!(c.weight(), 4);
    }

    #[test]
    fn oversized_values_are_not_cached() {
        let c = cache(60_000, 3);
        c.insert("huge", "HUGE", 4);
        assert_eq!(c.get(&"huge"), None);
        assert_eq!(c.weight(), 0);
    }

    #[test]
    fn zero_ttl_disables_the_cache() {
        let c = cache(0, 10);
        assert!(!c.is_enabled());
        c.insert("a", "A", 1);
        assert_eq!(c.get(&"a"), None);
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn replacing_a_key_updates_weight() {
        let c = cache(60_000, 10);
        c.insert("a", "A", 3);
        c.insert("a", "AA", 1);
        assert_eq!(c.get(&"a"), Some("AA"));
        assert_eq!(c.weight(), 1);
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn remove_releases_weight() {
        let c = cache(60_000, 10);
        c.insert("a", "A", 2);
        c.remove(&"a");
        assert_eq!(c.get(&"a"), None);
        assert_eq!(c.weight(), 0);
    }
}
