//! In-memory key-value store with expiry and
//! republishing support.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::node_id::Key;

struct StoredValue {
    value: Vec<u8>,
    published_at: Instant,
    republished_at: Instant,
    is_original: bool,
}

/// Local key-value store for DHT data.
pub struct Storage {
    entries: HashMap<Key, StoredValue>,
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage {
    /// Create an empty store.
    pub fn new() -> Self {
        Storage {
            entries: HashMap::new(),
        }
    }

    /// Insert or overwrite a value.
    ///
    /// `is_original` marks whether this node is the
    /// original publisher (used for republishing).
    pub fn put(
        &mut self,
        key: Key,
        value: Vec<u8>,
        is_original: bool,
    ) {
        let now = Instant::now();
        self.entries.insert(
            key,
            StoredValue {
                value,
                published_at: now,
                republished_at: now,
                is_original,
            },
        );
    }

    /// Look up a value by key.
    pub fn get(&self, key: &Key) -> Option<&[u8]> {
        self.entries
            .get(key)
            .map(|v| v.value.as_slice())
    }

    /// Return the number of stored entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return true if no entries are stored.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove entries older than `ttl`.
    pub fn remove_expired(&mut self, ttl: Duration) {
        self.entries
            .retain(|_, v| v.published_at.elapsed() < ttl);
    }

    /// Return originally-published values that are
    /// due for republishing and reset their timers.
    pub fn values_to_republish(
        &mut self,
        interval: Duration,
    ) -> Vec<(Key, Vec<u8>)> {
        let mut result = Vec::new();
        let now = Instant::now();
        for (key, stored) in &mut self.entries {
            if stored.is_original
                && stored.republished_at.elapsed()
                    >= interval
            {
                result.push((
                    *key,
                    stored.value.clone(),
                ));
                stored.republished_at = now;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_id::NodeId;

    #[test]
    fn put_and_get() {
        let mut s = Storage::new();
        let key = NodeId::from_sha1(b"test");
        s.put(key, b"hello".to_vec(), true);
        assert_eq!(s.get(&key), Some(b"hello".as_slice()));
    }

    #[test]
    fn get_missing_returns_none() {
        let s = Storage::new();
        let key = NodeId::from_sha1(b"missing");
        assert_eq!(s.get(&key), None);
    }

    #[test]
    fn len_tracks_entries() {
        let mut s = Storage::new();
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());

        let k1 = NodeId::from_sha1(b"a");
        let k2 = NodeId::from_sha1(b"b");
        s.put(k1, b"v1".to_vec(), true);
        assert_eq!(s.len(), 1);
        assert!(!s.is_empty());

        s.put(k2, b"v2".to_vec(), false);
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn overwrite() {
        let mut s = Storage::new();
        let key = NodeId::from_sha1(b"key");
        s.put(key, b"v1".to_vec(), true);
        s.put(key, b"v2".to_vec(), false);
        assert_eq!(
            s.get(&key),
            Some(b"v2".as_slice())
        );
    }
}
