//! In-memory key-value store with expiry and
//! republishing support.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::node_id::{Key, NodeId};

struct StoredValue {
    value: Vec<u8>,
    expires_at: Instant,
    republished_at: Instant,
    received_store_at: Instant,
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
        ttl: Duration,
    ) {
        let now = Instant::now();
        self.entries.insert(
            key,
            StoredValue {
                value,
                expires_at: now + ttl,
                republished_at: now,
                // Original publisher: set far in the
                // past so we don't skip our first
                // republish. Non-original: a peer just
                // stored to us, so set to now.
                received_store_at: if is_original {
                    now - Duration::from_secs(7200)
                } else {
                    now
                },
            },
        );
    }

    /// Look up a value by key.
    #[must_use]
    pub fn get(&self, key: &Key) -> Option<&[u8]> {
        self.entries
            .get(key)
            .map(|v| v.value.as_slice())
    }

    /// Return the number of stored entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return true if no entries are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove entries past their expiry deadline.
    pub fn remove_expired(&mut self) {
        let now = Instant::now();
        self.entries
            .retain(|_, v| now < v.expires_at);
    }

    /// Return values due for republishing and reset
    /// their timers. Skips entries that recently
    /// received a STORE from a peer (Section 2.5
    /// optimization).
    pub fn values_to_republish(
        &mut self,
        interval: Duration,
    ) -> Vec<(Key, Vec<u8>)> {
        let mut result = Vec::new();
        let now = Instant::now();
        for (key, stored) in &mut self.entries {
            if stored.republished_at.elapsed()
                >= interval
                && stored
                    .received_store_at
                    .elapsed()
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

    /// Return entries where `other` is closer to the
    /// key than `local` (for new-node STORE transfer).
    pub fn keys_closer_to(
        &self,
        local: &NodeId,
        other: &NodeId,
    ) -> Vec<(Key, Vec<u8>)> {
        self.entries
            .iter()
            .filter(|(key, _)| {
                other.distance(key)
                    < local.distance(key)
            })
            .map(|(key, stored)| {
                (*key, stored.value.clone())
            })
            .collect()
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
        s.put(
            key,
            b"hello".to_vec(),
            true,
            Duration::from_secs(86400),
        );
        assert_eq!(
            s.get(&key),
            Some(b"hello".as_slice()),
        );
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
        s.put(
            k1,
            b"v1".to_vec(),
            true,
            Duration::from_secs(86400),
        );
        assert_eq!(s.len(), 1);
        assert!(!s.is_empty());

        s.put(
            k2,
            b"v2".to_vec(),
            false,
            Duration::from_secs(86400),
        );
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn overwrite() {
        let mut s = Storage::new();
        let key = NodeId::from_sha1(b"key");
        s.put(
            key,
            b"v1".to_vec(),
            true,
            Duration::from_secs(86400),
        );
        s.put(
            key,
            b"v2".to_vec(),
            false,
            Duration::from_secs(86400),
        );
        assert_eq!(
            s.get(&key),
            Some(b"v2".as_slice()),
        );
    }

    #[test]
    fn keys_closer_to_returns_matching_entries() {
        let mut s = Storage::new();
        // local = all 0xFF, other = all 0x01
        let local =
            NodeId::from_bytes([0xFF; 20]);
        let other =
            NodeId::from_bytes([0x01; 20]);
        // key = all zeros — other is closer
        let close_key =
            NodeId::from_bytes([0x00; 20]);
        // key = all 0xFE — local is closer
        let far_key =
            NodeId::from_bytes([0xFE; 20]);

        s.put(
            close_key,
            b"yes".to_vec(),
            false,
            Duration::from_secs(86400),
        );
        s.put(
            far_key,
            b"no".to_vec(),
            false,
            Duration::from_secs(86400),
        );

        let result =
            s.keys_closer_to(&local, &other);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, close_key);
        assert_eq!(result[0].1, b"yes");
    }

    #[test]
    fn keys_closer_to_empty_storage() {
        let s = Storage::new();
        let local =
            NodeId::from_bytes([0xFF; 20]);
        let other =
            NodeId::from_bytes([0x01; 20]);
        let result =
            s.keys_closer_to(&local, &other);
        assert!(result.is_empty());
    }

    #[test]
    fn put_with_ttl_expires_entry() {
        let mut s = Storage::new();
        let key = NodeId::from_sha1(b"ephemeral");
        // Zero-duration TTL: already expired.
        s.put(
            key,
            b"gone".to_vec(),
            false,
            Duration::ZERO,
        );
        s.remove_expired();
        assert_eq!(s.get(&key), None);
    }

    #[test]
    fn put_with_long_ttl_keeps_entry() {
        let mut s = Storage::new();
        let key = NodeId::from_sha1(b"durable");
        s.put(
            key,
            b"here".to_vec(),
            true,
            Duration::from_secs(86400),
        );
        s.remove_expired();
        assert_eq!(
            s.get(&key),
            Some(b"here".as_slice())
        );
    }

    #[test]
    fn non_original_values_are_republished() {
        let mut s = Storage::new();
        let key = NodeId::from_sha1(b"replica");
        s.put(
            key,
            b"data".to_vec(),
            false,
            Duration::from_secs(86400),
        );

        // Simulate time passing by resetting timers.
        let entry =
            s.entries.get_mut(&key).unwrap();
        entry.republished_at =
            Instant::now() - Duration::from_secs(7200);
        entry.received_store_at =
            Instant::now() - Duration::from_secs(7200);

        let result = s.values_to_republish(
            Duration::from_secs(3600),
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, key);
    }

    #[test]
    fn skip_republish_after_recent_store() {
        let mut s = Storage::new();
        let key = NodeId::from_sha1(b"recent");
        s.put(
            key,
            b"data".to_vec(),
            false,
            Duration::from_secs(86400),
        );

        // republished_at is old, but
        // received_store_at is recent.
        let entry =
            s.entries.get_mut(&key).unwrap();
        entry.republished_at =
            Instant::now() - Duration::from_secs(7200);
        // received_store_at stays at now.

        let result = s.values_to_republish(
            Duration::from_secs(3600),
        );
        assert_eq!(result.len(), 0);
    }
}
