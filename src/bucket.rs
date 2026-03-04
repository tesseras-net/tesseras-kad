//! Kademlia k-bucket with replacement cache.

use std::time::{Duration, Instant};

use crate::contact::{Contact, ContactEntry};
use crate::node_id::NodeId;

/// Maximum number of contacts per k-bucket.
pub const K: usize = 20;

/// Result of inserting a contact into a [`KBucket`].
pub enum InsertResult {
    /// The contact was added to the bucket.
    Inserted,
    /// An existing contact was moved to the tail.
    Updated,
    /// The bucket is full. Contains the least-recently
    /// seen contact that should be pinged to decide
    /// eviction.
    BucketFull { lru: Contact },
}

/// A k-bucket holding up to [`K`] contacts ordered
/// by last-seen time, with a bounded replacement
/// cache for pending insertions.
pub struct KBucket {
    entries: Vec<ContactEntry>,
    cache: Vec<ContactEntry>,
    pub last_updated: Instant,
}

impl Default for KBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl KBucket {
    /// Create an empty k-bucket.
    pub fn new() -> Self {
        KBucket {
            entries: Vec::new(),
            cache: Vec::new(),
            last_updated: Instant::now(),
        }
    }

    /// Return the number of contacts in the bucket.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` if the bucket has no contacts.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return `true` if the replacement cache is empty.
    pub fn cache_is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Insert or update a contact.
    ///
    /// If the contact is already present it is moved
    /// to the tail. If the bucket is full the contact
    /// is placed in the replacement cache and the LRU
    /// entry is returned for liveness checking.
    pub fn insert(
        &mut self,
        contact: Contact,
    ) -> InsertResult {
        self.last_updated = Instant::now();

        // If already present, move to tail (most recent).
        if let Some(pos) = self
            .entries
            .iter()
            .position(|e| {
                e.contact.node_id == contact.node_id
            })
        {
            self.entries[pos].mark_seen();
            let entry = self.entries.remove(pos);
            self.entries.push(entry);
            return InsertResult::Updated;
        }

        if self.entries.len() < K {
            self.entries
                .push(ContactEntry::new(contact));
            return InsertResult::Inserted;
        }

        // Bucket full: add to replacement cache.
        self.add_to_cache(contact.clone());
        InsertResult::BucketFull {
            lru: self.entries[0].contact.clone(),
        }
    }

    /// Remove the contact with the given ID.
    pub fn remove(&mut self, id: &NodeId) {
        self.entries
            .retain(|e| e.contact.node_id != *id);
    }

    /// Mark a contact as seen, moving it to the tail.
    pub fn mark_seen(&mut self, id: &NodeId) {
        if let Some(pos) = self
            .entries
            .iter()
            .position(|e| e.contact.node_id == *id)
        {
            self.entries[pos].mark_seen();
            let entry = self.entries.remove(pos);
            self.entries.push(entry);
            self.last_updated = Instant::now();
        }
    }

    /// Evict the LRU entry and insert a new contact,
    /// or promote from replacement cache.
    ///
    /// If `old_id` is no longer present and the bucket
    /// is already full, the insertion is skipped to
    /// avoid exceeding K entries.
    pub fn evict_and_insert(
        &mut self,
        old_id: &NodeId,
        new: Option<Contact>,
    ) {
        let had_entry = self
            .entries
            .iter()
            .any(|e| e.contact.node_id == *old_id);
        self.remove(old_id);
        if !had_entry && self.entries.len() >= K {
            return;
        }
        if let Some(contact) = new {
            self.cache.retain(|e| {
                e.contact.node_id != contact.node_id
            });
            self.entries
                .push(ContactEntry::new(contact));
        } else if let Some(cached) = self.cache.pop() {
            self.entries.push(cached);
        }
        self.last_updated = Instant::now();
    }

    /// Return the `count` contacts closest to
    /// `target`, sorted by XOR distance.
    pub fn closest(
        &self,
        target: &NodeId,
        count: usize,
    ) -> Vec<Contact> {
        let mut sorted: Vec<_> = self
            .entries
            .iter()
            .map(|e| {
                (
                    target.distance(
                        &e.contact.node_id,
                    ),
                    e.contact.clone(),
                )
            })
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        sorted
            .into_iter()
            .take(count)
            .map(|(_, c)| c)
            .collect()
    }

    /// Return `true` if the bucket has not been
    /// updated within `threshold`.
    pub fn is_stale(
        &self,
        threshold: Duration,
    ) -> bool {
        self.last_updated.elapsed() > threshold
    }

    /// Return a slice of all entries.
    pub fn entries(&self) -> &[ContactEntry] {
        &self.entries
    }

    /// Return a mutable slice of all entries.
    pub fn entries_mut(
        &mut self,
    ) -> &mut [ContactEntry] {
        &mut self.entries
    }

    /// Consume the bucket and return its entries.
    pub fn into_entries(self) -> Vec<ContactEntry> {
        self.entries
    }

    /// Consume the bucket and return its cache.
    pub fn into_cache(self) -> Vec<ContactEntry> {
        self.cache
    }

    /// Consume the bucket, returning entries and cache.
    pub fn into_parts(
        self,
    ) -> (Vec<ContactEntry>, Vec<ContactEntry>) {
        (self.entries, self.cache)
    }

    /// Insert a pre-existing entry, preserving its
    /// liveness metadata. Used during bucket splitting.
    pub fn insert_entry(&mut self, entry: ContactEntry) {
        if self.entries.len() < K {
            self.entries.push(entry);
        }
    }

    /// Add a pre-existing entry to the replacement
    /// cache, preserving its metadata. Used during
    /// bucket splitting.
    pub fn add_to_cache_entry(
        &mut self,
        entry: ContactEntry,
    ) {
        if self.cache.len() < K {
            self.cache.push(entry);
        }
    }

    fn add_to_cache(&mut self, contact: Contact) {
        // Keep cache bounded.
        if self.cache.len() >= K {
            self.cache.remove(0);
        }
        self.cache
            .push(ContactEntry::new(contact));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn make_contact(id_byte: u8) -> Contact {
        Contact::new(
            NodeId::from_bytes([id_byte; 20]),
            SocketAddr::from(([127, 0, 0, 1], 8000)),
        )
    }

    #[test]
    fn insert_into_empty() {
        let mut bucket = KBucket::new();
        let result =
            bucket.insert(make_contact(1));
        assert!(matches!(
            result,
            InsertResult::Inserted
        ));
        assert_eq!(bucket.len(), 1);
    }

    #[test]
    fn insert_duplicate_updates() {
        let mut bucket = KBucket::new();
        bucket.insert(make_contact(1));
        let result =
            bucket.insert(make_contact(1));
        assert!(matches!(
            result,
            InsertResult::Updated
        ));
        assert_eq!(bucket.len(), 1);
    }

    #[test]
    fn full_bucket_returns_lru() {
        let mut bucket = KBucket::new();
        for i in 0..K {
            bucket.insert(make_contact(i as u8));
        }
        let result =
            bucket.insert(make_contact(0xFF));
        match result {
            InsertResult::BucketFull { lru } => {
                assert_eq!(
                    lru.node_id,
                    NodeId::from_bytes([0u8; 20])
                );
            }
            _ => panic!("expected BucketFull"),
        }
    }

    #[test]
    fn evict_and_insert_replaces() {
        let mut bucket = KBucket::new();
        for i in 0..K {
            bucket.insert(make_contact(i as u8));
        }
        let old =
            bucket.entries[0].contact.node_id;
        bucket.evict_and_insert(
            &old,
            Some(make_contact(0xFF)),
        );
        assert_eq!(bucket.len(), K);
        assert!(bucket.entries.iter().any(|e| {
            e.contact.node_id
                == NodeId::from_bytes(
                    [0xFF; 20],
                )
        }));
    }

    #[test]
    fn evict_none_with_empty_cache_shrinks() {
        let mut bucket = KBucket::new();
        for i in 0..5 {
            bucket.insert(make_contact(i));
        }
        assert_eq!(bucket.len(), 5);
        assert!(bucket.cache_is_empty());

        let old =
            bucket.entries[0].contact.node_id;
        bucket.evict_and_insert(&old, None);
        // With empty cache, entry is removed and
        // nothing replaces it.
        assert_eq!(bucket.len(), 4);
    }

    #[test]
    fn into_entries_returns_all() {
        let mut bucket = KBucket::new();
        for i in 0..5 {
            bucket.insert(make_contact(i));
        }
        let entries = bucket.into_entries();
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn into_cache_returns_cached() {
        let mut bucket = KBucket::new();
        for i in 0..K {
            bucket.insert(make_contact(i as u8));
        }
        // Goes to cache.
        bucket.insert(make_contact(0xFE));
        let cache = bucket.into_cache();
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn insert_entry_preserves_metadata() {
        let mut bucket = KBucket::new();
        let mut entry =
            ContactEntry::new(make_contact(1));
        entry.fail_count = 3;
        bucket.insert_entry(entry);
        assert_eq!(bucket.len(), 1);
        assert_eq!(
            bucket.entries()[0].fail_count,
            3
        );
    }

    #[test]
    fn add_to_cache_entry_preserves_metadata() {
        let mut bucket = KBucket::new();
        let mut entry =
            ContactEntry::new(make_contact(1));
        entry.fail_count = 2;
        bucket.add_to_cache_entry(entry);
        assert!(!bucket.cache_is_empty());
    }

    #[test]
    fn replacement_cache_promotion() {
        let mut bucket = KBucket::new();
        for i in 0..K {
            bucket.insert(make_contact(i as u8));
        }
        // This goes to cache.
        bucket.insert(make_contact(0xFE));
        assert_eq!(bucket.cache.len(), 1);

        // Evict LRU without replacement.
        let old =
            bucket.entries[0].contact.node_id;
        bucket.evict_and_insert(&old, None);
        // Cache entry should be promoted.
        assert_eq!(bucket.len(), K);
        assert!(bucket.cache.is_empty());
    }
}
