//! Kademlia routing table of 160 k-buckets.

use std::time::Duration;

use crate::bucket::{InsertResult, KBucket};
use crate::contact::Contact;
use crate::node_id::NodeId;

const NUM_BUCKETS: usize = 160;

/// A routing table with one [`KBucket`] per bit of
/// the 160-bit keyspace, indexed by the common prefix
/// length between a contact and the local node ID.
pub struct RoutingTable {
    local_id: NodeId,
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    /// Create a routing table for `local_id`.
    pub fn new(local_id: NodeId) -> Self {
        let mut buckets =
            Vec::with_capacity(NUM_BUCKETS);
        for _ in 0..NUM_BUCKETS {
            buckets.push(KBucket::new());
        }
        RoutingTable { local_id, buckets }
    }

    /// Return the local node ID.
    pub fn local_id(&self) -> &NodeId {
        &self.local_id
    }

    fn bucket_index(&self, id: &NodeId) -> usize {
        let cpl =
            self.local_id.common_prefix_length(id);
        if cpl >= NUM_BUCKETS {
            NUM_BUCKETS - 1
        } else {
            cpl
        }
    }

    /// Insert a contact into the appropriate bucket.
    ///
    /// Silently ignores the local node's own ID.
    pub fn insert(
        &mut self,
        contact: Contact,
    ) -> InsertResult {
        if contact.node_id == self.local_id {
            return InsertResult::Updated;
        }
        let idx =
            self.bucket_index(&contact.node_id);
        self.buckets[idx].insert(contact)
    }

    /// Return up to `count` contacts closest to
    /// `target`, sorted by XOR distance.
    pub fn closest(
        &self,
        target: &NodeId,
        count: usize,
    ) -> Vec<Contact> {
        let mut all: Vec<(
            crate::node_id::Distance,
            Contact,
        )> = Vec::new();

        for bucket in &self.buckets {
            for entry in bucket.entries() {
                let dist = target.distance(
                    &entry.contact.node_id,
                );
                all.push((
                    dist,
                    entry.contact.clone(),
                ));
            }
        }

        all.sort_by(|a, b| a.0.cmp(&b.0));
        all.into_iter()
            .take(count)
            .map(|(_, c)| c)
            .collect()
    }

    /// Mark a contact as recently seen.
    pub fn mark_seen(&mut self, id: &NodeId) {
        if *id == self.local_id {
            return;
        }
        let idx = self.bucket_index(id);
        self.buckets[idx].mark_seen(id);
    }

    /// Record an RPC failure for a contact.
    pub fn record_failure(&mut self, id: &NodeId) {
        if *id == self.local_id {
            return;
        }
        let idx = self.bucket_index(id);
        for entry in
            self.buckets[idx].entries_mut()
        {
            if entry.contact.node_id == *id {
                entry.record_failure();
                break;
            }
        }
    }

    /// Evict `old` and insert `new` in the same
    /// bucket.
    pub fn evict_and_insert(
        &mut self,
        old: &NodeId,
        new: Contact,
    ) {
        let idx = self.bucket_index(old);
        self.buckets[idx]
            .evict_and_insert(old, Some(new));
    }

    /// Return indices of buckets not updated within
    /// `threshold`.
    pub fn stale_buckets(
        &self,
        threshold: Duration,
    ) -> Vec<usize> {
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, b)| b.is_stale(threshold))
            .map(|(i, _)| i)
            .collect()
    }

    /// Generate a random ID that falls into bucket
    /// `index`.
    pub fn random_id_in_bucket(
        &self,
        index: usize,
    ) -> NodeId {
        KBucket::random_id_in_range(
            &self.local_id,
            index,
        )
    }

    /// Return the total number of contacts across all
    /// buckets.
    pub fn total_contacts(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn addr() -> SocketAddr {
        "127.0.0.1:8000".parse().unwrap()
    }

    #[test]
    fn does_not_insert_self() {
        let id = NodeId::random();
        let mut rt = RoutingTable::new(id);
        rt.insert(Contact::new(id, addr()));
        assert_eq!(rt.total_contacts(), 0);
    }

    #[test]
    fn correct_bucket_assignment() {
        let local = NodeId::from_bytes([0x00; 20]);
        let rt = RoutingTable::new(local);

        // ID with high bit set -> CPL = 0.
        let mut far = [0x00u8; 20];
        far[0] = 0x80;
        assert_eq!(
            rt.bucket_index(
                &NodeId::from_bytes(far)
            ),
            0
        );

        // ID with only bit 7 set -> CPL = 7.
        let mut mid = [0x00u8; 20];
        mid[0] = 0x01;
        assert_eq!(
            rt.bucket_index(
                &NodeId::from_bytes(mid)
            ),
            7
        );
    }

    #[test]
    fn closest_returns_sorted() {
        let local = NodeId::from_bytes([0x00; 20]);
        let mut rt = RoutingTable::new(local);

        let mut a_bytes = [0x00u8; 20];
        a_bytes[0] = 0x80; // far
        let mut b_bytes = [0x00u8; 20];
        b_bytes[0] = 0x01; // closer

        rt.insert(Contact::new(
            NodeId::from_bytes(a_bytes),
            addr(),
        ));
        rt.insert(Contact::new(
            NodeId::from_bytes(b_bytes),
            addr(),
        ));

        let target = NodeId::from_bytes([0x00; 20]);
        let result = rt.closest(&target, 2);
        assert_eq!(result.len(), 2);

        let d0 = target.distance(&result[0].node_id);
        let d1 = target.distance(&result[1].node_id);
        assert!(d0 <= d1);
    }
}
