//! Network contact information for Kademlia nodes.

use std::net::SocketAddr;
use std::time::Instant;

use crate::node_id::NodeId;

/// A remote node's identity and network address.
#[derive(Debug, Clone)]
pub struct Contact {
    /// The node's 160-bit identifier.
    pub node_id: NodeId,
    /// The node's UDP socket address.
    pub addr: SocketAddr,
}

impl Contact {
    /// Create a new contact.
    pub fn new(node_id: NodeId, addr: SocketAddr) -> Self {
        Contact { node_id, addr }
    }
}

/// A [`Contact`] with liveness metadata used inside
/// k-buckets.
pub struct ContactEntry {
    /// The underlying contact.
    pub contact: Contact,
    /// When this contact was last seen responding.
    pub last_seen: Instant,
    /// Consecutive failed RPCs to this contact.
    pub fail_count: u8,
}

impl ContactEntry {
    /// Create a new entry with `last_seen` set to now
    /// and zero failures.
    pub fn new(contact: Contact) -> Self {
        ContactEntry {
            contact,
            last_seen: Instant::now(),
            fail_count: 0,
        }
    }

    /// Reset the failure counter and update
    /// `last_seen` to now.
    pub fn mark_seen(&mut self) {
        self.fail_count = 0;
        self.last_seen = Instant::now();
    }

    /// Increment the failure counter (saturating).
    pub fn record_failure(&mut self) {
        self.fail_count = self.fail_count.saturating_add(1);
    }
}
