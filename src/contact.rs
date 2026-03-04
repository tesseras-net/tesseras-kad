//! Network contact information for Kademlia nodes.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

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

/// Base backoff interval (seconds) after a failure.
const BACKOFF_BASE_SECS: u64 = 5;

/// A [`Contact`] with liveness metadata used inside
/// k-buckets.
pub struct ContactEntry {
    /// The underlying contact.
    pub contact: Contact,
    /// When this contact was last seen responding.
    pub last_seen: Instant,
    /// Consecutive failed RPCs to this contact.
    pub fail_count: u8,
    /// Do not query this contact before this time
    /// (exponential backoff per paper §4.1).
    pub backoff_until: Option<Instant>,
}

/// A contact is considered stale after this many
/// consecutive failures (paper Section 4.1).
const STALE_THRESHOLD: u8 = 5;

impl ContactEntry {
    /// Create a new entry with `last_seen` set to now
    /// and zero failures.
    pub fn new(contact: Contact) -> Self {
        ContactEntry {
            contact,
            last_seen: Instant::now(),
            fail_count: 0,
            backoff_until: None,
        }
    }

    /// Reset the failure counter, clear backoff,
    /// and update `last_seen` to now.
    pub fn mark_seen(&mut self) {
        self.fail_count = 0;
        self.backoff_until = None;
        self.last_seen = Instant::now();
    }

    /// Increment the failure counter (saturating)
    /// and set exponential backoff.
    pub fn record_failure(&mut self) {
        self.fail_count =
            self.fail_count.saturating_add(1);
        let delay = Duration::from_secs(
            BACKOFF_BASE_SECS
                .saturating_mul(
                    1u64.checked_shl(
                        self.fail_count as u32,
                    )
                    .unwrap_or(u64::MAX),
                ),
        );
        self.backoff_until =
            Some(Instant::now() + delay);
    }

    /// Return `true` if this contact has exceeded the
    /// staleness threshold (5 consecutive failures).
    pub fn is_stale(&self) -> bool {
        self.fail_count >= STALE_THRESHOLD
    }

    /// Return `true` if this contact is currently
    /// backed off and should not be queried.
    pub fn is_backed_off(&self) -> bool {
        self.backoff_until
            .is_some_and(|t| Instant::now() < t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn addr() -> SocketAddr {
        "127.0.0.1:8000".parse().unwrap()
    }

    fn make_entry() -> ContactEntry {
        ContactEntry::new(Contact::new(
            crate::node_id::NodeId::random(),
            addr(),
        ))
    }

    #[test]
    fn new_entry_is_not_backed_off() {
        let entry = make_entry();
        assert!(!entry.is_backed_off());
    }

    #[test]
    fn first_failure_sets_backoff() {
        let mut entry = make_entry();
        entry.record_failure();
        assert!(entry.is_backed_off());
        assert_eq!(entry.fail_count, 1);
    }

    #[test]
    fn mark_seen_clears_backoff() {
        let mut entry = make_entry();
        entry.record_failure();
        assert!(entry.is_backed_off());

        entry.mark_seen();
        assert!(!entry.is_backed_off());
        assert_eq!(entry.fail_count, 0);
    }

    #[test]
    fn backoff_grows_with_failures() {
        let mut entry = make_entry();

        entry.record_failure(); // fail_count=1
        let bo1 = entry.backoff_until.unwrap();

        entry.record_failure(); // fail_count=2
        let bo2 = entry.backoff_until.unwrap();

        // Second backoff should expire later than
        // first (exponential growth).
        assert!(bo2 > bo1);
    }
}
