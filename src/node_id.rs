//! 160-bit node identifiers and XOR distance metric.

use std::fmt;

use crate::rand;
use crate::sha1;

/// A 160-bit identifier for a node or stored key.
///
/// Node IDs live in a 160-bit keyspace where
/// distance is measured by XOR (Kademlia Section 2.1).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub(crate) [u8; 20]);

/// The XOR distance between two [`NodeId`]s.
///
/// Ordered numerically as a big-endian 160-bit
/// unsigned integer.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Distance(pub(crate) [u8; 20]);

/// Type alias — a storage key is the same type as a
/// node identifier.
pub type Key = NodeId;

impl NodeId {
    /// Generate a random node ID using the platform
    /// CSPRNG.
    pub fn random() -> Self {
        let mut bytes = [0u8; 20];
        rand::fill_bytes(&mut bytes);
        NodeId(bytes)
    }

    /// Compute the SHA-1 hash of `data` and use it as
    /// a node ID.
    pub fn from_sha1(data: &[u8]) -> Self {
        NodeId(sha1::sha1(data))
    }

    /// Create a node ID from a raw 20-byte array.
    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        NodeId(bytes)
    }

    /// Return the underlying 20-byte array.
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Compute the XOR distance to `other`.
    pub fn distance(&self, other: &NodeId) -> Distance {
        let mut d = [0u8; 20];
        for (i, byte) in d.iter_mut().enumerate() {
            *byte = self.0[i] ^ other.0[i];
        }
        Distance(d)
    }

    /// Number of leading zero bits in the XOR distance.
    /// Returns 0..=159 for distinct IDs, 160 for identical.
    pub fn common_prefix_length(
        &self,
        other: &NodeId,
    ) -> usize {
        let dist = self.distance(other);
        for (i, &byte) in dist.0.iter().enumerate() {
            if byte != 0 {
                return i * 8 + byte.leading_zeros() as usize;
            }
        }
        160
    }
}

impl PartialOrd for Distance {
    fn partial_cmp(
        &self,
        other: &Self,
    ) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Distance {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({}..)", hex_encode(&self.0[..4]))
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex_encode(&self.0[..8]))
    }
}

impl fmt::Debug for Distance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Dist({}..)", hex_encode(&self.0[..4]))
    }
}

impl fmt::Display for Distance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex_encode(&self.0[..8]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_distance_is_zero() {
        let id = NodeId::random();
        assert_eq!(id.distance(&id), Distance([0u8; 20]));
    }

    #[test]
    fn distance_symmetry() {
        let a = NodeId::random();
        let b = NodeId::random();
        assert_eq!(a.distance(&b), b.distance(&a));
    }

    #[test]
    fn cpl_identical() {
        let id = NodeId::random();
        assert_eq!(id.common_prefix_length(&id), 160);
    }

    #[test]
    fn cpl_high_bit_differs() {
        let a = NodeId::from_bytes([0x00; 20]);
        let mut b_bytes = [0x00u8; 20];
        b_bytes[0] = 0x80;
        let b = NodeId::from_bytes(b_bytes);
        assert_eq!(a.common_prefix_length(&b), 0);
    }

    #[test]
    fn cpl_known_pattern() {
        let a = NodeId::from_bytes([0x00; 20]);
        let mut b_bytes = [0x00u8; 20];
        b_bytes[1] = 0x01; // bit 15
        let b = NodeId::from_bytes(b_bytes);
        assert_eq!(a.common_prefix_length(&b), 15);
    }

    #[test]
    fn distance_ordering() {
        let a = Distance([0x00; 20]);
        let mut b_arr = [0x00u8; 20];
        b_arr[0] = 0x01;
        let b = Distance(b_arr);
        assert!(a < b);
    }

    #[test]
    fn triangle_inequality() {
        let a = NodeId::random();
        let b = NodeId::random();
        let c = NodeId::random();
        let ab = a.distance(&b);
        let bc = b.distance(&c);
        let ac = a.distance(&c);
        // XOR triangle inequality: d(a,c) <= d(a,b) XOR d(b,c)
        // For XOR metric it's actually d(a,c) <= d(a,b) + d(b,c)
        // in terms of numeric value
        let ab_val = u128::from_be_bytes(
            ab.0[..16].try_into().unwrap(),
        );
        let bc_val = u128::from_be_bytes(
            bc.0[..16].try_into().unwrap(),
        );
        let ac_val = u128::from_be_bytes(
            ac.0[..16].try_into().unwrap(),
        );
        assert!(
            ac_val <= ab_val.saturating_add(bc_val)
        );
    }

    #[test]
    fn from_sha1_deterministic() {
        let a = NodeId::from_sha1(b"hello");
        let b = NodeId::from_sha1(b"hello");
        assert_eq!(a, b);
    }
}
