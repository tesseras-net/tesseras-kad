//! A Kademlia DHT implementation in pure Rust.
//!
//! Zero external dependencies. Implements the
//! Kademlia protocol (Maymounkov & Mazieres, 2002)
//! with iterative lookups, k-bucket routing, and
//! value storage with expiry and republishing.
//!
//! Start with [`Node::new`] to create a DHT node,
//! [`Node::join`] to connect to the network, then
//! [`Node::store`] and [`Node::find_value`] to
//! put and get values.

pub mod bucket;
pub mod contact;
pub mod error;
pub mod message;
pub mod node;
pub mod node_id;
pub mod rand;
pub mod routing;
pub mod rpc;
pub mod sha1;
pub mod storage;

pub use bucket::K;
pub use error::{Error, Result};
pub use node::Node;
pub use node_id::{Key, NodeId};
