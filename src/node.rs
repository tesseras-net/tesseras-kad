//! Kademlia DHT node — the main entry point.
//!
//! A [`Node`] binds a UDP socket, maintains a routing
//! table, and provides the four Kademlia RPCs: ping,
//! store, find_node, and find_value.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::bucket::K;
use crate::contact::Contact;
use crate::error::{Error, Result};
use crate::message::{
    FindValueResult, Header, Message,
};
use crate::node_id::{Key, NodeId};
use crate::routing::RoutingTable;
use crate::rpc::{Inbound, Rpc};
use crate::storage::Storage;

const ALPHA: usize = 3;
const REFRESH_INTERVAL: Duration =
    Duration::from_secs(3600);
const REPUBLISH_INTERVAL: Duration =
    Duration::from_secs(3600);
const EXPIRY_TTL: Duration =
    Duration::from_secs(86400);
const MAX_VALUE_SIZE: usize = 65000;

/// A Kademlia DHT node.
///
/// Created via [`Node::new`] or [`Node::with_id`],
/// then joined to the network with [`Node::join`].
pub struct Node {
    /// This node's identifier.
    pub id: NodeId,
    rpc: Arc<Rpc>,
    routing_table: Mutex<RoutingTable>,
    storage: Mutex<Storage>,
    inbound_rx: Mutex<Receiver<Inbound>>,
}

impl Node {
    /// Create a node with a random ID, bound to
    /// `addr`.
    pub fn new(
        addr: SocketAddr,
    ) -> Result<Arc<Self>> {
        let id = NodeId::random();
        Self::with_id(id, addr)
    }

    /// Create a node with a specific ID, bound to
    /// `addr`.
    pub fn with_id(
        id: NodeId,
        addr: SocketAddr,
    ) -> Result<Arc<Self>> {
        let (rpc, inbound_rx) =
            Rpc::bind(addr, id)?;
        Ok(Arc::new(Node {
            id,
            rpc: Arc::new(rpc),
            routing_table: Mutex::new(
                RoutingTable::new(id),
            ),
            storage: Mutex::new(Storage::new()),
            inbound_rx: Mutex::new(inbound_rx),
        }))
    }

    /// Return the local socket address.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.rpc.local_addr()
    }

    /// Ping a remote node, returning its node ID.
    pub fn ping(
        &self,
        addr: SocketAddr,
    ) -> Result<NodeId> {
        let (header, _) = self.rpc.send_request(
            addr,
            Message::PingRequest,
        )?;
        let contact =
            Contact::new(header.sender_id, addr);
        self.try_insert(contact);
        Ok(header.sender_id)
    }

    /// Return the number of contacts in the routing
    /// table.
    pub fn routing_table_size(&self) -> usize {
        let rt =
            self.routing_table.lock().unwrap();
        rt.total_contacts()
    }

    /// Return the number of values in local storage.
    pub fn storage_count(&self) -> usize {
        let storage =
            self.storage.lock().unwrap();
        storage.len()
    }

    /// Join the network via a bootstrap node.
    pub fn join(
        self: &Arc<Self>,
        bootstrap: SocketAddr,
    ) -> Result<()> {
        // Ping the bootstrap node.
        let (header, _) = self.rpc.send_request(
            bootstrap,
            Message::PingRequest,
        )?;

        let bootstrap_contact = Contact::new(
            header.sender_id,
            bootstrap,
        );
        self.try_insert(bootstrap_contact);

        // Self-lookup to populate routing table.
        self.find_node(self.id)?;

        // Refresh far buckets.
        let stale = {
            let rt =
                self.routing_table.lock().unwrap();
            rt.stale_buckets(REFRESH_INTERVAL)
        };
        for idx in stale {
            let target = {
                let rt = self
                    .routing_table
                    .lock()
                    .unwrap();
                rt.random_id_in_bucket(idx)
            };
            let _ = self.find_node(target);
        }

        Ok(())
    }

    /// Iterative node lookup (Kademlia Section 2.3).
    pub fn find_node(
        &self,
        target: NodeId,
    ) -> Result<Vec<Contact>> {
        let initial = {
            let rt =
                self.routing_table.lock().unwrap();
            rt.closest(&target, K)
        };

        if initial.is_empty() {
            return Ok(Vec::new());
        }

        self.iterative_lookup(
            target,
            initial,
            false,
        )
        .map(|r| r.0)
    }

    /// Iterative value lookup.
    pub fn find_value(
        &self,
        key: Key,
    ) -> Result<Vec<u8>> {
        // Check local storage first.
        {
            let storage =
                self.storage.lock().unwrap();
            if let Some(val) = storage.get(&key) {
                return Ok(val.to_vec());
            }
        }

        let initial = {
            let rt =
                self.routing_table.lock().unwrap();
            rt.closest(&key, K)
        };

        if initial.is_empty() {
            return Err(Error::NotFound);
        }

        let (contacts, value) = self
            .iterative_lookup(key, initial, true)?;

        match value {
            Some(v) => {
                // Cache at closest node that
                // didn't return the value.
                if let Some(closest) =
                    contacts.first()
                {
                    let _ = self.rpc.send_request(
                        closest.addr,
                        Message::StoreRequest {
                            key,
                            value: v.clone(),
                        },
                    );
                }
                Ok(v)
            }
            None => Err(Error::NotFound),
        }
    }

    /// Store a value in the DHT.
    pub fn store(
        &self,
        key: Key,
        value: Vec<u8>,
    ) -> Result<()> {
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::ValueTooLarge);
        }

        let targets = self.find_node(key)?;
        for contact in &targets {
            let _ = self.rpc.send_request(
                contact.addr,
                Message::StoreRequest {
                    key,
                    value: value.clone(),
                },
            );
        }

        // Also store locally.
        {
            let mut storage =
                self.storage.lock().unwrap();
            storage.put(key, value, true);
        }

        Ok(())
    }

    /// Run the node event loop. Blocks the current
    /// thread.
    pub fn run(self: &Arc<Self>) {
        let node = Arc::clone(self);
        let refresh = thread::spawn({
            let node = Arc::clone(&node);
            move || node.refresh_loop()
        });

        let republish = thread::spawn({
            let node = Arc::clone(&node);
            move || node.republish_loop()
        });

        let expiry = thread::spawn({
            let node = Arc::clone(&node);
            move || node.expiry_loop()
        });

        // Process inbound requests.
        loop {
            let inbound = {
                let rx = self
                    .inbound_rx
                    .lock()
                    .unwrap();
                rx.recv_timeout(
                    Duration::from_millis(100),
                )
            };

            match inbound {
                Ok(msg) => {
                    self.handle_request(
                        msg.from,
                        msg.header,
                        msg.message,
                    );
                }
                Err(
                    std::sync::mpsc::RecvTimeoutError::Timeout,
                ) => continue,
                Err(_) => break,
            }
        }

        let _ = refresh.join();
        let _ = republish.join();
        let _ = expiry.join();
    }

    fn handle_request(
        &self,
        from: SocketAddr,
        header: Header,
        msg: Message,
    ) {
        // Update routing table with sender.
        let sender_contact = Contact::new(
            header.sender_id,
            from,
        );
        self.try_insert(sender_contact);

        let response = match msg {
            Message::PingRequest => {
                Message::PingResponse
            }
            Message::StoreRequest { key, value } => {
                let mut storage =
                    self.storage.lock().unwrap();
                storage.put(key, value, false);
                Message::StoreResponse
            }
            Message::FindNodeRequest { target } => {
                let rt = self
                    .routing_table
                    .lock()
                    .unwrap();
                let contacts =
                    rt.closest(&target, K);
                Message::FindNodeResponse {
                    contacts,
                }
            }
            Message::FindValueRequest { key } => {
                let storage =
                    self.storage.lock().unwrap();
                if let Some(val) = storage.get(&key)
                {
                    Message::FindValueResponse(
                        FindValueResult::Value(
                            val.to_vec(),
                        ),
                    )
                } else {
                    drop(storage);
                    let rt = self
                        .routing_table
                        .lock()
                        .unwrap();
                    let contacts =
                        rt.closest(&key, K);
                    Message::FindValueResponse(
                        FindValueResult::Contacts(
                            contacts,
                        ),
                    )
                }
            }
            _ => return, // Ignore responses.
        };

        let _ = self.rpc.send_response(
            from,
            header.rpc_id,
            response,
        );
    }

    fn try_insert(&self, contact: Contact) {
        if contact.node_id == self.id {
            return;
        }
        let result = {
            let mut rt =
                self.routing_table.lock().unwrap();
            rt.insert(contact.clone())
        };

        if let crate::bucket::InsertResult::BucketFull {
            lru,
        } = result
        {
            // Ping the LRU. If it responds, keep
            // it; otherwise evict.
            let ping_result = self.rpc.send_request(
                lru.addr,
                Message::PingRequest,
            );
            let mut rt =
                self.routing_table.lock().unwrap();
            match ping_result {
                Ok(_) => {
                    rt.mark_seen(&lru.node_id);
                }
                Err(_) => {
                    rt.evict_and_insert(
                        &lru.node_id,
                        contact,
                    );
                }
            }
        }
    }

    fn iterative_lookup(
        &self,
        target: NodeId,
        seed: Vec<Contact>,
        find_value: bool,
    ) -> Result<(Vec<Contact>, Option<Vec<u8>>)> {
        let mut shortlist: Vec<Contact> = seed;
        let mut queried: HashSet<NodeId> =
            HashSet::new();
        let mut closest_seen: Vec<Contact> =
            Vec::new();

        loop {
            // Sort shortlist by distance to target.
            shortlist.sort_by(|a, b| {
                let da = target.distance(&a.node_id);
                let db = target.distance(&b.node_id);
                da.cmp(&db)
            });

            // Pick ALPHA unqueried from closest.
            let to_query: Vec<Contact> = shortlist
                .iter()
                .filter(|c| {
                    !queried.contains(&c.node_id)
                })
                .take(ALPHA)
                .cloned()
                .collect();

            if to_query.is_empty() {
                break;
            }

            let mut improved = false;

            for contact in &to_query {
                queried.insert(contact.node_id);

                let msg = if find_value {
                    Message::FindValueRequest {
                        key: target,
                    }
                } else {
                    Message::FindNodeRequest {
                        target,
                    }
                };

                match self
                    .rpc
                    .send_request(contact.addr, msg)
                {
                    Ok((header, response)) => {
                        // Update routing table.
                        {
                            let mut rt = self
                                .routing_table
                                .lock()
                                .unwrap();
                            rt.mark_seen(
                                &header.sender_id,
                            );
                        }

                        match response {
                            Message::FindNodeResponse {
                                contacts,
                            } => {
                                for c in contacts {
                                    if !queried
                                        .contains(
                                            &c.node_id,
                                        )
                                    {
                                        shortlist
                                            .push(c);
                                        improved =
                                            true;
                                    }
                                }
                            }
                            Message::FindValueResponse(
                                FindValueResult::Value(v),
                            ) => {
                                return Ok((
                                    shortlist, Some(v),
                                ));
                            }
                            Message::FindValueResponse(
                                FindValueResult::Contacts(
                                    contacts,
                                ),
                            ) => {
                                for c in contacts {
                                    if !queried
                                        .contains(
                                            &c.node_id,
                                        )
                                    {
                                        shortlist
                                            .push(c);
                                        improved =
                                            true;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(_) => {
                        let mut rt = self
                            .routing_table
                            .lock()
                            .unwrap();
                        rt.record_failure(
                            &contact.node_id,
                        );
                    }
                }
            }

            if !improved {
                // Query all remaining unqueried in
                // K closest.
                shortlist.sort_by(|a, b| {
                    let da =
                        target.distance(&a.node_id);
                    let db =
                        target.distance(&b.node_id);
                    da.cmp(&db)
                });
                shortlist.dedup_by(|a, b| {
                    a.node_id == b.node_id
                });

                let remaining: Vec<Contact> =
                    shortlist
                        .iter()
                        .filter(|c| {
                            !queried
                                .contains(&c.node_id)
                        })
                        .take(K)
                        .cloned()
                        .collect();

                if remaining.is_empty() {
                    break;
                }

                for contact in &remaining {
                    queried.insert(contact.node_id);

                    let msg = if find_value {
                        Message::FindValueRequest {
                            key: target,
                        }
                    } else {
                        Message::FindNodeRequest {
                            target,
                        }
                    };

                    match self.rpc.send_request(
                        contact.addr,
                        msg,
                    ) {
                        Ok((header, response)) => {
                            {
                                let mut rt = self
                                    .routing_table
                                    .lock()
                                    .unwrap();
                                rt.mark_seen(
                                    &header
                                        .sender_id,
                                );
                            }
                            match response {
                                Message::FindNodeResponse {
                                    contacts,
                                } => {
                                    for c in
                                        contacts
                                    {
                                        if !queried.contains(&c.node_id) {
                                            closest_seen.push(c);
                                        }
                                    }
                                }
                                Message::FindValueResponse(
                                    FindValueResult::Value(v),
                                ) => {
                                    return Ok((
                                        shortlist,
                                        Some(v),
                                    ));
                                }
                                Message::FindValueResponse(
                                    FindValueResult::Contacts(contacts),
                                ) => {
                                    for c in
                                        contacts
                                    {
                                        if !queried.contains(&c.node_id) {
                                            closest_seen.push(c);
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        Err(_) => {
                            let mut rt = self
                                .routing_table
                                .lock()
                                .unwrap();
                            rt.record_failure(
                                &contact.node_id,
                            );
                        }
                    }
                }
                break;
            }
        }

        // Merge and sort final results.
        shortlist.extend(closest_seen);
        shortlist.sort_by(|a, b| {
            let da = target.distance(&a.node_id);
            let db = target.distance(&b.node_id);
            da.cmp(&db)
        });
        shortlist
            .dedup_by(|a, b| a.node_id == b.node_id);
        shortlist.truncate(K);

        Ok((shortlist, None))
    }

    fn refresh_loop(&self) {
        loop {
            thread::sleep(REFRESH_INTERVAL);
            let stale = {
                let rt = self
                    .routing_table
                    .lock()
                    .unwrap();
                rt.stale_buckets(REFRESH_INTERVAL)
            };
            for idx in stale {
                let target = {
                    let rt = self
                        .routing_table
                        .lock()
                        .unwrap();
                    rt.random_id_in_bucket(idx)
                };
                let _ = self.find_node(target);
            }
        }
    }

    fn republish_loop(&self) {
        loop {
            thread::sleep(REPUBLISH_INTERVAL);
            let to_republish = {
                let mut storage =
                    self.storage.lock().unwrap();
                storage.values_to_republish(
                    REPUBLISH_INTERVAL,
                )
            };
            for (key, value) in to_republish {
                let _ = self.store(key, value);
            }
        }
    }

    fn expiry_loop(&self) {
        loop {
            thread::sleep(Duration::from_secs(300));
            let mut storage =
                self.storage.lock().unwrap();
            storage.remove_expired(EXPIRY_TTL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn localhost() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    #[test]
    fn create_node() {
        let node = Node::new(localhost()).unwrap();
        let addr = node.local_addr().unwrap();
        assert_ne!(addr.port(), 0);
    }

    #[test]
    fn two_node_bootstrap() {
        let node_a =
            Node::new(localhost()).unwrap();
        let addr_a =
            node_a.local_addr().unwrap();

        // Run node_a in a thread.
        let a_handle = {
            let node_a = Arc::clone(&node_a);
            thread::spawn(move || node_a.run())
        };

        // Give node_a time to start listening.
        thread::sleep(Duration::from_millis(100));

        let node_b =
            Node::new(localhost()).unwrap();
        node_b.join(addr_a).unwrap();

        // Verify routing tables are populated.
        let rt_b =
            node_b.routing_table.lock().unwrap();
        assert!(rt_b.total_contacts() > 0);

        // Clean up: drop forces Rpc to close.
        drop(rt_b);
        drop(node_b);
        drop(node_a);
        // a_handle will be detached.
        drop(a_handle);
    }

    #[test]
    fn ping_returns_remote_id() {
        let node_a =
            Node::new(localhost()).unwrap();
        let addr_a =
            node_a.local_addr().unwrap();

        let a_handle = {
            let node_a = Arc::clone(&node_a);
            thread::spawn(move || node_a.run())
        };

        thread::sleep(Duration::from_millis(100));

        let node_b =
            Node::new(localhost()).unwrap();
        let remote_id =
            node_b.ping(addr_a).unwrap();
        assert_eq!(remote_id, node_a.id);

        drop(node_b);
        drop(node_a);
        drop(a_handle);
    }

    #[test]
    fn routing_table_size_and_storage_count() {
        let node_a =
            Node::new(localhost()).unwrap();
        let addr_a =
            node_a.local_addr().unwrap();

        let a_handle = {
            let node_a = Arc::clone(&node_a);
            thread::spawn(move || node_a.run())
        };

        thread::sleep(Duration::from_millis(100));

        let node_b =
            Node::new(localhost()).unwrap();
        assert_eq!(node_b.routing_table_size(), 0);
        assert_eq!(node_b.storage_count(), 0);

        node_b.join(addr_a).unwrap();
        assert!(node_b.routing_table_size() > 0);

        let key = NodeId::from_sha1(b"count");
        node_b
            .store(key, b"val".to_vec())
            .unwrap();
        assert!(node_b.storage_count() > 0);

        drop(node_b);
        drop(node_a);
        drop(a_handle);
    }

    #[test]
    fn store_and_retrieve() {
        let node_a =
            Node::new(localhost()).unwrap();
        let addr_a =
            node_a.local_addr().unwrap();

        let a_handle = {
            let node_a = Arc::clone(&node_a);
            thread::spawn(move || node_a.run())
        };

        thread::sleep(Duration::from_millis(100));

        let node_b =
            Node::new(localhost()).unwrap();
        node_b.join(addr_a).unwrap();

        let key = NodeId::from_sha1(b"hello");
        node_b
            .store(key, b"world".to_vec())
            .unwrap();

        // Retrieve from node_b (which should have
        // it locally).
        let val = node_b.find_value(key).unwrap();
        assert_eq!(val, b"world");

        drop(node_b);
        drop(node_a);
        drop(a_handle);
    }
}
