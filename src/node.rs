//! Kademlia DHT node — the main entry point.
//!
//! A [`Node`] binds a UDP socket, maintains a routing
//! table, and provides the four Kademlia RPCs: ping,
//! store, find_node, and find_value.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::bucket::K;
use crate::contact::Contact;
use crate::error::{self, Error, Result};
use crate::message::{FindValueResult, Header, Message};
use crate::node_id::{Key, NodeId};
use crate::routing::RoutingTable;
use crate::rpc::{Inbound, Rpc};
use crate::storage::Storage;

/// Result of an iterative lookup: contacts sorted by
/// distance, and optionally the found value with the
/// ID of the node that returned it.
type LookupResult = Result<(Vec<Contact>, Option<(Vec<u8>, NodeId)>)>;

const ALPHA: usize = 3;
const REFRESH_INTERVAL: Duration = Duration::from_secs(3600);
const REPUBLISH_INTERVAL: Duration = Duration::from_secs(3600);
const EXPIRY_TTL: Duration = Duration::from_secs(86400);
const MAX_VALUE_SIZE: usize = 65000;
const MIN_CACHE_TTL: Duration = Duration::from_secs(60);

/// Sort contacts by XOR distance to `target`.
fn sort_by_distance(
    list: &mut [Contact],
    target: &NodeId,
) {
    list.sort_by(|a, b| {
        let da = target.distance(&a.node_id);
        let db = target.distance(&b.node_id);
        da.cmp(&db)
    });
}

/// Result sent back from each query thread.
struct QueryResult {
    contact: Contact,
    result: Result<(Header, Message)>,
}

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
    transfer_tx: Mutex<SyncSender<Contact>>,
    transfer_rx: Mutex<Receiver<Contact>>,
    stop: Arc<AtomicBool>,
}

impl Node {
    /// Create a node with a random ID, bound to
    /// `addr`.
    pub fn new(addr: SocketAddr) -> Result<Arc<Self>> {
        let id = NodeId::random();
        Self::with_id(id, addr)
    }

    /// Create a node with a specific ID, bound to
    /// `addr`.
    pub fn with_id(id: NodeId, addr: SocketAddr) -> Result<Arc<Self>> {
        let (rpc, inbound_rx) = Rpc::bind(addr, id)?;
        let (transfer_tx, transfer_rx) = mpsc::sync_channel(256);
        Ok(Arc::new(Node {
            id,
            rpc: Arc::new(rpc),
            routing_table: Mutex::new(RoutingTable::new(id)),
            storage: Mutex::new(Storage::new()),
            inbound_rx: Mutex::new(inbound_rx),
            transfer_tx: Mutex::new(transfer_tx),
            transfer_rx: Mutex::new(transfer_rx),
            stop: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Return the local socket address.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.rpc.local_addr()
    }

    /// Ping a remote node, returning its node ID.
    pub fn ping(&self, addr: SocketAddr) -> Result<NodeId> {
        let (header, _) = self.rpc.send_request(addr, Message::PingRequest)?;
        let contact = Contact::new(header.sender_id, addr);
        self.try_insert(contact);
        Ok(header.sender_id)
    }

    /// Return the number of contacts in the routing
    /// table.
    pub fn routing_table_size(&self) -> usize {
        let rt = error::lock(&self.routing_table);
        rt.total_contacts()
    }

    /// Return the number of values in local storage.
    pub fn storage_count(&self) -> usize {
        let storage = error::lock(&self.storage);
        storage.len()
    }

    /// Join the network via a bootstrap node.
    pub fn join(self: &Arc<Self>, bootstrap: SocketAddr) -> Result<()> {
        // Ping the bootstrap node.
        let (header, _) =
            self.rpc.send_request(bootstrap, Message::PingRequest)?;

        let bootstrap_contact = Contact::new(header.sender_id, bootstrap);
        self.try_insert(bootstrap_contact);

        // Self-lookup to populate routing table.
        self.find_node(self.id)?;

        // Refresh buckets further than closest
        // neighbor (per paper Section 2.3).
        let targets = {
            let rt = error::lock(&self.routing_table);
            rt.further_refresh_targets()
        };
        for target in targets {
            let _ = self.find_node(target);
        }

        Ok(())
    }

    /// Iterative node lookup (Kademlia Section 2.3).
    pub fn find_node(&self, target: NodeId) -> Result<Vec<Contact>> {
        let initial = {
            let rt = error::lock(&self.routing_table);
            rt.closest(&target, K)
        };

        if initial.is_empty() {
            return Ok(Vec::new());
        }

        self.iterative_lookup(target, initial, false).map(|r| r.0)
    }

    /// Iterative value lookup.
    pub fn find_value(&self, key: Key) -> Result<Vec<u8>> {
        // Check local storage first.
        {
            let storage = error::lock(&self.storage);
            if let Some(val) = storage.get(&key) {
                return Ok(val.to_vec());
            }
        }

        let initial = {
            let rt = error::lock(&self.routing_table);
            rt.closest(&key, K)
        };

        if initial.is_empty() {
            return Err(Error::NotFound);
        }

        let (contacts, value) = self.iterative_lookup(key, initial, true)?;

        match value {
            Some((v, source_id)) => {
                // Cache at closest node that
                // didn't return the value
                // (per Kademlia paper).
                if let Some(closest) = contacts
                    .iter()
                    .find(|c| c.node_id != source_id)
                    && let Ok((h, _)) =
                        self.rpc.send_request(
                            closest.addr,
                            Message::StoreRequest {
                                key,
                                value: v.clone(),
                                is_cache: true,
                            },
                        )
                {
                    self.try_insert(Contact::new(
                        h.sender_id,
                        closest.addr,
                    ));
                }
                Ok(v)
            }
            None => Err(Error::NotFound),
        }
    }

    /// Find the K closest nodes to `key` and send
    /// STORE RPCs to each. Does not modify local
    /// storage.
    fn store_to_closest(
        &self,
        key: Key,
        value: Vec<u8>,
    ) -> Result<()> {
        let targets = self.find_node(key)?;
        self.send_stores(&targets, key, &value);
        Ok(())
    }

    /// Send STORE RPCs to each contact and update the
    /// routing table from responses (paper §2.2).
    fn send_stores(
        &self,
        targets: &[Contact],
        key: Key,
        value: &[u8],
    ) {
        for contact in targets {
            if let Ok((h, _)) =
                self.rpc.send_request(
                    contact.addr,
                    Message::StoreRequest {
                        key,
                        value: value.to_vec(),
                        is_cache: false,
                    },
                )
            {
                self.try_insert(Contact::new(
                    h.sender_id,
                    contact.addr,
                ));
            }
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

        self.store_to_closest(key, value.clone())?;

        // Also store locally as original publisher.
        {
            let mut storage = error::lock(&self.storage);
            storage.put(key, value, true, EXPIRY_TTL);
        }

        Ok(())
    }

    /// Signal this node to stop its event loop and
    /// background threads.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Compute the cache TTL for a key based on how
    /// many known contacts are closer to it than we
    /// are (Kademlia Section 2.3).
    ///
    /// TTL = EXPIRY_TTL / 2^closer_count, clamped to
    /// MIN_CACHE_TTL.
    fn cache_ttl(&self, key: &Key) -> Duration {
        let closer = {
            let rt = error::lock(&self.routing_table);
            rt.count_closer(key)
        };
        let divisor = 1u64.checked_shl(closer as u32).unwrap_or(u64::MAX);
        let ttl_secs = EXPIRY_TTL.as_secs() / divisor;
        let ttl = Duration::from_secs(ttl_secs);
        ttl.max(MIN_CACHE_TTL)
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

        let transfer = thread::spawn({
            let node = Arc::clone(&node);
            move || node.transfer_loop()
        });

        // Process inbound requests.
        loop {
            let inbound = {
                let rx = error::lock(&self.inbound_rx);
                rx.recv_timeout(Duration::from_millis(100))
            };

            match inbound {
                Ok(msg) => {
                    self.handle_request(msg.from, msg.header, msg.message);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(_) => break,
            }
        }

        self.stop.store(true, Ordering::Relaxed);
        let _ = refresh.join();
        let _ = republish.join();
        let _ = expiry.join();
        let _ = transfer.join();
    }

    fn handle_request(&self, from: SocketAddr, header: Header, msg: Message) {
        // Update routing table with sender.
        let sender_contact = Contact::new(header.sender_id, from);
        self.try_insert(sender_contact);

        let response = match msg {
            Message::PingRequest => Message::PingResponse,
            Message::StoreRequest {
                key,
                value,
                is_cache,
            } => {
                if value.len() > MAX_VALUE_SIZE {
                    return;
                }
                let ttl = if is_cache {
                    self.cache_ttl(&key)
                } else {
                    EXPIRY_TTL
                };
                let mut storage =
                    error::lock(&self.storage);
                storage.put(key, value, false, ttl);
                Message::StoreResponse
            }
            Message::FindNodeRequest { target } => {
                let rt = error::lock(&self.routing_table);
                let contacts = rt.closest(&target, K);
                Message::FindNodeResponse { contacts }
            }
            Message::FindValueRequest { key } => {
                let storage = error::lock(&self.storage);
                if let Some(val) = storage.get(&key) {
                    Message::FindValueResponse(FindValueResult::Value(
                        val.to_vec(),
                    ))
                } else {
                    drop(storage);
                    let rt = error::lock(&self.routing_table);
                    let contacts = rt.closest(&key, K);
                    Message::FindValueResponse(FindValueResult::Contacts(
                        contacts,
                    ))
                }
            }
            _ => return, // Ignore responses.
        };

        let _ = self.rpc.send_response(from, header.rpc_id, response);
    }

    fn try_insert(&self, contact: Contact) {
        if contact.node_id == self.id {
            return;
        }
        let result = {
            let mut rt = error::lock(&self.routing_table);
            rt.insert(contact.clone())
        };

        if matches!(result, crate::bucket::InsertResult::Inserted) {
            let tx = error::lock(&self.transfer_tx);
            let _ = tx.try_send(contact.clone());
        }

        if let crate::bucket::InsertResult::BucketFull { lru } =
            result
        {
            // Ping the LRU. If it responds, keep
            // it; otherwise record failure.
            let ping_result =
                self.rpc.send_request(lru.addr, Message::PingRequest);
            let mut rt = error::lock(&self.routing_table);
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

    /// Pick up to `count` unqueried, non-backed-off
    /// contacts closest to `target` from `shortlist`.
    fn pick_candidates(
        &self,
        shortlist: &[Contact],
        queried: &HashSet<NodeId>,
        count: usize,
    ) -> Vec<Contact> {
        let rt = error::lock(&self.routing_table);
        shortlist
            .iter()
            .filter(|c| {
                !queried.contains(&c.node_id)
                    && !rt.is_backed_off(&c.node_id)
            })
            .take(count)
            .cloned()
            .collect()
    }

    /// Spawn a query thread that sends its result on
    /// `tx`.
    fn spawn_query(
        rpc: &Arc<Rpc>,
        contact: Contact,
        target: NodeId,
        find_value: bool,
        tx: &mpsc::Sender<QueryResult>,
    ) {
        let rpc = Arc::clone(rpc);
        let tx = tx.clone();
        let msg = if find_value {
            Message::FindValueRequest { key: target }
        } else {
            Message::FindNodeRequest { target }
        };
        let addr = contact.addr;
        thread::spawn(move || {
            let result = rpc.send_request(addr, msg);
            let _ = tx.send(QueryResult {
                contact,
                result,
            });
        });
    }

    fn iterative_lookup(
        &self,
        target: NodeId,
        seed: Vec<Contact>,
        find_value: bool,
    ) -> LookupResult {
        let mut shortlist: Vec<Contact> = seed;
        let mut queried: HashSet<NodeId> =
            HashSet::new();
        let mut in_flight: usize = 0;

        let (tx, rx) = mpsc::channel::<QueryResult>();

        // Sort initial shortlist.
        sort_by_distance(&mut shortlist, &target);
        shortlist
            .dedup_by(|a, b| a.node_id == b.node_id);

        // Dispatch initial ALPHA queries.
        let initial = self.pick_candidates(
            &shortlist,
            &queried,
            ALPHA,
        );
        for c in &initial {
            queried.insert(c.node_id);
            in_flight += 1;
            Self::spawn_query(
                &self.rpc, c.clone(), target,
                find_value, &tx,
            );
        }

        // Track closest seen for convergence.
        let mut closest_seen = shortlist
            .first()
            .map(|c| target.distance(&c.node_id));
        let mut no_improvement_count: usize = 0;

        // Main event loop: process results as they
        // arrive, dispatching new queries immediately
        // (overlapping rounds per paper §2.3).
        while in_flight > 0 {
            let recv = rx.recv_timeout(
                Duration::from_secs(6),
            );
            match recv {
                Ok(QueryResult {
                    contact,
                    result: Ok((header, response)),
                }) => {
                    in_flight -= 1;
                    self.try_insert(Contact::new(
                        header.sender_id,
                        contact.addr,
                    ));

                    match response {
                        Message::FindNodeResponse {
                            contacts,
                        } => {
                            for c in contacts {
                                if !queried
                                    .contains(&c.node_id)
                                {
                                    shortlist.push(c);
                                }
                            }
                        }
                        Message::FindValueResponse(
                            FindValueResult::Value(v),
                        ) => {
                            sort_by_distance(
                                &mut shortlist,
                                &target,
                            );
                            return Ok((
                                shortlist,
                                Some((
                                    v,
                                    header.sender_id,
                                )),
                            ));
                        }
                        Message::FindValueResponse(
                            FindValueResult::Contacts(
                                contacts,
                            ),
                        ) => {
                            for c in contacts {
                                if !queried
                                    .contains(&c.node_id)
                                {
                                    shortlist.push(c);
                                }
                            }
                        }
                        _ => {}
                    }

                    // Re-sort and check improvement.
                    sort_by_distance(
                        &mut shortlist,
                        &target,
                    );
                    shortlist.dedup_by(|a, b| {
                        a.node_id == b.node_id
                    });

                    let new_closest = shortlist
                        .first()
                        .map(|c| {
                            target.distance(&c.node_id)
                        });

                    let improved =
                        match (&closest_seen, &new_closest)
                        {
                            (Some(old), Some(new)) => {
                                *new < *old
                            }
                            (None, Some(_)) => true,
                            _ => false,
                        };

                    if improved {
                        closest_seen = new_closest;
                        no_improvement_count = 0;
                    } else {
                        no_improvement_count += 1;
                    }

                    // Dispatch more queries to keep
                    // ALPHA in flight.
                    let want = ALPHA
                        .saturating_sub(in_flight);
                    if want > 0 {
                        let new_candidates =
                            self.pick_candidates(
                                &shortlist,
                                &queried,
                                want,
                            );
                        for c in &new_candidates {
                            queried.insert(c.node_id);
                            in_flight += 1;
                            Self::spawn_query(
                                &self.rpc,
                                c.clone(),
                                target,
                                find_value,
                                &tx,
                            );
                        }
                    }
                }
                Ok(QueryResult {
                    contact,
                    result: Err(_),
                }) => {
                    in_flight -= 1;
                    {
                        let mut rt = error::lock(
                            &self.routing_table,
                        );
                        rt.record_failure(
                            &contact.node_id,
                        );
                    }
                    shortlist.retain(|c| {
                        c.node_id != contact.node_id
                    });

                    // Dispatch replacement.
                    let new_candidates =
                        self.pick_candidates(
                            &shortlist,
                            &queried,
                            1,
                        );
                    for c in &new_candidates {
                        queried.insert(c.node_id);
                        in_flight += 1;
                        Self::spawn_query(
                            &self.rpc,
                            c.clone(),
                            target,
                            find_value,
                            &tx,
                        );
                    }
                }
                Err(_) => {
                    // Timeout with nothing in flight
                    // shouldn't happen since in_flight
                    // > 0, but break to be safe.
                    break;
                }
            }

            // If no improvement after processing K
            // responses, enter exhaustive phase.
            if no_improvement_count >= K
                && in_flight == 0
            {
                break;
            }
        }

        // Exhaustive phase: query all remaining
        // unqueried in K closest.
        sort_by_distance(&mut shortlist, &target);
        shortlist
            .dedup_by(|a, b| a.node_id == b.node_id);

        let remaining = self.pick_candidates(
            &shortlist,
            &queried,
            K,
        );
        if !remaining.is_empty() {
            for c in &remaining {
                queried.insert(c.node_id);
                in_flight += 1;
                Self::spawn_query(
                    &self.rpc, c.clone(), target,
                    find_value, &tx,
                );
            }

            while in_flight > 0 {
                let recv = rx.recv_timeout(
                    Duration::from_secs(6),
                );
                match recv {
                    Ok(QueryResult {
                        contact,
                        result: Ok((header, resp)),
                    }) => {
                        in_flight -= 1;
                        self.try_insert(Contact::new(
                            header.sender_id,
                            contact.addr,
                        ));
                        match resp {
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
                                    }
                                }
                            }
                            Message::FindValueResponse(
                                FindValueResult::Value(
                                    v,
                                ),
                            ) => {
                                sort_by_distance(
                                    &mut shortlist,
                                    &target,
                                );
                                return Ok((
                                    shortlist,
                                    Some((
                                        v,
                                        header
                                            .sender_id,
                                    )),
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
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok(QueryResult {
                        contact,
                        result: Err(_),
                    }) => {
                        in_flight -= 1;
                        let mut rt = error::lock(
                            &self.routing_table,
                        );
                        rt.record_failure(
                            &contact.node_id,
                        );
                        shortlist.retain(|c| {
                            c.node_id
                                != contact.node_id
                        });
                    }
                    Err(_) => break,
                }
            }
        }

        // Sort final results.
        sort_by_distance(&mut shortlist, &target);
        shortlist
            .dedup_by(|a, b| a.node_id == b.node_id);
        shortlist.truncate(K);

        Ok((shortlist, None))
    }

    fn sleep_or_stop(&self, duration: Duration) -> bool {
        let tick = Duration::from_secs(1);
        let mut elapsed = Duration::ZERO;
        while elapsed < duration {
            if self.stop.load(Ordering::Relaxed) {
                return true;
            }
            thread::sleep(tick);
            elapsed += tick;
        }
        self.stop.load(Ordering::Relaxed)
    }

    fn refresh_loop(&self) {
        while !self.sleep_or_stop(REFRESH_INTERVAL) {
            let targets = {
                let rt = error::lock(&self.routing_table);
                rt.stale_refresh_targets(
                    REFRESH_INTERVAL,
                )
            };
            for target in targets {
                let _ = self.find_node(target);
            }
        }
    }

    fn republish_loop(&self) {
        while !self.sleep_or_stop(REPUBLISH_INTERVAL) {
            // Refresh stale buckets first so local
            // routing knowledge is current (paper
            // §2.5: subtree refresh before
            // republish).
            let stale = {
                let rt =
                    error::lock(&self.routing_table);
                rt.stale_refresh_targets(
                    REFRESH_INTERVAL,
                )
            };
            for target in stale {
                let _ = self.find_node(target);
            }

            let to_republish = {
                let mut storage =
                    error::lock(&self.storage);
                storage.values_to_republish(
                    REPUBLISH_INTERVAL,
                )
            };

            // Use local routing knowledge to place
            // replicas without per-key lookups
            // (paper §2.5).
            for (key, value) in to_republish {
                let targets = {
                    let rt = error::lock(
                        &self.routing_table,
                    );
                    rt.closest(&key, K)
                };
                self.send_stores(
                    &targets, key, &value,
                );
            }
        }
    }

    fn expiry_loop(&self) {
        while !self.sleep_or_stop(Duration::from_secs(300)) {
            let mut storage = error::lock(&self.storage);
            storage.remove_expired();
        }
    }

    fn transfer_loop(&self) {
        loop {
            let contact = {
                let rx = error::lock(&self.transfer_rx);
                rx.recv_timeout(Duration::from_millis(100))
            };

            match contact {
                Ok(contact) => {
                    let pairs = {
                        let storage = error::lock(&self.storage);
                        storage.keys_closer_to(&self.id, &contact.node_id)
                    };
                    for (key, value) in pairs {
                        // Only transfer if we are the
                        // closest known holder (paper
                        // Section 2.5).
                        let dominated = {
                            let rt = error::lock(
                                &self.routing_table,
                            );
                            !rt.is_closest_to(
                                &key,
                                &contact.node_id,
                            )
                        };
                        if dominated {
                            continue;
                        }
                        if let Ok((h, _)) =
                            self.rpc.send_request(
                                contact.addr,
                                Message::StoreRequest {
                                    key,
                                    value,
                                    is_cache: false,
                                },
                            )
                        {
                            self.try_insert(
                                Contact::new(
                                    h.sender_id,
                                    contact.addr,
                                ),
                            );
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if self.stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
                Err(_) => break,
            }
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
        let node_a = Node::new(localhost()).unwrap();
        let addr_a = node_a.local_addr().unwrap();

        // Run node_a in a thread.
        let a_handle = {
            let node_a = Arc::clone(&node_a);
            thread::spawn(move || node_a.run())
        };

        // Give node_a time to start listening.
        thread::sleep(Duration::from_millis(100));

        let node_b = Node::new(localhost()).unwrap();
        node_b.join(addr_a).unwrap();

        // Verify routing tables are populated.
        let rt_b = error::lock(&node_b.routing_table);
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
        let node_a = Node::new(localhost()).unwrap();
        let addr_a = node_a.local_addr().unwrap();

        let a_handle = {
            let node_a = Arc::clone(&node_a);
            thread::spawn(move || node_a.run())
        };

        thread::sleep(Duration::from_millis(100));

        let node_b = Node::new(localhost()).unwrap();
        let remote_id = node_b.ping(addr_a).unwrap();
        assert_eq!(remote_id, node_a.id);

        drop(node_b);
        drop(node_a);
        drop(a_handle);
    }

    #[test]
    fn routing_table_size_and_storage_count() {
        let node_a = Node::new(localhost()).unwrap();
        let addr_a = node_a.local_addr().unwrap();

        let a_handle = {
            let node_a = Arc::clone(&node_a);
            thread::spawn(move || node_a.run())
        };

        thread::sleep(Duration::from_millis(100));

        let node_b = Node::new(localhost()).unwrap();
        assert_eq!(node_b.routing_table_size(), 0);
        assert_eq!(node_b.storage_count(), 0);

        node_b.join(addr_a).unwrap();
        assert!(node_b.routing_table_size() > 0);

        let key = NodeId::from_sha1(b"count");
        node_b.store(key, b"val".to_vec()).unwrap();
        assert!(node_b.storage_count() > 0);

        drop(node_b);
        drop(node_a);
        drop(a_handle);
    }

    #[test]
    fn store_and_retrieve() {
        let node_a = Node::new(localhost()).unwrap();
        let addr_a = node_a.local_addr().unwrap();

        let a_handle = {
            let node_a = Arc::clone(&node_a);
            thread::spawn(move || node_a.run())
        };

        thread::sleep(Duration::from_millis(100));

        let node_b = Node::new(localhost()).unwrap();
        node_b.join(addr_a).unwrap();

        let key = NodeId::from_sha1(b"hello");
        node_b.store(key, b"world".to_vec()).unwrap();

        // Retrieve from node_b (which should have
        // it locally).
        let val = node_b.find_value(key).unwrap();
        assert_eq!(val, b"world");

        drop(node_b);
        drop(node_a);
        drop(a_handle);
    }

    #[test]
    fn store_transfer_to_closer_node() {
        // A is far from the key, B is close.
        let key = NodeId::from_bytes([0x00; 20]);
        let a_id = NodeId::from_bytes([0xFF; 20]);
        let mut b_bytes = [0x00u8; 20];
        b_bytes[19] = 0x01;
        let b_id = NodeId::from_bytes(b_bytes);

        let node_a = Node::with_id(a_id, localhost()).unwrap();
        let addr_a = node_a.local_addr().unwrap();

        let node_b = Node::with_id(b_id, localhost()).unwrap();

        // Start both nodes.
        let a_handle = {
            let node_a = Arc::clone(&node_a);
            thread::spawn(move || node_a.run())
        };
        let b_handle = {
            let node_b = Arc::clone(&node_b);
            thread::spawn(move || node_b.run())
        };

        thread::sleep(Duration::from_millis(100));

        // Insert value directly into A's storage.
        {
            let mut storage = error::lock(&node_a.storage);
            storage.put(
                key,
                b"transferred".to_vec(),
                false,
                Duration::from_secs(86400),
            );
        }

        // B joins A — A discovers B and should
        // transfer the value since B is closer.
        node_b.join(addr_a).unwrap();

        // Wait for the async transfer.
        thread::sleep(Duration::from_millis(500));

        // B should now have the value.
        let val = {
            let storage = error::lock(&node_b.storage);
            storage.get(&key).map(|v| v.to_vec())
        };
        assert_eq!(
            val,
            Some(b"transferred".to_vec()),
            "new closer node should receive \
             transferred value"
        );

        drop(node_b);
        drop(node_a);
        drop(a_handle);
        drop(b_handle);
    }

    #[test]
    fn exponential_ttl_scales_with_closer_nodes() {
        // Use fixed IDs so we control distances.
        // local = all 0xFF, key = all 0x00.
        let local_id = NodeId::from_bytes([0xFF; 20]);
        let key = NodeId::from_bytes([0x00; 20]);

        let node = Node::with_id(local_id, localhost()).unwrap();

        // No contacts — full TTL.
        let ttl0 = node.cache_ttl(&key);
        assert_eq!(ttl0, EXPIRY_TTL);

        // Add 1 contact closer to key than us.
        let mut c1 = [0x00u8; 20];
        c1[19] = 0x01;
        {
            let mut rt = error::lock(&node.routing_table);
            rt.insert(Contact::new(NodeId::from_bytes(c1), localhost()));
        }
        let ttl1 = node.cache_ttl(&key);
        assert_eq!(ttl1, Duration::from_secs(EXPIRY_TTL.as_secs() / 2));

        // Add 2nd closer contact — TTL / 4.
        let mut c2 = [0x00u8; 20];
        c2[19] = 0x02;
        {
            let mut rt = error::lock(&node.routing_table);
            rt.insert(Contact::new(NodeId::from_bytes(c2), localhost()));
        }
        let ttl2 = node.cache_ttl(&key);
        assert_eq!(ttl2, Duration::from_secs(EXPIRY_TTL.as_secs() / 4));
    }

    #[test]
    fn cache_ttl_never_below_minimum() {
        let local_id = NodeId::from_bytes([0xFF; 20]);
        let key = NodeId::from_bytes([0x00; 20]);

        let node = Node::with_id(local_id, localhost()).unwrap();

        // Add 30 contacts closer to key — would
        // make TTL sub-second without the floor.
        for i in 0u8..30 {
            let mut bytes = [0x00u8; 20];
            bytes[18] = i;
            let mut rt = error::lock(&node.routing_table);
            rt.insert(Contact::new(NodeId::from_bytes(bytes), localhost()));
        }
        let ttl = node.cache_ttl(&key);
        assert_eq!(ttl, MIN_CACHE_TTL);
    }
}
