//! UDP transport layer for Kademlia RPCs.
//!
//! Spawns a listener thread that routes incoming
//! packets: responses are matched to pending requests
//! by [`RpcId`], unsolicited requests are forwarded
//! to the inbound channel.

use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{
    self, Receiver, SyncSender,
};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::message::{
    self, Header, Message, RpcId,
};
use crate::node_id::NodeId;

const MAX_PACKET: usize = 65535;
const REQUEST_TIMEOUT: Duration =
    Duration::from_secs(5);

/// An inbound message received from the network.
pub struct Inbound {
    /// Source address of the packet.
    pub from: SocketAddr,
    /// Decoded message header.
    pub header: Header,
    /// Decoded message body.
    pub message: Message,
}

/// A UDP RPC transport bound to a local socket.
///
/// Manages a background listener thread and a
/// pending-request table for correlating responses.
pub struct Rpc {
    socket: Arc<UdpSocket>,
    local_id: NodeId,
    pending: Arc<
        Mutex<HashMap<RpcId, SyncSender<Inbound>>>,
    >,
    stop: Arc<AtomicBool>,
    listener: Option<JoinHandle<()>>,
}

impl Rpc {
    /// Bind to `addr` and start the listener thread.
    ///
    /// Returns the `Rpc` handle and a receiver for
    /// unsolicited inbound requests.
    pub fn bind(
        addr: SocketAddr,
        local_id: NodeId,
    ) -> Result<(Self, Receiver<Inbound>)> {
        let socket = Arc::new(UdpSocket::bind(addr)?);
        socket.set_read_timeout(Some(
            Duration::from_millis(500),
        ))?;

        let pending: Arc<
            Mutex<HashMap<RpcId, SyncSender<Inbound>>>,
        > = Arc::new(Mutex::new(HashMap::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let (inbound_tx, inbound_rx) =
            mpsc::sync_channel(256);

        let listener = {
            let socket = Arc::clone(&socket);
            let pending = Arc::clone(&pending);
            let stop = Arc::clone(&stop);

            thread::spawn(move || {
                let mut buf = vec![0u8; MAX_PACKET];
                while !stop.load(Ordering::Relaxed)
                {
                    let (n, from) = match socket
                        .recv_from(&mut buf)
                    {
                        Ok(r) => r,
                        Err(ref e)
                            if e.kind()
                                == std::io::ErrorKind::WouldBlock
                                || e.kind()
                                    == std::io::ErrorKind::TimedOut =>
                        {
                            continue;
                        }
                        Err(_) => continue,
                    };

                    let (header, message) =
                        match message::decode(
                            &buf[..n],
                        ) {
                            Ok(r) => r,
                            Err(_) => continue,
                        };

                    let inbound = Inbound {
                        from,
                        header: Header {
                            rpc_id: header.rpc_id,
                            sender_id: header
                                .sender_id,
                        },
                        message,
                    };

                    // Check if this is a response
                    // to a pending request.
                    let sender = {
                        let mut map = pending
                            .lock()
                            .unwrap();
                        map.remove(
                            &inbound.header.rpc_id,
                        )
                    };

                    if let Some(tx) = sender {
                        let _ = tx.send(inbound);
                    } else {
                        let _ = inbound_tx
                            .send(inbound);
                    }
                }
            })
        };

        Ok((
            Rpc {
                socket,
                local_id,
                pending,
                stop,
                listener: Some(listener),
            },
            inbound_rx,
        ))
    }

    /// Return the local socket address.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.socket.local_addr()?)
    }

    /// Send a request and block until a response
    /// arrives or the timeout expires.
    pub fn send_request(
        &self,
        to: SocketAddr,
        msg: Message,
    ) -> Result<(Header, Message)> {
        let rpc_id = message::new_rpc_id();
        let header = Header {
            rpc_id,
            sender_id: self.local_id,
        };

        let (tx, rx) = mpsc::sync_channel(1);

        {
            let mut map =
                self.pending.lock().unwrap();
            map.insert(rpc_id, tx);
        }

        let data = message::encode(&header, &msg);
        self.socket.send_to(&data, to)?;

        match rx.recv_timeout(REQUEST_TIMEOUT) {
            Ok(inbound) => Ok((
                inbound.header,
                inbound.message,
            )),
            Err(_) => {
                let mut map =
                    self.pending.lock().unwrap();
                map.remove(&rpc_id);
                Err(Error::Timeout)
            }
        }
    }

    /// Send a response to a prior request.
    pub fn send_response(
        &self,
        to: SocketAddr,
        rpc_id: RpcId,
        msg: Message,
    ) -> Result<()> {
        let header = Header {
            rpc_id,
            sender_id: self.local_id,
        };
        let data = message::encode(&header, &msg);
        self.socket.send_to(&data, to)?;
        Ok(())
    }
}

impl Drop for Rpc {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.listener.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_ping() {
        let id_a = NodeId::random();
        let id_b = NodeId::random();

        let addr_a: SocketAddr =
            "127.0.0.1:0".parse().unwrap();
        let addr_b: SocketAddr =
            "127.0.0.1:0".parse().unwrap();

        let (rpc_a, _rx_a) =
            Rpc::bind(addr_a, id_a).unwrap();
        let (rpc_b, rx_b) =
            Rpc::bind(addr_b, id_b).unwrap();

        let actual_b =
            rpc_b.local_addr().unwrap();

        // Spawn a responder for B.
        let rpc_b_arc = Arc::new(rpc_b);
        let rpc_b2 = Arc::clone(&rpc_b_arc);
        let responder = thread::spawn(move || {
            if let Ok(inbound) = rx_b.recv_timeout(
                Duration::from_secs(5),
            ) {
                rpc_b2
                    .send_response(
                        inbound.from,
                        inbound.header.rpc_id,
                        Message::PingResponse,
                    )
                    .unwrap();
            }
        });

        let (_, response) = rpc_a
            .send_request(
                actual_b,
                Message::PingRequest,
            )
            .unwrap();

        assert!(matches!(
            response,
            Message::PingResponse
        ));

        responder.join().unwrap();
    }

    #[test]
    fn timeout_on_dead_address() {
        let id = NodeId::random();
        let addr: SocketAddr =
            "127.0.0.1:0".parse().unwrap();
        let (rpc, _rx) =
            Rpc::bind(addr, id).unwrap();

        // Send to a port that nobody is listening
        // on. Use a high port to minimize collision.
        let dead: SocketAddr =
            "127.0.0.1:19999".parse().unwrap();
        let result = rpc.send_request(
            dead,
            Message::PingRequest,
        );
        assert!(matches!(result, Err(Error::Timeout)));
    }
}
