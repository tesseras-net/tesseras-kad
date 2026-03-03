//! Binary wire protocol for Kademlia RPCs.
//!
//! Each message has a 46-byte fixed header:
//! 2 magic (`"KD"`) + 1 version + 1 msg_type
//! + 20 rpc_id + 20 sender_id + 2 payload_len (BE).

use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr,
};

use crate::contact::Contact;
use crate::error::{Error, Result};
use crate::node_id::{Key, NodeId};

const MAGIC: [u8; 2] = [0x4B, 0x44]; // "KD"
const VERSION: u8 = 1;

/// Fixed header size in bytes.
pub const HEADER_SIZE: usize = 46;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum MsgType {
    PingRequest = 0,
    PingResponse = 1,
    StoreRequest = 2,
    StoreResponse = 3,
    FindNodeRequest = 4,
    FindNodeResponse = 5,
    FindValueRequest = 6,
    FindValueResponse = 7,
}

impl MsgType {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(MsgType::PingRequest),
            1 => Ok(MsgType::PingResponse),
            2 => Ok(MsgType::StoreRequest),
            3 => Ok(MsgType::StoreResponse),
            4 => Ok(MsgType::FindNodeRequest),
            5 => Ok(MsgType::FindNodeResponse),
            6 => Ok(MsgType::FindValueRequest),
            7 => Ok(MsgType::FindValueResponse),
            _ => Err(Error::Protocol(format!(
                "unknown message type: {v}"
            ))),
        }
    }
}

/// A 20-byte random identifier for matching RPC
/// requests to responses.
pub type RpcId = [u8; 20];

/// Header fields common to every message.
#[derive(Debug, Clone)]
pub struct Header {
    /// Identifier linking a request to its response.
    pub rpc_id: RpcId,
    /// The sender's node ID.
    pub sender_id: NodeId,
}

/// A Kademlia RPC message.
#[derive(Debug, Clone)]
pub enum Message {
    /// Liveness check request.
    PingRequest,
    /// Liveness check response.
    PingResponse,
    /// Request to store a key-value pair.
    StoreRequest { key: Key, value: Vec<u8> },
    /// Acknowledgement of a store.
    StoreResponse,
    /// Request for the `K` closest nodes to `target`.
    FindNodeRequest { target: NodeId },
    /// Response with the closest known contacts.
    FindNodeResponse { contacts: Vec<Contact> },
    /// Request for the value associated with `key`.
    FindValueRequest { key: Key },
    /// Response with either the value or closer
    /// contacts.
    FindValueResponse(FindValueResult),
}

/// The payload of a [`Message::FindValueResponse`].
#[derive(Debug, Clone)]
pub enum FindValueResult {
    /// The value was found.
    Value(Vec<u8>),
    /// The value was not found; here are closer
    /// contacts.
    Contacts(Vec<Contact>),
}

fn encode_contact(buf: &mut Vec<u8>, contact: &Contact) {
    buf.extend_from_slice(contact.node_id.as_bytes());
    match contact.addr.ip() {
        IpAddr::V4(ip) => {
            buf.push(4);
            buf.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            buf.push(6);
            buf.extend_from_slice(&ip.octets());
        }
    }
    buf.extend_from_slice(
        &contact.addr.port().to_be_bytes(),
    );
}

fn decode_contact(
    data: &[u8],
    offset: &mut usize,
) -> Result<Contact> {
    if *offset + 21 > data.len() {
        return Err(Error::Protocol(
            "truncated contact".into(),
        ));
    }
    let mut id_bytes = [0u8; 20];
    id_bytes.copy_from_slice(
        &data[*offset..*offset + 20],
    );
    *offset += 20;

    let family = data[*offset];
    *offset += 1;

    let ip = match family {
        4 => {
            if *offset + 4 > data.len() {
                return Err(Error::Protocol(
                    "truncated IPv4".into(),
                ));
            }
            let octets: [u8; 4] = data
                [*offset..*offset + 4]
                .try_into()
                .unwrap();
            *offset += 4;
            IpAddr::V4(Ipv4Addr::from(octets))
        }
        6 => {
            if *offset + 16 > data.len() {
                return Err(Error::Protocol(
                    "truncated IPv6".into(),
                ));
            }
            let octets: [u8; 16] = data
                [*offset..*offset + 16]
                .try_into()
                .unwrap();
            *offset += 16;
            IpAddr::V6(Ipv6Addr::from(octets))
        }
        _ => {
            return Err(Error::Protocol(format!(
                "unknown addr family: {family}"
            )));
        }
    };

    if *offset + 2 > data.len() {
        return Err(Error::Protocol(
            "truncated port".into(),
        ));
    }
    let port = u16::from_be_bytes(
        data[*offset..*offset + 2].try_into().unwrap(),
    );
    *offset += 2;

    Ok(Contact::new(
        NodeId::from_bytes(id_bytes),
        SocketAddr::new(ip, port),
    ))
}

fn decode_contacts(data: &[u8]) -> Result<Vec<Contact>> {
    let mut contacts = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        contacts.push(decode_contact(data, &mut offset)?);
    }
    Ok(contacts)
}

/// Serialize a header and message into a byte
/// vector.
pub fn encode(
    header: &Header,
    msg: &Message,
) -> Vec<u8> {
    let msg_type = match msg {
        Message::PingRequest => MsgType::PingRequest,
        Message::PingResponse => MsgType::PingResponse,
        Message::StoreRequest { .. } => {
            MsgType::StoreRequest
        }
        Message::StoreResponse => MsgType::StoreResponse,
        Message::FindNodeRequest { .. } => {
            MsgType::FindNodeRequest
        }
        Message::FindNodeResponse { .. } => {
            MsgType::FindNodeResponse
        }
        Message::FindValueRequest { .. } => {
            MsgType::FindValueRequest
        }
        Message::FindValueResponse(_) => {
            MsgType::FindValueResponse
        }
    };

    let mut payload = Vec::new();
    match msg {
        Message::PingRequest | Message::PingResponse => {}
        Message::StoreRequest { key, value } => {
            payload.extend_from_slice(key.as_bytes());
            payload.extend_from_slice(value);
        }
        Message::StoreResponse => {}
        Message::FindNodeRequest { target } => {
            payload
                .extend_from_slice(target.as_bytes());
        }
        Message::FindNodeResponse { contacts } => {
            for c in contacts {
                encode_contact(&mut payload, c);
            }
        }
        Message::FindValueRequest { key } => {
            payload.extend_from_slice(key.as_bytes());
        }
        Message::FindValueResponse(result) => {
            match result {
                FindValueResult::Value(v) => {
                    payload.push(1); // tag: value
                    payload.extend_from_slice(v);
                }
                FindValueResult::Contacts(contacts) => {
                    payload.push(0); // tag: contacts
                    for c in contacts {
                        encode_contact(&mut payload, c);
                    }
                }
            }
        }
    }

    let payload_len = payload.len() as u16;
    let mut buf =
        Vec::with_capacity(HEADER_SIZE + payload.len());

    buf.extend_from_slice(&MAGIC);
    buf.push(VERSION);
    buf.push(msg_type as u8);
    buf.extend_from_slice(&header.rpc_id);
    buf.extend_from_slice(header.sender_id.as_bytes());
    buf.extend_from_slice(&payload_len.to_be_bytes());
    buf.extend_from_slice(&payload);
    buf
}

/// Deserialize a byte slice into a header and
/// message.
pub fn decode(
    data: &[u8],
) -> Result<(Header, Message)> {
    if data.len() < HEADER_SIZE {
        return Err(Error::Protocol(
            "message too short".into(),
        ));
    }
    if data[0..2] != MAGIC {
        return Err(Error::Protocol(
            "invalid magic bytes".into(),
        ));
    }
    if data[2] != VERSION {
        return Err(Error::Protocol(format!(
            "unsupported version: {}",
            data[2]
        )));
    }

    let msg_type = MsgType::from_u8(data[3])?;

    let mut rpc_id = [0u8; 20];
    rpc_id.copy_from_slice(&data[4..24]);

    let mut sender_bytes = [0u8; 20];
    sender_bytes.copy_from_slice(&data[24..44]);
    let sender_id = NodeId::from_bytes(sender_bytes);

    let payload_len = u16::from_be_bytes(
        data[44..46].try_into().unwrap(),
    ) as usize;

    if data.len() < HEADER_SIZE + payload_len {
        return Err(Error::Protocol(
            "truncated payload".into(),
        ));
    }

    let payload =
        &data[HEADER_SIZE..HEADER_SIZE + payload_len];

    let header = Header { rpc_id, sender_id };

    let msg = match msg_type {
        MsgType::PingRequest => Message::PingRequest,
        MsgType::PingResponse => Message::PingResponse,
        MsgType::StoreRequest => {
            if payload.len() < 20 {
                return Err(Error::Protocol(
                    "store request too short".into(),
                ));
            }
            let mut key_bytes = [0u8; 20];
            key_bytes.copy_from_slice(&payload[..20]);
            Message::StoreRequest {
                key: Key::from_bytes(key_bytes),
                value: payload[20..].to_vec(),
            }
        }
        MsgType::StoreResponse => Message::StoreResponse,
        MsgType::FindNodeRequest => {
            if payload.len() < 20 {
                return Err(Error::Protocol(
                    "find node request too short"
                        .into(),
                ));
            }
            let mut target_bytes = [0u8; 20];
            target_bytes
                .copy_from_slice(&payload[..20]);
            Message::FindNodeRequest {
                target: NodeId::from_bytes(
                    target_bytes,
                ),
            }
        }
        MsgType::FindNodeResponse => {
            let contacts = decode_contacts(payload)?;
            Message::FindNodeResponse { contacts }
        }
        MsgType::FindValueRequest => {
            if payload.len() < 20 {
                return Err(Error::Protocol(
                    "find value request too short"
                        .into(),
                ));
            }
            let mut key_bytes = [0u8; 20];
            key_bytes.copy_from_slice(&payload[..20]);
            Message::FindValueRequest {
                key: Key::from_bytes(key_bytes),
            }
        }
        MsgType::FindValueResponse => {
            if payload.is_empty() {
                return Err(Error::Protocol(
                    "empty find value response"
                        .into(),
                ));
            }
            let tag = payload[0];
            let rest = &payload[1..];
            match tag {
                1 => Message::FindValueResponse(
                    FindValueResult::Value(
                        rest.to_vec(),
                    ),
                ),
                0 => {
                    let contacts =
                        decode_contacts(rest)?;
                    Message::FindValueResponse(
                        FindValueResult::Contacts(
                            contacts,
                        ),
                    )
                }
                _ => {
                    return Err(Error::Protocol(
                        format!(
                            "unknown find value \
                             tag: {tag}"
                        ),
                    ));
                }
            }
        }
    };

    Ok((header, msg))
}

/// Generate a random [`RpcId`].
pub fn new_rpc_id() -> RpcId {
    let mut id = [0u8; 20];
    crate::rand::fill_bytes(&mut id);
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_header() -> Header {
        Header {
            rpc_id: [1u8; 20],
            sender_id: NodeId::from_bytes([2u8; 20]),
        }
    }

    fn test_addr() -> SocketAddr {
        "127.0.0.1:8000".parse().unwrap()
    }

    fn roundtrip(msg: Message) {
        let header = test_header();
        let encoded = encode(&header, &msg);
        let (h2, m2) = decode(&encoded).unwrap();
        assert_eq!(h2.rpc_id, header.rpc_id);
        assert_eq!(h2.sender_id, header.sender_id);
        match (&msg, &m2) {
            (
                Message::PingRequest,
                Message::PingRequest,
            ) => {}
            (
                Message::PingResponse,
                Message::PingResponse,
            ) => {}
            (
                Message::StoreRequest {
                    key: k1,
                    value: v1,
                },
                Message::StoreRequest {
                    key: k2,
                    value: v2,
                },
            ) => {
                assert_eq!(k1, k2);
                assert_eq!(v1, v2);
            }
            (
                Message::StoreResponse,
                Message::StoreResponse,
            ) => {}
            (
                Message::FindNodeRequest { target: t1 },
                Message::FindNodeRequest { target: t2 },
            ) => {
                assert_eq!(t1, t2);
            }
            (
                Message::FindNodeResponse {
                    contacts: c1,
                },
                Message::FindNodeResponse {
                    contacts: c2,
                },
            ) => {
                assert_eq!(c1.len(), c2.len());
                for (a, b) in
                    c1.iter().zip(c2.iter())
                {
                    assert_eq!(
                        a.node_id, b.node_id
                    );
                    assert_eq!(a.addr, b.addr);
                }
            }
            (
                Message::FindValueRequest { key: k1 },
                Message::FindValueRequest { key: k2 },
            ) => {
                assert_eq!(k1, k2);
            }
            (
                Message::FindValueResponse(
                    FindValueResult::Value(v1),
                ),
                Message::FindValueResponse(
                    FindValueResult::Value(v2),
                ),
            ) => {
                assert_eq!(v1, v2);
            }
            (
                Message::FindValueResponse(
                    FindValueResult::Contacts(c1),
                ),
                Message::FindValueResponse(
                    FindValueResult::Contacts(c2),
                ),
            ) => {
                assert_eq!(c1.len(), c2.len());
            }
            _ => panic!(
                "message type mismatch: \
                 {msg:?} vs {m2:?}"
            ),
        }
    }

    #[test]
    fn roundtrip_ping() {
        roundtrip(Message::PingRequest);
        roundtrip(Message::PingResponse);
    }

    #[test]
    fn roundtrip_store() {
        roundtrip(Message::StoreRequest {
            key: Key::from_bytes([0xAA; 20]),
            value: b"hello world".to_vec(),
        });
        roundtrip(Message::StoreResponse);
    }

    #[test]
    fn roundtrip_find_node() {
        let contacts = vec![
            Contact::new(
                NodeId::from_bytes([3u8; 20]),
                test_addr(),
            ),
            Contact::new(
                NodeId::from_bytes([4u8; 20]),
                "[::1]:9000".parse().unwrap(),
            ),
        ];
        roundtrip(Message::FindNodeRequest {
            target: NodeId::from_bytes([5u8; 20]),
        });
        roundtrip(Message::FindNodeResponse {
            contacts,
        });
    }

    #[test]
    fn roundtrip_find_value() {
        roundtrip(Message::FindValueRequest {
            key: Key::from_bytes([6u8; 20]),
        });
        roundtrip(Message::FindValueResponse(
            FindValueResult::Value(
                b"some data".to_vec(),
            ),
        ));
        roundtrip(Message::FindValueResponse(
            FindValueResult::Contacts(vec![
                Contact::new(
                    NodeId::from_bytes([7u8; 20]),
                    test_addr(),
                ),
            ]),
        ));
    }

    #[test]
    fn reject_truncated() {
        let result = decode(&[0x4B, 0x44, 1]);
        assert!(result.is_err());
    }

    #[test]
    fn reject_bad_magic() {
        let mut data = [0u8; HEADER_SIZE];
        data[0] = 0xFF;
        let result = decode(&data);
        assert!(result.is_err());
    }

    #[test]
    fn reject_bad_version() {
        let mut data = [0u8; HEADER_SIZE];
        data[0..2].copy_from_slice(&MAGIC);
        data[2] = 99;
        let result = decode(&data);
        assert!(result.is_err());
    }
}
