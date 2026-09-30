//! KinePlex Net - Networking layer for cluster communication
//! 
//! This module provides:
//! - Gossip/SWIM protocol for cluster membership
//! - Routing between nodes
//! - Versioned protocol payloads

pub mod gossip;
pub mod routing;
pub mod protocol;

pub use gossip::{GossipProtocol, NodeInfo, MembershipEvent};
pub use routing::Router;
pub use protocol::{ProtocolVersion, ProtocolMessage, MessageType};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Node identifier in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NetNodeId(pub Uuid);

impl NetNodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for NetNodeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Network address
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkAddress {
    pub host: String,
    pub port: u16,
}

impl std::fmt::Display for NetworkAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl NetworkAddress {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

/// Connection status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Connecting,
    Failed(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_network_address() {
        let addr = NetworkAddress::new("localhost", 8080);
        assert_eq!(addr.to_string(), "localhost:8080");
    }
}