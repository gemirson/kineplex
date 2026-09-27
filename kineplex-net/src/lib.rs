//! Network address primitives and wire contracts for KinePlex.

pub mod gossip;

#[allow(
    unsafe_code,
    clippy::all,
    non_upper_case_globals,
    unused_imports,
    unknown_lints,
    mismatched_lifetime_syntaxes
)]
pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/synapse_header_generated.rs"));
}

pub mod network;
pub mod protocol;
pub mod routing;
pub mod telemetry;

use std::net::{AddrParseError, SocketAddr};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use protocol::{ProtocolVersion, ProtocolMessage, MessageType};
pub use gossip::{GossipProtocol, NodeInfo, ClusterMembershipEvent};
pub use routing::Router;

/// Parses a seed endpoint as a native IPv4 or IPv6 socket address.
///
/// IPv6 literals must use the standard bracketed socket form, for example
/// `[2001:db8::1]:8000`.
///
/// # Errors
///
/// Returns [`AddrParseError`] when `value` is not an IP literal followed by a valid
/// `u16` port.
pub fn parse_socket_addr(value: &str) -> Result<SocketAddr, AddrParseError> {
    value.parse().map_err(|error| {
        tracing::debug!("seed socket address validation failed");
        error
    })
}

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkAddress {
    pub host: String,
    pub port: u16,
}

impl NetworkAddress {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
    
    pub fn to_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
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
