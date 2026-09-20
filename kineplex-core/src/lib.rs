//! Core configuration and domain types for KinePlex.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;

/// Default UDP port used by a KinePlex node.
pub const DEFAULT_NODE_PORT: u16 = 8000;

/// Immutable runtime configuration shared by all node components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    /// Interface address on which the node accepts traffic.
    pub bind_ip: IpAddr,
    /// UDP port on which the node accepts traffic.
    pub port: u16,
    /// Peer addresses contacted while bootstrapping cluster membership.
    pub seeds: Vec<SocketAddr>,
}

impl NodeConfig {
    /// Creates a validated, natively typed node configuration.
    #[must_use]
    pub const fn new(bind_ip: IpAddr, port: u16, seeds: Vec<SocketAddr>) -> Self {
        Self {
            bind_ip,
            port,
            seeds,
        }
    }

    /// Returns the complete socket address on which the node should bind.
    #[must_use]
    pub const fn bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_ip, self.port)
    }
}

static NODE_CONFIG: OnceLock<NodeConfig> = OnceLock::new();

/// Failure returned when the process-wide node configuration cannot be initialized.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeConfigInitError {
    /// Another component already initialized the process-wide configuration.
    AlreadyInitialized,
    /// The initialized value could not be read back from the global cell.
    UnavailableAfterInitialization,
}

impl Display for NodeConfigInitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInitialized => {
                formatter.write_str("node configuration is already initialized")
            }
            Self::UnavailableAfterInitialization => {
                formatter.write_str("node configuration is unavailable after initialization")
            }
        }
    }
}

impl Error for NodeConfigInitError {}

/// Installs the process-wide configuration exactly once and returns its immutable reference.
///
/// # Errors
///
/// Returns [`NodeConfigInitError::AlreadyInitialized`] when this function has already
/// succeeded in the current process. The configuration cannot be replaced after boot.
pub fn initialize_node_config(
    config: NodeConfig,
) -> Result<&'static NodeConfig, NodeConfigInitError> {
    NODE_CONFIG
        .set(config)
        .map_err(|_| NodeConfigInitError::AlreadyInitialized)?;

    NODE_CONFIG
        .get()
        .ok_or(NodeConfigInitError::UnavailableAfterInitialization)
}

/// Returns the immutable process-wide configuration when boot initialization has completed.
#[must_use]
pub fn node_config() -> Option<&'static NodeConfig> {
    NODE_CONFIG.get()
}
