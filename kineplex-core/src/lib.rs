//! Core configuration and domain types for KinePlex.

pub mod arrow_ffi;
pub mod accumulator;
pub mod physical_plan;
pub mod graph;
pub mod observability;
pub mod wasm;

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
    /// Reachable interface address advertised to peers, if explicitly configured.
    pub advertise_ip: Option<IpAddr>,
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
            advertise_ip: None,
            seeds,
        }
    }

    /// Sets the reachable interface address announced to remote peers.
    #[must_use]
    pub const fn with_advertise_ip(mut self, advertise_ip: Option<IpAddr>) -> Self {
        self.advertise_ip = advertise_ip;
        self
    }

    /// Returns the reachable node endpoint, when one can be determined safely.
    ///
    /// An explicit advertise IP takes precedence. A concrete bind IP is otherwise
    /// usable, while an unspecified bind such as `0.0.0.0` requires explicit input.
    #[must_use]
    pub fn advertise_addr(&self) -> Option<SocketAddr> {
        self.advertise_ip
            .or_else(|| (!self.bind_ip.is_unspecified()).then_some(self.bind_ip))
            .map(|ip| SocketAddr::new(ip, self.port))
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
    NODE_CONFIG.set(config).map_err(|_| {
        tracing::warn!("node configuration initialization was attempted more than once");
        NodeConfigInitError::AlreadyInitialized
    })?;

    let config = NODE_CONFIG
        .get()
        .ok_or(NodeConfigInitError::UnavailableAfterInitialization)?;

    tracing::debug!(
        bind_addr = %config.bind_addr(),
        seed_count = config.seeds.len(),
        "node configuration initialized"
    );

    Ok(config)
}

/// Returns the immutable process-wide configuration when boot initialization has completed.
#[must_use]
pub fn node_config() -> Option<&'static NodeConfig> {
    NODE_CONFIG.get()
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::NodeConfig;

    #[test]
    fn concrete_bind_address_is_advertised_by_default() {
        let config = NodeConfig::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000, Vec::new());

        assert_eq!(
            config.advertise_addr(),
            Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 8000)))
        );
    }

    #[test]
    fn unspecified_bind_requires_explicit_advertise_address() {
        let config = NodeConfig::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8000, Vec::new());
        assert_eq!(config.advertise_addr(), None);

        let advertised = config.with_advertise_ip(Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert_eq!(
            advertised.advertise_addr(),
            Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 8000)))
        );
    }
}
