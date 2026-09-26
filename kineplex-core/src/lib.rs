//! Core configuration and domain types for KinePlex.

pub mod arrow_ffi;
pub mod accumulator;
pub mod arrow_concat;
pub mod buffer_recycle;
pub mod egress;
pub mod flatbuffers;
pub mod graph;
pub mod geometry;
pub mod metrics;
pub mod metric_formula;
pub mod iouaring;
pub mod iouring_net;
pub mod observability;
pub mod packet;
pub mod plane_isolation;
pub mod receptor;
pub mod reliability;
pub mod terminal;
pub mod topology;
pub mod physical_plan;
pub mod spike_tap;
pub mod quic;
pub mod synapse;
pub mod wasm;
pub use quic::stream;

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;

/// Default UDP port used by a KinePlex node.
pub const DEFAULT_NODE_PORT: u16 = 8000;

/// Immutable runtime configuration shared by all node components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    pub bind_ip: IpAddr,
    pub port: u16,
    pub advertise_ip: Option<IpAddr>,
    pub seeds: Vec<SocketAddr>,
    /// Whether to enable the native CPU sampling endpoint.
    pub enable_profiling: bool,
}

impl NodeConfig {
    #[must_use]
    pub const fn new(bind_ip: IpAddr, port: u16, seeds: Vec<SocketAddr>) -> Self {
        Self {
            bind_ip,
            port,
            advertise_ip: None,
            seeds,
            enable_profiling: false,
        }
    }

    #[must_use]
    pub const fn with_advertise_ip(mut self, advertise_ip: Option<IpAddr>) -> Self {
        self.advertise_ip = advertise_ip;
        self
    }

    /// Enables or disables native CPU profiling for this node process.
    #[must_use]
    pub const fn with_profiling(mut self, enable_profiling: bool) -> Self {
        self.enable_profiling = enable_profiling;
        self
    }

    #[must_use]
    pub fn advertise_addr(&self) -> Option<SocketAddr> {
        self.advertise_ip
            .or_else(|| (!self.bind_ip.is_unspecified()).then_some(self.bind_ip))
            .map(|ip| SocketAddr::new(ip, self.port))
    }

    #[must_use]
    pub const fn bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_ip, self.port)
    }
}

static NODE_CONFIG: OnceLock<NodeConfig> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeConfigInitError {
    AlreadyInitialized,
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
    tracing::debug!(bind_addr = %config.bind_addr(), seed_count = config.seeds.len(), "node configuration initialized");
    Ok(config)
}

#[must_use]
pub fn node_config() -> Option<&'static NodeConfig> {
    NODE_CONFIG.get()
}

#[cfg(test)]
mod tests {
    use super::NodeConfig;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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
