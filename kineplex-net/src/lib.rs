//! Network address primitives for KinePlex.

pub mod gossip;
pub mod telemetry;

use std::net::{AddrParseError, SocketAddr};

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
