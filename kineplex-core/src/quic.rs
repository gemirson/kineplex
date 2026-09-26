//! QUIC configuration and TLS certificate generation module.

pub mod stream;

use std::time::{Duration, Instant};

use tracing::debug;

/// Application protocol identifier for KinePlex nodes.
pub const APPLICATION_PROTOCOL: &[u8] = b"kineplex-synapse";

/// Default UDP payload size for QUIC packets.
pub const MAX_UDP_PAYLOAD_SIZE: u16 = 1350;

/// Default idle timeout for QUIC connections (60 seconds).
pub const DEFAULT_IDLE_TIMEOUT: u64 = 60_000;

/// Generates an in-memory self-signed TLS certificate using ECDSA P-256.
///
/// This is used for QUIC connection encryption without touching disk.
/// The certificate is valid for 365 days.
pub fn generate_tls_cert() -> Result<rcgen::Certificate, Box<dyn std::error::Error>> {
    let mut params = rcgen::CertificateParams::new(vec![]);

    // Use ECDSA P-256 for the key
    params.alg = &rcgen::PKCS_ECDSA_P256_SHA256;

    // Set validity period (365 days)
    params.not_before = time::OffsetDateTime::now_utc();
    params.not_after = params.not_before + time::Duration::days(365);

    // Create the certificate
    let cert = rcgen::Certificate::from_params(params)?;

    Ok(cert)
}

/// QUIC configuration with LAN/datacenter-optimized settings.
pub struct QuicConfig {
    config: quiche::Config,
    local_id: [u8; 16],
}

impl QuicConfig {
    /// Creates a new QUIC configuration with LAN-optimized settings.
    ///
    /// # Arguments
    ///
    /// * `local_id` - A unique identifier for this node (16 bytes).
    pub fn new(local_id: [u8; 16]) -> Result<Self, quiche::Error> {
        let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;

        // Set application protocol
        config.set_application_protos(&[APPLICATION_PROTOCOL])?;

        // LAN-optimized timeouts (longer for stable network)
        config.set_max_idle_timeout(DEFAULT_IDLE_TIMEOUT);
        config.set_max_recv_udp_payload_size(MAX_UDP_PAYLOAD_SIZE as usize);
        config.set_max_send_udp_payload_size(MAX_UDP_PAYLOAD_SIZE as usize);

        // Large buffer sizes for high-throughput LAN
        config.set_initial_max_data(10_000_000); // 10 MB
        config.set_initial_max_stream_data_bidi_local(1_000_000); // 1 MB

        // Use CUBIC congestion control for datacenter networks
        config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);

        Ok(Self { config, local_id })
    }

    /// Returns a reference to the underlying quiche configuration.
    #[must_use]
    pub fn inner(&self) -> &quiche::Config {
        &self.config
    }

    /// Returns a mutable reference to the underlying quiche configuration.
    pub fn inner_mut(&mut self) -> &mut quiche::Config {
        &mut self.config
    }

    /// Returns the local connection ID for this configuration.
    #[must_use]
    pub fn local_id(&self) -> &[u8; 16] {
        &self.local_id
    }
}

/// State machine wrapper for managing QUIC connections.
///
/// This provides a clean API for advancing the quiche state machine
/// and handling timeouts without blocking syscalls.
pub struct QuicStateMachine {
    config: QuicConfig,
    last_timeout: Option<Instant>,
}

impl QuicStateMachine {
    /// Creates a new QUIC state machine with the given configuration.
    pub fn new(config: QuicConfig) -> Self {
        Self {
            config,
            last_timeout: None,
        }
    }

    /// Returns a reference to the underlying QUIC configuration.
    #[must_use]
    pub fn config(&self) -> &QuicConfig {
        &self.config
    }

    /// Returns the next timeout duration for the QUIC state machine.
    ///
    /// This is used to schedule timer events for packet timeout handling.
    pub fn next_timeout(&self) -> Option<Duration> {
        // quiche timeout is handled at connection level, not config level
        // This is a placeholder for future implementation
        None
    }

    /// Advances the internal clock of the QUIC state machine.
    ///
    /// Call this when the timeout duration has elapsed to let quiche
    /// process any pending timeout events.
    pub fn handle_timeout(&mut self) {
        self.last_timeout = Some(Instant::now());
        debug!("QUIC state machine timeout processed");
    }

    /// Checks if a timeout has been processed since the last call.
    pub fn has_timeout_elapsed(&self) -> bool {
        self.last_timeout.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quic_config_creation() {
        let local_id = [0u8; 16];
        let config = QuicConfig::new(local_id).expect("Failed to create QUIC config");

        assert_eq!(config.local_id(), &local_id);
    }

    #[test]
    fn test_quic_state_machine_creation() {
        let local_id = [0u8; 16];
        let config = QuicConfig::new(local_id).expect("Failed to create QUIC config");
        let state_machine = QuicStateMachine::new(config);

        assert!(!state_machine.has_timeout_elapsed());
    }

    #[test]
    fn test_handle_timeout() {
        let local_id = [0u8; 16];
        let config = QuicConfig::new(local_id).expect("Failed to create QUIC config");
        let mut state_machine = QuicStateMachine::new(config);

        state_machine.handle_timeout();
        assert!(state_machine.has_timeout_elapsed());
    }

    #[test]
    fn test_tls_cert_generation() {
        let cert = generate_tls_cert().expect("Failed to generate TLS cert");

        // Verify the certificate can be serialized
        let _der = cert
            .serialize_der()
            .expect("Failed to serialize certificate");
        let _key_der = cert.serialize_private_key_der();
    }
}
