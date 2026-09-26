//! Compact node resource telemetry and background collection.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::sync::mpsc::{self as std_mpsc, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio::sync::mpsc;

/// Number of bytes occupied by a wire-encoded [`NodeTelemetry`] sample.
pub const NODE_TELEMETRY_ENCODED_LEN: usize = 5;

const BYTES_PER_MEBIBYTE: u64 = 1024 * 1024;
const COLLECTOR_THREAD_NAME: &str = "kineplex-telemetry";

/// Compact resource availability advertised through the Gossip mesh.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeTelemetry {
    /// Current system-wide CPU utilization, clamped to `0..=100` percent.
    pub cpu_utilization_pct: u8,
    /// Memory currently available to the Arrow allocation layer, in mebibytes.
    ///
    /// Until an Arrow memory pool is available, this value is derived from the
    /// operating system's reusable memory estimate and saturates at [`u32::MAX`].
    pub arrow_mem_avail_mb: u32,
}

impl NodeTelemetry {
    /// Creates one compact telemetry sample.
    #[must_use]
    pub const fn new(cpu_utilization_pct: u8, arrow_mem_avail_mb: u32) -> Self {
        Self {
            cpu_utilization_pct,
            arrow_mem_avail_mb,
        }
    }

    /// Encodes this sample into exactly five network-order bytes.
    #[must_use]
    pub const fn encode(self) -> [u8; NODE_TELEMETRY_ENCODED_LEN] {
        let memory = self.arrow_mem_avail_mb.to_be_bytes();
        [
            self.cpu_utilization_pct,
            memory[0],
            memory[1],
            memory[2],
            memory[3],
        ]
    }

    /// Decodes one five-byte telemetry sample.
    ///
    /// # Errors
    ///
    /// Returns [`TelemetryDecodeError`] unless `encoded` is exactly five bytes or
    /// when its CPU percentage is outside `0..=100`.
    pub fn decode(encoded: &[u8]) -> Result<Self, TelemetryDecodeError> {
        if encoded.len() != NODE_TELEMETRY_ENCODED_LEN {
            return Err(TelemetryDecodeError::InvalidLength(encoded.len()));
        }
        let cpu_utilization_pct = encoded[0];
        if cpu_utilization_pct > 100 {
            return Err(TelemetryDecodeError::InvalidCpuUtilization(
                cpu_utilization_pct,
            ));
        }

        Ok(Self::new(
            cpu_utilization_pct,
            u32::from_be_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]),
        ))
    }

    fn from_system(system: &System) -> Self {
        let cpu_utilization_pct = system
            .global_cpu_info()
            .cpu_usage()
            .clamp(0.0, 100.0)
            .round() as u8;
        let available_mb = system.available_memory() / BYTES_PER_MEBIBYTE;
        let arrow_mem_avail_mb = u32::try_from(available_mb).unwrap_or(u32::MAX);

        Self::new(cpu_utilization_pct, arrow_mem_avail_mb)
    }
}

/// Validation error for a compact telemetry wire sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetryDecodeError {
    /// The byte slice does not contain exactly one compact sample.
    InvalidLength(usize),
    /// The encoded CPU percentage is outside the valid range.
    InvalidCpuUtilization(u8),
}

impl Display for TelemetryDecodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLength(length) => write!(
                formatter,
                "telemetry payload must contain exactly {NODE_TELEMETRY_ENCODED_LEN} bytes, got {length}"
            ),
            Self::InvalidCpuUtilization(utilization) => {
                write!(formatter, "CPU utilization must be at most 100, got {utilization}")
            }
        }
    }
}

impl Error for TelemetryDecodeError {}

pub(crate) struct TelemetryCollector {
    shutdown: Option<std_mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl TelemetryCollector {
    pub(crate) fn spawn(
        interval: Duration,
        samples: mpsc::Sender<NodeTelemetry>,
    ) -> Result<Self, io::Error> {
        let (shutdown_tx, shutdown_rx) = std_mpsc::channel();
        let worker = thread::Builder::new()
            .name(COLLECTOR_THREAD_NAME.to_owned())
            .spawn(move || collect(interval, samples, shutdown_rx))?;

        Ok(Self {
            shutdown: Some(shutdown_tx),
            worker: Some(worker),
        })
    }

    pub(crate) fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _shutdown_result = shutdown.send(());
        }
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                tracing::warn!("telemetry collector thread terminated unexpectedly");
            }
        }
    }
}

impl Drop for TelemetryCollector {
    fn drop(&mut self) {
        self.stop();
    }
}

fn collect(
    interval: Duration,
    samples: mpsc::Sender<NodeTelemetry>,
    shutdown: std_mpsc::Receiver<()>,
) {
    let refreshes = RefreshKind::new()
        .with_cpu(CpuRefreshKind::new().with_cpu_usage())
        .with_memory(MemoryRefreshKind::new().with_ram());
    let mut system = System::new_with_specifics(refreshes);

    match shutdown.recv_timeout(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL) {
        Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
        Err(RecvTimeoutError::Timeout) => {}
    }

    loop {
        system.refresh_cpu_usage();
        system.refresh_memory();
        let sample = NodeTelemetry::from_system(&system);
        match samples.try_send(sample) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => return,
        }

        match shutdown.recv_timeout(interval) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{NodeTelemetry, TelemetryCollector, NODE_TELEMETRY_ENCODED_LEN};
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    #[test]
    fn telemetry_encodes_in_five_bytes() {
        let encoded = NodeTelemetry::new(73, 16_384).encode();

        assert_eq!(encoded.len(), NODE_TELEMETRY_ENCODED_LEN);
        assert!(encoded.len() < 6);
        assert_eq!(encoded, [73, 0, 0, 64, 0]);
    }

    #[test]
    fn telemetry_round_trips_exactly() {
        let telemetry = NodeTelemetry::new(42, u32::MAX);

        assert_eq!(NodeTelemetry::decode(&telemetry.encode()), Ok(telemetry));
    }

    #[test]
    fn telemetry_rejects_invalid_wire_values() {
        assert!(NodeTelemetry::decode(&[0; 4]).is_err());
        assert!(NodeTelemetry::decode(&[101, 0, 0, 0, 1]).is_err());
    }

    #[tokio::test]
    async fn collector_sends_sysinfo_sample_through_mpsc() {
        let (samples_tx, mut samples_rx) = mpsc::channel(1);
        let mut collector = TelemetryCollector::spawn(Duration::from_secs(60), samples_tx)
            .expect("the telemetry collector thread must start");

        let sample = timeout(Duration::from_secs(3), samples_rx.recv())
            .await
            .expect("sysinfo sampling must finish before the deadline")
            .expect("the collector must send one sample");
        collector.stop();

        assert!(sample.cpu_utilization_pct <= 100);
    }
}
