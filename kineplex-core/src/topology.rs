//! Background local topology diagnostics for differential mesh charts.

use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};

/// Cell counts and ranks needed to compute local Euler and first Betti values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TopologySample {
    pub vertices: u64,
    pub edges: u64,
    pub faces: u64,
    pub connected_components: u64,
    pub boundary_2_rank: u64,
}

/// Health indicators for one local chart complex.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TopologyHealth {
    pub euler_characteristic: i128,
    pub beta_0: u64,
    pub beta_1: u64,
}

impl TopologySample {
    #[must_use]
    pub fn health(self) -> TopologyHealth {
        let euler_characteristic =
            i128::from(self.vertices) - i128::from(self.edges) + i128::from(self.faces);
        let cycle_rank = self
            .edges
            .saturating_sub(self.vertices)
            .saturating_add(self.connected_components);
        TopologyHealth {
            euler_characteristic,
            beta_0: self.connected_components,
            beta_1: cycle_rank.saturating_sub(self.boundary_2_rank),
        }
    }
}

/// Non-blocking reporter that computes topology health on a background thread.
pub struct TopologyMonitor {
    samples: Option<SyncSender<TopologySample>>,
    worker: Option<JoinHandle<()>>,
}

impl TopologyMonitor {
    pub fn spawn() -> Result<Self, std::io::Error> {
        let (samples, receiver) = mpsc::sync_channel::<TopologySample>(1);
        let worker = thread::Builder::new()
            .name("kineplex-topology-health".to_owned())
            .spawn(move || {
                while let Ok(sample) = receiver.recv() {
                    let health = sample.health();
                    tracing::info!(
                        euler = health.euler_characteristic,
                        beta_0 = health.beta_0,
                        beta_1 = health.beta_1,
                        vertices = sample.vertices,
                        edges = sample.edges,
                        faces = sample.faces,
                        "local topology health sampled"
                    );
                }
            })?;
        Ok(Self {
            samples: Some(samples),
            worker: Some(worker),
        })
    }

    /// Publishes a sample without blocking the geometry or data-plane caller.
    pub fn try_report(&self, sample: TopologySample) -> Result<(), TopologySample> {
        let Some(samples) = &self.samples else {
            return Err(sample);
        };
        match samples.try_send(sample) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(sample) | TrySendError::Disconnected(sample)) => Err(sample),
        }
    }
}

impl Drop for TopologyMonitor {
    fn drop(&mut self) {
        self.samples.take();
        if let Some(worker) = self.worker.take() {
            let _join_result = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TopologySample;

    #[test]
    fn euler_and_homology_are_computed_for_a_filled_triangle() {
        let health = TopologySample {
            vertices: 3,
            edges: 3,
            faces: 1,
            connected_components: 1,
            boundary_2_rank: 1,
        }
        .health();
        assert_eq!(health.euler_characteristic, 1);
        assert_eq!(health.beta_0, 1);
        assert_eq!(health.beta_1, 0);
    }
}
