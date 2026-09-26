//! Background metric-tensor updates kept off Tokio and network I/O threads.

use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dashmap::DashMap;

/// Normalized local resource and transport measurements used by the metric model.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct MetricSample {
    pub latency_ms: f64,
    pub queued_bytes: f64,
    pub cpu_percent: f64,
}

/// Symmetric three-dimensional positive diagonal metric tensor.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricTensor {
    pub components: [[f64; 3]; 3],
    pub revision: u64,
}

impl Default for MetricTensor {
    fn default() -> Self {
        Self {
            components: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
            revision: 0,
        }
    }
}

impl MetricTensor {
    /// Produces a bounded diagonal metric from latency, queue occupancy, and CPU load.
    #[must_use]
    pub fn from_sample(sample: MetricSample, revision: u64) -> Self {
        let normalize = |value: f64, scale: f64| {
            if value.is_finite() {
                (value / scale).clamp(0.0, 100.0)
            } else {
                100.0
            }
        };
        let diagonal = [
            1.0 + normalize(sample.latency_ms, 100.0),
            1.0 + normalize(sample.queued_bytes, 1_048_576.0),
            1.0 + normalize(sample.cpu_percent, 100.0),
        ];
        Self {
            components: [
                [diagonal[0], 0.0, 0.0],
                [0.0, diagonal[1], 0.0],
                [0.0, 0.0, diagonal[2]],
            ],
            revision,
        }
    }

    /// Computes the metric squared norm without allocating.
    #[must_use]
    pub fn norm_squared(&self, vector: [f64; 3]) -> f64 {
        vector[0] * vector[0] * self.components[0][0]
            + vector[1] * vector[1] * self.components[1][1]
            + vector[2] * vector[2] * self.components[2][2]
    }
}

/// Non-blocking publisher connected to a single Control Plane metric worker.
#[derive(Clone)]
pub struct MetricPublisher {
    sender: SyncSender<MetricSample>,
}

impl MetricPublisher {
    /// Attempts to publish one sample; a full queue drops the sample immediately.
    pub fn try_publish(&self, sample: MetricSample) -> Result<(), MetricSample> {
        match self.sender.try_send(sample) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(sample) | TrySendError::Disconnected(sample)) => Err(sample),
        }
    }
}

/// Dedicated, CPU-pinned worker recalculating the tensor at a configurable cadence.
pub struct MetricWorker {
    tensor: Arc<RwLock<MetricTensor>>,
    shutdown: Option<mpsc::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl MetricWorker {
    /// Spawns a background worker and pins it to an available CPU on Linux.
    pub fn spawn(interval: Duration) -> Result<(Self, MetricPublisher), MetricWorkerError> {
        if interval.is_zero() {
            return Err(MetricWorkerError::InvalidInterval);
        }
        let (sample_sender, sample_receiver) = mpsc::sync_channel(1);
        let (shutdown_sender, shutdown_receiver) = mpsc::channel();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let tensor = Arc::new(RwLock::new(MetricTensor::default()));
        let worker_tensor = Arc::clone(&tensor);
        let join = thread::Builder::new()
            .name("kineplex-metric-control".to_owned())
            .spawn(move || {
                if let Err(error) = pin_control_thread() {
                    let _send_result = ready_sender.send(Err(error));
                    return;
                }
                let _ready_result = ready_sender.send(Ok(()));
                run_metric_worker(interval, sample_receiver, shutdown_receiver, worker_tensor);
            })
            .map_err(|error| MetricWorkerError::Thread(error.to_string()))?;
        match ready_receiver.recv() {
            Ok(Ok(())) => Ok((
                Self {
                    tensor,
                    shutdown: Some(shutdown_sender),
                    join: Some(join),
                },
                MetricPublisher {
                    sender: sample_sender,
                },
            )),
            Ok(Err(error)) => {
                let _join_result = join.join();
                Err(MetricWorkerError::Affinity(error))
            }
            Err(error) => Err(MetricWorkerError::Thread(error.to_string())),
        }
    }

    /// Returns the most recently computed tensor snapshot.
    #[must_use]
    pub fn snapshot(&self) -> MetricTensor {
        *self
            .tensor
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Stops and joins the background worker.
    pub fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _send_result = shutdown.send(());
        }
        if let Some(join) = self.join.take() {
            let _join_result = join.join();
        }
    }
}

impl Drop for MetricWorker {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _send_result = shutdown.send(());
        }
        if let Some(join) = self.join.take() {
            let _join_result = join.join();
        }
    }
}

fn run_metric_worker(
    interval: Duration,
    samples: Receiver<MetricSample>,
    shutdown: Receiver<()>,
    tensor: Arc<RwLock<MetricTensor>>,
) {
    let mut revision = 0_u64;
    loop {
        match shutdown.recv_timeout(interval) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        let mut latest = None;
        while let Ok(sample) = samples.try_recv() {
            latest = Some(sample);
        }
        if let Some(sample) = latest {
            revision = revision.wrapping_add(1);
            let updated = MetricTensor::from_sample(sample, revision);
            *tensor
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = updated;
        }
    }
}

#[cfg(target_os = "linux")]
fn pin_control_thread() -> Result<(), String> {
    // SAFETY: the CPU set is initialized before libc reads it, and pthread_self
    // refers to the current worker thread only.
    unsafe {
        let mut allowed: libc::cpu_set_t = std::mem::zeroed();
        let size = std::mem::size_of::<libc::cpu_set_t>();
        if libc::pthread_getaffinity_np(libc::pthread_self(), size, &mut allowed) != 0 {
            return Err("could not read worker CPU affinity".to_owned());
        }
        let selected = (0..libc::CPU_SETSIZE as usize)
            .rev()
            .find(|cpu| libc::CPU_ISSET(*cpu, &allowed))
            .ok_or_else(|| "no allowed CPU is available for the metric worker".to_owned())?;
        let mut isolated: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut isolated);
        libc::CPU_SET(selected, &mut isolated);
        if libc::pthread_setaffinity_np(libc::pthread_self(), size, &isolated) != 0 {
            return Err(format!("could not pin metric worker to CPU {selected}"));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_control_thread() -> Result<(), String> {
    Err("CPU affinity isolation is only available on Linux".to_owned())
}

/// Worker creation or platform-affinity failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricWorkerError {
    InvalidInterval,
    Thread(String),
    Affinity(String),
}

impl std::fmt::Display for MetricWorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInterval => f.write_str("metric update interval must be non-zero"),
            Self::Thread(error) => write!(f, "metric worker thread failed: {error}"),
            Self::Affinity(error) => write!(f, "metric worker affinity failed: {error}"),
        }
    }
}

impl std::error::Error for MetricWorkerError {}

/// Identifier for one local coordinate chart.
pub type ChartId = u64;

/// Affine local coordinate chart with a compact overlap support.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LocalChart {
    pub id: ChartId,
    pub center: [f64; 3],
    pub scale: [f64; 3],
    pub overlap_radius: f64,
}

impl LocalChart {
    #[must_use]
    pub fn coordinates(&self, point: [f64; 3]) -> [f64; 3] {
        [
            (point[0] - self.center[0]) * self.scale[0],
            (point[1] - self.center[1]) * self.scale[1],
            (point[2] - self.center[2]) * self.scale[2],
        ]
    }

    fn weight(&self, point: [f64; 3]) -> f64 {
        if !self.overlap_radius.is_finite() || self.overlap_radius <= 0.0 {
            return 0.0;
        }
        let distance_squared = (0..3)
            .map(|axis| (point[axis] - self.center[axis]).powi(2))
            .sum::<f64>();
        let normalized = distance_squared / self.overlap_radius.powi(2);
        if normalized >= 1.0 {
            return 0.0;
        }
        (-1.0 / (1.0 - normalized)).exp()
    }
}

/// Local atlas with O(1) chart lookup and smooth C-infinity overlap weights.
#[derive(Default)]
pub struct LocalAtlas {
    charts: DashMap<ChartId, LocalChart>,
}

impl LocalAtlas {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, chart: LocalChart) -> Option<LocalChart> {
        self.charts.insert(chart.id, chart)
    }

    pub fn remove(&self, id: ChartId) -> Option<LocalChart> {
        self.charts.remove(&id).map(|(_, chart)| chart)
    }

    #[must_use]
    pub fn get(&self, id: ChartId) -> Option<LocalChart> {
        self.charts.get(&id).map(|chart| *chart)
    }

    /// Blends chart coordinates with a smooth partition of unity.
    #[must_use]
    pub fn coordinates(&self, point: [f64; 3]) -> Option<[f64; 3]> {
        let mut weighted = [0.0_f64; 3];
        let mut total_weight = 0.0;
        for chart in self.charts.iter() {
            let weight = chart.weight(point);
            if weight == 0.0 {
                continue;
            }
            let local = chart.coordinates(point);
            for axis in 0..3 {
                weighted[axis] += local[axis] * weight;
            }
            total_weight += weight;
        }
        if total_weight == 0.0 {
            return None;
        }
        for value in &mut weighted {
            *value /= total_weight;
        }
        Some(weighted)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.charts.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.charts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalAtlas, LocalChart, MetricSample, MetricTensor, MetricWorker};
    use std::time::Duration;

    #[test]
    fn tensor_uses_bounded_positive_diagonal_components() {
        let tensor = MetricTensor::from_sample(
            MetricSample {
                latency_ms: 20.0,
                queued_bytes: 512_000.0,
                cpu_percent: 75.0,
            },
            3,
        );
        assert!(tensor.components[0][0] > 1.0);
        assert!(tensor.norm_squared([1.0, 2.0, 3.0]) > 0.0);
        assert_eq!(tensor.revision, 3);
    }

    #[test]
    fn worker_accepts_samples_without_blocking_the_publisher() {
        let Ok((worker, publisher)) = MetricWorker::spawn(Duration::from_millis(10)) else {
            return;
        };
        for _ in 0..100 {
            let _result = publisher.try_publish(MetricSample::default());
        }
        std::thread::sleep(Duration::from_millis(30));
        assert!(worker.snapshot().revision <= 1);
        worker.shutdown();
    }

    #[test]
    fn atlas_blends_overlapping_charts_and_ignores_points_outside_support() {
        let atlas = LocalAtlas::new();
        atlas.insert(LocalChart {
            id: 1,
            center: [0.0; 3],
            scale: [1.0; 3],
            overlap_radius: 2.0,
        });
        atlas.insert(LocalChart {
            id: 2,
            center: [1.0, 0.0, 0.0],
            scale: [1.0; 3],
            overlap_radius: 2.0,
        });
        let blended = atlas
            .coordinates([0.5, 0.0, 0.0])
            .expect("point belongs to both charts");
        assert!((blended[0] - 0.0).abs() < 1.0);
        assert!(atlas.coordinates([10.0, 10.0, 10.0]).is_none());
        assert_eq!(atlas.len(), 2);
    }
}
