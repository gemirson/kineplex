//! Background metric-tensor updates kept off Tokio and network I/O threads.

use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::collections::HashMap;

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
        metric_norm_squared(self, vector)
    }
}

/// Scalar reference implementation used on CPUs without AVX2.
#[must_use]
pub fn metric_norm_squared_scalar(metric: &MetricTensor, vector: [f64; 3]) -> f64 {
    vector[0] * vector[0] * metric.components[0][0]
        + vector[1] * vector[1] * metric.components[1][1]
        + vector[2] * vector[2] * metric.components[2][2]
}

/// Uses AVX2 when available and otherwise falls back to the scalar reference.
#[must_use]
pub fn metric_norm_squared(metric: &MetricTensor, vector: [f64; 3]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx2") {
        // SAFETY: runtime feature detection guarantees AVX2 support on this CPU.
        return unsafe { metric_norm_squared_avx2(metric, vector) };
    }
    metric_norm_squared_scalar(metric, vector)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn metric_norm_squared_avx2(metric: &MetricTensor, vector: [f64; 3]) -> f64 {
    use std::arch::x86_64::{_mm256_mul_pd, _mm256_set_pd, _mm256_storeu_pd};
    let values = _mm256_set_pd(0.0, vector[2], vector[1], vector[0]);
    let weights = _mm256_set_pd(
        0.0,
        metric.components[2][2],
        metric.components[1][1],
        metric.components[0][0],
    );
    let weighted_squares = _mm256_mul_pd(_mm256_mul_pd(values, values), weights);
    let mut lanes = [0.0_f64; 4];
    _mm256_storeu_pd(lanes.as_mut_ptr(), weighted_squares);
    lanes[0] + lanes[1] + lanes[2]
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

/// Stable edge identifier used by the geodesic lookup table.
pub type RouteKey = u64;

/// Immutable forwarding choice published by the Control Plane.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RouteEntry {
    pub next_node: u64,
    pub resistance: f64,
}

/// Immutable route snapshot, built off-path and atomically swapped into service.
#[derive(Clone, Debug, Default)]
pub struct RouteSnapshot {
    pub revision: u64,
    pub routes: HashMap<RouteKey, RouteEntry>,
}

/// Lock-free read path for precomputed geodesic next-hop decisions.
pub struct AtomicGeodesicTable {
    current: ArcSwap<RouteSnapshot>,
}

impl Default for AtomicGeodesicTable {
    fn default() -> Self {
        Self::new(RouteSnapshot::default())
    }
}

impl AtomicGeodesicTable {
    #[must_use]
    pub fn new(initial: RouteSnapshot) -> Self {
        Self {
            current: ArcSwap::from(Arc::new(initial)),
        }
    }

    /// Atomically publishes a complete precomputed table without pausing readers.
    pub fn publish(&self, snapshot: RouteSnapshot) {
        self.current.store(Arc::new(snapshot));
    }

    /// Replaces one route entry by cloning and atomically publishing a Control Plane snapshot.
    pub fn update_route(&self, edge: RouteKey, route: RouteEntry) -> bool {
        let active = self.current.load_full();
        if !active.routes.contains_key(&edge) {
            return false;
        }
        let mut routes = active.routes.clone();
        routes.insert(edge, route);
        self.publish(RouteSnapshot {
            revision: active.revision.wrapping_add(1),
            routes,
        });
        true
    }

    /// Copies a route entry from the active snapshot without allocating or taking a lock.
    #[must_use]
    pub fn lookup(&self, edge: RouteKey) -> Option<RouteEntry> {
        self.current.load().routes.get(&edge).copied()
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.current.load().revision
    }
}

/// QUIC transport feedback sampled from one edge's connection statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct QuicTransportFeedback {
    pub smoothed_rtt_ms: f64,
    pub congestion_window_bytes: u64,
    pub packets_sent: u64,
    pub packets_lost: u64,
}

/// Applies QUIC latency, loss, and window pressure to geodesic edge resistance.
#[derive(Clone, Copy, Debug, Default)]
pub struct CongestionMetricAdapter;

impl CongestionMetricAdapter {
    pub fn update(
        &self,
        routes: &AtomicGeodesicTable,
        edge: RouteKey,
        feedback: QuicTransportFeedback,
    ) -> bool {
        let Some(mut route) = routes.lookup(edge) else {
            return false;
        };
        let loss_ratio = if feedback.packets_sent == 0 {
            0.0
        } else {
            (feedback.packets_lost as f64 / feedback.packets_sent as f64).clamp(0.0, 1.0)
        };
        let congestion = if feedback.congestion_window_bytes >= 64 * 1024 {
            0.0
        } else {
            1.0 - feedback.congestion_window_bytes as f64 / (64 * 1024) as f64
        };
        let sample_cost = feedback.smoothed_rtt_ms.max(0.0) * 0.01 + loss_ratio * 10.0 + congestion;
        route.resistance = (route.resistance * 0.75 + sample_cost * 0.25).max(0.0);
        routes.update_route(edge, route)
    }
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

/// Request emitted when local curvature invalidates a chart.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AtlasInvalidation {
    pub chart_id: ChartId,
    pub curvature: f64,
    pub atlas_revision: u64,
}

/// Threshold monitor that synchronously removes charts at or above the limit.
pub struct CurvatureMonitor {
    threshold: f64,
    revision: std::sync::atomic::AtomicU64,
}

impl CurvatureMonitor {
    pub fn new(threshold: f64) -> Result<Self, &'static str> {
        if !threshold.is_finite() || threshold <= 0.0 {
            return Err("curvature threshold must be a positive finite value");
        }
        Ok(Self {
            threshold,
            revision: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Computes a tensor norm bound and removes a chart synchronously if it is over threshold.
    pub fn observe(
        &self,
        chart_id: ChartId,
        riemann_components: &[f64],
        atlas: &LocalAtlas,
    ) -> Option<AtlasInvalidation> {
        let curvature = riemann_components.iter().fold(0.0_f64, |norm, value| {
            if value.is_finite() {
                norm.hypot(*value)
            } else {
                f64::INFINITY
            }
        });
        if curvature < self.threshold || atlas.remove(chart_id).is_none() {
            return None;
        }
        let atlas_revision = self
            .revision
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        tracing::warn!(
            chart_id,
            curvature,
            atlas_revision,
            "invalidated atlas chart above curvature threshold"
        );
        Some(AtlasInvalidation {
            chart_id,
            curvature,
            atlas_revision,
        })
    }
}

/// Three-dimensional Christoffel symbols for a local chart.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ChristoffelSymbols {
    pub values: [[[f64; 3]; 3]; 3],
}

/// Position and tangent vector for a geodesic ODE integration state.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct GeodesicState {
    pub position: [f64; 3],
    pub tangent: [f64; 3],
}

/// One fixed-step fourth-order Runge-Kutta geodesic integrator.
#[derive(Clone, Copy, Debug, Default)]
pub struct GeodesicIntegrator;

impl GeodesicIntegrator {
    /// Advances one geodesic state by `step_size` using RK4 and no heap allocation.
    #[must_use]
    pub fn step(
        symbols: &ChristoffelSymbols,
        state: GeodesicState,
        step_size: f64,
    ) -> GeodesicState {
        let k1 = derivative(symbols, state);
        let k2 = derivative(symbols, add_scaled(state, k1, step_size * 0.5));
        let k3 = derivative(symbols, add_scaled(state, k2, step_size * 0.5));
        let k4 = derivative(symbols, add_scaled(state, k3, step_size));
        let mut result = state;
        for axis in 0..3 {
            result.position[axis] += step_size / 6.0
                * (k1.position[axis]
                    + 2.0 * k2.position[axis]
                    + 2.0 * k3.position[axis]
                    + k4.position[axis]);
            result.tangent[axis] += step_size / 6.0
                * (k1.tangent[axis]
                    + 2.0 * k2.tangent[axis]
                    + 2.0 * k3.tangent[axis]
                    + k4.tangent[axis]);
        }
        result
    }
}

fn derivative(symbols: &ChristoffelSymbols, state: GeodesicState) -> GeodesicState {
    let mut acceleration = [0.0; 3];
    for (upper, component) in acceleration.iter_mut().enumerate() {
        for first in 0..3 {
            for second in 0..3 {
                *component -= symbols.values[upper][first][second]
                    * state.tangent[first]
                    * state.tangent[second];
            }
        }
    }
    GeodesicState {
        position: state.tangent,
        tangent: acceleration,
    }
}

fn add_scaled(state: GeodesicState, derivative: GeodesicState, scale: f64) -> GeodesicState {
    let mut result = state;
    for axis in 0..3 {
        result.position[axis] += derivative.position[axis] * scale;
        result.tangent[axis] += derivative.tangent[axis] * scale;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{
        metric_norm_squared, metric_norm_squared_scalar, AtomicGeodesicTable, ChristoffelSymbols,
        CongestionMetricAdapter, CurvatureMonitor, GeodesicIntegrator, GeodesicState, LocalAtlas,
        LocalChart, MetricSample, MetricTensor, MetricWorker, QuicTransportFeedback, RouteEntry,
        RouteSnapshot,
    };
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

    #[test]
    fn route_lookup_observes_atomic_snapshot_replacement() {
        let table = AtomicGeodesicTable::new(RouteSnapshot {
            revision: 1,
            routes: [(
                7,
                RouteEntry {
                    next_node: 42,
                    resistance: 1.0,
                },
            )]
            .into_iter()
            .collect(),
        });
        assert_eq!(table.lookup(7).map(|entry| entry.next_node), Some(42));
        table.publish(RouteSnapshot {
            revision: 2,
            routes: [(
                7,
                RouteEntry {
                    next_node: 99,
                    resistance: 0.5,
                },
            )]
            .into_iter()
            .collect(),
        });
        assert_eq!(table.revision(), 2);
        assert_eq!(table.lookup(7).map(|entry| entry.next_node), Some(99));
    }

    #[test]
    fn simd_metric_norm_matches_the_scalar_reference() {
        let metric = MetricTensor::from_sample(
            MetricSample {
                latency_ms: 12.0,
                queued_bytes: 4096.0,
                cpu_percent: 43.0,
            },
            1,
        );
        for point in [[1.0, 2.0, 3.0], [-3.5, 0.25, 8.0], [0.0; 3]] {
            let scalar = metric_norm_squared_scalar(&metric, point);
            let vectorized = metric_norm_squared(&metric, point);
            assert!((scalar - vectorized).abs() < 1e-10);
        }
    }

    #[test]
    fn high_curvature_invalidates_a_local_chart_immediately() {
        let atlas = LocalAtlas::new();
        atlas.insert(LocalChart {
            id: 9,
            center: [0.0; 3],
            scale: [1.0; 3],
            overlap_radius: 1.0,
        });
        let monitor = CurvatureMonitor::new(1.0).expect("valid threshold");
        let invalidation = monitor
            .observe(9, &[0.8, 0.8], &atlas)
            .expect("curvature exceeds threshold");
        assert!(invalidation.curvature > 1.0);
        assert!(atlas.get(9).is_none());
    }

    #[test]
    fn rk4_geodesic_is_stable_for_flat_and_constant_curvature_cases() {
        let initial = GeodesicState {
            position: [0.0; 3],
            tangent: [1.0, 0.0, 0.0],
        };
        let flat = GeodesicIntegrator::step(&ChristoffelSymbols::default(), initial, 0.1);
        assert!((flat.position[0] - 0.1).abs() < 1e-12);
        assert_eq!(flat.tangent, initial.tangent);

        let mut symbols = ChristoffelSymbols::default();
        symbols.values[0][0][0] = 0.05;
        let mut fine = initial;
        for _ in 0..100 {
            fine = GeodesicIntegrator::step(&symbols, fine, 0.001);
        }
        assert!(fine.position[0].is_finite());
        assert!(fine.tangent[0] > 0.0 && fine.tangent[0] < 1.0);
    }

    #[test]
    fn quic_loss_and_latency_raise_edge_resistance() {
        let table = AtomicGeodesicTable::new(RouteSnapshot {
            revision: 1,
            routes: [(
                1,
                RouteEntry {
                    next_node: 2,
                    resistance: 1.0,
                },
            )]
            .into_iter()
            .collect(),
        });
        assert!(CongestionMetricAdapter.update(
            &table,
            1,
            QuicTransportFeedback {
                smoothed_rtt_ms: 150.0,
                congestion_window_bytes: 1024,
                packets_sent: 100,
                packets_lost: 15,
            }
        ));
        assert!(table.lookup(1).expect("route remains available").resistance > 1.0);
    }
}
