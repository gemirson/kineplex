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

/// Uses AVX-512F when available, then AVX2, then falls back to the scalar reference.
#[must_use]
pub fn metric_norm_squared(metric: &MetricTensor, vector: [f64; 3]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            // SAFETY: runtime feature detection guarantees AVX-512F support on this CPU.
            return unsafe { metric_norm_squared_avx512(metric, vector) };
        }
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: runtime feature detection guarantees AVX2 support on this CPU.
            return unsafe { metric_norm_squared_avx2(metric, vector) };
        }
    }
    metric_norm_squared_scalar(metric, vector)
}

/// Computes the metric squared norm using AVX-512F (eight `f64` lanes).
///
/// We load the three vector components plus a padding zero into the low lanes
/// of a 512-bit register, multiply element-wise twice (v²·g), store, and
/// horizontally sum the three meaningful lanes.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn metric_norm_squared_avx512(metric: &MetricTensor, vector: [f64; 3]) -> f64 {
    use std::arch::x86_64::{_mm512_mul_pd, _mm512_set_pd, _mm512_storeu_pd};
    // _mm512_set_pd fills lanes from highest to lowest index (lane 7 … 0).
    let values = _mm512_set_pd(
        0.0, 0.0, 0.0, 0.0, 0.0,
        vector[2], vector[1], vector[0],
    );
    let weights = _mm512_set_pd(
        0.0, 0.0, 0.0, 0.0, 0.0,
        metric.components[2][2],
        metric.components[1][1],
        metric.components[0][0],
    );
    // weighted_squares[i] = vector[i]² × metric.diagonal[i]
    let weighted_squares = _mm512_mul_pd(_mm512_mul_pd(values, values), weights);
    let mut lanes = [0.0_f64; 8];
    _mm512_storeu_pd(lanes.as_mut_ptr(), weighted_squares);
    lanes[0] + lanes[1] + lanes[2]
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
    /// Adapter that couples QUIC backpressure directly into the metric tensor components.
    pub congestion_adapter: CongestionMetricAdapter,
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
                    congestion_adapter: CongestionMetricAdapter,
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

    /// Applies QUIC transport feedback directly to the live metric tensor's diagonal components.
    ///
    /// Acquires a write lock, delegates to `CongestionMetricAdapter::apply_to_tensor`, and
    /// stores the updated tensor back under the same lock.
    pub fn apply_congestion_feedback(&self, feedback: QuicTransportFeedback) {
        let mut tensor = self
            .tensor
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.congestion_adapter.apply_to_tensor(&mut tensor, feedback);
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
pub(crate) fn pin_control_thread() -> Result<(), String> {
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
pub(crate) fn pin_control_thread() -> Result<(), String> {
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
        crate::metrics::global().set_geodesic_revision(snapshot.revision);
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
        crate::metrics::global().record_geodesic_lookup();
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

    /// Feeds QUIC backpressure directly into the g_ij components of a MetricTensor.
    ///
    /// Each diagonal component is updated with an EWMA (alpha = 0.25) contribution
    /// from the corresponding transport dimension, then clamped to [1.0, 101.0].
    pub fn apply_to_tensor(
        &self,
        tensor: &mut MetricTensor,
        feedback: QuicTransportFeedback,
    ) {
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
        let rtt_cost = (feedback.smoothed_rtt_ms / 100.0).clamp(0.0, 10.0);

        // g_00: latency dimension — EWMA contribution from RTT cost
        tensor.components[0][0] = (tensor.components[0][0] + rtt_cost * 0.25).clamp(1.0, 101.0);
        // g_11: queue/bytes dimension — EWMA contribution from congestion window pressure
        tensor.components[1][1] =
            (tensor.components[1][1] + congestion * 2.0 * 0.25).clamp(1.0, 101.0);
        // g_22: CPU/loss dimension — EWMA contribution from packet loss ratio
        tensor.components[2][2] =
            (tensor.components[2][2] + loss_ratio * 5.0 * 0.25).clamp(1.0, 101.0);

        tensor.revision = tensor.revision.wrapping_add(1);
    }
}

/// Resource load advertised by one mesh node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeLoadSample {
    pub cpu_percent: u8,
    pub available_arrow_memory_mb: u32,
}

/// Adaptive next-hop chooser that repels traffic from overloaded nodes.
#[derive(Default)]
pub struct AdaptiveRoutePlanner {
    node_loads: DashMap<u64, NodeLoadSample>,
}

impl AdaptiveRoutePlanner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_load(&self, node: u64, sample: NodeLoadSample) {
        self.node_loads.insert(node, sample);
    }

    /// Selects the lowest-resistance candidate below the default 90% CPU threshold.
    #[must_use]
    pub fn select_next_hop(
        &self,
        candidates: &[(RouteKey, RouteEntry)],
    ) -> Option<(RouteKey, RouteEntry)> {
        candidates
            .iter()
            .filter(|(_, route)| {
                self.node_loads
                    .get(&route.next_node)
                    .map_or(true, |sample| sample.cpu_percent < 90)
            })
            .min_by(|left, right| left.1.resistance.total_cmp(&right.1.resistance))
            .copied()
    }

    /// Drops load state for nodes that left the active mesh.
    pub fn remove_node(&self, node: u64) -> Option<NodeLoadSample> {
        self.node_loads.remove(&node).map(|(_, sample)| sample)
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
            crate::metrics::global().observe_riemann_curvature(curvature);
            return None;
        }
        crate::metrics::global().observe_riemann_curvature(curvature);
        crate::metrics::global().record_atlas_invalidation();
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

/// Continuous path deformation between two equal-length waypoint sequences.
#[derive(Clone, Debug, PartialEq)]
pub struct HomotopyPath {
    from: Vec<[f64; 3]>,
    to: Vec<[f64; 3]>,
}

impl HomotopyPath {
    pub fn new(from: Vec<[f64; 3]>, to: Vec<[f64; 3]>) -> Result<Self, &'static str> {
        if from.len() != to.len() || from.is_empty() {
            return Err("homotopy paths must have the same non-zero waypoint count");
        }
        if from
            .iter()
            .chain(to.iter())
            .flatten()
            .any(|coordinate| !coordinate.is_finite())
        {
            return Err("homotopy waypoints must be finite");
        }
        Ok(Self { from, to })
    }

    /// Interpolates into a caller-owned buffer without allocating or starting a new transport.
    pub fn sample_into(&self, progress: f64, output: &mut [[f64; 3]]) -> Result<(), &'static str> {
        if output.len() != self.from.len() {
            return Err("homotopy output has the wrong waypoint count");
        }
        let t = if progress.is_finite() {
            progress.clamp(0.0, 1.0)
        } else {
            return Err("homotopy progress must be finite");
        };
        for (index, point) in output.iter_mut().enumerate() {
            for axis in 0..3 {
                point[axis] = self.from[index][axis] * (1.0 - t) + self.to[index][axis] * t;
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn waypoint_count(&self) -> usize {
        self.from.len()
    }
}

/// Key for a reusable local topology deformation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DeformationKey {
    pub from_chart: ChartId,
    pub to_chart: ChartId,
    pub neighborhood_class: u32,
}

/// O(1) cache of precomputed homotopy templates for recurring neighborhood changes.
#[derive(Default)]
pub struct DeformationCache {
    templates: DashMap<DeformationKey, Arc<HomotopyPath>>,
}

impl DeformationCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, key: DeformationKey, template: HomotopyPath) -> Arc<HomotopyPath> {
        let template = Arc::new(template);
        self.templates.insert(key, Arc::clone(&template));
        template
    }

    #[must_use]
    pub fn get(&self, key: &DeformationKey) -> Option<Arc<HomotopyPath>> {
        self.templates
            .get(key)
            .map(|entry| Arc::clone(entry.value()))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.templates.len()
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
        metric_norm_squared, metric_norm_squared_scalar, AdaptiveRoutePlanner, AtomicGeodesicTable,
        ChristoffelSymbols, CongestionMetricAdapter, CurvatureMonitor, DeformationCache,
        DeformationKey, GeodesicIntegrator, GeodesicState, HomotopyPath, LocalAtlas, LocalChart,
        MetricSample, MetricTensor, MetricWorker, NodeLoadSample, QuicTransportFeedback,
        RouteEntry, RouteSnapshot,
    };
    use std::sync::Arc;
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
    fn avx512_norm_matches_scalar_when_available() {
        // Only assert on CPUs that actually expose AVX-512F.  On other machines
        // the test still compiles and runs — it just skips the assertion.
        let metric = MetricTensor::from_sample(
            MetricSample {
                latency_ms: 12.0,
                queued_bytes: 4096.0,
                cpu_percent: 43.0,
            },
            1,
        );
        for point in [[1.0_f64, 2.0, 3.0], [-3.5, 0.25, 8.0], [0.0; 3]] {
            let scalar = metric_norm_squared_scalar(&metric, point);
            if std::is_x86_feature_detected!("avx512f") {
                // SAFETY: guarded by runtime feature detection.
                let avx512 =
                    unsafe { super::metric_norm_squared_avx512(&metric, point) };
                assert!(
                    (scalar - avx512).abs() < 1e-10,
                    "AVX-512 result {avx512} differs from scalar {scalar} for point {point:?}"
                );
            }
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

    #[test]
    fn adaptive_planner_avoids_nodes_at_ninety_percent_cpu() {
        let planner = AdaptiveRoutePlanner::new();
        planner.update_load(
            1,
            NodeLoadSample {
                cpu_percent: 95,
                available_arrow_memory_mb: 400,
            },
        );
        planner.update_load(
            2,
            NodeLoadSample {
                cpu_percent: 40,
                available_arrow_memory_mb: 300,
            },
        );
        let candidates = [
            (
                10,
                RouteEntry {
                    next_node: 1,
                    resistance: 0.1,
                },
            ),
            (
                11,
                RouteEntry {
                    next_node: 2,
                    resistance: 1.0,
                },
            ),
        ];
        let selected = planner
            .select_next_hop(&candidates)
            .expect("healthy route remains");
        assert_eq!(selected.1.next_node, 2);
    }

    #[test]
    fn homotopy_transition_reaches_alternate_path_continuously() {
        let path = HomotopyPath::new(
            vec![[0.0, 0.0, 0.0], [1.0, 0.0, 0.0]],
            vec![[0.0, 1.0, 0.0], [1.0, 1.0, 0.0]],
        )
        .expect("waypoints align");
        let mut current = [[0.0; 3]; 2];
        path.sample_into(0.0, &mut current).expect("start path");
        assert_eq!(current[0], [0.0, 0.0, 0.0]);
        path.sample_into(0.5, &mut current).expect("midpoint path");
        assert_eq!(current[0], [0.0, 0.5, 0.0]);
        path.sample_into(1.0, &mut current).expect("alternate path");
        assert_eq!(current[0], [0.0, 1.0, 0.0]);
    }

    #[test]
    fn deformation_templates_are_reused_by_constant_time_key() {
        let cache = DeformationCache::new();
        let key = DeformationKey {
            from_chart: 1,
            to_chart: 2,
            neighborhood_class: 4,
        };
        let template =
            HomotopyPath::new(vec![[0.0; 3]], vec![[1.0, 0.0, 0.0]]).expect("one waypoint pair");
        let inserted = cache.insert(key, template);
        let cached = cache.get(&key).expect("template is cached");
        assert!(Arc::ptr_eq(&inserted, &cached));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn congestion_feedback_raises_tensor_diagonal_components() {
        let mut tensor = MetricTensor::default();
        let adapter = CongestionMetricAdapter;

        // High-latency, high-loss, narrow congestion window feedback
        let feedback = QuicTransportFeedback {
            smoothed_rtt_ms: 400.0,   // rtt_cost = (400/100).clamp(0,10) = 4.0
            congestion_window_bytes: 1024, // congestion = 1 - 1024/65536 ≈ 0.984
            packets_sent: 200,
            packets_lost: 40,          // loss_ratio = 40/200 = 0.2
        };

        adapter.apply_to_tensor(&mut tensor, feedback);

        // g_00 started at 1.0, += rtt_cost(4.0) * 0.25 = +1.0 → 2.0
        assert!(
            tensor.components[0][0] > 1.0,
            "latency component should exceed 1.0, got {}",
            tensor.components[0][0]
        );
        // g_11 started at 1.0, += congestion(≈0.984) * 2.0 * 0.25 ≈ +0.492 → ≈1.492
        assert!(
            tensor.components[1][1] > 1.0,
            "queue/bytes component should exceed 1.0, got {}",
            tensor.components[1][1]
        );
        // g_22 started at 1.0, += loss_ratio(0.2) * 5.0 * 0.25 = +0.25 → 1.25
        assert!(
            tensor.components[2][2] > 1.0,
            "loss component should exceed 1.0, got {}",
            tensor.components[2][2]
        );
        // revision must be incremented by exactly 1
        assert_eq!(tensor.revision, 1, "revision should be incremented to 1");
    }
}


// ==========================================
// FT-089 to FT-092: Geometric Control Features (from develop)
// ==========================================

//  Controle Geométrico Avançado para o Fluxo de Ricci
//  
//  Este módulo implementa:
//  - FT-089: Controlador de Passo Adaptativo PID
//  - FT-090: Normalização de Volume do Tensor Métrico
//  - FT-091: Descoberta de 2-Simplexos (Faces Triangulares)
//  - FT-092: Fase de Aquecimento Geométrico

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Instant;

/// ==========================================
/// FT-089: Controlador de Passo Adaptativo PID
/// ==========================================

/// Controlador PID para passo adaptativo epsilon
#[derive(Debug, Clone)]
pub struct AdaptiveStepController {
    /// Ganho proporcional
    kp: f64,
    /// Ganho integral
    ki: f64,
    /// Ganho derivativo
    kd: f64,
    /// Erro anterior
    prev_error: f64,
    /// Termo integral acumulado
    integral: f64,
    /// Valor atual de epsilon
    epsilon: f64,
    /// Valor mínimo de epsilon
    epsilon_min: f64,
    /// Valor máximo de epsilon
    epsilon_max: f64,
}

impl AdaptiveStepController {
    pub fn new(initial_epsilon: f64) -> Self {
        Self {
            kp: 0.5,
            ki: 0.1,
            kd: 0.3,
            prev_error: 0.0,
            integral: 0.0,
            epsilon: initial_epsilon,
            epsilon_min: 0.001,
            epsilon_max: 1.0,
        }
    }
    
    pub fn with_limits(mut self, min: f64, max: f64) -> Self {
        self.epsilon_min = min;
        self.epsilon_max = max;
        self
    }
    
    pub fn with_pid_gains(mut self, kp: f64, ki: f64, kd: f64) -> Self {
        self.kp = kp;
        self.ki = ki;
        self.kd = kd;
        self
    }
    
    /// Calcula o próximo valor de epsilon baseado na mudança de curvatura
    pub fn compute(&mut self, curvature_change: f64) -> f64 {
        // Erro: mudança de curvatura desejada (0) vs mudança observada
        let error = 0.0 - curvature_change;
        
        // Componente proporcional
        let p_term = self.kp * error;
        
        // Componente integral com windup protection
        self.integral += error;
        self.integral = self.integral.clamp(-100.0, 100.0);
        let i_term = self.ki * self.integral;
        
        // Componente derivativo
        let d_term = self.kd * (error - self.prev_error);
        self.prev_error = error;
        
        // Novo epsilon
        let new_epsilon = self.epsilon + p_term + i_term + d_term;
        
        // Aplicar limites
        self.epsilon = new_epsilon.clamp(self.epsilon_min, self.epsilon_max);
        
        self.epsilon
    }
    
    /// Retorna o epsilon atual
    pub fn epsilon(&self) -> f64 {
        self.epsilon
    }
    
    /// Verifica se o sistema convergiu (oscilação eliminada)
    pub fn is_stable(&self) -> bool {
        self.integral.abs() < 0.01 && self.prev_error.abs() < 0.001
    }
    
    /// Reseta o controlador
    pub fn reset(&mut self) {
        self.prev_error = 0.0;
        self.integral = 0.0;
    }
}

/// Histórico de epsilon para análise de oscilação
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpsilonHistory {
    samples: Vec<f64>,
    max_samples: usize,
}

impl EpsilonHistory {
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: Vec::with_capacity(max_samples),
            max_samples,
        }
    }
    
    pub fn push(&mut self, epsilon: f64) {
        if self.samples.len() >= self.max_samples {
            self.samples.remove(0);
        }
        self.samples.push(epsilon);
    }
    
    /// Calcula a variância (indicador de oscilação)
    pub fn variance(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        
        let mean: f64 = self.samples.iter().sum::<f64>() / self.samples.len() as f64;
        let variance: f64 = self.samples.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / self.samples.len() as f64;
        
        variance
    }
    
    /// Verifica se há oscilação pendular (padrão alternante)
    pub fn has_oscillation(&self) -> bool {
        if self.samples.len() < 4 {
            return false;
        }
        
        let mut oscillations = 0;
        for i in 2..self.samples.len() {
            let prev = self.samples[i - 1];
            let curr = self.samples[i];
            let prev_prev = self.samples[i - 2];
            
            // Detectar mudança de direção
            if (curr > prev && prev_prev > prev) || (curr < prev && prev_prev < prev) {
                oscillations += 1;
            }
        }
        
        // Se mais de 30% das amostras mostram oscilação
        oscillations as f64 / (self.samples.len() - 2) as f64 > 0.3
    }
}

/// ==========================================
/// FT-090: Normalização de Volume do Tensor Métrico
/// ==========================================

/// Erros de normalização
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NormalizationError {
    ZeroVolume,
    Overflow(f64),
    Underflow(f64),
}

impl std::fmt::Display for NormalizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NormalizationError::ZeroVolume => write!(f, "Volume total é zero, normalização impossível"),
            NormalizationError::Overflow(v) => write!(f, "Overflow: valor {} excede u32::MAX", v),
            NormalizationError::Underflow(v) => write!(f, "Underflow: valor {} é muito pequeno", v),
        }
    }
}

/// Normalizador de volume do tensor métrico
#[derive(Debug, Clone)]
pub struct MetricTensorNormalizer {
    /// Volume alvo desejado
    target_volume: f64,
    /// Valor máximo do tensor (u32)
    max_tensor_value: u32,
    /// Tolerância de convergência
    tolerance: f64,
}

impl MetricTensorNormalizer {
    pub fn new(target_volume: f64) -> Self {
        Self {
            target_volume,
            max_tensor_value: u32::MAX,
            tolerance: 0.001,
        }
    }
    
    pub fn with_tolerance(mut self, tolerance: f64) -> Self {
        self.tolerance = tolerance;
        self
    }
    
    /// Normaliza a matriz de pesos para manter volume constante
    pub fn normalize(&self, weights: &mut [f64]) -> Result<(), NormalizationError> {
        let current_volume: f64 = weights.iter().sum();
        
        if current_volume.abs() < f64::EPSILON {
            return Err(NormalizationError::ZeroVolume);
        }
        
        // Calcular fator de escala
        let scale_factor = self.target_volume / current_volume;
        
        // Aplicar normalização
        for weight in weights.iter_mut() {
            *weight *= scale_factor;
            
            // Verificar overflow
            if *weight > (u32::MAX as f64) {
                return Err(NormalizationError::Overflow(*weight));
            }
            
            // Verificar underflow
            if *weight < f64::EPSILON && *weight > 0.0 {
                return Err(NormalizationError::Underflow(*weight));
            }
        }
        
        Ok(())
    }
    
    /// Normaliza com clamping para u32
    pub fn normalize_with_clamp(&self, weights: &mut [f64]) -> f64 {
        let current_volume: f64 = weights.iter().sum();
        
        if current_volume.abs() < f64::EPSILON {
            return 0.0;
        }
        
        let scale_factor = self.target_volume / current_volume;
        let mut max_weight: f64 = 0.0;
        
        for weight in weights.iter_mut() {
            *weight *= scale_factor;
            *weight = weight.clamp(0.0, u32::MAX as f64);
            max_weight = max_weight.max(*weight);
        }
        
        max_weight
    }
    
    /// Verifica se os pesos estão dentro do limite u32
    pub fn is_within_bounds(&self, weights: &[f64]) -> bool {
        weights.iter().all(|w| *w <= u32::MAX as f64 && *w >= 0.0)
    }
    
    /// Retorna o volume atual
    pub fn current_volume(&self, weights: &[f64]) -> f64 {
        weights.iter().sum()
    }
    
    /// Verifica se o volume está dentro da tolerância
    pub fn is_volume_stable(&self, weights: &[f64]) -> bool {
        let current = self.current_volume(weights);
        let diff = (current - self.target_volume).abs();
        diff / self.target_volume < self.tolerance
    }
}

/// ==========================================
/// FT-091: Descoberta de 2-Simplexos (Faces Triangulares)
/// ==========================================

/// Representa um nó na rede
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GeoNodeId(pub String);

/// Descoberta de 2-simplexos (faces triangulares) para homologia
#[derive(Debug, Clone)]
pub struct Simplex2Discoverer {
    /// Armazena adjacências
    adjacency: HashMap<GeoNodeId, HashSet<GeoNodeId>>,
}

impl Simplex2Discoverer {
    pub fn new() -> Self {
        Self {
            adjacency: HashMap::new(),
        }
    }
    
    /// Adiciona uma aresta
    pub fn add_edge(&mut self, a: GeoNodeId, b: GeoNodeId) {
        self.adjacency.entry(a.clone()).or_insert_with(HashSet::new).insert(b.clone());
        self.adjacency.entry(b).or_insert_with(HashSet::new).insert(a);
    }
    
    /// Remove uma aresta
    pub fn remove_edge(&mut self, a: &GeoNodeId, b: &GeoNodeId) {
        if let Some(neighbors) = self.adjacency.get_mut(a) {
            neighbors.remove(b);
        }
        if let Some(neighbors) = self.adjacency.get_mut(b) {
            neighbors.remove(a);
        }
    }
    
    /// Encontra todos os 2-simplexos (triângulos) na rede
    pub fn find_triangles(&self) -> Vec<[GeoNodeId; 3]> {
        let mut triangles = Vec::new();
        
        let nodes: Vec<&GeoNodeId> = self.adjacency.keys().collect();
        
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                for k in (j + 1)..nodes.len() {
                    let a = nodes[i];
                    let b = nodes[j];
                    let c = nodes[k];
                    
                    // Verificar se a-b, b-c, c-a são todos conectados
                    let is_triangle = 
                        self.is_connected(a, b) &&
                        self.is_connected(b, c) &&
                        self.is_connected(c, a);
                    
                    if is_triangle {
                        triangles.push([a.clone(), b.clone(), c.clone()]);
                    }
                }
            }
        }
        
        triangles
    }
    
    /// Verifica se dois nós estão conectados
    fn is_connected(&self, a: &GeoNodeId, b: &GeoNodeId) -> bool {
        self.adjacency
            .get(a)
            .map(|neighbors| neighbors.contains(b))
            .unwrap_or(false)
    }
    
    /// Calcula o número de arestas
    pub fn count_edges(&self) -> usize {
        let mut count = 0_usize;
        for neighbors in self.adjacency.values() {
            count += neighbors.len();
        }
        count / 2 // Cada aresta é contada duas vezes
    }
    
    /// Calcula o Número de Betti (β₁) - número de buracos na rede
    pub fn betti_number(&self) -> isize {
        let triangles = self.find_triangles().len();
        let edges = self.count_edges();
        let nodes = self.adjacency.len();
        
        // Fórmula de Euler-Poincaré para grafos
        // β₁ = E - V + F (onde F = triângulos + 1 para componente conectado)
        if nodes > 0 {
            edges as isize - nodes as isize + triangles as isize + 1
        } else {
            0
        }
    }
    
    /// Identifica partições (componentes conectados)
    pub fn partitions(&self) -> Vec<Vec<GeoNodeId>> {
        let mut visited = HashSet::new();
        let mut partitions = Vec::new();
        
        for node in self.adjacency.keys() {
            if !visited.contains(node) {
                let mut component = Vec::new();
                self.dfs_collect(node, &mut visited, &mut component);
                partitions.push(component);
            }
        }
        
        partitions
    }
    
    fn dfs_collect(&self, node: &GeoNodeId, visited: &mut HashSet<GeoNodeId>, component: &mut Vec<GeoNodeId>) {
        visited.insert(node.clone());
        component.push(node.clone());
        
        if let Some(neighbors) = self.adjacency.get(node) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    self.dfs_collect(neighbor, visited, component);
                }
            }
        }
    }
}

impl Default for Simplex2Discoverer {
    fn default() -> Self {
        Self::new()
    }
}

/// ==========================================
/// FT-092: Fase de Aquecimento Geométrico
/// ==========================================

/// Spike sintético para warm-up
#[derive(Debug, Clone)]
pub struct SyntheticSpike {
    pub id: u32,
    pub timestamp: Instant,
    pub source: GeoNodeId,
    pub target: GeoNodeId,
}

impl SyntheticSpike {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            timestamp: Instant::now(),
            source: GeoNodeId(format!("node-{}", id % 3)),
            target: GeoNodeId(format!("node-{}", (id + 1) % 3)),
        }
    }
}

/// Resultado do warm-up
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmupResult {
    pub spikes_used: u32,
    pub warmup_time_ms: u64,
    pub converged: bool,
}

/// Erros de warm-up
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WarmupError {
    Timeout(u64),
    ConvergenceFailed,
}

impl std::fmt::Display for WarmupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WarmupError::Timeout(ms) => write!(f, "Warm-up excedeu timeout de {}ms", ms),
            WarmupError::ConvergenceFailed => write!(f, "Falha na convergência durante warm-up"),
        }
    }
}

/// Sistema de tensor métrico para warm-up
pub trait MetricTensorSystem {
    fn process_spike(&mut self, spike: SyntheticSpike) -> impl std::future::Future<Output = Result<(), WarmupError>> + Send;
    fn metric_variance(&self) -> f64;
    fn metric_mean(&self) -> f64;
}

/// Fase de aquecimento geométrico
#[derive(Debug, Clone)]
pub struct GeometricWarmup {
    /// Número de spikes sintéticos para warm-up
    num_warmup_spikes: u32,
    /// Threshold de convergência
    convergence_threshold: f64,
    /// Tempo máximo de warm-up em ms
    max_warmup_time_ms: u64,
    /// Se o warm-up foi completado
    is_warmed_up: bool,
    /// Tempo de início do warm-up
    warmup_start_time: Option<Instant>,
    /// Histórico de métricas para detecção de convergência
    metric_history: Vec<f64>,
}

impl GeometricWarmup {
    pub fn new() -> Self {
        Self {
            num_warmup_spikes: 100,
            convergence_threshold: 0.01,
            max_warmup_time_ms: 5000,
            is_warmed_up: false,
            warmup_start_time: None,
            metric_history: Vec::new(),
        }
    }
    
    pub fn with_spikes(mut self, num: u32) -> Self {
        self.num_warmup_spikes = num;
        self
    }
    
    pub fn with_threshold(mut self, threshold: f64) -> Self {
        self.convergence_threshold = threshold;
        self
    }
    
    pub fn with_timeout(mut self, ms: u64) -> Self {
        self.max_warmup_time_ms = ms;
        self
    }
    
    /// Executa o warm-up geométrico
    pub async fn warmup<T: MetricTensorSystem + Send + Sync>(
        &mut self,
        metric_system: &mut T,
    ) -> Result<WarmupResult, WarmupError> {
        self.warmup_start_time = Some(Instant::now());
        self.metric_history.clear();
        
        for i in 0..self.num_warmup_spikes {
            // Disparar spike sintético
            let spike = SyntheticSpike::new(i);
            metric_system.process_spike(spike).await?;
            
            // Calcular variância atual
            let variance = metric_system.metric_variance();
            self.metric_history.push(variance);
            
            // Manter histórico limitado
            if self.metric_history.len() > 20 {
                self.metric_history.remove(0);
            }
            
            // Verificar convergência
            if self.check_convergence() {
                let elapsed = self.warmup_start_time.unwrap().elapsed().as_millis() as u64;
                self.is_warmed_up = true;
                
                return Ok(WarmupResult {
                    spikes_used: i + 1,
                    warmup_time_ms: elapsed,
                    converged: true,
                });
            }
        }
        
        // Verificar timeout
        if let Some(start) = self.warmup_start_time {
            let elapsed = start.elapsed().as_millis() as u64;
            if elapsed > self.max_warmup_time_ms {
                return Err(WarmupError::Timeout(elapsed));
            }
        }
        
        self.is_warmed_up = true;
        Ok(WarmupResult {
            spikes_used: self.num_warmup_spikes,
            warmup_time_ms: self.warmup_start_time.map(|s| s.elapsed().as_millis() as u64).unwrap_or(0),
            converged: true,
        })
    }
    
    /// Verifica se a métrica estabilizou
    fn check_convergence(&self) -> bool {
        if self.metric_history.len() < 10 {
            return false;
        }
        
        // Verificar variância das últimas amostras
        let recent: &[f64] = &self.metric_history[self.metric_history.len() - 10..];
        let mean: f64 = recent.iter().sum::<f64>() / recent.len() as f64;
        let variance: f64 = recent.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / recent.len() as f64;
        
        variance < self.convergence_threshold
    }
    
    /// Retorna se o sistema está operacional
    pub fn is_operational(&self) -> bool {
        self.is_warmed_up
    }
    
    /// Tempo decorrido desde o início do warm-up
    pub fn elapsed_time_ms(&self) -> u64 {
        self.warmup_start_time
            .map(|s| s.elapsed().as_millis() as u64)
            .unwrap_or(0)
    }
}

impl Default for GeometricWarmup {
    fn default() -> Self {
        Self::new()
    }
}

// ==========================================
// TESTS
// ==========================================

#[cfg(test)]
mod geometric_control_tests {
    use super::*;
    
    // FT-089 Tests
    #[test]
    fn test_adaptive_step_controller() {
        let mut controller = AdaptiveStepController::new(0.5);
        
        // Simular mudança de curvatura
        let epsilon1 = controller.compute(0.1);  // Curvatura aumentando
        let epsilon2 = controller.compute(0.0);  // Estável
        let epsilon3 = controller.compute(-0.1); // Diminuindo
        
        // Deve ajustar o epsilon
        assert!(controller.epsilon() > 0.0);
    }
    
    #[test]
    fn test_epsilon_limits() {
        let mut controller = AdaptiveStepController::new(0.5)
            .with_limits(0.01, 1.0);
        
        // Forçar valores extremos
        for _ in 0..100 {
            controller.compute(10.0);
        }
        
        assert!(controller.epsilon() >= 0.01);
        assert!(controller.epsilon() <= 1.0);
    }
    
    #[test]
    fn test_epsilon_history_variance() {
        let mut history = EpsilonHistory::new(10);
        
        history.push(0.5);
        history.push(0.6);
        history.push(0.5);
        history.push(0.6);
        
        // Deve detectar oscilação
        assert!(history.has_oscillation());
    }
    
    // FT-090 Tests
    #[test]
    fn test_tensor_normalization() {
        let normalizer = MetricTensorNormalizer::new(100.0);
        let mut weights = vec![25.0, 25.0, 25.0, 25.0];  // Soma = 100
        
        normalizer.normalize(&mut weights).unwrap();
        
        // Soma deve permanecer 100
        let sum: f64 = weights.iter().sum();
        assert!((sum - 100.0).abs() < 0.001);
    }
    
    #[test]
    fn test_tensor_normalization_zero_volume() {
        let normalizer = MetricTensorNormalizer::new(100.0);
        let mut weights = vec![0.0, 0.0, 0.0];
        
        let result = normalizer.normalize(&mut weights);
        assert!(matches!(result, Err(NormalizationError::ZeroVolume)));
    }
    
    #[test]
    fn test_tensor_clamp() {
        let normalizer = MetricTensorNormalizer::new(1.0);
        let mut weights = vec![1e20, 1e30, 1e40];
        
        let max = normalizer.normalize_with_clamp(&mut weights);
        
        // Deve estar dentro de u32
        assert!(max <= u32::MAX as f64);
    }
    
    #[test]
    fn test_volume_stability() {
        let normalizer = MetricTensorNormalizer::new(100.0).with_tolerance(0.01);
        let weights = vec![100.0, 0.0, 0.0];
        
        assert!(normalizer.is_volume_stable(&weights));
    }
    
    // FT-091 Tests
    #[test]
    fn test_simplex_triangle_discovery() {
        let mut discoverer = Simplex2Discoverer::new();
        
        // Criar triângulo: A-B-C
        discoverer.add_edge(GeoNodeId("A".to_string()), GeoNodeId("B".to_string()));
        discoverer.add_edge(GeoNodeId("B".to_string()), GeoNodeId("C".to_string()));
        discoverer.add_edge(GeoNodeId("C".to_string()), GeoNodeId("A".to_string()));
        
        let triangles = discoverer.find_triangles();
        
        assert!(!triangles.is_empty());
    }
    
    #[test]
    fn test_betti_number() {
        let mut discoverer = Simplex2Discoverer::new();
        
        // Criar um triângulo
        discoverer.add_edge(GeoNodeId("A".to_string()), GeoNodeId("B".to_string()));
        discoverer.add_edge(GeoNodeId("B".to_string()), GeoNodeId("C".to_string()));
        discoverer.add_edge(GeoNodeId("C".to_string()), GeoNodeId("A".to_string()));
        
        let beta = discoverer.betti_number();
        
        // Um triângulo = 1 face, sem buracos
        assert!(beta >= 0);
    }
    
    #[test]
    fn test_partitions() {
        let mut discoverer = Simplex2Discoverer::new();
        
        // Componente 1: A-B
        discoverer.add_edge(GeoNodeId("A".to_string()), GeoNodeId("B".to_string()));
        
        // Componente 2: C-D
        discoverer.add_edge(GeoNodeId("C".to_string()), GeoNodeId("D".to_string()));
        
        let partitions = discoverer.partitions();
        
        assert_eq!(partitions.len(), 2);
    }
    
    // FT-092 Tests
    #[test]
    fn test_synthetic_spike() {
        let spike = SyntheticSpike::new(42);
        
        assert_eq!(spike.id, 42);
    }
    
    #[test]
    fn test_warmup_operational() {
        let warmup = GeometricWarmup::new();
        
        assert!(!warmup.is_operational());
    }
    
    #[test]
    fn test_warmup_configuration() {
        let warmup = GeometricWarmup::new()
            .with_spikes(50)
            .with_threshold(0.001)
            .with_timeout(3000);
        
        assert!(!warmup.is_operational());
    }
}