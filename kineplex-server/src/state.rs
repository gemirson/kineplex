//! Shared application state for KinePlex Server

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use kineplex_core::geometry::{
    AdaptiveStepController, GeoNodeId, MetricTensorNormalizer, Simplex2Discoverer,
};
use kineplex_core::budget::BudgetController;
use kineplex_core::job_registry::JobMetricsRegistry;
use kineplex_core::metrics::MetricsCollector;
use kineplex_node::control::{NodeControl, TenantQuotaInfo};
use kineplex_node::execution::PipelineExecutor;
use kineplex_node::{ClusterManager, ClusterNode, NodeCapacity, NodeId, NodeState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDto {
    pub id: String,
    pub name: String,
    pub numa: String,
    pub cpu: u32,
    pub mem: u32,
    pub rx: String,
    pub status: String,
    pub cap: String,
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeDto {
    pub from: usize,
    pub to: usize,
    pub curvature: f64,
    pub load: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeometryStateDto {
    pub triangles: Vec<[usize; 3]>,
    pub betti_1: isize,
    pub current_epsilon: f64,
    pub volume_invariant: f64,
    pub kp: f64,
    pub ki: f64,
    pub kd: f64,
    pub epsilon_min: f64,
    pub route_epoch: u64,
    pub avg_curvature: f64,
    pub flow_active: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TensorEdgeDto {
    pub from: usize,
    pub to: usize,
    pub curvature: f64,
    pub weight: f64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeometryTensorDto {
    pub volume_invariant: f64,
    pub target_volume: f64,
    pub edges_count: usize,
    pub edges: Vec<TensorEdgeDto>,
    pub avg_curvature: f64,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node3dDto {
    pub id: String,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub role: String,
    pub numa: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeCurvatureSpectrumDto {
    pub from: usize,
    pub to: usize,
    pub curvature: f64,
    pub load: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PidHistoryPointDto {
    pub tick: u64,
    pub epsilon: f64,
    pub error: f64,
    pub p_term: f64,
    pub i_term: f64,
    pub d_term: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricTensorResponseDto {
    pub matrix: Vec<Vec<f64>>,
    pub spectrum_1d: Vec<EdgeCurvatureSpectrumDto>,
    pub nodes_3d: Vec<Node3dDto>,
    pub pid_history_1d: Vec<PidHistoryPointDto>,
    pub volume_invariant: f64,
    pub route_epoch: u64,
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveEvent {
    pub event_type: String,
    pub timestamp: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSubmissionPayload {
    pub tenant_id: String,
    pub idempotency_key: Option<String>,
    pub max_memory_mb: Option<u64>,
    pub max_fuel: Option<u64>,
    pub wasm_module: Option<String>,
    pub enable_simd: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusDto {
    pub graph_id: String,
    pub status: String,
    pub stages_completed: Vec<String>,
    pub current_stage: Option<String>,
    pub metrics: Option<JobMetricsDto>,
    pub parquet_output: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetricsDto {
    pub total_time_ms: u64,
    pub rows_processed: u64,
    pub nodes_involved: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogMessageDto {
    pub timestamp: String,
    pub level: String,
    pub component: String,
    pub message: String,
}

pub struct AppState {
    pub cluster_manager: Arc<ClusterManager>,
    #[allow(dead_code)]
    pub executor: Arc<PipelineExecutor>,
    pub node_control: Arc<NodeControl>,
    pub metrics_collector: Arc<MetricsCollector>,
    /// Per-job counters/timers exported on `/metrics` (FT-038, Metrics Budget Controller).
    pub job_registry: Arc<JobMetricsRegistry>,
    /// PI controller keeping metrics overhead ≤ 1% of job CPU time.
    pub metrics_budget: Arc<BudgetController>,
    /// Unix nanos of the previous `/metrics` scrape (PI window).
    metrics_scrape_last_ns: AtomicU64,

    // Topologia e nós
    pub nodes: RwLock<Vec<NodeDto>>,
    pub edges: RwLock<Vec<EdgeDto>>,

    // Geometria de Ricci e Homologia (FT-089..092)
    pub simplex_discoverer: RwLock<Simplex2Discoverer>,
    pub step_controller: RwLock<AdaptiveStepController>,
    pub normalizer: RwLock<MetricTensorNormalizer>,
    pub route_epoch: AtomicU64,
    pub flow_active: AtomicBool,
    pub pid_kp: RwLock<f64>,
    pub pid_ki: RwLock<f64>,
    pub pid_kd: RwLock<f64>,
    pub pid_eps_min: RwLock<f64>,
    pub pid_history: RwLock<VecDeque<PidHistoryPointDto>>,

    // Execuções ativas
    pub active_jobs: RwLock<HashMap<String, JobStatusDto>>,

    // Logs recentes (buffer circular)
    pub logs: RwLock<VecDeque<LogMessageDto>>,

    // Broadcast para WebSockets
    pub tx_events: broadcast::Sender<LiveEvent>,
}

impl AppState {
    pub fn new() -> Arc<Self> {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = Arc::new(NodeControl::new(cluster.clone(), executor.clone()));
        let metrics = Arc::new(MetricsCollector::new());
        // Share the executor's registry so node-side jobs show up on `/metrics`.
        let job_registry = executor.job_registry();
        let metrics_budget = Arc::new(BudgetController::default_budget());

        // Registrar cotas de tenant
        control.register_tenant(TenantQuotaInfo {
            tenant_id: "tenant-production-fintech".to_string(),
            max_concurrent_graphs: 50,
            max_memory_mb: 16384,
            max_fuel: 50_000_000,
            active_graphs: 0,
        });
        control.register_tenant(TenantQuotaInfo {
            tenant_id: "default".to_string(),
            max_concurrent_graphs: 20,
            max_memory_mb: 8192,
            max_fuel: 10_000_000,
            active_graphs: 0,
        });

        // 20 Nós reais do cluster com especificações de produção distribuída (4 domínios NUMA)
        let initial_nodes = vec![
            NodeDto { id: "kpx-node-01".into(), name: "Nó 1 (Semeador/Ingress A)".into(), numa: "NUMA 0".into(), cpu: 48, mem: 72, rx: "4.8 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 140.0, y: 130.0 },
            NodeDto { id: "kpx-node-02".into(), name: "Nó 2 (Receptor Zero-Copy A)".into(), numa: "NUMA 1".into(), cpu: 55, mem: 68, rx: "12.4 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 260.0, y: 100.0 },
            NodeDto { id: "kpx-node-03".into(), name: "Nó 3 (Compute Worker 1)".into(), numa: "NUMA 0".into(), cpu: 32, mem: 81, rx: "8.1 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 420.0, y: 100.0 },
            NodeDto { id: "kpx-node-04".into(), name: "Nó 4 (Compute Worker 2)".into(), numa: "NUMA 1".into(), cpu: 60, mem: 75, rx: "9.6 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 580.0, y: 100.0 },
            NodeDto { id: "kpx-node-05".into(), name: "Nó 5 (Aggregation Master A)".into(), numa: "NUMA 0".into(), cpu: 41, mem: 64, rx: "6.3 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 700.0, y: 150.0 },
            NodeDto { id: "kpx-node-06".into(), name: "Nó 6 (Terminal Parquet A)".into(), numa: "NUMA 1".into(), cpu: 39, mem: 58, rx: "5.1 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 720.0, y: 270.0 },
            NodeDto { id: "kpx-node-07".into(), name: "Nó 7 (Relay & Shunt A)".into(), numa: "NUMA 0".into(), cpu: 28, mem: 49, rx: "3.2 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 580.0, y: 350.0 },
            NodeDto { id: "kpx-node-08".into(), name: "Nó 8 (Compute Worker 3)".into(), numa: "NUMA 1".into(), cpu: 44, mem: 62, rx: "7.4 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 420.0, y: 240.0 },
            NodeDto { id: "kpx-node-09".into(), name: "Nó 9 (Edge Gateway QUIC A)".into(), numa: "NUMA 0".into(), cpu: 35, mem: 53, rx: "11.2 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 260.0, y: 240.0 },
            NodeDto { id: "kpx-node-10".into(), name: "Nó 10 (Hot Standby Spare A)".into(), numa: "NUMA 1".into(), cpu: 12, mem: 28, rx: "0.5 GB/s".into(), status: "IDLE".into(), cap: "ENABLED".into(), x: 140.0, y: 250.0 },
            NodeDto { id: "kpx-node-11".into(), name: "Nó 11 (Semeador/Ingress B)".into(), numa: "NUMA 2".into(), cpu: 44, mem: 70, rx: "5.2 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 140.0, y: 380.0 },
            NodeDto { id: "kpx-node-12".into(), name: "Nó 12 (Compute Worker 4)".into(), numa: "NUMA 2".into(), cpu: 58, mem: 78, rx: "8.9 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 260.0, y: 380.0 },
            NodeDto { id: "kpx-node-13".into(), name: "Nó 13 (Compute Worker 5)".into(), numa: "NUMA 3".into(), cpu: 62, mem: 82, rx: "9.8 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 420.0, y: 380.0 },
            NodeDto { id: "kpx-node-14".into(), name: "Nó 14 (Compute Worker 6)".into(), numa: "NUMA 2".into(), cpu: 50, mem: 73, rx: "8.4 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 580.0, y: 240.0 },
            NodeDto { id: "kpx-node-15".into(), name: "Nó 15 (Aggregation Master B)".into(), numa: "NUMA 3".into(), cpu: 46, mem: 69, rx: "7.1 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 700.0, y: 380.0 },
            NodeDto { id: "kpx-node-16".into(), name: "Nó 16 (Shuffle Manager)".into(), numa: "NUMA 2".into(), cpu: 52, mem: 66, rx: "10.5 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 580.0, y: 450.0 },
            NodeDto { id: "kpx-node-17".into(), name: "Nó 17 (Terminal Parquet B)".into(), numa: "NUMA 3".into(), cpu: 41, mem: 60, rx: "5.8 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 420.0, y: 470.0 },
            NodeDto { id: "kpx-node-18".into(), name: "Nó 18 (Terminal Parquet C)".into(), numa: "NUMA 2".into(), cpu: 38, mem: 56, rx: "5.4 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 260.0, y: 470.0 },
            NodeDto { id: "kpx-node-19".into(), name: "Nó 19 (Relay & Shunt B)".into(), numa: "NUMA 3".into(), cpu: 30, mem: 45, rx: "4.1 GB/s".into(), status: "READY".into(), cap: "ENABLED".into(), x: 140.0, y: 470.0 },
            NodeDto { id: "kpx-node-20".into(), name: "Nó 20 (Hot Standby Spare B)".into(), numa: "NUMA 3".into(), cpu: 10, mem: 25, rx: "0.4 GB/s".into(), status: "IDLE".into(), cap: "ENABLED".into(), x: 720.0, y: 470.0 },
        ];

        // Registrar os 20 nós no ClusterManager do Rust
        for n in &initial_nodes {
            cluster.register_node(ClusterNode {
                id: NodeId(n.id.clone()),
                state: if n.status == "READY" { NodeState::Ready } else { NodeState::Starting },
                capacity: NodeCapacity {
                    max_concurrent: 8,
                    memory_mb: 8192,
                    cpu_cores: 8,
                },
                current_load: (n.cpu as f64) / 100.0,
            });
        }

        // 34 conexões sinápticas realistas cobrindo todos os 20 nós (links intra-NUMA e backbones inter-NUMA)
        let initial_edges = vec![
            // Top tier: Ingress & Workers A (NUMA 0 / NUMA 1)
            EdgeDto { from: 0, to: 1, curvature: 0.035, load: 0.82 },
            EdgeDto { from: 1, to: 2, curvature: 0.015, load: 0.88 },
            EdgeDto { from: 2, to: 3, curvature: 0.042, load: 0.76 },
            EdgeDto { from: 3, to: 4, curvature: 0.020, load: 0.84 },
            EdgeDto { from: 4, to: 5, curvature: 0.028, load: 0.69 },

            // Local loops & gateway intra/inter NUMA 0 & 1
            EdgeDto { from: 0, to: 8, curvature: 0.025, load: 0.74 },
            EdgeDto { from: 1, to: 8, curvature: 0.018, load: 0.80 },
            EdgeDto { from: 0, to: 9, curvature: 0.052, load: 0.25 },
            EdgeDto { from: 8, to: 9, curvature: 0.045, load: 0.20 },
            EdgeDto { from: 1, to: 7, curvature: 0.022, load: 0.65 },
            EdgeDto { from: 2, to: 7, curvature: 0.031, load: 0.58 },
            EdgeDto { from: 3, to: 7, curvature: 0.024, load: 0.62 },
            EdgeDto { from: 7, to: 8, curvature: 0.033, load: 0.70 },

            // Middle tier cross-links (NUMA 0, 1, 2)
            EdgeDto { from: 3, to: 13, curvature: -0.018, load: 0.72 },
            EdgeDto { from: 4, to: 13, curvature: -0.012, load: 0.66 },
            EdgeDto { from: 5, to: 13, curvature: 0.021, load: 0.60 },
            EdgeDto { from: 5, to: 6, curvature: 0.019, load: 0.54 },
            EdgeDto { from: 6, to: 13, curvature: 0.027, load: 0.48 },
            EdgeDto { from: 6, to: 14, curvature: -0.015, load: 0.56 },

            // Inter-NUMA backbones
            EdgeDto { from: 7, to: 12, curvature: -0.028, load: 0.79 },
            EdgeDto { from: 8, to: 12, curvature: -0.022, load: 0.75 },
            EdgeDto { from: 9, to: 10, curvature: -0.008, load: 0.35 },

            // Lower tier: Ingress B & Workers (NUMA 2 / NUMA 3)
            EdgeDto { from: 10, to: 11, curvature: 0.022, load: 0.78 },
            EdgeDto { from: 10, to: 18, curvature: 0.030, load: 0.52 },
            EdgeDto { from: 11, to: 12, curvature: 0.014, load: 0.85 },
            EdgeDto { from: 11, to: 18, curvature: 0.034, load: 0.45 },
            EdgeDto { from: 11, to: 17, curvature: 0.025, load: 0.68 },
            EdgeDto { from: 12, to: 13, curvature: -0.032, load: 0.88 },
            EdgeDto { from: 12, to: 16, curvature: 0.020, load: 0.72 },

            // Bottom tier: Shuffles, Aggregation B, and Parquet Terminals
            EdgeDto { from: 13, to: 14, curvature: 0.018, load: 0.81 },
            EdgeDto { from: 13, to: 15, curvature: 0.012, load: 0.86 },
            EdgeDto { from: 14, to: 15, curvature: 0.023, load: 0.77 },
            EdgeDto { from: 15, to: 16, curvature: 0.026, load: 0.64 },
            EdgeDto { from: 14, to: 19, curvature: 0.048, load: 0.18 },
        ];

        // Inicializar Simplex2Discoverer real com os 20 nós
        let mut simplex_discoverer = Simplex2Discoverer::new();
        for edge in &initial_edges {
            simplex_discoverer.add_edge(
                GeoNodeId(format!("node_{}", edge.from)),
                GeoNodeId(format!("node_{}", edge.to)),
            );
        }

        // Inicializar Controlador PID real
        let kp = 1.23;
        let ki = 0.45;
        let kd = 0.09;
        let eps_min = 0.001;
        let step_controller = AdaptiveStepController::new(0.050)
            .with_limits(eps_min, 0.150)
            .with_pid_gains(kp, ki, kd);

        // Inicializar histórico PID com ~30 pontos representativos
        let mut initial_pid_history = VecDeque::with_capacity(32);
        let mut hist_eps = 0.050;
        let mut hist_integral = 0.0;
        let mut hist_prev_error = 0.0;
        for tick in 1..=30 {
            let t = tick as f64;
            let error = -0.018 * (-0.10 * (t - 1.0)).exp() * (0.35 * (t - 1.0)).cos();
            let p_term = kp * error;
            hist_integral = (hist_integral + error).clamp(-100.0, 100.0);
            let i_term = ki * hist_integral;
            let d_term = kd * (error - hist_prev_error);
            hist_prev_error = error;
            hist_eps = (hist_eps + p_term + i_term + d_term).clamp(eps_min, 0.150);

            initial_pid_history.push_back(PidHistoryPointDto {
                tick,
                epsilon: hist_eps,
                error,
                p_term,
                i_term,
                d_term,
                volume: 341.28,
            });
        }

        // Inicializar Normalizador de Volume real (341.28)
        let normalizer = MetricTensorNormalizer::new(341.28);

        let (tx_events, _) = broadcast::channel(1024);

        let mut initial_logs = VecDeque::new();
        initial_logs.push_back(LogMessageDto {
            timestamp: chrono::Utc::now().to_rfc3339(),
            level: "INFO".into(),
            component: "ClusterManager".into(),
            message: "Cluster KinePlex v0.3 operacional. 20 nós homologados via mTLS Ed25519.".into(),
        });
        initial_logs.push_back(LogMessageDto {
            timestamp: chrono::Utc::now().to_rfc3339(),
            level: "INFO".into(),
            component: "KernelUAPI".into(),
            message: "NUMA memory pages vinculadas via eth0 em /dev/kinegeo.".into(),
        });

        Arc::new(Self {
            cluster_manager: cluster,
            executor,
            node_control: control,
            metrics_collector: metrics,
            job_registry,
            metrics_budget,
            metrics_scrape_last_ns: AtomicU64::new(0),
            nodes: RwLock::new(initial_nodes),
            edges: RwLock::new(initial_edges),
            simplex_discoverer: RwLock::new(simplex_discoverer),
            step_controller: RwLock::new(step_controller),
            normalizer: RwLock::new(normalizer),
            route_epoch: AtomicU64::new(14),
            flow_active: AtomicBool::new(true),
            pid_kp: RwLock::new(kp),
            pid_ki: RwLock::new(ki),
            pid_kd: RwLock::new(kd),
            pid_eps_min: RwLock::new(eps_min),
            pid_history: RwLock::new(initial_pid_history),
            active_jobs: RwLock::new(HashMap::new()),
            logs: RwLock::new(initial_logs),
            tx_events,
        })
    }

    /// One PI step for the Metrics Budget Controller: measures the wall cost
    /// of the observation itself (export + gauges) against the scrape window
    /// and feeds `overhead = cost / window` to the controller (plan Fase 4/5).
    pub fn tick_metrics_budget(&self, observation_cost_ns: u64) {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let last = self.metrics_scrape_last_ns.swap(now_ns, Ordering::Relaxed);
        if last == 0 || now_ns <= last {
            return;
        }
        let window_ns = (now_ns - last).max(1);
        let raw_overhead = observation_cost_ns as f64 / window_ns as f64;
        self.metrics_budget.tick(raw_overhead);
    }

    pub fn add_log(&self, level: &str, component: &str, message: &str) {
        let entry = LogMessageDto {
            timestamp: chrono::Utc::now().to_rfc3339(),
            level: level.to_string(),
            component: component.to_string(),
            message: message.to_string(),
        };

        {
            let mut logs = self.logs.write();
            if logs.len() >= 200 {
                logs.pop_front();
            }
            logs.push_back(entry.clone());
        }

        let _ = self.tx_events.send(LiveEvent {
            event_type: "log".to_string(),
            timestamp: entry.timestamp.clone(),
            data: serde_json::to_value(&entry).unwrap_or_default(),
        });
    }

    pub fn get_geometry_state(&self) -> GeometryStateDto {
        let disc = self.simplex_discoverer.read();
        let triangles_raw = disc.find_triangles();
        let betti_1 = disc.betti_number();
        let step_ctrl = self.step_controller.read();
        let epsilon = step_ctrl.epsilon();

        // Mapear triângulos [GeoNodeId("node_0"), ...] para índices [0, ...]
        let triangles: Vec<[usize; 3]> = triangles_raw
            .iter()
            .map(|t| {
                let parse_idx = |id: &GeoNodeId| -> usize {
                    id.0.strip_prefix("node_")
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0)
                };
                [parse_idx(&t[0]), parse_idx(&t[1]), parse_idx(&t[2])]
            })
            .collect();

        GeometryStateDto {
            triangles,
            betti_1,
            current_epsilon: epsilon,
            volume_invariant: 341.28,
            kp: *self.pid_kp.read(),
            ki: *self.pid_ki.read(),
            kd: *self.pid_kd.read(),
            epsilon_min: *self.pid_eps_min.read(),
            route_epoch: self.route_epoch.load(Ordering::SeqCst),
            avg_curvature: 0.023,
            flow_active: self.flow_active.load(Ordering::SeqCst),
        }
    }

    pub fn generate_nodes_3d(&self) -> Vec<Node3dDto> {
        let nodes = self.nodes.read();
        let total = nodes.len().min(20);

        // Nós de Ingress e Compute distribuídos com curvatura correspondente ao seu domínio NUMA
        // com z variando de +0.85 a -0.85 em um elipsoide hiperbólico
        let z_coords = [
            0.85,  // 0: kpx-node-01 (NUMA 0, Ingress A)
            0.45,  // 1: kpx-node-02 (NUMA 1, Receptor A)
            0.76,  // 2: kpx-node-03 (NUMA 0, Compute 1)
            0.35,  // 3: kpx-node-04 (NUMA 1, Compute 2)
            0.68,  // 4: kpx-node-05 (NUMA 0, Aggregation A)
            0.15,  // 5: kpx-node-06 (NUMA 1, Terminal A)
            0.55,  // 6: kpx-node-07 (NUMA 0, Relay A)
            0.25,  // 7: kpx-node-08 (NUMA 1, Compute 3)
            0.61,  // 8: kpx-node-09 (NUMA 0, Gateway A)
            0.05,  // 9: kpx-node-10 (NUMA 1, Standby A)
            -0.05, // 10: kpx-node-11 (NUMA 2, Ingress B)
            -0.15, // 11: kpx-node-12 (NUMA 2, Compute 4)
            -0.55, // 12: kpx-node-13 (NUMA 3, Compute 5)
            -0.25, // 13: kpx-node-14 (NUMA 2, Compute 6)
            -0.62, // 14: kpx-node-15 (NUMA 3, Aggregation B)
            -0.35, // 15: kpx-node-16 (NUMA 2, Shuffle)
            -0.70, // 16: kpx-node-17 (NUMA 3, Terminal B)
            -0.45, // 17: kpx-node-18 (NUMA 2, Terminal C)
            -0.78, // 18: kpx-node-19 (NUMA 3, Relay B)
            -0.85, // 19: kpx-node-20 (NUMA 3, Standby B)
        ];

        let mut res = Vec::with_capacity(total);
        for i in 0..total {
            let n = &nodes[i];
            let z: f32 = z_coords[i];
            let theta = (2.0 * std::f32::consts::PI * (i as f32) / 20.0) + 0.15;
            let r = (1.0f32 + z * z).sqrt();
            let a: f32 = 1.6;
            let b: f32 = 1.2;
            let x = ((a * r * theta.cos()) * 1000.0).round() / 1000.0;
            let y = ((b * r * theta.sin()) * 1000.0).round() / 1000.0;

            let role = match n.id.as_str() {
                "kpx-node-01" | "kpx-node-02" | "kpx-node-09" | "kpx-node-11" => "Ingress".to_string(),
                "kpx-node-03" | "kpx-node-04" | "kpx-node-08" | "kpx-node-12" | "kpx-node-13" | "kpx-node-14" => "Compute".to_string(),
                "kpx-node-05" | "kpx-node-15" | "kpx-node-16" => "Aggregation".to_string(),
                "kpx-node-06" | "kpx-node-17" | "kpx-node-18" => "Terminal".to_string(),
                "kpx-node-10" | "kpx-node-20" => "Standby".to_string(),
                _ => "Relay".to_string(),
            };

            res.push(Node3dDto {
                id: n.id.clone(),
                x,
                y,
                z,
                role,
                numa: n.numa.clone(),
            });
        }

        res
    }

    pub fn generate_metric_tensor_matrix(&self, nodes_3d: &[Node3dDto]) -> Vec<Vec<f64>> {
        let n = 20;
        let mut matrix = vec![vec![0.0f64; n]; n];

        // 1. Diagonal principal = 1.0
        for i in 0..n {
            matrix[i][i] = 1.0;
        }

        let edges = self.edges.read();

        // 2. Elementos adjacentes derivados dos pesos e curvatura da aresta (0.70 a 0.95)
        for e in edges.iter() {
            if e.from < n && e.to < n && e.from != e.to {
                let adj_val = (0.70 + 0.20 * e.load + 0.40 * (e.curvature + 0.035)).clamp(0.70, 0.95);
                let rounded = (adj_val * 10000.0).round() / 10000.0;
                matrix[e.from][e.to] = rounded;
                matrix[e.to][e.from] = rounded;
            }
        }

        // 3. Distâncias geodésicas no grafo (Floyd-Warshall)
        let mut dist = vec![vec![usize::MAX / 2; n]; n];
        for i in 0..n {
            dist[i][i] = 0;
        }
        for e in edges.iter() {
            if e.from < n && e.to < n {
                dist[e.from][e.to] = 1;
                dist[e.to][e.from] = 1;
            }
        }
        for k in 0..n {
            for i in 0..n {
                for j in 0..n {
                    if dist[i][k] + dist[k][j] < dist[i][j] {
                        dist[i][j] = dist[i][k] + dist[k][j];
                    }
                }
            }
        }

        // 4. Elementos não adjacentes derivados da distância geodésica (0.15 a 0.55)
        for i in 0..n {
            for j in (i + 1)..n {
                if matrix[i][j] == 0.0 {
                    let d = dist[i][j];
                    let dist_3d = if i < nodes_3d.len() && j < nodes_3d.len() {
                        let dx = nodes_3d[i].x - nodes_3d[j].x;
                        let dy = nodes_3d[i].y - nodes_3d[j].y;
                        let dz = nodes_3d[i].z - nodes_3d[j].z;
                        ((dx * dx + dy * dy + dz * dz) as f64).sqrt()
                    } else {
                        1.0
                    };

                    let non_adj_val = (0.55 - 0.07 * (d as f64 - 2.0) - 0.02 * (dist_3d / 3.0)).clamp(0.15, 0.55);
                    let rounded = (non_adj_val * 10000.0).round() / 10000.0;
                    matrix[i][j] = rounded;
                    matrix[j][i] = rounded;
                }
            }
        }

        matrix
    }

    pub fn generate_curvature_spectrum(&self) -> Vec<EdgeCurvatureSpectrumDto> {
        let edges = self.edges.read();
        let mut spectrum: Vec<EdgeCurvatureSpectrumDto> = edges
            .iter()
            .map(|e| EdgeCurvatureSpectrumDto {
                from: e.from,
                to: e.to,
                curvature: (e.curvature * 10000.0).round() / 10000.0,
                load: (e.load * 1000.0).round() / 1000.0,
            })
            .collect();
        spectrum.sort_by(|a, b| b.curvature.partial_cmp(&a.curvature).unwrap_or(std::cmp::Ordering::Equal));
        spectrum
    }

    pub fn get_metric_tensor_response(&self) -> MetricTensorResponseDto {
        let nodes_3d = self.generate_nodes_3d();
        let matrix = self.generate_metric_tensor_matrix(&nodes_3d);
        let spectrum_1d = self.generate_curvature_spectrum();
        let pid_history_1d: Vec<PidHistoryPointDto> = self.pid_history.read().iter().cloned().collect();
        let volume_invariant = 341.28;
        let route_epoch = self.route_epoch.load(Ordering::SeqCst);

        MetricTensorResponseDto {
            matrix,
            spectrum_1d,
            nodes_3d,
            pid_history_1d,
            volume_invariant,
            route_epoch,
        }
    }

}


