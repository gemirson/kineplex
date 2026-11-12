//! KinePlex API & WebSocket Production Server

mod handlers;
mod state;
mod telemetry;

use std::net::SocketAddr;
use std::path::PathBuf;

use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;

use crate::state::AppState;
use crate::telemetry::start_telemetry_worker;

#[derive(Parser, Debug)]
#[command(name = "kineplex-server")]
#[command(about = "KinePlex Cluster Manager & API Server for Production UI", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "8080")]
    port: u16,

    #[arg(short = 'H', long, default_value = "0.0.0.0")]
    host: String,

    #[arg(long, default_value = "kineplex-ui")]
    ui_dir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();
    let state = AppState::new();

    // Iniciar loop de telemetria em segundo plano
    start_telemetry_worker(state.clone());

    // Configurar CORS permissivo para dev e produção distribuída
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Identificar diretório da UI
    let ui_path = if args.ui_dir.exists() {
        args.ui_dir.clone()
    } else if PathBuf::from("../kineplex-ui").exists() {
        PathBuf::from("../kineplex-ui")
    } else {
        PathBuf::from("kineplex-ui")
    };

    let app = Router::new()
        // Health & Prometheus
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/metrics", get(handlers::get_metrics))
        // Cluster & Topologia
        .route("/api/v1/cluster/nodes", get(handlers::get_nodes))
        .route("/api/v1/cluster/chaos/failover", post(handlers::toggle_chaos))
        .route("/api/v1/cluster/simulate", post(handlers::simulate_cluster))
        // Controle Geométrico (Ricci, PID, Homologia)
        .route("/api/v1/geometry/state", get(handlers::get_geometry_state))
        .route("/api/v1/geometry/tensor", get(handlers::get_geometry_tensor))
        .route("/api/v1/geometry/pid/tune", post(handlers::tune_pid))
        .route("/api/v1/geometry/normalize", post(handlers::normalize_volume))
        .route("/api/v1/geometry/ricci/step", post(handlers::step_ricci_flow))
        .route("/api/v1/geometry/ricci/toggle", post(handlers::toggle_ricci_flow))
        .route("/api/v1/geometry/perturb", post(handlers::perturb_metric))
        // DAGs & Pipeline Execution
        .route("/api/v1/graphs", post(handlers::submit_graph).get(handlers::list_graphs))
        .route("/api/v1/graphs/:id", get(handlers::get_graph_status))
        .route("/api/v1/logs", get(handlers::get_logs))
        // WebSocket em tempo real
        .route("/ws/live", get(handlers::ws_handler))
        .with_state(state.clone())
        .layer(cors)
        // Arquivos estáticos da UI
        .fallback_service(ServeDir::new(&ui_path));

    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    tracing::info!("=======================================================");
    tracing::info!("  🚀 KINEPLEX PRODUCTION API & WEBSOCKET DAEMON ATIVO");
    tracing::info!("  🌐 Endereço: http://{}", addr);
    tracing::info!("  🎨 UI Estática montada de: {:?}", ui_path);
    tracing::info!("  📡 WebSocket de Telemetria: ws://{}/ws/live", addr);
    tracing::info!("=======================================================");

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::JobSubmissionPayload;
    use std::time::Duration;

    #[tokio::test]
    async fn test_state_initialization() {
        let state = AppState::new();
        assert_eq!(state.nodes.read().len(), 20);
        assert!(state.edges.read().len() >= 30);
        assert_eq!(state.edges.read().len(), 34);

        let geo = state.get_geometry_state();
        assert_eq!(geo.volume_invariant, 341.28);
        assert_eq!(geo.route_epoch, 14);
        assert!(geo.betti_1 > 0);
    }

    #[tokio::test]
    async fn test_chaos_failover_toggle() {
        let state = AppState::new();
        let payload = Some(handlers::ChaosRequest {
            target_node: Some("kpx-node-02".to_string()),
        });

        // Trigger failover
        let _ = handlers::toggle_chaos(axum::extract::State(state.clone()), axum::Json(payload.clone())).await;
        {
            let nodes = state.nodes.read();
            let n2 = nodes.iter().find(|n| n.id == "kpx-node-02").unwrap();
            assert_eq!(n2.status, "FAILED");
        }
        assert_eq!(state.route_epoch.load(std::sync::atomic::Ordering::SeqCst), 15);

        // Rejoin
        let _ = handlers::toggle_chaos(axum::extract::State(state.clone()), axum::Json(payload)).await;
        {
            let nodes = state.nodes.read();
            let n2 = nodes.iter().find(|n| n.id == "kpx-node-02").unwrap();
            assert_eq!(n2.status, "READY");
        }
        assert_eq!(state.route_epoch.load(std::sync::atomic::Ordering::SeqCst), 16);
    }

    #[tokio::test]
    async fn test_pid_tuning_and_normalization() {
        let state = AppState::new();

        let req = handlers::TunePidRequest {
            kp: 2.10,
            ki: 0.60,
            kd: 0.15,
            epsilon_min: 0.002,
        };
        let _ = handlers::tune_pid(axum::extract::State(state.clone()), axum::Json(req)).await;
        assert_eq!(*state.pid_kp.read(), 2.10);
        assert_eq!(*state.pid_ki.read(), 0.60);

        let _ = handlers::normalize_volume(axum::extract::State(state.clone())).await;
        let geo = state.get_geometry_state();
        assert_eq!(geo.volume_invariant, 341.28);
    }

    #[tokio::test]
    async fn test_graph_submit_and_pipeline_execution() {
        let state = AppState::new();
        let payload = JobSubmissionPayload {
            tenant_id: "tenant-production-fintech".to_string(),
            idempotency_key: Some("test-idemp-1".to_string()),
            max_memory_mb: Some(1024),
            max_fuel: Some(1_000_000),
            wasm_module: Some("filter_sensor.wasm".to_string()),
            enable_simd: Some(true),
        };

        let _response = handlers::submit_graph(axum::extract::State(state.clone()), axum::Json(payload)).await;
        // Verify job is recorded in active jobs
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!state.active_jobs.read().is_empty());
    }

    #[tokio::test]
    async fn test_simulation_20_nodes() {
        let state = AppState::new();
        let payload = handlers::SimulationRequest {
            dataset_size: Some("test".into()),
            num_rows: Some(20_000),
            enable_parquet: Some(true),
        };

        let (status, axum::Json(response)) = handlers::simulate_cluster(axum::extract::State(state.clone()), axum::Json(Some(payload))).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(response.nodes_count, 20);
        assert_eq!(response.allocated_nodes.len(), 20);
        assert_eq!(response.stages.len(), 5);
        assert!(response.throughput_rows_per_sec > 0.0);
        assert_eq!(response.parquet_partitions.len(), 4);
        for p in &response.parquet_partitions {
            assert!(std::path::Path::new(p).exists(), "Parquet partition {} must exist", p);
        }
    }

    #[tokio::test]
    async fn test_chaos_compute_sibling_failover() {
        let state = AppState::new();
        let payload = Some(handlers::ChaosRequest {
            target_node: Some("kpx-node-03".to_string()),
        });

        // Failover on compute node kpx-node-03
        let _ = handlers::toggle_chaos(axum::extract::State(state.clone()), axum::Json(payload.clone())).await;
        {
            let nodes = state.nodes.read();
            let n3 = nodes.iter().find(|n| n.id == "kpx-node-03").unwrap();
            assert_eq!(n3.status, "FAILED");
            assert_eq!(n3.cpu, 0);
        }

        // Rejoin
        let _ = handlers::toggle_chaos(axum::extract::State(state.clone()), axum::Json(payload)).await;
        {
            let nodes = state.nodes.read();
            let n3 = nodes.iter().find(|n| n.id == "kpx-node-03").unwrap();
            assert_eq!(n3.status, "READY");
        }
    }

    #[tokio::test]
    async fn test_chaos_aggregation_sibling_failover() {
        let state = AppState::new();
        let payload = Some(handlers::ChaosRequest {
            target_node: Some("kpx-node-05".to_string()),
        });

        // Failover on aggregation node kpx-node-05
        let _ = handlers::toggle_chaos(axum::extract::State(state.clone()), axum::Json(payload.clone())).await;
        {
            let nodes = state.nodes.read();
            let n5 = nodes.iter().find(|n| n.id == "kpx-node-05").unwrap();
            assert_eq!(n5.status, "FAILED");
            assert_eq!(n5.cpu, 0);
        }

        // Rejoin
        let _ = handlers::toggle_chaos(axum::extract::State(state.clone()), axum::Json(payload)).await;
        {
            let nodes = state.nodes.read();
            let n5 = nodes.iter().find(|n| n.id == "kpx-node-05").unwrap();
            assert_eq!(n5.status, "READY");
        }
    }

    #[tokio::test]
    async fn test_http_endpoints_integration() {
        use axum::response::IntoResponse;
        let state = AppState::new();

        let health_res = handlers::healthz().await.into_response();
        assert_eq!(health_res.status(), axum::http::StatusCode::OK);

        let ready_res = handlers::readyz(axum::extract::State(state.clone())).await.into_response();
        assert_eq!(ready_res.status(), axum::http::StatusCode::OK);

        let nodes_res = handlers::get_nodes(axum::extract::State(state.clone())).await.into_response();
        assert_eq!(nodes_res.status(), axum::http::StatusCode::OK);

        let geo_res = handlers::get_geometry_state(axum::extract::State(state.clone())).await.into_response();
        assert_eq!(geo_res.status(), axum::http::StatusCode::OK);

        let metrics_res = handlers::get_metrics(axum::extract::State(state.clone())).await.into_response();
        assert_eq!(metrics_res.status(), axum::http::StatusCode::OK);

        let tensor_res = handlers::get_geometry_tensor(axum::extract::State(state.clone())).await.into_response();
        assert_eq!(tensor_res.status(), axum::http::StatusCode::OK);

        let logs_res = handlers::get_logs(axum::extract::State(state.clone())).await.into_response();
        assert_eq!(logs_res.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_geometry_tensor_endpoint() {
        use axum::response::IntoResponse;
        let state = AppState::new();

        // 1. Validar resposta HTTP 200 OK do handler
        let res = handlers::get_geometry_tensor(axum::extract::State(state.clone())).await.into_response();
        assert_eq!(res.status(), axum::http::StatusCode::OK);

        let dto = state.get_metric_tensor_response();

        // 2. Validar Matriz 20x20 e propriedades métricas ($g_{ij}$)
        assert_eq!(dto.matrix.len(), 20, "Matriz deve ter 20 linhas");
        for (i, row) in dto.matrix.iter().enumerate() {
            assert_eq!(row.len(), 20, "Linha {} deve ter 20 colunas", i);
            for (j, &val) in row.iter().enumerate() {
                if i == j {
                    // Diagonal principal = 1.0
                    assert!((val - 1.0).abs() < 1e-6, "Diagonal [{}][{}] deve ser 1.0, obteve {}", i, j, val);
                } else {
                    // Simetria g_ij = g_ji
                    assert!((val - dto.matrix[j][i]).abs() < 1e-6, "Simetria violada em ({}, {}): {} vs {}", i, j, val, dto.matrix[j][i]);
                    // Elementos devem estar entre 0.15 e 0.95
                    assert!(val >= 0.15 && val <= 0.95, "Elemento ({}, {}) fora dos limites: {}", i, j, val);
                }
            }
        }

        // 3. Validar 20 nós 3D no elipsoide hiperbólico
        assert_eq!(dto.nodes_3d.len(), 20, "Devem existir 20 nós 3D");
        let max_z = dto.nodes_3d.iter().map(|n| n.z).fold(f32::NEG_INFINITY, f32::max);
        let min_z = dto.nodes_3d.iter().map(|n| n.z).fold(f32::INFINITY, f32::min);
        assert!((max_z - 0.85).abs() < 0.01, "max_z deve ser +0.85, obteve {}", max_z);
        assert!((min_z - (-0.85)).abs() < 0.01, "min_z deve ser -0.85, obteve {}", min_z);

        // Validar distribuição de papéis (Ingress, Compute, etc.) e NUMA
        for node in &dto.nodes_3d {
            assert!(!node.id.is_empty());
            assert!(!node.role.is_empty());
            assert!(!node.numa.is_empty());
        }

        // 4. Validar espectro 1D das 34 arestas ordenadas por curvatura
        assert_eq!(dto.spectrum_1d.len(), 34, "Espectro deve ter 34 arestas");
        let first_curv = dto.spectrum_1d.first().unwrap().curvature;
        let last_curv = dto.spectrum_1d.last().unwrap().curvature;
        assert!(first_curv > 0.0, "Maior curvatura deve ser positiva (+0.052), obteve {}", first_curv);
        assert!(last_curv < 0.0, "Menor curvatura deve ser negativa (-0.032), obteve {}", last_curv);
        for i in 1..dto.spectrum_1d.len() {
            assert!(
                dto.spectrum_1d[i - 1].curvature >= dto.spectrum_1d[i].curvature,
                "Espectro não ordenado decrescentemente no índice {}: {} < {}",
                i, dto.spectrum_1d[i - 1].curvature, dto.spectrum_1d[i].curvature
            );
        }

        // 5. Validar buffer de histórico PID (~30 pontos históricos)
        assert_eq!(dto.pid_history_1d.len(), 30, "Histórico PID deve iniciar com 30 pontos");
        assert_eq!(dto.volume_invariant, 341.28, "Volume invariante deve ser 341.28");
        assert_eq!(dto.route_epoch, 14, "Route epoch inicial deve ser 14");

        // 6. Validar que tune_pid adiciona novo ponto ao histórico PID
        let tune_req = handlers::TunePidRequest {
            kp: 1.50,
            ki: 0.50,
            kd: 0.10,
            epsilon_min: 0.001,
        };
        let _ = handlers::tune_pid(axum::extract::State(state.clone()), axum::Json(tune_req)).await;
        let updated_dto = state.get_metric_tensor_response();
        assert_eq!(updated_dto.pid_history_1d.len(), 31, "Histórico deve conter 31 pontos após tune_pid");
        assert_eq!(updated_dto.pid_history_1d.last().unwrap().tick, 31);
    }
}

