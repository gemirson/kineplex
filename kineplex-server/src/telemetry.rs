//! Background telemetry and hardware probe loop

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::state::{AppState, LiveEvent};

/// Executa um tick do ciclo de telemetria e calibração de estado.
/// Retorna o LiveEvent gerado com métricas consolidadas dos 20 nós do cluster.
pub fn run_telemetry_tick(state: &AppState, tick: u64) -> LiveEvent {
    // 1. Verificar se Ricci Flow está ativo para aplicar adaptação PID
    if state.flow_active.load(Ordering::SeqCst) {
        let mut controller = state.step_controller.write();
        // Simulação de perturbação residual de curvatura tendendo a zero
        let delta = (tick as f64 * 0.1).sin() * 0.002;
        let _new_eps = controller.compute(delta);
    }

    // 2. Telemetria dos nós (oscilação dinâmica e realista de CPU e memória para todos os nós)
    let (healthy_count, failed_count, total_nodes) = {
        let mut nodes = state.nodes.write();
        let mut healthy: usize = 0;
        let mut failed: usize = 0;

        for (idx, node) in nodes.iter_mut().enumerate() {
            let node_idx = node
                .id
                .strip_prefix("kpx-node-")
                .and_then(|s| s.parse::<usize>().ok())
                .map(|n| n.saturating_sub(1))
                .unwrap_or(idx);

            if node.status == "FAILED" {
                node.cpu = 0;
                node.rx = "0.0 KB/s".to_string();
                failed += 1;
                continue;
            }

            healthy += 1;

            // Bases realistas de CPU e memória conforme a função do nó no cluster:
            // - Ingress (nós 0, 1, 8, 10): 45-55%
            // - Compute Wasm SIMD (nós 2, 3, 7, 11, 12, 13): 60-80%
            // - Aggregation & Shuffle (nós 4, 14, 15): 40-65%
            // - Terminal Parquet (nós 5, 16, 17): 35-50%
            // - Relay & Standby (nós 6, 9, 18, 19): 10-30%
            let (base_cpu, cpu_min, cpu_max, base_mem, mem_min, mem_max) = match node_idx {
                // Ingress (nós 0, 1, 8, 10): 45-55%
                0 => (49, 45, 55, 72, 65, 80),
                1 => (53, 45, 55, 68, 60, 75),
                8 => (48, 45, 55, 66, 58, 74),
                10 => (51, 45, 55, 64, 56, 72),

                // Compute Wasm SIMD (nós 2, 3, 7, 11, 12, 13): 60-80%
                2 => (68, 60, 80, 81, 72, 88),
                3 => (74, 60, 80, 76, 68, 84),
                7 => (65, 60, 80, 70, 62, 78),
                11 => (72, 60, 80, 78, 70, 86),
                12 => (77, 60, 80, 82, 74, 90),
                13 => (69, 60, 80, 74, 66, 82),

                // Aggregation & Shuffle (nós 4, 14, 15): 40-65%
                4 => (52, 40, 65, 64, 55, 75),
                14 => (58, 40, 65, 67, 58, 76),
                15 => (46, 40, 65, 62, 54, 72),

                // Terminal Parquet (nós 5, 16, 17): 35-50%
                5 => (42, 35, 50, 58, 50, 68),
                16 => (39, 35, 50, 54, 46, 64),
                17 => (47, 35, 50, 62, 52, 70),

                // Relay & Standby (nós 6, 9, 18, 19): 10-30%
                6 => (22, 10, 30, 48, 38, 56),
                9 => (16, 10, 30, 28, 20, 36),
                18 => (24, 10, 30, 32, 22, 40),
                19 => (14, 10, 30, 24, 16, 32),

                // Fallback dinâmico para nós adicionais
                _ => (25, 10, 50, 40, 20, 60),
            };

            // Oscilações determinísticas suaves
            let cpu_variation = ((tick + node_idx as u64) % 7) as i32 - 3;
            let mem_variation = ((tick.wrapping_mul(3) + node_idx as u64) % 5) as i32 - 2;

            node.cpu = ((base_cpu as i32) + cpu_variation).clamp(cpu_min, cpu_max) as u32;
            node.mem = ((base_mem as i32) + mem_variation).clamp(mem_min, mem_max) as u32;
        }

        (healthy, failed, nodes.len())
    };

    // 3. Montar payload consolidado de telemetria
    let geo_state = state.get_geometry_state();
    let nodes_snapshot = state.nodes.read().clone();

    // Métricas agregadas proporcionais aos 20 nós do cluster e nós saudáveis
    let health_ratio = if total_nodes > 0 {
        healthy_count as f64 / total_nodes as f64
    } else {
        1.0
    };

    // Bandwidth de rede Arrow agregado proporcional aos 20 nós (~85.4 GB/s)
    let bw_fluctuation = ((tick % 7) as f64 - 3.0) * 0.12;
    let arrow_bandwidth_gb = (((85.4 + bw_fluctuation) * health_ratio) * 10.0).round() / 10.0;

    // Spike rate atualizado (~2.40 M/s)
    let spike_fluctuation = ((tick % 9) as f64 - 4.0) * 0.01;
    let spike_rate_val = 2.40 + spike_fluctuation;
    let spike_rate_str = format!("{:.2} M/s", spike_rate_val);

    // P95 Wasm Latency (~1.12 ms)
    let wasm_latency_p95_ms = ((1.12 + ((tick % 5) as f64 - 2.0) * 0.01) * 100.0).round() / 100.0;
    let wasm_latency_p95_str = format!("{:.2} ms", wasm_latency_p95_ms);

    let telemetry_data = serde_json::json!({
        "spike_rate": spike_rate_str,
        "spike_rate_val": ((spike_rate_val * 100.0).round()) / 100.0,
        "arrow_bandwidth_gb": arrow_bandwidth_gb,
        "arrow_bandwidth": format!("{:.1} GB/s", arrow_bandwidth_gb),
        "wasm_latency_p95_ms": wasm_latency_p95_ms,
        "wasm_latency_p95": wasm_latency_p95_str,
        "healthy_nodes": healthy_count,
        "failed_nodes": failed_count,
        "healthy_count": healthy_count,
        "failed_count": failed_count,
        "total_nodes": total_nodes,
        "dropped_cqes": 0,
        "numa_misses_pct": 0.0,
        "cap_net_admin": true,
        "nodes": nodes_snapshot,
        "geometry": geo_state,
        "tick": tick
    });

    LiveEvent {
        event_type: "telemetry".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        data: telemetry_data,
    }
}

pub fn start_telemetry_worker(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(1000));
        let mut tick: u64 = 0;

        loop {
            interval.tick().await;
            tick += 1;

            let event = run_telemetry_tick(&state, tick);

            // 4. Enviar para todos os WebSockets conectados via broadcast
            let _ = state.tx_events.send(event);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_20_nodes_cpu_ranges() {
        let state = AppState::new();
        assert_eq!(state.nodes.read().len(), 20);

        // Testar múltiplos ticks para garantir que oscilações ficam estritamente nos intervalos
        for tick in 1..=30 {
            let event = run_telemetry_tick(&state, tick);
            assert_eq!(event.event_type, "telemetry");

            let nodes = state.nodes.read();
            for (idx, node) in nodes.iter().enumerate() {
                let node_idx = node
                    .id
                    .strip_prefix("kpx-node-")
                    .and_then(|s| s.parse::<usize>().ok())
                    .map(|n| n.saturating_sub(1))
                    .unwrap_or(idx);

                if node.status == "FAILED" {
                    assert_eq!(node.cpu, 0);
                    continue;
                }

                match node_idx {
                    // Ingress (nós 0, 1, 8, 10): 45-55%
                    0 | 1 | 8 | 10 => {
                        assert!(
                            node.cpu >= 45 && node.cpu <= 55,
                            "Nó {} (Ingress) fora do range 45-55%: {}",
                            node.id,
                            node.cpu
                        );
                    }
                    // Compute Wasm SIMD (nós 2, 3, 7, 11, 12, 13): 60-80%
                    2 | 3 | 7 | 11 | 12 | 13 => {
                        assert!(
                            node.cpu >= 60 && node.cpu <= 80,
                            "Nó {} (Compute) fora do range 60-80%: {}",
                            node.id,
                            node.cpu
                        );
                    }
                    // Aggregation & Shuffle (nós 4, 14, 15): 40-65%
                    4 | 14 | 15 => {
                        assert!(
                            node.cpu >= 40 && node.cpu <= 65,
                            "Nó {} (Aggregation) fora do range 40-65%: {}",
                            node.id,
                            node.cpu
                        );
                    }
                    // Terminal Parquet (nós 5, 16, 17): 35-50%
                    5 | 16 | 17 => {
                        assert!(
                            node.cpu >= 35 && node.cpu <= 50,
                            "Nó {} (Terminal) fora do range 35-50%: {}",
                            node.id,
                            node.cpu
                        );
                    }
                    // Relay & Standby (nós 6, 9, 18, 19): 10-30%
                    6 | 9 | 18 | 19 => {
                        assert!(
                            node.cpu >= 10 && node.cpu <= 30,
                            "Nó {} (Relay/Standby) fora do range 10-30%: {}",
                            node.id,
                            node.cpu
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn test_telemetry_payload_metrics() {
        let state = AppState::new();
        let event = run_telemetry_tick(&state, 1);
        let data = event.data;

        let spike_rate_val = data["spike_rate_val"].as_f64().unwrap();
        assert!((spike_rate_val - 2.40).abs() < 0.15);

        let arrow_bw = data["arrow_bandwidth_gb"].as_f64().unwrap();
        assert!((arrow_bw - 85.4).abs() < 2.0);

        let wasm_p95 = data["wasm_latency_p95_ms"].as_f64().unwrap();
        assert!((wasm_p95 - 1.12).abs() < 0.1);

        assert_eq!(data["healthy_nodes"].as_u64().unwrap(), 20);
        assert_eq!(data["failed_nodes"].as_u64().unwrap(), 0);
        assert_eq!(data["total_nodes"].as_u64().unwrap(), 20);

        let nodes_arr = data["nodes"].as_array().unwrap();
        assert_eq!(nodes_arr.len(), 20);
    }

    #[test]
    fn test_telemetry_chaos_failure_handling() {
        let state = AppState::new();

        // Falhar o nó 2 (kpx-node-02)
        {
            let mut nodes = state.nodes.write();
            nodes[1].status = "FAILED".to_string();
        }

        let event = run_telemetry_tick(&state, 1);
        let data = event.data;

        assert_eq!(data["healthy_nodes"].as_u64().unwrap(), 19);
        assert_eq!(data["failed_nodes"].as_u64().unwrap(), 1);

        let nodes = state.nodes.read();
        assert_eq!(nodes[1].cpu, 0);
        assert_eq!(nodes[1].rx, "0.0 KB/s");
    }
}
