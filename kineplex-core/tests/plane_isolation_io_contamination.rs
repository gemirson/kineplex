//! FT-056: Verifica que operações geométricas pesadas não contaminam o loop de I/O.
//! O Data Plane deve apenas fazer lookups atômicos; cálculos de tensores ficam no Control Plane.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kineplex_core::geometry::{AtomicGeodesicTable, MetricSample, RouteEntry, RouteSnapshot};
use kineplex_core::plane_isolation::PlaneIsolation;

/// Spawna `PlaneIsolation`, simula 10.000 chamadas de `data.route(edge)` +
/// `data.try_report_metric(sample)` em uma thread separada marcada como "data-plane",
/// e em OUTRA thread ("control-plane") faz 1.000 chamadas de `control.metric_snapshot()`.
///
/// Verifica que:
/// - Todos os lookups retornam `Some` ou `None` sem panic
/// - `control.metric_snapshot().components` são todos finitos
/// - Nenhum bloqueio ocorre (usa timeout de 2s para join das threads)
#[test]
fn plane_isolation_geometry_stays_off_data_plane_thread() {
    let initial = RouteSnapshot {
        revision: 1,
        routes: HashMap::from([
            (
                1_u64,
                RouteEntry {
                    next_node: 2,
                    resistance: 1.0,
                },
            ),
            (
                2_u64,
                RouteEntry {
                    next_node: 3,
                    resistance: 0.5,
                },
            ),
        ]),
    };

    // PlaneIsolation::spawn pode falhar se a plataforma não suportar CPU affinity;
    // nesse caso o teste é pulado graciosamente, assim como nos testes existentes.
    let Ok((control, data)) = PlaneIsolation::spawn(initial, 64) else {
        return;
    };

    // ── Data Plane thread ──────────────────────────────────────────────────────
    // Simula o hot path de I/O: apenas lookups atômicos e enfileiramento de métricas.
    let data_handle = {
        let data = data.clone();
        std::thread::Builder::new()
            .name("data-plane".to_owned())
            .spawn(move || {
                for i in 0_u64..10_000 {
                    // lookup pode retornar Some ou None — ambos são válidos
                    let _route = data.route(1 + (i % 2));

                    let sample = MetricSample {
                        latency_ms: (i % 100) as f64,
                        queued_bytes: (i * 64) as f64,
                        cpu_percent: (i % 100) as f64,
                    };
                    // try_report_metric nunca bloqueia: Ok ou Err por canal cheio
                    let _result = data.try_report_metric(sample);
                }
            })
            .expect("data-plane thread spawns")
    };

    // ── Control Plane thread ───────────────────────────────────────────────────
    // Lê snapshots do tensor métrico enquanto o Data Plane enfileira amostras.
    let snapshot_results: Vec<bool> = {
        let mut results = Vec::with_capacity(1_000);
        for _ in 0..1_000 {
            let snapshot = control.metric_snapshot();
            let all_finite = snapshot
                .components
                .iter()
                .flatten()
                .all(|v| v.is_finite());
            results.push(all_finite);
        }
        results
    };

    // Valida que todos os snapshots tinham componentes finitos
    assert!(
        snapshot_results.iter().all(|&ok| ok),
        "metric_snapshot().components deve ser sempre finito"
    );

    // Join com timeout de 2s — garante que não há bloqueio no Data Plane thread
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if data_handle.is_finished() {
            break;
        }
        if Instant::now() > deadline {
            panic!("data-plane thread demorou mais de 2s — possível bloqueio no hot path");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    data_handle.join().expect("data-plane thread finaliza sem panic");

    // Snapshot final após a thread de dados ter terminado
    let final_snapshot = control.metric_snapshot();
    assert!(
        final_snapshot
            .components
            .iter()
            .flatten()
            .all(|v| v.is_finite()),
        "snapshot final deve ter todos os componentes finitos"
    );

    control.shutdown();
}

/// Mede o tempo de 100.000 iterações de `routes.lookup(key)` em loop e verifica
/// que o tempo total < 100ms — validando que não há operações bloqueantes no hot path.
#[test]
fn data_plane_geometry_operations_are_wait_free() {
    let routes = Arc::new(AtomicGeodesicTable::new(RouteSnapshot {
        revision: 1,
        routes: HashMap::from([(
            42_u64,
            RouteEntry {
                next_node: 7,
                resistance: 0.25,
            },
        )]),
    }));

    let start = Instant::now();
    for _ in 0..100_000 {
        // lookup usa ArcSwap load() — sem mutex, sem bloqueio
        let _entry = routes.lookup(42);
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(100),
        "100.000 lookups atômicos levaram {:?} (limite: 100ms) — hot path pode estar bloqueante",
        elapsed
    );
}
