//! FT-043/FT-044 — Criterion benchmarks for geometry stability.
//!
//! Covers:
//!   • atomic route lookup latency (RouteKey → RouteEntry, target < 50 ns)
//!   • SIMD AVX2 vs scalar norm ratio (target > 3×)
//!   • concurrent snapshot publish + lookup
//!   • HomotopyPath norm sampling

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kineplex_core::geometry::{
    metric_norm_squared, metric_norm_squared_scalar, AtomicGeodesicTable, GeodesicState,
    HomotopyPath, MetricSample, MetricTensor, RouteEntry, RouteSnapshot,
};

// ---------------------------------------------------------------------------
// 1. Atomic route lookup latency
// ---------------------------------------------------------------------------

fn bench_atomic_route_lookup(c: &mut Criterion) {
    // Pre-populate a table with 65 536 routes so the lookup exercises HashMap
    // path logic, not just an empty-map short-circuit.
    let routes: HashMap<u64, RouteEntry> = (0_u64..65_536)
        .map(|k| (k, RouteEntry { next_node: k.wrapping_add(1), resistance: 1.0 }))
        .collect();
    let table = AtomicGeodesicTable::new(RouteSnapshot { revision: 1, routes });

    let mut group = c.benchmark_group("atomic_route_lookup");
    // Report throughput as one lookup per iteration so ns/op is directly
    // comparable to the < 50 ns target.
    group.throughput(Throughput::Elements(1));

    group.bench_function("lookup_65k_table", |b| {
        b.iter(|| table.lookup(black_box(42_001)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. AVX2 vs scalar SIMD gain
// ---------------------------------------------------------------------------

fn bench_simd_avx2_vs_scalar(c: &mut Criterion) {
    let metric = MetricTensor::from_sample(
        MetricSample { latency_ms: 15.0, queued_bytes: 4096.0, cpu_percent: 30.0 },
        1,
    );
    let vector = black_box([1.25_f64, 2.5, 3.75]);

    let mut group = c.benchmark_group("simd_norm");
    group.throughput(Throughput::Elements(1));

    group.bench_function("scalar", |b| {
        b.iter(|| metric_norm_squared_scalar(black_box(&metric), black_box(vector)));
    });

    // `metric_norm_squared` dispatches to AVX-512, AVX2, or scalar at runtime.
    group.bench_function("avx2_dispatch", |b| {
        b.iter(|| metric_norm_squared(black_box(&metric), black_box(vector)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Concurrent snapshot publish + lookup
// ---------------------------------------------------------------------------

fn bench_concurrent_publish_lookup(c: &mut Criterion) {
    const READERS: usize = 4;
    const ITERATIONS: u64 = 512;

    let table = Arc::new(AtomicGeodesicTable::new(RouteSnapshot {
        revision: 0,
        routes: HashMap::from([(1_u64, RouteEntry { next_node: 2, resistance: 1.0 })]),
    }));

    let mut group = c.benchmark_group("concurrent_publish_lookup");
    group.throughput(Throughput::Elements(ITERATIONS));

    group.bench_function(
        BenchmarkId::new("publish_and_lookup", READERS),
        |b| {
            b.iter(|| {
                let writer_table = Arc::clone(&table);
                let writer = thread::spawn(move || {
                    for i in 0..ITERATIONS {
                        if i % 16 == 0 {
                            writer_table.publish(RouteSnapshot {
                                revision: i + 1,
                                routes: HashMap::from([(
                                    1_u64,
                                    RouteEntry {
                                        next_node: i % 8,
                                        resistance: 1.0 + i as f64 / 512.0,
                                    },
                                )]),
                            });
                        }
                    }
                });

                let reader_handles: Vec<_> = (0..READERS)
                    .map(|_| {
                        let t = Arc::clone(&table);
                        thread::spawn(move || {
                            for i in 0..ITERATIONS {
                                black_box(t.lookup(black_box(1)));
                                // Tiny spin to simulate realistic interleaving.
                                if i % 64 == 0 {
                                    std::hint::spin_loop();
                                }
                            }
                        })
                    })
                    .collect();

                writer.join().unwrap();
                for h in reader_handles {
                    h.join().unwrap();
                }
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. HomotopyPath norm (sample_into + metric norm)
// ---------------------------------------------------------------------------

fn bench_homotopy_path_norm(c: &mut Criterion) {
    let path = HomotopyPath::new(
        vec![[0.0_f64; 3], [1.0, 0.0, 0.0], [2.0, 0.0, 0.0]],
        vec![[0.0_f64; 3], [1.0, 1.0, 0.0], [2.0, 1.0, 0.0]],
    )
    .expect("aligned waypoints");

    let metric = MetricTensor::from_sample(
        MetricSample { latency_ms: 5.0, queued_bytes: 1024.0, cpu_percent: 20.0 },
        2,
    );

    let mut output = [[0.0_f64; 3]; 3];

    let mut group = c.benchmark_group("homotopy_path_norm");
    group.throughput(Throughput::Elements(1));

    group.bench_function("sample_and_norm", |b| {
        b.iter(|| {
            let progress = black_box(0.5_f64);
            path.sample_into(progress, &mut output).expect("aligned output");
            // Compute norm on the midpoint waypoint so the compiler cannot
            // eliminate the sample_into call.
            let norm = metric.norm_squared(black_box(output[1]));
            black_box(norm);
        });
    });

    // Extra variant: full path sweep across 100 progress steps.
    group.bench_function("full_sweep_100_steps", |b| {
        b.iter(|| {
            let mut total = 0.0_f64;
            for step in 0..100_u64 {
                let progress = step as f64 / 99.0;
                path.sample_into(black_box(progress), &mut output)
                    .expect("aligned output");
                total += metric.norm_squared(black_box(output[1]));
            }
            black_box(total);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Combined mutation + forwarding loop (legacy workload)
// ---------------------------------------------------------------------------

fn bench_mutate_routes_while_forwarding(c: &mut Criterion) {
    let routes = AtomicGeodesicTable::new(RouteSnapshot {
        revision: 0,
        routes: HashMap::from([(1_u64, RouteEntry { next_node: 2, resistance: 1.0 })]),
    });
    let path = HomotopyPath::new(
        vec![[0.0_f64; 3], [1.0, 0.0, 0.0]],
        vec![[0.0_f64; 3], [1.0, 1.0, 0.0]],
    )
    .expect("homotopy fixture is valid");
    let mut waypoint_buffer = [[0.0_f64; 3]; 2];
    let metric = MetricTensor::from_sample(
        MetricSample { latency_ms: 1.0, queued_bytes: 0.0, cpu_percent: 10.0 },
        1,
    );

    let mut group = c.benchmark_group("geometry_mutation_forwarding");
    group.throughput(Throughput::Elements(256));

    group.bench_function(BenchmarkId::new("atomic_route_and_homotopy", 256), |b| {
        b.iter(|| {
            for index in 0..256_u64 {
                if index % 32 == 0 {
                    routes.publish(RouteSnapshot {
                        revision: index + 1,
                        routes: HashMap::from([(
                            1_u64,
                            RouteEntry {
                                next_node: index % 4,
                                resistance: 1.0 + index as f64 / 256.0,
                            },
                        )]),
                    });
                }
                let route =
                    routes.lookup(black_box(1)).expect("route snapshot remains complete");
                let progress = (index % 100) as f64 / 99.0;
                path.sample_into(progress, &mut waypoint_buffer)
                    .expect("fixed output size");
                black_box((
                    route.next_node,
                    metric.norm_squared(waypoint_buffer[1]),
                    GeodesicState::default(),
                ));
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_atomic_route_lookup,
    bench_simd_avx2_vs_scalar,
    bench_concurrent_publish_lookup,
    bench_homotopy_path_norm,
    bench_mutate_routes_while_forwarding,
);
criterion_main!(benches);
