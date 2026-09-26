use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use kineplex_core::geometry::{
    metric_norm_squared, metric_norm_squared_scalar, AtomicGeodesicTable, MetricSample,
    MetricTensor, RouteEntry, RouteSnapshot,
};

fn precomputed_lookup(c: &mut Criterion) {
    let routes: HashMap<_, _> = (0..65_536)
        .map(|key| {
            (
                key,
                RouteEntry {
                    next_node: key + 1,
                    resistance: 1.0,
                },
            )
        })
        .collect();
    let table = AtomicGeodesicTable::new(RouteSnapshot {
        revision: 1,
        routes,
    });
    c.bench_function("geodesic_atomic_route_lookup", |bencher| {
        bencher.iter(|| table.lookup(black_box(42_001)));
    });
}

fn metric_norm(c: &mut Criterion) {
    let metric = MetricTensor::from_sample(
        MetricSample {
            latency_ms: 15.0,
            queued_bytes: 4096.0,
            cpu_percent: 30.0,
        },
        1,
    );
    let vector = [1.25, 2.5, 3.75];
    c.bench_function("metric_norm_scalar", |bencher| {
        bencher.iter(|| metric_norm_squared_scalar(black_box(&metric), black_box(vector)));
    });
    c.bench_function("metric_norm_avx2_dispatch", |bencher| {
        bencher.iter(|| metric_norm_squared(black_box(&metric), black_box(vector)));
    });
}

criterion_group!(benches, precomputed_lookup, metric_norm);
criterion_main!(benches);
