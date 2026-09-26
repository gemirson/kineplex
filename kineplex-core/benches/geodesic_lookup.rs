use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use kineplex_core::geometry::{AtomicGeodesicTable, RouteEntry, RouteSnapshot};

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

criterion_group!(benches, precomputed_lookup);
criterion_main!(benches);
