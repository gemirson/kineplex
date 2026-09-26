use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use kineplex_core::geometry::{AtomicGeodesicTable, RouteEntry, RouteSnapshot};

#[test]
fn readers_keep_valid_routes_during_repeated_topology_deformations() {
    let table = Arc::new(AtomicGeodesicTable::new(RouteSnapshot {
        revision: 0,
        routes: HashMap::from([(42, RouteEntry { next_node: 1, resistance: 0.5 })]),
    }));
    let writer = {
        let table = Arc::clone(&table);
        thread::spawn(move || {
            for revision in 1..=10_000_u64 {
                let next_node = revision % 50;
                table.publish(RouteSnapshot {
                    revision,
                    routes: HashMap::from([(42, RouteEntry {
                        next_node,
                        resistance: 0.1 + (revision % 100) as f64 / 100.0,
                    })]),
                });
            }
        })
    };

    for _ in 0..100_000 {
        let route = table.lookup(42).expect("active snapshots always retain the test edge");
        assert!(route.next_node < 50);
        assert!(route.resistance.is_finite());
    }
    writer.join().expect("Control Plane deformation writer joins");
    assert_eq!(table.revision(), 10_000);
}
