use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use kineplex_core::geometry::{
    AtomicGeodesicTable, MetricSample, MetricWorker, RouteEntry, RouteSnapshot,
};

#[test]
fn route_snapshot_replacement_and_metric_updates_converge_under_concurrency() {
    let routes = Arc::new(AtomicGeodesicTable::new(RouteSnapshot {
        revision: 0,
        routes: HashMap::from([(
            17,
            RouteEntry {
                next_node: 1,
                resistance: 1.0,
            },
        )]),
    }));
    let (worker, publisher) =
        MetricWorker::spawn(Duration::from_millis(2)).expect("background geometry worker starts");

    let route_writer = {
        let routes = Arc::clone(&routes);
        thread::spawn(move || {
            for revision in 1..=2_000_u64 {
                routes.publish(RouteSnapshot {
                    revision,
                    routes: HashMap::from([(
                        17,
                        RouteEntry {
                            next_node: revision % 8,
                            resistance: 1.0 + (revision % 100) as f64 / 100.0,
                        },
                    )]),
                });
            }
        })
    };
    let metric_writer = thread::spawn(move || {
        for index in 0..2_000 {
            let sample = MetricSample {
                latency_ms: (index % 100) as f64,
                queued_bytes: (index * 64) as f64,
                cpu_percent: (index % 100) as f64,
            };
            let _try_send_result = publisher.try_publish(sample);
        }
    });

    for _ in 0..10_000 {
        let route = routes
            .lookup(17)
            .expect("published route is always complete");
        assert!(route.resistance.is_finite());
    }
    route_writer.join().expect("route publisher joins");
    metric_writer.join().expect("metric publisher joins");
    thread::sleep(Duration::from_millis(15));
    let snapshot = worker.snapshot();
    assert!(snapshot
        .components
        .iter()
        .flatten()
        .all(|value| value.is_finite()));
    assert_eq!(routes.revision(), 2_000);
    worker.shutdown();
}
