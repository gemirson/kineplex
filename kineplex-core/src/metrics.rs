//! Lock-light Prometheus metrics for the KinePlex data and control planes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const WASM_BUCKETS_MS: [u64; 9] = [1, 5, 10, 25, 50, 100, 250, 500, 1000];

pub struct MetricsRegistry {
    spikes_total: AtomicU64,
    arrow_bytes_total: AtomicU64,
    wasm_count: AtomicU64,
    wasm_sum_micros: AtomicU64,
    wasm_buckets: [AtomicU64; WASM_BUCKETS_MS.len()],
    cqe_drops: AtomicU64,
    topology_nodes: AtomicU64,
    topology_edges: AtomicU64,
    curvature_tensors: AtomicU64,
    riemann_curvature_bits: AtomicU64,
    geodesic_revision: AtomicU64,
    geodesic_lookups: AtomicU64,
    atlas_invalidations: AtomicU64,
    cache: Mutex<Option<(Instant, String)>>,
}

impl MetricsRegistry {
    fn new() -> Self {
        Self {
            spikes_total: AtomicU64::new(0),
            arrow_bytes_total: AtomicU64::new(0),
            wasm_count: AtomicU64::new(0),
            wasm_sum_micros: AtomicU64::new(0),
            wasm_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            cqe_drops: AtomicU64::new(0),
            topology_nodes: AtomicU64::new(0),
            topology_edges: AtomicU64::new(0),
            curvature_tensors: AtomicU64::new(0),
            riemann_curvature_bits: AtomicU64::new(0.0_f64.to_bits()),
            geodesic_revision: AtomicU64::new(0),
            geodesic_lookups: AtomicU64::new(0),
            atlas_invalidations: AtomicU64::new(0),
            cache: Mutex::new(None),
        }
    }

    pub fn record_spike(&self, arrow_bytes: usize) {
        self.spikes_total.fetch_add(1, Ordering::Relaxed);
        self.arrow_bytes_total
            .fetch_add(arrow_bytes as u64, Ordering::Relaxed);
    }

    pub fn record_wasm_execution(&self, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.wasm_count.fetch_add(1, Ordering::Relaxed);
        self.wasm_sum_micros.fetch_add(micros, Ordering::Relaxed);
        let millis = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        for (index, boundary) in WASM_BUCKETS_MS.iter().enumerate() {
            if millis <= *boundary {
                self.wasm_buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn record_cqe_drop(&self) {
        self.cqe_drops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_topology(&self, nodes: usize, edges: usize, curvature_tensors: usize) {
        self.topology_nodes.store(nodes as u64, Ordering::Relaxed);
        self.topology_edges.store(edges as u64, Ordering::Relaxed);
        self.curvature_tensors
            .store(curvature_tensors as u64, Ordering::Relaxed);
    }

    pub fn observe_riemann_curvature(&self, curvature: f64) {
        let value = if curvature.is_finite() {
            curvature.max(0.0)
        } else {
            f64::MAX
        };
        self.riemann_curvature_bits
            .store(value.to_bits(), Ordering::Relaxed);
    }

    pub fn set_geodesic_revision(&self, revision: u64) {
        self.geodesic_revision.store(revision, Ordering::Relaxed);
    }

    pub fn record_geodesic_lookup(&self) {
        self.geodesic_lookups.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_atlas_invalidation(&self) {
        self.atlas_invalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns Prometheus text, reusing a one-second snapshot when available.
    #[must_use]
    pub fn prometheus_text(&self) -> String {
        if let Ok(cache) = self.cache.try_lock() {
            if let Some((created, body)) = cache.as_ref() {
                if created.elapsed() < Duration::from_secs(1) {
                    return body.clone();
                }
            }
        }
        let body = self.render();
        if let Ok(mut cache) = self.cache.try_lock() {
            *cache = Some((Instant::now(), body.clone()));
        }
        body
    }

    fn render(&self) -> String {
        let mut out = String::with_capacity(2048);
        out.push_str("# HELP kineplex_spikes_total Total emitted KinePlex spikes.\n# TYPE kineplex_spikes_total counter\n");
        out.push_str(&format!(
            "kineplex_spikes_total {}\n",
            self.spikes_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP kineplex_arrow_bytes_total Arrow payload bytes emitted.\n# TYPE kineplex_arrow_bytes_total counter\n");
        out.push_str(&format!(
            "kineplex_arrow_bytes_total {}\n",
            self.arrow_bytes_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP kineplex_wasm_execution_ms Wasm execution duration in milliseconds.\n# TYPE kineplex_wasm_execution_ms histogram\n");
        for (index, boundary) in WASM_BUCKETS_MS.iter().enumerate() {
            out.push_str(&format!(
                "kineplex_wasm_execution_ms_bucket{{le=\"{boundary}\"}} {}\n",
                self.wasm_buckets[index].load(Ordering::Relaxed)
            ));
        }
        out.push_str(&format!(
            "kineplex_wasm_execution_ms_bucket{{le=\"+Inf\"}} {}\n",
            self.wasm_count.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "kineplex_wasm_execution_ms_sum {}\nkineplex_wasm_execution_ms_count {}\n",
            self.wasm_sum_micros.load(Ordering::Relaxed) as f64 / 1000.0,
            self.wasm_count.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP kineplex_cqe_drops Dropped io_uring completion events.\n# TYPE kineplex_cqe_drops counter\n");
        out.push_str(&format!(
            "kineplex_cqe_drops {}\n",
            self.cqe_drops.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP kineplex_topology_nodes Current known topology node count.\n# TYPE kineplex_topology_nodes gauge\n");
        out.push_str(&format!(
            "kineplex_topology_nodes {}\n",
            self.topology_nodes.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP kineplex_topology_edges Current known topology edge count.\n# TYPE kineplex_topology_edges gauge\n");
        out.push_str(&format!(
            "kineplex_topology_edges {}\n",
            self.topology_edges.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP kineplex_curvature_tensors Current curvature tensor count.\n# TYPE kineplex_curvature_tensors gauge\n");
        out.push_str(&format!(
            "kineplex_curvature_tensors {}\n",
            self.curvature_tensors.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP kineplex_riemann_curvature Current local Riemann curvature norm.\n# TYPE kineplex_riemann_curvature gauge\n");
        out.push_str(&format!(
            "kineplex_riemann_curvature {}\n",
            f64::from_bits(self.riemann_curvature_bits.load(Ordering::Relaxed))
        ));
        out.push_str("# HELP kineplex_geodesic_route_revision Active route snapshot revision.\n# TYPE kineplex_geodesic_route_revision gauge\n");
        out.push_str(&format!(
            "kineplex_geodesic_route_revision {}\n",
            self.geodesic_revision.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP kineplex_geodesic_route_lookups_total Geodesic route lookup count.\n# TYPE kineplex_geodesic_route_lookups_total counter\n");
        out.push_str(&format!(
            "kineplex_geodesic_route_lookups_total {}\n",
            self.geodesic_lookups.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP kineplex_atlas_invalidations_total Local charts invalidated by curvature.\n# TYPE kineplex_atlas_invalidations_total counter\n");
        out.push_str(&format!(
            "kineplex_atlas_invalidations_total {}\n",
            self.atlas_invalidations.load(Ordering::Relaxed)
        ));
        out
    }

    /// Returns metrics as an OTLP/HTTP JSON payload (no external dependencies).
    #[must_use]
    pub fn otlp_json(&self) -> String {
        let spikes = self.spikes_total.load(Ordering::Relaxed) as f64;
        let arrow_bytes = self.arrow_bytes_total.load(Ordering::Relaxed) as f64;
        let cqe_drops = self.cqe_drops.load(Ordering::Relaxed) as f64;
        let topology_nodes = self.topology_nodes.load(Ordering::Relaxed) as f64;
        let topology_edges = self.topology_edges.load(Ordering::Relaxed) as f64;
        let curvature_tensors = self.curvature_tensors.load(Ordering::Relaxed) as f64;
        let riemann_curvature =
            f64::from_bits(self.riemann_curvature_bits.load(Ordering::Relaxed));
        let geodesic_revision = self.geodesic_revision.load(Ordering::Relaxed) as f64;
        let geodesic_lookups = self.geodesic_lookups.load(Ordering::Relaxed) as f64;
        let atlas_invalidations = self.atlas_invalidations.load(Ordering::Relaxed) as f64;

        // Wasm histogram
        let wasm_count = self.wasm_count.load(Ordering::Relaxed) as f64;
        let wasm_sum_ms = self.wasm_sum_micros.load(Ordering::Relaxed) as f64 / 1000.0;
        let mut wasm_bucket_points = String::new();
        for (index, boundary) in WASM_BUCKETS_MS.iter().enumerate() {
            let count = self.wasm_buckets[index].load(Ordering::Relaxed) as f64;
            wasm_bucket_points.push_str(&format!(
                r#"{{"attributes":[{{"key":"le","value":{{"stringValue":"{boundary}"}}}}],"asDouble":{count},"startTimeUnixNano":"0","timeUnixNano":"0"}},"#
            ));
        }
        // +Inf bucket
        wasm_bucket_points.push_str(&format!(
            r#"{{"attributes":[{{"key":"le","value":{{"stringValue":"+Inf"}}}}],"asDouble":{wasm_count},"startTimeUnixNano":"0","timeUnixNano":"0"}}"#
        ));

        format!(
            r#"{{"resourceMetrics":[{{"resource":{{"attributes":[]}},"scopeMetrics":[{{"scope":{{"name":"kineplex"}},"metrics":[{{"name":"kineplex_spikes_total","description":"Total emitted KinePlex spikes.","unit":"1","sum":{{"dataPoints":[{{"asDouble":{spikes},"startTimeUnixNano":"0","timeUnixNano":"0"}}],"aggregationTemporality":2,"isMonotonic":true}}}},{{"name":"kineplex_arrow_bytes_total","description":"Arrow payload bytes emitted.","unit":"1","sum":{{"dataPoints":[{{"asDouble":{arrow_bytes},"startTimeUnixNano":"0","timeUnixNano":"0"}}],"aggregationTemporality":2,"isMonotonic":true}}}},{{"name":"kineplex_wasm_execution_ms","description":"Wasm execution duration in milliseconds.","unit":"ms","histogram":{{"dataPoints":[{{"bucketCounts":[{wasm_bucket_points}],"sum":{wasm_sum_ms},"count":{wasm_count},"startTimeUnixNano":"0","timeUnixNano":"0"}}],"aggregationTemporality":2}}}},{{"name":"kineplex_cqe_drops","description":"Dropped io_uring completion events.","unit":"1","sum":{{"dataPoints":[{{"asDouble":{cqe_drops},"startTimeUnixNano":"0","timeUnixNano":"0"}}],"aggregationTemporality":2,"isMonotonic":true}}}},{{"name":"kineplex_topology_nodes","description":"Current known topology node count.","unit":"1","gauge":{{"dataPoints":[{{"asDouble":{topology_nodes},"startTimeUnixNano":"0","timeUnixNano":"0"}}]}}}},{{"name":"kineplex_topology_edges","description":"Current known topology edge count.","unit":"1","gauge":{{"dataPoints":[{{"asDouble":{topology_edges},"startTimeUnixNano":"0","timeUnixNano":"0"}}]}}}},{{"name":"kineplex_curvature_tensors","description":"Current curvature tensor count.","unit":"1","gauge":{{"dataPoints":[{{"asDouble":{curvature_tensors},"startTimeUnixNano":"0","timeUnixNano":"0"}}]}}}},{{"name":"kineplex_riemann_curvature","description":"Current local Riemann curvature norm.","unit":"1","gauge":{{"dataPoints":[{{"asDouble":{riemann_curvature},"startTimeUnixNano":"0","timeUnixNano":"0"}}]}}}},{{"name":"kineplex_geodesic_route_revision","description":"Active route snapshot revision.","unit":"1","gauge":{{"dataPoints":[{{"asDouble":{geodesic_revision},"startTimeUnixNano":"0","timeUnixNano":"0"}}]}}}},{{"name":"kineplex_geodesic_route_lookups_total","description":"Geodesic route lookup count.","unit":"1","sum":{{"dataPoints":[{{"asDouble":{geodesic_lookups},"startTimeUnixNano":"0","timeUnixNano":"0"}}],"aggregationTemporality":2,"isMonotonic":true}}}},{{"name":"kineplex_atlas_invalidations_total","description":"Local charts invalidated by curvature.","unit":"1","sum":{{"dataPoints":[{{"asDouble":{atlas_invalidations},"startTimeUnixNano":"0","timeUnixNano":"0"}}],"aggregationTemporality":2,"isMonotonic":true}}}}]}}]}}]}}"#
        )
    }

static REGISTRY: OnceLock<MetricsRegistry> = OnceLock::new();

/// Global metrics registry shared by instrumentation and `/metrics`.
#[must_use]
pub fn global() -> &'static MetricsRegistry {
    REGISTRY.get_or_init(MetricsRegistry::new)
}

#[cfg(test)]
mod tests {
    use super::MetricsRegistry;
    use std::time::Duration;

    #[test]
    fn otlp_json_contains_required_fields() {
        let metrics = MetricsRegistry::new();
        metrics.record_spike(64);
        metrics.observe_riemann_curvature(0.5);
        let json = metrics.otlp_json();
        assert!(json.contains("resourceMetrics"), "missing resourceMetrics");
        assert!(
            json.contains("kineplex_spikes_total"),
            "missing kineplex_spikes_total"
        );
        assert!(
            json.contains("kineplex_riemann_curvature"),
            "missing kineplex_riemann_curvature"
        );
    }

    #[test]
    fn exposes_required_counters_histogram_and_topology_gauges() {
        let metrics = MetricsRegistry::new();
        metrics.record_spike(128);
        metrics.record_wasm_execution(Duration::from_millis(3));
        metrics.record_cqe_drop();
        metrics.set_topology(4, 3, 2);
        metrics.observe_riemann_curvature(0.25);
        metrics.set_geodesic_revision(8);
        metrics.record_geodesic_lookup();
        metrics.record_atlas_invalidation();
        let text = metrics.render();
        for metric in [
            "kineplex_spikes_total 1",
            "kineplex_arrow_bytes_total 128",
            "kineplex_wasm_execution_ms_count 1",
            "kineplex_cqe_drops 1",
            "kineplex_topology_nodes 4",
            "kineplex_curvature_tensors 2",
            "kineplex_riemann_curvature 0.25",
            "kineplex_geodesic_route_revision 8",
            "kineplex_geodesic_route_lookups_total 1",
            "kineplex_atlas_invalidations_total 1",
        ] {
            assert!(text.contains(metric), "missing {metric}");
        }
    }
}
