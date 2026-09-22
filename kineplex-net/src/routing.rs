//! Concurrent in-memory routing decisions derived from Gossip telemetry.

use std::collections::BinaryHeap;
use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;

use crate::telemetry::NodeTelemetry;

/// Thread-safe routing view keyed by active Gossip member endpoint.
///
/// Each operation touches only the relevant DashMap shard. The Gossip supervisor
/// owns writes in production, while consumers such as the graph seeder may clone
/// this handle and perform concurrent reads without taking a table-wide lock.
#[derive(Clone, Default)]
pub struct RoutingTable {
    entries: Arc<DashMap<SocketAddr, NodeTelemetry>>,
}

impl RoutingTable {
    /// Creates an empty concurrent routing table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or replaces one node's latest telemetry in average O(1) time.
    pub(crate) fn upsert(&self, node: SocketAddr, telemetry: NodeTelemetry) {
        self.entries.insert(node, telemetry);
    }

    /// Inserts a default entry only when a member has just become known.
    pub(crate) fn ensure(&self, node: SocketAddr, telemetry: NodeTelemetry) {
        self.entries.entry(node).or_insert(telemetry);
    }

    /// Removes one node immediately after its effective MemberDown transition.
    pub(crate) fn remove(&self, node: &SocketAddr) -> Option<NodeTelemetry> {
        self.entries.remove(node).map(|(_, telemetry)| telemetry)
    }

    /// Returns the latest telemetry for one node, if it is currently routable.
    #[must_use]
    pub fn get(&self, node: &SocketAddr) -> Option<NodeTelemetry> {
        self.entries.get(node).map(|entry| *entry)
    }

    /// Returns whether the table currently contains a route for `node`.
    #[must_use]
    pub fn contains(&self, node: &SocketAddr) -> bool {
        self.entries.contains_key(node)
    }

    /// Returns the number of nodes with telemetry-backed routes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no node currently has a telemetry-backed route.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns a point-in-time copy of all routing entries.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(SocketAddr, NodeTelemetry)> {
        self.entries
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }

    /// Selects the `required` active nodes with the lowest CPU utilization.
    ///
    /// The bounded max-heap retains only the current K best candidates, so the
    /// selection costs O(N log K) and uses O(K) additional memory. Ties are
    /// deterministic by endpoint address.
    #[must_use]
    pub fn get_best_nodes(&self, required: usize) -> Vec<SocketAddr> {
        if required == 0 {
            return Vec::new();
        }

        let mut candidates = BinaryHeap::with_capacity(required);
        for entry in self.entries.iter() {
            let candidate = (entry.value().cpu_utilization_pct, *entry.key());
            if candidates.len() < required {
                candidates.push(candidate);
            } else if let Some(worst) = candidates.peek() {
                if candidate < *worst {
                    let _discarded = candidates.pop();
                    candidates.push(candidate);
                }
            }
        }

        candidates
            .into_sorted_vec()
            .into_iter()
            .map(|(_, node)| node)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::thread;

    use super::RoutingTable;
    use crate::telemetry::NodeTelemetry;

    fn node(index: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, index], 8000 + u16::from(index)))
    }

    #[test]
    fn selects_lowest_cpu_nodes_with_deterministic_ties() {
        let table = RoutingTable::new();
        table.upsert(node(1), NodeTelemetry::new(60, 100));
        table.upsert(node(2), NodeTelemetry::new(10, 100));
        table.upsert(node(3), NodeTelemetry::new(30, 100));
        table.upsert(node(4), NodeTelemetry::new(20, 100));

        assert_eq!(table.get_best_nodes(0), Vec::<SocketAddr>::new());
        assert_eq!(table.get_best_nodes(2), vec![node(2), node(4)]);
        assert_eq!(
            table.get_best_nodes(20),
            vec![node(2), node(4), node(3), node(1)]
        );
    }

    #[test]
    fn updates_and_eviction_are_immediate() {
        let table = RoutingTable::new();
        let endpoint = node(1);
        let original = NodeTelemetry::new(80, 100);
        let updated = NodeTelemetry::new(5, 200);

        table.upsert(endpoint, original);
        assert_eq!(table.get(&endpoint), Some(original));
        table.upsert(endpoint, updated);
        assert_eq!(table.get(&endpoint), Some(updated));
        assert_eq!(table.remove(&endpoint), Some(updated));
        assert!(!table.contains(&endpoint));
        assert!(table.is_empty());
    }

    #[test]
    fn supports_one_hundred_readers_and_ten_writers() {
        let table = RoutingTable::new();
        for index in 1..=100_u8 {
            table.upsert(node(index), NodeTelemetry::new(index % 100, 100));
        }

        thread::scope(|scope| {
            for _ in 0..100 {
                let reader = table.clone();
                scope.spawn(move || {
                    for _ in 0..1_000 {
                        let best = reader.get_best_nodes(10);
                        assert_eq!(best.len(), 10);
                    }
                });
            }
            for writer_id in 0..10_u8 {
                let writer = table.clone();
                scope.spawn(move || {
                    for sequence in 0..1_000_u16 {
                        let endpoint = node((sequence % 100 + 1) as u8);
                        writer.upsert(
                            endpoint,
                            NodeTelemetry::new(writer_id.saturating_mul(10), u32::from(sequence)),
                        );
                    }
                });
            }
        });

        assert_eq!(table.len(), 100);
    }
}
