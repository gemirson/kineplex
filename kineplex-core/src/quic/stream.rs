//! QUIC stream multiplexing module.
//!
//! Maps logical graph edges (Graph_ID, Node_Src, Node_Dst) to QUIC stream IDs
//! enabling O(1) routing of Arrow data to the correct Wasm module.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::debug;

/// Stream identifier for QUIC streams.
pub type StreamID = u64;

/// A unique key identifying a graph edge between two nodes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgeKey {
    /// The graph identifier.
    pub graph_id: u64,
    /// Source node identifier.
    pub src_node: u64,
    /// Destination node identifier.
    pub dst_node: u64,
}

impl EdgeKey {
    /// Creates a new edge key.
    #[must_use]
    pub const fn new(graph_id: u64, src_node: u64, dst_node: u64) -> Self {
        Self {
            graph_id,
            src_node,
            dst_node,
        }
    }
}

/// Manages QUIC stream allocation and mapping for graph edges.
///
/// Thread-safe mapping from (Graph_ID, Src_Node, Dst_Node) -> Stream_ID.
/// Uses DashMap for O(1) concurrent lookups and allocations.
#[derive(Debug, Default)]
pub struct StreamManager {
    /// Maps edge keys to stream IDs.
    edge_to_stream: DashMap<EdgeKey, StreamID>,
    /// Maps stream IDs back to edge keys (reverse lookup).
    stream_to_edge: DashMap<StreamID, EdgeKey>,
    /// Next available stream ID.
    next_stream_id: AtomicU64,
}

impl StreamManager {
    /// Creates a new StreamManager.
    #[must_use]
    pub fn new() -> Self {
        Self {
            edge_to_stream: DashMap::new(),
            stream_to_edge: DashMap::new(),
            next_stream_id: AtomicU64::new(1), // Start from 1 (0 is reserved)
        }
    }

    /// Gets or creates a QUIC stream for the given graph edge.
    ///
    /// Returns the stream ID associated with (graph_id, src_node, dst_node).
    /// If the stream doesn't exist, it's created atomically.
    ///
    /// # Arguments
    ///
    /// * `graph_id` - The graph identifier.
    /// * `src_node` - Source node identifier.
    /// * `dst_node` - Destination node identifier.
    ///
    /// # Returns
    ///
    /// The allocated or existing stream ID.
    pub fn get_or_create_stream(&self, graph_id: u64, src_node: u64, dst_node: u64) -> StreamID {
        let edge_key = EdgeKey::new(graph_id, src_node, dst_node);

        // Fast path: check if stream already exists
        if let Some(entry) = self.edge_to_stream.get(&edge_key) {
            return *entry;
        }

        // Slow path: allocate new stream
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::SeqCst);

        // Insert both mappings atomically
        self.edge_to_stream.insert(edge_key.clone(), stream_id);
        self.stream_to_edge.insert(stream_id, edge_key);

        debug!(
            stream_id,
            graph_id, src_node, dst_node, "Created new QUIC stream for graph edge"
        );

        stream_id
    }

    /// Gets the stream ID for an existing edge.
    ///
    /// # Returns
    ///
    /// `Some(StreamID)` if the edge exists, `None` otherwise.
    pub fn get_stream_id(&self, graph_id: u64, src_node: u64, dst_node: u64) -> Option<StreamID> {
        let edge_key = EdgeKey::new(graph_id, src_node, dst_node);
        self.edge_to_stream.get(&edge_key).map(|id| *id)
    }

    /// Gets the edge key for a given stream ID.
    ///
    /// # Returns
    ///
    /// `Some(EdgeKey)` if the stream exists, `None` otherwise.
    pub fn get_edge_for_stream(&self, stream_id: StreamID) -> Option<EdgeKey> {
        self.stream_to_edge
            .get(&stream_id)
            .map(|entry| (*entry).clone())
    }

    /// Removes a stream and its reverse mapping.
    ///
    /// # Returns
    ///
    /// `true` if the stream was removed, `false` if it didn't exist.
    pub fn remove_stream(&self, graph_id: u64, src_node: u64, dst_node: u64) -> bool {
        let edge_key = EdgeKey::new(graph_id, src_node, dst_node);

        if let Some(stream_id) = self.edge_to_stream.remove(&edge_key) {
            self.stream_to_edge.remove(&stream_id.1);
            true
        } else {
            false
        }
    }

    /// Returns the current number of active streams.
    #[must_use]
    pub fn stream_count(&self) -> usize {
        self.edge_to_stream.len()
    }

    /// Clears all streams from the manager.
    pub fn clear(&self) {
        self.edge_to_stream.clear();
        self.stream_to_edge.clear();
        self.next_stream_id.store(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_or_create_stream_creates_new() {
        let manager = StreamManager::new();

        let stream_id = manager.get_or_create_stream(1, 100, 200);

        assert_eq!(stream_id, 1);
    }

    #[test]
    fn test_get_or_create_stream_reuses_existing() {
        let manager = StreamManager::new();

        let stream1 = manager.get_or_create_stream(1, 100, 200);
        let stream2 = manager.get_or_create_stream(1, 100, 200);

        assert_eq!(stream1, stream2);
    }

    #[test]
    fn test_different_graph_ids_get_different_streams() {
        let manager = StreamManager::new();

        let stream1 = manager.get_or_create_stream(1, 100, 200);
        let stream2 = manager.get_or_create_stream(2, 100, 200);

        assert_ne!(stream1, stream2);
    }

    #[test]
    fn test_different_src_nodes_get_different_streams() {
        let manager = StreamManager::new();

        let stream1 = manager.get_or_create_stream(1, 100, 200);
        let stream2 = manager.get_or_create_stream(1, 101, 200);

        assert_ne!(stream1, stream2);
    }

    #[test]
    fn test_different_dst_nodes_get_different_streams() {
        let manager = StreamManager::new();

        let stream1 = manager.get_or_create_stream(1, 100, 200);
        let stream2 = manager.get_or_create_stream(1, 100, 201);

        assert_ne!(stream1, stream2);
    }

    #[test]
    fn test_get_stream_id_returns_none_for_missing() {
        let manager = StreamManager::new();

        let stream_id = manager.get_stream_id(999, 100, 200);

        assert!(stream_id.is_none());
    }

    #[test]
    fn test_get_edge_for_stream() {
        let manager = StreamManager::new();

        let stream_id = manager.get_or_create_stream(1, 100, 200);
        let edge_key = manager
            .get_edge_for_stream(stream_id)
            .expect("Stream should exist");

        assert_eq!(edge_key.graph_id, 1);
        assert_eq!(edge_key.src_node, 100);
        assert_eq!(edge_key.dst_node, 200);
    }

    #[test]
    fn test_remove_stream() {
        let manager = StreamManager::new();

        let stream_id = manager.get_or_create_stream(1, 100, 200);
        assert!(manager.remove_stream(1, 100, 200));
        assert!(manager.get_stream_id(1, 100, 200).is_none());
        assert!(manager.get_edge_for_stream(stream_id).is_none());
    }

    #[test]
    fn test_stream_count() {
        let manager = StreamManager::new();

        assert_eq!(manager.stream_count(), 0);

        manager.get_or_create_stream(1, 100, 200);
        assert_eq!(manager.stream_count(), 1);

        manager.get_or_create_stream(1, 101, 200);
        assert_eq!(manager.stream_count(), 2);
    }

    #[test]
    fn test_clear() {
        let manager = StreamManager::new();

        manager.get_or_create_stream(1, 100, 200);
        manager.get_or_create_stream(2, 100, 200);

        manager.clear();

        assert_eq!(manager.stream_count(), 0);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let manager = Arc::new(StreamManager::new());
        let mut handles = vec![];

        for i in 0..10 {
            let manager_clone = Arc::clone(&manager);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    manager_clone.get_or_create_stream(i, j, j + 1000);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Each thread creates 100 unique streams (different graph_id)
        assert_eq!(manager.stream_count(), 1000);
    }
}
