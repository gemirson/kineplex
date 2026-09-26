//! Synapse handshake establishment module.
//!
//! Handles the handshake protocol between nodes before data transmission.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Default handshake timeout (1 second)
pub const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 1000;

/// Synapse connection states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SynapseState {
    #[default]
    Pending,
    Ready,
    Flowing,
    Closed,
}

impl std::fmt::Display for SynapseState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Ready => write!(f, "Ready"),
            Self::Flowing => write!(f, "Flowing"),
            Self::Closed => write!(f, "Closed"),
        }
    }
}

/// Flags for Synapse packet header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynapseFlag {
    Handshake,
    HandshakeAck,
    Data,
    Eof,
    Error,
}

impl SynapseFlag {
    pub fn to_u32(self) -> u32 {
        match self {
            Self::Handshake => 0x01,
            Self::HandshakeAck => 0x02,
            Self::Data => 0x04,
            Self::Eof => 0x08,
            Self::Error => 0x10,
        }
    }

    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0x01 => Some(Self::Handshake),
            0x02 => Some(Self::HandshakeAck),
            0x04 => Some(Self::Data),
            0x08 => Some(Self::Eof),
            0x10 => Some(Self::Error),
            _ => None,
        }
    }

    pub fn is_handshake(self) -> bool {
        matches!(self, Self::Handshake | Self::HandshakeAck)
    }

    pub fn is_data(self) -> bool {
        matches!(self, Self::Data)
    }
}

/// A synapse connection representing a logical graph edge between nodes.
pub struct Synapse {
    pub id: u64,
    pub graph_id: u64,
    pub src_node: u64,
    pub dst_node: u64,
    state: SynapseState,
    sequence: AtomicU64,
    last_activity: Option<Instant>,
    handshake_timeout: Duration,
}

impl Synapse {
    pub fn new(id: u64, graph_id: u64, src_node: u64, dst_node: u64) -> Self {
        Self {
            id,
            graph_id,
            src_node,
            dst_node,
            state: SynapseState::Pending,
            sequence: AtomicU64::new(0),
            last_activity: Some(Instant::now()),
            handshake_timeout: Duration::from_millis(DEFAULT_HANDSHAKE_TIMEOUT_MS),
        }
    }

    #[must_use]
    pub fn state(&self) -> SynapseState {
        self.state
    }

    pub fn can_send_data(&self) -> bool {
        matches!(self.state, SynapseState::Ready | SynapseState::Flowing)
    }

    pub fn set_ready(&mut self) -> bool {
        if self.state == SynapseState::Pending {
            self.state = SynapseState::Ready;
            self.last_activity = Some(Instant::now());
            true
        } else {
            false
        }
    }

    pub fn set_flowing(&mut self) -> bool {
        if self.state == SynapseState::Ready {
            self.state = SynapseState::Flowing;
            self.last_activity = Some(Instant::now());
            true
        } else {
            false
        }
    }

    pub fn close(&mut self) {
        self.state = SynapseState::Closed;
    }

    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }

    pub fn handshake_timed_out(&self) -> bool {
        if self.state != SynapseState::Pending {
            return false;
        }
        self.last_activity
            .map_or(false, |last| last.elapsed() > self.handshake_timeout)
    }

    pub fn set_handshake_timeout(&mut self, timeout: Duration) {
        self.handshake_timeout = timeout;
    }

}

impl std::fmt::Debug for Synapse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Synapse")
            .field("id", &self.id)
            .field("graph_id", &self.graph_id)
            .field("src_node", &self.src_node)
            .field("dst_node", &self.dst_node)
            .field("state", &self.state)
            .finish()
    }
}

/// Synapse manager for tracking multiple synapses.
#[derive(Debug, Default)]
pub struct SynapseManager {
    next_id: AtomicU64,
    synapses: DashMap<u64, Synapse>,
}

impl SynapseManager {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            synapses: DashMap::new(),
        }
    }

    pub fn create_synapse(&self, graph_id: u64, src_node: u64, dst_node: u64) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let synapse = Synapse::new(id, graph_id, src_node, dst_node);
        self.synapses.insert(id, synapse);
        id
    }

    pub fn get_synapse_state(&self, id: u64) -> Option<SynapseState> {
        self.synapses.get(&id).map(|r| r.state())
    }

    pub fn create_handshake_packet(&self, synapse_id: u64) -> Option<(u64, u64, u64, u32, u32)> {
        let entry = self.synapses.get_mut(&synapse_id)?;
        let graph_id = entry.graph_id;
        let dst_node = entry.dst_node;
        let sequence = entry.next_sequence();
        Some((
            graph_id,
            dst_node,
            sequence,
            SynapseFlag::Handshake.to_u32(),
            0,
        ))
    }

    pub fn handle_handshake_ack(&self, synapse_id: u64) -> bool {
        self.synapses
            .get_mut(&synapse_id)
            .map(|mut s| s.set_ready())
            .unwrap_or(false)
    }

    pub fn remove_synapse(&self, id: u64) -> Option<Synapse> {
        self.synapses.remove(&id).map(|(_, v)| v)
    }

    #[must_use]
    pub fn synapse_count(&self) -> usize {
        self.synapses.len()
    }

    pub fn cleanup_timed_out(&self) -> Vec<u64> {
        let timed_out: Vec<u64> = self
            .synapses
            .iter()
            .filter(|r| r.handshake_timed_out())
            .map(|r| *r.key())
            .collect();

        for id in &timed_out {
            self.synapses.remove(id);
        }
        timed_out
    }

    pub fn can_send_data(&self, synapse_id: u64) -> bool {
        self.synapses
            .get(&synapse_id)
            .map_or(false, |s| s.can_send_data())
    }

    pub fn start_flowing(&self, synapse_id: u64) -> bool {
        self.synapses
            .get_mut(&synapse_id)
            .map_or(false, |mut s| s.set_flowing())
    }

    pub fn close_synapse(&self, synapse_id: u64) {
        if let Some(mut s) = self.synapses.get_mut(&synapse_id) {
            s.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synapse_state_transitions() {
        let mut synapse = Synapse::new(1, 42, 100, 200);
        assert_eq!(synapse.state(), SynapseState::Pending);
        assert!(!synapse.can_send_data());

        assert!(synapse.set_ready());
        assert_eq!(synapse.state(), SynapseState::Ready);
        assert!(synapse.can_send_data());

        assert!(synapse.set_flowing());
        assert_eq!(synapse.state(), SynapseState::Flowing);

        synapse.close();
        assert_eq!(synapse.state(), SynapseState::Closed);
    }

    #[test]
    fn test_synapse_flag_conversion() {
        assert_eq!(SynapseFlag::Handshake.to_u32(), 0x01);
        assert_eq!(SynapseFlag::HandshakeAck.to_u32(), 0x02);
        assert_eq!(SynapseFlag::Data.to_u32(), 0x04);
        assert_eq!(SynapseFlag::from_u32(0x01), Some(SynapseFlag::Handshake));
    }

    #[test]
    fn test_synapse_manager() {
        let manager = SynapseManager::new();
        let id = manager.create_synapse(42, 100, 200);

        assert_eq!(manager.get_synapse_state(id), Some(SynapseState::Pending));
        assert!(manager.handle_handshake_ack(id));
        assert_eq!(manager.get_synapse_state(id), Some(SynapseState::Ready));
    }

    #[test]
    fn test_cannot_send_data_in_pending() {
        let manager = SynapseManager::new();
        let id = manager.create_synapse(42, 100, 200);
        assert!(!manager.can_send_data(id));
    }

    #[test]
    fn test_can_send_data_after_handshake() {
        let manager = SynapseManager::new();
        let id = manager.create_synapse(42, 100, 200);
        manager.handle_handshake_ack(id);
        assert!(manager.can_send_data(id));
    }

    #[test]
    fn test_create_handshake_packet() {
        let manager = SynapseManager::new();
        let id = manager.create_synapse(42, 100, 200);
        let result = manager.create_handshake_packet(id);

        assert!(result.is_some());
        let (graph_id, dst, _sequence, flags, payload_size) = result.unwrap();
        assert_eq!(graph_id, 42);
        assert_eq!(dst, 200);
        assert_eq!(flags, 0x01);
        assert_eq!(payload_size, 0);
    }

    #[test]
    fn test_start_flowing() {
        let manager = SynapseManager::new();
        let id = manager.create_synapse(42, 100, 200);

        assert!(!manager.start_flowing(id));
        manager.handle_handshake_ack(id);
        assert!(manager.start_flowing(id));
        assert_eq!(manager.get_synapse_state(id), Some(SynapseState::Flowing));
    }
}
