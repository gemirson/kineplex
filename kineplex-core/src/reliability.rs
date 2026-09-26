//! Bounded at-least-once resend storage for spikes spanning route transitions.

use std::collections::VecDeque;

use crate::packet::OwnedSynapsePacket;

/// One queued spike retained until a cumulative sequence acknowledgement.
#[derive(Debug)]
struct PendingSpike {
    sequence: u64,
    packet: OwnedSynapsePacket,
}

/// Per-synapse resend queue that survives changes to the active route snapshot.
#[derive(Debug)]
pub struct ReliableSpikeQueue {
    capacity: usize,
    acknowledged_through: Option<u64>,
    pending: VecDeque<PendingSpike>,
}

impl ReliableSpikeQueue {
    /// Creates a bounded resend queue.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            acknowledged_through: None,
            pending: VecDeque::with_capacity(capacity),
        }
    }

    /// Retains an owned packet and returns its sequence number.
    pub fn enqueue(&mut self, packet: OwnedSynapsePacket) -> Result<u64, ReliableQueueError> {
        if self.pending.len() >= self.capacity {
            return Err(ReliableQueueError::Full);
        }
        let sequence = packet
            .as_packet()
            .sequence()
            .ok_or(ReliableQueueError::InvalidHeader)?;
        if self
            .acknowledged_through
            .map_or(false, |acked| sequence <= acked)
            || self
                .pending
                .iter()
                .any(|pending| pending.sequence == sequence)
        {
            return Err(ReliableQueueError::DuplicateOrStaleSequence(sequence));
        }
        if self
            .pending
            .back()
            .is_some_and(|previous| sequence <= previous.sequence)
        {
            return Err(ReliableQueueError::NonMonotonicSequence(sequence));
        }
        self.pending.push_back(PendingSpike { sequence, packet });
        Ok(sequence)
    }

    /// Acknowledges all packets up to and including `sequence`.
    pub fn acknowledge(&mut self, sequence: u64) -> Result<usize, ReliableQueueError> {
        if self
            .acknowledged_through
            .map_or(false, |previous| sequence < previous)
        {
            return Err(ReliableQueueError::NonMonotonicAcknowledgement(sequence));
        }
        let before = self.pending.len();
        while self
            .pending
            .front()
            .is_some_and(|pending| pending.sequence <= sequence)
        {
            self.pending.pop_front();
        }
        self.acknowledged_through = Some(sequence);
        Ok(before - self.pending.len())
    }

    /// Returns borrowed packets for retransmission without copying their payloads.
    pub fn pending_packets(&self) -> impl Iterator<Item = (u64, &OwnedSynapsePacket)> {
        self.pending
            .iter()
            .map(|pending| (pending.sequence, &pending.packet))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReliableQueueError {
    Full,
    InvalidHeader,
    DuplicateOrStaleSequence(u64),
    NonMonotonicSequence(u64),
    NonMonotonicAcknowledgement(u64),
}

impl std::fmt::Display for ReliableQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => f.write_str("reliable spike resend queue is full"),
            Self::InvalidHeader => f.write_str("spike header has no valid sequence number"),
            Self::DuplicateOrStaleSequence(sequence) => write!(
                f,
                "spike sequence {sequence} is already acknowledged or queued"
            ),
            Self::NonMonotonicSequence(sequence) => {
                write!(f, "spike sequence {sequence} is not monotonic")
            }
            Self::NonMonotonicAcknowledgement(sequence) => {
                write!(f, "acknowledgement {sequence} moved backwards")
            }
        }
    }
}

impl std::error::Error for ReliableQueueError {}

#[cfg(test)]
mod tests {
    use super::ReliableSpikeQueue;
    use crate::packet::OwnedSynapsePacket;

    fn packet(sequence: u64) -> OwnedSynapsePacket {
        let mut header = vec![0_u8; 32];
        header[16..24].copy_from_slice(&sequence.to_le_bytes());
        OwnedSynapsePacket::new(header, vec![1, 2, 3])
    }

    #[test]
    fn packets_remain_owned_until_cumulative_ack() {
        let mut queue = ReliableSpikeQueue::new(3);
        queue.enqueue(packet(1)).expect("first packet is retained");
        queue.enqueue(packet(2)).expect("second packet is retained");
        assert_eq!(
            queue
                .pending_packets()
                .map(|(sequence, _)| sequence)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(queue.acknowledge(1).expect("ack accepted"), 1);
        assert_eq!(
            queue.pending_packets().next().map(|(sequence, _)| sequence),
            Some(2)
        );
    }
}
