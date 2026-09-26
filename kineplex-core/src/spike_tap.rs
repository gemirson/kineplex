//! Lossy, header-only observation channel for active synapses.

use std::sync::OnceLock;

use tokio::sync::broadcast;

const TAP_CAPACITY: usize = 1024;
static GLOBAL_TAP: OnceLock<SpikeTap> = OnceLock::new();

/// Metadata and copied 32-byte header for one transmitted spike.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TappedSpike {
    pub graph_id: u64,
    pub synapse_id: u64,
    pub sequence: u64,
    pub flags: u32,
    pub header: [u8; 32],
}

/// Process-wide bounded tap. Publication never waits for a subscriber.
pub struct SpikeTap {
    sender: broadcast::Sender<TappedSpike>,
}

impl SpikeTap {
    fn new() -> Self {
        let (sender, _receiver) = broadcast::channel(TAP_CAPACITY);
        Self { sender }
    }

    /// Returns the process-wide tap shared by egress and Control Plane subscribers.
    #[must_use]
    pub fn global() -> &'static Self {
        GLOBAL_TAP.get_or_init(Self::new)
    }

    /// Subscribes without adding backpressure to the Data Plane.
    #[must_use]
    pub fn subscribe(&self) -> TapSubscription {
        TapSubscription {
            receiver: self.sender.subscribe(),
        }
    }

    /// Copies a valid 32-byte KinePlex header into the bounded observation channel.
    /// Returns false for malformed headers or when there are no active subscribers.
    pub fn publish_header(&self, header: &[u8]) -> bool {
        let Ok(header) = <&[u8; 32]>::try_from(header) else {
            return false;
        };
        let read_u64 = |start: usize| {
            u64::from_le_bytes(header[start..start + 8].try_into().unwrap_or([0; 8]))
        };
        let read_u32 = |start: usize| {
            u32::from_le_bytes(header[start..start + 4].try_into().unwrap_or([0; 4]))
        };
        self.sender
            .send(TappedSpike {
                graph_id: read_u64(0),
                synapse_id: read_u64(8),
                sequence: read_u64(16),
                flags: read_u32(24),
                header: *header,
            })
            .is_ok()
    }
}

/// Receiver for a header-only tapping subscription.
pub struct TapSubscription {
    receiver: broadcast::Receiver<TappedSpike>,
}

impl TapSubscription {
    /// Receives an event, skipping lag notifications. Slow receivers lose old events.
    pub async fn recv(&mut self) -> Option<TappedSpike> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SpikeTap;

    #[tokio::test]
    async fn publishes_only_header_metadata_to_subscribers() {
        let tap = SpikeTap::global();
        let mut subscription = tap.subscribe();
        let mut header = [0_u8; 32];
        header[0..8].copy_from_slice(&7_u64.to_le_bytes());
        header[8..16].copy_from_slice(&9_u64.to_le_bytes());
        header[16..24].copy_from_slice(&11_u64.to_le_bytes());
        header[24..28].copy_from_slice(&4_u32.to_le_bytes());
        assert!(tap.publish_header(&header));
        let event = subscription.recv().await.expect("tap event arrives");
        assert_eq!(
            (
                event.graph_id,
                event.synapse_id,
                event.sequence,
                event.flags
            ),
            (7, 9, 11, 4)
        );
        assert_eq!(event.header, header);
    }
}
