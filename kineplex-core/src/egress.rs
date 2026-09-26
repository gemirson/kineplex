//! Arrow IPC egress packet construction for a synapse.

use std::error::Error;
use std::fmt::{Display, Formatter};

use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;

use crate::flatbuffers::HeaderBuffer;
use crate::geometry::{AtomicGeodesicTable, RouteKey};
use crate::packet::OwnedSynapsePacket;
use crate::synapse::Synapse;

/// Builds the 32-byte synapse header using the next monotonic sequence number.
pub fn build_synapse_header(
    synapse: &Synapse,
    flags: u32,
    payload_size: usize,
) -> Result<(Vec<u8>, u64), EgressError> {
    let payload_size = u32::try_from(payload_size).map_err(|_| EgressError::PayloadTooLarge)?;
    let sequence = synapse.next_sequence();
    let mut header = HeaderBuffer::new();
    header.write_header(synapse.graph_id, synapse.id, sequence, flags, payload_size);
    let header = header.buffer().to_vec();
    let _published = crate::spike_tap::SpikeTap::global().publish_header(&header);
    Ok((header, sequence))
}

/// Serializes an Arrow batch to IPC and couples it to the updated synapse header.
pub fn build_outbound_spike(
    synapse: &Synapse,
    batch: &RecordBatch,
    flags: u32,
) -> Result<OutboundSpike, EgressError> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, batch.schema().as_ref())
            .map_err(|error| EgressError::Arrow(error.to_string()))?;
        writer
            .write(batch)
            .map_err(|error| EgressError::Arrow(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| EgressError::Arrow(error.to_string()))?;
    }
    crate::metrics::global().record_spike(payload.len());
    let (header, sequence) = build_synapse_header(synapse, flags, payload.len())?;
    Ok(OutboundSpike {
        packet: OwnedSynapsePacket::new(header, payload),
        sequence,
    })
}

/// Egress packet paired with the next hop selected by the active geodesic snapshot.
#[derive(Debug)]
pub struct RoutedOutboundSpike {
    pub next_node: u64,
    pub resistance: f64,
    pub spike: OutboundSpike,
}

/// Builds an Arrow packet and chooses its next hop using one atomic route lookup.
pub fn build_routed_spike(
    routes: &AtomicGeodesicTable,
    edge: RouteKey,
    synapse: &Synapse,
    batch: &RecordBatch,
    flags: u32,
) -> Result<RoutedOutboundSpike, EgressError> {
    let route = routes
        .lookup(edge)
        .ok_or(EgressError::RouteUnavailable(edge))?;
    let spike = build_outbound_spike(synapse, batch, flags)?;
    Ok(RoutedOutboundSpike {
        next_node: route.next_node,
        resistance: route.resistance,
        spike,
    })
}

/// Owns both buffers until a downstream transport reports completion.
#[derive(Debug)]
pub struct OutboundSpike {
    packet: OwnedSynapsePacket,
    sequence: u64,
}

impl OutboundSpike {
    /// Monotonic sequence number encoded in the header.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Borrowed header bytes. Keep this owner alive through transport completion.
    #[must_use]
    pub fn header(&self) -> &[u8] {
        self.packet.header()
    }

    /// Borrowed Arrow IPC payload. Keep this owner alive through transport completion.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.packet.payload()
    }

    /// Transfers both buffers to a transport completion owner.
    #[must_use]
    pub fn into_packet(self) -> OwnedSynapsePacket {
        self.packet
    }

    /// Transfers the sequence and owned network buffers to a pool-backed sender.
    #[must_use]
    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>, u64) {
        let (header, payload) = self.packet.into_parts();
        (header, payload, self.sequence)
    }
}

/// Error while serializing or framing an outgoing Arrow batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EgressError {
    PayloadTooLarge,
    Arrow(String),
    RouteUnavailable(RouteKey),
}

impl Display for EgressError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadTooLarge => {
                formatter.write_str("Arrow IPC payload exceeds u32 framing limit")
            }
            Self::Arrow(error) => write!(formatter, "Arrow IPC serialization failed: {error}"),
            Self::RouteUnavailable(edge) => {
                write!(formatter, "no active geodesic route exists for edge {edge}")
            }
        }
    }
}

impl Error for EgressError {}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Float32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;

    use super::{build_outbound_spike, build_routed_spike};
    use crate::geometry::{AtomicGeodesicTable, RouteEntry, RouteSnapshot};
    use crate::synapse::Synapse;

    #[test]
    fn egress_serializes_batch_and_increments_synapse_sequence() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Float32Array::from(vec![1.0, 2.0]))])
                .expect("batch is valid");
        let synapse = Synapse::new(10, 3, 1, 2);
        let first = build_outbound_spike(&synapse, &batch, 4).expect("packet builds");
        let second = build_outbound_spike(&synapse, &batch, 4).expect("packet builds");
        assert_eq!(first.sequence(), 0);
        assert_eq!(second.sequence(), 1);
        let mut reader =
            StreamReader::try_new(Cursor::new(first.payload()), None).expect("Arrow stream opens");
        assert_eq!(
            reader
                .next()
                .expect("batch exists")
                .expect("valid batch")
                .num_rows(),
            2
        );
    }

    #[test]
    fn routed_egress_uses_the_active_geodesic_next_hop() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Float32Array::from(vec![3.0]))])
            .expect("batch is valid");
        let routes = AtomicGeodesicTable::new(RouteSnapshot {
            revision: 4,
            routes: [(
                8,
                RouteEntry {
                    next_node: 12,
                    resistance: 0.25,
                },
            )]
            .into_iter()
            .collect(),
        });
        let routed = build_routed_spike(&routes, 8, &Synapse::new(1, 2, 3, 4), &batch, 4)
            .expect("active route exists");
        assert_eq!(routed.next_node, 12);
        assert_eq!(routed.resistance, 0.25);
        assert!(!routed.spike.payload().is_empty());
    }
}
