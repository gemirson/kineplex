//! SynapsePacket: Header + Arrow Payload coupling module.
//!
//! Provides zero-copy coupling between FlatBuffers header and Arrow IPC payload.
//! The header contains metadata needed to route and parse the Arrow data.

use crate::flatbuffers::HeaderBuffer;

/// Maximum Arrow payload size (64KB default)
const MAX_ARROW_PAYLOAD_SIZE: usize = 65536;

/// A packet containing header metadata and Arrow payload data.
///
/// This struct holds references to the header and payload without owning them,
/// enabling zero-copy network transmission.
#[derive(Debug, Clone)]
pub struct SynapsePacket<'a> {
    /// Reference to the serialized header (FlatBuffers format)
    header: &'a [u8],
    /// Reference to the Arrow IPC payload
    payload: &'a [u8],
}

impl<'a> SynapsePacket<'a> {
    /// Creates a new SynapsePacket from existing slices.
    ///
    /// # Arguments
    /// * `header` - Serialized header bytes (32 bytes typically)
    /// * `payload` - Arrow IPC payload bytes
    ///
    /// # Returns
    /// A new SynapsePacket without taking ownership
    pub fn new(header: &'a [u8], payload: &'a [u8]) -> Self {
        Self { header, payload }
    }

    /// Gets the header slice.
    #[must_use]
    pub fn header(&self) -> &[u8] {
        self.header
    }

    /// Gets the payload slice.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload
    }

    /// Gets the total packet size (header + payload).
    #[must_use]
    pub fn total_size(&self) -> usize {
        self.header.len() + self.payload.len()
    }

    /// Gets just the payload size from the header.
    ///
    /// The payload size is stored at bytes 28-31 of the header (little-endian u32).
    /// Returns `None` if the header is too short.
    pub fn payload_size_from_header(&self) -> Option<u32> {
        if self.header.len() < 32 {
            return None;
        }
        let size = u32::from_le_bytes([
            self.header[28],
            self.header[29],
            self.header[30],
            self.header[31],
        ]);
        Some(size)
    }

    /// Gets the graph ID from the header.
    pub fn graph_id(&self) -> Option<u64> {
        if self.header.len() < 8 {
            return None;
        }
        let id = u64::from_le_bytes([
            self.header[0],
            self.header[1],
            self.header[2],
            self.header[3],
            self.header[4],
            self.header[5],
            self.header[6],
            self.header[7],
        ]);
        Some(id)
    }

    /// Gets the synapse ID from the header.
    pub fn synapse_id(&self) -> Option<u64> {
        if self.header.len() < 16 {
            return None;
        }
        let id = u64::from_le_bytes([
            self.header[8],
            self.header[9],
            self.header[10],
            self.header[11],
            self.header[12],
            self.header[13],
            self.header[14],
            self.header[15],
        ]);
        Some(id)
    }

    /// Gets the sequence number from the header.
    pub fn sequence(&self) -> Option<u64> {
        if self.header.len() < 24 {
            return None;
        }
        let seq = u64::from_le_bytes([
            self.header[16],
            self.header[17],
            self.header[18],
            self.header[19],
            self.header[20],
            self.header[21],
            self.header[22],
            self.header[23],
        ]);
        Some(seq)
    }

    /// Gets the flags from the header.
    pub fn flags(&self) -> Option<u32> {
        if self.header.len() < 28 {
            return None;
        }
        let flags = u32::from_le_bytes([
            self.header[24],
            self.header[25],
            self.header[26],
            self.header[27],
        ]);
        Some(flags)
    }
}

/// A builder for creating SynapsePackets with managed buffers.
///
/// This enables building packets with local buffers that are then
/// referenced by the packet without copying.
pub struct SynapsePacketBuilder {
    header_buffer: HeaderBuffer,
    payload_buffer: Vec<u8>,
}

impl SynapsePacketBuilder {
    /// Creates a new packet builder.
    pub fn new() -> Self {
        Self {
            header_buffer: HeaderBuffer::new(),
            payload_buffer: Vec::with_capacity(MAX_ARROW_PAYLOAD_SIZE),
        }
    }

    /// Sets the header parameters and builds the header.
    pub fn with_header(
        &mut self,
        graph_id: u64,
        synapse_id: u64,
        sequence: u64,
        flags: u32,
        arrow_payload_size: u32,
    ) -> &mut Self {
        self.header_buffer
            .write_header(graph_id, synapse_id, sequence, flags, arrow_payload_size);
        self
    }

    /// Sets the Arrow payload data.
    pub fn with_payload(&mut self, payload: &[u8]) -> &mut Self {
        self.payload_buffer.clear();
        self.payload_buffer.extend_from_slice(payload);
        self
    }

    /// Builds the SynapsePacket.
    ///
    /// Returns a packet with references to the internal buffers.
    /// The packet is valid as long as the builder is not modified or dropped.
    pub fn build(&self) -> SynapsePacket<'_> {
        SynapsePacket::new(self.header_buffer.buffer(), &self.payload_buffer)
    }

    /// Builds and consumes the packet, returning owned data.
    ///
    /// This transfers ownership of the header and payload data.
    pub fn build_owned(self) -> OwnedSynapsePacket {
        OwnedSynapsePacket {
            header: self.header_buffer.buffer().to_vec(),
            payload: self.payload_buffer,
        }
    }

    /// Resets the builder for reuse.
    pub fn reset(&mut self) {
        self.header_buffer.reset();
        self.payload_buffer.clear();
    }
}

impl Default for SynapsePacketBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// An owned SynapsePacket that owns its data.
#[derive(Debug, Clone)]
pub struct OwnedSynapsePacket {
    /// Owned header bytes
    header: Vec<u8>,
    /// Owned payload bytes
    payload: Vec<u8>,
}

impl OwnedSynapsePacket {
    /// Creates a new owned packet.
    pub fn new(header: Vec<u8>, payload: Vec<u8>) -> Self {
        Self { header, payload }
    }

    /// Gets a reference to the packet.
    pub fn as_packet(&self) -> SynapsePacket<'_> {
        SynapsePacket::new(&self.header, &self.payload)
    }

    /// Gets the header slice.
    #[must_use]
    pub fn header(&self) -> &[u8] {
        &self.header
    }

    /// Gets the payload slice.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Gets the total packet size.
    #[must_use]
    pub fn total_size(&self) -> usize {
        self.header.len() + self.payload.len()
    }

    /// Extracts the header and payload components.
    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>) {
        (self.header, self.payload)
    }
}

/// A receiver that handles framing of incoming packets.
///
/// Reads the header first, then uses the payload_size to extract
/// the correct number of bytes for the Arrow payload.
pub struct PacketReceiver {
    buffer: Vec<u8>,
    header_size: usize,
}

impl PacketReceiver {
    /// Creates a new packet receiver.
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(MAX_ARROW_PAYLOAD_SIZE + 64),
            header_size: 32, // Default header size
        }
    }

    /// Sets the expected header size.
    pub fn with_header_size(mut self, size: usize) -> Self {
        self.header_size = size;
        self
    }

    /// Processes incoming data and extracts complete packets.
    ///
    /// This method consumes owned data to avoid borrow conflicts.
    ///
    /// # Arguments
    /// * `data` - Incoming bytes from the network
    ///
    /// # Returns
    /// A vector of complete packets found in the data
    pub fn receive(&mut self, data: &[u8]) -> Vec<OwnedSynapsePacket> {
        // Append data to our buffer
        self.buffer.extend_from_slice(data);

        let mut packets = Vec::new();

        // Process all complete packets in the buffer
        while self.buffer.len() >= self.header_size {
            // Read payload size from header (bytes 28-31)
            if self.buffer.len() < 32 {
                break;
            }

            let payload_size = u32::from_le_bytes([
                self.buffer[28],
                self.buffer[29],
                self.buffer[30],
                self.buffer[31],
            ]) as usize;
            if payload_size == 0 {
                break;
            }

            let total_needed = self.header_size + payload_size;

            // Check if we have enough data for a complete packet
            if self.buffer.len() < total_needed {
                break;
            }

            // Extract owned copies of the packet data
            let header = self.buffer[..self.header_size].to_vec();
            let payload = self.buffer[self.header_size..total_needed].to_vec();

            packets.push(OwnedSynapsePacket::new(header, payload));

            // Remove the processed packet from the buffer
            self.buffer.drain(..total_needed);
        }

        packets
    }

    /// Gets the current buffer size.
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Clears the receive buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for PacketReceiver {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata extracted from a packet header.
#[derive(Debug, Clone)]
pub struct PacketMetadata {
    pub graph_id: u64,
    pub synapse_id: u64,
    pub sequence: u64,
    pub flags: u32,
    pub payload_size: u32,
}

impl TryFrom<&SynapsePacket<'_>> for PacketMetadata {
    type Error = &'static str;

    fn try_from(packet: &SynapsePacket<'_>) -> Result<Self, Self::Error> {
        Ok(PacketMetadata {
            graph_id: packet.graph_id().ok_or("Invalid graph_id")?,
            synapse_id: packet.synapse_id().ok_or("Invalid synapse_id")?,
            sequence: packet.sequence().ok_or("Invalid sequence")?,
            flags: packet.flags().ok_or("Invalid flags")?,
            payload_size: packet
                .payload_size_from_header()
                .ok_or("Invalid payload_size")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synapse_packet_creation() {
        let header = vec![0u8; 32];
        let payload = vec![1u8; 1024];

        let packet = SynapsePacket::new(&header, &payload);

        assert_eq!(packet.header().len(), 32);
        assert_eq!(packet.payload().len(), 1024);
        assert_eq!(packet.total_size(), 1056);
    }

    #[test]
    fn test_header_field_extraction() {
        let mut builder = SynapsePacketBuilder::new();
        builder.with_header(42, 100, 999, 1, 2048);

        let packet = builder.build();

        assert_eq!(packet.graph_id(), Some(42));
        assert_eq!(packet.synapse_id(), Some(100));
        assert_eq!(packet.sequence(), Some(999));
        assert_eq!(packet.flags(), Some(1));
        assert_eq!(packet.payload_size_from_header(), Some(2048));
    }

    #[test]
    fn test_packet_builder() {
        let mut builder = SynapsePacketBuilder::new();
        builder
            .with_header(1, 100, 1, 0, 1024)
            .with_payload(&[2u8; 1024]);

        let packet = builder.build();

        assert_eq!(packet.total_size(), 1056);
    }

    #[test]
    fn test_packet_receiver() {
        let mut receiver = PacketReceiver::new();

        // Create a complete packet
        let mut packet_data = vec![0u8; 32];
        // Set payload size at bytes 28-31
        packet_data[28] = 64; // payload size = 64
        packet_data.extend_from_slice(&[1u8; 64]); // payload

        let packets = receiver.receive(&packet_data);

        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].payload().len(), 64);
    }

    #[test]
    fn test_packet_receiver_partial_data() {
        let mut receiver = PacketReceiver::new();

        // Send only header first
        let header = vec![0u8; 32];
        let packets = receiver.receive(&header);

        assert!(packets.is_empty());
        assert_eq!(receiver.buffer_size(), 32);
    }

    #[test]
    fn test_packet_receiver_multiple_packets() {
        let mut receiver = PacketReceiver::new();

        // Create two complete packets
        let mut packet1 = vec![0u8; 32];
        packet1[28] = 32;
        packet1.extend_from_slice(&[1u8; 32]);

        let mut packet2 = vec![0u8; 32];
        packet2[28] = 48;
        packet2.extend_from_slice(&[2u8; 48]);

        // Send both together
        let mut combined = packet1;
        combined.extend_from_slice(&packet2);

        let packets = receiver.receive(&combined);

        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].payload().len(), 32);
        assert_eq!(packets[1].payload().len(), 48);
    }

    #[test]
    fn test_packet_metadata() {
        let mut builder = SynapsePacketBuilder::new();
        builder.with_header(42, 100, 999, 1, 2048);
        let packet = builder.build();

        let metadata = PacketMetadata::try_from(&packet).expect("Failed to extract metadata");

        assert_eq!(metadata.graph_id, 42);
        assert_eq!(metadata.synapse_id, 100);
        assert_eq!(metadata.sequence, 999);
        assert_eq!(metadata.flags, 1);
        assert_eq!(metadata.payload_size, 2048);
    }

    #[test]
    fn test_owned_packet() {
        let mut builder = SynapsePacketBuilder::new();
        builder
            .with_header(1, 100, 1, 0, 1024)
            .with_payload(&[2u8; 1024]);

        let owned = builder.build_owned();

        assert_eq!(owned.total_size(), 1056);

        let packet = owned.as_packet();
        assert_eq!(packet.total_size(), 1056);
    }

    #[test]
    fn test_loopback_integration() {
        // Create a packet with header and payload
        let mut builder = SynapsePacketBuilder::new();
        builder
            .with_header(1, 100, 1, 0, 64)
            .with_payload(&[0x41u8; 64]); // "A" bytes for Arrow-like payload

        let owned_packet = builder.build_owned();
        let total_size = owned_packet.total_size();

        // Simulate sending: serialize to bytes
        let (header, payload) = owned_packet.into_parts();
        let mut wire_data = header.clone();
        wire_data.extend_from_slice(&payload);

        // Simulate receiving: use PacketReceiver
        let mut receiver = PacketReceiver::new();
        let received = receiver.receive(&wire_data);

        assert_eq!(received.len(), 1);

        let received_packet = &received[0];
        assert_eq!(received_packet.total_size(), total_size);

        // Verify header fields were preserved
        let packet_ref = received_packet.as_packet();
        assert_eq!(packet_ref.graph_id(), Some(1));
        assert_eq!(packet_ref.synapse_id(), Some(100));
        assert_eq!(packet_ref.payload_size_from_header(), Some(64));
    }
}
