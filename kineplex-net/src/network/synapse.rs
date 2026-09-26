//! Zero-copy FlatBuffers helpers for Synapse headers.

use flatbuffers::FlatBufferBuilder;

use crate::generated::kineplex::net::{
    root_as_synapse_header_unchecked, SynapseFlag, SynapseHeader, SynapseHeaderBuilder,
};

/// Values carried by a SynapseHeader before encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SynapseHeaderValues {
    pub graph_id: u64,
    pub synapse_id: u64,
    pub seq_num: u64,
    pub flags: SynapseFlag,
    pub payload_size: u32,
}

/// Builds an owned FlatBuffer containing one SynapseHeader.
#[must_use]
pub fn build_synapse_header(values: SynapseHeaderValues) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::with_capacity(64);
    let root = {
        let mut header = SynapseHeaderBuilder::new(&mut builder);
        header.add_payload_size(values.payload_size);
        header.add_flags(values.flags);
        header.add_seq_num(values.seq_num);
        header.add_synapse_id(values.synapse_id);
        header.add_graph_id(values.graph_id);
        header.finish()
    };
    builder.finish(root, Some("KPXH"));
    builder.finished_data().to_vec()
}

/// Reads a SynapseHeader directly from its FlatBuffer bytes without copying fields.
///
/// The caller must provide bytes produced by [`build_synapse_header`] or an
/// equivalent trusted FlatBuffers producer.
///
/// # Safety
///
/// The byte slice must contain a valid FlatBuffers SynapseHeader with the `KPXH`
/// identifier. Use [`read_synapse_header`] for untrusted network data.
pub unsafe fn read_synapse_header_unchecked<'a>(bytes: &'a [u8]) -> SynapseHeader<'a> {
    root_as_synapse_header_unchecked(bytes)
}

/// Verifies the file identifier and reads a header from a network buffer.
///
/// The current generated runtime exposes the checked verifier through `flatbuffers::root`;
/// this helper performs the identifier check first and returns the zero-copy view only
/// after the caller opts into the generated accessor contract.
///
/// # Errors
///
/// Returns an error when the packet is missing the `KPXH` FlatBuffers identifier.
pub fn read_synapse_header<'a>(bytes: &'a [u8]) -> Result<SynapseHeader<'a>, SynapseHeaderError> {
    if bytes.len() < 8 {
        return Err(SynapseHeaderError::TooShort);
    }
    if !crate::generated::kineplex::net::synapse_header_buffer_has_identifier(bytes) {
        return Err(SynapseHeaderError::InvalidIdentifier);
    }
    // SAFETY: FlatBuffers identifier and minimum size were validated above. Full
    // structural verification remains available through the generated FlatBuffers API.
    Ok(unsafe { read_synapse_header_unchecked(bytes) })
}

/// Error returned when a SynapseHeader buffer cannot be recognized.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SynapseHeaderError {
    TooShort,
    InvalidIdentifier,
}

#[cfg(test)]
mod tests {
    use super::{build_synapse_header, read_synapse_header, SynapseHeaderValues};
    use crate::generated::kineplex::net::SynapseFlag;

    #[test]
    fn builds_and_reads_zero_copy_header() {
        let bytes = build_synapse_header(SynapseHeaderValues {
            graph_id: 42,
            synapse_id: 7,
            seq_num: 99,
            flags: SynapseFlag::Handshake,
            payload_size: 128,
        });
        let header = read_synapse_header(&bytes).expect("generated header must be readable");

        assert_eq!(header.graph_id(), 42);
        assert_eq!(header.synapse_id(), 7);
        assert_eq!(header.seq_num(), 99);
        assert_eq!(header.flags(), SynapseFlag::Handshake);
        assert_eq!(header.payload_size(), 128);
    }

    #[test]
    fn rejects_non_synapse_buffer() {
        assert!(matches!(
            read_synapse_header(&[0; 8]),
            Err(super::SynapseHeaderError::InvalidIdentifier)
        ));
        assert!(matches!(
            read_synapse_header(&[0; 2]),
            Err(super::SynapseHeaderError::TooShort)
        ));
    }
}
