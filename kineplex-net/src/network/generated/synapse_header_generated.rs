// Fallback generated shape for environments without flatc.
// The normal build path is produced by flatc-rust from synapse_header.fbs.

pub mod kineplex {
    pub mod net {
        use flatbuffers::{EndianScalar, Follow, Push};

        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[repr(transparent)]
        pub struct SynapseFlag(pub u8);

        impl SynapseFlag {
            pub const None: Self = Self(0);
            pub const Handshake: Self = Self(1);
            pub const Backpressure: Self = Self(2);
            pub const EOF: Self = Self(4);
        }

        impl Follow<'_> for SynapseFlag {
            type Inner = Self;
            unsafe fn follow(buf: &[u8], loc: usize) -> Self::Inner {
                Self(flatbuffers::read_scalar_at::<u8>(buf, loc))
            }
        }

        impl Push for SynapseFlag {
            type Output = Self;
            unsafe fn push(&self, dst: &mut [u8], _written_len: usize) {
                flatbuffers::emplace_scalar::<u8>(dst, self.0);
            }
        }

        impl EndianScalar for SynapseFlag {
            type Scalar = u8;
            fn to_little_endian(self) -> u8 { self.0 }
            fn from_little_endian(v: u8) -> Self { Self(v) }
        }

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct SynapseHeader<'a> { pub _tab: flatbuffers::Table<'a> }

        impl<'a> Follow<'a> for SynapseHeader<'a> {
            type Inner = Self;
            unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
                Self { _tab: flatbuffers::Table::new(buf, loc) }
            }
        }

        impl<'a> SynapseHeader<'a> {
            pub const VT_GRAPH_ID: flatbuffers::VOffsetT = 4;
            pub const VT_SYNAPSE_ID: flatbuffers::VOffsetT = 6;
            pub const VT_SEQ_NUM: flatbuffers::VOffsetT = 8;
            pub const VT_FLAGS: flatbuffers::VOffsetT = 10;
            pub const VT_PAYLOAD_SIZE: flatbuffers::VOffsetT = 12;

            pub fn graph_id(&self) -> u64 { unsafe { self._tab.get::<u64>(Self::VT_GRAPH_ID, Some(0)).unwrap() } }
            pub fn synapse_id(&self) -> u64 { unsafe { self._tab.get::<u64>(Self::VT_SYNAPSE_ID, Some(0)).unwrap() } }
            pub fn seq_num(&self) -> u64 { unsafe { self._tab.get::<u64>(Self::VT_SEQ_NUM, Some(0)).unwrap() } }
            pub fn flags(&self) -> SynapseFlag { unsafe { self._tab.get::<SynapseFlag>(Self::VT_FLAGS, Some(SynapseFlag::None)).unwrap() } }
            pub fn payload_size(&self) -> u32 { unsafe { self._tab.get::<u32>(Self::VT_PAYLOAD_SIZE, Some(0)).unwrap() } }
        }

        pub struct SynapseHeaderArgs {
            pub graph_id: u64,
            pub synapse_id: u64,
            pub seq_num: u64,
            pub flags: SynapseFlag,
            pub payload_size: u32,
        }

        impl Default for SynapseHeaderArgs {
            fn default() -> Self {
                Self { graph_id: 0, synapse_id: 0, seq_num: 0, flags: SynapseFlag::None, payload_size: 0 }
            }
        }

        pub struct SynapseHeaderBuilder<'a: 'b, 'b> {
            fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
            start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
        }

        impl<'a: 'b, 'b> SynapseHeaderBuilder<'a, 'b> {
            pub fn add_graph_id(&mut self, value: u64) { self.fbb_.push_slot(Self::VT_GRAPH_ID, value, 0); }
            pub fn add_synapse_id(&mut self, value: u64) { self.fbb_.push_slot(Self::VT_SYNAPSE_ID, value, 0); }
            pub fn add_seq_num(&mut self, value: u64) { self.fbb_.push_slot(Self::VT_SEQ_NUM, value, 0); }
            pub fn add_flags(&mut self, value: SynapseFlag) { self.fbb_.push_slot(Self::VT_FLAGS, value, SynapseFlag::None); }
            pub fn add_payload_size(&mut self, value: u32) { self.fbb_.push_slot(Self::VT_PAYLOAD_SIZE, value, 0); }
            pub fn new(fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> Self { Self { start_: fbb.start_table(), fbb_ : fbb } }
            pub fn finish(self) -> flatbuffers::WIPOffset<SynapseHeader<'a>> { flatbuffers::WIPOffset::new(self.fbb_.end_table(self.start_).value()) }
            const VT_GRAPH_ID: flatbuffers::VOffsetT = SynapseHeader::VT_GRAPH_ID;
            const VT_SYNAPSE_ID: flatbuffers::VOffsetT = SynapseHeader::VT_SYNAPSE_ID;
            const VT_SEQ_NUM: flatbuffers::VOffsetT = SynapseHeader::VT_SEQ_NUM;
            const VT_FLAGS: flatbuffers::VOffsetT = SynapseHeader::VT_FLAGS;
            const VT_PAYLOAD_SIZE: flatbuffers::VOffsetT = SynapseHeader::VT_PAYLOAD_SIZE;
        }

        pub fn create_synapse_header<'a: 'b, 'b>(fbb: &'a mut flatbuffers::FlatBufferBuilder<'b>, args: &SynapseHeaderArgs) -> flatbuffers::WIPOffset<SynapseHeader<'b>> {
            let start = fbb.start_table();
            fbb.push_slot(SynapseHeader::VT_PAYLOAD_SIZE, args.payload_size, 0);
            fbb.push_slot(SynapseHeader::VT_FLAGS, args.flags, SynapseFlag::None);
            fbb.push_slot(SynapseHeader::VT_SEQ_NUM, args.seq_num, 0);
            fbb.push_slot(SynapseHeader::VT_SYNAPSE_ID, args.synapse_id, 0);
            fbb.push_slot(SynapseHeader::VT_GRAPH_ID, args.graph_id, 0);
            flatbuffers::WIPOffset::new(fbb.end_table(start).value())
        }

        pub unsafe fn root_as_synapse_header_unchecked<'a>(buf: &'a [u8]) -> SynapseHeader<'a> { flatbuffers::root_unchecked::<SynapseHeader<'a>>(buf) }
        pub const SYNAPSE_HEADER_IDENTIFIER: &str = "KPXH";
        pub fn synapse_header_buffer_has_identifier(buf: &[u8]) -> bool { flatbuffers::buffer_has_identifier(buf, SYNAPSE_HEADER_IDENTIFIER, false) }
    }
}
