//! Zero-copy header serialization module for Neurotransmissor headers.
//!
//! Provides efficient serialization without heap allocations.
//! Uses fixed-capacity buffers for optimal performance.
//!
//! The header format is: [graph_id(8) | synapse_id(8) | sequence(8) | flags(4) | payload_size(4)] = 32 bytes

/// Size of the fixed buffer for header building (1KB should be enough for headers)
const BUFFER_SIZE: usize = 1024;

/// A pre-allocated buffer for header building.
/// This provides zero-heap-allocation serialization.
#[derive(Debug)]
pub struct HeaderBuffer {
    buffer: [u8; BUFFER_SIZE],
    size: usize,
}

impl HeaderBuffer {
    /// Creates a new header buffer with default capacity.
    pub fn new() -> Self {
        Self {
            buffer: [0u8; BUFFER_SIZE],
            size: 0,
        }
    }

    /// Gets a reference to the underlying buffer.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer[..self.size]
    }

    /// Gets the current size of data in the buffer.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Resets the buffer for reuse without reallocation.
    pub fn reset(&mut self) {
        self.size = 0;
    }

    /// Writes header data to the buffer.
    /// Returns a slice of the written bytes.
    pub fn write_header(
        &mut self,
        graph_id: u64,
        synapse_id: u64,
        sequence: u64,
        flags: u32,
        arrow_payload_size: u32,
    ) -> &[u8] {
        // Write header data as little-endian bytes
        self.buffer[0..8].copy_from_slice(&graph_id.to_le_bytes());
        self.buffer[8..16].copy_from_slice(&synapse_id.to_le_bytes());
        self.buffer[16..24].copy_from_slice(&sequence.to_le_bytes());
        self.buffer[24..28].copy_from_slice(&flags.to_le_bytes());
        self.buffer[28..32].copy_from_slice(&arrow_payload_size.to_le_bytes());

        self.size = 32;

        &self.buffer[..32]
    }

    /// Gets the buffer as a slice without ownership transfer.
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer[..self.size]
    }

    /// Gets the full underlying buffer.
    pub fn full_buffer(&self) -> &[u8] {
        &self.buffer
    }
}

impl Default for HeaderBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-local storage for HeaderBuffer reuse.
/// Provides zero-allocation header building.
pub struct ThreadLocalBuffer {
    buffer: HeaderBuffer,
}

impl ThreadLocalBuffer {
    /// Creates a new thread-local buffer.
    pub fn new() -> Self {
        Self {
            buffer: HeaderBuffer::new(),
        }
    }

    /// Gets the underlying buffer.
    pub fn buffer(&mut self) -> &mut HeaderBuffer {
        &mut self.buffer
    }

    /// Builds a header and returns the buffer (which holds the slice).
    pub fn build_header(
        &mut self,
        graph_id: u64,
        synapse_id: u64,
        sequence: u64,
        flags: u32,
        arrow_payload_size: u32,
    ) -> &HeaderBuffer {
        self.buffer
            .write_header(graph_id, synapse_id, sequence, flags, arrow_payload_size);
        &self.buffer
    }

    /// Resets the buffer for reuse.
    pub fn reset(&mut self) {
        self.buffer.reset();
    }
}

impl Default for ThreadLocalBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_buffer_creation() {
        let buffer = HeaderBuffer::new();
        assert_eq!(buffer.size(), 0);
        assert!(buffer.buffer().is_empty());
    }

    #[test]
    fn test_write_header() {
        let mut buffer = HeaderBuffer::new();
        let slice = buffer.write_header(42, 100, 999, 1, 2048);

        assert_eq!(slice.len(), 32);

        // Verify the data is correct
        let graph_id = u64::from_le_bytes([
            slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
        ]);
        assert_eq!(graph_id, 42);

        let synapse_id = u64::from_le_bytes([
            slice[8], slice[9], slice[10], slice[11], slice[12], slice[13], slice[14], slice[15],
        ]);
        assert_eq!(synapse_id, 100);
    }

    #[test]
    fn test_different_headers_produce_different_bytes() {
        let mut buffer = HeaderBuffer::new();
        let header1 = buffer.write_header(1, 100, 1, 0, 1024);

        buffer.reset();
        let header2 = buffer.write_header(2, 100, 1, 0, 1024);

        assert_ne!(header1, header2);
    }

    #[test]
    fn test_buffer_reuse_with_reset() {
        let mut buffer = HeaderBuffer::new();

        // First write
        buffer.write_header(1, 100, 1, 0, 1024);
        assert_eq!(buffer.size(), 32);

        // Reset and reuse
        buffer.reset();
        assert_eq!(buffer.size(), 0);

        // Second write
        buffer.write_header(2, 200, 2, 0, 2048);
        assert_eq!(buffer.size(), 32);
    }

    #[test]
    fn test_thread_local_buffer() {
        let mut tlb = ThreadLocalBuffer::new();

        let header = tlb.build_header(42, 100, 999, 1, 2048);
        assert_eq!(header.size(), 32);
        assert_eq!(header.buffer().len(), 32);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let pool = Arc::new(std::sync::Mutex::new(ThreadLocalBuffer::new()));
        let mut handles = vec![];

        for i in 0..10 {
            let pool_clone = Arc::clone(&pool);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let mut tlb = pool_clone.lock().expect("Failed to acquire lock");
                    let header = tlb.build_header(i as u64, j as u64, j as u64, 0, 1024);
                    assert_eq!(header.size(), 32);
                    tlb.reset();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    }
}
