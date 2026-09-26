//! io_uring network I/O primitives for high-performance networking.
//!
//! This module provides:
//! - FT-017: Direct FD registration for UDP sockets
//! - FT-018: Provided buffers allocation (memory pool)
//! - FT-019: Multishot receive operations
//! - FT-020: Vectored write (scatter-gather)

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

/// Default buffer size (64KB, suitable for large MTU)
pub const DEFAULT_BUFFER_SIZE: usize = 65536;

/// Default number of buffers in the pool
pub const DEFAULT_BUFFER_COUNT: usize = 512;

/// Default alignment for cache line (64 bytes)
pub const CACHE_LINE_ALIGN: usize = 64;

// ============================================================================
// FT-017: Direct File Descriptors for UDP
// ============================================================================

#[derive(Debug, Clone)]
pub struct DirectUdpSocket {
    pub fd: std::os::fd::RawFd,
    pub registered_fd_index: Option<usize>,
}

impl DirectUdpSocket {
    pub fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let socket = std::net::UdpSocket::bind(addr)?;
        socket.set_nonblocking(true)?;
        let fd = socket.into_raw_fd();

        Ok(Self {
            fd,
            registered_fd_index: None,
        })
    }

    pub fn set_registered_index(&mut self, index: usize) {
        self.registered_fd_index = Some(index);
    }
}

#[derive(Debug, Default)]
pub struct FdRegistry {
    fds: Mutex<Vec<DirectUdpSocket>>,
}

impl FdRegistry {
    pub fn new() -> Self {
        Self {
            fds: Mutex::new(Vec::new()),
        }
    }

    pub fn register_socket(&self, socket: DirectUdpSocket) -> usize {
        let mut fds = self.fds.lock().unwrap();
        let index = fds.len();
        fds.push(socket);
        index
    }

    pub fn len(&self) -> usize {
        self.fds.lock().unwrap().len()
    }
}

// ============================================================================
// FT-018: Provided Buffers Allocation
// ============================================================================

#[derive(Debug, Clone)]
pub struct Buffer {
    data: Vec<u8>,
    id: usize,
}

impl Buffer {
    pub fn new(size: usize, id: usize) -> Self {
        // Pre-allocate with capacity, then grow to ensure alignment
        let mut data = Vec::with_capacity(size + CACHE_LINE_ALIGN);
        // Initialize with zeros
        data.resize(size, 0);

        Self { data, id }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

#[derive(Debug)]
pub struct BufferPool {
    buffers: Vec<Buffer>,
    available: Mutex<Vec<usize>>,
    buffer_size: usize,
    total_memory: usize,
}

impl BufferPool {
    pub fn new(buffer_size: usize, buffer_count: usize) -> Self {
        let mut buffers = Vec::with_capacity(buffer_count);
        let mut available = Vec::with_capacity(buffer_count);

        for i in 0..buffer_count {
            buffers.push(Buffer::new(buffer_size, i));
            available.push(i);
        }

        let total_memory = buffer_size * buffer_count;

        Self {
            buffers,
            available: Mutex::new(available),
            buffer_size,
            total_memory,
        }
    }

    pub fn acquire(&self) -> Option<usize> {
        self.available.lock().unwrap().pop()
    }

    pub fn release(&self, buffer_id: usize) {
        let mut available = self.available.lock().unwrap();
        if !available.contains(&buffer_id) {
            available.push(buffer_id);
        }
    }

    pub fn get(&self, id: usize) -> Option<&Buffer> {
        self.buffers.get(id)
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn available_count(&self) -> usize {
        self.available.lock().unwrap().len()
    }

    pub fn total_memory(&self) -> usize {
        self.total_memory
    }

    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }
}

// ============================================================================
// FT-019: Multishot Receive
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvState {
    Idle,
    Receiving,
    Aborted,
    Error,
}

pub struct MultishotReceiver {
    state: RecvState,
    socket_fd: Option<usize>,
    buffer_pool: Option<Arc<BufferPool>>,
}

impl MultishotReceiver {
    pub fn new() -> Self {
        Self {
            state: RecvState::Idle,
            socket_fd: None,
            buffer_pool: None,
        }
    }

    pub fn configure(&mut self, socket_fd: usize, buffer_pool: Arc<BufferPool>) {
        self.socket_fd = Some(socket_fd);
        self.buffer_pool = Some(buffer_pool);
    }

    /// Returns (opcode, flags) for the SQE.
    pub fn start_receive(&mut self) -> (u32, u32) {
        self.state = RecvState::Receiving;
        (18, 0x08 | 0x04) // recvmsg opcode, fixed_file | multishot
    }

    #[allow(dead_code)]
    pub fn handle_cqe(&self, buffer_id: usize, bytes_received: usize) -> Option<(usize, &[u8])> {
        let pool = self.buffer_pool.as_ref()?;
        let buffer = pool.get(buffer_id)?;
        Some((bytes_received, buffer.as_slice()))
    }

    pub fn release_buffer(&self, buffer_id: usize) {
        if let Some(pool) = &self.buffer_pool {
            pool.release(buffer_id);
        }
    }

    pub fn abort(&mut self) {
        self.state = RecvState::Aborted;
    }

    pub fn restart(&mut self) {
        self.state = RecvState::Idle;
    }

    pub fn state(&self) -> RecvState {
        self.state
    }
}

impl Default for MultishotReceiver {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// FT-020: Vectored Write (Scatter-Gather)
// ============================================================================

#[derive(Debug, Clone)]
pub struct IoVec {
    pub base: *mut u8,
    pub len: usize,
}

impl IoVec {
    pub fn from_slice(slice: &[u8]) -> Self {
        Self {
            base: slice.as_ptr() as *mut u8,
            len: slice.len(),
        }
    }
}

pub struct VectoredWriter {
    socket_fd: Option<usize>,
}

impl VectoredWriter {
    pub fn new() -> Self {
        Self { socket_fd: None }
    }

    pub fn configure(&mut self, socket_fd: usize) {
        self.socket_fd = Some(socket_fd);
    }

    pub fn prepare_write(&self, header: &[u8], payload: &[u8]) -> Vec<IoVec> {
        vec![IoVec::from_slice(header), IoVec::from_slice(payload)]
    }

    pub fn get_write_params(&self) -> (u32, u32, usize) {
        (17, 0x08, 2) // opcode: sendmsg, flags: fixed_file
    }
}

impl Default for VectoredWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Combined Network I/O Driver
// ============================================================================

pub struct NetworkDriver {
    fd_registry: Arc<FdRegistry>,
    buffer_pool: Option<Arc<BufferPool>>,
    receiver: MultishotReceiver,
    writer: VectoredWriter,
}

impl NetworkDriver {
    pub fn new() -> Self {
        Self {
            fd_registry: Arc::new(FdRegistry::new()),
            buffer_pool: None,
            receiver: MultishotReceiver::new(),
            writer: VectoredWriter::new(),
        }
    }

    pub fn init_buffer_pool(
        &mut self,
        buffer_size: usize,
        buffer_count: usize,
    ) -> std::result::Result<(), std::alloc::AllocError> {
        let pool = Arc::new(BufferPool::new(buffer_size, buffer_count));
        self.buffer_pool = Some(pool.clone());
        self.receiver.configure(0, pool);
        Ok(())
    }

    pub fn register_socket(&mut self, socket: DirectUdpSocket) -> usize {
        let index = self.fd_registry.register_socket(socket);

        if let Some(ref pool) = self.buffer_pool {
            self.receiver.configure(index, pool.clone());
        }
        self.writer.configure(index);

        index
    }

    #[allow(dead_code)]
    pub fn buffer_pool(&self) -> Option<&Arc<BufferPool>> {
        self.buffer_pool.as_ref()
    }

    pub fn receiver(&self) -> &MultishotReceiver {
        &self.receiver
    }

    pub fn writer(&self) -> &VectoredWriter {
        &self.writer
    }

    pub fn fd_registry(&self) -> &Arc<FdRegistry> {
        &self.fd_registry
    }
}

impl Default for NetworkDriver {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_creation() {
        let buffer = Buffer::new(1024, 0);
        assert_eq!(buffer.size(), 1024);
        assert_eq!(buffer.id(), 0);
    }

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(1024, 10);
        assert_eq!(pool.buffer_count(), 10);
        assert_eq!(pool.available_count(), 10);

        let id = pool.acquire().expect("No buffers available");
        assert_eq!(pool.available_count(), 9);

        pool.release(id);
        assert_eq!(pool.available_count(), 10);
    }

    #[test]
    fn test_multishot_receiver() {
        let mut receiver = MultishotReceiver::new();
        let (opcode, flags) = receiver.start_receive();

        assert_eq!(receiver.state(), RecvState::Receiving);

        receiver.abort();
        assert_eq!(receiver.state(), RecvState::Aborted);

        receiver.restart();
        assert_eq!(receiver.state(), RecvState::Idle);
    }

    #[test]
    fn test_vectored_writer() {
        let mut writer = VectoredWriter::new();
        writer.configure(5);

        let header = b"HEADER";
        let payload = b"PAYLOAD";

        let iovecs = writer.prepare_write(header, payload);

        assert_eq!(iovecs.len(), 2);
        assert_eq!(iovecs[0].len, 6);
        assert_eq!(iovecs[1].len, 7);
    }

    #[test]
    fn test_network_driver() {
        let mut driver = NetworkDriver::new();
        driver
            .init_buffer_pool(1024, 10)
            .expect("Failed to init buffer pool");
        assert!(driver.buffer_pool().is_some());
    }
}
