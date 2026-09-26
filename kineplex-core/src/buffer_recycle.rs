//! Fixed-size network payload buffers with completion-owned recycling guards.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::egress::OutboundSpike;
use crate::iouring_net::IoVec;

#[derive(Debug)]
struct PoolState {
    buffers: Vec<Vec<u8>>,
    available: Vec<usize>,
}

#[derive(Debug)]
struct PoolInner {
    state: Mutex<PoolState>,
}

/// A fixed-size memory pool for outgoing Arrow IPC buffers.
#[derive(Clone, Debug)]
pub struct RecyclingBufferPool {
    inner: Arc<PoolInner>,
}

impl RecyclingBufferPool {
    /// Creates `buffer_count` zeroed buffers, each with `buffer_size` capacity.
    #[must_use]
    pub fn new(buffer_size: usize, buffer_count: usize) -> Self {
        let buffers = (0..buffer_count).map(|_| vec![0; buffer_size]).collect();
        let available = (0..buffer_count).rev().collect();
        Self {
            inner: Arc::new(PoolInner {
                state: Mutex::new(PoolState { buffers, available }),
            }),
        }
    }

    /// Takes ownership of one available buffer.
    pub fn acquire(&self) -> Option<BufferLease> {
        let mut state = self.lock_state();
        let index = state.available.pop()?;
        Some(BufferLease {
            inner: Arc::clone(&self.inner),
            index,
            used: 0,
        })
    }

    /// Number of currently available buffers.
    #[must_use]
    pub fn available_count(&self) -> usize {
        self.lock_state().available.len()
    }

    /// Total number of buffers managed by this pool.
    #[must_use]
    pub fn buffer_count(&self) -> usize {
        self.lock_state().buffers.len()
    }

    fn lock_state(&self) -> MutexGuard<'_, PoolState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// An acquired buffer. Dropping it zeroes the memory and returns it to the pool.
pub struct BufferLease {
    inner: Arc<PoolInner>,
    index: usize,
    used: usize,
}

impl BufferLease {
    /// Writes payload bytes into the fixed-size pooled storage.
    pub fn write(&mut self, bytes: &[u8]) -> Result<(), BufferPoolError> {
        let mut state = self.lock_state();
        let buffer = state
            .buffers
            .get_mut(self.index)
            .ok_or(BufferPoolError::InvalidBuffer)?;
        if bytes.len() > buffer.len() {
            return Err(BufferPoolError::PayloadTooLarge {
                size: bytes.len(),
                capacity: buffer.len(),
            });
        }
        buffer[..bytes.len()].copy_from_slice(bytes);
        drop(state);
        self.used = bytes.len();
        Ok(())
    }

    /// Calls `f` with the initialized payload bytes while retaining pool access.
    pub fn with_bytes<R>(&self, f: impl for<'a> FnOnce(&'a [u8]) -> R) -> R {
        let state = self.lock_state();
        f(&state.buffers[self.index][..self.used])
    }

    /// Transfers the buffer into a completion-owned state.
    #[must_use]
    pub fn into_flight(self) -> InFlightBuffer {
        InFlightBuffer { lease: self }
    }

    fn lock_state(&self) -> MutexGuard<'_, PoolState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        let mut state = self.lock_state();
        if let Some(buffer) = state.buffers.get_mut(self.index) {
            buffer.fill(0);
        }
        state.available.push(self.index);
    }
}

/// A payload retained until the caller processes its network completion.
pub struct InFlightBuffer {
    lease: BufferLease,
}

impl InFlightBuffer {
    /// Calls `f` with the payload bytes while the completion guard keeps storage alive.
    pub fn with_bytes<R>(&self, f: impl for<'a> FnOnce(&'a [u8]) -> R) -> R {
        self.lease.with_bytes(f)
    }

    /// Completes the send and recycles the zeroed payload buffer.
    pub fn complete(self) {
        drop(self);
    }
}

/// Outbound spike whose payload is retained in the pool through send completion.
pub struct PooledOutboundSpike {
    header: Vec<u8>,
    payload: InFlightBuffer,
    sequence: u64,
}

impl PooledOutboundSpike {
    /// Copies an owned egress packet into a fixed-size pooled payload buffer.
    pub fn from_outbound(
        pool: &RecyclingBufferPool,
        spike: OutboundSpike,
    ) -> Result<Self, BufferPoolError> {
        let (header, payload, sequence) = spike.into_parts();
        let mut lease = pool.acquire().ok_or(BufferPoolError::PoolExhausted)?;
        lease.write(&payload)?;
        Ok(Self {
            header,
            payload: lease.into_flight(),
            sequence,
        })
    }

    /// Sequence number included in the synapse header.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Builds temporary scatter-gather descriptors for one synchronous submit call.
    ///
    /// The owner must remain alive until the asynchronous completion is received.
    pub fn with_iovecs<R>(&self, submit: impl FnOnce(&[IoVec]) -> R) -> R {
        self.payload.with_bytes(|payload| {
            let iovecs = [IoVec::from_slice(&self.header), IoVec::from_slice(payload)];
            submit(&iovecs)
        })
    }

    /// Borrows the header bytes for inspection or submission.
    #[must_use]
    pub fn header(&self) -> &[u8] {
        &self.header
    }

    /// Processes the send completion and returns the zeroed payload buffer.
    pub fn complete(self) {
        self.payload.complete();
    }
}

/// Pool exhaustion or fixed-size write failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BufferPoolError {
    PoolExhausted,
    InvalidBuffer,
    PayloadTooLarge { size: usize, capacity: usize },
}

impl Display for BufferPoolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PoolExhausted => formatter.write_str("no network payload buffer is available"),
            Self::InvalidBuffer => {
                formatter.write_str("buffer lease refers to an invalid pool slot")
            }
            Self::PayloadTooLarge { size, capacity } => {
                write!(
                    formatter,
                    "payload is {size} bytes; buffer capacity is {capacity}"
                )
            }
        }
    }
}

impl Error for BufferPoolError {}

#[cfg(test)]
mod tests {
    use super::RecyclingBufferPool;

    #[test]
    fn drop_zeroes_and_returns_the_buffer_once() {
        let pool = RecyclingBufferPool::new(16, 1);
        let mut lease = pool.acquire().expect("buffer available");
        lease.write(b"secret").expect("payload fits");
        assert_eq!(pool.available_count(), 0);
        drop(lease);
        assert_eq!(pool.available_count(), 1);
        let lease = pool.acquire().expect("buffer returned");
        lease.with_bytes(|bytes| assert!(bytes.is_empty()));
    }

    #[test]
    fn completion_guard_keeps_buffer_unavailable_until_complete() {
        let pool = RecyclingBufferPool::new(32, 1);
        let mut lease = pool.acquire().expect("buffer available");
        lease.write(b"in flight").expect("payload fits");
        let pending = lease.into_flight();
        assert_eq!(pool.available_count(), 0);
        pending.complete();
        assert_eq!(pool.available_count(), 1);
    }

    #[test]
    fn outbound_packet_keeps_scatter_gather_payload_until_completion() {
        use std::sync::Arc;

        use crate::egress::build_outbound_spike;
        use crate::synapse::Synapse;
        use arrow::array::{Float32Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Float32Array::from(vec![1.0]))])
            .expect("batch is valid");
        let spike = build_outbound_spike(&Synapse::new(1, 2, 3, 4), &batch, 1)
            .expect("outbound packet builds");
        let pool = RecyclingBufferPool::new(4096, 1);
        let pooled =
            super::PooledOutboundSpike::from_outbound(&pool, spike).expect("payload fits pool");
        pooled.with_iovecs(|vectors| {
            assert_eq!(vectors.len(), 2);
            assert_eq!(vectors[0].len, 32);
            assert!(vectors[1].len > 0);
        });
        assert_eq!(pool.available_count(), 0);
        pooled.complete();
        assert_eq!(pool.available_count(), 1);
    }
}
