use crate::quic::engine::{EngineConfig, IoUringQuicEngine};
use crate::quic::error::QuicTransportError;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct PidTelemetrySnapshot {
    pub current_y: f64,
    pub setpoint_r: f64,
    pub tracking_error: f64,
    pub p_term: f64,
    pub i_term: f64,
    pub d_term: f64,
    pub u_sat: f64,
    pub u_raw: f64,
    pub smoothed_rtt_micros: u64,
    pub min_rtt_micros: u64,
    pub inflight_packets: u32,
    pub zero_syscall_active: bool,
    pub total_cycles: u64,
}

#[async_trait]
pub trait QuicBlockTransport: Send + Sync {
    async fn send_block(
        &self,
        peer_addr: SocketAddr,
        stream_id: u64,
        data: &[u8],
    ) -> Result<usize, QuicTransportError>;

    async fn receive_block(
        &self,
        stream_id: u64,
    ) -> Result<Vec<u8>, QuicTransportError>;

    fn telemetry_snapshot(&self) -> PidTelemetrySnapshot;

    fn set_target_setpoint(&self, target_r: f64) -> Result<(), QuicTransportError>;
}

/// Implementação de referência de transporte QUIC com io_uring
pub struct IoUringQuicTransport {
    engine: Arc<Mutex<IoUringQuicEngine>>,
    streams: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
}

impl IoUringQuicTransport {
    pub fn new(socket_fd: std::os::unix::io::RawFd, config: EngineConfig) -> Result<Self, QuicTransportError> {
        let engine = IoUringQuicEngine::new(socket_fd, config)?;
        Ok(Self {
            engine: Arc::new(Mutex::new(engine)),
            streams: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl QuicBlockTransport for IoUringQuicTransport {
    async fn send_block(
        &self,
        _peer_addr: SocketAddr,
        stream_id: u64,
        data: &[u8],
    ) -> Result<usize, QuicTransportError> {
        let mut engine = self.engine.lock();

        // 1. Tenta adquirir um slot na arena registrada
        let slot_idx = match engine.arena().acquire_slot() {
            Some(idx) => idx,
            None => {
                // Se esgotado, colhe conclusões para liberar slots
                let _ = engine.reap_completions(500);
                engine.arena().acquire_slot().ok_or(QuicTransportError::QueueFull)?
            }
        };

        // 2. Copia os dados para o slot da arena
        let slot_len = engine.arena().slot_size();
        let copy_len = data.len().min(slot_len);
        if let Some(slice) = engine.arena().get_slot_slice_mut(slot_idx) {
            slice[..copy_len].copy_from_slice(&data[..copy_len]);
        }

        // 3. Submete o super-buffer no anel io_uring
        let _ = engine.submit_gso_super_buffer(slot_idx, copy_len)?;

        // Armazena no buffer de stream (simulando avanço do stream QUIC independente)
        {
            let mut streams = self.streams.lock();
            streams.insert(stream_id, data[..copy_len].to_vec());
        }

        Ok(copy_len)
    }

    async fn receive_block(
        &self,
        stream_id: u64,
    ) -> Result<Vec<u8>, QuicTransportError> {
        let mut streams = self.streams.lock();
        streams.remove(&stream_id).ok_or_else(|| {
            QuicTransportError::QuicProtocol(format!("Stream {} não encontrado", stream_id))
        })
    }

    fn telemetry_snapshot(&self) -> PidTelemetrySnapshot {
        let engine = self.engine.lock();
        let pid_state = engine.pid().state();
        let setpoint_r = engine.pid().setpoint();
        let tracking_error = setpoint_r - pid_state.last_measured_y;

        PidTelemetrySnapshot {
            current_y: pid_state.last_measured_y,
            setpoint_r,
            tracking_error,
            p_term: engine.pid().params().kp * tracking_error,
            i_term: pid_state.integral_state,
            d_term: pid_state.derivative_state,
            u_sat: pid_state.last_u_sat,
            u_raw: pid_state.last_u_raw,
            smoothed_rtt_micros: 500,
            min_rtt_micros: 500,
            inflight_packets: engine.inflight(),
            zero_syscall_active: engine.is_sqpoll(),
            total_cycles: pid_state.total_cycles_executed,
        }
    }

    fn set_target_setpoint(&self, target_r: f64) -> Result<(), QuicTransportError> {
        let mut engine = self.engine.lock();
        engine.set_target_setpoint(target_r);
        Ok(())
    }
}
