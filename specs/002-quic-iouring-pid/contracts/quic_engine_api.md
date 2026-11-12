# Contract: Motor de Transporte QUIC + io_uring (`kineplex-net`)

## 1. Interface Pública da Crate (`kineplex-net::quic`)

### 1.1 Trait `QuicBlockTransport`
Contrato para transmissão assíncrona de blocos colunares Arrow e Parquet sobre HTTP/3 / QUIC:

```rust
use async_trait::async_trait;
use std::net::SocketAddr;

#[async_trait]
pub trait QuicBlockTransport: Send + Sync {
    /// Transmite um lote de bytes ou RecordBatch fatiado via stream QUIC independente
    async fn send_block(
        &self,
        peer_addr: SocketAddr,
        stream_id: u64,
        data: &[u8],
    ) -> Result<usize, QuicTransportError>;

    /// Lê um bloco completo recebido em um stream QUIC específico
    async fn receive_block(
        &self,
        stream_id: u64,
    ) -> Result<Vec<u8>, QuicTransportError>;

    /// Obtém instantâneo de telemetria do controlador PID e anel io_uring
    fn telemetry_snapshot(&self) -> PidTelemetrySnapshot;

    /// Ajusta dinamicamente o setpoint de operação r* sem choque derivativo
    fn set_target_setpoint(&self, target_r: f64) -> Result<(), QuicTransportError>;
}
```

### 1.2 Estrutura de Telemetria (`PidTelemetrySnapshot`)
```rust
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
```

### 1.3 Tratamento de Erros Estruturados (`QuicTransportError`)
```rust
#[derive(thiserror::Error, Debug)]
pub enum QuicTransportError {
    #[error("io_uring setup failed: {0}")]
    IoUringInitFailed(String),

    #[error("Kernel SQPOLL unsupported or permission denied (CAP_SYS_ADMIN missing)")]
    SqPollUnavailable,

    #[error("Buffer arena memory registration failed: {0}")]
    BufferRegistrationFailed(String),

    #[error("QUIC protocol error: {0}")]
    QuicProtocol(String),

    #[error("Submission queue ring exhausted (EBUSY)")]
    QueueFull,

    #[error("Channel communication error: {0}")]
    ChannelError(String),
}
```
