use thiserror::Error;

#[derive(Error, Debug)]
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

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
