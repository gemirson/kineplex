use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use kineplex_core::laguerre::config::KantorovichDistributionConfig;
use kineplex_core::laguerre::distributor::LaguerreOptimalDistributor;
use kineplex_net::quic::engine::EngineConfig;
use kineplex_net::quic::mpsc::SetpointChannel;
use kineplex_net::quic::transport::{IoUringQuicTransport, QuicBlockTransport};
use object_store::memory::InMemory;
use std::net::UdpSocket;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;

#[tokio::test]
async fn test_laguerre_distributor_to_quic_integration() {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("Falha ao ligar socket");
    let addr = socket.local_addr().unwrap();
    socket.connect(addr).unwrap();

    let config = EngineConfig {
        enable_sqpoll: false,
        ..Default::default()
    };

    let transport = IoUringQuicTransport::new(socket.as_raw_fd(), config).unwrap();

    // Cria distribuidor de Laguerre
    let store = Arc::new(InMemory::new());
    let seeds = vec![[0.0, 0.0, 0.0, 0.0], [1.0, 1.0, 1.0, 1.0]];
    let mut distributor = LaguerreOptimalDistributor::new(
        store,
        "test-bucket".to_string(),
        "quic-test".to_string(),
        seeds,
        KantorovichDistributionConfig::default(),
    );

    // Canal MPSC para setpoint ótimo
    let channel = SetpointChannel::new(16);
    let (tx, mut rx) = channel.split();

    // Envia novo setpoint dinâmico r*
    tx.send_target_r(0.35).unwrap();
    assert_eq!(rx.try_recv_latest(), Some(0.35));

    // Atualiza no transporte sem derivative kick
    transport.set_target_setpoint(0.35).unwrap();
    let tele = transport.telemetry_snapshot();
    assert_eq!(tele.setpoint_r, 0.35);

    // Roteia um RecordBatch pelo Laguerre e transmite bloco via QUIC
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Float64, false)]));
    let col = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
    let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

    let raw_size = LaguerreOptimalDistributor::estimate_markov_zstd_mass(&batch);
    assert!(raw_size > 0);

    distributor.route_and_distribute(batch, [0.1, 0.1, 0.1, 0.1]).await.unwrap();

    // Envia bloco pelo transporte QUIC
    let payload = vec![0xEEu8; 512];
    let sent = transport.send_block(addr, 42, &payload).await.unwrap();
    assert_eq!(sent, 512);

    let rec = transport.receive_block(42).await.unwrap();
    assert_eq!(rec, payload);
}
