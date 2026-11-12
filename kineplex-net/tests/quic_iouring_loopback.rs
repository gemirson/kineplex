use kineplex_net::quic::engine::EngineConfig;
use kineplex_net::quic::transport::{IoUringQuicTransport, QuicBlockTransport};
use std::net::UdpSocket;
use std::os::unix::io::AsRawFd;

#[tokio::test]
async fn test_quic_iouring_loopback_transport() {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("Falha ao ligar socket UDP");
    let addr = socket.local_addr().unwrap();
    socket.connect(addr).expect("Falha ao conectar socket a si mesmo");

    let config = EngineConfig {
        enable_sqpoll: false, // Modo seguro para rodar em qualquer runner de CI / sandbox
        ..Default::default()
    };

    let transport = IoUringQuicTransport::new(socket.as_raw_fd(), config)
        .expect("Falha ao criar IoUringQuicTransport");

    let payload = vec![0x42u8; 1024]; // 1 KB
    let sent = transport
        .send_block(addr, 1, &payload)
        .await
        .expect("Falha ao enviar bloco");

    assert_eq!(sent, 1024);

    let received = transport
        .receive_block(1)
        .await
        .expect("Falha ao receber bloco");

    assert_eq!(received, payload);

    let tele = transport.telemetry_snapshot();
    assert_eq!(tele.setpoint_r, 0.25);
}
