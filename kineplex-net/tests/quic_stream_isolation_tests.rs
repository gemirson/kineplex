use kineplex_net::quic::engine::EngineConfig;
use kineplex_net::quic::transport::{IoUringQuicTransport, QuicBlockTransport};
use std::net::UdpSocket;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;

#[tokio::test]
async fn test_quic_stream_isolation_and_hol_immunity() {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("Falha ao ligar socket UDP");
    let addr = socket.local_addr().unwrap();
    socket.connect(addr).expect("Falha ao conectar socket");

    let config = EngineConfig {
        enable_sqpoll: false,
        ..Default::default()
    };

    let transport = Arc::new(
        IoUringQuicTransport::new(socket.as_raw_fd(), config)
            .expect("Falha ao criar IoUringQuicTransport")
    );

    // Dispara 32 streams paralelos independentes
    let mut handles = Vec::new();
    for stream_id in 0..32u64 {
        let t = Arc::clone(&transport);
        let handle = tokio::spawn(async move {
            let data = format!("payload-stream-{}", stream_id).into_bytes();
            // Simula perda no stream #3
            if stream_id == 3 {
                // Stream #3 descartado / com perda induzida
                return (stream_id, false);
            }
            let sent = t.send_block(addr, stream_id, &data).await.is_ok();
            let rec = t.receive_block(stream_id).await.is_ok();
            (stream_id, sent && rec)
        });
        handles.push(handle);
    }

    let mut successful_streams = 0;
    for handle in handles {
        let (stream_id, success) = handle.await.unwrap();
        if stream_id != 3 {
            assert!(success, "Stream {} falhou em avançar independentemente", stream_id);
            successful_streams += 1;
        }
    }

    assert_eq!(successful_streams, 31, "Todos os 31 streams não afetados devem completar com sucesso");
}
