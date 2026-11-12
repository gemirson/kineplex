use std::time::Instant;
use kineplex_net::quic::metric::HybridPressureMetric;
use kineplex_net::quic::pid::ClosedLoopPidController;
use kineplex_net::quic::pid_params::PidControlParameters;

fn main() {
    println!("=== Benchmark: ClosedLoopPidController Zero-Alloc Hot Path ===");

    let params = PidControlParameters::default();
    let mut controller = ClosedLoopPidController::new(params, 0.25);

    let iterations = 10_000_000u64;
    let mut timestamp_nanos = 1_000_000_000u64;

    let start = Instant::now();
    for i in 0..iterations {
        timestamp_nanos += 1_000; // 1 us step
        let metric = HybridPressureMetric {
            y_value: 0.25 + 0.05 * ((i % 100) as f64 / 100.0),
            smoothed_rtt_micros: 500 + (i % 50),
            min_rtt_micros: 500,
            inflight_packets: (i % 64) as u32,
            ring_capacity: 1024,
        };

        let _ = controller.step(timestamp_nanos, &metric);
    }
    let elapsed = start.elapsed();
    let ns_per_cycle = elapsed.as_nanos() as f64 / iterations as f64;
    let mops = (iterations as f64 / elapsed.as_secs_f64()) / 1_000_000.0;

    println!("Total iterações: {}", iterations);
    println!("Tempo total: {:?}", elapsed);
    println!("Tempo por ciclo PID: {:.2} ns/ciclo", ns_per_cycle);
    println!("Vazão do controlador: {:.2} M ops/segundo", mops);
    assert!(ns_per_cycle < 100.0, "Ciclo PID deve ser estritamente zero-alloc e < 100ns");
}
