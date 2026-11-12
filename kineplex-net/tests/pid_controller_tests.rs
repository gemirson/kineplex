use kineplex_net::quic::metric::HybridPressureMetric;
use kineplex_net::quic::pid::{ClosedLoopPidController, PidControllerState};
use kineplex_net::quic::pid_params::PidControlParameters;

#[test]
fn test_pid_controller_state_cache_line_alignment() {
    assert_eq!(
        std::mem::align_of::<PidControllerState>(),
        64,
        "PidControllerState deve ser alinhado a 64 bytes para evitar false-sharing"
    );
}

#[test]
fn test_hybrid_pressure_metric_computation() {
    // VPC sa-east-1: RTT_min = 500 us, RTT_smooth = 500 us (regime laminar), Q_inflight = 0
    let m_laminar = HybridPressureMetric::compute(0.70, 500, 500, 0, 1024);
    assert_eq!(m_laminar.y_value, 0.0);

    // Quando RTT_smooth dobra (1000 us) e anel está em 50% de capacidade (512 / 1024)
    // y = 0.70 * ((1000 - 500)/500) + 0.30 * (512 / 1024) = 0.70 * 1.0 + 0.30 * 0.5 = 0.85
    let m_loaded = HybridPressureMetric::compute(0.70, 1000, 500, 512, 1024);
    assert!((m_loaded.y_value - 0.85).abs() < 1e-6);
}

#[test]
fn test_pid_controller_step_response_and_anti_windup() {
    let params = PidControlParameters {
        kp: 2.0,
        ki: 1.0,
        kd: 0.1,
        kaw: 0.5,
        tau_d: 0.005,
        alpha_rtt: 0.70,
        u_min: 1.0,
        u_max: 32.0,
        min_dt_seconds: 1e-6,
    };

    let mut controller = ClosedLoopPidController::new(params, 0.25);
    assert_eq!(controller.setpoint(), 0.25);

    let mut t_nanos = 1_000_000_000u64;
    let dt_nanos = 1_000_000u64; // 1 ms

    // Simula saturação: erro muito positivo (y = 0.0, r* = 0.25)
    // O atuador deve saturar em u_max (32.0), mas o termo integral deve sofrer clamping via kaw
    for _ in 0..100 {
        t_nanos += dt_nanos;
        let metric = HybridPressureMetric::compute(0.70, 500, 500, 0, 1024);
        let out = controller.step(t_nanos, &metric);
        assert!(out.u_sat <= 32.0);
        assert!(out.u_sat >= 1.0);
    }

    // Verifica que o termo integral não explodiu para infinito graças ao anti-windup
    let state = controller.state();
    assert!(state.integral_state < 100.0, "Anti-windup falhou em conter crescimento de I_k");
}

#[test]
fn test_derivative_on_measurement_eliminates_derivative_kick() {
    let params = PidControlParameters {
        kp: 1.0,
        ki: 0.0,
        kd: 5.0, // Alto ganho derivativo para evidenciar qualquer kick
        kaw: 0.0,
        tau_d: 0.01,
        alpha_rtt: 0.70,
        u_min: 0.0,
        u_max: 100.0,
        min_dt_seconds: 1e-6,
    };

    let mut controller = ClosedLoopPidController::new(params, 0.25);
    let mut t_nanos = 1_000_000_000u64;

    // Estado estacionário em y = 0.25
    let metric = HybridPressureMetric {
        y_value: 0.25,
        smoothed_rtt_micros: 500,
        min_rtt_micros: 500,
        inflight_packets: 0,
        ring_capacity: 1024,
    };

    let _ = controller.step(t_nanos, &metric);
    t_nanos += 1_000_000;
    let _ = controller.step(t_nanos, &metric);

    // Agora muda o setpoint r* bruscamente de 0.25 para 0.80
    controller.update_setpoint(0.80);

    // A medida física y_value não mudou (continua 0.25)
    t_nanos += 1_000_000;
    let out = controller.step(t_nanos, &metric);

    // O termo derivativo deve ser estritamente zero (ou infinitesimal), comprovando que NÃO HOUVE DERIVATIVE KICK
    assert!(
        out.d_term.abs() < 1e-4,
        "Derivative kick detectado: d_term = {} após salto no setpoint",
        out.d_term
    );
}
