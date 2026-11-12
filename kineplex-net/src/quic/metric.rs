/// Amostra instantânea de esforço híbrido de latência e profundidade de fila
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
pub struct HybridPressureMetric {
    /// Variável de processo adimensionalizada y(t_k)
    pub y_value: f64,
    /// Estimativa de RTT suavizado calculado pela pilha QUIC (microssegundos)
    pub smoothed_rtt_micros: u64,
    /// Atraso físico mínimo de propagação na VPC (microssegundos)
    pub min_rtt_micros: u64,
    /// Datagramas e SQEs em voo sem confirmação CQE (Q_{in-flight})
    pub inflight_packets: u32,
    /// Capacidade nominal de submissão do anel io_uring (Q_{ring_cap})
    pub ring_capacity: u32,
}

impl HybridPressureMetric {
    /// Calcula a variável adimensional y(t_k) a partir do gradiente de atraso e ocupação do anel:
    /// y(t_k) = \alpha_{rtt} * ((RTT_smooth - RTT_min) / RTT_min) + (1 - \alpha_{rtt}) * (Q_{in-flight} / Q_{ring_cap})
    #[inline(always)]
    pub fn compute(
        alpha_rtt: f64,
        smoothed_rtt_micros: u64,
        min_rtt_micros: u64,
        inflight_packets: u32,
        ring_capacity: u32,
    ) -> Self {
        let safe_min_rtt = min_rtt_micros.max(1) as f64;
        let rtt_diff = (smoothed_rtt_micros as f64 - safe_min_rtt).max(0.0);
        let rtt_term = rtt_diff / safe_min_rtt;

        let safe_cap = ring_capacity.max(1) as f64;
        let queue_term = (inflight_packets as f64) / safe_cap;

        let y_value = alpha_rtt * rtt_term + (1.0 - alpha_rtt) * queue_term;

        Self {
            y_value,
            smoothed_rtt_micros,
            min_rtt_micros,
            inflight_packets,
            ring_capacity,
        }
    }
}
