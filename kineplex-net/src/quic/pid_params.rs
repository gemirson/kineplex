/// Hiperparâmetros imutáveis de sintonia analítica do Controlador PID Discreto em Malha Fechada
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
pub struct PidControlParameters {
    /// Ganho Proporcional (K_p)
    pub kp: f64,
    /// Ganho Integral (K_i)
    pub ki: f64,
    /// Ganho Derivativo (K_d)
    pub kd: f64,
    /// Ganho de Anti-Windup por Back-Calculation (K_aw)
    pub kaw: f64,
    /// Constante de tempo do filtro derivativo passa-baixas em segundos (\tau_d)
    pub tau_d: f64,
    /// Fator de ponderação de acoplamento do RTT em y(t_k) (\alpha_{rtt} \in [0.0, 1.0])
    pub alpha_rtt: f64,
    /// Cota inferior de injeção u_min (ex.: 1.0 pacote/ciclo ou bytes/\mu s)
    pub u_min: f64,
    /// Cota superior de injeção u_max (limite de saturação)
    pub u_max: f64,
    /// Cota inferior de amostragem \Delta t_min para prevenir singularidades (1 \mu s)
    pub min_dt_seconds: f64,
}

impl Default for PidControlParameters {
    fn default() -> Self {
        Self {
            kp: 1.25,
            ki: 0.45,
            kd: 0.15,
            kaw: 0.80,
            tau_d: 0.002,           // 2 ms de constante de filtro passa-baixas
            alpha_rtt: 0.70,        // 70% peso no gradiente de RTT, 30% na pressão do anel
            u_min: 1.0,
            u_max: 64.0,            // Até 64 super-buffers por ciclo
            min_dt_seconds: 1e-6,   // 1 microssegundo
        }
    }
}
