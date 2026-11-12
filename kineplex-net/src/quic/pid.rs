use crate::quic::metric::HybridPressureMetric;
use crate::quic::pid_params::PidControlParameters;

/// Estado dinâmico do controlador PID em tempo discreto
/// Alinhado a 64 bytes para evitar false sharing em hot-paths multi-thread
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C, align(64))]
pub struct PidControllerState {
    pub integral_state: f64,
    pub derivative_state: f64,
    pub last_measured_y: f64,
    pub last_u_raw: f64,
    pub last_u_sat: f64,
    pub last_timestamp_nanos: u64,
    pub total_cycles_executed: u64,
}

impl Default for PidControllerState {
    fn default() -> Self {
        Self {
            integral_state: 0.0,
            derivative_state: 0.0,
            last_measured_y: 0.0,
            last_u_raw: 0.0,
            last_u_sat: 0.0,
            last_timestamp_nanos: 0,
            total_cycles_executed: 0,
        }
    }
}

/// Saída detalhada de cálculo de um ciclo do Controlador PID
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PidCycleOutput {
    pub p_term: f64,
    pub i_term: f64,
    pub d_term: f64,
    pub u_raw: f64,
    pub u_sat: f64,
    pub tracking_error: f64,
    pub y_measured: f64,
    pub dt_seconds: f64,
}

/// Controlador PID Discreto em Malha Fechada operando no Ponto de Kleinrock
pub struct ClosedLoopPidController {
    params: PidControlParameters,
    state: PidControllerState,
    setpoint_r: f64,
}

impl ClosedLoopPidController {
    pub fn new(params: PidControlParameters, initial_setpoint_r: f64) -> Self {
        Self {
            params,
            state: PidControllerState::default(),
            setpoint_r: initial_setpoint_r,
        }
    }

    #[inline(always)]
    pub fn setpoint(&self) -> f64 {
        self.setpoint_r
    }

    /// Atualiza o setpoint r* sem causar derivative kick (pois D_k é calculado sobre y, não sobre erro)
    #[inline(always)]
    pub fn update_setpoint(&mut self, new_setpoint_r: f64) {
        self.setpoint_r = new_setpoint_r;
    }

    #[inline(always)]
    pub fn state(&self) -> &PidControllerState {
        &self.state
    }

    #[inline(always)]
    pub fn params(&self) -> &PidControlParameters {
        &self.params
    }

    /// Executa um ciclo de amostragem da lei de controle discreta:
    /// 1. e_k = r* - y(t_k)
    /// 2. P_k = K_p * e_k
    /// 3. I_k = I_{k-1} + (K_i * e_k + K_aw * (u_{k-1}^sat - u_{k-1}^raw)) * \Delta t_k
    /// 4. D_k = ((2*\tau_d - \Delta t_k)/(2*\tau_d + \Delta t_k)) * D_{k-1} - ((2*K_d)/(2*\tau_d + \Delta t_k)) * (y(t_k) - y(t_{k-1}))
    /// 5. u_k = sat_{[u_min, u_max]}(P_k + I_k + D_k)
    pub fn step(&mut self, timestamp_nanos: u64, metric: &HybridPressureMetric) -> PidCycleOutput {
        let dt_seconds = if self.state.total_cycles_executed == 0 || self.state.last_timestamp_nanos == 0 {
            self.params.min_dt_seconds
        } else {
            let elapsed_nanos = timestamp_nanos.saturating_sub(self.state.last_timestamp_nanos);
            let dt = (elapsed_nanos as f64) * 1e-9;
            dt.max(self.params.min_dt_seconds)
        };

        let y_k = metric.y_value;
        let e_k = self.setpoint_r - y_k;

        // 1. Termo Proporcional
        let p_term = self.params.kp * e_k;

        // 2. Termo Integral com Back-Calculation Anti-Windup
        // I_k = I_{k-1} + (K_i * e_k + K_aw * (u_{k-1}^sat - u_{k-1}^raw)) * dt
        let windup_diff = self.state.last_u_sat - self.state.last_u_raw;
        let integral_delta = (self.params.ki * e_k + self.params.kaw * windup_diff) * dt_seconds;
        let i_term = self.state.integral_state + integral_delta;

        // 3. Termo Derivativo Filtrado sobre a Medida (Derivative-on-Measurement)
        // D_k = ((2\tau_d - dt)/(2\tau_d + dt)) * D_{k-1} - ((2*K_d)/(2\tau_d + dt)) * (y_k - y_{k-1})
        let denom = 2.0 * self.params.tau_d + dt_seconds;
        let d_factor_prev = (2.0 * self.params.tau_d - dt_seconds) / denom;
        let d_factor_meas = (2.0 * self.params.kd) / denom;

        let delta_y = if self.state.total_cycles_executed == 0 {
            0.0
        } else {
            y_k - self.state.last_measured_y
        };

        let d_term = d_factor_prev * self.state.derivative_state - d_factor_meas * delta_y;

        // 4. Sinal de Atuação Bruto e Saturado
        let u_raw = p_term + i_term + d_term;
        let u_sat = u_raw.clamp(self.params.u_min, self.params.u_max);

        // 5. Atualização de Estado
        self.state.integral_state = i_term;
        self.state.derivative_state = d_term;
        self.state.last_measured_y = y_k;
        self.state.last_u_raw = u_raw;
        self.state.last_u_sat = u_sat;
        self.state.last_timestamp_nanos = timestamp_nanos;
        self.state.total_cycles_executed += 1;

        PidCycleOutput {
            p_term,
            i_term,
            d_term,
            u_raw,
            u_sat,
            tracking_error: e_k,
            y_measured: y_k,
            dt_seconds,
        }
    }
}
