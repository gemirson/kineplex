# Data Model: HTTP/3 QUIC + io_uring com Controle PID (FT-098)

## 1. Visão Geral das Entidades de Domínio

O motor de transporte de alto rendimento é estruturado em torno de cinco entidades essenciais em `kineplex-net`:

```
+------------------------------------+       +------------------------------------+
|       PidControlParameters         |       |        KleinrockSetpoint           |
| (Hiperparâmetros imutáveis de PID) |       | (Rastreamento RTT_min e r*)        |
+-----------------+------------------+       +-----------------+------------------+
                  |                                            |
                  v                                            v
+---------------------------------------------------------------------------------+
|                               PidControllerState                                |
|  - Integrador I_k com Back-Calculation Anti-Windup                             |
|  - Derivador D_k com Filtro Passa-Baixas tau_d (Derivative-on-Measurement)      |
|  - Registrador de Atuação [u_min, u_max] (Pacing Rate em bytes/us)              |
+---------------------------------------------------------------------------------+
                                      |
                                      v
+---------------------------------------------------------------------------------+
|                             HybridPressureMetric                                |
|  - y(t_k) = alpha_rtt * ((RTT_smooth - RTT_min)/RTT_min) +                     |
|             (1 - alpha_rtt) * (Q_inflight / Q_ring_cap)                         |
+---------------------------------------------------------------------------------+
                                      |
                                      v
+-------------------------------------+       +-----------------------------------+
|        RegisteredMemoryArena        |       |        IoUringQuicEngine          |
|  - 2 MB HugePage Arena              |       |  - SQ Ring / CQ Ring              |
|  - Slots fixos de 64 KB (GSO)       | <===> |  - quinn-proto Endpoint           |
|  - IORING_REGISTER_BUFFERS          |       |  - Fixed Socket Fixed(0)          |
+-------------------------------------+       +-----------------------------------+
```

---

## 2. Layouts de Estruturas e Alinhamento de Hardware

### 2.1 Hiperparâmetros do Controlador (`PidControlParameters`)
```rust
/// Configuração imutável dos ganhos e cotas do controlador PID em malha fechada
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct PidControlParameters {
    pub kp: f64,              // Ganho Proporcional
    pub ki: f64,              // Ganho Integral
    pub kd: f64,              // Ganho Derivativo
    pub kaw: f64,             // Ganho de Anti-Windup por Back-Calculation
    pub tau_d: f64,           // Constante de tempo do filtro derivativo (segundos)
    pub alpha_rtt: f64,       // Ponderação do gradiente de RTT em y(t_k) [0.0, 1.0]
    pub u_min: f64,           // Cota inferior de injeção (ex: 1.0 pacote/ciclo ou bytes/us)
    pub u_max: f64,           // Cota superior de injeção (ex: capacidade do anel)
    pub min_dt_seconds: f64,  // Cota inferior delta t para evitar instabilidade numérica (1 us)
}

impl Default for PidControlParameters {
    fn default() -> Self {
        Self {
            kp: 1.25,
            ki: 0.45,
            kd: 0.15,
            kaw: 0.80,
            tau_d: 0.002,           // 2 ms de constante de filtro
            alpha_rtt: 0.70,        // 70% peso no RTT, 30% na ocupação do anel
            u_min: 1.0,
            u_max: 64.0,            // Até 64 super-buffers por ciclo
            min_dt_seconds: 1e-6,   // 1 microssegundo
        }
    }
}
```

### 2.2 Estado Interno do Controlador PID (`PidControllerState`)
Alinhado à linha de cache L1 (64 bytes) para evitar false sharing em ambientes multi-thread.

```rust
/// Estado dinâmico do controlador PID em tempo discreto
#[derive(Debug)]
#[repr(C, align(64))]
pub struct PidControllerState {
    pub integral_state: f64,        // I_k acumulado
    pub derivative_state: f64,      // D_k filtrado
    pub last_measured_y: f64,       // y(t_{k-1})
    pub last_u_raw: f64,            // u_{k-1}^{raw}
    pub last_u_sat: f64,            // u_{k-1}^{sat}
    pub last_timestamp_nanos: u64,  // t_{k-1} em nanossegundos
    pub total_cycles_executed: u64,
}
```

### 2.3 Métrica Híbrida de Pressão de Rede (`HybridPressureMetric`)
```rust
/// Amostra instantânea de esforço combinado de latência e profundidade de anel
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct HybridPressureMetric {
    pub y_value: f64,               // y(t_k) adimensional
    pub smoothed_rtt_nanos: u64,    // RTT_smoothed medido pelo QUIC
    pub min_rtt_nanos: u64,         // RTT_min físico rastreado
    pub inflight_packets: u32,      // Q_{in-flight}(t_k)
    pub ring_capacity: u32,         // Q_{ring_cap}
}
```

### 2.4 Arena de Memória Pré-Registrada (`RegisteredMemoryArena`)
```rust
/// Arena contígua de 2 MB alinhada em HugePages particionada em slots de 64 KB
pub struct RegisteredMemoryArena {
    ptr: *mut u8,
    size_bytes: usize,              // 2 * 1024 * 1024 (2 MB)
    slot_size: usize,               // 64 * 1024 (64 KB)
    total_slots: usize,             // 32 slots
    free_slots: Vec<usize>,
}
```

### 2.5 Motor de I/O `io_uring` + QUIC (`IoUringQuicEngine`)
```rust
/// Motor de transporte HTTP/3 sobre QUIC com integração nativa ao io_uring
pub struct IoUringQuicEngine {
    ring: io_uring::IoUring,
    fixed_socket_fd: i32,
    arena: RegisteredMemoryArena,
    quic_endpoint: quinn_proto::Endpoint,
    pid_params: PidControlParameters,
    pid_state: PidControllerState,
    setpoint_r: f64,                // r* nominal (ex: 0.25)
    min_rtt_nanos: u64,
    setpoint_receiver: tokio::sync::mpsc::Receiver<f64>,
}
```

---

## 3. Máquina de Estados e Transições de Ciclo

```
 [ Início do Ciclo k ]
          |
          v
 [ Coleta de CQEs no io_uring ]
          |
          +---> Atualiza Q_inflight e RTT_smoothed (via quinn-proto)
          |
          v
 [ Cálculo da Métrica Híbrida y(t_k) ]
   y(t_k) = alpha_rtt * ((RTT_smoothed - RTT_min)/RTT_min) +
            (1 - alpha_rtt) * (Q_inflight / Q_ring_cap)
          |
          v
 [ Cálculo do Erro de Rastreamento ]
   e_k = r* - y(t_k)
          |
          v
 [ Execução da Lei de Controle PID ]
   P_k = K_p * e_k
   I_k = I_{k-1} + (K_i * e_k + K_aw * (u_{k-1}^sat - u_{k-1}^raw)) * dt
   D_k = ((2*tau_d - dt)/(2*tau_d + dt)) * D_{k-1} -
         ((2*K_d)/(2*tau_d + dt)) * (y(t_k) - y(t_{k-1}))
   u_k^raw = P_k + I_k + D_k
   u_k^sat = sat_{[u_min, u_max]}(u_k^raw)
          |
          v
 [ Injeção de Super-Buffers no SQ ]
   Submete até floor(u_k^sat) super-buffers de 64 KB com UDP_SEGMENT
          |
          v
 [ Espera de Pacing ou Próxima Conclusão CQE ]
```
