# Tasks: HTTP/3 QUIC + io_uring com Controle PID em Malha Fechada (FT-098)

**Input**: Design documents from `/specs/002-quic-iouring-pid/` (`spec.md`, `plan.md`, `research.md`, `data-model.md`, `contracts/quic_engine_api.md`, `quickstart.md`)  
**Prerequisites**: `spec.md`, `plan.md`, `research.md`, `data-model.md`, `contracts/`  
**Constitution**: Enforce TDD cycle, >= 95% coverage gate, zero-allocation in hot path, and cache line alignment (64B).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Configuração de dependências de kernel e crate `kineplex-net`

- [X] T001 Adicionar dependências `quinn-proto = "0.10"`, `io-uring = "0.6"` e `bytemuck = "1.14"` em `kineplex/kineplex-net/Cargo.toml`
- [X] T002 [P] Estruturar hierarquia de módulos em `kineplex/kineplex-net/src/quic/mod.rs` e expor módulo público em `kineplex/kineplex-net/src/lib.rs`
- [X] T003 [P] Implementar enum de erros estruturados `QuicTransportError` em `kineplex/kineplex-net/src/quic/error.rs`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Estruturas de dados fundamentais, arena de memória HugePages e canal MPSC

**⚠️ CRITICAL**: Pré-requisito bloqueante para todas as histórias de usuário

- [X] T004 [P] Implementar struct imutável de hiperparâmetros `PidControlParameters` com valores padrão em `kineplex/kineplex-net/src/quic/pid_params.rs`
- [X] T005 [P] Implementar struct de medição instantânea `HybridPressureMetric` em `kineplex/kineplex-net/src/quic/metric.rs`
- [X] T006 Implementar `RegisteredMemoryArena` (2 MB HugePages/mmap contígua particionada em slots de 64 KB) em `kineplex/kineplex-net/src/quic/arena.rs`
- [X] T007 [P] Criar testes unitários da arena de memória (alinhamento, reciclagem de slots e zero-leak) em `kineplex/kineplex-net/tests/memory_arena_tests.rs`
- [X] T008 Implementar canal MPSC desacoplado para atualização assíncrona do setpoint $r^*$ sem lock em `kineplex/kineplex-net/src/quic/mpsc.rs`

---

## Phase 3: User Story 1 - Roteamento HTTP/3 sobre QUIC com Zero-Syscall via io_uring (Priority: P1) 🎯 MVP

**Goal**: Submissão e recepção zero-syscall via `io_uring SQPOLL`, descritores de arquivo registrados `Fixed(0)`, buffers pré-registrados e super-buffers de 64 KB com hardware GSO (`UDP_SEGMENT`).

**Independent Test**: Loopback UDP local sobre anel `io_uring SQPOLL` sem chamadas de sistema no espaço de usuário em regime estacionário verificado em `quic_iouring_loopback.rs`.

### Tests for User Story 1
- [X] T009 [P] [US1] Criar teste de integração loopback do motor io_uring SQPOLL em `kineplex/kineplex-net/tests/quic_iouring_loopback.rs`

### Implementation for User Story 1
- [X] T010 [US1] Implementar inicialização do anel `io_uring` com `IORING_SETUP_SQPOLL` e detecção de fallback gracioso em `kineplex/kineplex-net/src/quic/engine.rs`
- [X] T011 [US1] Implementar registro de descritor de socket UDP `Fixed(0)` via `IORING_REGISTER_FILES` em `kineplex/kineplex-net/src/quic/engine.rs`
- [X] T012 [US1] Implementar registro dos slots de 64 KB da arena via `IORING_REGISTER_BUFFERS` em `kineplex/kineplex-net/src/quic/engine.rs`
- [X] T013 [US1] Implementar montagem e despacho de super-buffers agregados de até 64 KB com flag de controle `UDP_SEGMENT = 1350` (GSO) em `kineplex/kineplex-net/src/quic/engine.rs`
- [X] T014 [US1] Implementar trait `QuicBlockTransport` acoplado ao motor `IoUringQuicEngine` em `kineplex/kineplex-net/src/quic/transport.rs`

---

## Phase 4: User Story 2 - Estabilização de Fila e Vazão no Ponto de Kleinrock via Controlador PID (Priority: P2)

**Goal**: Modular dinamicamente a taxa de injeção $u_k$ no `io_uring` através do PID na forma de velocidade com esforço híbrido $y(t_k)$, anti-windup por back-calculation e filtro derivativo na medida.

**Independent Test**: Simulação de resposta ao degrau em `pid_controller_tests.rs` comprovando convergência de $y(t_k) \to r^*$, ausência de overshoot via back-calculation e amortecimento de derivative kick.

### Tests for User Story 2
- [X] T015 [P] [US2] Criar testes unitários para a lei de controle PID ($y(t_k)$, $I_k$, $D_k$, $u_k$, anti-windup e filtro $\tau_d$) em `kineplex/kineplex-net/tests/pid_controller_tests.rs`

### Implementation for User Story 2
- [X] T016 [US2] Implementar struct `PidControllerState` com alinhamento de cache de 64 bytes (`#[repr(C, align(64))]`) em `kineplex/kineplex-net/src/quic/pid.rs`
- [X] T017 [US2] Implementar cálculo analítico de $y(t_k) = \alpha_{\text{rtt}} \left(\frac{\text{RTT}_{\text{smoothed}} - \text{RTT}_{\min}}{\text{RTT}_{\min}}\right) + (1 - \alpha_{\text{rtt}}) \left(\frac{Q_{\text{in-flight}}}{Q_{\text{ring\_cap}}}\right)$ em `kineplex/kineplex-net/src/quic/pid.rs`
- [X] T018 [US2] Implementar termo integral com anti-windup por back-calculation: $I_k = I_{k-1} + (K_i e_k + K_{aw}(u_{k-1}^{\text{sat}} - u_{k-1}^{\text{raw}})) \Delta t_k$ em `kineplex/kineplex-net/src/quic/pid.rs`
- [X] T019 [US2] Implementar termo derivativo filtrado sobre a medida (*Derivative-on-Measurement*): $D_k = \frac{2\tau_d - \Delta t_k}{2\tau_d + \Delta t_k} D_{k-1} - \frac{2 K_d}{2\tau_d + \Delta t_k}(y(t_k) - y(t_{k-1}))$ em `kineplex/kineplex-net/src/quic/pid.rs`
- [X] T020 [US2] Integrar cálculo de pacing e limite de batching $u_k = \text{sat}_{[u_{\min}, u_{\max}]}(P_k + I_k + D_k)$ no loop de submissão do `IoUringQuicEngine` em `kineplex/kineplex-net/src/quic/engine.rs`

---

## Phase 5: User Story 3 - Resiliência e Multiplexação Imune a Head-of-Line Blocking (Priority: P3)

**Goal**: Garantir que perdas de pacotes induzidas em um stream QUIC isolado não degradem a entrega dos demais 63 streams concorrentes.

**Independent Test**: Teste de concorrência com descarte de 10% no stream #3 comprovando avanço desimpedido dos demais streams em `quic_stream_isolation_tests.rs`.

### Tests for User Story 3
- [X] T021 [P] [US3] Criar teste de injeção de falhas e isolamento de streams em `kineplex/kineplex-net/tests/quic_stream_isolation_tests.rs`

### Implementation for User Story 3
- [X] T022 [US3] Implementar multiplexador concorrente de streams QUIC desacoplados em `kineplex/kineplex-net/src/quic/transport.rs`
- [X] T023 [US3] Implementar controle de fluxo por stream integrado ao motor `quinn-proto` em `kineplex/kineplex-net/src/quic/transport.rs`

---

## Phase 6: User Story 4 - Integração de Ponta a Ponta com Ignis e Storage S3/MinIO (Priority: P4)

**Goal**: Conectar o transporte QUIC HTTP/3 ao ecossistema do `LaguerreOptimalDistributor` com modulação de setpoint em tempo real.

**Independent Test**: Teste end-to-end transmitindo batches colunares serializados em Arrow/Parquet via `QuicBlockTransport`.

### Tests for User Story 4
- [X] T024 [P] [US4] Criar teste de integração entre `LaguerreOptimalDistributor` e `QuicBlockTransport` em `kineplex/kineplex-net/tests/quic_laguerre_integration_tests.rs`

### Implementation for User Story 4
- [X] T025 [US4] Implementar adaptador de canal MPSC no `LaguerreOptimalDistributor` para reprogramação contínua de $r^*$ sem derivative kick em `kineplex/kineplex-net/src/quic/mpsc.rs`

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Telemetria, benchmarks e validação completa do workspace

- [X] T026 [P] Implementar snapshot serializável `PidTelemetrySnapshot` em `kineplex/kineplex-net/src/quic/transport.rs`
- [X] T027 [P] Criar benchmark comparativo de vazão e ciclos de CPU em `kineplex/kineplex-net/benches/quic_bench.rs`
- [X] T028 Executar validação global de compilação e suíte de testes (`cargo test --workspace`) assegurando cobertura $\ge 95\%$ e conformidade integral com a Constituição

---

## Dependencies & Execution Order

### Phase Dependencies
- **Phase 1 (Setup)**: Sem dependências externas.
- **Phase 2 (Foundational)**: Depende de Phase 1 (BLOQUEIA todas as histórias de usuário).
- **Phase 3 (User Story 1 - MVP)**: Depende de Phase 2.
- **Phase 4 (User Story 2)**: Depende de Phase 2 e Phase 3.
- **Phase 5 (User Story 3)**: Depende de Phase 3.
- **Phase 6 (User Story 4)**: Depende de Phase 3 e Phase 4.
- **Phase 7 (Polish)**: Depende de todas as fases anteriores.

### Parallel Opportunities
- Tarefas T002, T003 (Phase 1) podem rodar em paralelo.
- Tarefas T004, T005, T007 (Phase 2) podem rodar em paralelo.
- Testes T009, T015, T021, T024, T026, T027 podem ser desenvolvidos em paralelo aos seus respectivos componentes.
