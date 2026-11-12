# Implementation Plan: HTTP/3 QUIC + io_uring com Controle PID em Malha Fechada (FT-098)

**Branch**: `002-quic-iouring-pid` | **Date**: 2026-11-10 | **Spec**: [specs/002-quic-iouring-pid/spec.md](spec.md)

**Input**: Feature specification from `specs/002-quic-iouring-pid/spec.md`

## Summary

Implementação do motor de rede de alto desempenho baseado em HTTP/3 sobre QUIC (`quinn-proto`) acoplado diretamente ao `io_uring` do kernel Linux na crate `kineplex-net`, governado em tempo real por um Controlador PID Discreto na forma de velocidade com variável híbrida de esforço $y(t_k)$, anti-windup por back-calculation e filtro derivativo na medida (*Derivative-on-Measurement*), operando em *zero-syscall* estacionário (`IORING_SETUP_SQPOLL`) e super-buffers de 64 KB via hardware GSO/GRO (`UDP_SEGMENT`).

---

## Technical Context

**Language/Version**: Rust 1.75+ (Edition 2021)  
**Primary Dependencies**:
- `kineplex-core` (Workspace v0.2.0)
- `quinn-proto = "0.10"` (QUIC determinístico sans-I/O)
- `io-uring = "0.6"` (Submissão e conclusão de baixo nível em anéis de kernel)
- `bytemuck = "1.14"` (Casting seguro zero-cópia para buffers fixados)
- `tokio` (Workspace v1.35)
- `thiserror`, `anyhow`, `parking_lot`

**Storage**: N/A (Camada de Transporte e Rede; conecta-se assincronamente a S3/MinIO e nós remotos)  
**Testing**: `cargo test -p kineplex-net --test pid_controller_tests`, `cargo test -p kineplex-net --test memory_arena_tests`, `cargo test -p kineplex-net --test quic_iouring_loopback`  
**Target Platform**: Linux x86_64 / aarch64 (Kernel 5.10+ / 6.1+ LTS, Amazon Linux 2023, AWS Glue 5.0)  
**Project Type**: Workspace Crate (`kineplex-net`)  
**Performance Goals**:
- $\ge 20\text{ Gbps}$ sustentados por nó em instâncias AWS EC2 com ENA;
- Redução de $\ge 99.5\%$ em syscalls no thread de I/O em regime estável (`SQPOLL`);
- Latência $p99 \le 1.25 \times \text{RTT}_{\min}$ (eliminação de bufferbloat no Ponto de Kleinrock).  

**Constraints**:
- Hot-path de transmissão estritamente zero-allocation na heap;
- Estrutura de estado do PID alinhada a 64 bytes (`#[repr(C, align(64))]`) para prevenir false sharing;
- Fallback gracioso para modo sem `SQPOLL` caso o host não possua privilégios `CAP_SYS_ADMIN`.  

**Scale/Scope**:
- 64 streams QUIC paralelos por conexão;
- Arena de 2 MB particionada em slots de 64 KB para super-buffers GSO.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Princípio da Constituição | Verificação no Projeto | Status |
|---|---|---|
| **1. Rigor Matemático** | Equações formais de $y(t_k)$, $I_k$ (anti-windup back-calculation), $D_k$ (filtro $\tau_d$) e $u_k$ rigorosamente preservadas sem aproximações heurísticas espúrias. | **PASS** |
| **2. Zero-Copy & Alinhamento** | Buffers contíguos registrados no anel `io_uring` (`IORING_REGISTER_BUFFERS`), fatiamento seguro de super-buffers e struct `PidControllerState` alinhada a 64 bytes. | **PASS** |
| **3. Isolamento de I/O** | O núcleo analítico (`kineplex-core`) permanece desacoplado de sockets e drivers de kernel; a camada de transporte é isolada em `kineplex-net` sob traits limpas (`QuicBlockTransport`). | **PASS** |
| **4. Governança SDD & CI** | Fluxo rigoroso do Spec Kit com spec, plan, data model, contratos, quickstart e suite de testes com cobertura $\ge 95\%$. | **PASS** |

---

## Project Structure

### Documentation (this feature)

```text
specs/002-quic-iouring-pid/
├── spec.md              # Especificação formal completa
├── plan.md              # Este plano de implementação
├── research.md          # Decisões arquiteturais (Phase 0)
├── data-model.md        # Entidades de domínio e layouts (Phase 1)
├── quickstart.md        # Guia de validação executável (Phase 1)
├── contracts/           # Contratos de interface pública (Phase 1)
│   └── quic_engine_api.md
└── checklists/
    └── requirements.md  # Checklist de conformidade
```

### Source Code (`kineplex/kineplex-net`)

```text
kineplex/kineplex-net/
├── Cargo.toml
├── src/
│   ├── lib.rs                      # Exportação dos novos módulos quic e pid
│   ├── gossip.rs                   # (existente)
│   ├── protocol.rs                 # (existente)
│   ├── routing.rs                  # (existente)
│   └── quic/                       # [NOVO MÓDULO]
│       ├── mod.rs                  # Módulo raiz de transporte QUIC/io_uring
│       ├── pid.rs                  # Controlador PID Discreto em malha fechada
│       ├── arena.rs                # Arena de 2 MB em HugePages registrada
│       ├── engine.rs               # Loop io_uring com SQPOLL, Fixed(0) e GSO
│       ├── transport.rs            # Implementação do trait QuicBlockTransport
│       └── mpsc.rs                 # Receptor assíncrono de setpoint r*
└── tests/
    ├── pid_controller_tests.rs     # Testes da lei PID e anti-windup
    ├── memory_arena_tests.rs       # Testes da arena de 2 MB e slots
    └── quic_iouring_loopback.rs    # Teste de integração end-to-end
```

---

## Complexity Tracking

Nenhuma violação aos princípios da Constituição foi detectada. A complexidade do anel `io_uring` e da máquina de estados do QUIC fica estritamente encapsulada em `kineplex-net::quic`, preservando o núcleo computacional `kineplex-core` 100% puro e testável em memória.
