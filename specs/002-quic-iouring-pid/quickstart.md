# Quickstart: Validação do Motor HTTP/3 QUIC + io_uring com PID (FT-098)

## 1. Pré-Requisitos do Ambiente
- **Sistema Operacional**: Linux (Kernel $\ge 5.10$, preferencialmente $\ge 6.1$ como Amazon Linux 2023).
- **Toolchain Rust**: `rustc` 1.75+ e `cargo`.
- **Permissões de Sistema**:
  - Para validação de `SQPOLL`, o processo deve possuir capacidade `CAP_SYS_ADMIN` ou estar rodando em ambiente compatível.
  - Se executado sem privilégios em contêiner ou kernel não suportado, o fallback para modo de submissão dirigida por eventos é acionado automaticamente.

---

## 2. Cenários de Validação Rápida

### Cenário 1: Teste Unitário da Lei de Controle PID e Anti-Windup
Valida a convergência assintótica de $y(t_k) \to r^*$, a resposta ao degrau e o congelamento do integrador via back-calculation sob saturação.

```bash
cargo test -p kineplex-net --test pid_controller_tests
```

**Resultado Esperado**:
- Todos os testes passam em $< 0.05\text{s}$.
- Verificação do amortecimento de *derivative kick* ao comutar $r^*$ de $0.25$ para $0.50$.
- O erro de rastreamento converge com tolerância $< 10^{-4}$.

---

### Cenário 2: Validação da Arena de Memória Registrada e Slots de 64 KB
Valida o particionamento da arena de 2 MB em HugePages/mmap e a reciclagem sem alocação dos slots para os super-buffers GSO.

```bash
cargo test -p kineplex-net --test memory_arena_tests
```

**Resultado Esperado**:
- Alocação contígua e alinhamento de 4096B / 2MB validados.
- Zero vazamentos de slots livres após 100.000 ciclos de empréstimo e devolução.

---

### Cenário 3: Teste de Integração Loopback QUIC sobre io_uring
Transmite 100 blocos de 64 MB em 32 streams QUIC paralelos sobre loopback local UDP.

```bash
cargo test -p kineplex-net --test quic_iouring_loopback -- --nocapture
```

**Resultado Esperado**:
- Zero pacotes descartados.
- Throughput saturado no barramento local.
- Instantâneo de telemetria comprova operação contínua do PID modulando $u_k$.
