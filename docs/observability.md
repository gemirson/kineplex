# Observabilidade Operacional - FT-079

## Visão Geral

Este documento detalha a implementação de observabilidade para o pipeline de dados KinePlex, permitindo medição real de métricas de pipeline e tomada de decisão baseada em dados para prontidão de produção.

## Métricas Coletadas

### Métricas de Pipeline

| Métrica | Tipo | Descrição |
|---------|------|-----------|
| `kineplex_stage_latency_ms` | Histogram | Latência por estágio em milliseconds |
| `kineplex_stage_rows` | Counter | Linhas processadas por estágio |
| `kineplex_stage_errors` | Counter | Erros por estágio |
| `kineplex_stage_bytes` | Counter | Bytes transferidos entre estágios |
| `kineplex_total_execution_time_ms` | Histogram | Tempo total de execução E2E |
| `kineplex_allocation_failures` | Counter | Falhas de alocação de nós |
| `kineplex_retries` | Counter | Número de tentativas de execução |

### Métricas de Recursos

| Métrica | Tipo | Descrição |
|---------|------|-----------|
| `kineplex_memory_peak_bytes` | Gauge | Pico de memória em bytes |
| `kineplex_cpu_time_ms` | Gauge | Tempo de CPU em milliseconds |
| `kineplex_network_bytes_sent` | Counter | Bytes enviados via rede |
| `kineplex_network_bytes_received` | Counter | Bytes recebidos via rede |

### Métricas por Tenant

| Métrica | Tipo | Descrição |
|---------|------|-----------|
| `kineplex_tenant_graphs_active` | Gauge | Grafos ativos por tenant |
| `kineplex_tenant_quota_used` | Gauge | Quota utilizada por tenant |
| `kineplex_tenant_executions_total` | Counter | Total de execuções por tenant |

## Integração Prometheus/OTLP

### Configuração Prometheus

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'kineplex'
    static_configs:
      - targets: ['localhost:9090']
    metrics_path: '/metrics'
```

### Exportação OTLP

```rust
use opentelemetry::sdk::export::metrics::OTLPMetricExporter;

let exporter = OTLPMetricExporter::new(
    otlp_endpoint,
    OTLPMetricConfig::default()
);
```

## Dashboard de SLO

O dashboard deve exibir:

1. **Availability** - Percentual de requisições bem-sucedidas
2. **Latência p95** - Latência no percentil 95
3. **Throughput** - Requisições por segundo
4. **Error Rate** - Taxa de erros
5. **Resource Usage** - Uso de CPU, memória, rede

## Campos de Relatório E2E

O harness E2E agora preenche campos críticos que antes eram null:

```yaml
e2e_report:
  stage_latency:
    receptor_ms: 15
    wasm_ms: 150
    aggregation_ms: 45
    terminal_ms: 30
  total_time_ms: 240
  rows_processed: 10000
  allocation_time_ms: 50
  retry_count: 0
  allocation_failures: 0
```

## Metrics Budget Controller (FT-038) — método

Métricas por job com **overhead ≤ 1% de CPU por job** (`OVERHEAD_BUDGET = 0.01`),
implementado em `kineplex-core` (`job_metrics.rs`, `job_registry.rs`, `budget.rs`).

### Método matemático

1. **Caminho quente (monoide + sketches)**
   - `JobCounters` (`#[repr(align(64))]`) com `AtomicU64`/`Ordering::Relaxed` — sem
     lock, sem alloc, sem `format!`. Handle thread-local do job corrente →
     `record_rows`/`record_event` = 1 `fetch_add`.
   - Distribuições online: **Welford** (média/variação) e **DDSketch α = 1%**
     (erro relativo ≤ 1% no p95). `trait Merge` com leis testadas
     (associatividade, identidade, comutatividade).
   - Perdas permitidas (lossy) em eventos de alta taxa; contagem em
     `stage_drops` — o job nunca bloqueia.

2. **Controlador PI de overhead** (`BudgetController`, filosofia do PID FT-089)
   - Amostra o custo real da observação (`measure_cost_ns` ao redor da escrita)
     e calcula `overhead_ratio = custo_amortizado / janela_decorrida`.
   - PI com anti-windup: `p ← clamp(p + kp·e + ki·Σe, p_min, 1.0)`, `e = budget − ratio`,
     ganhos `kp = 0.5`, `ki = 0.2`, `p_min = 0.01`, Q16.16 atômico.
   - `p` é a **taxa de amostragem** lida no caminho quente (relaxed load);
     contadores/timers de job seguem 100%, amostragem só para eventos de alta taxa.
   - Gauges no `/metrics`: `kineplex_metrics_overhead_ratio`,
     `kineplex_metrics_sample_rate`.

3. **Loop real no scrape** — `GET /metrics` (`kineplex-server/src/handlers.rs`)
   mede o próprio custo de observação (concat de exporters + formatação) e chama
   `AppState::tick_metrics_budget(cost_ns)` → `BudgetController::tick` no fim de
   cada scrape. Exportação de jobs com **cache de 1 s** (DoD FT-038).

4. **Instrumentação E2E**
   - `execute_real_pipeline` (server): `begin_job(graph_id)` → timers por estágio
     → `rows_processed` real do snapshot (removido o `500_000` fixo) → `end_job`.
   - `PipelineExecutor::execute` (node): registry interno + `JobScope` por estágio
     (re-criado no início de cada estágio — seguro contra task tokio migratória);
     `end_job` garantido mesmo em erro.

### Tabela de overhead medida

Teste `metrics_overhead_on_off_within_budget` (`kineplex-core/tests/local_perf.rs`),
pares intercalados ON/OFF (10 reps × 2M linhas), **mediana de cada braço**
(robusta a jitter de ±ms do scheduler em debug e à contenção de testes irmãos):

| Custo | Medido |
|---|---|
| Job instrumentado completo (begin + 4 estágios + end) | **~4 µs** |
| Overhead mediano ON vs OFF | **≤ 1.00%** em todas as execuções (típico ~0.45%) |
| Orçamento (`OVERHEAD_BUDGET`) | **1.00%** — ✅ atendido |

Evidência: execuções isoladas 0.44% / 0.04% / 0.24% / 0.73% / 0.52%;
suite completa (`cargo test --workspace`, 3 testes de `local_perf` em paralelo)
3× consecutivas verdes. Jitter por rep de até ±2 ms é absorvido pela mediana
dos braços interleaved (contenção atinge OFF e ON igualmente).