# Plano: Métricas por job com overhead ~zero — "Metrics Budget Controller"

> Aprovado em 2026-11-08. Especialista bare-metal. Coleta de métricas por job sem impactar
> a performance da aplicação, com base matemática.

## Diagnóstico do estado atual (por que hoje custa caro)

| Problema | Onde | Efeito |
|---|---|---|
| `RwLock<Vec<Metric>>` + push por evento | `kineplex-core/src/metrics.rs:81-117` | lock de escrita + **alocação por evento** + `Vec` sem limite (cresce para sempre) |
| `make_key` formata `String` por evento | `metrics.rs:119-132` | 2+ allocações no caminho quente |
| Métricas de job com valor fixo `500_000` | `kineplex-server/src/handlers.rs:569-573` | não é medição, é enfeite |
| Sem métricas por estágio durante execução | `execute_real_pipeline` | só `total_time_ms` no fim |

## Ideia central (a parte inovadora)

**Duas teses matemáticas + uma de controle:**

1. **Métrica = monoide fundível (mergeable monoid).** Todo valor de métrica (contador,
   média/variancia, sketch de quantil) implementa `merge(a,b)` com leis comprovadas por teste:
   associativo, comutativo, elemento identidade. Consequência: agregação distribuída entre nós
   **sem coordenação e sem enviar amostras cruas** — igual CRDT.

2. **Estatística suficiente, não amostra crua.** Em vez de gravar latências, coletamos apenas
   a estatística mínima que preserva a inferência:
   - **Welford online** → média/variancia em 1 passada, O(1) memória, estável numericamente;
   - **DDSketch** → buckets `⌊ln v / ln(1+α)⌋`: erro relativo **garantido** ≤ α (ex.: 1%),
     fundível (merge = soma de buckets), O(1/α) buckets.

3. **Loop fechado sobre o próprio custo da observação** (a inovação): o coletor **se mede**
   (delta de relógio amostrado ao redor da própria gravação) e um **controlador PI ajusta a
   taxa de amostragem `p`** para manter `overhead_ratio ≤ orçamento` (1% de CPU do job).
   A telemetria se auto-calibra — reaproveitamos a filosofia do PID (FT-089), agora aplicada
   ao observador, não ao dado.

## Caminho quente (bare-metal)

- Handle `JobMetrics` alocado **uma vez por job**, `#[repr(align(64))]` (anti false-sharing),
  campos `AtomicU64` com **`Ordering::Relaxed`** (~1 ns, sem lock);
- ponteiro **thread-local** do job corrente durante o estágio → gravação = `fetch_add` direto,
  **zero alloc, zero lock, zero `format!`**;
- temporização de estágio com `Instant` (vDSO no Linux) — 4 leituras por job;
- perda permitida (lossy) em eventos de alta taxa, com contador de drops — **nunca bloquear o job**;
- leitura do Prometheus monta string só no scrape, com **cache de 1 s** (DoD do FT-038),
  a partir de snapshot imutável.

## Decisões aprovadas

| # | Questão | Decisão |
|---|---------|---------|
| 1 | Orçamento de overhead | **≤ 1% de CPU por job** (`OVERHEAD_BUDGET` = 0.01) |
| 2 | Onde instrumentar primeiro | **`kineplex-server` `execute_real_pipeline`** + primitivas em `kineplex-core`; `PipelineExecutor` (node) na mesma fase |
| 3 | Precisão do quantil | **α = 1%** (DDSketch: erro relativo ≤ 1% no p95) |
| 4 | Cobertura | **100% dos jobs** para contadores/timers; amostragem **só** para eventos de alta taxa, com `p` ajustado pelo PI |

## Arquivos novos/alterados

```
kineplex/kineplex-core/src/job_metrics.rs      NOVO  — monoide, Welford, DDSketch, JobScope
kineplex/kineplex-core/src/job_registry.rs     NOVO  — registry sharded job_id → Arc<JobMetrics>
kineplex/kineplex-core/src/budget.rs           NOVO  — controlador PI de overhead (Fase 4)
kineplex/kineplex-core/src/lib.rs              EDIT  — pub mod job_metrics/job_registry/budget
kineplex/kineplex-core/src/metrics.rs          EDIT  — deprecar push no Vec; exportar do registry
kineplex/kineplex-core/tests/job_metrics.rs    NOVO  — leis do monoide + sketches + Welford
kineplex/kineplex-core/tests/local_perf.rs     EDIT  — cenário de overhead (ligado vs desligado)
kineplex/kineplex-server/src/handlers.rs       EDIT  — instrumentar execute_real_pipeline
kineplex/kineplex-server/src/state.rs          EDIT  — JobMetricsDto real vindo do handle
kineplex/kineplex-node/src/execution.rs        EDIT  — stage timers via JobScope
kineplex/docs/observability.md                 EDIT  — método + resultados (FT-038/079)
```

Sem dependências novas (benchmarks como `#[test]` com `--nocapture`, padrão de `local_perf.rs`).

## Fases

### Fase 1 — Contrato matemático (`job_metrics.rs`)
- `trait Merge { fn merge(&mut self, other: &Self); fn identity() -> Self; }` — leis testadas
  por propriedade (pares + aleatórios deterministas via seed fixa).
- `Counter` (AtomicU64 relaxed), `Welford` (count/mean/M2 → `variance()`),
  `DdSketch { α, buckets }` com `floor(ln v / ln(1+α))` e `merge` = soma de buckets;
  `quantile(q)` invertendo a CDF.
- Constante `OVERHEAD_BUDGET: f64 = 0.01`.
- **Critério:** testes das leis do monoide; erro do DDSketch ≤ α verificado contra dataset
  brute-force; `cargo test -p kineplex-core` verde.

### Fase 2 — Hot path bare-metal (`JobScope`)
- `#[repr(align(64))] struct JobCounters` com `AtomicU64` (`Relaxed`).
- `thread_local! { static CURRENT_JOB: Cell<...> }` — `record_rows`, `record_stage_ms`,
  `record_event` = 1 `fetch_add`, **sem alloc/lock/format**.
- `stage_timer()` devolve guard com `Instant`; dtor grava no contador do job.
- **Critério:** ≥ 10M gravações em < 1s; teste de 8 threads sem panic/data race.

### Fase 3 — Registro + exportação
- `JobMetricsRegistry`: lock **só** em `begin_job`/`end_job`; gravações nunca tocam o mapa.
- `export_prometheus()` monta `kineplex_job_stage_ms{job,stage}`, `kineplex_job_rows_total{job}`,
  `kineplex_job_p95_ms{job}`; **cache de 1 s** (DoD FT-038).
- `MetricsCollector` deixa de fazer `push` por evento no `Vec` (corta o vazamento).
- **Critério:** séries por job no scrape; cache (2 chamadas no mesmo segundo → mesma String).

### Fase 4 — Controlador PI de overhead (`budget.rs`)
- Amostra 1/N gravações medindo custo real (delta de `Instant` ao redor da escrita);
  `overhead_ratio = custo_amortizado / tempo_decorrido`.
- `BudgetController { kp, ki, budget }` → ajusta `p ∈ [p_min, 1.0]`; `p` é `AtomicU64`
  (Q16.16) lido no hot path com relaxed load.
- Gauges: `kineplex_metrics_overhead_ratio`, `kineplex_metrics_sample_rate`.
- **Critério:** planta simulada (overhead ∝ p) converge ≤ budget em < 20 iterações,
  oscilação < 20% do setpoint; teste de rajada de jobs.

### Fase 5 — Integração E2E + validação
- `execute_real_pipeline`: `begin_job(graph_id)`; por estágio `record_stage_ms`; no fim
  preencher `JobMetricsDto` **real** (remover `500_000` fixo) e `end_job()`.
- `PipelineExecutor::execute`: mesmo padrão nos 4 estágios.
- Teste on/off (feature flag / `p=0`): delta de wall-time ≤ 1%.
- `docs/observability.md`: método matemático + tabela de overhead medida.
- **Critério:** `cargo test --workspace` verde; evidência numérica do ≤ 1% documentada.

## Riscos e mitigações

| Risco | Mitigação |
|---|---|
| `thread_local` com lifetimes em async | ponteiro bruto + `Arc` mantida viva pelo registry até `end_job`; job nunca reporta após fim |
| Thread-local não vê job em task tokio migratória | setter chamado no início de cada estágio (não uma vez por task) |
| Flutuação do PI causando ruído no scrape | janela de suavização exponencial (λ = e^{-Δt/τ}) no cálculo de `overhead_ratio` |
| Cardialidade de labels `job` alta no Prometheus | TTL: job concluído fica no registry 60 s e depois some do `/metrics` |
