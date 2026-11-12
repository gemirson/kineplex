# Implementation Plan: TPC-H / TPC-DS Benchmarks & Scientific Whitepaper (FT-101)

**Branch**: `005-tpch-tpcds-benchmarks` | **Date**: 2026-11-11 | **Spec**: [specs/005-tpch-tpcds-benchmarks/spec.md](spec.md)

**Input**: Feature specification from `specs/005-tpch-tpcds-benchmarks/spec.md`

---

## Summary

Implementação do arcabouço completo de benchmarking padronizado e comprovação empírica da superioridade analítica do KinePlex:
1. **Módulo de Consultas Analíticas Canônicas (`kineplex-df`)**: Implementação das 22 consultas analíticas padrão TPC-H (Q1 a Q22) e 6 consultas de TPC-DS em esquema Snowflake (DS-Q01, DS-Q03, DS-Q14, DS-Q64, DS-Q72, DS-Q95), alavancando Whole-Stage SIMD, otimizador CBO por Curvatura de Ollivier-Ricci, Conexão de Galois e particionamento de Laguerre CCVT.
2. **Gerador e Particionador de Dados TPC-H (`kineplex-bench/tpch_datagen`)**: Geração e conversão de dados sintéticos para escalas SF=1 (1GB para CI e testes locais), SF=100, SF=1000 (1TB) e SF=10000 (10TB) com fatiamento em blocos ótimos de 128 MB sem resíduos.
3. **Harness Comparativo Automatizado (`kineplex-bench/comparative_runner`)**: Execução pareada e isolada por processos contra **DuckDB** e **Apache Spark** sob as mesmas condições de hardware e arquivos Parquet/Delta, com coleta de tempo de warmup, 5 repetições, mediana estatística, desvio padrão e telemetria de hardware (IPC, L1d/LLC cache misses, branch mispredictions via `perf_event_open`).
4. **Compilador Autônomo do Whitepaper Científico (`kineplex-bench/whitepaper`)**: Geração determinística do relatório formal `docs/whitepaper/kineplex_analytical_superiority.md` com tabelas comparativas, gráficos em formato Markdown/Mermaid e seções de reprodutibilidade científica.

---

## Technical Context

**Language/Version**: Rust 1.75+ (Edition 2021)  
**Primary Dependencies**:
- `kineplex-core = { path = "../../kineplex-core", version = "0.2.0" }`
- `kineplex-df = { path = "../../kineplex-df", version = "0.2.0" }`
- `arrow = { version = "46.0", features = ["ipc"] }`
- `arrow-array = "46.0"`
- `parquet = "46.0"`
- `rayon = "1.8"`
- `serde = { version = "1.0", features = ["derive"] }`
- `serde_json = "1.0"`
- `clap = { version = "4.4", features = ["derive"] }`

**Storage**: Arquivos de dados Parquet (Apache Iceberg v2 e Delta Lake v2) gerados na arena contígua de 128 MB pelo `LaguerreOptimalDistributor` e mantidos em NVMe SSD com buffers de I/O zerados em cold/warm cache.  
**Testing**:
- `cargo test -p kineplex-df --test tpch_queries_tests`
- `cargo test -p kineplex-bench --test tpch_datagen_tests`
- `cargo test -p kineplex-bench --test comparative_runner_tests`
- `cargo test -p kineplex-bench --test whitepaper_compiler_tests`

**Target Platform**: Linux x86_64 (AVX2, AVX-512BW, VPCLMULQDQ) e Linux ARM64 (NEON, Crypto Extension).  
**Project Type**: Benchmark Harness & Scientific Publication Pipeline (Rust CLI + Integration Suite).  
**Performance Goals**:
- $\ge 20\text{ GB/s}$ de throughput sustentado em varreduras e agregações massivas (Q1 e Q6) por nó de 64 núcleos;
- Speedup $\ge 2.0\times$ em relação ao DuckDB (GeoMean de latência) em escala 1TB (SF=1000);
- Speedup $\ge 5.0\times$ em relação ao Apache Spark (GeoMean de latência) em escala 1TB (SF=1000);
- Medição com precisão de microssegundos ($< 1\,\mu\text{s}$) e contagem determinística de ciclos de CPU via `perf_event_open`.

**Constraints**:
- Consumo máximo de memória RSS $\le 32\text{ GB}$ durante a bateria completa de 1TB (proteção total contra OOM);
- Zero acoplamento de binários externos no core do engine (execução de DuckDB e Spark via subprocessos isolados);
- Preservação da meta estrita de governança do workspace: cobertura de testes $\ge 95\%$.

**Scale/Scope**:
- Escalas SF=1 (~1GB para validação de testes), SF=100 (~100GB), SF=1000 (~1TB, 6 bilhões de linhas de `lineitem`), SF=10000 (~10TB).
- 22 consultas analíticas TPC-H + 6 consultas de TPC-DS.

---

## Constitution Check

| Princípio da Constituição | Verificação no Projeto | Status |
|---|---|---|
| **1. Rigor Matemático** | Validação empírica das 5 inovações formais (Transporte Ótimo de Laguerre-Kantorovich CCVT, Curvatura de Ollivier-Ricci $\kappa$, Monoides de Commit Lakehouse, Álgebra de Polinômios em $\mathbb{F}_2[x]$ e Pacing PID QUIC) com cálculo de intervalos de confiança de 95% e médias geométricas canônicas. | **PASS** |
| **2. Zero-Copy & Alinhamento** | Leitura e varredura de tabelas Arrow/Parquet sem cópia intermediária de buffers; batches de agregação e tabelas hash alinhados a 64 bytes (cache line friendly). | **PASS** |
| **3. Isolamento de I/O** | Motores externos (DuckDB e Spark) isolados em processos filhos com medição independente de RSS e CPU; engine nativo KinePlex processando via pipelines vetorizados em memória. | **PASS** |
| **4. Governança SDD & CI** | Especificação, plano arquitetural, contratos, data model, quickstart e testes com cobertura $\ge 95\%$. | **PASS** |

---

## Project Structure

### Documentation (`specs/005-tpch-tpcds-benchmarks/`)
```text
specs/005-tpch-tpcds-benchmarks/
├── spec.md              # Especificação formal completa (FT-101)
├── plan.md              # Este plano de implementação
├── research.md          # Decisões arquiteturais e análise de trade-offs
├── data-model.md        # Entidades de dados, layouts e structs Rust
├── quickstart.md        # Guia rápido de execução e validação
├── contracts/           # Contratos públicos da API do benchmark
│   └── tpch_benchmark_api.md
└── checklists/
    └── requirements.md  # Checklist de conformidade com a Constituição
```

### Source Code (`kineplex/`)
```text
kineplex/
├── kineplex-df/
│   ├── src/
│   │   └── ...                          # Módulos existentes de Catalyst, Decimal, Kernel e SQL
│   └── tests/
│       ├── tpch_queries_tests.rs        # Implementação e validação canônica de Q1..Q22 em SF=1
│       └── tpcds_samples_tests.rs       # Implementação e validação de 6 queries TPC-DS
│
├── bench/tpch/                          # [NOVO PACOTE: kineplex-bench]
│   ├── Cargo.toml                       # Manifesto do crate de benchmark (workspace member)
│   ├── src/
│   │   ├── lib.rs                       # Exportação dos runners, traits e telemetria
│   │   ├── config.rs                    # BenchmarkRunConfig e argumentos CLI
│   │   ├── metrics.rs                   # Coleta de tempo, memória RSS e IPC
│   │   ├── perf_events.rs               # Wrapper Linux perf_event_open para contadores de CPU
│   │   ├── datagen.rs                   # Gerador TPC-H dbgen e particionador Laguerre CCVT
│   │   ├── runner_kineplex.rs           # Executor nativo KinePlex Whole-Stage SIMD
│   │   ├── runner_duckdb.rs             # Subprocess runner para DuckDB
│   │   ├── runner_spark.rs              # Subprocess runner para Apache Spark
│   │   ├── comparative.rs               # Orquestrador comparativo e cálculo de speedups
│   │   └── whitepaper.rs                # Compilador e gerador do Whitepaper em Markdown
│   ├── bin/
│   │   ├── tpch_datagen.rs              # CLI: Geração e partição de dados
│   │   ├── tpch_runner.rs               # CLI: Execução da suíte no KinePlex
│   │   ├── comparative_runner.rs        # CLI: Execução comparativa pareada
│   │   └── generate_whitepaper.rs       # CLI: Compilação do Whitepaper científico
│   └── tests/
│       ├── datagen_tests.rs
│       ├── comparative_runner_tests.rs
│       └── whitepaper_compiler_tests.rs
│
└── docs/whitepaper/
    └── kineplex_analytical_superiority.md # Relatório técnico final compilado
```

---

## Complexity Tracking

Nenhuma violação aos princípios da Constituição detectada. O novo crate `bench/tpch` (`kineplex-bench`) opera como um consumidor desacoplado do `kineplex-df` e `kineplex-core`, preservando a pureza das bibliotecas centrais do engine analítico.
