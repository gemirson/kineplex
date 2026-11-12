# Research & Architectural Analysis: TPC-H / TPC-DS Benchmarks & Whitepaper (FT-101)

## 1. Análise Comparativa de Motores Analíticos

| Dimensão Arquitetural | Apache Spark 3.5 (Catalyst / Photon) | DuckDB 0.10+ | KinePlex (FT-097 .. FT-101) |
|---|---|---|---|
| **Modelo de Execução** | Whole-stage CodeGen (JVM) / Vetorizado (C++ Photon) | Vectorized Volcano / Morsel-driven (C++) | **Whole-Stage Vectorized Pipeline em Rust nativo alinhado à Cache L1d** |
| **Otimizador CBO** | Heurístico / Baseado em Cardinalidade 1D ($P(A \land B) = P(A)P(B)$) | Dynamic Programming / Join Order Enumeration | **Riemannian Cascades CBO com Curvatura de Ollivier-Ricci ($\kappa$)** |
| **Mitigação de Skew** | Adaptive Query Execution (AQE split de partição reativa) | Sem suporte nativo distribuído (in-memory) | **Hyperbolic Skew Split analítico a priori ($\kappa < -0.15$) com Quotient Coalesce** |
| **Formato e Storage** | Parquet / Iceberg / Delta com múltiplos small files | Parquet / DuckDB file | **Laguerre CCVT (128 MB sem resíduo) + Dual Monoid Commit Iceberg/Delta** |
| **Parsing Texto/CSV** | Parser C++/Java com branches condicionais | Sniffer CSV com amostragem multithread | **KineTxt SIMD com Carry-Less em $\mathbb{F}_2[x]$ ($9.85\text{ GB/s}$)** |
| **Transporte em Cluster** | TCP / Netty com Head-of-Line Blocking | N/A (Single-Node) | **QUIC HTTP/3 sobre `io_uring SQPOLL` com Pacing PID em malha fechada** |

---

## 2. As 8 Tabelas Canônicas do Schema TPC-H

1. **`lineitem`** (Tabela Fato Principal, $\sim 75\%$ do volume de dados total):
   - Chaves: `l_orderkey` (FK), `l_partkey` (FK), `l_suppkey` (FK), `l_linenumber`.
   - Métricas: `l_quantity`, `l_extendedprice`, `l_discount`, `l_tax`.
   - Datas: `l_shipdate`, `l_commitdate`, `l_receiptdate`.
   - Flags: `l_returnflag`, `l_linestatus`, `l_shipinstruct`, `l_shipmode`.
2. **`orders`** (Tabela Fato Secundária, $\sim 15\%$ do volume):
   - `o_orderkey` (PK), `o_custkey` (FK), `o_orderstatus`, `o_totalprice`, `o_orderdate`, `o_orderpriority`, `o_clerk`, `o_shippriority`, `o_comment`.
3. **`part`** (Dimensão de Peças):
   - `p_partkey` (PK), `p_name`, `p_mfgr`, `p_brand`, `p_type`, `p_size`, `p_container`, `p_retailprice`, `p_comment`.
4. **`supplier`** (Dimensão de Fornecedores):
   - `s_suppkey` (PK), `s_name`, `s_address`, `s_nationkey` (FK), `s_phone`, `s_acctbal`, `s_comment`.
5. **`partsupp`** (Relacionamento Fornecedor-Peça):
   - `ps_partkey` (FK), `ps_suppkey` (FK), `ps_availqty`, `ps_supplycost`, `ps_comment`.
6. **`customer`** (Dimensão de Clientes):
   - `c_custkey` (PK), `c_name`, `c_address`, `c_nationkey` (FK), `c_phone`, `c_acctbal`, `c_mktsegment`, `c_comment`.
7. **`nation`** (Dimensão Geográfica de Países):
   - `n_nationkey` (PK), `n_name`, `n_regionkey` (FK), `n_comment`.
8. **`region`** (Dimensão Geográfica de Continentes):
   - `r_regionkey` (PK), `r_name`, `r_comment`.

---

## 3. Metodologia de Medição e Protocolo de Execução

1. **Warmup**: Execução prévia da consulta 1 vez para aquecimento de páginas de memória e tabelas de hash.
2. **Repetições**: 5 execuções consecutivas para cada query.
3. **Métrica Canônica**:
   $$T_{\text{reported}} = \text{median}(T_1, T_2, T_3, T_4, T_5)$$
4. **Média Geométrica (GeoMean)**:
   $$\text{GeoMean} = \left( \prod_{q=1}^{22} T_q \right)^{1/22}$$
5. **Monitoramento Microarquitetural (Linux `perf`)**:
   - `cycles`, `instructions` $\to$ IPC (Instructions Per Cycle).
   - `L1-dcache-load-misses`, `LLC-load-misses`.
   - `branch-misses` e taxa percentual de mispredictions.

---

## 4. Decisões Arquiteturais e Análise de Trade-Offs

### Decisão 1: Estruturação Modular da Suíte no Repositório
- **Decisão**: Implementar as 22 consultas TPC-H nativas diretamente em `kineplex-df/tests/tpch_queries.rs` e no módulo de benchmark `bench/tpch` (novo crate `kineplex-bench` no workspace).
- **Rationale**: Permite que as consultas funcionem como testes de regressão automatizados e contínuos em CI (`cargo test -p kineplex-df`), enquanto o runner comparativo (`kineplex-bench`) orquestra o benchmarking em grande escala (SF=1000/SF=10000) e telemetria de hardware sem poluir as dependências do core do engine.
- **Alternativas rejeitadas**:
  - *Embutir código de benchmark dentro de `kineplex-core`*: Rejeitado por violar o princípio de separação de responsabilidades e inchar a biblioteca central.
  - *Criar scripts avulsos em Python*: Rejeitado porque introduz overhead de interpretador e latência na medição de chamadas nativas de alta resolução ($< 1\,\mu\text{s}$).

### Decisão 2: Integração e Execução dos Runners Comparativos (DuckDB e Spark)
- **Decisão**: Utilizar subprocessos isolados invocando a CLI oficial do DuckDB (`duckdb -json -c "<sql>"`) e o Spark SQL Runner (`spark-sql --conf ...`) apontando para os mesmos arquivos Parquet e Delta Lake gerados pelo distribuidor de Laguerre.
- **Rationale**: O isolamento por processo garante que falhas, alocações de JVM (Spark) ou locks de C++ (DuckDB) não interfiram no espaço de endereçamento do KinePlex, permitindo medição estrita de RSS via `/proc/[pid]/status` e `getrusage()`.
- **Alternativas rejeitadas**:
  - *Vincular DuckDB via C FFI em runtime compartilhado*: Rejeitado devido ao risco de contaminação de alocadores (jemalloc/glibc) e concorrência no uso de threads.
  - *Emulação aproximada dos outros motores*: Rejeitado por falta de rigor científico; o whitepaper exige medições reais nos binários oficiais estáveis.

### Decisão 3: Coleta de Telemetria de Baixo Nível
- **Decisão**: Empregar a chamada de sistema Linux `perf_event_open` encapsulada em Rust para contagem determinística de eventos de CPU (`PERF_COUNT_HW_INSTRUCTIONS`, `PERF_COUNT_HW_CPU_CYCLES`, `PERF_COUNT_HW_CACHE_L1D_MISS`, `PERF_COUNT_HW_BRANCH_MISSES`) durante a janela exata de execução de cada query.
- **Rationale**: `perf_event_open` fornece contadores de ciclo de instrução e misses de cache por thread sem overhead de amostragem estatística, gerando evidências empíricas inquestionáveis para o whitepaper.
- **Alternativas rejeitadas**:
  - *Invocação externa de `perf stat`*: Rejeitado devido ao overhead de inicialização do executável `perf` interferindo em queries ultra-rápidas ($< 10\text{ ms}$).

### Decisão 4: Compilador do Whitepaper Científico
- **Decisão**: Criar um gerador autônomo em Rust (`kineplex-bench/src/whitepaper.rs`) que ingere o relatório JSON consolidado e renderiza o documento final `docs/whitepaper/kineplex_analytical_superiority.md` com tabelas LaTeX/Markdown, gráficos em Mermaid e blocos de análise formal.
- **Rationale**: Garante 100% de reprodutibilidade: qualquer alteração de medição no benchmark atualiza automaticamente os dados do Whitepaper sem necessidade de edição manual passível de erros.
- **Alternativas rejeitadas**:
  - *Whitepaper estático escrito à mão*: Rejeitado por rápida desatualização e incompatibilidade com o princípio de governança SDD.
