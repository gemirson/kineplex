# Feature Specification: Suíte de Benchmarks TPC-H / TPC-DS (1TB e 10TB) e Whitepaper Científico do KinePlex (FT-101)

**Feature Branch**: `005-tpch-tpcds-benchmarks`  
**Feature ID**: FT-101 / SPEC-005  
**Created**: 2026-11-11  
**Status**: Complete  
**Target Crates / Modules**: `kineplex-core`, `kineplex-df`, `bench/tpch`, `docs/whitepaper`  
**Author**: Antigravity Pair Programmer / KinePlex Team  
**Input**: Especificar a suíte padronizada de benchmarks comparativos TPC-H (Q1 a Q22) e amostras complexas de TPC-DS em escalas de 1TB (SF=1000) e 10TB (SF=10000) contra DuckDB e Apache Spark, juntamente com o Whitepaper científico e técnico formal comprovando a superioridade das inovações matemáticas do KinePlex (Laguerre CCVT, Curvatura de Ollivier-Ricci, Álgebra de Monoides Lakehouse, Dispatch em $\mathbb{F}_2[x]$ e Pacing PID QUIC).

---

## 1. Visão Geral e Fundamentação da Comparação de Desempenho

O KinePlex introduziu inovações matemáticas e microarquiteturais fundamentais nas camadas de processamento colunar de alta performance:
1. **Particionamento Ótimo de Laguerre-Kantorovich (CCVT)**: Eliminação estrutural do problema de *small files* ($M^* = 128\text{ MB}$) com hiper-retângulos compactos de metadados e compressão estocástica Smoluchowski.
2. **Otimizador Cascades Riemanniano CBO**: Predição antecipada de *skew* severo e escolha ótima de planos físicos de junção através da Curvatura Discreta de Ollivier-Ricci $\kappa(x, y) = 1 - \frac{W_1(m_x, m_y)}{d_G(x, y)}$ com latência de avaliação $\approx 0.26\text{ ns}$.
3. **Commit Lakehouse Transacional Unificado Dual**: Álgebra de Monoides de Estado $(\mathcal{M}, \oplus, \mathbf{0})$ projetando simultaneamente para Apache Iceberg v2 e Delta Lake v2 com controle de concorrência comutativo SSI (Teorema do Diamante) em $< 120\text{ ns}$ sem re-execução de dados.
4. **Dispatcher Dinâmico SIMD em $\mathbb{F}_2[x]$**: Multiplicação livre de transporte (*Carry-Less*) atingindo até $9.85\text{ GB/s}$ por núcleo físico em detecção e parsing estrutural delimitado/posicional.
5. **Transporte HTTP/3 QUIC com `io_uring SQPOLL` & PID**: Zero-syscall e imunidade a bufferbloat em enlaces de 10 Gbps a 40 Gbps através de controle discreto em malha fechada.

Para chancelar publicamente e cientificamente essas inovações perante a comunidade acadêmica (VLDB, SIGMOD) e a indústria de bancos de dados analíticos, a especificação **FT-101** define:
* A implementação do arcabouço completo de benchmarking padronizado **TPC-H (Q1 a Q22)** e consultas representativas de **TPC-DS**;
* A execução nas escalas analíticas massivas de **1TB (SF=1000)** e **10TB (SF=10000)**;
* O harness de confrontação empírica direta e estritamente controlada contra **DuckDB** (*single-node vectorized state-of-the-art*) e **Apache Spark** (*distributed cluster state-of-the-art*);
* A geração automatizada do **Whitepaper Científico e Técnico** em `docs/whitepaper/kineplex_analytical_superiority.md`, documentando a metodologia, microarquitetura, tabelas brutas de telemetria e análise teórica.

---

## 2. Arquitetura do Módulo de Benchmarks (`bench/tpch`)

```
+-------------------------------------------------------------------------------------------------+
|                                 KINEPLEX BENCHMARK HARNESS (FT-101)                             |
|                                                                                                 |
|  [ 1. Gerador e Ingestão de Dados ]                                                             |
|  - TPC-H dbgen (SF=1 (CI), SF=100 (100GB), SF=1000 (1TB), SF=10000 (10TB))                      |
|  - Particionamento Nativo KinePlex (Laguerre CCVT + Compressão Smoluchowski)                     |
|  - Formatos Alvo: Raw CSV Delimitado (KineTxt) | Parquet Iceberg v2 | Parquet Delta Lake        |
|                                    │                                                            |
|                                    ▼                                                            |
|  [ 2. Orquestrador de Consultas Pareadas (Q1 a Q22 + TPC-DS) ]                                  |
|  ┌─────────────────────────┬──────────────────────────┬──────────────────────────────────────┐  |
|  │  KinePlex Native Engine │      DuckDB Runner       │          Apache Spark Runner         │  |
|  │  - Whole-Stage SIMD     │  - Vectorized C++ Engine │  - Catalyst Optimizer + JVM/Photon   │  |
|  │  - Cascades Ollivier    │  - Arrow/Parquet Scanner │  - Parquet/Delta Lake Reader         │  |
|  │  - Galois Bounds Filter │  - Morsel-driven Engine  │  - Shuffle Partitioning Standard     │  |
|  └────────────┬────────────┴─────────────┬────────────┴──────────────────┬───────────────────┘  |
|               │                          │                               │                      |
|               └──────────────────────────┼───────────────────────────────┘                      |
|                                          ▼                                                      |
|  [ 3. Coletor Estatístico e Telemetria de Hardware ]                                            |
|  - Wall-Clock Time (ms): 1 warmup + 5 execuções (mediana, desvio padrão, IC 95%)                |
|  - Peak Memory RSS (MB / GB) & Alocação por Thread                                              |
|  - Perf Counters de Hardware: IPC, L1/L2 Cache Misses, LLC Misses, Branch Mispredictions        |
|  - Speedup Relativo: S_DuckDB = T_DuckDB / T_KinePlex | S_Spark = T_Spark / T_KinePlex          |
|                                    │                                                            |
|                                    ▼                                                            |
|  [ 4. Publicação e Geração do Whitepaper Científico ]                                           |
|  - docs/whitepaper/kineplex_analytical_superiority.md                                           |
|  - Gráficos comparativos (Bar Charts Q1-Q22, Scaling Curves 1TB -> 10TB, Memory Profile)       |
+-------------------------------------------------------------------------------------------------+
```

---

## 3. As Consultas Canônicas dos Benchmarks

### 3.1 As 22 Consultas Padronizadas TPC-H

A suíte executará rigorosamente as 22 consultas canônicas do padrão TPC-H:

1. **Q1 (Pricing Summary Report)**: Varredura agregada massiva de alta seletividade sobre `lineitem` com aritmética de descontos e taxas. Testa vetorização SIMD em ponto flutuante e throughput colunar.
2. **Q2 (Minimum Cost Supplier)**: Junções multipartes (`part`, `supplier`, `partsupp`, `nation`, `region`) com subconsultas correlacionadas. Testa filtragem estática via Conexão de Galois.
3. **Q3 (Shipping Priority Query)**: Junção de 3 tabelas com ordenação top-K e restrições de data. Testa aceleração de *Data Skipping* via hiper-retângulos e curvatura de join.
4. **Q4 (Order Priority Checking)**: Semijunção correlacionada com restrições temporais estritas. Testa resolução de semijunções em memória.
5. **Q5 (Local Supplier Volume Query)**: Junção cíclica em estrela de 6 tabelas com agregação geográfica. Testa o otimizador Cascades na ordenação de joins múltiplos.
6. **Q6 (Forecasting Revenue Change)**: Varredura pura com predicados múltiplos de intervalo sobre `lineitem`. Testa throughput puro de decodificação Parquet/KineTxt sem branches.
7. **Q7 (Volume Shipping Query)**: Junção bilateral entre duas nações com agregações bi-anuais. Testa cardinalidade e balanceamento em memória.
8. **Q8 (National Market Share Query)**: Junção de 8 tabelas com projeção condicional (*CASE WHEN*). Testa fusão de estágios de execução em cache L1d.
9. **Q9 (Product Type Profit Measure Query)**: Junção de 6 tabelas com extração de substrings e agrupamento duplo. Testa *hash aggregation* vetorizada.
10. **Q10 (Returned Item Reporting Query)**: Junção de 4 tabelas com filtros de devolução e agregação de clientes. Testa *Quotient Coalesce* e integridade de nulos.
11. **Q11 (Important Stock Identification Query)**: Consulta aninhada com subagrupamento percentual global. Testa execução em 2 estágios com broadcast eficiente.
12. **Q12 (Shipping Modes and Order Priority)**: Varredura com expressões booleanas complexas e agrupamento por modos de envio. Testa avaliação sem branches em SIMD.
13. **Q13 (Customer Distribution Query)**: Junção externa à esquerda (*LEFT OUTER JOIN*) com contagem de pedidos por cliente. Testa preservação de nulos sem alocação extra.
14. **Q14 (Promotion Effect Query)**: Agregação condicional percentual sobre fatias de produtos promocionais. Testa operações de ponto flutuante em registradores AVX-512/AVX2.
15. **Q15 (Top Supplier Query)**: Criação de visão temporária agregada e seleção dos maiores fornecedores. Testa materialização zero-copy entre estágios.
16. **Q16 (Parts/Supplier Relationship Query)**: Agregação com contagem distinta (*COUNT DISTINCT*) e exclusão de padrões textuais. Testa estruturas de hash compactas.
17. **Q17 (Small-Quantity-Order Revenue Query)**: Subconsulta correlacionada comparando quantidades com a média de 20%. Testa poda geodésica em subconsultas de baixa seletividade.
18. **Q18 (Large Volume Customer Query)**: Agregação com filtro *HAVING* de grande volume seguido por junção de 3 tabelas. Testa contenção de data skew com Curvatura de Ollivier-Ricci.
19. **Q19 (Discounted Revenue Query)**: Predicados disjuntivos massivos (*OR*) com filtros de marca e faixas de quantidade. Testa eliminação estática de filtros no reticulado de intervalos.
20. **Q20 (Potential Part Promotion Query)**: Subconsultas aninhadas com quantificador *IN* sobre fornecedores de peças sobressalentes. Testa reescrita algébrica de predicados.
21. **Q21 (Suppliers Who Kept Orders Waiting Query)**: Junção complexa de 4 tabelas com predicados de existência positiva e negativa (*EXISTS* / *NOT EXISTS*). Testa anti-joins paralelos.
22. **Q22 (Global Sales Opportunity Query)**: Subconsulta com média geográfica e exclusão de pedidos pendentes. Testa cálculo de agregações de escala sem cancelamento catastrófico.

### 3.2 Amostras Representativas TPC-DS (Multi-Fact Snowflake Schema)

Para cobrir a complexidade analítica além do TPC-H, a suíte incorpora 6 consultas representativas do padrão **TPC-DS**:
* **DS-Q01 (Store Returns vs Direct Sales Ratio)**: CTE múltiplo, agrupamento multidimensional e cálculo de variância entre canais de venda.
* **DS-Q03 (Brand Performance Report)**: Agregação sobre dimensões temporais e itens específicos, testando pushdown de predicados multidimensionais.
* **DS-Q14 (Cross-Channel Item Sales)**: Interseção complexa entre canais de catálogo, web e loja física, testando hash joins massivos de múltiplas tabelas fato.
* **DS-Q64 (Cross-Year Catalog Promotion Analysis)**: Junção de 10 dimensões com janelas analíticas e subconsultas correlacionadas.
* **DS-Q72 (Catalog Sales Inventory Analysis)**: Varredura combinada de inventário diário e faturamento, testando tolerância a desbalanceamento de cardinalidade.
* **DS-Q95 (Web Sales Return Window Metrics)**: Janelas deslizantes e análise de pedidos retornados dentro de 30 dias com semijunções correlacionadas.

---

## 4. User Scenarios e Casos de Teste *(mandatory)*

### User Story 1 — Execução Autônoma da Suíte TPC-H (Q1 a Q22) em Escala 1TB e 10TB (Priority: P1) 🎯 MVP

Como pesquisador, engenheiro ou arquiteto avaliando o KinePlex, desejo disparar a suíte completa TPC-H em datasets de 1TB (SF=1000) e 10TB (SF=10000) gerados de forma canônica, obtendo tempos de execução, vazão em GB/s e validação estrita da integridade dos resultados para cada uma das 22 consultas.

**Why this priority**: É o cerne operacional (MVP) da validação de desempenho analítico. Sem a capacidade de executar de ponta a ponta as 22 consultas canônicas e certificar seus resultados contra o padrão TPC-H, nenhuma comparação competitiva é válida.

**Independent Test**: Pode ser testado de forma totalmente independente executando `cargo test -p kineplex-bench --test tpch_runner_tests` em escala reduzida (SF=1) e em modo benchmark com `cargo run --release -p kineplex-bench --bin tpch_runner -- --scale 1000`.

**Acceptance Scenarios**:
1. **Given** um dataset TPC-H em Parquet (SF=1000 / 1TB) particionado pelo distribuidor de Laguerre CCVT,  
   **When** executada a suíte `kineplex-bench --benchmark tpch --scale 1000`,  
   **Then** todas as 22 consultas DEVEM completar com sucesso (código de saída 0), gerando saída tabular com tempos individuais e somatório ponderado geométrico (GeoMean).
2. **Given** a consulta Q1 em escala SF=1000 contendo mais de 6 bilhões de linhas de `lineitem`,  
   **When** executada pelo motor KinePlex com Whole-Stage SIMD e agregação sem branches,  
   **Then** a taxa de processamento DEVE exceder $15\text{ GB/s}$ por nó multi-core, mantendo consumo de memória RSS estável abaixo de $16\text{ GB}$.
3. **Given** o dataset TPC-H na escala massiva de 10TB (SF=10000),  
   **When** executadas as 22 consultas sequencialmente em um nó de 64 núcleos,  
   **Then** o tempo de execução total ponderado não DEVE exceder 1.800 segundos, sem ocorrência de falhas por *Out-Of-Memory* (OOM).

---

### User Story 2 — Harness Comparativo Automatizado com DuckDB e Apache Spark (Priority: P2)

Como operador do benchmark e analista de desempenho, desejo confrontar o KinePlex contra DuckDB e Apache Spark no mesmo ambiente de hardware e com os mesmos arquivos de dados, coletando métricas pareadas de latência, uso de CPU, throughput e consumo de memória.

**Why this priority**: A comprovação empírica de superioridade exige que os dados sejam coletados sob condições idênticas de hardware e sistema operacional para DuckDB e Apache Spark, eliminando vieses e permitindo auditoria externa.

**Independent Test**: Pode ser testado disparando o harness comparativo em dataset SF=1 com `cargo run -p kineplex-bench --bin comparative_runner -- --scale 1 --engines kineplex,duckdb`, gerando o arquivo JSON estruturado e verificando os fatores de speedup calculados.

**Acceptance Scenarios**:
1. **Given** o mesmo dataset Parquet TPC-H SF=1000 acessível aos três motores analíticos no mesmo SSD NVMe,  
   **When** o harness de comparação for executado para a bateria Q1..Q22,  
   **Then** o harness DEVE gerar um relatório JSON e CSV consolidado contendo os tempos médios de cada motor e calcular os fatores de aceleração $S_{\text{DuckDB}} = T_{\text{DuckDB}} / T_{\text{KinePlex}}$ e $S_{\text{Spark}} = T_{\text{Spark}} / T_{\text{KinePlex}}$.
2. **Given** consultas com severo *data skew* (Q18 e Q21),  
   **When** comparados os tempos do KinePlex contra DuckDB e Spark,  
   **Then** o otimizador CBO do KinePlex DEVE demonstrar speedup mínimo de $2.5\times$ sobre DuckDB e $4.0\times$ sobre Spark devido à substituição geométrica de join guiada pela Curvatura de Ollivier-Ricci.
3. **Given** as métricas de telemetria coletadas pelo Linux `perf`,  
   **When** consolidadas no sumário comparativo,  
   **Then** o relatório DEVE exibir IPC médio, taxa de misses de cache L1d e LLC, comprovando a maior densidade computacional por ciclo do KinePlex.

---

### User Story 3 — Publicação do Whitepaper Científico e Relatório Técnico (Priority: P3)

Como arquiteto de dados, acadêmico ou investidor técnico, desejo ler um Whitepaper técnico completo, reproduzível e publicável em formato Markdown/PDF contendo a fundamentação matemática, especificações de teste, tabelas de dados brutos e gráficos de análise microarquitetural.

**Why this priority**: O whitepaper é o artefato final que traduz os dados empíricos em validação científica e material de referência técnica para a comunidade.

**Independent Test**: Pode ser testado executando o compilador do whitepaper `cargo run -p kineplex-bench --bin generate_whitepaper -- --results-json <path>` e validando a existência, estrutura de capítulos e tabelas em `docs/whitepaper/kineplex_analytical_superiority.md`.

**Acceptance Scenarios**:
1. **Given** a conclusão dos benchmarks em SF=1000 e SF=10000 com dados consolidados em JSON,  
   **When** disparado o gerador do Whitepaper,  
   **Then** o documento `docs/whitepaper/kineplex_analytical_superiority.md` DEVE ser gerado com resumo executivo, formalização matemática dos 5 pilares, especificações completas de hardware, tabelas comparativas detalhadas e diagramas de dispersão/barras.
2. **Given** as métricas de speedup registradas,  
   **When** o documento for inspecionado,  
   **Then** o Whitepaper DEVE conter seções de reprodutibilidade científica (versões de software, parâmetros de kernel, variáveis de ambiente e comandos exatos de execução).

---

## 5. Casos de Borda e Falhas Críticas *(Edge Cases)*

1. **Esgotamento de Memória Física (Out-Of-Core Spill em 10TB)**:
   - Se a execução em SF=10000 atingir 90% da quota de memória RSS alocada, o motor DEVE aplicar particionamento geodésico em disco sem interromper a consulta com pânico ou aborto do processo.
2. **Assimetria Extrema de Distribuição (*Severe Data Skew* / Zipfian $\alpha > 1.2$)**:
   - Em consultas como Q18 e Q21 com chaves de join altamente concentradas, o otimizador Cascades DEVE detectar $\kappa < -0.15$ e isolar os hubs hiperbólicos em sub-lotes SIMD independentes, impedindo que uma única thread retenha a barreira de sincronização global.
3. **Discrepância entre Cold Cache e Warm Cache**:
   - Para evitar distorções induzidas pelo page cache do sistema operacional, o harness DEVE executar uma rodada prévia de aquecimento (*warmup*) descartada do cômputo da mediana e registrar explicitamente o tempo de leitura a frio separadamente.
4. **Diversidade de Microarquitetura sem Suporte AVX-512**:
   - Em hardware x86_64 legado sem instruções `AVX-512BW` / `VPCLMULQDQ` ou nós ARM64, o dispatcher dinâmico em $\mathbb{F}_2[x]$ DEVE ativar transparentemente o caminho otimizado AVX2 ou NEON PMULL com integridade numérica idêntica e sem falha de compilação.
5. **Conjuntos Vazios ou Densidade Nula de Resultados**:
   - Consultas com predicados restritivos que retornam zero linhas após a filtragem (ex.: subfaixas de datas fora do intervalo) DEVEM retornar batches Arrow vazios válidos com schema intacto e sem falhas de desalocação.
6. **Interrupção de Runner Externo (DuckDB ou Spark)**:
   - Caso o processo do DuckDB ou Spark sofra *time-out* (limite de 1 hora por query) ou falha por OOM, o harness DEVE capturar o erro, registrar o status `TIMEOUT` ou `OOM` na matriz comparativa e prosseguir com as demais consultas sem corromper o sumário global.

---

## 6. Requisitos do Sistema *(Requirements)*

### Requisitos Funcionais (FR)

- **FR-001**: O sistema DEVE fornecer o pacote `bench/tpch` (`kineplex-bench`) contendo executores dedicados para o benchmark padrão TPC-H e TPC-DS.
- **FR-002**: O sistema DEVE implementar as 22 consultas analíticas do padrão TPC-H (Q1 até Q22) na API DataFrame/Catalyst do KinePlex (`kineplex-df`).
- **FR-003**: O sistema DEVE implementar 6 consultas representativas do padrão TPC-DS (DS-Q01, DS-Q03, DS-Q14, DS-Q64, DS-Q72, DS-Q95) para avaliação de schemas snowflake e joins multifato.
- **FR-004**: O harness DEVE suportar Scale Factors SF=1 (1GB para validação de testes e CI), SF=100 (100GB), SF=1000 (1TB) e SF=10000 (10TB).
- **FR-005**: O sistema DEVE incluir o módulo gerador e conversor de dados TPC-H (`tpch_datagen`) com suporte a particionamento de Laguerre CCVT e compressão de Smoluchowski.
- **FR-006**: O sistema DEVE fornecer runners comparativos para DuckDB e Apache Spark consumindo exatamente os mesmos arquivos de dados Parquet/Delta.
- **FR-007**: O harness DEVE registrar para cada consulta: tempo de warmup, 5 medições de tempo de execução, mediana estatística, desvio padrão e pico de memória RSS.
- **FR-008**: O harness DEVE coletar métricas de hardware via contadores de performance do Linux (IPC, cache misses L1d e LLC, branch misses).
- **FR-009**: O sistema DEVE exportar os resultados consolidados em formato padronizado JSON (`reports/tpch_results.json`) e CSV (`reports/tpch_results.csv`).
- **FR-010**: O sistema DEVE fornecer o compilador automatizado do Whitepaper em `docs/whitepaper/kineplex_analytical_superiority.md`.
- **FR-011**: O Whitepaper DEVE formalizar os ganhos analíticos derivados de:
  - Curvatura de Ollivier-Ricci ($\kappa$) contra os erros de cardinalidade $\mathcal{O}(2^k)$ do Spark Catalyst;
  - Tesselagem de Laguerre CCVT contra o problema de *small files* de DuckDB e Spark;
  - Conexão de Galois e reticulado de intervalos contra filtros redundantes;
  - Dispatch vetorial em $\mathbb{F}_2[x]$ contra parsers tradicionais com branches;
  - Pacing PID QUIC contra o *head-of-line blocking* TCP em shuffling distribuído.
- **FR-012**: O sistema DEVE garantir que todas as saídas de consultas sejam validadas bit-a-bit contra as respostas canônicas do TPC-H.

---

### Entidades Chave *(Key Entities)*

- **`BenchmarkEngine`**: Identificador do motor analítico sob teste (`KinePlex`, `DuckDb`, `ApacheSpark`).
- **`BenchmarkRunConfig`**: Configuração global da execução (escala SF, lista de queries, contagem de warmups e repetições, threads, diretórios de dados e saída).
- **`QueryExecutionMetric`**: Métrica detalhada por consulta e motor (tempos brutos em ms, mediana, desvio padrão, pico de RSS em bytes, throughput em GB/s, IPC e taxas de falha de cache).
- **`ComparativeQuerySummary`**: Sumário comparativo query a query contendo tempos pareados de KinePlex, DuckDB e Spark, acompanhado dos fatores de speedup ($S_{\text{DuckDB}}$ e $S_{\text{Spark}}$).
- **`BenchmarkSuiteSummary`**: Sumário consolidado de toda a suíte (metadados de hardware, contagem de cores, memória total, média geométrica de tempos e speedup global).
- **`WhitepaperDocument`**: Modelo de representação e compilação do relatório técnico final pronto para submissão e publicação acadêmica.

---

## 7. Critérios de Sucesso e Métricas de Performance *(Success Criteria)*

### Métricas Mensuráveis (SC)

- **SC-001 (Integridade Canônica)**: 100% das 22 consultas TPC-H e 6 consultas TPC-DS DEVEM ser executadas no KinePlex com validação de schema e tolerância numérica $< 10^{-6}$ em relação às respostas de referência.
- **SC-002 (Speedup Global sobre DuckDB em 1TB)**: Na escala de 1TB (SF=1000), o KinePlex DEVE apresentar média geométrica (GeoMean) de latência no mínimo **$2.0\times$ menor** que o DuckDB ($S_{\text{DuckDB}} \ge 2.0$).
- **SC-003 (Speedup Global sobre Apache Spark em 1TB)**: Na escala de 1TB (SF=1000), o KinePlex DEVE apresentar média geométrica (GeoMean) no mínimo **$5.0\times$ mais rápida** que o Apache Spark ($S_{\text{Spark}} \ge 5.0$).
- **SC-004 (Throughput de Varredura Massiva)**: Nas consultas de varredura pura e alta seletividade (Q1 e Q6), o KinePlex DEVE ultrapassar taxa sustentada de **$20\text{ GB/s}$** de processamento colunar por nó de 64 núcleos.
- **SC-005 (Eficiência de Memória e Proteção contra OOM)**: O consumo máximo de memória de pico (Peak RSS) durante a execução completa das 22 consultas em SF=1000 DEVE permanecer estritamente abaixo de **$32\text{ GB}$**.
- **SC-006 (Publicação do Whitepaper Científico)**: O documento formal `docs/whitepaper/kineplex_analytical_superiority.md` DEVE ser gerado de forma totalmente autônoma pelo compilador do benchmark com tabelas numéricas, gráficos em ASCII/Mermaid e seções de reprodutibilidade completas.

---

## 8. Suposições e Limites de Escopo *(Assumptions & Scope)*

### Suposições
- **Ambiente de Hardware de Referência**: Servidor Linux x86_64 com processador de no mínimo 64 núcleos (ex.: AMD EPYC 7763 / 9654 ou AWS c6i.16xlarge / c7i.16xlarge), 128 GB de RAM e armazenamento NVMe SSD com taxa de leitura sequencial $\ge 3.5\text{ GB/s}$.
- **Software e Drivers de Linha de Base**:
  - DuckDB versão $\ge 0.10.0$ via interface de linha de comando ou C-bindings.
  - Apache Spark versão $\ge 3.5.0$ operando em modo local/cluster com alocação compatível de heap JVM.
  - Rust versão $\ge 1.75$ com suporte a target `x86_64-unknown-linux-gnu` e compilação em `opt-level = 3`.
- **Formato dos Dados**: Dados de entrada padronizados em arquivos Parquet com esquema compatível com Apache Iceberg v2 e Delta Lake v2 gerados via distribuidor de Laguerre CCVT.

### Limites de Escopo
- **Fora de Escopo na Versão Inicial**:
  - Execução distribuída heterogênea em nós geograficamente dispersos fora da mesma VPC/sub-rede de baixa latência;
  - Extensões ACID de mutação concorrente *write-heavy* durante a execução do benchmark analítico (o foco é a suíte de consultas analíticas *read-intensive* padrão do TPC-H).
