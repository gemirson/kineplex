# Tasks: Riemannian Cascades, Lakehouse Commit & SIMD Dispatcher (FT-100)

**Input**: Design documents from `/specs/004-cascades-lakehouse-simd/` (`spec.md`, `plan.md`, `research.md`, `data-model.md`, `contracts/cascades_lakehouse_simd_api.md`, `quickstart.md`)  
**Prerequisites**: `spec.md`, `plan.md`, `research.md`, `data-model.md`, `contracts/`  
**Constitution**: Enforce TDD cycle, >= 95% coverage gate, zero-allocation in hot path, and cache line alignment (64B).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Estrutura de módulos e re-exports em `kineplex-core` para os três eixos

- [x] T001 Criar hierarquia de módulos em `kineplex/kineplex-core/src/cbo/mod.rs`, `kineplex/kineplex-core/src/lakehouse/mod.rs`, `kineplex/kineplex-core/src/simd/mod.rs` e registrar em `kineplex/kineplex-core/src/lib.rs`
- [x] T002 [P] Declarar enums, structs e traits públicas em `cbo`, `lakehouse` e `simd`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Primitivas matemáticas fundamentais (SWAR GF(2), hiper-retângulos e curvatura)

**⚠️ CRITICAL**: Pré-requisito bloqueante para todas as histórias de usuário

- [x] T003 [P] Implementar algoritmo de Prefix-XOR em $\mathbb{F}_2[x]$ `prefix_xor_swar_64` em 6 shifts logarítmicos em `kineplex/kineplex-core/src/simd/swar.rs`
- [x] T004 [P] Implementar teste de interseção de hiper-retângulos em $\mathbb{R}^C$ `intersects_hyper_rectangle` em `kineplex/kineplex-core/src/lakehouse/monoid.rs`
- [x] T005 [P] Implementar cálculo analítico da Curvatura de Ollivier-Ricci $\kappa(x, y) = 1 - \frac{W_1(m_x, m_y)}{d_G(x, y)}$ com proteção de zero em `kineplex/kineplex-core/src/cbo/curvature.rs`

---

## Phase 3: User Story 1 - Otimizador Cascades Riemanniano com Curvatura de Ollivier-Ricci (Priority: P1) 🎯 MVP

**Goal**: Predição de *data skew* e seleção de planos físicos ótimos em tempo $< 50\text{ ns}$ com base na topologia da curvatura de Wasserstein.

**Independent Test**: Teste unitário e de integração em `cascades_cbo_tests.rs` validando transições de curvatura positiva, nula e hiperbólica.

### Tests for User Story 1
- [x] T006 [P] [US1] Criar teste unitário e de integração para o otimizador CBO em `kineplex/kineplex-core/tests/cascades_cbo_tests.rs`

### Implementation for User Story 1
- [x] T007 [US1] Implementar regras de substituição física (`LaguerreCoLocatedMergeJoin`, `SimdPartitionedHashJoin`, `HyperbolicSkewBroadcastJoin`) em `kineplex/kineplex-core/src/cbo/curvature.rs`
- [x] T008 [US1] Implementar tensor de custo com penalidade geodésica $\exp(-\bar{\kappa})$ e poda Bellman-Ford em `kineplex/kineplex-core/src/cbo/memo.rs`
- [x] T009 [US1] Implementar struct `KinePlexCascadesOptimizer` e trait `CascadesCostOptimizer` em `kineplex/kineplex-core/src/cbo/mod.rs`

---

## Phase 4: User Story 2 - Commit Transacional Unificado Dual Lakehouse (Priority: P2)

**Goal**: Gravação simultânea nos metadados do Apache Iceberg v2 e log Delta Lake com controle de concorrência comutativo SSI (Teorema do Diamante) em $< 2\text{ ms}$.

**Independent Test**: Validação de comutatividade e serialização de log em `unified_lakehouse_tests.rs`.

### Tests for User Story 2
- [x] T010 [P] [US2] Criar teste unitário para comutatividade SSI e serialização de log em `kineplex/kineplex-core/tests/unified_lakehouse_tests.rs`

### Implementation for User Story 2
- [x] T011 [US2] Implementar serialização canônica JSON do protocolo Delta Lake v2 (`to_delta_log_json_line`) em `kineplex/kineplex-core/src/lakehouse/delta.rs`
- [x] T012 [US2] Implementar agregação de limites no reticulado completo ($L_c, U_c$) para manifestos Iceberg em `kineplex/kineplex-core/src/lakehouse/iceberg.rs`
- [x] T013 [US2] Implementar validação geométrica de comutatividade `can_commute_concurrent_commit` em `kineplex/kineplex-core/src/lakehouse/ssi.rs`
- [x] T014 [US2] Implementar trait `LakehouseCommitEngine` e struct `UnifiedDataFileAction` em `kineplex/kineplex-core/src/lakehouse/mod.rs`

---

## Phase 5: User Story 3 - Dispatcher SIMD Dinâmico em $\mathbb{F}_2[x]$ (Priority: P3)

**Goal**: Extração de máscaras estruturais de 64 bytes em tempo de execução selecionando automaticamente AVX-512, AVX2, ARM NEON ou SWAR a $> 12\text{ GB/s}$.

**Independent Test**: Testes de equivalência de saída de bits entre todos os Tiers em `simd_gf2_dispatcher_tests.rs`.

### Tests for User Story 3
- [x] T015 [P] [US3] Criar testes de equivalência de saída de bits entre Tiers em `kineplex/kineplex-core/tests/simd_gf2_dispatcher_tests.rs`

### Implementation for User Story 3
- [x] T016 [US3] Implementar kernel AVX2 + PCLMULQDQ (`extract_structural_mask_avx2`) em `kineplex/kineplex-core/src/simd/x86_avx2.rs`
- [x] T017 [US3] Implementar kernel AVX-512BW + VPCLMULQDQ (`extract_structural_mask_avx512`) em `kineplex/kineplex-core/src/simd/x86_avx512.rs`
- [x] T018 [US3] Implementar kernel ARM64 NEON + PMULL (`prefix_xor_pmull_arm64`) em `kineplex/kineplex-core/src/simd/arm_neon.rs`
- [x] T019 [US3] Implementar função de despacho dinâmico `compute_quote_mask_dispatch` com trait `SimdStructuralParser` em `kineplex/kineplex-core/src/simd/mod.rs`

---

## Phase 6: User Story 4 - Pipeline Integrado End-to-End (Priority: P4)

**Goal**: Conectar o novo dispatcher SIMD ao `KineTxtDriver`, o otimizador CBO ao grafo de execução e o commit Lakehouse ao `LaguerreOptimalDistributor`.

**Independent Test**: Teste E2E de consulta com join, otimização de plano e gravação dual em `e2e_cascades_lakehouse_tests.rs`.

### Tests for User Story 4
- [x] T020 [P] [US4] Criar teste E2E unindo parsing SIMD, CBO e commit Lakehouse em `kineplex/kineplex-core/tests/e2e_cascades_lakehouse_tests.rs`

### Implementation for User Story 4
- [x] T021 [US4] Conectar `compute_quote_mask_dispatch` como backend acelerado do `KineTxtDriver` em `kineplex/kineplex-core/src/kinetxt/delimited.rs`

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Micro-benchmarks de throughput/latência e validação completa do workspace

- [x] T022 [P] Criar benchmark de latência e vazão (`cascades_lakehouse_bench.rs`) medindo tempo de CBO ($< 50\text{ ns}$), validação SSI ($< 100\text{ ns}$) e vazão SIMD em `kineplex/kineplex-core/benches/cascades_lakehouse_bench.rs`
- [x] T023 Executar validação global de compilação e suíte de testes (`cargo clippy` e `cargo test --workspace`) assegurando cobertura $\ge 95\%$ e conformidade integral com a Constituição

---

## Dependencies & Execution Order

### Phase Dependencies
- **Phase 1 (Setup)**: Sem dependências externas.
- **Phase 2 (Foundational)**: Depende de Phase 1 (BLOQUEIA as histórias de usuário).
- **Phase 3 (User Story 1 - MVP)**: Depende de Phase 2.
- **Phase 4 (User Story 2)**: Depende de Phase 2.
- **Phase 5 (User Story 3)**: Depende de Phase 2.
- **Phase 6 (User Story 4 - E2E)**: Depende de Phase 3, 4 e 5.
- **Phase 7 (Polish)**: Depende de todas as fases anteriores.

### Parallel Opportunities
- Tarefas T003, T004 e T005 (Phase 2) podem rodar em paralelo.
- User Story 1 (Phase 3), User Story 2 (Phase 4) e User Story 3 (Phase 5) podem ser desenvolvidas em paralelo por agentes distintos.
- Testes T006, T010, T015, T020 e T022 podem ser criados em paralelo às implementações.
