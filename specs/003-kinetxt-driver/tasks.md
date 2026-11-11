# Tasks: KineTxt — Driver Nativo SIMD & Branchless para Arquivos Texto (FT-099)

**Input**: Design documents from `/specs/003-kinetxt-driver/` (`spec.md`, `plan.md`, `research.md`, `data-model.md`, `contracts/kinetxt_api.md`, `quickstart.md`)  
**Prerequisites**: `spec.md`, `plan.md`, `research.md`, `data-model.md`, `contracts/`  
**Constitution**: Enforce TDD cycle, >= 95% coverage gate, zero-allocation in hot path, and cache line alignment (64B).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Configuração de dependências de Arrow e estrutura de módulos em `kineplex-core`

- [x] T001 Adicionar dependências `arrow-buffer = "46.0"` e `arrow-schema = "46.0"` em `kineplex/kineplex-core/Cargo.toml`
- [x] T002 [P] Criar hierarquia de módulos em `kineplex/kineplex-core/src/kinetxt/mod.rs` e registrar `pub mod kinetxt;` em `kineplex/kineplex-core/src/lib.rs`
- [x] T003 [P] Definir enum `TxtColumnSpec` e struct base `KineTxtDriver` em `kineplex/kineplex-core/src/kinetxt/driver.rs`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Algoritmos SWAR de conversão numérica e particionamento de fronteira

**⚠️ CRITICAL**: Pré-requisito bloqueante para todas as histórias de usuário

- [x] T004 [P] Implementar algoritmo branchless SWAR de 4 ciclos `parse_8_digits_swar` em `kineplex/kineplex-core/src/kinetxt/swar.rs`
- [x] T005 [P] Implementar parser numérico rápido `fast_parse_f64_ascii` com suporte a escala implícita e sinal condicional em `kineplex/kineplex-core/src/kinetxt/swar.rs`
- [x] T006 [P] Criar testes unitários para o parser SWAR e decimais em `kineplex/kineplex-core/tests/kinetxt_swar_tests.rs`
- [x] T007 Implementar particionador zero-copy de fronteira `split_txt_buffer_into_aligned_chunks` em `kineplex/kineplex-core/src/kinetxt/slicing.rs`

---

## Phase 3: User Story 1 - Leitura Posicional de Alta Velocidade (CNAB / Mainframe) (Priority: P1) 🎯 MVP

**Goal**: Indexação direta $O(1)$ (`base + row * stride + offset`) sem busca de quebras de linha `\n`, materializando em Arrow `Float64Array`, `Int64Array` e `StringArray` zero-copy a $> 6\text{ GB/s}$.

**Independent Test**: Teste com buffer CNAB 240 sintético verificando contagem de linhas e tipos estritos em `kinetxt_fixed_width_tests.rs`.

### Tests for User Story 1
- [x] T008 [P] [US1] Criar teste de integração para o leitor posicional em `kineplex/kineplex-core/tests/kinetxt_fixed_width_tests.rs`

### Implementation for User Story 1
- [x] T009 [US1] Implementar materialização de colunas numéricas (`Float64` e `Int64`) paralelizadas com Rayon em `kineplex/kineplex-core/src/kinetxt/fixed_width.rs`
- [x] T010 [US1] Implementar materialização zero-copy de `StringArray` via `OffsetBuffer` e `MutableBuffer` sem alocar `String` em `kineplex/kineplex-core/src/kinetxt/fixed_width.rs`
- [x] T011 [US1] Integrar método `parse_fixed_width_to_arrow` na struct `KineTxtDriver` em `kineplex/kineplex-core/src/kinetxt/driver.rs`

---

## Phase 4: User Story 2 - Leitura Delimitada Branchless com CLMUL Quote-Masking (Priority: P2)

**Goal**: Escaneamento em blocos de 64 bytes gerando bitmasks de delimitadores e quebras de linha em `u64`, com cálculo de Prefix-XOR em $\mathbb{F}_2[x]$ para eliminação de delimitadores dentro de aspas duplas sem branches.

**Independent Test**: Parsing delimitado em `kinetxt_delimited_tests.rs` validando que campos com aspas e delimitadores embutidos mantêm os offsets corretos.

### Tests for User Story 2
- [x] T012 [P] [US2] Criar teste unitário e de integração para o leitor delimitado com aspas em `kineplex/kineplex-core/tests/kinetxt_delimited_tests.rs`

### Implementation for User Story 2
- [x] T013 [US2] Implementar extração de bitmasks de 64 bytes (`tzcnt` e `blsr`) para quebras de linha em `kineplex/kineplex-core/src/kinetxt/delimited.rs`
- [x] T014 [US2] Implementar cálculo analítico de `QuoteMask` via Prefix-XOR em $\text{GF}(2)$ e filtro `ValidDelims = RawDelims & !QuoteMask` em `kineplex/kineplex-core/src/kinetxt/delimited.rs`
- [x] T015 [US2] Implementar extração paralela Rayon de colunas delimitadas para `RecordBatch` Arrow em `kineplex/kineplex-core/src/kinetxt/delimited.rs`
- [x] T016 [US2] Integrar método `parse_delimited_chunk_to_arrow` no `KineTxtDriver` em `kineplex/kineplex-core/src/kinetxt/driver.rs`

---

## Phase 5: User Story 3 - Bisseção Assíncrona e Particionamento Zero-Copy (Priority: P3)

**Goal**: Fatiamento em $P$ chunks sem ler do byte zero usando avanço especulativo de até 256 bytes até `\n`.

**Independent Test**: Particionamento de buffer de 10 MB em múltiplos chunks verificando contiguidade e integridade de registros em `kinetxt_slicing_tests.rs`.

### Tests for User Story 3
- [x] T017 [P] [US3] Criar testes unitários para a rotina `split_txt_buffer_into_aligned_chunks` em `kineplex/kineplex-core/tests/kinetxt_slicing_tests.rs`

### Implementation for User Story 3
- [x] T018 [US3] Otimizar busca do próximo `\n` na fronteira com verificação de limites em `kineplex/kineplex-core/src/kinetxt/slicing.rs`

---

## Phase 6: User Story 4 - Pipeline End-to-End: io_uring -> KineTxt -> BanachFrame -> Parquet (Priority: P4)

**Goal**: Conectar `KineTxtDriver` ao `LaguerreOptimalDistributor` para distribuição semi-discreta em arquivos Parquet ZSTD de 128 MB.

**Independent Test**: Ingestão de buffer texto, conversão em `RecordBatch` e alimentação de `LaguerreOptimalDistributor` em `kinetxt_pipeline_tests.rs`.

### Tests for User Story 4
- [x] T019 [P] [US4] Criar teste end-to-end conectando `KineTxtDriver` ao `LaguerreOptimalDistributor` em `kineplex/kineplex-core/tests/kinetxt_pipeline_tests.rs`

### Implementation for User Story 4
- [x] T020 [US4] Implementar trait `TextBlockParser` para interoperabilidade no runtime em `kineplex/kineplex-core/src/kinetxt/driver.rs`

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Benchmarks de throughput e validação completa do workspace

- [x] T021 [P] Criar benchmark de throughput (`kinetxt_bench.rs`) medindo GB/s por núcleo nos modos posicional e delimitado em `kineplex/kineplex-core/benches/kinetxt_bench.rs`
- [x] T022 Executar validação global de compilação e suíte de testes (`cargo test --workspace`) assegurando cobertura $\ge 95\%$ e conformidade integral com a Constituição

---

## Dependencies & Execution Order

### Phase Dependencies
- **Phase 1 (Setup)**: Sem dependências externas.
- **Phase 2 (Foundational)**: Depende de Phase 1 (BLOQUEIA todas as histórias de usuário).
- **Phase 3 (User Story 1 - MVP)**: Depende de Phase 2.
- **Phase 4 (User Story 2)**: Depende de Phase 2 e Phase 3.
- **Phase 5 (User Story 3)**: Depende de Phase 2.
- **Phase 6 (User Story 4)**: Depende de Phase 3 e Phase 4.
- **Phase 7 (Polish)**: Depende de todas as fases anteriores.

### Parallel Opportunities
- Tarefas T002, T003 (Phase 1) podem rodar em paralelo.
- Tarefas T004, T005, T006 (Phase 2) podem rodar em paralelo.
- Testes T008, T012, T017, T019, T021 podem ser desenvolvidos em paralelo aos seus respectivos componentes.
