# Implementation Plan: Verificação e Correções do Distribuidor de Laguerre-Kantorovich (MK-EMD)

**Branch**: `001-laguerre-kantorovich-fixes` | **Date**: 2026-11-10 | **Spec**: [specs/001-laguerre-kantorovich-fixes/spec.md](spec.md)  
**Feature ID**: FT-097 / SPEC-001  
**Target Crates**: `kineplex-core`, `kineplex-df`  

---

## Summary

Implementar e verificar rigorosamente no `kineplex-core` o subsistema de **Distribuição Ótima de Laguerre-Kantorovich (MK-EMD Anti-Small-Files)**, garantindo conformidade matemática e de tipos com a formulação canônica do Problema de Transporte Ótimo Semi-Discreto:
1. **Teorema 1**: Estimador de massa comprimida ZSTD via taxa de entropia de Markov de 1ª ordem $H(X_t | X_{t-1})$ com unigrama de 256 posições, tabela dobrada de bigramas de 1024 slots e sobrevivência RLE.
2. **Adimensionalização de Welford**: Normalização contínua no espaço de Banach $L^p$ gerando escala invariante $\sigma_X^p \ge 10^{-6}$.
3. **Teorema 2**: Passo de Quase-Newton Log-Sinkhorn no espaço logarítmico $\psi_k \leftarrow \psi_k + \varepsilon \ln(M^* / M_k)$ com projeção estrita no hiperplano de soma zero $\sum_{k=1}^K \psi_k = 0$.
4. **Lazy Promotion & Selagem S3**: Retenção em `staged_arena` enquanto $M_k < 64\text{ MB}$, abertura assíncrona de `AsyncArrowWriter` com `BufWriter` (16 MB, concorrência 8) e selagem automática ao atingir $M^* = 128\text{ MB}$.
5. **Teorema 3**: Coalescência Geodésica Terminal de Ward-Steiner com massa reduzida $\mu_{kj}$, penalidade de dilatação de Bounding Box do Iceberg $\lambda_{\text{hull}} \Delta_{\text{bbox}}$ e consolidação única para jobs diminutos ($< 64\text{ MB}$).

---

## Technical Context

**Language/Version**: Rust 1.75+ (2021 Edition)  
**Primary Dependencies**: `arrow` 46.0, `arrow-array` 46.0, `parquet` 46.0 (com feature `async`), `object_store` 0.7/0.8, `tokio` 1.35, `uuid` 1.6  
**Storage**: `Arc<dyn ObjectStore>` com suporte transparente a `object_store::memory::InMemory` (para testes locais) e provedores de nuvem (AWS S3, GCS, Azure)  
**Testing**: `cargo test --test laguerre_distributor_tests`, suíte de propriedades e invariantes matemáticos  
**Target Platform**: Linux x86_64, bare-metal e conteinerizado  
**Project Type**: Submódulo analítico de alta performance colunar em biblioteca Rust (`kineplex-core`)  
**Performance Goals**: Inferência de roteamento $\le 5\,\mu\text{s}$ por lote Arrow de até 65.536 linhas; zero alocações adicionais na heap no cálculo de Markov  
**Constraints**: Alinhamento estrito com os teoremas de transporte ótimo; zero arquivos residuais pequenos ($< 64\text{ MB}$) emitidos no catálogo  
**Scale/Scope**: Lakehouse com streams contínuos de até milhões de eventos/s particionados em lotes equi-massivos de 128 MB  

---

## Constitution Check

*GATE: Avaliação contra as regras de governança de [AGENTES.MD](file:///home/james/Documentos/KinePlex/AGENTES.MD).*

1. **Aderência ao Spec-Driven Development**: Aprovado. A feature seguiu formalmente o ciclo `/speckit.specify` e `/speckit.plan`.
2. **Rigor Matemático**: Aprovado. Todos os três teoremas (Markov-ZSTD, Log-Sinkhorn com soma zero e Ward-Steiner com Bounding Box penalty) estão formalizados sem simplificações heurísticas.
3. **Zero-Copy e Alocação**: Aprovado. O cálculo do unigrama e bigrama de Markov utiliza arrays alocados na pilha (`[0u32; 256]` e `[0u32; 1024]`).
4. **Isolamento de Nuvem**: Aprovado. O uso de `Arc<dyn ObjectStore>` permite execução 100% isolada e determinística em memória via `InMemory`.
5. **Gates de CI e Não-Regressão**: Aprovado. Nenhuma modificação quebra a compilação do workspace ou reduz a cobertura abaixo de 95%.

---

## Project Structure

### Documentation (this feature)

```text
specs/001-laguerre-kantorovich-fixes/
├── spec.md              # Especificação formal e requisitos funcionais
├── plan.md              # Este plano de implementação
├── research.md          # Pesquisa técnica da Fase 0
├── data-model.md        # Modelo de dados, structs e invariantes da Fase 1
├── quickstart.md        # Guia rápido de uso e testes da Fase 1
├── contracts/           # Contratos de interfaces e assinaturas públicas
│   └── distributor_api.md
└── tasks.md             # Tarefas atômicas TDD (gerado via /speckit.tasks)
```

### Source Code (repository root)

```text
kineplex/kineplex-core/
├── Cargo.toml                              # Inclusão de object_store e feature async em parquet
├── src/
│   └── laguerre/
│       ├── mod.rs                          # Exportações do módulo laguerre
│       ├── config.rs                       # KantorovichDistributionConfig
│       ├── cell.rs                         # SemiDiscreteLaguerreCell
│       ├── entropy.rs                      # estimate_markov_zstd_mass (Teorema 1)
│       └── distributor.rs                  # LaguerreOptimalDistributor (Teoremas 2 e 3)
└── tests/
    └── laguerre_distributor_tests.rs       # Testes de integração, invariantes de soma zero e S3
```

---

## Phases & Execution Workflow

### Phase 0: Research & Mathematical Grounding (Completa)
- [x] Resolução da integração com `object_store` e `AsyncArrowWriter` em `research.md`.
- [x] Verificação da complexidade $\mathcal{O}(1)$ de memória da taxa de entropia de Markov em `research.md`.
- [x] Formalização do passo Log-Sinkhorn com projeção ortogonal em `research.md`.
- [x] Formalização da métrica de fusão de Ward-Steiner com regularização de Bounding Box Iceberg em `research.md`.

### Phase 1: Data Model & Contract Design (Completa)
- [x] Definição formal das structs e campos em `data-model.md`.
- [x] Definição de contratos de pré e pós-condições em `contracts/distributor_api.md`.
- [x] Exemplo executável de ponta a ponta em `quickstart.md`.

### Phase 2: Task Generation
- A ser disparado via `/speckit.tasks` para decomposição em tarefas atômicas TDD ordenadas.
