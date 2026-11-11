# Implementation Plan: Riemannian Cascades, Lakehouse Commit & SIMD Dispatcher (FT-100)

**Branch**: `004-cascades-lakehouse-simd` | **Date**: 2026-11-11 | **Spec**: [specs/004-cascades-lakehouse-simd/spec.md](spec.md)

**Input**: Feature specification from `specs/004-cascades-lakehouse-simd/spec.md`

## Summary

Implementação da tríade analítica fundamental no `kineplex-core`:
1. **Otimizador Lógico/Físico Cascades CBO (`kineplex_core::cbo`)**: Baseado em Geometria Diferencial Discreta com Curvatura de Ollivier-Ricci $\kappa(x, y) = 1 - \frac{W_1(m_x, m_y)}{d_G(x, y)}$ sobre a estrutura MEMO, prevendo gargalos de *data skew* severos e decidindo entre junção co-localizada de Laguerre, particionamento SIMD hash ou separação de hubs hiperbólicos.
2. **Monoide Unificado de Commit Lakehouse (`kineplex_core::lakehouse`)**: Álgebra de Monoides $(\mathcal{M}, \oplus, \mathbf{0})$ que projeta simultaneamente o mesmo estado em memória para manifestos Avro (Apache Iceberg v2) e logs de transação canônicos JSON (Delta Lake), resolvendo concorrência através do Teorema do Diamante (SSI) em $< 2\text{ ms}$ sem re-execução de dados.
3. **Dispatcher SIMD Dinâmico em $\mathbb{F}_2[x]$ (`kineplex_core::simd`)**: Multiplicação sem transporte com seleção automática em runtime do silício da CPU (AVX-512BW VPCLMULQDQ a $> 12\text{ GB/s}$, AVX2 PCLMULQDQ a $> 6\text{ GB/s}$, ARM64 Graviton NEON PMULL a $> 8\text{ GB/s}$ e Fallback Universal SWAR).

---

## Technical Context

**Language/Version**: Rust 1.75+ (Edition 2021)  
**Primary Dependencies**:
- `arrow = { version = "46.0", features = ["ipc"] }`
- `arrow-array = "46.0"`
- `arrow-buffer = "46.0"`
- `arrow-schema = "46.0"`
- `rayon = "1.8"`
- `serde = "1.0"`
- `serde_json = "1.0"`
- `bytemuck = "1.14"`

**Storage**: Buffers em memória contígua da arena do `io_uring`, fatias Parquet de 128 MB geradas pelo `LaguerreOptimalDistributor` e estruturas em memória de `_delta_log` e `ManifestTree`.  
**Testing**:
- `cargo test -p kineplex-core --test cascades_cbo_tests`
- `cargo test -p kineplex-core --test unified_lakehouse_tests`
- `cargo test -p kineplex-core --test simd_gf2_dispatcher_tests`
- `cargo test -p kineplex-core --test e2e_cascades_lakehouse_tests`

**Target Platform**: Linux x86_64 (AVX2, AVX-512BW, VPCLMULQDQ) e ARM64 (NEON, Crypto Extension).  
**Performance Goals**:
- $\le 50\text{ ns}$ por aresta na avaliação de Curvatura de Ollivier-Ricci no CBO;
- $\le 100\text{ ns}$ para validação geométrica de comutatividade SSI sobre até 1.000 arquivos;
- $\le 3\text{ ciclos de clock}$ para quote-masking de 64 bytes em $\mathbb{F}_2[x]$ via AVX-512;
- Taxa de transferência $\ge 12\text{ GB/s}$ por núcleo no parsing delimitado em hardware com AVX-512BW.

**Constraints**:
- Zero duplicação de I/O de dados Parquet entre Iceberg e Delta Lake;
- Hot-path de dispatching de hardware sem recompilação de código (deteção dinâmica em runtime);
- Rigor matemático e conformidade com a Constituição do KinePlex.

---

## Constitution Check

| Princípio da Constituição | Verificação no Projeto | Status |
|---|---|---|
| **1. Rigor Matemático** | Distância de Wasserstein $W_1$ em grafos Riemannianos discretos, Álgebra de Monoides Abelianos Livres sob união disjunta, e Anel de Polinômios $\mathbb{F}_2[x]$ com produto de Cauchy sem transporte. | **PASS** |
| **2. Zero-Copy & Alinhamento** | Projeções $\Psi_{\text{Iceberg}}$ e $\Psi_{\text{Delta}}$ geradas a partir do mesmo objeto `UnifiedDataFileAction` em memória; vetores de bits SIMD alinhados a 64 bytes (512 bits). | **PASS** |
| **3. Isolamento de I/O** | Avaliação do grafo MEMO e resolução de conflitos SSI operam 100% sobre estruturas indexadas em memória RAM (VP-Tree métrica), sem chamadas de rede bloqueantes. | **PASS** |
| **4. Governança SDD & CI** | Especificação, plano de arquitetura, contratos e testes automatizados com cobertura $\ge 95\%$. | **PASS** |

---

## Project Structure

### Documentation (`specs/004-cascades-lakehouse-simd/`)
```text
specs/004-cascades-lakehouse-simd/
├── spec.md              # Especificação formal completa (FT-100)
├── plan.md              # Este plano de implementação
├── research.md          # Decisões arquiteturais e análise de trade-offs
├── data-model.md        # Entidades de domínio, layouts e tipos Rust
├── quickstart.md        # Guia executável de validação e testes
├── contracts/           # Contratos de interface pública
│   └── cascades_lakehouse_simd_api.md
└── checklists/
    └── requirements.md  # Checklist de conformidade com a Constituição
```

### Source Code (`kineplex/kineplex-core`)
```text
kineplex/kineplex-core/
├── src/
│   ├── lib.rs
│   ├── cbo/                            # [NOVO MÓDULO: Eixo 1]
│   │   ├── mod.rs                      # Exportações do CBO Riemanniano
│   │   ├── curvature.rs                # Curvatura de Ollivier-Ricci e Wasserstein
│   │   └── memo.rs                     # Grafo AND-OR, tensor de custo e Bellman-Ford
│   ├── lakehouse/                      # [NOVO MÓDULO: Eixo 2]
│   │   ├── mod.rs                      # Exportações do Commit Lakehouse
│   │   ├── monoid.rs                   # UnifiedDataFileAction e Álgebra de Monoides
│   │   ├── iceberg.rs                  # Funtor \Psi_Iceberg (ManifestTree Avro)
│   │   ├── delta.rs                    # Funtor \Psi_Delta (_delta_log JSON)
│   │   └── ssi.rs                      # Resolução de conflitos geométrica (Diamond Property)
│   └── simd/                           # [NOVO MÓDULO: Eixo 3]
│       ├── mod.rs                      # Dispatcher dinâmico e re-exports
│       ├── gf2.rs                      # Álgebra de polinômios em GF(2)[x]
│       ├── x86_avx512.rs               # Kernel AVX-512BW + VPCLMULQDQ (Tier 1)
│       ├── x86_avx2.rs                 # Kernel AVX2 + PCLMULQDQ (Tier 2)
│       ├── arm_neon.rs                 # Kernel ARM64 NEON + PMULL (Tier 3)
│       └── swar.rs                     # Fallback Universal SWAR (Tier 4)
└── tests/
    ├── cascades_cbo_tests.rs
    ├── unified_lakehouse_tests.rs
    ├── simd_gf2_dispatcher_tests.rs
    └── e2e_cascades_lakehouse_tests.rs
```

---

## Complexity Tracking

Nenhuma violação aos princípios da Constituição detectada. Os três módulos são independentes em termos de teste unitário e convergem no pipeline de execução do runtime KinePlex.
