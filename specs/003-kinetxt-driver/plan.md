# Implementation Plan: KineTxt — Driver Nativo SIMD & Branchless para Arquivos Texto (FT-099)

**Branch**: `003-kinetxt-driver` | **Date**: 2026-11-11 | **Spec**: [specs/003-kinetxt-driver/spec.md](spec.md)

**Input**: Feature specification from `specs/003-kinetxt-driver/spec.md`

## Summary

Implementação do sub-motor **KineTxt** em `kineplex-core::kinetxt`: driver nativo de leitura e parsing vetorial para arquivos texto posicionais e delimitados (.txt, .csv, .cnab, .fix) tratando buffers brutos como fluxo em $\mathbb{F}_2^8$, eliminando os três crimes de performance da JVM (instanciação de `String`, branch mispredictions e parsing lento com `strtod`), operando a $> 6\text{ GB/s}$ por núcleo via SWAR de 4 ciclos, máscaras de 64 bits em $\text{GF}(2)$ (CLMUL) e materialização zero-copy em buffers Apache Arrow (`RecordBatch`) compatíveis com `BanachFrame` e `LaguerreOptimalDistributor`.

---

## Technical Context

**Language/Version**: Rust 1.75+ (Edition 2021)  
**Primary Dependencies**:
- `arrow = { version = "46.0", features = ["ipc"] }`
- `arrow-array = "46.0"`
- `arrow-buffer = "46.0"`
- `arrow-schema = "46.0"`
- `rayon = "1.8"`
- `bytemuck = "1.14"`

**Storage**: Buffers em memória contígua da arena do `io_uring` e fatias de arquivos em memória RAM.  
**Testing**: `cargo test -p kineplex-core --test kinetxt_fixed_width_tests`, `cargo test -p kineplex-core --test kinetxt_delimited_tests`, `cargo test -p kineplex-core --test kinetxt_swar_tests`  
**Target Platform**: Linux x86_64 (AVX2/AVX-512) e ARM64 (NEON)  
**Project Type**: Workspace Crate (`kineplex-core`)  
**Performance Goals**:
- $\ge 6\text{ GB/s}$ por núcleo para arquivos posicionais de largura fixa;
- $\ge 2.5\text{ GB/s}$ por núcleo para arquivos delimitados com suporte a aspas;
- Conversão de 8 dígitos ASCII em `u64` via SWAR em $< 5\text{ ns}$ (4 ciclos de clock).  

**Constraints**:
- Zero alocações de `String` no heap para o corpo dos dados das colunas;
- Hot-path de parsing estritamente branchless e loopless para campos numéricos;
- Alinhamento de buffers Arrow para instruções SIMD.  

**Scale/Scope**:
- Blocos de 64 MB alinhados em páginas de 4096 bytes;
- Suporte a CNAB 240/400, B3, BACEN, Cetip, FIX e CSVs industriais.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Princípio da Constituição | Verificação no Projeto | Status |
|---|---|---|
| **1. Rigor Matemático** | Álgebra no corpo finito de Galois $\mathbb{F}_2[x]$ para eliminação de aspas via Prefix-XOR e redução SWAR formal em registradores de 64 bits. | **PASS** |
| **2. Zero-Copy & Alinhamento** | Empréstimo direto de fatias para `StringArray`, `OffsetBuffer<i32>` e buffers contíguos de memória sem instâncias de `String`. | **PASS** |
| **3. Isolamento de I/O** | O driver de parsing é puramente computacional em memória (`&[u8] -> RecordBatch`); não depende de chamadas diretas de rede ou disco, facilitando testes unitários instantâneos. | **PASS** |
| **4. Governança SDD & CI** | Fluxo rigoroso do Spec Kit com spec, plan, data model, contratos, quickstart e testes com cobertura $\ge 95\%$. | **PASS** |

---

## Project Structure

### Documentation (this feature)

```text
specs/003-kinetxt-driver/
├── spec.md              # Especificação formal completa
├── plan.md              # Este plano de implementação
├── research.md          # Decisões arquiteturais (Phase 0)
├── data-model.md        # Entidades de domínio e layouts (Phase 1)
├── quickstart.md        # Guia de validação executável (Phase 1)
├── contracts/           # Contratos de interface pública (Phase 1)
│   └── kinetxt_api.md
└── checklists/
    └── requirements.md  # Checklist de conformidade
```

### Source Code (`kineplex/kineplex-core`)

```text
kineplex/kineplex-core/
├── Cargo.toml                      # Adição de arrow-buffer e arrow-schema se necessário
├── src/
│   ├── lib.rs                      # Exportação de pub mod kinetxt;
│   └── kinetxt/                    # [NOVO MÓDULO]
│       ├── mod.rs                  # Interface raiz e re-exports
│       ├── driver.rs               # KineTxtDriver e TxtColumnSpec
│       ├── fixed_width.rs          # Leitor posicional O(1) com stride
│       ├── delimited.rs            # Leitor delimitado com bitmasks de 64 bits
│       ├── swar.rs                 # Parsing SWAR e fast_parse_f64_ascii
│       └── slicing.rs              # split_txt_buffer_into_aligned_chunks
└── tests/
    ├── kinetxt_fixed_width_tests.rs
    ├── kinetxt_delimited_tests.rs
    └── kinetxt_swar_tests.rs
```

---

## Complexity Tracking

Nenhuma violação aos princípios da Constituição foi detectada. A lógica de parsing vetorial permanece desacoplada de I/O físico, alimentando diretamente o ecossistema colunar Arrow do KinePlex.
