# Quickstart: Validação FT-100 (CBO, Lakehouse & SIMD)

## 1. Pré-Requisitos
- Toolchain Rust 1.75+
- Crate `kineplex-core` compilada com suporte a Arrow e Rayon

---

## 2. Cenários de Validação

### Cenário 1: Seleção de Plano Físico com Curvatura de Ollivier-Ricci
Valida a escolha do operador de junção no CBO sob curvatura positiva, nula e hiperbólica.

```bash
cargo test -p kineplex-core --test cascades_cbo_tests
```

**Resultado Esperado**:
- Curvatura $\kappa > 0.15$ seleciona `LaguerreCoLocatedMergeJoin`;
- Curvatura $\kappa < -0.15$ seleciona `HyperbolicSkewBroadcastJoin`;
- Custo avaliado em $< 50\text{ ns}$ por aresta.

---

### Cenário 2: Validação Comutativa SSI e Serialização Delta Lake
Valida a ausência de conflitos em commits concorrentes via interseção de hiper-retângulos e serialização JSON.

```bash
cargo test -p kineplex-core --test unified_lakehouse_tests
```

**Resultado Esperado**:
- Arquivos com intervalos disjuntos comutam sem abort;
- Arquivos com sobreposição disparam detecção correta de conflito;
- Linhas JSON do `_delta_log` formatadas canonicamente.

---

### Cenário 3: Equivalência e Performance do Dispatcher SIMD em $\mathbb{F}_2[x]$
Valida que o cálculo de máscaras estruturais no Tier detectado produz bits idênticos ao SWAR.

```bash
cargo test -p kineplex-core --test simd_gf2_dispatcher_tests
```

**Resultado Esperado**:
- Delimitadores internos de aspas duplas filtrados em 1 ciclo;
- Delimitadores externos preservados;
- Máscaras AVX2/AVX-512 idênticas ao fallback SWAR.
