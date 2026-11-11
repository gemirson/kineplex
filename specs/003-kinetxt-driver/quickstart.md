# Quickstart: Validação do Driver KineTxt (FT-099)

## 1. Pré-Requisitos
- Toolchain Rust 1.75+
- Crate `kineplex-core` compilada com suporte a Arrow e Rayon

---

## 2. Cenários de Validação

### Cenário 1: Parsing Posicional de Largura Fixa (CNAB / B3)
Valida a indexação $O(1)$ sem busca de `\n` em buffer com stride de 240 bytes.

```bash
cargo test -p kineplex-core --test kinetxt_fixed_width_tests
```

**Resultado Esperado**:
- Processamento de 100.000 linhas em $< 10\text{ ms}$ ($> 6\text{ GB/s}$).
- Valores de `Float64Array`, `Int64Array` e `StringArray` validados contra o layout.

---

### Cenário 2: Parsing Delimitado com Máscaras Bitwise e Aspas
Valida a eliminação de delimitadores internos de aspas duplas via $\text{GF}(2)$ Prefix-XOR.

```bash
cargo test -p kineplex-core --test kinetxt_delimited_tests
```

**Resultado Esperado**:
- Delimitadores internos preservados no texto da coluna.
- Zero branch mispredictions no loop de 64 bytes.

---

### Cenário 3: Validação do Algoritmo SWAR de 8 Dígitos
Valida a conversão de 8 dígitos ASCII em `u64` em $< 5\text{ ns}$.

```bash
cargo test -p kineplex-core --test kinetxt_swar_tests
```

**Resultado Esperado**:
- Todos os testes de conversão numérica com e sem ponto/vírgula passam em $< 0.01\text{s}$.
