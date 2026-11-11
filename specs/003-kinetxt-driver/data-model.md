# Data Model: KineTxt Driver (FT-099)

## 1. Entidades de Domínio

```
+---------------------------------------------------------------------------------+
|                                 TxtColumnSpec                                   |
|  - Float64 { name, col_index, fixed_range: Option<(start, end)>, scale: f64 }   |
|  - Int64   { name, col_index, fixed_range: Option<(start, end)> }               |
|  - Utf8    { name, col_index, fixed_range: Option<(start, end)> }               |
+---------------------------------------------------------------------------------+
                                      |
                                      v
+---------------------------------------------------------------------------------+
|                                 KineTxtDriver                                   |
|  - delimiter: u8 (ex: b';', b',', b'|')                                         |
|  - fixed_line_stride: Option<usize> (ex: Some(240) para CNAB 240)               |
|  - schema_specs: Vec<TxtColumnSpec>                                             |
+---------------------------------------------------------------------------------+
                                      |
                   +------------------+------------------+
                   |                                     |
                   v                                     v
+------------------------------------+ +------------------------------------+
|    FixedWidthPositionalScanner     | |        DelimitedSimdScanner        |
|  - Acesso direto row*stride+offset | |  - Máscaras de 64 bits (AVX-512)   |
|  - Zero varredura de '\n'          | |  - GF(2) CLMUL Quote-Masking       |
|  - Paralelismo Rayon por coluna    | |  - tzcnt / blsr extração de campos |
+------------------------------------+ +------------------------------------+
                   |                                     |
                   +------------------+------------------+
                                      |
                                      v
                       +-----------------------------+
                       |    RecordBatch (Apache Arrow)|
                       |  - Float64Array             |
                       |  - Int64Array               |
                       |  - StringArray (Zero-Copy)  |
                       +-----------------------------+
```

---

## 2. Estruturas e Assinaturas em Rust

### 2.1 Especificação de Coluna (`TxtColumnSpec`)
```rust
#[derive(Clone, Debug, PartialEq)]
pub enum TxtColumnSpec {
    /// Extrai como Float64 (com escala implícita opcional, ex: scale=0.01 para centavos CNAB)
    Float64 {
        name: String,
        col_index: usize,
        fixed_range: Option<(usize, usize)>,
        scale: f64,
    },
    /// Extrai como Int64 puro via SWAR
    Int64 {
        name: String,
        col_index: usize,
        fixed_range: Option<(usize, usize)>,
    },
    /// Extrai como String UTF-8 Zero-Copy direto para OffsetBuffer + ValuesBuffer do Arrow
    Utf8 {
        name: String,
        col_index: usize,
        fixed_range: Option<(usize, usize)>,
    },
}
```

### 2.2 Estrutura do Motor (`KineTxtDriver`)
```rust
pub struct KineTxtDriver {
    pub delimiter: u8,
    pub fixed_line_stride: Option<usize>,
    pub schema_specs: Vec<TxtColumnSpec>,
}

impl KineTxtDriver {
    pub fn new(delimiter: u8, schema_specs: Vec<TxtColumnSpec>) -> Self {
        Self {
            delimiter,
            fixed_line_stride: None,
            schema_specs,
        }
    }

    pub fn new_fixed_width(stride: usize, schema_specs: Vec<TxtColumnSpec>) -> Self {
        Self {
            delimiter: b'\n',
            fixed_line_stride: Some(stride),
            schema_specs,
        }
    }
}
```

### 2.3 Funções de Alta Performance no Hot-Path
- `pub fn parse_8_digits_swar(ascii_be: u64) -> u64`
- `pub fn fast_parse_f64_ascii(bytes: &[u8], default_scale: f64) -> f64`
- `pub fn split_txt_buffer_into_aligned_chunks(raw_txt: &[u8], num_chunks: usize) -> Vec<&[u8]>`
- `pub fn compute_quote_mask_64(quote_mask_raw: u64) -> u64` (Prefix-XOR em $\mathbb{F}_2$)
