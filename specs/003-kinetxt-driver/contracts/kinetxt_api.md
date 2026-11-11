# Contract: KineTxt Driver Public API (`kineplex_core::kinetxt`)

## 1. Contrato da API Pública

```rust
use arrow_array::RecordBatch;

pub trait TextBlockParser: Send + Sync {
    /// Converte um bloco de texto posicional com stride constante em RecordBatch Arrow
    fn parse_fixed_width(&self, raw_bytes: &[u8], stride: usize) -> Result<RecordBatch, String>;

    /// Converte um bloco de texto delimitado por caractere em RecordBatch Arrow
    fn parse_delimited(&self, raw_bytes: &[u8]) -> Result<RecordBatch, String>;
}
```

## 2. Invariantes do Contrato
1. **Zero-Copy**: Nenhuma cópia intermediária de fatias para structs `String` no heap; os buffers `StringArray` do Arrow devem apontar diretamente para a memória alocada contígua.
2. **Determinismo Numérico**: Campos numéricos com zeros à esquerda ou casas decimais fixas são convertidos estritamente sem perda de precisão em ponto flutuante IEEE 754 de 64 bits.
3. **Imunidade a Pânico**: Sequências de bytes truncadas ou caracteres não numéricos retornam `0.0` ou tratam o valor sem disparar panics no runtime.
