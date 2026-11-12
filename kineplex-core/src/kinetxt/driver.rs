use crate::kinetxt::delimited::DelimitedSimdScanner;
use crate::kinetxt::fixed_width::FixedWidthPositionalScanner;
use arrow_array::RecordBatch;

/// Especificação de Coluna para o Driver KineTxt (Suporta Delimitado e Posicional de Largura Fixa)
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

/// Contrato para parsers de blocos de texto
pub trait TextBlockParser: Send + Sync {
    fn parse_fixed_width(&self, raw_bytes: &[u8], stride: usize) -> Result<RecordBatch, String>;
    fn parse_delimited(&self, raw_bytes: &[u8]) -> Result<RecordBatch, String>;
}

/// Motor Principal do Driver KineTxt: Converte um bloco bruto .TXT/.CSV em um RecordBatch do Apache Arrow
pub struct KineTxtDriver {
    pub delimiter: u8,
    pub fixed_line_stride: Option<usize>, // Se Some(stride), ativa o modo Mainframe/CNAB O(1) Stride
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

    /// MODO 1: Leitor Posicional de Largura Fixa (CNAB / Mainframe / B3) com Stride Constante O(1)
    /// Salta diretamente via `row * stride + offset` sem escanear quebras de linha '\n'.
    pub fn parse_fixed_width_to_arrow(
        &self,
        raw_txt: &[u8],
        stride: usize,
    ) -> Result<RecordBatch, String> {
        FixedWidthPositionalScanner::parse(&self.schema_specs, raw_txt, stride)
    }

    /// MODO 2: Leitor Delimitado Vetorizado (Máscaras Bitwise de 64 bytes + Prefix-XOR em GF(2))
    /// Escaneia blocos de 64 bytes gerando máscaras de bits para delimitadores e '\n' sem branching.
    pub fn parse_delimited_chunk_to_arrow(&self, raw_txt: &[u8]) -> Result<RecordBatch, String> {
        DelimitedSimdScanner::parse(&self.schema_specs, raw_txt, self.delimiter)
    }
}

impl TextBlockParser for KineTxtDriver {
    fn parse_fixed_width(&self, raw_bytes: &[u8], stride: usize) -> Result<RecordBatch, String> {
        self.parse_fixed_width_to_arrow(raw_bytes, stride)
    }

    fn parse_delimited(&self, raw_bytes: &[u8]) -> Result<RecordBatch, String> {
        self.parse_delimited_chunk_to_arrow(raw_bytes)
    }
}
