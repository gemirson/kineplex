use crate::kinetxt::driver::TxtColumnSpec;
use crate::kinetxt::swar::fast_parse_f64_ascii;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_buffer::{Buffer, MutableBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use rayon::prelude::*;
use std::sync::Arc;

/// Scanner Posicional de Largura Fixa (CNAB 240/400, Mainframe, B3)
/// Opera com stride constante L em tempo O(1): base + row * stride + offset
pub struct FixedWidthPositionalScanner;

impl FixedWidthPositionalScanner {
    /// Converte um buffer de texto posicional com stride constante em um RecordBatch Arrow
    pub fn parse(
        specs: &[TxtColumnSpec],
        raw_txt: &[u8],
        stride: usize,
    ) -> Result<RecordBatch, String> {
        if stride == 0 {
            return Err("Stride não pode ser zero no modo Fixed-Width".to_string());
        }

        let num_rows = raw_txt.len() / stride;
        let mut arrow_fields = Vec::with_capacity(specs.len());
        let mut arrow_columns: Vec<Arc<dyn Array>> = Vec::with_capacity(specs.len());

        for spec in specs {
            match spec {
                TxtColumnSpec::Float64 {
                    name,
                    fixed_range,
                    scale,
                    ..
                } => {
                    let (start_off, end_off) = fixed_range
                        .ok_or_else(|| "Modo Fixed-Width exige fixed_range=(start, end)".to_string())?;
                    if start_off > end_off || end_off > stride {
                        return Err(format!(
                            "Intervalo fixo inválido ({}, {}) para stride {}",
                            start_off, end_off, stride
                        ));
                    }
                    let s = *scale;

                    // Paraleliza por linha com Rayon indexando diretamente em O(1)
                    let values: Vec<f64> = (0..num_rows)
                        .into_par_iter()
                        .map(|row_idx| {
                            let base = row_idx * stride;
                            if base + end_off <= raw_txt.len() {
                                let field_bytes = &raw_txt[base + start_off..base + end_off];
                                fast_parse_f64_ascii(field_bytes, s)
                            } else {
                                0.0
                            }
                        })
                        .collect();

                    arrow_fields.push(Field::new(name, DataType::Float64, false));
                    arrow_columns.push(Arc::new(Float64Array::from(values)));
                }
                TxtColumnSpec::Int64 {
                    name,
                    fixed_range,
                    ..
                } => {
                    let (start_off, end_off) = fixed_range
                        .ok_or_else(|| "Modo Fixed-Width exige fixed_range=(start, end)".to_string())?;
                    if start_off > end_off || end_off > stride {
                        return Err(format!(
                            "Intervalo fixo inválido ({}, {}) para stride {}",
                            start_off, end_off, stride
                        ));
                    }

                    let values: Vec<i64> = (0..num_rows)
                        .into_par_iter()
                        .map(|row_idx| {
                            let base = row_idx * stride;
                            if base + end_off <= raw_txt.len() {
                                let field_bytes = &raw_txt[base + start_off..base + end_off];
                                fast_parse_f64_ascii(field_bytes, 1.0) as i64
                            } else {
                                0
                            }
                        })
                        .collect();

                    arrow_fields.push(Field::new(name, DataType::Int64, false));
                    arrow_columns.push(Arc::new(Int64Array::from(values)));
                }
                TxtColumnSpec::Utf8 {
                    name,
                    fixed_range,
                    ..
                } => {
                    let (start_off, end_off) = fixed_range
                        .ok_or_else(|| "Modo Fixed-Width exige fixed_range=(start, end)".to_string())?;
                    if start_off > end_off || end_off > stride {
                        return Err(format!(
                            "Intervalo fixo inválido ({}, {}) para stride {}",
                            start_off, end_off, stride
                        ));
                    }
                    let field_len = end_off.saturating_sub(start_off);

                    // Constrói os buffers nativos do StringArray do Arrow sem alocar Vec<String>
                    let mut offsets = Vec::with_capacity(num_rows + 1);
                    let mut values_buf = MutableBuffer::with_capacity(num_rows * field_len);
                    offsets.push(0i32);

                    let mut curr_offset = 0i32;
                    for row_idx in 0..num_rows {
                        let base = row_idx * stride;
                        if base + end_off <= raw_txt.len() {
                            let slice = &raw_txt[base + start_off..base + end_off];
                            values_buf.extend_from_slice(slice);
                            curr_offset += field_len as i32;
                        }
                        offsets.push(curr_offset);
                    }

                    let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
                    let str_array = unsafe {
                        StringArray::new_unchecked(offset_buffer, Buffer::from(values_buf), None)
                    };

                    arrow_fields.push(Field::new(name, DataType::Utf8, false));
                    arrow_columns.push(Arc::new(str_array));
                }
            }
        }

        let schema = Arc::new(Schema::new(arrow_fields));
        RecordBatch::try_new(schema, arrow_columns).map_err(|e| e.to_string())
    }
}
