use crate::kinetxt::driver::TxtColumnSpec;
use crate::kinetxt::swar::fast_parse_f64_ascii;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_buffer::{Buffer, MutableBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use rayon::prelude::*;
use std::sync::Arc;

/// Calcula a máscara analítica de aspas (QuoteMask) em GF(2)[x] usando soma cumulativa módulo 2 (Prefix-XOR).
/// Equivalente à multiplicação livre de transporte (CLMUL) com 0xFFFF_FFFF_FFFF_FFFF.
/// Retorna a máscara de 64 bits para o bloco e se o bloco encerra dentro de aspas para o próximo chunk.
#[inline(always)]
pub fn compute_quote_mask_64(quote_mask_raw: u64, prev_in_quotes: bool) -> (u64, bool) {
    // Redução rápida de Prefix-XOR paralelo em GF(2) (Hillis-Steele)
    let mut q = quote_mask_raw;
    q ^= q << 1;
    q ^= q << 2;
    q ^= q << 4;
    q ^= q << 8;
    q ^= q << 16;
    q ^= q << 32;

    // Propaga o estado de aspas do bloco anterior
    if prev_in_quotes {
        q = !q;
    }

    // Bit 63 determina se encerra dentro de aspas
    let next_in_quotes = (q & (1u64 << 63)) != 0;
    (q, next_in_quotes)
}

/// Remove aspas externas envolventes de um campo
#[inline(always)]
pub fn clean_quoted_field(mut slice: &[u8]) -> &[u8] {
    if slice.len() >= 2 && slice.starts_with(b"\"") && slice.ends_with(b"\"") {
        slice = &slice[1..slice.len() - 1];
    }
    slice
}

/// Extrai a k-ésima coluna delimitada dentro de uma linha respeitando aspas duplas via bitmasks de 64 bits
#[inline(always)]
pub fn extract_nth_delimited_field(
    line: &[u8],
    delim: u8,
    target_col: usize,
) -> &[u8] {
    let mut fields = vec![&b""[..]; target_col + 1];
    extract_fields_single_pass(line, delim, target_col, &mut fields);
    fields.get(target_col).copied().unwrap_or(&[])
}

/// Extrai todos os campos necessários de uma linha em passagem única vetorizada
#[inline(always)]
pub fn extract_fields_single_pass<'a>(
    mut line: &'a [u8],
    delim: u8,
    max_col: usize,
    output: &mut [&'a [u8]],
) {
    if line.ends_with(b"\r") {
        line = &line[..line.len() - 1];
    }
    for item in output.iter_mut() {
        *item = &[];
    }
    if line.is_empty() {
        return;
    }

    // Caminho rápido: se não houver aspas na linha, usa busca direta com memchr
    if memchr::memchr(b'"', line).is_none() {
        let mut start = 0;
        let mut col = 0;
        while col <= max_col && col < output.len() {
            if let Some(pos) = memchr::memchr(delim, &line[start..]) {
                output[col] = &line[start..start + pos];
                start = start + pos + 1;
                col += 1;
            } else {
                output[col] = &line[start..];
                break;
            }
        }
        return;
    }

    // Caminho com aspas usando Quote-Masking em GF(2)
    let mut col = 0;
    let mut field_start = 0;
    let mut in_quotes = false;

    let chunks_64 = line.len() / 64;
    for c in 0..chunks_64 {
        let base = c * 64;
        let block = &line[base..base + 64];

        let mut raw_quotes: u64 = 0;
        let mut raw_delims: u64 = 0;

        for (i, &b) in block.iter().enumerate().take(64) {
            if b == b'"' {
                raw_quotes |= 1u64 << i;
            } else if b == delim {
                raw_delims |= 1u64 << i;
            }
        }

        let (quote_mask, next_in_quotes) = compute_quote_mask_64(raw_quotes, in_quotes);
        in_quotes = next_in_quotes;

        let mut valid_delims = raw_delims & !quote_mask;

        while valid_delims != 0 {
            let bit_pos = valid_delims.trailing_zeros() as usize;
            let delim_pos = base + bit_pos;
            if col < output.len() {
                output[col] = clean_quoted_field(&line[field_start..delim_pos]);
            }
            col += 1;
            field_start = delim_pos + 1;
            valid_delims &= valid_delims - 1; // BLSR
            if col > max_col {
                return;
            }
        }
    }

    // Tail (< 64 bytes)
    let tail_start = chunks_64 * 64;
    if tail_start < line.len() {
        let tail_len = line.len() - tail_start;
        let mut raw_quotes: u64 = 0;
        let mut raw_delims: u64 = 0;
        for i in 0..tail_len {
            let b = line[tail_start + i];
            if b == b'"' {
                raw_quotes |= 1u64 << i;
            } else if b == delim {
                raw_delims |= 1u64 << i;
            }
        }

        let (quote_mask, _) = compute_quote_mask_64(raw_quotes, in_quotes);
        let mut valid_delims = (raw_delims & !quote_mask) & ((1u64 << tail_len) - 1);

        while valid_delims != 0 {
            let bit_pos = valid_delims.trailing_zeros() as usize;
            let delim_pos = tail_start + bit_pos;
            if col < output.len() {
                output[col] = clean_quoted_field(&line[field_start..delim_pos]);
            }
            col += 1;
            field_start = delim_pos + 1;
            valid_delims &= valid_delims - 1; // BLSR
            if col > max_col {
                return;
            }
        }
    }

    if col <= max_col && col < output.len() && field_start <= line.len() {
        output[col] = clean_quoted_field(&line[field_start..]);
    }
}

enum ColumnChunk {
    Float64(Vec<f64>),
    Int64(Vec<i64>),
    Utf8 {
        values: Vec<u8>,
        offsets: Vec<i32>,
    },
}

/// Scanner Delimitado Vetorizado com Bitmasks de 64 bits e Quote-Masking em GF(2)
pub struct DelimitedSimdScanner;

impl DelimitedSimdScanner {
    /// Extrai os limites das linhas (start, end) escaneando blocos de 64 bytes
    /// e filtrando quebras de linha protegidas por aspas via QuoteMask em GF(2)
    pub fn extract_line_offsets(raw_txt: &[u8]) -> Vec<(usize, usize)> {
        if raw_txt.is_empty() {
            return Vec::new();
        }

        let mut line_offsets: Vec<(usize, usize)> = Vec::with_capacity(raw_txt.len() / 48 + 1);
        let mut line_start = 0usize;
        let mut in_quotes = false;

        let chunks_64 = raw_txt.len() / 64;
        for c in 0..chunks_64 {
            let base = c * 64;
            let block = &raw_txt[base..base + 64];

            let mut raw_quotes: u64 = 0;
            let mut raw_newlines: u64 = 0;

            for (i, &b) in block.iter().enumerate().take(64) {
                if b == b'"' {
                    raw_quotes |= 1u64 << i;
                } else if b == b'\n' {
                    raw_newlines |= 1u64 << i;
                }
            }

            let (quote_mask, next_in_quotes) = compute_quote_mask_64(raw_quotes, in_quotes);
            in_quotes = next_in_quotes;

            // Filtra quebras de linha válidas eliminando quebras dentro de aspas
            let mut valid_newlines = raw_newlines & !quote_mask;

            while valid_newlines != 0 {
                let bit_pos = valid_newlines.trailing_zeros() as usize;
                let line_end = base + bit_pos;
                if line_end >= line_start {
                    line_offsets.push((line_start, line_end));
                }
                line_start = line_end + 1;
                valid_newlines &= valid_newlines - 1; // BLSR
            }
        }

        // Tail loop para os bytes restantes (< 64 bytes)
        let tail_start = chunks_64 * 64;
        if tail_start < raw_txt.len() {
            let tail_len = raw_txt.len() - tail_start;
            let mut raw_quotes: u64 = 0;
            let mut raw_newlines: u64 = 0;
            for i in 0..tail_len {
                let b = raw_txt[tail_start + i];
                if b == b'"' {
                    raw_quotes |= 1u64 << i;
                } else if b == b'\n' {
                    raw_newlines |= 1u64 << i;
                }
            }

            let (quote_mask, _) = compute_quote_mask_64(raw_quotes, in_quotes);
            let mut valid_newlines = (raw_newlines & !quote_mask) & ((1u64 << tail_len) - 1);

            while valid_newlines != 0 {
                let bit_pos = valid_newlines.trailing_zeros() as usize;
                let line_end = tail_start + bit_pos;
                if line_end >= line_start {
                    line_offsets.push((line_start, line_end));
                }
                line_start = line_end + 1;
                valid_newlines &= valid_newlines - 1; // BLSR
            }
        }

        if line_start < raw_txt.len() {
            line_offsets.push((line_start, raw_txt.len()));
        }

        line_offsets
    }

    /// Converte um buffer de texto delimitado em um RecordBatch Arrow
    pub fn parse(
        specs: &[TxtColumnSpec],
        raw_txt: &[u8],
        delim: u8,
    ) -> Result<RecordBatch, String> {
        let line_offsets = Self::extract_line_offsets(raw_txt);
        let num_rows = line_offsets.len();

        let max_col = specs
            .iter()
            .map(|s| match s {
                TxtColumnSpec::Float64 { col_index, .. }
                | TxtColumnSpec::Int64 { col_index, .. }
                | TxtColumnSpec::Utf8 { col_index, .. } => *col_index,
            })
            .max()
            .unwrap_or(0);

        // Processa lotes de linhas em paralelo via Rayon
        let chunk_size = (num_rows / rayon::current_num_threads().max(1)).clamp(512, 8192);
        let chunk_results: Vec<Vec<ColumnChunk>> = line_offsets
            .par_chunks(chunk_size)
            .map(|chunk_offsets| {
                let n = chunk_offsets.len();
                let mut cols: Vec<ColumnChunk> = specs
                    .iter()
                    .map(|spec| match spec {
                        TxtColumnSpec::Float64 { .. } => ColumnChunk::Float64(Vec::with_capacity(n)),
                        TxtColumnSpec::Int64 { .. } => ColumnChunk::Int64(Vec::with_capacity(n)),
                        TxtColumnSpec::Utf8 { .. } => {
                            let mut offsets = Vec::with_capacity(n + 1);
                            offsets.push(0i32);
                            ColumnChunk::Utf8 {
                                values: Vec::with_capacity(n * 24),
                                offsets,
                            }
                        }
                    })
                    .collect();

                let mut row_fields = vec![&b""[..]; max_col + 1];

                for &(l_start, l_end) in chunk_offsets {
                    extract_fields_single_pass(&raw_txt[l_start..l_end], delim, max_col, &mut row_fields);

                    for (spec_idx, spec) in specs.iter().enumerate() {
                        match spec {
                            TxtColumnSpec::Float64 { col_index, scale, .. } => {
                                let slice = row_fields.get(*col_index).copied().unwrap_or(&[]);
                                let val = fast_parse_f64_ascii(slice, *scale);
                                if let ColumnChunk::Float64(ref mut vec) = cols[spec_idx] {
                                    vec.push(val);
                                }
                            }
                            TxtColumnSpec::Int64 { col_index, .. } => {
                                let slice = row_fields.get(*col_index).copied().unwrap_or(&[]);
                                let val = fast_parse_f64_ascii(slice, 1.0) as i64;
                                if let ColumnChunk::Int64(ref mut vec) = cols[spec_idx] {
                                    vec.push(val);
                                }
                            }
                            TxtColumnSpec::Utf8 { col_index, .. } => {
                                let slice = row_fields.get(*col_index).copied().unwrap_or(&[]);
                                if let ColumnChunk::Utf8 { ref mut values, ref mut offsets } = cols[spec_idx] {
                                    values.extend_from_slice(slice);
                                    let last = *offsets.last().unwrap();
                                    offsets.push(last + slice.len() as i32);
                                }
                            }
                        }
                    }
                }

                cols
            })
            .collect();

        // Consolida colunas finais Arrow
        let mut arrow_fields = Vec::with_capacity(specs.len());
        let mut arrow_columns: Vec<Arc<dyn Array>> = Vec::with_capacity(specs.len());

        for (spec_idx, spec) in specs.iter().enumerate() {
            match spec {
                TxtColumnSpec::Float64 { name, .. } => {
                    let mut vals = Vec::with_capacity(num_rows);
                    for chunk in &chunk_results {
                        if let ColumnChunk::Float64(ref v) = chunk[spec_idx] {
                            vals.extend_from_slice(v);
                        }
                    }
                    arrow_fields.push(Field::new(name, DataType::Float64, false));
                    arrow_columns.push(Arc::new(Float64Array::from(vals)));
                }
                TxtColumnSpec::Int64 { name, .. } => {
                    let mut vals = Vec::with_capacity(num_rows);
                    for chunk in &chunk_results {
                        if let ColumnChunk::Int64(ref v) = chunk[spec_idx] {
                            vals.extend_from_slice(v);
                        }
                    }
                    arrow_fields.push(Field::new(name, DataType::Int64, false));
                    arrow_columns.push(Arc::new(Int64Array::from(vals)));
                }
                TxtColumnSpec::Utf8 { name, .. } => {
                    let mut total_values = MutableBuffer::with_capacity(num_rows * 24);
                    let mut total_offsets = Vec::with_capacity(num_rows + 1);
                    total_offsets.push(0i32);
                    let mut cumulative_bytes = 0i32;

                    for chunk in &chunk_results {
                        if let ColumnChunk::Utf8 { ref values, ref offsets } = chunk[spec_idx] {
                            total_values.extend_from_slice(values);
                            // offsets[0] é 0, offsets[1..] são os deltas locais do chunk
                            for &off in &offsets[1..] {
                                total_offsets.push(cumulative_bytes + off);
                            }
                            cumulative_bytes += values.len() as i32;
                        }
                    }

                    let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(total_offsets));
                    let str_array = unsafe {
                        StringArray::new_unchecked(offset_buffer, Buffer::from(total_values), None)
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
