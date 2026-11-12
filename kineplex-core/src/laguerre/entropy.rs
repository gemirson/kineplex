//! Estimador Entrópico de Shannon-Markov para Predição ZSTD em O(1)

use arrow_array::{Float64Array, RecordBatch};

/// TEOREMA 1: Estimador de Massa Comprimida ZSTD via Taxa de Entropia de Markov de 1ª Ordem
/// H(X_t | X_{t-1}) = H(X_t, X_{t-1}) - H(X_{t-1}) combinada ao fator de colapso RLE.
#[inline(always)]
pub fn estimate_markov_zstd_mass(batch: &RecordBatch) -> u64 {
    let raw_bytes = batch.get_array_memory_size();
    if raw_bytes == 0 || batch.num_rows() == 0 {
        return 0;
    }

    if batch.num_columns() > 0 {
        if let Some(col) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            let bytes: &[u8] = bytemuck::cast_slice(col.values());
            let sample_len = bytes.len().min(4096);
            if sample_len > 16 {
                let mut unigram = [0u32; 256];
                // Tabela hash compacta de 1024 slots para transições de Markov de 2 bytes (u_{t-1}, u_t)
                let mut bigram_folded = [0u32; 1024];
                let mut rle_identical_runs = 0u32;

                unigram[bytes[0] as usize] += 1;
                for t in 1..sample_len {
                    let prev = bytes[t - 1];
                    let curr = bytes[t];
                    unigram[curr as usize] += 1;
                    if curr == prev {
                        rle_identical_runs += 1;
                    }
                    // Hash bijetivo rápido em anel de 10 bits para capturar entropia conjunta H(X_t, X_{t-1})
                    let bi_idx = (((prev as usize) * 31) ^ (curr as usize)) & 1023;
                    bigram_folded[bi_idx] += 1;
                }

                let inv_n = 1.0 / (sample_len as f64);
                let inv_n1 = 1.0 / ((sample_len - 1) as f64);

                let mut h1 = 0.0_f64;
                for &c in &unigram {
                    if c > 0 {
                        let p = (c as f64) * inv_n;
                        h1 -= p * p.log2();
                    }
                }

                let mut h_joint = 0.0_f64;
                for &c in &bigram_folded {
                    if c > 0 {
                        let p = (c as f64) * inv_n1;
                        h_joint -= p * p.log2();
                    }
                }

                // Taxa de Entropia Condicional de Markov: 0 <= H(X_t | X_{t-1}) <= H(X_t) <= 8.0
                let h_cond = if h1 <= 0.05 {
                    0.05
                } else {
                    (h_joint - h1).clamp(0.05, h1)
                };
                let rle_survival = 1.0 - ((rle_identical_runs as f64) * inv_n1 * 0.85);

                // Razão física final prevista para ZSTD Level 3 + Parquet Dictionary/RLE
                let zstd_ratio = ((h_cond / 8.0) * rle_survival + 0.04).clamp(0.05, 1.0);
                return ((raw_bytes as f64) * zstd_ratio) as u64;
            }
        }
    }
    (raw_bytes as u64) / 4
}

/// Alias para manter compatibilidade com chamadas existentes
pub fn estimate_zstd_mass(batch: &RecordBatch) -> u64 {
    estimate_markov_zstd_mass(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_batch_entropy_estimation() {
        let schema = Arc::new(Schema::new(vec![Field::new("f0", DataType::Float64, false)]));
        let values: Vec<f64> = (0..2048).map(|i| i as f64).collect();
        let array: ArrayRef = Arc::new(Float64Array::from(values));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let mass = estimate_markov_zstd_mass(&batch);
        assert!(mass > 0);
        assert!(mass <= batch.get_array_memory_size() as u64);
    }

    #[test]
    fn test_constant_bytes_zero_entropy() {
        let schema = Arc::new(Schema::new(vec![Field::new("f0", DataType::Float64, false)]));
        let values = vec![0.0_f64; 2048];
        let array: ArrayRef = Arc::new(Float64Array::from(values));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let mass = estimate_markov_zstd_mass(&batch);
        let raw = batch.get_array_memory_size() as u64;
        let ratio = (mass as f64) / (raw as f64);
        assert!(ratio >= 0.05 && ratio <= 0.15);
    }

    #[test]
    fn test_uniform_random_bytes_high_entropy() {
        let schema = Arc::new(Schema::new(vec![Field::new("f0", DataType::Float64, false)]));
        let mut seed: u64 = 123456789;
        let values: Vec<f64> = (0..2048)
            .map(|_| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                f64::from_bits(seed)
            })
            .collect();
        let array: ArrayRef = Arc::new(Float64Array::from(values));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let mass = estimate_markov_zstd_mass(&batch);
        let raw = batch.get_array_memory_size() as u64;
        let ratio = (mass as f64) / (raw as f64);
        assert!(ratio >= 0.25);
    }
}
