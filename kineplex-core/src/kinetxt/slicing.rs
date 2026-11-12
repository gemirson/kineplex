use memchr::memchr;

/// Estágio 1: Particionador de Blocos Zero-Copy (Bisseção Assíncrona de Fronteira / Speculative Midpoint Seek)
/// Divide um buffer de texto massivo em N fatias perfeitamente alinhadas em quebras de linha '\n'.
/// Utiliza busca especulativa vetorizada com `memchr` em janelas de até 256 bytes a partir dos midpoints teóricos.
pub fn split_txt_buffer_into_aligned_chunks(raw_txt: &[u8], num_chunks: usize) -> Vec<&[u8]> {
    if raw_txt.is_empty() || num_chunks <= 1 {
        return vec![raw_txt];
    }
    let total_len = raw_txt.len();
    let approx_chunk = (total_len / num_chunks).max(1);
    let mut slices = Vec::with_capacity(num_chunks);
    let mut start = 0;

    for c in 1..num_chunks {
        let target = (c * approx_chunk).min(total_len);
        if target <= start {
            continue;
        }

        // Bisseção especulativa: busca '\n' em uma janela de até 256 bytes com memchr vetorizado
        let window_end = (target + 256).min(total_len);
        let found = if let Some(pos) = memchr(b'\n', &raw_txt[target..window_end]) {
            target + pos + 1
        } else if let Some(pos) = memchr(b'\n', &raw_txt[window_end..total_len]) {
            window_end + pos + 1
        } else {
            total_len
        };

        if found > start {
            slices.push(&raw_txt[start..found]);
            start = found;
        }
        if start >= total_len {
            break;
        }
    }
    if start < total_len {
        slices.push(&raw_txt[start..total_len]);
    }
    slices
}
