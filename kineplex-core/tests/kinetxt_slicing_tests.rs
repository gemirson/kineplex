use kineplex_core::kinetxt::slicing::split_txt_buffer_into_aligned_chunks;

#[test]
fn test_split_txt_buffer_into_aligned_chunks() {
    let mut raw_data = Vec::new();
    for i in 0..5000 {
        let line = format!("line_{:05}_payload_content_1234567890\n", i);
        raw_data.extend_from_slice(line.as_bytes());
    }

    let total_len = raw_data.len();
    let num_chunks = 4;
    let chunks = split_txt_buffer_into_aligned_chunks(&raw_data, num_chunks);

    assert_eq!(chunks.len(), num_chunks);

    // Soma das fatias deve ser igual ao total
    let sum_len: usize = chunks.iter().map(|c| c.len()).sum();
    assert_eq!(sum_len, total_len);

    // Todo chunk deve terminar com '\n'
    for (idx, chunk) in chunks.iter().enumerate() {
        assert_eq!(
            chunk[chunk.len() - 1],
            b'\n',
            "Chunk {} não termina em '\\n'",
            idx
        );
    }

    // A partir do chunk 1, o primeiro caractere deve ser o início de uma nova linha ("line_")
    for (idx, chunk) in chunks.iter().enumerate().skip(1) {
        assert!(
            chunk.starts_with(b"line_"),
            "Chunk {} não começa no início de uma linha válida",
            idx
        );
    }
}

#[test]
fn test_split_txt_buffer_edge_cases() {
    // Buffer vazio
    let empty_chunks = split_txt_buffer_into_aligned_chunks(b"", 4);
    assert_eq!(empty_chunks.len(), 1);
    assert_eq!(empty_chunks[0], b"");

    // 1 chunk solicitado
    let single = split_txt_buffer_into_aligned_chunks(b"line1\nline2\n", 1);
    assert_eq!(single.len(), 1);
    assert_eq!(single[0], b"line1\nline2\n");

    // 0 chunks solicitado
    let zero_chunks = split_txt_buffer_into_aligned_chunks(b"line1\n", 0);
    assert_eq!(zero_chunks.len(), 1);

    // Buffer sem nenhuma quebra de linha
    let no_nl = b"abcdefghijklmnopqrstuvwxyz";
    let chunks_no_nl = split_txt_buffer_into_aligned_chunks(no_nl, 3);
    let sum_len: usize = chunks_no_nl.iter().map(|c| c.len()).sum();
    assert_eq!(sum_len, no_nl.len());
}
