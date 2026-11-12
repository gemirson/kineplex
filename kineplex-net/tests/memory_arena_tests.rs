use kineplex_net::quic::arena::{RegisteredMemoryArena, DEFAULT_ARENA_SIZE, DEFAULT_SLOT_SIZE, DEFAULT_TOTAL_SLOTS};

#[test]
fn test_memory_arena_allocation_and_alignment() {
    let arena = RegisteredMemoryArena::new(DEFAULT_ARENA_SIZE, DEFAULT_SLOT_SIZE).unwrap();
    assert_eq!(arena.total_bytes(), 2 * 1024 * 1024);
    assert_eq!(arena.slot_size(), 64 * 1024);
    assert_eq!(arena.total_slots(), DEFAULT_TOTAL_SLOTS);
    assert!(!arena.raw_ptr().is_null());
    assert_eq!((arena.raw_ptr() as usize) % 4096, 0, "A arena deve ser alinhada a 4096 bytes");
}

#[test]
fn test_memory_arena_slot_acquisition_and_release() {
    let arena = RegisteredMemoryArena::new(DEFAULT_ARENA_SIZE, DEFAULT_SLOT_SIZE).unwrap();

    let mut acquired = Vec::new();
    for _ in 0..DEFAULT_TOTAL_SLOTS {
        let slot = arena.acquire_slot();
        assert!(slot.is_some());
        acquired.push(slot.unwrap());
    }

    // Arena esgotada
    assert!(arena.acquire_slot().is_none());

    // Escreve e valida dados em cada slot
    for &slot_idx in &acquired {
        let slice = arena.get_slot_slice_mut(slot_idx).unwrap();
        slice[0] = 0xAA;
        slice[slice.len() - 1] = 0xBB;
    }

    for &slot_idx in &acquired {
        let slice = arena.get_slot_slice(slot_idx).unwrap();
        assert_eq!(slice[0], 0xAA);
        assert_eq!(slice[slice.len() - 1], 0xBB);
    }

    // Libera metade dos slots
    for &slot_idx in &acquired[0..16] {
        arena.release_slot(slot_idx);
    }

    // Adquire novamente exatamente 16 slots
    for _ in 0..16 {
        assert!(arena.acquire_slot().is_some());
    }

    // Esgotado novamente
    assert!(arena.acquire_slot().is_none());
}
