use crate::quic::error::QuicTransportError;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::sync::atomic::{AtomicU32, Ordering};

pub const DEFAULT_ARENA_SIZE: usize = 2 * 1024 * 1024; // 2 MB
pub const DEFAULT_SLOT_SIZE: usize = 64 * 1024;        // 64 KB
pub const DEFAULT_TOTAL_SLOTS: usize = DEFAULT_ARENA_SIZE / DEFAULT_SLOT_SIZE; // 32 slots

/// Arena de memória contígua alinhada em página para IORING_REGISTER_BUFFERS
pub struct RegisteredMemoryArena {
    ptr: *mut u8,
    layout: Layout,
    slot_size: usize,
    total_slots: usize,
    /// Bitmask atômico para alocação/liberação lock-free de slots (até 32 slots)
    free_slots_mask: AtomicU32,
}

// Safety: A arena gerencia ponteiros de memória contígua fixada acessados com segurança
unsafe impl Send for RegisteredMemoryArena {}
unsafe impl Sync for RegisteredMemoryArena {}

impl RegisteredMemoryArena {
    /// Cria uma nova arena contígua alinhada a 4096 bytes (ou HugePage 2 MB)
    pub fn new(total_size: usize, slot_size: usize) -> Result<Self, QuicTransportError> {
        let align = 4096;
        let layout = Layout::from_size_align(total_size, align)
            .map_err(|e| QuicTransportError::BufferRegistrationFailed(e.to_string()))?;

        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(QuicTransportError::BufferRegistrationFailed(
                "Falha ao alocar memória contígua para arena de I/O".to_string(),
            ));
        }

        let total_slots = total_size / slot_size;
        if total_slots > 32 {
            unsafe { dealloc(ptr, layout) };
            return Err(QuicTransportError::BufferRegistrationFailed(
                "Capacidade máxima de 32 slots por arena excedida".to_string(),
            ));
        }

        // Todos os slots inicialmente livres (bit 1 = livre)
        let mask = if total_slots == 32 {
            !0u32
        } else {
            (1u32 << total_slots) - 1
        };

        Ok(Self {
            ptr,
            layout,
            slot_size,
            total_slots,
            free_slots_mask: AtomicU32::new(mask),
        })
    }

    #[inline(always)]
    pub fn raw_ptr(&self) -> *mut u8 {
        self.ptr
    }

    #[inline(always)]
    pub fn total_bytes(&self) -> usize {
        self.layout.size()
    }

    #[inline(always)]
    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    #[inline(always)]
    pub fn total_slots(&self) -> usize {
        self.total_slots
    }

    /// Aloca um slot livre da arena (retorna índice do slot) em O(1) lock-free
    pub fn acquire_slot(&self) -> Option<usize> {
        loop {
            let current = self.free_slots_mask.load(Ordering::Acquire);
            if current == 0 {
                return None; // Sem slots livres
            }
            let slot_idx = current.trailing_zeros() as usize;
            let next = current & !(1u32 << slot_idx);
            if self.free_slots_mask.compare_exchange_weak(
                current,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            ).is_ok() {
                return Some(slot_idx);
            }
        }
    }

    /// Libera um slot de volta para a arena em O(1) lock-free
    pub fn release_slot(&self, slot_idx: usize) {
        if slot_idx < self.total_slots {
            self.free_slots_mask.fetch_or(1u32 << slot_idx, Ordering::Release);
        }
    }

    /// Obtém fatia mutável segura para escrita em um slot adquirido
    pub fn get_slot_slice_mut(&self, slot_idx: usize) -> Option<&mut [u8]> {
        if slot_idx >= self.total_slots {
            return None;
        }
        let offset = slot_idx * self.slot_size;
        unsafe {
            let slot_ptr = self.ptr.add(offset);
            Some(std::slice::from_raw_parts_mut(slot_ptr, self.slot_size))
        }
    }

    /// Obtém fatia imutável para leitura em um slot adquirido
    pub fn get_slot_slice(&self, slot_idx: usize) -> Option<&[u8]> {
        if slot_idx >= self.total_slots {
            return None;
        }
        let offset = slot_idx * self.slot_size;
        unsafe {
            let slot_ptr = self.ptr.add(offset);
            Some(std::slice::from_raw_parts(slot_ptr, self.slot_size))
        }
    }
}

impl Drop for RegisteredMemoryArena {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                dealloc(self.ptr, self.layout);
            }
        }
    }
}
