//! # Hardware Mechanical Sympathy Memory Subsystem (`memory`)
//!
//! Substitui o `UnsafeRow` (orientado a linhas de 8 bytes da JVM/Tungsten) por
//! buffers colunares contíguos estritamente alinhados a linhas de cache de **64 bytes**
//! (`CACHE_LINE_BYTES = 64`), eliminando *split-line loads* na L1d/L2 e habilitando
//! instruções vetoriais alinhadas (`VMOVAPD` / `VMOVDQA` em AVX2 / AVX-512).

use std::alloc::{self, Layout};
use std::fmt;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::ptr::NonNull;
use std::slice;

/// Tamanho exato da linha de cache L1d/L2/L3 em arquiteturas x86_64 modernas e ARM64.
pub const CACHE_LINE_BYTES: usize = 64;

/// Tamanho ótimo do micro-lote (Batch) vetorizado para residir 100% na Cache L1d (32 KiB/core).
/// 2048 linhas * 8 bytes = 16 KiB por coluna ativa.
pub const L1D_OPTIMAL_BATCH_SIZE: usize = 2048;

/// Buffer contíguo alocado diretamente no alinhamento de linha de cache de 64 bytes.
/// Zero overhead de cabeçalho de objeto (ao contrário do `Array` na JVM que possui 16-24 bytes de Mark/Klass header).
pub struct AlignedBuffer<T: Copy> {
    ptr: NonNull<T>,
    len: usize,
    cap: usize,
}

unsafe impl<T: Copy + Send> Send for AlignedBuffer<T> {}
unsafe impl<T: Copy + Sync> Sync for AlignedBuffer<T> {}

impl<T: Copy> AlignedBuffer<T> {
    #[inline]
    fn layout_for(cap: usize) -> Layout {
        let size = cap
            .checked_mul(std::mem::size_of::<T>())
            .expect("Overflow no calculo de tamanho do AlignedBuffer");
        let align = std::mem::align_of::<T>().max(CACHE_LINE_BYTES);
        // Arredonda o tamanho para múltiplo da linha de cache para evitar false sharing na cauda
        let padded_size = if size == 0 {
            align
        } else {
            (size + align - 1) & !(align - 1)
        };
        Layout::from_size_align(padded_size, align).expect("Layout de cache-line invalido")
    }

    /// Aloca um buffer alinhado a 64 bytes inicializado com `default_val` para `len` elementos.
    pub fn from_elem(default_val: T, len: usize) -> Self {
        assert!(std::mem::size_of::<T>() > 0, "ZST nao suportado em AlignedBuffer");
        let cap = len.max(1);
        let layout = Self::layout_for(cap);
        let raw_ptr = unsafe { alloc::alloc_zeroed(layout) as *mut T };
        let ptr = NonNull::new(raw_ptr).unwrap_or_else(|| alloc::handle_alloc_error(layout));

        let mut buf = Self { ptr, len, cap };
        // Se `default_val` não for todos-zeros, preenchemos de forma contígua
        let is_zero = unsafe {
            let bytes = slice::from_raw_parts(&default_val as *const T as *const u8, std::mem::size_of::<T>());
            bytes.iter().all(|&b| b == 0)
        };
        if !is_zero && len > 0 {
            buf.as_mut_slice().fill(default_val);
        }
        buf
    }

    /// Aloca um buffer alinhado a 64 bytes copiando os dados de um slice existente.
    pub fn from_slice(src: &[T]) -> Self {
        assert!(std::mem::size_of::<T>() > 0, "ZST nao suportado em AlignedBuffer");
        let len = src.len();
        let cap = len.max(1);
        let layout = Self::layout_for(cap);
        let raw_ptr = unsafe { alloc::alloc(layout) as *mut T };
        let ptr = NonNull::new(raw_ptr).unwrap_or_else(|| alloc::handle_alloc_error(layout));
        if len > 0 {
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr(), ptr.as_ptr(), len);
            }
        }
        Self { ptr, len, cap }
    }

    /// Constrói um buffer alinhado a 64 bytes usando uma função geradora `f(idx)`.
    pub fn tabulate<F: FnMut(usize) -> T>(len: usize, mut f: F) -> Self {
        assert!(std::mem::size_of::<T>() > 0, "ZST nao suportado em AlignedBuffer");
        let cap = len.max(1);
        let layout = Self::layout_for(cap);
        let raw_ptr = unsafe { alloc::alloc(layout) as *mut T };
        let ptr = NonNull::new(raw_ptr).unwrap_or_else(|| alloc::handle_alloc_error(layout));
        let base = ptr.as_ptr();
        for i in 0..len {
            unsafe {
                base.add(i).write(f(i));
            }
        }
        Self { ptr, len, cap }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Verifica em tempo de execução que o ponteiro base está alinhado a 64 bytes (Cache Line).
    #[inline(always)]
    pub fn is_cache_line_aligned(&self) -> bool {
        (self.ptr.as_ptr() as usize) % CACHE_LINE_BYTES == 0
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Acesso sem bound-check para laços internos onde o índice já foi mascarado (ex: `& DAY_MASK`).
    ///
    /// # Safety
    /// O chamador deve garantir que `idx < self.len`.
    #[inline(always)]
    pub unsafe fn get_unchecked_val(&self, idx: usize) -> T {
        debug_assert!(idx < self.len);
        *self.ptr.as_ptr().add(idx)
    }
}

impl<T: Copy> Clone for AlignedBuffer<T> {
    fn clone(&self) -> Self {
        Self::from_slice(self.as_slice())
    }
}

impl<T: Copy> Deref for AlignedBuffer<T> {
    type Target = [T];
    #[inline(always)]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: Copy> DerefMut for AlignedBuffer<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T: Copy> Index<usize> for AlignedBuffer<T> {
    type Output = T;
    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<T: Copy> IndexMut<usize> for AlignedBuffer<T> {
    #[inline(always)]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.as_mut_slice()[index]
    }
}

impl<T: Copy + fmt::Debug> fmt::Debug for AlignedBuffer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlignedBuffer")
            .field("len", &self.len)
            .field("aligned_64b", &self.is_cache_line_aligned())
            .field("data", &self.as_slice())
            .finish()
    }
}

impl<T: Copy + PartialEq> PartialEq for AlignedBuffer<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Copy + Eq> Eq for AlignedBuffer<T> {}

impl<T: Copy> Drop for AlignedBuffer<T> {
    fn drop(&mut self) {
        let layout = Self::layout_for(self.cap);
        unsafe {
            alloc::dealloc(self.ptr.as_ptr() as *mut u8, layout);
        }
    }
}

/// Arena de registradores vetoriais com simpatia mecânica para eliminar alocações dinâmicas
/// na avaliação de expressões da AST e fusão de estágios.
pub struct VectorArena {
    storage: AlignedBuffer<u8>,
    cursor: usize,
}

impl VectorArena {
    /// Inicializa a arena com capacidade em bytes (padrão 256 KiB, cabendo na Cache L2).
    pub fn new() -> Self {
        Self::with_capacity(256 * 1024)
    }

    /// Inicializa a arena com capacidade arbitrária alinhada a 64 bytes.
    pub fn with_capacity(capacity_bytes: usize) -> Self {
        let cap = capacity_bytes.max(CACHE_LINE_BYTES);
        Self {
            storage: AlignedBuffer::from_elem(0_u8, cap),
            cursor: 0,
        }
    }

    /// Aloca uma fatia contígua alinhada a 64 bytes de `count` elementos do tipo `T`.
    #[inline(always)]
    pub fn alloc_slice_mut<T: Copy>(&mut self, count: usize) -> &mut [T] {
        let align = std::mem::align_of::<T>().max(CACHE_LINE_BYTES);
        let size = count * std::mem::size_of::<T>();
        let offset = (self.cursor + align - 1) & !(align - 1);
        if offset + size > self.storage.len() {
            // Em caso de extrapolação da capacidade, expande dobrando o buffer
            let new_cap = (self.storage.len() * 2).max(offset + size);
            self.storage = AlignedBuffer::from_elem(0_u8, new_cap);
            self.cursor = 0;
            return self.alloc_slice_mut(count);
        }
        self.cursor = offset + size;
        unsafe {
            let ptr = self.storage.as_mut_ptr().add(offset) as *mut T;
            slice::from_raw_parts_mut(ptr, count)
        }
    }

    /// Reseta o cursor para reutilização imediata sem desalocação de silício.
    #[inline(always)]
    pub fn reset(&mut self) {
        self.cursor = 0;
    }
}

impl Default for VectorArena {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    static THREAD_ARENA: std::cell::RefCell<VectorArena> = std::cell::RefCell::new(VectorArena::new());
}

/// Executa uma operação com a arena de vetorização local da thread corrente.
#[inline(always)]
pub fn with_thread_arena<R, F: FnOnce(&mut VectorArena) -> R>(f: F) -> R {
    THREAD_ARENA.with(|cell| {
        let mut arena = cell.borrow_mut();
        let res = f(&mut arena);
        arena.reset();
        res
    })
}
