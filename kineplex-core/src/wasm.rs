//! Sandboxed execution primitives for user supplied WebAssembly modules.
//!
//! The process shares one Wasmtime engine. Each invocation gets a fresh store,
//! its own fuel budget, and a linear-memory limit; WASI is deliberately absent.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, Float32Array, RecordBatch};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use dashmap::DashMap;
use ring::digest::{digest, SHA256};
use wasmtime::{
    Caller, Config, Engine, Instance, Linker, Module, OptLevel, Store, StoreLimits,
    StoreLimitsBuilder,
};

/// Maximum linear memory allowed for one WebAssembly instance (2 GiB).
pub const MAX_WASM_MEMORY_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// A process-wide, lazily initialized Wasmtime engine.
static WASM_ENGINE: OnceLock<Result<Engine, String>> = OnceLock::new();
static MODULE_CACHE: OnceLock<ModuleCache> = OnceLock::new();

/// Errors produced while constructing or using the shared Wasmtime engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WasmEngineError(String);

impl Display for WasmEngineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "failed to initialize Wasmtime engine: {}",
            self.0
        )
    }
}

impl Error for WasmEngineError {}

/// Returns the singleton engine, initializing it on the first call.
pub fn wasm_engine() -> Result<&'static Engine, WasmEngineError> {
    WASM_ENGINE
        .get_or_init(|| {
            let mut config = Config::new();
            config
                .cranelift_opt_level(OptLevel::SpeedAndSize)
                .consume_fuel(true)
                .wasm_multi_memory(true);
            Engine::new(&config).map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| WasmEngineError(error.clone()))
}

/// Process-wide cache of compiled modules keyed by SHA-256 of their source bytes.
pub struct ModuleCache {
    modules: DashMap<[u8; 32], Arc<Module>>,
    compilations: AtomicUsize,
}

impl ModuleCache {
    fn new() -> Self {
        Self {
            modules: DashMap::new(),
            compilations: AtomicUsize::new(0),
        }
    }

    fn get_or_compile(
        &self,
        engine: &Engine,
        bytes: &[u8],
    ) -> Result<Arc<Module>, WasmModuleError> {
        let hash: [u8; 32] = digest(&SHA256, bytes)
            .as_ref()
            .try_into()
            .expect("SHA-256 always returns 32 bytes");
        if let Some(module) = self.modules.get(&hash) {
            return Ok(Arc::clone(module.value()));
        }

        // DashMap's entry lock makes concurrent first loads compile exactly once.
        match self.modules.entry(hash) {
            dashmap::mapref::entry::Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let module = Module::new(engine, bytes).map_err(|error| WasmModuleError {
                    hash,
                    message: error.to_string(),
                })?;
                self.compilations.fetch_add(1, Ordering::Relaxed);
                let module = Arc::new(module);
                entry.insert(Arc::clone(&module));
                Ok(module)
            }
        }
    }

    /// Number of successful JIT compilations since process startup.
    #[must_use]
    pub fn compilation_count(&self) -> usize {
        self.compilations.load(Ordering::Relaxed)
    }

    /// Number of compiled modules retained in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.modules.len()
    }

    /// Whether the cache contains no compiled modules.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }
}

/// Returns the process-wide compiled module cache.
#[must_use]
pub fn module_cache() -> &'static ModuleCache {
    MODULE_CACHE.get_or_init(ModuleCache::new)
}

/// A malformed or unsupported Wasm binary, identified by its SHA-256 digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WasmModuleError {
    hash: [u8; 32],
    message: String,
}

impl WasmModuleError {
    /// SHA-256 digest of the rejected input bytes.
    #[must_use]
    pub const fn hash(&self) -> &[u8; 32] {
        &self.hash
    }
}

impl Display for WasmModuleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid WebAssembly module {:02x?}: {}",
            self.hash, self.message
        )
    }
}

impl Error for WasmModuleError {}

/// State isolated to a single Wasmtime store.
struct StoreState {
    limits: StoreLimits,
    arrow_batch: Option<Arc<RecordBatch>>,
}

/// Runs exported, zero-argument WebAssembly functions in a resource-limited store.
#[derive(Clone, Copy)]
pub struct WasmRuntime {
    engine: &'static Engine,
    cache: &'static ModuleCache,
}

impl WasmRuntime {
    /// Gets a runtime backed by the process-wide engine.
    pub fn new() -> Result<Self, WasmEngineError> {
        Ok(Self {
            engine: wasm_engine()?,
            cache: module_cache(),
        })
    }

    /// Compiles `wasm`, then calls its named `() -> ()` export with a fuel budget.
    ///
    /// Wasmtime traps, including fuel exhaustion, are returned to the caller.
    pub fn call(&self, wasm: &[u8], export: &str, fuel: u64) -> Result<(), WasmExecutionError> {
        self.instantiate(wasm, fuel)?.call(export)
    }

    /// Compiles or loads a cached module and creates a fresh, fuel-limited instance.
    pub fn instantiate(&self, wasm: &[u8], fuel: u64) -> Result<WasmInstance, WasmExecutionError> {
        let module = self
            .cache
            .get_or_compile(self.engine, wasm)
            .map_err(WasmExecutionError::Compile)?;
        let mut store = self
            .new_store(fuel, None)
            .map_err(WasmExecutionError::Fuel)?;
        let instance =
            Instance::new(&mut store, &module, &[]).map_err(WasmExecutionError::Instantiate)?;
        Ok(WasmInstance { store, instance })
    }

    /// Calls a guest function with a read-only Arrow `Float32` input capability.
    ///
    /// The guest imports `kineplex.read_f32(column: i32, row: i32) -> f32`.
    /// Values cross the Wasm boundary by value; the guest never receives a host
    /// pointer and cannot mutate the source batch.
    pub fn call_with_arrow_f32(
        &self,
        wasm: &[u8],
        export: &str,
        batch: Arc<RecordBatch>,
        fuel: u64,
    ) -> Result<f32, WasmExecutionError> {
        let module = self
            .cache
            .get_or_compile(self.engine, wasm)
            .map_err(WasmExecutionError::Compile)?;
        let mut store = self
            .new_store(fuel, Some(batch))
            .map_err(WasmExecutionError::Fuel)?;
        let mut linker = Linker::new(self.engine);
        linker
            .func_wrap(
                "kineplex",
                "read_f32",
                |caller: Caller<'_, StoreState>, column: i32, row: i32| -> wasmtime::Result<f32> {
                    if column < 0 || row < 0 {
                        return Err(wasmtime::Error::msg(
                            "Arrow column and row must be non-negative",
                        ));
                    }
                    let batch = caller
                        .data()
                        .arrow_batch
                        .as_ref()
                        .ok_or_else(|| wasmtime::Error::msg("Arrow input is unavailable"))?;
                    let array = batch
                        .columns()
                        .get(column as usize)
                        .ok_or_else(|| wasmtime::Error::msg("Arrow column is out of bounds"))?
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .ok_or_else(|| wasmtime::Error::msg("Arrow input column is not Float32"))?;
                    let row = row as usize;
                    if row >= array.len() {
                        return Err(wasmtime::Error::msg("Arrow row is out of bounds"));
                    }
                    if array.is_null(row) {
                        return Err(wasmtime::Error::msg("Arrow row is null"));
                    }
                    Ok(array.value(row))
                },
            )
            .map_err(WasmExecutionError::Instantiate)?;
        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(WasmExecutionError::Instantiate)?;
        let function = instance
            .get_typed_func::<(), f32>(&mut store, export)
            .map_err(WasmExecutionError::Export)?;
        let started = std::time::Instant::now();
        let result = function
            .call(&mut store, ())
            .map_err(WasmExecutionError::Trap);
        crate::metrics::global().record_wasm_execution(started.elapsed());
        result
    }

    /// Executes the standard `kineplex_run(i32, i32) -> i32` Arrow IPC ABI.
    ///
    /// The guest must export `alloc_ffi(i32) -> i32`, `memory`, and
    /// `kineplex_result_size() -> i32`. Input and output are Arrow IPC streams
    /// copied through guest-owned linear memory. The second argument points to
    /// an 8-byte descriptor containing ABI version `1` and the input byte size.
    /// All memory offsets and lengths are checked before host reads or writes.
    pub fn invoke_arrow(
        &self,
        wasm: &[u8],
        input: &RecordBatch,
        fuel: u64,
    ) -> Result<RecordBatch, WasmExecutionError> {
        let module = self
            .cache
            .get_or_compile(self.engine, wasm)
            .map_err(WasmExecutionError::Compile)?;
        let mut input_bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut input_bytes, input.schema().as_ref())
                .map_err(|error| WasmExecutionError::Abi(error.to_string()))?;
            writer
                .write(input)
                .map_err(|error| WasmExecutionError::Abi(error.to_string()))?;
            writer
                .finish()
                .map_err(|error| WasmExecutionError::Abi(error.to_string()))?;
        }
        let input_len = i32::try_from(input_bytes.len()).map_err(|_| {
            WasmExecutionError::Abi("input IPC stream exceeds the Wasm ABI size limit".to_owned())
        })?;
        let mut store = self
            .new_store(fuel, None)
            .map_err(WasmExecutionError::Fuel)?;
        let instance =
            Instance::new(&mut store, &module, &[]).map_err(WasmExecutionError::Instantiate)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| WasmExecutionError::Abi("guest must export memory".to_owned()))?;
        let allocator = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc_ffi")
            .map_err(WasmExecutionError::Export)?;
        let input_pointer = allocator
            .call(&mut store, input_len)
            .map_err(WasmExecutionError::Trap)?;
        let mut descriptor = [0_u8; 8];
        descriptor[..4].copy_from_slice(&1_u32.to_le_bytes());
        descriptor[4..].copy_from_slice(&(input_bytes.len() as u32).to_le_bytes());
        let descriptor_pointer = allocator
            .call(&mut store, descriptor.len() as i32)
            .map_err(WasmExecutionError::Trap)?;
        let mut output_allocation = None;
        let started = std::time::Instant::now();
        let result = (|| {
            write_guest_memory(&memory, &mut store, input_pointer, &input_bytes)?;
            write_guest_memory(&memory, &mut store, descriptor_pointer, &descriptor)?;
            let run = instance
                .get_typed_func::<(i32, i32), i32>(&mut store, "kineplex_run")
                .map_err(WasmExecutionError::Export)?;
            let output_pointer = run
                .call(&mut store, (input_pointer, descriptor_pointer))
                .map_err(WasmExecutionError::Trap)?;
            let size_function = instance
                .get_typed_func::<(), i32>(&mut store, "kineplex_result_size")
                .map_err(WasmExecutionError::Export)?;
            let output_size = size_function
                .call(&mut store, ())
                .map_err(WasmExecutionError::Trap)?;
            if output_size < 0 || output_size as usize > MAX_WASM_MEMORY_BYTES {
                return Err(WasmExecutionError::Abi(
                    "guest returned an invalid Arrow IPC result size".to_owned(),
                ));
            }
            output_allocation = Some((output_pointer, output_size));
            let mut output_bytes = vec![0; output_size as usize];
            read_guest_memory(&memory, &store, output_pointer, &mut output_bytes)?;
            let mut reader = StreamReader::try_new(std::io::Cursor::new(output_bytes), None)
                .map_err(|error| WasmExecutionError::Abi(error.to_string()))?;
            let batch = reader
                .next()
                .transpose()
                .map_err(|error| WasmExecutionError::Abi(error.to_string()))?
                .ok_or_else(|| {
                    WasmExecutionError::Abi("guest returned no Arrow record batch".to_owned())
                })?;
            if reader.next().is_some() {
                return Err(WasmExecutionError::Abi(
                    "guest must return exactly one Arrow record batch".to_owned(),
                ));
            }
            Ok(batch)
        })();

        if let Ok(deallocate) = instance.get_typed_func::<(i32, i32), ()>(&mut store, "free_ffi") {
            if let Some((pointer, size)) = output_allocation {
                if pointer != input_pointer && pointer != descriptor_pointer {
                    let _ = deallocate.call(&mut store, (pointer, size));
                }
            }
            if Some(input_pointer) != output_allocation.map(|(pointer, _)| pointer) {
                let _ = deallocate.call(&mut store, (input_pointer, input_len));
            }
            if Some(descriptor_pointer) != output_allocation.map(|(pointer, _)| pointer) {
                let _ = deallocate.call(&mut store, (descriptor_pointer, descriptor.len() as i32));
            }
        }
        crate::metrics::global().record_wasm_execution(started.elapsed());
        result
    }

    fn new_store(
        &self,
        fuel: u64,
        arrow_batch: Option<Arc<RecordBatch>>,
    ) -> Result<Store<StoreState>, wasmtime::Error> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(MAX_WASM_MEMORY_BYTES)
            .build();
        let mut store = Store::new(
            self.engine,
            StoreState {
                limits,
                arrow_batch,
            },
        );
        store.limiter(|state| &mut state.limits);
        store.set_fuel(fuel)?;
        Ok(store)
    }
}

/// A fresh Wasmtime store and instance created from a cached compiled module.
pub struct WasmInstance {
    store: Store<StoreState>,
    instance: Instance,
}

fn checked_guest_range(
    memory: &wasmtime::Memory,
    store: &Store<StoreState>,
    pointer: i32,
    length: usize,
) -> Result<usize, WasmExecutionError> {
    let start = usize::try_from(pointer).map_err(|_| {
        WasmExecutionError::Abi("guest returned a negative memory offset".to_owned())
    })?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| WasmExecutionError::Abi("guest memory range overflowed".to_owned()))?;
    if end > memory.data_size(store) {
        return Err(WasmExecutionError::Abi(
            "guest memory range is out of bounds".to_owned(),
        ));
    }
    Ok(start)
}

fn write_guest_memory(
    memory: &wasmtime::Memory,
    store: &mut Store<StoreState>,
    pointer: i32,
    bytes: &[u8],
) -> Result<(), WasmExecutionError> {
    let start = checked_guest_range(memory, store, pointer, bytes.len())?;
    memory
        .write(store, start, bytes)
        .map_err(|error| WasmExecutionError::Abi(error.to_string()))
}

fn read_guest_memory(
    memory: &wasmtime::Memory,
    store: &Store<StoreState>,
    pointer: i32,
    bytes: &mut [u8],
) -> Result<(), WasmExecutionError> {
    let start = checked_guest_range(memory, store, pointer, bytes.len())?;
    memory
        .read(store, start, bytes)
        .map_err(|error| WasmExecutionError::Abi(error.to_string()))
}

impl WasmInstance {
    /// Invokes an exported zero-argument, zero-result function.
    pub fn call(&mut self, export: &str) -> Result<(), WasmExecutionError> {
        let function = self
            .instance
            .get_typed_func::<(), ()>(&mut self.store, export)
            .map_err(WasmExecutionError::Export)?;
        let started = std::time::Instant::now();
        let result = function
            .call(&mut self.store, ())
            .map_err(WasmExecutionError::Trap);
        crate::metrics::global().record_wasm_execution(started.elapsed());
        result
    }
}

/// Compilation, instantiation, export lookup, or guest trap failure.
#[derive(Debug)]
pub enum WasmExecutionError {
    Abi(String),
    Fuel(wasmtime::Error),
    Compile(WasmModuleError),
    Instantiate(wasmtime::Error),
    Export(wasmtime::Error),
    Trap(wasmtime::Error),
}

impl Display for WasmExecutionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Abi(error) => write!(formatter, "invalid Wasm Arrow ABI result: {error}"),
            Self::Fuel(error) => {
                write!(formatter, "failed to set WebAssembly fuel budget: {error}")
            }
            Self::Compile(error) => write!(formatter, "WebAssembly compilation failed: {error}"),
            Self::Instantiate(error) => {
                write!(formatter, "WebAssembly instantiation failed: {error}")
            }
            Self::Export(error) => write!(formatter, "WebAssembly export lookup failed: {error}"),
            Self::Trap(error) => write!(formatter, "WebAssembly execution trapped: {error}"),
        }
    }
}

impl Error for WasmExecutionError {}

#[cfg(test)]
mod tests {
    use super::{module_cache, WasmExecutionError, WasmRuntime};

    // (module (func (export "run") (loop $forever (br $forever))) )
    const INFINITE_LOOP: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x04, 0x01, 0x60, 0x00, 0x00, 0x03,
        0x02, 0x01, 0x00, 0x07, 0x07, 0x01, 0x03, 0x72, 0x75, 0x6e, 0x00, 0x00, 0x0a, 0x09, 0x01,
        0x07, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x0b,
    ];

    #[test]
    fn fuel_exhaustion_is_reported_as_a_trap() {
        let runtime = WasmRuntime::new().expect("Wasmtime engine initializes");
        let error = runtime
            .call(INFINITE_LOOP, "run", 1_000)
            .expect_err("loop exhausts fuel");
        assert!(matches!(error, WasmExecutionError::Trap(_)));
    }

    #[test]
    fn repeated_module_loads_compile_once_and_reuse_the_cached_module() {
        let runtime = WasmRuntime::new().expect("Wasmtime engine initializes");
        let initial_compilations = module_cache().compilation_count();
        for _ in 0..10 {
            let _instance = runtime
                .instantiate(INFINITE_LOOP, 1_000)
                .expect("valid module instantiates");
        }
        assert_eq!(module_cache().compilation_count() - initial_compilations, 1);
    }

    #[test]
    fn malformed_module_has_a_descriptive_error() {
        let runtime = WasmRuntime::new().expect("Wasmtime engine initializes");
        let error = runtime
            .instantiate(b"not wasm", 1_000)
            .err()
            .expect("invalid bytes are rejected");
        assert!(error.to_string().contains("invalid WebAssembly module"));
    }

    #[test]
    fn wasm_can_sum_arrow_values_through_read_only_host_calls() {
        use std::sync::Arc;

        use arrow::array::{Float32Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};

        let wasm = wat::parse_str(
            r#"(module
                (import "kineplex" "read_f32" (func $read_f32 (param i32 i32) (result f32)))
                (func (export "sum") (result f32)
                    f32.const 0
                    i32.const 0 i32.const 0 call $read_f32 f32.add
                    i32.const 0 i32.const 1 call $read_f32 f32.add))"#,
        )
        .expect("WAT parses");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        let batch = Arc::new(
            RecordBatch::try_new(schema, vec![Arc::new(Float32Array::from(vec![4.0, 7.0]))])
                .expect("batch is valid"),
        );
        let runtime = WasmRuntime::new().expect("Wasmtime engine initializes");
        let sum = runtime
            .call_with_arrow_f32(&wasm, "sum", batch, 100_000)
            .expect("guest call succeeds");
        assert_eq!(sum, 11.0);
    }

    #[test]
    fn kineplex_run_round_trips_arrow_ipc_through_guest_memory() {
        use std::sync::Arc;

        use arrow::array::{Float32Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};

        let wasm = wat::parse_str(
            r#"(module
                (memory (export "memory") 2)
                (global $heap (mut i32) (i32.const 8192))
                (global $result_size (mut i32) (i32.const 0))
                (func (export "alloc_ffi") (param $size i32) (result i32)
                    (local $ptr i32)
                    global.get $heap
                    local.tee $ptr
                    local.get $size
                    i32.add
                    global.set $heap
                    local.get $ptr)
                (func (export "kineplex_run") (param $input i32) (param $schema i32) (result i32)
                    local.get $schema
                    i32.load offset=4
                    global.set $result_size
                    local.get $input)
                (func (export "kineplex_result_size") (result i32)
                    global.get $result_size))"#,
        )
        .expect("WAT parses");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        let input = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float32Array::from_iter_values(
                (0..100).map(|value| value as f32),
            ))],
        )
        .expect("batch is valid");
        let output = WasmRuntime::new()
            .expect("Wasmtime engine initializes")
            .invoke_arrow(&wasm, &input, 1_000_000)
            .expect("guest returns a valid Arrow stream");
        assert_eq!(output.num_rows(), 100);
        assert_eq!(output.schema(), input.schema());
    }
}
