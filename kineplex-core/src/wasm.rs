//! Sandboxed execution primitives for user supplied WebAssembly modules.
//!
//! The process shares one Wasmtime engine. Each invocation gets a fresh store,
//! its own fuel budget, and a linear-memory limit; WASI is deliberately absent.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use ring::digest::{digest, SHA256};
use wasmtime::{
    Config, Engine, Instance, Module, OptLevel, Store, StoreLimits, StoreLimitsBuilder,
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
        let mut store = self.new_store(fuel).map_err(WasmExecutionError::Fuel)?;
        let instance =
            Instance::new(&mut store, &module, &[]).map_err(WasmExecutionError::Instantiate)?;
        Ok(WasmInstance { store, instance })
    }

    fn new_store(&self, fuel: u64) -> Result<Store<StoreState>, wasmtime::Error> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(MAX_WASM_MEMORY_BYTES)
            .build();
        let mut store = Store::new(self.engine, StoreState { limits });
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

impl WasmInstance {
    /// Invokes an exported zero-argument, zero-result function.
    pub fn call(&mut self, export: &str) -> Result<(), WasmExecutionError> {
        let function = self
            .instance
            .get_typed_func::<(), ()>(&mut self.store, export)
            .map_err(WasmExecutionError::Export)?;
        function
            .call(&mut self.store, ())
            .map_err(WasmExecutionError::Trap)
    }
}

/// Compilation, instantiation, export lookup, or guest trap failure.
#[derive(Debug)]
pub enum WasmExecutionError {
    Fuel(wasmtime::Error),
    Compile(WasmModuleError),
    Instantiate(wasmtime::Error),
    Export(wasmtime::Error),
    Trap(wasmtime::Error),
}

impl Display for WasmExecutionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
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
}
