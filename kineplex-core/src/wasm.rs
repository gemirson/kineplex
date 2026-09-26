//! Sandboxed execution primitives for user supplied WebAssembly modules.
//!
//! The process shares one Wasmtime engine. Each invocation gets a fresh store,
//! its own fuel budget, and a linear-memory limit; WASI is deliberately absent.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::OnceLock;

use wasmtime::{
    Config, Engine, Instance, Module, OptLevel, Store, StoreLimits, StoreLimitsBuilder,
};

/// Maximum linear memory allowed for one WebAssembly instance (2 GiB).
pub const MAX_WASM_MEMORY_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// A process-wide, lazily initialized Wasmtime engine.
static WASM_ENGINE: OnceLock<Result<Engine, String>> = OnceLock::new();

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

/// State isolated to a single Wasmtime store.
struct StoreState {
    limits: StoreLimits,
}

/// Runs exported, zero-argument WebAssembly functions in a resource-limited store.
#[derive(Clone, Copy)]
pub struct WasmRuntime {
    engine: &'static Engine,
}

impl WasmRuntime {
    /// Gets a runtime backed by the process-wide engine.
    pub fn new() -> Result<Self, WasmEngineError> {
        Ok(Self {
            engine: wasm_engine()?,
        })
    }

    /// Compiles `wasm`, then calls its named `() -> ()` export with a fuel budget.
    ///
    /// Wasmtime traps, including fuel exhaustion, are returned to the caller.
    pub fn call(&self, wasm: &[u8], export: &str, fuel: u64) -> Result<(), WasmExecutionError> {
        let module = Module::new(self.engine, wasm).map_err(WasmExecutionError::Compile)?;
        let mut store = self.new_store(fuel).map_err(WasmExecutionError::Fuel)?;
        let instance =
            Instance::new(&mut store, &module, &[]).map_err(WasmExecutionError::Instantiate)?;
        let function = instance
            .get_typed_func::<(), ()>(&mut store, export)
            .map_err(WasmExecutionError::Export)?;
        function
            .call(&mut store, ())
            .map_err(WasmExecutionError::Trap)
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

/// Compilation, instantiation, export lookup, or guest trap failure.
#[derive(Debug)]
pub enum WasmExecutionError {
    Fuel(wasmtime::Error),
    Compile(wasmtime::Error),
    Instantiate(wasmtime::Error),
    Export(wasmtime::Error),
    Trap(wasmtime::Error),
}

impl Display for WasmExecutionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fuel(error) => write!(formatter, "failed to set WebAssembly fuel budget: {error}"),
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
    use super::{WasmExecutionError, WasmRuntime};

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
}
