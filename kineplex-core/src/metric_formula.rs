//! Reloadable Wasmtime JIT formulas for Control Plane metric calculations.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use wasmtime::{Engine, Instance, Module, Store, StoreLimits, StoreLimitsBuilder};

use crate::wasm::wasm_engine;

const FORMULA_MEMORY_LIMIT: usize = 1024 * 1024;

struct FormulaStore {
    limits: StoreLimits,
}

/// A compiled metric formula. Wasm must export `christoffel(f64,f64,f64)->f64`.
pub struct MetricFormula {
    engine: &'static Engine,
    module: Module,
}

impl MetricFormula {
    /// JIT-compiles a formula module against the process-wide Wasmtime engine.
    pub fn compile(wasm: &[u8]) -> Result<Self, FormulaError> {
        let engine = wasm_engine().map_err(|error| FormulaError::Engine(error.to_string()))?;
        let module =
            Module::new(engine, wasm).map_err(|error| FormulaError::Compile(error.to_string()))?;
        Ok(Self { engine, module })
    }

    /// Evaluates one formula with an instruction budget and capped guest memory.
    pub fn evaluate(&self, x: f64, y: f64, z: f64, fuel: u64) -> Result<f64, FormulaError> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(FORMULA_MEMORY_LIMIT)
            .build();
        let mut store = Store::new(self.engine, FormulaStore { limits });
        store.limiter(|state| &mut state.limits);
        store
            .set_fuel(fuel)
            .map_err(|error| FormulaError::Fuel(error.to_string()))?;
        let instance = Instance::new(&mut store, &self.module, &[])
            .map_err(|error| FormulaError::Instantiate(error.to_string()))?;
        let function = instance
            .get_typed_func::<(f64, f64, f64), f64>(&mut store, "christoffel")
            .map_err(|error| FormulaError::Export(error.to_string()))?;
        let output = function
            .call(&mut store, (x, y, z))
            .map_err(|error| FormulaError::Trap(error.to_string()))?;
        if output.is_finite() {
            Ok(output)
        } else {
            Err(FormulaError::NonFiniteResult)
        }
    }
}

struct VersionedFormula {
    revision: u64,
    formula: MetricFormula,
}

/// Atomically replaceable formula slot; existing readers finish on their snapshot.
pub struct FormulaRegistry {
    current: ArcSwap<VersionedFormula>,
    next_revision: AtomicU64,
}

impl FormulaRegistry {
    /// Creates a registry from an initial formula module.
    pub fn new(initial_wasm: &[u8]) -> Result<Self, FormulaError> {
        Ok(Self {
            current: ArcSwap::from(Arc::new(VersionedFormula {
                revision: 1,
                formula: MetricFormula::compile(initial_wasm)?,
            })),
            next_revision: AtomicU64::new(2),
        })
    }

    /// Compiles the replacement before publishing it, without restarting the node.
    pub fn replace(&self, wasm: &[u8]) -> Result<u64, FormulaError> {
        let formula = MetricFormula::compile(wasm)?;
        let revision = self.next_revision.fetch_add(1, Ordering::Relaxed);
        self.current
            .store(Arc::new(VersionedFormula { revision, formula }));
        Ok(revision)
    }

    pub fn evaluate(&self, x: f64, y: f64, z: f64, fuel: u64) -> Result<(u64, f64), FormulaError> {
        let snapshot = self.current.load();
        Ok((snapshot.revision, snapshot.formula.evaluate(x, y, z, fuel)?))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FormulaError {
    Engine(String),
    Compile(String),
    Fuel(String),
    Instantiate(String),
    Export(String),
    Trap(String),
    NonFiniteResult,
}

impl std::fmt::Display for FormulaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Engine(error) => write!(f, "metric formula engine failed: {error}"),
            Self::Compile(error) => write!(f, "metric formula compilation failed: {error}"),
            Self::Fuel(error) => write!(f, "metric formula fuel setup failed: {error}"),
            Self::Instantiate(error) => write!(f, "metric formula instantiation failed: {error}"),
            Self::Export(error) => write!(f, "metric formula export failed: {error}"),
            Self::Trap(error) => write!(f, "metric formula trapped: {error}"),
            Self::NonFiniteResult => f.write_str("metric formula returned a non-finite value"),
        }
    }
}

impl std::error::Error for FormulaError {}

#[cfg(test)]
mod tests {
    use super::FormulaRegistry;

    #[test]
    fn formulas_can_be_replaced_without_restarting_the_registry() {
        let add = wat::parse_str(
            r#"(module (func (export "christoffel") (param f64 f64 f64) (result f64)
                local.get 0 local.get 1 f64.add local.get 2 f64.add))"#,
        )
        .expect("add formula parses");
        let multiply = wat::parse_str(
            r#"(module (func (export "christoffel") (param f64 f64 f64) (result f64)
                local.get 0 local.get 1 f64.mul local.get 2 f64.mul))"#,
        )
        .expect("multiply formula parses");
        let registry = FormulaRegistry::new(&add).expect("initial formula compiles");
        let (first_revision, first) = registry
            .evaluate(2.0, 3.0, 4.0, 10_000)
            .expect("first formula runs");
        let second_revision = registry.replace(&multiply).expect("replacement compiles");
        let (active_revision, second) = registry
            .evaluate(2.0, 3.0, 4.0, 10_000)
            .expect("replacement runs");
        assert_eq!((first_revision, first), (1, 9.0));
        assert_eq!(second_revision, 2);
        assert_eq!((active_revision, second), (2, 24.0));
    }
}
