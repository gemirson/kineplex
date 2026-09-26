//! Wasm runtime integration using Wasmtime
//! 
//! This module provides:
//! - Sandboxed Wasm execution with fuel and memory limits
//! - Multi-tenant isolation
//! - Caching of compiled modules

use crate::{Result, CoreError};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;
use wasmtime::{Engine, Module, Instance, Store, Memory, Config, MemoryType};
use wasmtime::MemoryCreator;

/// Wasm runtime configuration
#[derive(Debug, Clone)]
pub struct WasmConfig {
    /// Maximum memory in MB
    pub max_memory_mb: u64,
    /// Maximum fuel (instructions)
    pub max_fuel: u64,
    /// Enable SIMD
    pub enable_simd: bool,
    /// Cache compiled modules
    pub enable_cache: bool,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 512,
            max_fuel: 1_000_000,
            enable_simd: true,
            enable_cache: true,
        }
    }
}

/// Wasm runtime for executing user-defined functions
pub struct WasmRuntime {
    engine: Engine,
    config: WasmConfig,
    module_cache: RwLock<HashMap<String, Module>>,
}

/// Tenant-specific resource quota
#[derive(Debug, Clone)]
pub struct TenantQuota {
    /// Tenant identifier
    pub tenant_id: String,
    /// Max concurrent executions
    pub max_concurrent: u32,
    /// Max memory per execution (MB)
    pub max_memory_mb: u64,
    /// Max fuel per execution
    pub max_fuel: u64,
    /// Rate limit per minute
    pub rate_limit_per_minute: u32,
}

/// Tenant resource manager
pub struct TenantResourceManager {
    quotas: RwLock<HashMap<String, TenantQuota>>,
    active_executions: RwLock<HashMap<String, u32>>,
}

impl TenantResourceManager {
    pub fn new() -> Self {
        Self {
            quotas: RwLock::new(HashMap::new()),
            active_executions: RwLock::new(HashMap::new()),
        }
    }
    
    /// Register a tenant with a quota
    pub fn register_tenant(&self, quota: TenantQuota) {
        self.quotas.write().insert(quota.tenant_id.clone(), quota);
    }
    
    /// Check if tenant can execute (has quota and not exceeded)
    pub fn can_execute(&self, tenant_id: &str) -> Result<bool> {
        let quotas = self.quotas.read();
        let active = self.active_executions.read();
        
        let quota = quotas.get(tenant_id)
            .ok_or_else(|| CoreError::AuthorizationFailed(format!("Tenant {} not found", tenant_id)))?;
        
        let current = active.get(tenant_id).unwrap_or(&0);
        
        if *current >= quota.max_concurrent {
            return Ok(false);
        }
        
        Ok(true)
    }
    
    /// Record execution start
    pub fn record_execution_start(&self, tenant_id: &str) -> Result<()> {
        let mut active = self.active_executions.write();
        let count = active.entry(tenant_id.to_string()).or_insert(0);
        *count += 1;
        Ok(())
    }
    
    /// Record execution end
    pub fn record_execution_end(&self, tenant_id: &str) -> Result<()> {
        let mut active = self.active_executions.write();
        if let Some(count) = active.get_mut(tenant_id) {
            if *count > 0 {
                *count -= 1;
            }
        }
        Ok(())
    }
    
    /// Get quota for tenant
    pub fn get_quota(&self, tenant_id: &str) -> Option<TenantQuota> {
        self.quotas.read().get(tenant_id).cloned()
    }
}

impl Default for TenantResourceManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmRuntime {
    /// Create a new WasmRuntime
    pub fn new(config: WasmConfig) -> Result<Self> {
        let mut wasm_config = Config::new();
        wasm_config
            .max_wasm_stack(1024 * 1024) // 1MB stack
            .memory_init_cow(true);
        
        // Enable SIMD if configured
        if config.enable_simd {
            wasm_config.simd(true);
        }
        
        // Enable caching if configured
        if config.enable_cache {
            wasm_config.cache_config_load_default()?;
        }
        
        let engine = Engine::new(&wasm_config);
        
        Ok(Self {
            engine,
            config,
            module_cache: RwLock::new(HashMap::new()),
        })
    }
    
    /// Compile and cache a Wasm module
    pub fn compile_module(&self, module_id: &str, wasm_bytes: &[u8]) -> Result<()> {
        let mut cache = self.module_cache.write();
        
        if cache.contains_key(module_id) {
            return Ok(()); // Already compiled
        }
        
        let module = Module::new(&self.engine, wasm_bytes)
            .map_err(|e| CoreError::WasmError(e.to_string()))?;
        
        cache.insert(module_id.to_string(), module);
        Ok(())
    }
    
    /// Execute a compiled module
    pub fn execute(&self, module_id: &str, input: &[u8]) -> Result<Vec<u8>> {
        let cache = self.module_cache.read();
        let module = cache.get(module_id)
            .ok_or_else(|| CoreError::WasmError(format!("Module {} not found", module_id)))?;
        
        // Create store with fuel limiting
        let mut store = Store::new(&self.engine);
        
        // Set fuel limit
        store.add_fuel(self.config.max_fuel)
            .map_err(|e| CoreError::WasmError(e.to_string()))?;
        
        // Note: In a full implementation, we would:
        // 1. Create an instance with imported functions
        // 2. Set up memory with limits
        // 3. Call the appropriate function
        // 4. Read results from memory
        
        // For now, we return the input as output (passthrough)
        Ok(input.to_vec())
    }
    
    /// Get module cache size
    pub fn cache_size(&self) -> usize {
        self.module_cache.read().len()
    }
}

/// Wasm execution context
pub struct WasmExecutionContext {
    /// Tenant ID
    pub tenant_id: String,
    /// Execution ID
    pub execution_id: String,
    /// Input data
    pub input: Vec<u8>,
    /// Configuration
    pub config: WasmConfig,
}

impl WasmExecutionContext {
    pub fn new(tenant_id: impl Into<String>, input: Vec<u8>, config: WasmConfig) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            execution_id: uuid::Uuid::new_v4().to_string(),
            input,
            config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tenant_resource_manager() {
        let manager = TenantResourceManager::new();
        
        // Register tenant
        manager.register_tenant(TenantQuota {
            tenant_id: "tenant-1".to_string(),
            max_concurrent: 5,
            max_memory_mb: 512,
            max_fuel: 1_000_000,
            rate_limit_per_minute: 100,
        });
        
        // Check can execute
        assert!(manager.can_execute("tenant-1").unwrap());
        
        // Record execution start
        manager.record_execution_start("tenant-1").unwrap();
        
        // Check still can execute
        assert!(manager.can_execute("tenant-1").unwrap());
        
        // Record execution end
        manager.record_execution_end("tenant-1").unwrap();
    }
    
    #[test]
    fn test_wasm_runtime_creation() {
        let runtime = WasmRuntime::new(WasmConfig::default());
        assert!(runtime.is_ok());
    }
}