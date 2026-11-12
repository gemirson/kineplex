# Contract: Cascades CBO, Unified Lakehouse & SIMD Dispatcher API

## 1. Contratos de Interface Pública

### 1.1 Contrato do Otimizador Cascades Riemanniano
```rust
use crate::cbo::{GraphEdgeStatistics, PhysicalJoinOperator};

pub trait CascadesCostOptimizer: Send + Sync {
    /// Determina o operador físico ótimo e seu custo geodésico correspondente
    fn select_optimal_join_plan(
        &self,
        stats: &GraphEdgeStatistics,
    ) -> (PhysicalJoinOperator, f64);
}
```

### 1.2 Contrato do Commit Transacional Unificado Lakehouse
```rust
use crate::lakehouse::UnifiedDataFileAction;

pub trait LakehouseCommitEngine: Send + Sync {
    /// Serializa as ações atômicas para formato Delta Lake _delta_log JSON
    fn serialize_delta_log(&self, actions: &[UnifiedDataFileAction]) -> Vec<String>;

    /// Valida comutatividade SSI contra ações comitadas concorrentemente
    fn validate_ssi_commutativity(
        &self,
        concurrent_actions: &[UnifiedDataFileAction],
        read_bounds_min: &[f64],
        read_bounds_max: &[f64],
    ) -> bool;
}
```

### 1.3 Contrato do Dispatcher SIMD em $\mathbb{F}_2[x]$
```rust
use crate::simd::StructuralMask64;

pub trait SimdStructuralParser: Send + Sync {
    /// Extrai máscaras estruturais de 64 bytes com quote-masking em GF(2)[x]
    fn parse_block_64(&self, block: &[u8; 64], delim: u8, prev_in_quote: u64) -> StructuralMask64;
}
```

---

## 2. Invariantes dos Contratos
1. **Determinismo Curvatura**: A Curvatura de Ollivier-Ricci é analítica e contínua em $(- \infty, 1.0]$, mapeando deterministicamente para o mesmo operador sob as mesmas estatísticas de partição.
2. **Serializabilidade SSI Sem Perda**: A condição $\mathcal{R}(T_{\text{own}}) \cap \mathcal{W}(T_{\text{ext}}) = \emptyset$ garante equivalência estrita de serialização ($T_{\text{own}} \circ T_{\text{ext}} \equiv T_{\text{ext}} \circ T_{\text{own}}$).
3. **Equivalência Semântica SIMD**: Todos os Tiers de microarquitetura (AVX-512, AVX2, NEON e SWAR) DEVEM produzir saídas de bits rigorosamente idênticas para qualquer bloco de 64 bytes.
