# Data Model: Riemannian Cascades, Lakehouse Commit & SIMD Dispatcher (FT-100)

## 1. Entidades de Domínio e Arquitetura Lógica

```
+-----------------------------------------------------------------------------------------+
|                                    KinePlex Data Model                                  |
|                                                                                         |
|  [ Eixo 1: CBO Riemanniano ]           [ Eixo 2: Commit Lakehouse ]                     |
|  +--------------------------------+   +---------------------------------------------+  |
|  | PhysicalJoinOperator (Enum)    |   | UnifiedDataFileAction (Struct)              |  |
|  | - LaguerreCoLocatedMergeJoin   |   | - relative_path: String                     |  |
|  | - SimdPartitionedHashJoin      |   | - file_size_bytes: u64                      |  |
|  | - HyperbolicSkewBroadcastJoin  |   | - record_count: u64                         |  |
|  +--------------------------------+   | - min_values: Vec<f64>  (\vec{\ell}_k)       |  |
|                 │                     | - max_values: Vec<f64>  (\vec{u}_k)          |  |
|                 ▼                     | - null_counts: Vec<u64> (\vec{\nu}_k)        |  |
|  +--------------------------------+   | - modification_time_ms: i64                 |  |
|  | GraphEdgeStatistics (Struct)   |   +---------------------------------------------+  |
|  | - metric_distance_dg: f64      |                          │                          |
|  | - wasserstein_w1: f64          |            +-------------+-------------+            |
|  | - left_cardinality: u64        |            ▼                           ▼            |
|  | - right_cardinality: u64       |   +------------------+       +------------------+  |
|  | - is_laguerre_aligned: bool    |   |  \Psi_Iceberg    |       |   \Psi_Delta     |  |
|  +--------------------------------+   | (Manifest Avro)  |       | (_delta_log JSON)|  |
|                 │                     +------------------+       +------------------+  |
|                 ▼                                                                       |
|  +--------------------------------+                                                     |
|  | KinePlexCascadesOptimizer      |                                                     |
|  | - cpu_cost_weight: f64         |   [ Eixo 3: Dispatcher SIMD GF(2)[x] ]              |
|  | - io_cost_weight: f64          |   +---------------------------------------------+  |
|  | - net_enforcer_weight: f64     |   | StructuralMask64 (Struct)                   |  |
|  +--------------------------------+   | - valid_delimiters_mask: u64                |  |
|                                       | - valid_newlines_mask: u64                  |  |
|                                       | - next_prev_in_quote: u64                   |  |
|                                       +---------------------------------------------+  |
+-----------------------------------------------------------------------------------------+
```

---

## 2. Especificação das Estruturas em Rust

### 2.1 Eixo 1: Otimizador Lógico/Físico Cascades com Curvatura de Ollivier-Ricci
```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhysicalJoinOperator {
    /// \kappa(x, y) > 0.15: Vizinhanças superpostas em L^p -> Merge local nas células de Laguerre
    LaguerreCoLocatedMergeJoin,
    /// |\kappa(x, y)| <= 0.15: Espaço plano euclidiano -> Hash Join vetorizado em AVX-512
    SimdPartitionedHashJoin,
    /// \kappa(x, y) < -0.15: Gargalo hiperbólico (Heavy Skew) -> Separa hubs via Broadcast + Quotient Coalesce
    HyperbolicSkewBroadcastJoin,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphEdgeStatistics {
    pub metric_distance_dg: f64,     // Distância entre os baricentros d_G(x, y) no índice .khvp
    pub wasserstein_w1: f64,         // Distância de Transporte Ótimo W_1(m_x, m_y) entre partições
    pub left_cardinality: u64,
    pub right_cardinality: u64,
    pub is_laguerre_aligned: bool,   // Se ambas as tabelas já estão co-particionadas por MK-EMD
}

impl GraphEdgeStatistics {
    /// Calcula a Curvatura de Ollivier-Ricci: \kappa(x, y) = 1 - W_1(m_x, m_y) / d_G(x, y)
    #[inline(always)]
    pub fn ollivier_ricci_curvature(&self) -> f64 {
        let denom = self.metric_distance_dg.max(1e-9);
        1.0 - (self.wasserstein_w1 / denom)
    }
}

pub struct KinePlexCascadesOptimizer {
    pub cpu_cost_weight: f64,
    pub io_cost_weight: f64,
    pub net_enforcer_weight: f64,
}
```

### 2.2 Eixo 2: Monoide Unificado de Commit Lakehouse (Iceberg v2 & Delta Lake)
```rust
#[derive(Clone, Debug, PartialEq)]
pub struct UnifiedDataFileAction {
    pub relative_path: String,
    pub file_size_bytes: u64,
    pub record_count: u64,
    pub min_values: Vec<f64>, // \vec{\ell}_k para SSI Conflict Detection e Data Skipping
    pub max_values: Vec<f64>, // \vec{u}_k
    pub null_counts: Vec<u64>,
    pub modification_time_ms: i64,
}

impl UnifiedDataFileAction {
    /// Projeta a ação do monoide diretamente para o formato JSON do _delta_log (Delta Lake Protocol)
    pub fn to_delta_log_json_line(&self) -> String;

    /// Verifica a Propriedade de Diamante (Comutatividade SSI) entre dois hiper-retângulos em R^C
    #[inline(always)]
    pub fn intersects_hyper_rectangle(
        &self,
        query_min: &[f64],
        query_max: &[f64],
    ) -> bool;
}

/// Valida em O(K) se uma transação concorrente T_ext invalida o predicado de leitura de T_own
pub fn can_commute_concurrent_commit(
    concurrent_added_files: &[UnifiedDataFileAction],
    read_predicate_min: &[f64],
    read_predicate_max: &[f64],
) -> bool;
```

### 2.3 Eixo 3: Dispatch Dinâmico SIMD em $\mathbb{F}_2[x]$
```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StructuralMask64 {
    pub valid_delimiters_mask: u64, // Bits 1 exatamente nos ';' fora de aspas
    pub valid_newlines_mask: u64,   // Bits 1 exatamente nos '\n' fora de aspas
    pub next_prev_in_quote: u64,    // 0xFFFF_FFFF_FFFF_FFFF se terminou dentro de aspas, 0 caso contrário
}

pub fn prefix_xor_swar_64(quote_bits: u64) -> u64;

#[cfg(target_arch = "x86_64")]
pub unsafe fn prefix_xor_clmul_x86(quote_bits: u64) -> u64;

pub fn compute_quote_mask_dispatch(
    block_64: &[u8; 64],
    delim: u8,
    prev_in_quote: u64,
) -> StructuralMask64;
```
