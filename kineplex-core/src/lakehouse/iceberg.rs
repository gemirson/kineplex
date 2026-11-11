use crate::lakehouse::monoid::UnifiedDataFileAction;

/// Agregador de limites sobre o Reticulado Completo (Complete Lattice) para ManifestFiles do Apache Iceberg v2
pub struct IcebergManifestLattice;

impl IcebergManifestLattice {
    /// Computa o Ínfimo (mínimo global) e Supremo (máximo global) por coluna:
    /// L_manifest^{(c)} = \bigwedge_{k=1}^K \ell_k^{(c)},  U_manifest^{(c)} = \bigvee_{k=1}^K u_k^{(c)}
    pub fn compute_manifest_bounds(
        actions: &[UnifiedDataFileAction],
    ) -> (Vec<f64>, Vec<f64>, Vec<u64>) {
        if actions.is_empty() {
            return (Vec::new(), Vec::new(), Vec::new());
        }

        let num_cols = actions[0].min_values.len();
        let mut min_bounds = actions[0].min_values.clone();
        let mut max_bounds = actions[0].max_values.clone();
        let mut total_nulls = vec![0u64; num_cols];

        for action in actions {
            for c in 0..num_cols {
                if c < action.min_values.len() && action.min_values[c] < min_bounds[c] {
                    min_bounds[c] = action.min_values[c];
                }
                if c < action.max_values.len() && action.max_values[c] > max_bounds[c] {
                    max_bounds[c] = action.max_values[c];
                }
                if c < action.null_counts.len() {
                    total_nulls[c] += action.null_counts[c];
                }
            }
        }

        (min_bounds, max_bounds, total_nulls)
    }
}
