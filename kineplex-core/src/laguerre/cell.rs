//! Célula de Laguerre-Voronoi com rastreamento de Hyper-Rectangle Bounding Box

use super::buf_writer::BufWriter;
use arrow_array::RecordBatch;
use parquet::arrow::AsyncArrowWriter;

/// Célula de Laguerre-Voronoi com rastreamento de Caixa Envolvente (Hyper-Rectangle Bounding Box)
pub struct SemiDiscreteLaguerreCell {
    pub cell_id: usize,
    pub centroid: [f64; 4],            // Baricentro c_k \in R^4
    pub bbox_min: [f64; 4],            // Limite inferior para cálculo de volume morto no Iceberg
    pub bbox_max: [f64; 4],            // Limite superior para cálculo de volume morto no Iceberg
    pub kantorovich_psi: f64,          // Potencial dual adimensionalizado \psi_k (\sum \psi_k = 0)
    pub current_mass_bytes: u64,       // Massa comprimida acumulada M_k
    pub staged_arena: Vec<RecordBatch>,// Retenção em RAM enquanto M_k < M_min
    pub s3_multipart_sink: Option<(String, AsyncArrowWriter<BufWriter>)>,
}

impl SemiDiscreteLaguerreCell {
    /// Cria uma nova célula semi-discreta inicializada a partir de uma semente baricêntrica.
    pub fn new(cell_id: usize, seed: [f64; 4]) -> Self {
        Self {
            cell_id,
            centroid: seed,
            bbox_min: seed,
            bbox_max: seed,
            kantorovich_psi: 0.0,
            current_mass_bytes: 0,
            staged_arena: Vec::with_capacity(32),
            s3_multipart_sink: None,
        }
    }

    /// Retorna verdadeiro se a célula possui um writer físico aberto.
    #[inline(always)]
    pub fn is_promoted(&self) -> bool {
        self.s3_multipart_sink.is_some()
    }

    /// Retorna a quantidade de lotes atualmente retidos na arena.
    #[inline(always)]
    pub fn staged_count(&self) -> usize {
        self.staged_arena.len()
    }
}

/// Alias para manter compatibilidade com implementações existentes
pub type LaguerreCell = SemiDiscreteLaguerreCell;
