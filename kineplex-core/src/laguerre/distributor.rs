//! Distribuidor Ótimo de Laguerre-Kantorovich (MK-EMD Anti-Small-Files)
//!
//! Coordena o particionamento dinâmico e o roteamento de lotes Arrow RecordBatch
//! utilizando Transporte Ótimo Semi-Discreto acoplado a Tesselagens Centroidais
//! de Voronoi com Restrição de Capacidade (Diagramas de Potência de Laguerre).

use super::buf_writer::BufWriter;
use super::cell::SemiDiscreteLaguerreCell;
use super::config::KantorovichDistributionConfig;
use super::entropy::estimate_markov_zstd_mass;
use arrow_array::{Array, Float64Array, RecordBatch};
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::format::FileMetaData;
pub use parquet::format::FileMetaData as ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use std::sync::Arc;
use uuid::Uuid;

/// Motor de Distribuição Equi-Massiva de Monge-Kantorovich (Padrão Numerische Mathematik)
pub struct LaguerreOptimalDistributor {
    store: Arc<dyn ObjectStore>,
    s3_bucket: String,
    base_prefix: String,
    config: KantorovichDistributionConfig,
    cells: Vec<SemiDiscreteLaguerreCell>,
    // Estimador online de Welford para a escala característica \sigma_X^p da variedade
    running_mean_sig: [f64; 4],
    running_scale_norm_p: f64,
    total_batches_observed: u64,
    sealed_datafiles: Vec<(String, FileMetaData)>,
}

impl LaguerreOptimalDistributor {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        s3_bucket: String,
        base_prefix: String,
        initial_seeds: Vec<[f64; 4]>,
        config: KantorovichDistributionConfig,
    ) -> Self {
        let seeds = if initial_seeds.is_empty() {
            vec![[0.0, 0.0, 0.0, 0.0]]
        } else {
            initial_seeds
        };

        let cells = seeds
            .into_iter()
            .enumerate()
            .map(|(id, seed)| SemiDiscreteLaguerreCell::new(id, seed))
            .collect();

        Self {
            store,
            s3_bucket,
            base_prefix,
            config,
            cells,
            running_mean_sig: [0.0; 4],
            running_scale_norm_p: 1.0,
            total_batches_observed: 0,
            sealed_datafiles: Vec::new(),
        }
    }

    /// Retorna as células de Laguerre gerenciadas.
    pub fn cells(&self) -> &[SemiDiscreteLaguerreCell] {
        &self.cells
    }

    /// Retorna a lista de datafiles selados e metadados Parquet.
    pub fn sealed_datafiles(&self) -> &[(String, ParquetMetaData)] {
        &self.sealed_datafiles
    }

    /// TEOREMA 1: Estimador de Massa Comprimida ZSTD via Taxa de Entropia de Markov de 1ª Ordem
    /// H(X_t | X_{t-1}) = H(X_t, X_{t-1}) - H(X_{t-1}) combinada ao fator de colapso RLE.
    #[inline(always)]
    pub fn estimate_markov_zstd_mass(batch: &RecordBatch) -> u64 {
        estimate_markov_zstd_mass(batch)
    }

    /// Extrai automaticamente a assinatura no espaço de Banach [f64; 4] de um RecordBatch.
    /// Dimensões: (L1 normalizada, L2 normalizada, Lp normalizada, Linf normalizada).
    pub fn extract_banach_signature(&self, batch: &RecordBatch) -> [f64; 4] {
        let p = self.config.banach_p;
        for col in batch.columns() {
            if let Some(f64_col) = col.as_any().downcast_ref::<Float64Array>() {
                if f64_col.len() > 0 {
                    let mut l1 = 0.0;
                    let mut l2_sq = 0.0;
                    let mut lp_pow = 0.0;
                    let mut linf: f64 = 0.0;
                    let n = f64_col.len() as f64;

                    for i in 0..f64_col.len() {
                        let v = f64_col.value(i).abs();
                        l1 += v;
                        l2_sq += v * v;
                        lp_pow += v.powf(p);
                        if v > linf {
                            linf = v;
                        }
                    }

                    let norm_l1 = l1 / n;
                    let norm_l2 = (l2_sq / n).sqrt();
                    let norm_lp = (lp_pow / n).powf(1.0 / p);
                    return [norm_l1, norm_l2, norm_lp, linf];
                }
            }
        }
        [0.0, 0.0, 0.0, 0.0]
    }

    /// Atualiza o fator de adimensionalização \sigma_X^p via recorrência online de Welford
    #[inline(always)]
    pub fn update_manifold_scale(&mut self, sig: &[f64; 4]) {
        self.total_batches_observed += 1;
        let n = self.total_batches_observed as f64;
        let p = self.config.banach_p;

        let mut dist_p = 0.0_f64;
        for d in 0..4 {
            let delta = sig[d] - self.running_mean_sig[d];
            self.running_mean_sig[d] += delta / n;
            dist_p += delta.abs().powf(p);
        }

        if self.total_batches_observed == 1 {
            self.running_scale_norm_p = dist_p.max(1e-6);
        } else {
            self.running_scale_norm_p =
                (0.95 * self.running_scale_norm_p + 0.05 * dist_p).max(1e-6);
        }
    }

    /// Custo de Laguerre Adimensionalizado: c(x, c_k) - \psi_k = (||x - c_k||_p^p / \sigma_X^p) - \psi_k
    #[inline(always)]
    pub fn dimensionless_laguerre_cost(&self, sig: &[f64; 4], cell: &SemiDiscreteLaguerreCell) -> f64 {
        let p = self.config.banach_p;
        let raw_lp: f64 = sig
            .iter()
            .zip(cell.centroid.iter())
            .map(|(x, c)| (x - c).abs().powf(p))
            .sum();

        let normalized_cost = raw_lp / self.running_scale_norm_p;
        normalized_cost - cell.kantorovich_psi
    }

    /// TEOREMA 2: Passo de Quase-Newton Log-Sinkhorn com Projeção no Hiperplano de Soma Zero (\sum \psi_k = 0)
    pub fn update_sinkhorn_kantorovich_potentials(&mut self) {
        if self.cells.is_empty() {
            return;
        }
        let target = self.config.target_mass_bytes as f64;
        let eps = self.config.entropic_epsilon;
        let floor_mass = (self.config.min_critical_bytes / 8).max(1024 * 1024) as f64;

        let mut mean_psi = 0.0_f64;
        for cell in &mut self.cells {
            let effective_mass = (cell.current_mass_bytes as f64).max(floor_mass);
            // Atualização multiplicativa de escalonamento entrópico no espaço logarítmico
            cell.kantorovich_psi += eps * (target / effective_mass).ln();
            mean_psi += cell.kantorovich_psi;
        }

        // Projeta no subespaço ortogonal \sum_{k=1}^K \psi_k = 0 (unicidade do potencial dual!)
        mean_psi /= self.cells.len() as f64;
        for cell in &mut self.cells {
            cell.kantorovich_psi -= mean_psi;
        }
    }

    pub fn parquet_writer_props() -> WriterProperties {
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_max_row_group_size(1_048_576)
            .set_bloom_filter_enabled(true)
            .build()
    }

    /// Roteia o lote pela célula de Laguerre ótima e aplica promoção preguiçosa (Lazy S3 Promotion)
    pub async fn route_and_distribute(
        &mut self,
        batch: RecordBatch,
        lp_signature: [f64; 4],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.update_manifold_scale(&lp_signature);
        let mass = Self::estimate_markov_zstd_mass(&batch);

        // 1. Atribuição Ótima na Tesselação de Laguerre-Voronoi
        let mut winner_idx = 0;
        let mut best_cost = f64::INFINITY;
        for (idx, cell) in self.cells.iter().enumerate() {
            let cost = self.dimensionless_laguerre_cost(&lp_signature, cell);
            if cost < best_cost {
                best_cost = cost;
                winner_idx = idx;
            }
        }

        // 2. Atualização Baricêntrica de Banach e Expansão da Bounding Box
        {
            let cell = &mut self.cells[winner_idx];
            let m_old = cell.current_mass_bytes as f64;
            let m_new = mass as f64;
            let m_tot = (m_old + m_new).max(1.0);

            for d in 0..4 {
                cell.centroid[d] = (m_old * cell.centroid[d] + m_new * lp_signature[d]) / m_tot;
                cell.bbox_min[d] = cell.bbox_min[d].min(lp_signature[d]);
                cell.bbox_max[d] = cell.bbox_max[d].max(lp_signature[d]);
            }
            cell.current_mass_bytes += mass;
        }

        // 3. Lazy Promotion: Só inicia Multipart Upload no S3 ao cruzar M_min (64 MB)
        let cell = &mut self.cells[winner_idx];
        if let Some((_, writer)) = cell.s3_multipart_sink.as_mut() {
            writer.write(&batch).await?;
        } else {
            cell.staged_arena.push(batch);

            if cell.current_mass_bytes >= self.config.min_critical_bytes {
                let s3_key = format!(
                    "{}/laguerre_cell_{:02}/kinehaus-mk-{}.parquet",
                    self.base_prefix,
                    cell.cell_id,
                    Uuid::new_v4()
                );
                let buf_writer = BufWriter::with_capacity(
                    Arc::clone(&self.store),
                    Path::from(s3_key.as_str()),
                    16 * 1024 * 1024,
                )
                .with_max_concurrency(8);

                let schema = cell.staged_arena[0].schema();
                let mut writer = AsyncArrowWriter::try_new(
                    buf_writer,
                    schema,
                    1024 * 1024,
                    Some(Self::parquet_writer_props()),
                )?;

                for staged_batch in cell.staged_arena.drain(..) {
                    writer.write(&staged_batch).await?;
                }
                let uri = format!("s3://{}/{}", self.s3_bucket, s3_key);
                cell.s3_multipart_sink = Some((uri, writer));
            }
        }

        // 4. Selagem Estacionária ao atingir a Massa Alvo M^* (128 MB)
        if cell.current_mass_bytes >= self.config.target_mass_bytes {
            if let Some((uri, writer)) = cell.s3_multipart_sink.take() {
                let meta = writer.close().await?;
                self.sealed_datafiles.push((uri, meta));
                cell.current_mass_bytes = 0;
                cell.kantorovich_psi = 0.0;
                cell.bbox_min = cell.centroid;
                cell.bbox_max = cell.centroid;
            }
        }

        // 5. Atualiza os potenciais duais via passo Log-Sinkhorn centrado
        self.update_sinkhorn_kantorovich_potentials();
        Ok(())
    }

    /// TEOREMA 3: Coalescência Geodésica Terminal de Ward-Steiner (Zero-Residue Boundary)
    /// Cada célula sub-crítica (< M_min) é absorvida pela célula super-crítica que minimiza
    /// o incremento de variância de Ward-Steiner + dilatação de Bounding Box no Iceberg!
    pub async fn finalize_optimal_distribution(
        mut self,
    ) -> Result<Vec<(String, ParquetMetaData)>, Box<dyn std::error::Error + Send + Sync>> {
        let p = self.config.banach_p;
        let lambda_hull = self.config.hull_penalty_lambda;
        let max_cap = self.config.max_ceiling_bytes;

        // 1. Separa células sub-críticas (órfãs em RAM) das super-críticas (writers ativos no S3)
        let mut orphan_cells: Vec<(Vec<RecordBatch>, [f64; 4], [f64; 4], [f64; 4], u64)> = Vec::new();
        for cell in &mut self.cells {
            if cell.s3_multipart_sink.is_none() && !cell.staged_arena.is_empty() {
                let batches = std::mem::take(&mut cell.staged_arena);
                orphan_cells.push((
                    batches,
                    cell.centroid,
                    cell.bbox_min,
                    cell.bbox_max,
                    cell.current_mass_bytes,
                ));
                cell.current_mass_bytes = 0;
            }
        }

        // 2. Para cada célula órfã k, encontra o vizinho super-crítico j^*(k) ótimo segundo Ward-Steiner
        let mut unmerged_batches: Vec<RecordBatch> = Vec::new();

        for (batches, c_k, min_k, max_k, m_k) in orphan_cells {
            let mut best_j: Option<usize> = None;
            let mut min_ward_cost = f64::INFINITY;

            for (j, target_cell) in self.cells.iter().enumerate() {
                if target_cell.s3_multipart_sink.is_some()
                    && (target_cell.current_mass_bytes + m_k <= max_cap)
                {
                    let m_j = target_cell.current_mass_bytes as f64;
                    let m_k_f = m_k as f64;
                    // Massa reduzida de física de dois corpos: \mu_{kj} = (M_k * M_j) / (M_k + M_j)
                    let reduced_mass = (m_k_f * m_j) / (m_k_f + m_j).max(1.0);

                    let dist_p: f64 = c_k
                        .iter()
                        .zip(target_cell.centroid.iter())
                        .map(|(a, b)| (a - b).abs().powf(p))
                        .sum();

                    // Incremento de volume morto na Hyper-Bounding Box do Iceberg
                    let mut dead_bbox_dilation = 0.0_f64;
                    for d in 0..4 {
                        let union_span = max_k[d].max(target_cell.bbox_max[d])
                            - min_k[d].min(target_cell.bbox_min[d]);
                        let sum_spans = (max_k[d] - min_k[d])
                            + (target_cell.bbox_max[d] - target_cell.bbox_min[d]);
                        dead_bbox_dilation += (union_span - sum_spans).max(0.0);
                    }

                    let total_merge_cost =
                        (reduced_mass / self.running_scale_norm_p) * dist_p
                        + lambda_hull * dead_bbox_dilation;

                    if total_merge_cost < min_ward_cost {
                        min_ward_cost = total_merge_cost;
                        best_j = Some(j);
                    }
                }
            }

            if let Some(j_star) = best_j {
                let target = &mut self.cells[j_star];
                let (_, writer) = target.s3_multipart_sink.as_mut().unwrap();
                for b in &batches {
                    writer.write(b).await?;
                }
                target.current_mass_bytes += m_k;
            } else {
                unmerged_batches.extend(batches);
            }
        }

        // 3. Se restaram lotes órfãos (ex: volume total do job < M_min), consolida em 1 arquivo coeso
        if !unmerged_batches.is_empty() {
            let s3_key = format!(
                "{}/consolidated_ward/kinehaus-coalesced-{}.parquet",
                self.base_prefix,
                Uuid::new_v4()
            );
            let buf_writer = BufWriter::with_capacity(
                Arc::clone(&self.store),
                Path::from(s3_key.as_str()),
                16 * 1024 * 1024,
            )
            .with_max_concurrency(8);

            let schema = unmerged_batches[0].schema();
            let mut single_writer = AsyncArrowWriter::try_new(
                buf_writer,
                schema,
                1024 * 1024,
                Some(Self::parquet_writer_props()),
            )?;

            for b in &unmerged_batches {
                single_writer.write(b).await?;
            }
            let meta = single_writer.close().await?;
            let uri = format!("s3://{}/{}", self.s3_bucket, s3_key);
            self.sealed_datafiles.push((uri, meta));
        }

        // 4. Fecha todos os writers super-críticos ativos no S3
        for cell in &mut self.cells {
            if let Some((uri, writer)) = cell.s3_multipart_sink.take() {
                let meta = writer.close().await?;
                self.sealed_datafiles.push((uri, meta));
            }
        }

        Ok(self.sealed_datafiles)
    }
}
