//! # Smoluchowski Metric Partition Coalesce (Coagulação Métrica de Arquivos/Partições)
//!
//! Compacta $M$ partições físicas ou arquivos Parquet em $N$ arquivos maiores,
//! guiada pela Equação Cinética de Coagulação de Smoluchowski:
//!
//! $$\mathcal{K}(P_a, P_b) = \exp\!\Big( -\gamma \, \Vert{}\Phi(P_a) - \Phi(P_b)\Vert{}_{L^p} - \lambda \, \mathcal{D}_{\text{box}}(P_a, P_b) \Big) \cdot \mathbf{1}_{\{m_a + m_b \le M_{\max}\}}$$
//!
//! Onde $\mathcal{D}_{\text{box}}(P_a, P_b) = (\max(u_a, u_b) - \min(l_a, l_b)) - ((u_a - l_a) + (u_b - l_b))$
//! penaliza o surgimento de "espaço morto", mantendo os hiper-retângulos `lower_bounds` / `upper_bounds`
//! do Apache Iceberg estritamente compactos e preservando o Data Skipping.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Descritor métrico de uma partição ou arquivo Parquet candidato à compactação.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionMetricDescriptor {
    pub partition_id: usize,
    pub byte_size: u64,
    pub min_bound: f64,
    pub max_bound: f64,
    /// Assinatura funcional em $\mathbb{R}^4$: $(\|f\|_1, \|f\|_2, \|f\|_p, \|f\|_\infty)$
    pub lp_signature: [f64; 4],
    pub active: bool,
}

#[derive(Clone, Copy, Debug)]
struct SmoluchowskiPair {
    kernel_affinity: f64,
    left_idx: usize,
    right_idx: usize,
}

impl PartialEq for SmoluchowskiPair {
    fn eq(&self, other: &Self) -> bool {
        self.kernel_affinity.to_bits() == other.kernel_affinity.to_bits()
    }
}

impl Eq for SmoluchowskiPair {}

impl PartialOrd for SmoluchowskiPair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SmoluchowskiPair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kernel_affinity
            .partial_cmp(&other.kernel_affinity)
            .unwrap_or(Ordering::Equal)
    }
}

/// Agrupa $M$ partições em clusters ótimos usando o Kernel de Coagulação de Smoluchowski,
/// minimizando a dilatação dos limites e a distância funcional de Banach.
pub fn smoluchowski_partition_coalesce(
    mut descriptors: Vec<PartitionMetricDescriptor>,
    max_partition_bytes: u64,
    target_clusters: usize,
    gamma_lp: f64,
    lambda_box: f64,
) -> Vec<Vec<usize>> {
    let m = descriptors.len();
    if m == 0 {
        return Vec::new();
    }
    if target_clusters >= m {
        return descriptors.iter().map(|d| vec![d.partition_id]).collect();
    }

    let mut merged_groups: Vec<Vec<usize>> =
        descriptors.iter().map(|d| vec![d.partition_id]).collect();
    let mut active_clusters = m;

    let evaluate_kernel = |a: &PartitionMetricDescriptor, b: &PartitionMetricDescriptor| -> f64 {
        let hull_min = a.min_bound.min(b.min_bound);
        let hull_max = a.max_bound.max(b.max_bound);
        let vol_a = (a.max_bound - a.min_bound).max(1e-9);
        let vol_b = (b.max_bound - b.min_bound).max(1e-9);
        let dead_box_dilation = ((hull_max - hull_min) - (vol_a + vol_b)).max(0.0);

        let lp_dist = a
            .lp_signature
            .iter()
            .zip(b.lp_signature.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f64>()
            .sqrt();

        (-gamma_lp * lp_dist - lambda_box * dead_box_dilation).exp()
    };

    let mut heap = BinaryHeap::new();
    for i in 0..m {
        for j in (i + 1)..m {
            if descriptors[i].byte_size + descriptors[j].byte_size <= max_partition_bytes {
                heap.push(SmoluchowskiPair {
                    kernel_affinity: evaluate_kernel(&descriptors[i], &descriptors[j]),
                    left_idx: i,
                    right_idx: j,
                });
            }
        }
    }

    while active_clusters > target_clusters {
        let candidate = match heap.pop() {
            Some(c) => c,
            None => break,
        };

        let (a, b) = (candidate.left_idx, candidate.right_idx);
        if !descriptors[a].active || !descriptors[b].active {
            continue;
        }
        if descriptors[a].byte_size + descriptors[b].byte_size > max_partition_bytes {
            continue;
        }

        let mass_a = descriptors[a].byte_size as f64;
        let mass_b = descriptors[b].byte_size as f64;
        let total_mass = (mass_a + mass_b).max(1.0);

        descriptors[a].byte_size += descriptors[b].byte_size;
        descriptors[a].min_bound = descriptors[a].min_bound.min(descriptors[b].min_bound);
        descriptors[a].max_bound = descriptors[a].max_bound.max(descriptors[b].max_bound);
        for k in 0..4 {
            descriptors[a].lp_signature[k] = (mass_a * descriptors[a].lp_signature[k]
                + mass_b * descriptors[b].lp_signature[k])
                / total_mass;
        }
        descriptors[b].active = false;

        let mut absorbed = std::mem::take(&mut merged_groups[b]);
        merged_groups[a].append(&mut absorbed);
        active_clusters -= 1;

        // Reinsere afinidades atualizadas do novo cluster coagulado `a`
        for other in 0..m {
            if other != a && descriptors[other].active {
                if descriptors[a].byte_size + descriptors[other].byte_size <= max_partition_bytes {
                    heap.push(SmoluchowskiPair {
                        kernel_affinity: evaluate_kernel(&descriptors[a], &descriptors[other]),
                        left_idx: a,
                        right_idx: other,
                    });
                }
            }
        }
    }

    merged_groups.into_iter().filter(|g| !g.is_empty()).collect()
}
