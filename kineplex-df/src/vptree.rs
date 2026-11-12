//! # Topologia Métrica Zero-Copy: Árvore de Ponto de Vantagem (`vptree`)
//!
//! Estrutura compacta serializada em cachelines de 32 bytes (`FlatVpNode`),
//! permitindo poda geométrica de partições e busca métrica $k$-NN com base
//! na desigualdade triangular em Espaços de Banach $(L^p)$.

use crate::memory::AlignedBuffer;

/// Estatísticas de busca e poda métrica para avaliação de desempenho pós-Banach.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VpSearchStats {
    pub dist_evaluations: usize,
    pub nodes_visited: usize,
    pub nodes_pruned: usize,
    pub pruning_ratio: f64,
}

/// Métrica nos Espaços de Banach $L^p$.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BanachMetric {
    L1,
    L2,
    LInf,
    Lp(f64),
}

impl BanachMetric {
    #[inline(always)]
    pub fn distance(&self, a: &[f64], b: &[f64]) -> f64 {
        let n = a.len().min(b.len());
        if n == 0 {
            return 0.0;
        }

        match self {
            BanachMetric::L1 => {
                let mut sum = 0.0;
                for i in 0..n {
                    sum += (a[i] - b[i]).abs();
                }
                sum
            }
            BanachMetric::L2 => {
                // ILP x4 para cálculo de distância Euclidiana
                let chunks = n / 4;
                let mut acc0 = 0.0;
                let mut acc1 = 0.0;
                let mut acc2 = 0.0;
                let mut acc3 = 0.0;

                for c in 0..chunks {
                    let base = c * 4;
                    let d0 = a[base] - b[base];
                    let d1 = a[base + 1] - b[base + 1];
                    let d2 = a[base + 2] - b[base + 2];
                    let d3 = a[base + 3] - b[base + 3];
                    acc0 += d0 * d0;
                    acc1 += d1 * d1;
                    acc2 += d2 * d2;
                    acc3 += d3 * d3;
                }
                let mut sum_sq = (acc0 + acc1) + (acc2 + acc3);
                for i in (chunks * 4)..n {
                    let d = a[i] - b[i];
                    sum_sq += d * d;
                }
                sum_sq.sqrt()
            }
            BanachMetric::LInf => {
                let mut max_d = 0.0;
                for i in 0..n {
                    let d = (a[i] - b[i]).abs();
                    if d > max_d {
                        max_d = d;
                    }
                }
                max_d
            }
            BanachMetric::Lp(p) => {
                let p_val = *p;
                let mut sum = 0.0;
                for i in 0..n {
                    sum += (a[i] - b[i]).abs().powf(p_val);
                }
                sum.powf(1.0 / p_val)
            }
        }
    }
}

/// Nó compacto de 32 bytes (`FlatVpNode`).
/// Exatamente 2 nós cabem em 1 cacheline de 64 bytes.
#[repr(C, align(32))]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FlatVpNode {
    pub point_idx: u32,     // 4 bytes: índice do ponto pivô na tabela de coordenadas
    pub left_child: u32,    // 4 bytes: índice do filho esquerdo (dentro do raio), u32::MAX se nulo
    pub right_child: u32,   // 4 bytes: índice do filho direito (fora do raio), u32::MAX se nulo
    pub _reserved0: u32,    // 4 bytes de alinhamento
    pub threshold: f64,     // 8 bytes: raio mediano divisório
    pub max_radius: f64,    // 8 bytes: distância máxima de qualquer descendente ao pivô
}

pub const NULL_NODE: u32 = u32::MAX;

/// Cabeçalho binário de 64 bytes (`VpIndexHeader`) compatível com validação de checksum rápida.
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
pub struct VpIndexHeader {
    pub magic: u64,         // 0x5450_5653_494E_4749 ("IGNISVPT")
    pub version: u32,       // 1
    pub num_points: u32,    // quantidade total de pontos
    pub point_dim: u32,     // dimensão do vetor funcional (ex: 4 para assinaturas)
    pub num_nodes: u32,     // quantidade de nós da árvore
    pub metric_code: u32,   // 1=L1, 2=L2, 3=LInf, 4=Lp
    pub metric_param: f64,  // p caso metric_code == 4
    pub checksum_64: u64,   // hash de integridade dos nós e pontos
    pub root_idx: u32,      // índice do nó raiz
    pub _padding: [u8; 16],
}

pub const VP_MAGIC: u64 = 0x5450_5653_494E_4749;

/// Índice Topológico de Vantage Point com busca por Desigualdade Triangular.
pub struct TopologicalVpIndex {
    pub metric: BanachMetric,
    pub point_dim: usize,
    pub coords: AlignedBuffer<f64>,
    pub nodes: Vec<FlatVpNode>,
    pub root_idx: u32,
}

impl TopologicalVpIndex {
    /// Constrói o índice a partir de um vetor contíguo de coordenadas de dimensão fixa.
    pub fn build(coords_flat: &[f64], point_dim: usize, metric: BanachMetric) -> Self {
        assert!(point_dim > 0, "Dimensão deve ser positiva");
        assert_eq!(
            coords_flat.len() % point_dim,
            0,
            "Tamanho das coordenadas deve ser múltiplo de point_dim"
        );
        let num_points = coords_flat.len() / point_dim;

        let coords = AlignedBuffer::<f64>::from_slice(coords_flat);

        if num_points == 0 {
            return Self {
                metric,
                point_dim,
                coords,
                nodes: Vec::new(),
                root_idx: NULL_NODE,
            };
        }

        let mut indices: Vec<usize> = (0..num_points).collect();
        let mut nodes = Vec::with_capacity(num_points);

        let root_idx = Self::build_recursive(
            coords.as_slice(),
            point_dim,
            metric,
            &mut indices,
            &mut nodes,
        );

        Self {
            metric,
            point_dim,
            coords,
            nodes,
            root_idx,
        }
    }

    fn point_slice(coords: &[f64], dim: usize, idx: usize) -> &[f64] {
        &coords[idx * dim..(idx + 1) * dim]
    }

    fn build_recursive(
        coords: &[f64],
        dim: usize,
        metric: BanachMetric,
        indices: &mut [usize],
        nodes: &mut Vec<FlatVpNode>,
    ) -> u32 {
        if indices.is_empty() {
            return NULL_NODE;
        }

        let node_idx = nodes.len() as u32;
        // Ponto pivô é o primeiro do subvetor
        let pivot_pt_idx = indices[0];

        if indices.len() == 1 {
            nodes.push(FlatVpNode {
                point_idx: pivot_pt_idx as u32,
                left_child: NULL_NODE,
                right_child: NULL_NODE,
                _reserved0: 0,
                threshold: 0.0,
                max_radius: 0.0,
            });
            return node_idx;
        }

        // Reserva o slot do nó
        nodes.push(FlatVpNode {
            point_idx: pivot_pt_idx as u32,
            left_child: NULL_NODE,
            right_child: NULL_NODE,
            _reserved0: 0,
            threshold: 0.0,
            max_radius: 0.0,
        });

        let pivot_slice = Self::point_slice(coords, dim, pivot_pt_idx);
        let rem_indices = &mut indices[1..];

        // Calcula distâncias de todos os pontos ao pivô
        let mut dists: Vec<(usize, f64)> = rem_indices
            .iter()
            .map(|&idx| {
                let pt = Self::point_slice(coords, dim, idx);
                (idx, metric.distance(pivot_slice, pt))
            })
            .collect();

        // Encontra o raio mediano
        let mid = dists.len() / 2;
        dists.select_nth_unstable_by(mid, |a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let threshold = dists[mid].1;

        let max_radius = dists.iter().map(|d| d.1).fold(0.0f64, f64::max);

        // Particiona em interior (<= threshold) e exterior (> threshold)
        let mut left_pts: Vec<usize> = Vec::with_capacity(mid);
        let mut right_pts: Vec<usize> = Vec::with_capacity(dists.len() - mid);

        for (i, &(idx, _dist)) in dists.iter().enumerate() {
            if i < mid {
                left_pts.push(idx);
            } else {
                right_pts.push(idx);
            }
        }

        let left_child = Self::build_recursive(coords, dim, metric, &mut left_pts, nodes);
        let right_child = Self::build_recursive(coords, dim, metric, &mut right_pts, nodes);

        nodes[node_idx as usize].left_child = left_child;
        nodes[node_idx as usize].right_child = right_child;
        nodes[node_idx as usize].threshold = threshold;
        nodes[node_idx as usize].max_radius = max_radius;

        node_idx
    }

    /// Busca por Bola Fechada: retorna todos os índices de pontos com $d(q, x) \le \varepsilon$.
    /// Aplica a desigualdade triangular:
    /// - Se $d(q, v) + \varepsilon \le \tau$, poda completamente o ramo externo.
    /// - Se $d(q, v) - \varepsilon > \tau$, poda completamente o ramo interno.
    pub fn search_closed_ball(&self, query: &[f64], epsilon: f64) -> Vec<usize> {
        let mut results = Vec::new();
        if self.root_idx == NULL_NODE {
            return results;
        }

        let mut stack = Vec::with_capacity(64);
        stack.push(self.root_idx);

        let coords_slice = self.coords.as_slice();

        while let Some(curr_idx) = stack.pop() {
            if curr_idx == NULL_NODE {
                continue;
            }
            let node = &self.nodes[curr_idx as usize];
            let pivot = Self::point_slice(coords_slice, self.point_dim, node.point_idx as usize);
            let dist = self.metric.distance(query, pivot);

            if dist <= epsilon {
                results.push(node.point_idx as usize);
            }

            // Poda pela Desigualdade Triangular
            if node.left_child != NULL_NODE && dist - epsilon <= node.threshold {
                stack.push(node.left_child);
            }
            if node.right_child != NULL_NODE && dist + epsilon > node.threshold {
                stack.push(node.right_child);
            }
        }

        results
    }

    /// Busca $k$-NN com encolhimento dinâmico do raio de poda $\tau$.
    pub fn search_knn(&self, query: &[f64], k: usize) -> Vec<(usize, f64)> {
        if k == 0 || self.root_idx == NULL_NODE {
            return Vec::new();
        }

        let mut nearest: Vec<(usize, f64)> = Vec::with_capacity(k);
        let mut tau = f64::INFINITY;

        let mut stack = Vec::with_capacity(64);
        stack.push(self.root_idx);

        let coords_slice = self.coords.as_slice();

        while let Some(curr_idx) = stack.pop() {
            if curr_idx == NULL_NODE {
                continue;
            }
            let node = &self.nodes[curr_idx as usize];
            let pivot = Self::point_slice(coords_slice, self.point_dim, node.point_idx as usize);
            let dist = self.metric.distance(query, pivot);

            if dist < tau {
                if nearest.len() < k {
                    nearest.push((node.point_idx as usize, dist));
                    nearest.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                    if nearest.len() == k {
                        tau = nearest.last().unwrap().1;
                    }
                } else {
                    nearest.pop();
                    nearest.push((node.point_idx as usize, dist));
                    nearest.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                    tau = nearest.last().unwrap().1;
                }
            }

            let explore_left = node.left_child != NULL_NODE && dist - tau <= node.threshold;
            let explore_right = node.right_child != NULL_NODE && dist + tau > node.threshold;

            // Prioriza ramo mais promissor
            if dist <= node.threshold {
                if explore_right { stack.push(node.right_child); }
                if explore_left { stack.push(node.left_child); }
            } else {
                if explore_left { stack.push(node.left_child); }
                if explore_right { stack.push(node.right_child); }
            }
        }

        nearest
    }

    /// Busca $k$-NN coletando telemetria detalhada de poda métrica para avaliação de desempenho.
    pub fn search_knn_with_stats(&self, query: &[f64], k: usize) -> (Vec<(usize, f64)>, VpSearchStats) {
        if k == 0 || self.root_idx == NULL_NODE {
            return (
                Vec::new(),
                VpSearchStats {
                    dist_evaluations: 0,
                    nodes_visited: 0,
                    nodes_pruned: 0,
                    pruning_ratio: 0.0,
                },
            );
        }

        let total_nodes = self.nodes.len();
        let mut nearest: Vec<(usize, f64)> = Vec::with_capacity(k);
        let mut tau = f64::INFINITY;

        let mut stack = Vec::with_capacity(64);
        stack.push(self.root_idx);

        let coords_slice = self.coords.as_slice();
        let mut dist_evaluations = 0;
        let mut nodes_visited = 0;
        let mut nodes_pruned = 0;

        while let Some(curr_idx) = stack.pop() {
            if curr_idx == NULL_NODE {
                continue;
            }
            nodes_visited += 1;
            let node = &self.nodes[curr_idx as usize];
            let pivot = Self::point_slice(coords_slice, self.point_dim, node.point_idx as usize);
            let dist = self.metric.distance(query, pivot);
            dist_evaluations += 1;

            if dist < tau {
                if nearest.len() < k {
                    nearest.push((node.point_idx as usize, dist));
                    nearest.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                    if nearest.len() == k {
                        tau = nearest.last().unwrap().1;
                    }
                } else {
                    nearest.pop();
                    nearest.push((node.point_idx as usize, dist));
                    nearest.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                    tau = nearest.last().unwrap().1;
                }
            }

            let explore_left = node.left_child != NULL_NODE && dist - tau <= node.threshold;
            let explore_right = node.right_child != NULL_NODE && dist + tau > node.threshold;

            if node.left_child != NULL_NODE && !explore_left {
                nodes_pruned += 1;
            }
            if node.right_child != NULL_NODE && !explore_right {
                nodes_pruned += 1;
            }

            // Prioriza ramo mais promissor
            if dist <= node.threshold {
                if explore_right { stack.push(node.right_child); }
                if explore_left { stack.push(node.left_child); }
            } else {
                if explore_left { stack.push(node.left_child); }
                if explore_right { stack.push(node.right_child); }
            }
        }

        let pruning_ratio = if total_nodes > 0 {
            1.0 - (dist_evaluations as f64 / total_nodes as f64)
        } else {
            0.0
        };

        (
            nearest,
            VpSearchStats {
                dist_evaluations,
                nodes_visited,
                nodes_pruned,
                pruning_ratio,
            },
        )
    }

    /// Busca $k$-NN exaustiva (brute-force linear) para validação de aceleração e fidelidade métrica.
    pub fn search_knn_brute_force(&self, query: &[f64], k: usize) -> (Vec<(usize, f64)>, usize) {
        if k == 0 || self.nodes.is_empty() {
            return (Vec::new(), 0);
        }
        let n = self.nodes.len();
        let coords_slice = self.coords.as_slice();
        let mut nearest: Vec<(usize, f64)> = Vec::with_capacity(k);
        let mut dist_evals = 0;

        for i in 0..n {
            let pt = Self::point_slice(coords_slice, self.point_dim, i);
            let dist = self.metric.distance(query, pt);
            dist_evals += 1;

            if nearest.len() < k {
                nearest.push((i, dist));
                nearest.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            } else if dist < nearest.last().unwrap().1 {
                nearest.pop();
                nearest.push((i, dist));
                nearest.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            }
        }
        (nearest, dist_evals)
    }

    /// Calcula checksum XXH3 simplificado de 64 bits para validação instantânea de integridade.
    pub fn compute_checksum(&self) -> u64 {
        let mut hash: u64 = 0xCBF2_9CE4_8422_2325; // FNV-1a 64 base
        for &c in self.coords.as_slice() {
            let bits = c.to_bits();
            hash ^= bits;
            hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
        }
        for n in &self.nodes {
            hash ^= n.point_idx as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
            hash ^= n.threshold.to_bits();
            hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
        }
        hash
    }
}
