//! Parâmetros de Configuração da Distribuição Ótima de Laguerre-Kantorovich (MK-EMD)

/// Hiperparâmetros formais do Problema de Transporte Ótimo Semi-Discreto (MK-EMD)
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct KantorovichDistributionConfig {
    pub target_mass_bytes: u64,   // \nu_k = M^* (128 MB)
    pub min_critical_bytes: u64,  // M_min (64 MB - Limite inferior estrito Anti-Small-Files)
    pub max_ceiling_bytes: u64,   // M_max (256 MB - Limite superior de selagem)
    pub banach_p: f64,            // Ordem p \in [1, \infty) do espaço L^p
    pub entropic_epsilon: f64,    // Parâmetro de regularização de Sinkhorn-Laguerre \varepsilon > 0
    pub hull_penalty_lambda: f64, // Penalidade \lambda_{hull} contra dilatação de Min/Max no Iceberg
}

impl Default for KantorovichDistributionConfig {
    fn default() -> Self {
        Self {
            target_mass_bytes: 128 * 1024 * 1024,
            min_critical_bytes: 64 * 1024 * 1024,
            max_ceiling_bytes: 256 * 1024 * 1024,
            banach_p: 2.0,
            entropic_epsilon: 0.25,
            hull_penalty_lambda: 0.50,
        }
    }
}

impl KantorovichDistributionConfig {
    /// Construtor de teste com escala reduzida para validação instantânea em memória.
    pub fn test_scale(min_bytes: u64, target_bytes: u64, max_bytes: u64) -> Self {
        Self {
            target_mass_bytes: target_bytes,
            min_critical_bytes: min_bytes,
            max_ceiling_bytes: max_bytes,
            banach_p: 2.0,
            entropic_epsilon: 0.25,
            hull_penalty_lambda: 0.50,
        }
    }
}

/// Alias para manter compatibilidade com implementações existentes
pub type OptimalDistributionConfig = KantorovichDistributionConfig;
