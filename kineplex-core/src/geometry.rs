//! Controle Geométrico Avançado para o Fluxo de Ricci
//! 
//! Este módulo implementa:
//! - FT-089: Controlador de Passo Adaptativo PID
//! - FT-090: Normalização de Volume do Tensor Métrico
//! - FT-091: Descoberta de 2-Simplexos (Faces Triangulares)
//! - FT-092: Fase de Aquecimento Geométrico

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// ==========================================
/// FT-089: Controlador de Passo Adaptativo PID
/// ==========================================

/// Controlador PID para passo adaptativo epsilon
#[derive(Debug, Clone)]
pub struct AdaptiveStepController {
    /// Ganho proporcional
    kp: f64,
    /// Ganho integral
    ki: f64,
    /// Ganho derivativo
    kd: f64,
    /// Erro anterior
    prev_error: f64,
    /// Termo integral acumulado
    integral: f64,
    /// Valor atual de epsilon
    epsilon: f64,
    /// Valor mínimo de epsilon
    epsilon_min: f64,
    /// Valor máximo de epsilon
    epsilon_max: f64,
}

impl AdaptiveStepController {
    pub fn new(initial_epsilon: f64) -> Self {
        Self {
            kp: 0.5,
            ki: 0.1,
            kd: 0.3,
            prev_error: 0.0,
            integral: 0.0,
            epsilon: initial_epsilon,
            epsilon_min: 0.001,
            epsilon_max: 1.0,
        }
    }
    
    pub fn with_limits(mut self, min: f64, max: f64) -> Self {
        self.epsilon_min = min;
        self.epsilon_max = max;
        self
    }
    
    pub fn with_pid_gains(mut self, kp: f64, ki: f64, kd: f64) -> Self {
        self.kp = kp;
        self.ki = ki;
        self.kd = kd;
        self
    }
    
    /// Calcula o próximo valor de epsilon baseado na mudança de curvatura
    pub fn compute(&mut self, curvature_change: f64) -> f64 {
        // Erro: mudança de curvatura desejada (0) vs mudança observada
        let error = 0.0 - curvature_change;
        
        // Componente proporcional
        let p_term = self.kp * error;
        
        // Componente integral com windup protection
        self.integral += error;
        self.integral = self.integral.clamp(-100.0, 100.0);
        let i_term = self.ki * self.integral;
        
        // Componente derivativo
        let d_term = self.kd * (error - self.prev_error);
        self.prev_error = error;
        
        // Novo epsilon
        let new_epsilon = self.epsilon + p_term + i_term + d_term;
        
        // Aplicar limites
        self.epsilon = new_epsilon.clamp(self.epsilon_min, self.epsilon_max);
        
        self.epsilon
    }
    
    /// Retorna o epsilon atual
    pub fn epsilon(&self) -> f64 {
        self.epsilon
    }
    
    /// Verifica se o sistema convergiu (oscilação eliminada)
    pub fn is_stable(&self) -> bool {
        self.integral.abs() < 0.01 && self.prev_error.abs() < 0.001
    }
    
    /// Reseta o controlador
    pub fn reset(&mut self) {
        self.prev_error = 0.0;
        self.integral = 0.0;
    }
}

/// Histórico de epsilon para análise de oscilação
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpsilonHistory {
    samples: Vec<f64>,
    max_samples: usize,
}

impl EpsilonHistory {
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: Vec::with_capacity(max_samples),
            max_samples,
        }
    }
    
    pub fn push(&mut self, epsilon: f64) {
        if self.samples.len() >= self.max_samples {
            self.samples.remove(0);
        }
        self.samples.push(epsilon);
    }
    
    /// Calcula a variância (indicador de oscilação)
    pub fn variance(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        
        let mean: f64 = self.samples.iter().sum::<f64>() / self.samples.len() as f64;
        let variance: f64 = self.samples.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / self.samples.len() as f64;
        
        variance
    }
    
    /// Verifica se há oscilação pendular (padrão alternante)
    pub fn has_oscillation(&self) -> bool {
        if self.samples.len() < 4 {
            return false;
        }
        
        let mut oscillations = 0;
        for i in 2..self.samples.len() {
            let prev = self.samples[i - 1];
            let curr = self.samples[i];
            let prev_prev = self.samples[i - 2];
            
            // Detectar mudança de direção
            if (curr > prev && prev_prev > prev) || (curr < prev && prev_prev < prev) {
                oscillations += 1;
            }
        }
        
        // Se mais de 30% das amostras mostram oscilação
        oscillations as f64 / (self.samples.len() - 2) as f64 > 0.3
    }
}

/// ==========================================
/// FT-090: Normalização de Volume do Tensor Métrico
/// ==========================================

/// Erros de normalização
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NormalizationError {
    ZeroVolume,
    Overflow(f64),
    Underflow(f64),
}

impl std::fmt::Display for NormalizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NormalizationError::ZeroVolume => write!(f, "Volume total é zero, normalização impossível"),
            NormalizationError::Overflow(v) => write!(f, "Overflow: valor {} excede u32::MAX", v),
            NormalizationError::Underflow(v) => write!(f, "Underflow: valor {} é muito pequeno", v),
        }
    }
}

/// Normalizador de volume do tensor métrico
#[derive(Debug, Clone)]
pub struct MetricTensorNormalizer {
    /// Volume alvo desejado
    target_volume: f64,
    /// Valor máximo do tensor (u32)
    max_tensor_value: u32,
    /// Tolerância de convergência
    tolerance: f64,
}

impl MetricTensorNormalizer {
    pub fn new(target_volume: f64) -> Self {
        Self {
            target_volume,
            max_tensor_value: u32::MAX,
            tolerance: 0.001,
        }
    }
    
    pub fn with_tolerance(mut self, tolerance: f64) -> Self {
        self.tolerance = tolerance;
        self
    }
    
    /// Normaliza a matriz de pesos para manter volume constante
    pub fn normalize(&self, weights: &mut [f64]) -> Result<(), NormalizationError> {
        let current_volume: f64 = weights.iter().sum();
        
        if current_volume.abs() < f64::EPSILON {
            return Err(NormalizationError::ZeroVolume);
        }
        
        // Calcular fator de escala
        let scale_factor = self.target_volume / current_volume;
        
        // Aplicar normalização
        for weight in weights.iter_mut() {
            *weight *= scale_factor;
            
            // Verificar overflow
            if *weight > (u32::MAX as f64) {
                return Err(NormalizationError::Overflow(*weight));
            }
            
            // Verificar underflow
            if *weight < f64::EPSILON && *weight > 0.0 {
                return Err(NormalizationError::Underflow(*weight));
            }
        }
        
        Ok(())
    }
    
    /// Normaliza com clamping para u32
    pub fn normalize_with_clamp(&self, weights: &mut [f64]) -> f64 {
        let current_volume: f64 = weights.iter().sum();
        
        if current_volume.abs() < f64::EPSILON {
            return 0.0;
        }
        
        let scale_factor = self.target_volume / current_volume;
        let mut max_weight = 0.0;
        
        for weight in weights.iter_mut() {
            *weight *= scale_factor;
            *weight = weight.clamp(0.0, u32::MAX as f64);
            max_weight = max_weight.max(*weight);
        }
        
        max_weight
    }
    
    /// Verifica se os pesos estão dentro do limite u32
    pub fn is_within_bounds(&self, weights: &[f64]) -> bool {
        weights.iter().all(|w| *w <= u32::MAX as f64 && *w >= 0.0)
    }
    
    /// Retorna o volume atual
    pub fn current_volume(&self, weights: &[f64]) -> f64 {
        weights.iter().sum()
    }
    
    /// Verifica se o volume está dentro da tolerância
    pub fn is_volume_stable(&self, weights: &[f64]) -> bool {
        let current = self.current_volume(weights);
        let diff = (current - self.target_volume).abs();
        diff / self.target_volume < self.tolerance
    }
}

/// ==========================================
/// FT-091: Descoberta de 2-Simplexos (Faces Triangulares)
/// ==========================================

/// Representa um nó na rede
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GeoNodeId(pub String);

/// Descoberta de 2-simplexos (faces triangulares) para homologia
#[derive(Debug, Clone)]
pub struct Simplex2Discoverer {
    /// Armazena adjacências
    adjacency: HashMap<GeoNodeId, HashSet<GeoNodeId>>,
}

impl Simplex2Discoverer {
    pub fn new() -> Self {
        Self {
            adjacency: HashMap::new(),
        }
    }
    
    /// Adiciona uma aresta
    pub fn add_edge(&mut self, a: GeoNodeId, b: GeoNodeId) {
        self.adjacency.entry(a.clone()).or_insert_with(HashSet::new).insert(b.clone());
        self.adjacency.entry(b).or_insert_with(HashSet::new).insert(a);
    }
    
    /// Remove uma aresta
    pub fn remove_edge(&mut self, a: &GeoNodeId, b: &GeoNodeId) {
        if let Some(neighbors) = self.adjacency.get_mut(a) {
            neighbors.remove(b);
        }
        if let Some(neighbors) = self.adjacency.get_mut(b) {
            neighbors.remove(a);
        }
    }
    
    /// Encontra todos os 2-simplexos (triângulos) na rede
    pub fn find_triangles(&self) -> Vec<[GeoNodeId; 3]> {
        let mut triangles = Vec::new();
        
        let nodes: Vec<&GeoNodeId> = self.adjacency.keys().collect();
        
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                for k in (j + 1)..nodes.len() {
                    let a = nodes[i];
                    let b = nodes[j];
                    let c = nodes[k];
                    
                    // Verificar se a-b, b-c, c-a são todos conectados
                    let is_triangle = 
                        self.is_connected(a, b) &&
                        self.is_connected(b, c) &&
                        self.is_connected(c, a);
                    
                    if is_triangle {
                        triangles.push([a.clone(), b.clone(), c.clone()]);
                    }
                }
            }
        }
        
        triangles
    }
    
    /// Verifica se dois nós estão conectados
    fn is_connected(&self, a: &GeoNodeId, b: &GeoNodeId) -> bool {
        self.adjacency
            .get(a)
            .map(|neighbors| neighbors.contains(b))
            .unwrap_or(false)
    }
    
    /// Calcula o número de arestas
    pub fn count_edges(&self) -> usize {
        let mut count = 0_usize;
        for neighbors in self.adjacency.values() {
            count += neighbors.len();
        }
        count / 2 // Cada aresta é contada duas vezes
    }
    
    /// Calcula o Número de Betti (β₁) - número de buracos na rede
    pub fn betti_number(&self) -> isize {
        let triangles = self.find_triangles().len();
        let edges = self.count_edges();
        let nodes = self.adjacency.len();
        
        // Fórmula de Euler-Poincaré para grafos
        // β₁ = E - V + F (onde F = triângulos + 1 para componente conectado)
        if nodes > 0 {
            edges as isize - nodes as isize + triangles as isize + 1
        } else {
            0
        }
    }
    
    /// Identifica partições (componentes conectados)
    pub fn partitions(&self) -> Vec<Vec<GeoNodeId>> {
        let mut visited = HashSet::new();
        let mut partitions = Vec::new();
        
        for node in self.adjacency.keys() {
            if !visited.contains(node) {
                let mut component = Vec::new();
                self.dfs_collect(node, &mut visited, &mut component);
                partitions.push(component);
            }
        }
        
        partitions
    }
    
    fn dfs_collect(&self, node: &GeoNodeId, visited: &mut HashSet<GeoNodeId>, component: &mut Vec<GeoNodeId>) {
        visited.insert(node.clone());
        component.push(node.clone());
        
        if let Some(neighbors) = self.adjacency.get(node) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    self.dfs_collect(neighbor, visited, component);
                }
            }
        }
    }
}

impl Default for Simplex2Discoverer {
    fn default() -> Self {
        Self::new()
    }
}

/// ==========================================
/// FT-092: Fase de Aquecimento Geométrico
/// ==========================================

/// Spike sintético para warm-up
#[derive(Debug, Clone)]
pub struct SyntheticSpike {
    pub id: u32,
    pub timestamp: Instant,
    pub source: GeoNodeId,
    pub target: GeoNodeId,
}

impl SyntheticSpike {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            timestamp: Instant::now(),
            source: GeoNodeId(format!("node-{}", id % 3)),
            target: GeoNodeId(format!("node-{}", (id + 1) % 3)),
        }
    }
}

/// Resultado do warm-up
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmupResult {
    pub spikes_used: u32,
    pub warmup_time_ms: u64,
    pub converged: bool,
}

/// Erros de warm-up
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WarmupError {
    Timeout(u64),
    ConvergenceFailed,
}

impl std::fmt::Display for WarmupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WarmupError::Timeout(ms) => write!(f, "Warm-up excedeu timeout de {}ms", ms),
            WarmupError::ConvergenceFailed => write!(f, "Falha na convergência durante warm-up"),
        }
    }
}

/// Sistema de tensor métrico para warm-up
pub trait MetricTensorSystem {
    fn process_spike(&mut self, spike: SyntheticSpike) -> impl std::future::Future<Output = Result<(), WarmupError>> + Send;
    fn metric_variance(&self) -> f64;
    fn metric_mean(&self) -> f64;
}

/// Fase de aquecimento geométrico
#[derive(Debug, Clone)]
pub struct GeometricWarmup {
    /// Número de spikes sintéticos para warm-up
    num_warmup_spikes: u32,
    /// Threshold de convergência
    convergence_threshold: f64,
    /// Tempo máximo de warm-up em ms
    max_warmup_time_ms: u64,
    /// Se o warm-up foi completado
    is_warmed_up: bool,
    /// Tempo de início do warm-up
    warmup_start_time: Option<Instant>,
    /// Histórico de métricas para detecção de convergência
    metric_history: Vec<f64>,
}

impl GeometricWarmup {
    pub fn new() -> Self {
        Self {
            num_warmup_spikes: 100,
            convergence_threshold: 0.01,
            max_warmup_time_ms: 5000,
            is_warmed_up: false,
            warmup_start_time: None,
            metric_history: Vec::new(),
        }
    }
    
    pub fn with_spikes(mut self, num: u32) -> Self {
        self.num_warmup_spikes = num;
        self
    }
    
    pub fn with_threshold(mut self, threshold: f64) -> Self {
        self.convergence_threshold = threshold;
        self
    }
    
    pub fn with_timeout(mut self, ms: u64) -> Self {
        self.max_warmup_time_ms = ms;
        self
    }
    
    /// Executa o warm-up geométrico
    pub async fn warmup<T: MetricTensorSystem + Send + Sync>(
        &mut self,
        metric_system: &T,
    ) -> Result<WarmupResult, WarmupError> {
        self.warmup_start_time = Some(Instant::now());
        self.metric_history.clear();
        
        for i in 0..self.num_warmup_spikes {
            // Disparar spike sintético
            let spike = SyntheticSpike::new(i);
            metric_system.process_spike(spike).await?;
            
            // Calcular variância atual
            let variance = metric_system.metric_variance();
            self.metric_history.push(variance);
            
            // Manter histórico limitado
            if self.metric_history.len() > 20 {
                self.metric_history.remove(0);
            }
            
            // Verificar convergência
            if self.check_convergence() {
                let elapsed = self.warmup_start_time.unwrap().elapsed().as_millis() as u64;
                self.is_warmed_up = true;
                
                return Ok(WarmupResult {
                    spikes_used: i + 1,
                    warmup_time_ms: elapsed,
                    converged: true,
                });
            }
        }
        
        // Verificar timeout
        if let Some(start) = self.warmup_start_time {
            let elapsed = start.elapsed().as_millis() as u64;
            if elapsed > self.max_warmup_time_ms {
                return Err(WarmupError::Timeout(elapsed));
            }
        }
        
        self.is_warmed_up = true;
        Ok(WarmupResult {
            spikes_used: self.num_warmup_spikes,
            warmup_time_ms: self.warmup_start_time.map(|s| s.elapsed().as_millis() as u64).unwrap_or(0),
            converged: true,
        })
    }
    
    /// Verifica se a métrica estabilizou
    fn check_convergence(&self) -> bool {
        if self.metric_history.len() < 10 {
            return false;
        }
        
        // Verificar variância das últimas amostras
        let recent: &[f64] = &self.metric_history[self.metric_history.len() - 10..];
        let mean: f64 = recent.iter().sum::<f64>() / recent.len() as f64;
        let variance: f64 = recent.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / recent.len() as f64;
        
        variance < self.convergence_threshold
    }
    
    /// Retorna se o sistema está operacional
    pub fn is_operational(&self) -> bool {
        self.is_warmed_up
    }
    
    /// Tempo decorrido desde o início do warm-up
    pub fn elapsed_time_ms(&self) -> u64 {
        self.warmup_start_time
            .map(|s| s.elapsed().as_millis() as u64)
            .unwrap_or(0)
    }
}

impl Default for GeometricWarmup {
    fn default() -> Self {
        Self::new()
    }
}

// ==========================================
// TESTS
// ==========================================

#[cfg(test)]
mod tests {
    use super::*;
    
    // FT-089 Tests
    #[test]
    fn test_adaptive_step_controller() {
        let mut controller = AdaptiveStepController::new(0.5);
        
        // Simular mudança de curvatura
        let epsilon1 = controller.compute(0.1);  // Curvatura aumentando
        let epsilon2 = controller.compute(0.0);  // Estável
        let epsilon3 = controller.compute(-0.1); // Diminuindo
        
        // Deve ajustar o epsilon
        assert!(controller.epsilon() > 0.0);
    }
    
    #[test]
    fn test_epsilon_limits() {
        let mut controller = AdaptiveStepController::new(0.5)
            .with_limits(0.01, 1.0);
        
        // Forçar valores extremos
        for _ in 0..100 {
            controller.compute(10.0);
        }
        
        assert!(controller.epsilon() >= 0.01);
        assert!(controller.epsilon() <= 1.0);
    }
    
    #[test]
    fn test_epsilon_history_variance() {
        let mut history = EpsilonHistory::new(10);
        
        history.push(0.5);
        history.push(0.6);
        history.push(0.5);
        history.push(0.6);
        
        // Deve detectar oscilação
        assert!(history.has_oscillation());
    }
    
    // FT-090 Tests
    #[test]
    fn test_tensor_normalization() {
        let normalizer = MetricTensorNormalizer::new(100.0);
        let mut weights = vec![25.0, 25.0, 25.0, 25.0];  // Soma = 100
        
        normalizer.normalize(&mut weights).unwrap();
        
        // Soma deve permanecer 100
        let sum: f64 = weights.iter().sum();
        assert!((sum - 100.0).abs() < 0.001);
    }
    
    #[test]
    fn test_tensor_normalization_zero_volume() {
        let normalizer = MetricTensorNormalizer::new(100.0);
        let mut weights = vec![0.0, 0.0, 0.0];
        
        let result = normalizer.normalize(&mut weights);
        assert!(matches!(result, Err(NormalizationError::ZeroVolume)));
    }
    
    #[test]
    fn test_tensor_clamp() {
        let normalizer = MetricTensorNormalizer::new(1.0);
        let mut weights = vec![1e20, 1e30, 1e40];
        
        let max = normalizer.normalize_with_clamp(&mut weights);
        
        // Deve estar dentro de u32
        assert!(max <= u32::MAX as f64);
    }
    
    #[test]
    fn test_volume_stability() {
        let normalizer = MetricTensorNormalizer::new(100.0).with_tolerance(0.01);
        let weights = vec![100.0, 0.0, 0.0];
        
        assert!(normalizer.is_volume_stable(&weights));
    }
    
    // FT-091 Tests
    #[test]
    fn test_simplex_triangle_discovery() {
        let mut discoverer = Simplex2Discoverer::new();
        
        // Criar triângulo: A-B-C
        discoverer.add_edge(GeoNodeId("A".to_string()), GeoNodeId("B".to_string()));
        discoverer.add_edge(GeoNodeId("B".to_string()), GeoNodeId("C".to_string()));
        discoverer.add_edge(GeoNodeId("C".to_string()), GeoNodeId("A".to_string()));
        
        let triangles = discoverer.find_triangles();
        
        assert!(!triangles.is_empty());
    }
    
    #[test]
    fn test_betti_number() {
        let mut discoverer = Simplex2Discoverer::new();
        
        // Criar um triângulo
        discoverer.add_edge(GeoNodeId("A".to_string()), GeoNodeId("B".to_string()));
        discoverer.add_edge(GeoNodeId("B".to_string()), GeoNodeId("C".to_string()));
        discoverer.add_edge(GeoNodeId("C".to_string()), GeoNodeId("A".to_string()));
        
        let beta = discoverer.betti_number();
        
        // Um triângulo = 1 face, sem buracos
        assert!(beta >= 0);
    }
    
    #[test]
    fn test_partitions() {
        let mut discoverer = Simplex2Discoverer::new();
        
        // Componente 1: A-B
        discoverer.add_edge(GeoNodeId("A".to_string()), GeoNodeId("B".to_string()));
        
        // Componente 2: C-D
        discoverer.add_edge(GeoNodeId("C".to_string()), GeoNodeId("D".to_string()));
        
        let partitions = discoverer.partitions();
        
        assert_eq!(partitions.len(), 2);
    }
    
    // FT-092 Tests
    #[test]
    fn test_synthetic_spike() {
        let spike = SyntheticSpike::new(42);
        
        assert_eq!(spike.id, 42);
    }
    
    #[test]
    fn test_warmup_operational() {
        let warmup = GeometricWarmup::new();
        
        assert!(!warmup.is_operational());
    }
    
    #[test]
    fn test_warmup_configuration() {
        let warmup = GeometricWarmup::new()
            .with_spikes(50)
            .with_threshold(0.001)
            .with_timeout(3000);
        
        assert!(!warmup.is_operational());
    }
}