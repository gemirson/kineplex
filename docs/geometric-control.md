# Controle Geométrico Avançado - FT-089 a FT-092

Este documento detalha as features de controle geométrico avançado para o fluxo de Ricci no KinePlex.

---

## FT-089 — Controlador de Passo Adaptativo (ε) para o Fluxo de Ricci

### Problema
A deformação contínua de rotas (homotopia) depende de um tamanho de passo ε. Se ε for estático e a rede sofrer um pico de tráfego, a deformação será demasiado agressiva, causando oscilação infinita de rotas (efeito ping-pong entre os nós).

### Solução
Implementar um controlador PID no kernel que ajusta ε dinamicamente:
- Se a geometria muda muito rápido → ε diminui para amortecer a transição
- Se estável → ε aumenta para encontrar a geodésica mais rapidamente

### Implementação

```rust
/// Controlador PID para passo adaptativo
pub struct AdaptiveStepController {
    // Parâmetros PID
    kp: f64,  // Proporcional
    ki: f64,  // Integral
    kd: f64,  // Derivativo
    
    // Estado
    prev_error: f64,
    integral: f64,
    epsilon: f64,
    
    // Limites
    epsilon_min: f64,
    epsilon_max: f64,
}

impl AdaptiveStepController {
    /// Calcula o próximo valor de epsilon baseado no erro de curvatura
    pub fn compute(&mut self, curvature_change: f64) -> f64 {
        // Erro: mudança de curvatura desejada (0) vs mudança observada
        let error = 0.0 - curvature_change;
        
        // Componente proporcional
        let p_term = self.kp * error;
        
        // Componente integral (com windup protection)
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
    
    /// Verifica se o sistema converged (oscilação eliminada)
    pub fn is_stable(&self) -> bool {
        self.integral.abs() < 0.01 && self.prev_error.abs() < 0.001
    }
}
```

### Critérios de Aceite
- [ ] A malha converge para uma rota estável sem oscilações pendulares
- [ ] Após injeção forçada de falha num nó central, sistema se recupera
- [ ] Métrica de oscilação (variance) < threshold após convergência

---

## FT-090 — Normalização de Volume do Tensor Métrico (Prevenção de Overflow)

### Problema
O Fluxo de Ricci tem a propriedade de "encolher" espaços de curvatura positiva e "expandir" espaços de curvatura negativa. Num loop contínuo, os pesos tenderiam a infinito ou a zero, causando um Integer Overflow nos registradores bare-metal.

### Solução
Aplicar uma restrição de "Volume Constante":
- O driver normaliza a matriz de pesos a cada iteração
- A soma total das distâncias das cartas locais permanece constante
- Limita os tensores ao tamanho de um u32

### Implementação

```rust
/// Normalizador de volume do tensor métrico
pub struct MetricTensorNormalizer {
    target_volume: f64,
    max_tensor_value: u32,
}

impl MetricTensorNormalizer {
    /// Normaliza a matriz de pesos para manter volume constante
    pub fn normalize(&self, weights: &mut [f64]) -> Result<(), NormalizationError> {
        let current_volume: f64 = weights.iter().sum();
        
        if current_volume == 0.0 {
            return Err(NormalizationError::ZeroVolume);
        }
        
        // Calcular fator de escala
        let scale_factor = self.target_volume / current_volume;
        
        // Aplicar normalização
        for weight in weights.iter_mut() {
            *weight *= scale_factor;
            
            // Verificar overflow (limite u32)
            if *weight > (u32::MAX as f64) {
                return Err(NormalizationError::Overflow(*weight));
            }
        }
        
        Ok(())
    }
    
    /// Garante que o tensor não exceda o limite u32
    pub fn clamp_to_u32(&self, weights: &mut [f64]) {
        for w in weights.iter_mut() {
            *w = w.clamp(0.0, u32::MAX as f64);
        }
    }
}
```

### Critérios de Aceite
- [ ] Testes unitários atestam que o limite superior da métrica nunca gera overflow
- [ ] Independente do tempo de uptime do cluster
- [ ] Volume total permanece constante dentro de 0.1% de tolerância

---

## FT-091 — Descoberta de 2-Simplexos (Faces Triangulares) para Homologia

### Problema
Para o monitoramento de invariantes topológicos locais, analisar apenas ligações ponto a ponto (arestas) não é suficiente. É preciso identificar triângulos fechados para mapear "buracos" na rede.

### Solução
Um algoritmo de background que identifica ciclos de tamanho 3 (2-simplexos) entre os nós:
- A curvatura de Forman-Ricci é enriquecida para incluir a influência dessas faces
- Permite distinguir entre um nó isolado e um "buraco topológico"

### Implementação

```rust
/// Descoberta de 2-simplexos (faces triangulares)
pub struct Simplex2Discoverer {
    // Armazena adjacências
    adjacency: HashMap<NodeId, HashSet<NodeId>>,
}

impl Simplex2Discoverer {
    /// Encontra todos os 2-simplexos (triângulos) na rede
    pub fn find_triangles(&self) -> Vec<[NodeId; 3]> {
        let mut triangles = Vec::new();
        
        for (a, neighbors_a) in &self.adjacency {
            for b in neighbors_a {
                if a >= b { continue; } // Evitar duplicatas
                
                // Interseção de vizinhos de a e b
                let neighbors_b = self.adjacency.get(b);
                if let Some(nb) = neighbors_b {
                    let common: Vec<&NodeId> = neighbors_a.intersection(nb)
                        .filter(|c| *c > a && *c > b)
                        .collect();
                    
                    for c in common {
                        triangles.push([a.clone(), b.clone(), (*c).clone()]);
                    }
                }
            }
        }
        
        triangles
    }
    
    /// Calcula o Número de Betti (β₁) - número de buracos
    pub fn betti_number(&self) -> usize {
        let triangles = self.find_triangles();
        let edges = self.count_edges();
        let nodes = self.adjacency.len();
        
        // Fórmula de Euler para grafos planar
        // β₁ = E - V + F (onde F = triângulos)
        if nodes > 0 && edges >= nodes {
            edges - nodes + triangles.len()
        } else {
            0
        }
    }
}
```

### Critérios de Aceite
- [ ] O exportador de métricas reflete corretamente o Número de Betti (β₁)
- [ ] Identifica partições lógicas na topologia
- [ ] Detecta corretamente nós isolados vs buracos

---

## FT-092 — Fase de Aquecimento Geométrico (Topological Warm-up)

### Problema
Na inicialização, sem tráfego de dados, a curvatura e o tensor métrico são zero. Se o sistema injetar 10 GB repentinamente, as geodésicas se comportarão de forma aleatória no primeiro segundo.

### Solução
Antes de permitir o fluxo real, o motor realiza um warm-up:
- Dispara Spikes sintéticos (cabeçalhos sem payload Arrow)
- Inicializa os pesos do tensor métrico na malha
- Sistema aguarda estabilização antes de mudar para estado operacional

### Implementação

```rust
/// Fase de aquecimento geométrico
pub struct GeometricWarmup {
    // Configuração
    num_warmup_spikes: u32,
    convergence_threshold: f64,
    max_warmup_time_ms: u64,
    
    // Estado
    is_warmed_up: bool,
    warmup_start_time: Option<Instant>,
}

impl GeometricWarmup {
    /// Executa o warm-up geométrico
    pub async fn warmup(&mut self, metric_system: &mut MetricTensorSystem) -> Result<WarmupResult, WarmupError> {
        self.warmup_start_time = Some(Instant::now());
        
        for i in 0..self.num_warmup_spikes {
            // Disparar spike sintético
            let spike = SyntheticSpike::new(i);
            metric_system.process_spike(spike).await?;
            
            // Verificar convergência
            if self.check_convergence(metric_system) {
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
        let elapsed = self.warmup_start_time.unwrap().elapsed().as_millis() as u64;
        if elapsed > self.max_warmup_time_ms {
            return Err(WarmupError::Timeout(elapsed));
        }
        
        self.is_warmed_up = true;
        Ok(WarmupResult {
            spikes_used: self.num_warmup_spikes,
            warmup_time_ms: elapsed,
            converged: true,
        })
    }
    
    /// Verifica se a métrica estabilizou
    fn check_convergence(&self, system: &MetricTensorSystem) -> bool {
        let variance = system.metric_variance();
        variance < self.convergence_threshold
    }
    
    /// Retorna se o sistema está operacional
    pub fn is_operational(&self) -> bool {
        self.is_warmed_up
    }
}
```

### Critérios de Aceite
- [ ] A sinapse apenas muda para estado operacional após estabilização
- [ ] Métrica base do atlas local está estável (variance < threshold)
- [ ] Warm-up completa em tempo limite definido (< 5 segundos)

---

## Resumo das Novas Features

| Feature | Descrição | Critério Principal |
|---------|-----------|-------------------|
| FT-089 | Controlador PID adaptativo | Convergência sem oscilação |
| FT-090 | Normalização de volume | Sem overflow após uptime longo |
| FT-091 | Descoberta de 2-simplexos | Número de Betti correto |
| FT-092 | Warm-up geométrico | Estabilização antes de operação |