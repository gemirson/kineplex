# SLOs Formais e Alertas Acionáveis - FT-080

## Service Level Objectives

Este documento define os SLIs (Service Level Indicators), SLOs (Service Level Objectives) e alertas para operação do KinePlex.

## SLIs e SLOs Definidos

### 1. Disponibilidade (Availability)

| Componente | SLI | SLO | Janela |
|------------|-----|-----|--------|
| API Server | Requisições bem-sucedidas / Total | 99.9% | 30 dias |
| Graph Execution | Execuções completadas / Total | 99.5% | 30 dias |
| Cluster | Nós healthy / Total nós | 99.9% | 30 dias |

### 2. Latência (Latency)

| Componente | SLI | SLO | Janela |
|------------|-----|-----|--------|
| API Latency | p95 de latência de API | < 500ms | 5 minutos |
| Stage Latency | p95 de latência por estágio | < 200ms | 5 minutos |
| E2E Execution | p95 de tempo total E2E | < 5s | 5 minutos |

### 3. Qualidade (Quality)

| Componente | SLI | SLO | Janela |
|------------|-----|-----|--------|
| Error Rate | Erros / Total requisições | < 1% | 5 minutos |
| Allocation Success | Alocações bem-sucedidas | > 99% | 5 minutos |

### 4. Throughput

| Componente | SLI | SLO | Janela |
|------------|-----|-----|--------|
| Graph Submission | Grafos submetidos | > 10/s | 5 minutos |
| Data Processing | Linhas processadas | > 10000/s | 5 minutos |

## Alertas Configuradas

### Alertas Críticas (P1)

```yaml
alerts:
  - name: "availability_below_99_percent"
    condition: "availability < 0.99"
    window: "5m"
    severity: "critical"
    action: "Page on-call immediately"
    
  - name: "latency_p95_above_1s"
    condition: "latency_p95 > 1000"
    window: "5m"
    severity: "critical"
    action: "Page on-call immediately"
    
  - name: "error_rate_above_5_percent"
    condition: "error_rate > 0.05"
    window: "5m"
    severity: "critical"
    action: "Page on-call immediately"

  - name: "cluster_less_than_3_nodes"
    condition: "healthy_nodes < 3"
    window: "2m"
    severity: "critical"
    action: "Page on-call immediately"
```

### Alertas de Aviso (P2)

```yaml
  - name: "latency_p95_above_500ms"
    condition: "latency_p95 > 500"
    window: "5m"
    severity: "warning"
    action: "Create incident"
    
  - name: "error_rate_above_1_percent"
    condition: "error_rate > 0.01"
    window: "5m"
    severity: "warning"
    action: "Create incident"
    
  - name: "quota_usage_above_80_percent"
    condition: "quota_used / quota_total > 0.8"
    window: "10m"
    severity: "warning"
    action: "Notify team"
```

## Error Budget

### Política de Error Budget

- **Disponibilidade**: 0.1% de erro permitido por mês
  - 43.8 minutos de downtime permitido por mês
  
- **Latência**: 5% de requisições podem exceder SLO

### Ações baseadas em Error Budget

| Estado | Ação |
|--------|------|
| Budget > 50% | Operação normal |
| Budget 25-50% | Alerta de atenção |
| Budget < 25% | Reunião de revisão |
| Budget exhausted | Freeze de mudanças não críticas |

## Validação

Para declarar 95% de prontidão, todos os SLOs devem ter:
- [ ] Métricas expostas em Prometheus/OTLP
- [ ] Dashboard operacional funcionando
- [ ] Alertas configurados e testados
- [ ] Policy de error budget definida
- [ ] Revisão operacional aprovada