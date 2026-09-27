# Estratégia de Deploy Progressivo e Rollback - FT-086

## Visão Geral

Este documento define a estratégia de deploy canary/rolling com rollback automático e manual para o KinePlex.

## Estratégia de Deploy

### 1. Canary Deployment

```yaml
deploy_strategy:
  type: "canary"
  steps:
    - stage: "canary"
      traffic: 5%
      duration: "10m"
      metric_threshold:
        error_rate: < 1%
        latency_p95: < 500ms
    - stage: "partial"
      traffic: 25%
      duration: "20m"
    - stage: "majority"
      traffic: 50%
      duration: "30m"
    - stage: "full"
      traffic: 100%
```

### 2. Rolling Deployment

```yaml
rolling_strategy:
  type: "rolling"
  max_surge: "25%"
  max_unavailable: "10%"
  health_check_interval: "30s"
```

## Gatilhos de Rollback

### Rollback Automático

| Condição | Threshold | Ação |
|----------|-----------|------|
| Error rate | > 5% | Rollback imediato |
| Latency p95 | > 1s por 2min | Rollback imediato |
| Health check failures | > 3 consecutivas | Rollback automático |
| SLO budget exhausted | 100% | Rollback imediato |

### Rollback Manual

```bash
# Trigger rollback manual
./deploy rollback --version v1.0.0 --reason "Performance degradation"
```

## Validação de Rollback

### Plano de Rollback Testado

1. **Teste em Homologação**
   ```bash
   # Simular falha e rollback
   ./test_rollback.sh --env homolog --failure-scenario "node_failure"
   ```

2. **Métricas de Rollback**
   - Tempo de detecção: < 30s
   - Tempo de rollback: < 2min
   - Tempo de recuperação: < 5min

3. **Evidência Operacional**
   ```
   rollback_log:
     triggered_at: "2024-01-15T10:30:00Z"
     reason: "Error rate exceeded threshold (7%)"
     duration_seconds: 85
     nodes_affected: ["node-3", "node-5"]
     final_state: "Healthy"
   ```

## Configuração de Deploy

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kineplex-control-plane
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  progressDeadlineSeconds: 600
  minReadySeconds: 30
```

### Service Mesh (Istio)

```yaml
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: kineplex
spec:
  hosts:
    - kineplex
  http:
    - route:
        - destination:
            host: kineplex
            subset: v1.0.0
          weight: 95
        - destination:
            host: kineplex
            subset: v1.0.1-canary
          weight: 5
```

## Checklist de Validação

- [ ] Plano de rollback testado em homologação
- [ ] Gatilhos de rollback conectados a SLO/erro budget
- [ ] Tempo de reversão registrado em evidência
- [ ] Procedure de rollback documentado para equipe
- [ ] Rollback já exercitado em ambiente de teste