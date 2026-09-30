# Observabilidade Operacional - FT-079

## Visão Geral

Este documento detalha a implementação de observabilidade para o pipeline de dados KinePlex, permitindo medição real de métricas de pipeline e tomada de decisão baseada em dados para prontidão de produção.

## Métricas Coletadas

### Métricas de Pipeline

| Métrica | Tipo | Descrição |
|---------|------|-----------|
| `kineplex_stage_latency_ms` | Histogram | Latência por estágio em milliseconds |
| `kineplex_stage_rows` | Counter | Linhas processadas por estágio |
| `kineplex_stage_errors` | Counter | Erros por estágio |
| `kineplex_stage_bytes` | Counter | Bytes transferidos entre estágios |
| `kineplex_total_execution_time_ms` | Histogram | Tempo total de execução E2E |
| `kineplex_allocation_failures` | Counter | Falhas de alocação de nós |
| `kineplex_retries` | Counter | Número de tentativas de execução |

### Métricas de Recursos

| Métrica | Tipo | Descrição |
|---------|------|-----------|
| `kineplex_memory_peak_bytes` | Gauge | Pico de memória em bytes |
| `kineplex_cpu_time_ms` | Gauge | Tempo de CPU em milliseconds |
| `kineplex_network_bytes_sent` | Counter | Bytes enviados via rede |
| `kineplex_network_bytes_received` | Counter | Bytes recebidos via rede |

### Métricas por Tenant

| Métrica | Tipo | Descrição |
|---------|------|-----------|
| `kineplex_tenant_graphs_active` | Gauge | Grafos ativos por tenant |
| `kineplex_tenant_quota_used` | Gauge | Quota utilizada por tenant |
| `kineplex_tenant_executions_total` | Counter | Total de execuções por tenant |

## Integração Prometheus/OTLP

### Configuração Prometheus

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'kineplex'
    static_configs:
      - targets: ['localhost:9090']
    metrics_path: '/metrics'
```

### Exportação OTLP

```rust
use opentelemetry::sdk::export::metrics::OTLPMetricExporter;

let exporter = OTLPMetricExporter::new(
    otlp_endpoint,
    OTLPMetricConfig::default()
);
```

## Dashboard de SLO

O dashboard deve exibir:

1. **Availability** - Percentual de requisições bem-sucedidas
2. **Latência p95** - Latência no percentil 95
3. **Throughput** - Requisições por segundo
4. **Error Rate** - Taxa de erros
5. **Resource Usage** - Uso de CPU, memória, rede

## Campos de Relatório E2E

O harness E2E agora preenche campos críticos que antes eram null:

```yaml
e2e_report:
  stage_latency:
    receptor_ms: 15
    wasm_ms: 150
    aggregation_ms: 45
    terminal_ms: 30
  total_time_ms: 240
  rows_processed: 10000
  allocation_time_ms: 50
  retry_count: 0
  allocation_failures: 0
```