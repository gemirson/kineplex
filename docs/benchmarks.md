# Benchmarks Reproduzíveis do Data Plane - FT-081

## Metodologia de Benchmark

Este documento define o protocolo padronizado para execução de benchmarks reproduzíveis no data plane do KinePlex.

## Configuração de Ambiente

### Requisitos Mínimos

- **CPU**: 8 cores
- **Memória**: 16GB RAM
- **Rede**: 10Gbps
- **SO**: Linux (kernel 5.15+)

### Ambiente de Referência

```yaml
benchmark_environment:
  os: "Ubuntu 22.04"
  kernel: "5.15.0-91-generic"
  cpu: "Intel Xeon E-2388G"
  memory: "DDR4-3200 32GB"
  network: "10Gbps"
  
  rust_version: "1.75.0"
  wasmtime_version: "17.0.0"
  arrow_version: "46.0.0"
```

## Dataset de Referência

### Datasets para Benchmark

| Dataset | Tamanho | Linhas | Descrição |
|---------|---------|--------|-----------|
| small | 10MB | 100,000 | Testes unitários |
| medium | 100MB | 1,000,000 | Testes de carga |
| large | 1GB | 10,000,000 | Testes de stress |

## Protocolo de Execução

### 1. Warmup

```bash
# Warmup: 5 execuções para JIT/hot cache
for i in {1..5}; do
  ./run_benchmark.sh --dataset medium
done
```

### 2. Execução de Referência

```bash
# Execução: 10 execuções com resultado médio
./run_benchmark.sh \
  --dataset medium \
  --iterations 10 \
  --output results.json
```

### 3. Coleta de Métricas

```yaml
metrics_collected:
  - total_execution_time_ms
  - stage_latency_ms
  - memory_peak_bytes
  - cpu_time_ms
  - network_bytes
  - rows_processed_per_second
  - cache_hit_rate
```

## Resultados Esperados

### Baseline (sem otimizações)

| Métrica | small | medium | large |
|---------|-------|--------|-------|
| Tempo total (ms) | 150 | 1,200 | 12,000 |
| Throughput (rows/s) | 66,666 | 83,333 | 83,333 |
| Memória peak (MB) | 256 | 512 | 1024 |

### Target (com otimizações)

| Métrica | small | medium | large |
|---------|-------|--------|-------|
| Tempo total (ms) | 100 | 800 | 8,000 |
| Throughput (rows/s) | 100,000 | 125,000 | 125,000 |
| Memória peak (MB) | 200 | 400 | 800 |

## Relatório de Benchmark

O relatório deve incluir:

```json
{
  "version": "1.0.0",
  "timestamp": "2024-01-15T10:30:00Z",
  "environment": { ... },
  "dataset": {
    "name": "medium",
    "rows": 1000000,
    "size_bytes": 104857600
  },
  "results": {
    "iterations": 10,
    "mean_ms": 1200,
    "p50_ms": 1180,
    "p95_ms": 1350,
    "p99_ms": 1500,
    "std_dev_ms": 80,
    "min_ms": 1150,
    "max_ms": 1400
  },
  "variability": {
    "coefficient_of_variation": "6.7%",
    "stable": true
  }
}
```

## Validação

Para FT-081, os critérios são:
- [ ] Protocolo documentado
- [ ] Ambiente fixo definido
- [ ] Scripts de execução prontos
- [ ] Relatório versionado por execução
- [ ] Variabilidade reportada (p50/p95/range)