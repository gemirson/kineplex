# Quickstart: TPC-H / TPC-DS Benchmarks & Whitepaper (FT-101)

Este guia rápido demonstra como gerar os dados TPC-H, executar as consultas padronizadas (Q1 a Q22) no KinePlex e confrontar o desempenho com DuckDB e Apache Spark.

---

## 1. Geração dos Dados TPC-H (Scale Factor 1 para Testes ou SF=1000 para 1TB)

```bash
# Modo rápido para validação de testes unitários e CI (SF=1 / ~1GB)
cargo run -p kineplex-bench --bin tpch_datagen -- --scale 1 --out-dir /tmp/tpch_sf1

# Modo Produção / Benchmark Oficial (SF=1000 / ~1TB)
cargo run -p kineplex-bench --bin tpch_datagen -- --scale 1000 --out-dir /mnt/nvme/tpch_sf1000 --laguerre-partition
```

---

## 2. Executando a Suíte TPC-H Completa (Q1 a Q22) no KinePlex

```bash
# Executa as 22 consultas no KinePlex em modo release
cargo run --release -p kineplex-bench --bin tpch_runner -- \
  --data-dir /tmp/tpch_sf1 \
  --scale 1 \
  --engine kineplex \
  --warmup 1 \
  --runs 5
```

---

## 3. Executando o Benchmark Comparativo (KinePlex vs DuckDB vs Spark)

```bash
cargo run --release -p kineplex-bench --bin comparative_runner -- \
  --data-dir /mnt/nvme/tpch_sf1000 \
  --scale 1000 \
  --engines kineplex,duckdb,spark \
  --output-json ./reports/tpch_sf1000_results.json
```

---

## 4. Gerando e Compilando o Whitepaper Científico

```bash
cargo run --release -p kineplex-bench --bin generate_whitepaper -- \
  --results-json ./reports/tpch_sf1000_results.json \
  --output docs/whitepaper/kineplex_analytical_superiority.md
```
