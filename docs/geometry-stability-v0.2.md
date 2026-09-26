# Geometry Stability Benchmark — v0.2

Documentação formal do harness de stress test e dos benchmarks Criterion para a
tabela de geometria de roteamento do KinePlex (`kineplex-core`).

---

## Ambiente de referência

| Campo  | Valor                  |
|--------|------------------------|
| CPU    | `{{ to be filled }}`   |
| Kernel | `{{ to be filled }}`   |
| Rustc  | `{{ to be filled }}`   |

Preencha esta tabela ao executar os benchmarks na máquina de CI ou no nó
integrado com o data plane. Use `uname -r`, `rustc --version`, e `/proc/cpuinfo`
(ou `lscpu`) para coletar os valores.

---

## Resultados dos testes de stress

Os testes de stress são executados via `cargo test` e validam a corretude
concorrente da tabela de geometria sob carga extrema.

### `geometry_deformation_stress`

**O que valida:** Publica 10.000 snapshots de substituição de rota enquanto
threads leitoras realizam 100.000 lookups simultâneos. Verifica que nenhuma
leitura observa estado parcialmente gravado (invariante de atomicidade) e que
todos os lookups retornam um resultado válido (sem panics, sem valores nulos
inesperados).

**Critérios de pass/fail:**

| Critério                         | Pass                          | Fail                         |
|----------------------------------|-------------------------------|------------------------------|
| Ausência de data race            | Nenhum UB detectado (Miri/loom ou execução limpa) | Panic, SIGSEGV, ou resultado corrompido |
| Todos os lookups retornam valor  | `result.is_ok()` em 100% das chamadas | Qualquer `Err` ou `None` inesperado     |
| Conclusão sem deadlock           | Teste termina dentro do timeout | Timeout ou hang              |

---

### `geometry_stress`

**O que valida:** Carga contínua de substituições de rota atômicas sobre a
tabela de geometria, verificando que o throughput de escrita não degrada a
latência de leitura além do limiar aceitável e que a estrutura interna permanece
consistente (checksums de integridade).

**Critérios de pass/fail:**

| Critério                              | Pass                                | Fail                          |
|---------------------------------------|-------------------------------------|-------------------------------|
| Integridade estrutural pós-stress     | Checksum interno consistente        | Divergência de checksum        |
| Sem vazamento de memória observável   | Heap estável ao final do teste      | Crescimento monotônico de heap |
| Teste conclui sem erro               | Exit code 0                         | Panic ou exit code ≠ 0         |

---

### `geometry_allocations`

**O que valida:** Confirma que operações críticas no hot path (lookup atômico de
next-hop, avaliação de norma tensorial, amostragem de homotopia) não realizam
alocações de heap desnecessárias. O objetivo é que o caminho quente seja
allocation-free ou com alocações amortizadas/previsíveis.

**Critérios de pass/fail:**

| Critério                                    | Pass                         | Fail                                  |
|---------------------------------------------|------------------------------|---------------------------------------|
| Lookups no hot path sem alocação de heap     | 0 alocações por lookup       | Qualquer alocação não amortizada       |
| Substituição de rota com alocação controlada | Alocação única por snapshot  | Alocações proporcionais ao número de leitores |

---

## Resultados Criterion — `geometry_stability`

O benchmark Criterion (`bench/geometry_stability`) mede latências absolutas e
ganho SIMD das operações centrais da tabela de geometria.

Execute com:

```sh
cargo bench -p kineplex-core --bench geometry_stability
```

### Tabela de métricas

| Benchmark              | Métrica          | Esperado (critério)   | Medido                  |
|------------------------|------------------|-----------------------|-------------------------|
| `geodesic_lookup`      | Latência p50     | `< 50 ns`             | `{{ run cargo bench }}` |
| `route_replace`        | Latência p50     | `< 200 ns`            | `{{ run cargo bench }}` |
| `tensor_norm`          | Latência p50     | `< 30 ns`             | `{{ run cargo bench }}` |
| `homotopy_sample`      | Latência p50     | `< 100 ns`            | `{{ run cargo bench }}` |
| `simd_ratio`           | Ganho SIMD       | `> 3.0x`              | `{{ run cargo bench }}` |
| `atomic_nexthop`       | Latência p50     | `< 20 ns`             | `{{ run cargo bench }}` |

> **Nota:** Os valores "Esperado" são os critérios de aceitação do FT-060. O
> campo "Medido" deve ser preenchido com a saída do Criterion (`mean` e `slope`)
> obtida na máquina de referência declarada na seção anterior.

---

## Status FT-060 E2E

### O que está implementado

O harness E2E completo existe em `bench/e2e/` e inclui:

- Cluster Docker Compose de quatro nós (`docker-compose.yml`)
- Geração determinística de dataset Parquet de 10 GiB (`generate_dataset.py`)
- Três filtros Wasm compilados via SDK (`wasm_filters/`)
- Estágio de agregação nativa e comparação de checksum terminal contra Polars
- Runner Python (`run_e2e.py`) que orquestra o ciclo completo: geração de dados,
  compilação Wasm, `docker compose up`, submissão de job, e validação de output
- Relatório JSON gerado em `bench/e2e/report.json` quando o resultado está disponível

### O que ainda falta / limitações honestas

| Item                                    | Status atual                                                         |
|-----------------------------------------|----------------------------------------------------------------------|
| Data Plane físico conectado             | **Não implementado.** O Control Plane valida e aloca o grafo, mas o Receptor, a invocação Wasm, a agregação nativa e o Terminal não formam um caminho de execução real ainda. |
| Resultado Parquet do job                | **Não produzido.** O runner retorna exit code `3` ao não encontrar `part-*.parquet` no diretório de output após o timeout. |
| p95 de latência capturado               | **Não medido.** O campo `p95_latency_ms_measured` está `null` no relatório. O alvo definido é `50 ms`. |
| Fração de CPU em rede/serialização      | **Não medido.** Campo `network_serialization_cpu_fraction` permanece `null`. |
| Run de shutdown/rejoin com quatro nós   | **Não executado.** Nenhuma captura de cenário de falha foi realizada. |
| Ganho SIMD medido em produção           | **Não capturado.** `simd_gain_target` definido como `3.0x`; valor real depende do data plane ativo. |

O harness está pronto para ser ligado ao data plane assim que a execução
distribuída real estiver disponível. Nenhum resultado foi fabricado.

---

## Como executar

### Testes de stress (corretude concorrente)

```sh
# Stress test de deformação concorrente
cargo test -p kineplex-core --test geometry_deformation_stress

# Stress test de substituição de rotas
cargo test -p kineplex-core --test geometry_stress

# Validação de alocações no hot path
cargo test -p kineplex-core --test geometry_allocations
```

### Benchmarks Criterion (latências absolutas e ganho SIMD)

```sh
cargo bench -p kineplex-core --bench geometry_stability
```

Os resultados HTML ficam em `target/criterion/geometry_stability/report/index.html`.

### Harness E2E completo (requer Docker Compose)

```sh
cd bench/e2e
pip install -r requirements.txt
rustup target add wasm32-unknown-unknown
python3 run_e2e.py
```

Parâmetros opcionais do runner:

| Flag              | Padrão            | Descrição                                      |
|-------------------|-------------------|------------------------------------------------|
| `--size-gib`      | `10.0`            | Tamanho do dataset Parquet gerado              |
| `--seed`          | `127.0.0.1:8000`  | Endereço do nó seed para submissão de job      |
| `--result-timeout`| `180`             | Segundos de espera pelo output Parquet         |

**Exit codes do runner:**

| Código | Significado                                                    |
|--------|----------------------------------------------------------------|
| `0`    | Sucesso — checksum terminal validado                           |
| `3`    | Data plane não conectado — resultado Parquet não produzido     |
| outros | Erro inesperado (falha de rede, erro de compilação Wasm, etc.) |
