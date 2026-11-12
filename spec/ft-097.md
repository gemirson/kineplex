# Feature Specification: Verificação e Aplicação das Correções do Distribuidor Ótimo de Laguerre-Kantorovich (MK-EMD Anti-Small-Files)

**Feature Branch**: `001-laguerre-kantorovich-fixes`  
**Feature ID**: FT-097 / SPEC-001  
**Created**: 2026-11-10  
**Status**: Ready for Verification / Implementation Plan  
**Target Crates**: `kineplex-core`, `kineplex-df`  
**Author**: Antigravity Pair Programmer / KinePlex Team  

---

## 1. Visão Geral e Fundamentação Matemática *(Context & Problem Statement)*

O problema da proliferação de pequenos arquivos (*Small Files Problem*) em arquiteturas Lakehouse (Apache Iceberg, Parquet) degrada severamente o desempenho de metadados, amplifica chamadas à API de armazenamento em nuvem (S3/GCS/Azure) e anula os ganhos de *Data Skipping* baseados em estatísticas de colunas (mínimo e máximo).

Esta especificação define o framework estrito de verificação e aplicação das correções do **Distribuidor Ótimo de Laguerre-Kantorovich**, modelando o particionamento de lotes Arrow `RecordBatch` como um **Problema de Transporte Ótimo Semi-Discreto de Monge-Kantorovich** acoplado a Tesselagens Centroidais de Voronoi com Restrição de Capacidade (CCVT / Diagramas de Potência de Laguerre).

A formulação matemática implementa três teoremas centrais:
1. **Teorema 1 (Estimador Entrópico de Markov-ZSTD $H(X_t | X_{t-1})$)**: Predição em $\mathcal{O}(1)$ da massa física pós-compressão via taxa de entropia condicional de Markov combinada à detecção em anel de sequências repetidas (RLE survival rate).
2. **Teorema 2 (Passo Log-Sinkhorn com Projeção no Hiperplano de Soma Zero $\sum \psi_k = 0$)**: Estabilização e convergência quasi-Newton do potencial dual $\psi_k$ garantindo unicidade topológica e impedindo divergência monotônica dos potenciais.
3. **Teorema 3 (Coalescência Geodésica Terminal de Ward-Steiner com Regularização de Bounding Box Iceberg)**: Eliminação total de resíduos sub-críticos ($< M_{\min}$) através da fusão no vizinho super-crítico ótimo que minimiza o incremento de variância de dois corpos e penaliza a dilatação do hiper-retângulo envolvente (*Bounding Box*).

---

## 2. Assinatura de Tipos e Contrato Formal da API

A especificação exige a aderência aos seguintes tipos públicos e assinaturas formais:

```rust
use arrow_array::{Float64Array, RecordBatch};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use std::sync::Arc;
use uuid::Uuid;

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

/// Célula de Laguerre-Voronoi com rastreamento de Caixa Envolvente (Hyper-Rectangle Bounding Box)
pub struct SemiDiscreteLaguerreCell {
    pub cell_id: usize,
    pub centroid: [f64; 4],            // Baricentro c_k \in R^4
    pub bbox_min: [f64; 4],            // Limite inferior para cálculo de volume morto no Iceberg
    pub bbox_max: [f64; 4],            // Limite superior para cálculo de volume morto no Iceberg
    pub kantorovich_psi: f64,          // Potencial dual adimensionalizado \psi_k (\sum \psi_k = 0)
    pub current_mass_bytes: u64,       // Massa comprimida acumulada M_k
    pub staged_arena: Vec<RecordBatch>,// Retenção em RAM enquanto M_k < M_min
    pub s3_multipart_sink: Option<(String, AsyncArrowWriter<BufWriter>)>,
}

/// Motor de Distribuição Equi-Massiva de Monge-Kantorovich
pub struct LaguerreOptimalDistributor {
    store: Arc<dyn ObjectStore>,
    s3_bucket: String,
    base_prefix: String,
    config: KantorovichDistributionConfig,
    cells: Vec<SemiDiscreteLaguerreCell>,
    running_mean_sig: [f64; 4],
    running_scale_norm_p: f64,
    total_batches_observed: u64,
    sealed_datafiles: Vec<(String, ParquetMetaData)>,
}
```

---

## 3. Cenários de Uso e User Stories Priorizadas *(User Scenarios)*

### User Story 1 - Predição Entrópica de Markov em $\mathcal{O}(1)$ e Adimensionalização de Welford (Priority: P1)
**Como** o motor de execução colunar do KinePlex,  
**Quero** estimar com acurácia a massa ZSTD final de um `RecordBatch` antes de qualquer gravação ou compressão,  
**Para que** o roteamento métrico e os potenciais duais reflitam a massa física real no disco, eliminando o custo de serialização especulativa.

- **Por que esta prioridade**: Sem predição exata da massa compactada, o particionador subestima ou superestima lotes, rompendo o equilíbrio equi-massivo e a barreira de 64 MB.
- **Teste Independente**: Invocar `LaguerreOptimalDistributor::estimate_markov_zstd_mass` sobre lotes com repetições RLE, dados puramente aleatórios e dados contínuos, verificando convergência da razão prevista com a física.
- **Cenários de Aceite**:
  1. **Dado** um lote com valores numéricos repetidos (RLE puro), **Quando** `estimate_markov_zstd_mass` é executado, **Então** a razão estimada deve se aproximar do piso estrito $0.05 \times \text{raw\_bytes}$.
  2. **Dado** um lote uniforme de alta entropia (ruído gaussiano descorrelacionado), **Quando** avaliado, **Então** a taxa condicional $H(X_t | X_{t-1})$ aproxima-se de 8 bits e a razão prevista deve atingir o teto de 1.0.
  3. **Dado** um fluxo contínuo de assinaturas no espaço de Banach, **Quando** `update_manifold_scale` é invocado, **Então** a variância online de Welford normaliza $\sigma_X^p$ sem transbordamento numérico, mantendo $\sigma_X^p \ge 10^{-6}$.

---

### User Story 2 - Roteamento Adimensionalizado, Log-Sinkhorn e Lazy S3 Promotion (Priority: P1)
**Como** o pipeline analítico de ingestão para Lakehouse,  
**Quero** que os lotes sejam alocados na célula de menor custo de Laguerre ($c(x, c_k) - \psi_k$), retidos em RAM até cruzarem 64 MB e abertos assincronamente em S3 com *Multipart Upload*,  
**Para que** nenhum arquivo físico seja criado prematuramente com tamanho inferior a 64 MB.

- **Por que esta prioridade**: Garante a propriedade *Anti-Small-Files* e preserva recursos de I/O na nuvem através do writer concorrente `AsyncArrowWriter<BufWriter>`.
- **Teste Independente**: Enviar batches sucessivos totalizando menos de 64 MB em uma célula e validar que `s3_multipart_sink` permanece `None`, com batches residentes em `staged_arena`. Ao cruzar 64 MB, verificar transição automática para multipart writer ativo.
- **Cenários de Aceite**:
  1. **Dado** uma célula com massa acumulada $M_k < 64\text{ MB}$, **Quando** novos lotes são roteados, **Então** os batches são acumulados na arena de memória sem emitir chamadas de upload ao `ObjectStore`.
  2. **Dado** o acúmulo ultrapassando $M_k \ge 64\text{ MB}$, **Quando** o próximo lote é processado, **Então** o writer `AsyncArrowWriter` é inicializado em buffer de 16 MB com 8 concorrentes e descarrega a arena em lote.
  3. **Dado** o avanço de lotes, **Quando** `update_sinkhorn_kantorovich_potentials` é executado, **Então** a soma dos potenciais duais projeta-se no hiperplano de soma zero ($\sum_{k=1}^K \psi_k = 0$) com tolerância numérica $\le 10^{-12}$.
  4. **Dado** uma célula que atinge a meta $M_k \ge M^* = 128\text{ MB}$, **Quando** selada, **Então** o arquivo Parquet é fechado assincronamente, seu metadado é registrado em `sealed_datafiles` e os estados locais são reinicializados.

---

### User Story 3 - Coalescência Terminal de Ward-Steiner e Fechamento Zero-Resíduo (Priority: P2)
**Como** o orquestrador de encerramento do job analítico,  
**Quero** que todas as células órfãs residuais ($M_k < 64\text{ MB}$) sofram colapso topológico no fechamento do job,  
**Para que** sejam absorvidas pelas células super-críticas que minimizem a distância geodésica e a dilatação da Bounding Box do Iceberg, ou consolidadas em um único arquivo no caso de micro-jobs.

- **Por que esta prioridade**: Elimina completamente resíduos na fronteira de encerramento do stream (*Zero-Residue Boundary*), impedindo vazamento de micro-arquivos ao término de queries.
- **Teste Independente**: Executar jobs com massas residuais variadas e verificar que o número de arquivos finais gerados é estritamente ótimo e nenhum arquivo possui tamanho $< 64\text{ MB}$ (exceto quando o volume total de todo o job for inferior a 64 MB, hipótese na qual exatamente 1 único arquivo coeso é emitido).
- **Cenários de Aceite**:
  1. **Dado** duas células ativas (uma com 80 MB e writer no S3, outra órfã com 20 MB em RAM), **Quando** `finalize_optimal_distribution` é chamado, **Então** a célula órfã é absorvida pela ativa se $80 + 20 \le 256\text{ MB}$, resultando em 1 arquivo final de 100 MB.
  2. **Dado** um job diminuto totalizando 15 MB divididos em 3 células órfãs, **Quando** finalizado, **Então** todos os lotes são unificados em `consolidated_ward/kinehaus-coalesced-{uuid}.parquet` emitindo exatamente 1 arquivo no catálogo.
  3. **Dado** a escolha entre múltiplos vizinhos super-críticos, **Quando** calculada a métrica de Ward-Steiner com penalidade $\lambda_{\text{hull}}$, **Então** é selecionada a célula que minimiza o acréscimo de volume morto das caixas delimitadoras $[min, max]$.

---

## 4. Requisitos Funcionais *(Functional Requirements)*

- **FR-001**: O sistema DEVE fornecer a struct `KantorovichDistributionConfig` com valores padronizados: $M^* = 128\text{ MB}$, $M_{\min} = 64\text{ MB}$, $M_{\max} = 256\text{ MB}$, $p = 2.0$, $\varepsilon = 0.25$ e $\lambda_{\text{hull}} = 0.50$.
- **FR-002**: O método `estimate_markov_zstd_mass` DEVE analisar os primeiros 4096 bytes da primeira coluna numérica (`Float64Array`) utilizando tabela hash de 1024 posições para transições bijetivas de bigramas: `(((prev as usize) * 31) ^ (curr as usize)) & 1023`.
- **FR-003**: O método `estimate_markov_zstd_mass` DEVE extrair a taxa de entropia condicional $H(X_t | X_{t-1}) = H(X_t, X_{t-1}) - H(X_t)$ e aplicar o fator de sobrevivência RLE `1.0 - (runs * inv_n1 * 0.85)`.
- **FR-004**: Quando o lote não possuir `Float64Array` ou tiver tamanho insuficiente, o estimador DEVE retornar `(raw_bytes as u64) / 4` como fallback determinístico sadio.
- **FR-005**: A função `update_manifold_scale` DEVE atualizar online a média móvel de Welford para as 4 dimensões da assinatura e o desvio na norma $L^p$: $\sigma_X^p \leftarrow 0.95 \cdot \sigma_X^p + 0.05 \cdot \Delta^p$, limitado inferiormente por $10^{-6}$.
- **FR-006**: O cálculo de custo de Laguerre `dimensionless_laguerre_cost` DEVE computar rigorosamente $\frac{\sum_{d=0}^3 |x_d - c_d|^p}{\sigma_X^p} - \psi_k$.
- **FR-007**: O método `update_sinkhorn_kantorovich_potentials` DEVE aplicar atualização multiplicativa logarítmica $\psi_k \leftarrow \psi_k + \varepsilon \ln(M^* / M_k)$ com piso de massa de $\max(M_{\min} / 8, 1\text{ MB})$.
- **FR-008**: O método `update_sinkhorn_kantorovich_potentials` DEVE projetar os potenciais no subespaço ortogonal $\sum_{k=1}^K \psi_k = 0$ subtraindo a média $\bar{\psi} = \frac{1}{K}\sum \psi_k$.
- **FR-009**: O método `route_and_distribute` DEVE atribuir o lote à célula $k^* = \arg\min_k (c(x, c_k) - \psi_k)$ e atualizar o baricentro de Banach de acordo com as massas anterior e nova.
- **FR-010**: O método `route_and_distribute` DEVE manter o lote em `staged_arena` enquanto $M_k < M_{\min}$ ($64\text{ MB}$).
- **FR-011**: Ao cruzar o limiar $M_k \ge 64\text{ MB}$, o sistema DEVE abrir um stream assíncrono via `AsyncArrowWriter` apontando para o `ObjectStore` com `BufWriter` configurado para 16 MB e concorrência 8.
- **FR-012**: As propriedades do Parquet Writer DEVEM configurar Parquet 2.0, compressão ZSTD Level 3, Page Statistics habilitadas, Bloom Filter habilitado e tamanho máximo de row group de 1.048.576 linhas.
- **FR-013**: Ao cruzar $M_k \ge M^* = 128\text{ MB}$, o writer DEVE ser finalizado assincronamente (`writer.close().await`), registrando `(uri, ParquetMetaData)` em `sealed_datafiles` e reinicializando a célula.
- **FR-014**: O método `finalize_optimal_distribution` DEVE fundir células órfãs nas células ativas cujo custo de fusão de Ward-Steiner $(\mu_{kj} / \sigma_X^p) \|c_k - c_j\|_p^p + \lambda_{\text{hull}} \Delta_{\text{bbox}}$ seja mínimo, respeitando o teto rígido $M_{\max} = 256\text{ MB}$.
- **FR-015**: Se todas as células forem órfãs, `finalize_optimal_distribution` DEVE consolidar todos os lotes residuais em um único arquivo Parquet sob o prefixo `consolidated_ward/`.

---

## 5. Requisitos Não-Funcionais *(Non-Functional Requirements)*

- **NFR-001 (Latência de Roteamento)**: A execução de `estimate_markov_zstd_mass` e atribuição métrica de Laguerre DEVE ser completada em tempo $\le 5\,\mu\text{s}$ por lote de até 65.536 registros.
- **NFR-002 (Zero Alocação na Heap no Estimador)**: O cálculo do histograma unigrama (256 elementos) e bigrama de Markov dobrado (1024 elementos) DEVE utilizar buffers fixos alocados na pilha (`[0u32; 256]` e `[0u32; 1024]`).
- **NFR-003 (Estabilidade Numérica do Log-Sinkhorn)**: O desvio da soma dos potenciais duais em relação a zero ($\left|\sum \psi_k\right|$) DEVE ser menor que $10^{-12}$ em qualquer número arbitrário de iterações.
- **NFR-004 (Garantia Estrita Anti-Small-Files)**: Sob qualquer distribuição estocástica de entrada, o sistema JAMAIS deve emitir mais de 1 arquivo com tamanho inferior a $M_{\min} = 64\text{ MB}$ por job.
- **NFR-005 (Concorrência e Assincronismo)**: As operações de I/O de escrita e selagem DEVEM ser `async` nativas, integradas com o runtime `tokio` 1.35+.

---

## 6. Casos de Borda e Resiliência *(Edge Cases)*

1. **Lote Vazio (`raw_bytes == 0`)**: `estimate_markov_zstd_mass` retorna imediatamente 0 sem alocações ou divisões por zero.
2. **Coluna de Dados sem `Float64Array`**: O estimador reverte com segurança para o fallback conservador `raw_bytes / 4`.
3. **Array com Amostra $\le 16$ bytes**: Reverte imediatamente para `raw_bytes / 4`, evitando subdimensionamento estatístico do unigrama.
4. **Vetor de Sementes Iniciais Vazio (`initial_seeds.is_empty()`)**: O construtor inicializa ao menos 1 célula canônica com baricentro `[0.0, 0.0, 0.0, 0.0]`.
5. **Todas as Células Saturadas ($> M_{\max}$)**: O particionador veda novas absorções na coalescência de Ward que violem $M_{\max}$, forçando o escoamento para consolidação segura.
6. **Desvio de Escala Inicial Nulo ($\sigma_X^p \to 0$)**: O clamp `.max(1e-6)` impede divisões por zero ou singularidades nos custos normalizados.

---

## 7. Matriz de Verificação e Critérios de Aceitação *(Definition of Done)*

| ID | Critério de Aceitação (AC) | Método de Validação |
|---|---|---|
| **AC-001** | `estimate_markov_zstd_mass` atinge razão $0.05 \pm 0.01$ para repetições uniformes RLE | Teste unitário em `tests/laguerre_distributor_tests.rs` |
| **AC-002** | `estimate_markov_zstd_mass` atinge razão $\ge 0.85$ para dados de alta entropia | Teste unitário sintético |
| **AC-003** | Adimensionalização de Welford estabiliza $\sigma_X^p$ em 4D sem NaN/Inf | Teste de estresse com 10.000 batches |
| **AC-004** | Soma dos potenciais $\sum \psi_k$ permanece estritamente nula ($\pm 10^{-12}$) | Asserção após cada chamada a `update_sinkhorn_kantorovich_potentials` |
| **AC-005** | Nenhum upload S3 é acionado enquanto a massa acumulada for $< 64\text{ MB}$ | Mock de `ObjectStore` verificando ausência de `put_multipart` |
| **AC-006** | Abertura automática de `AsyncArrowWriter` ao cruzar $64\text{ MB}$ | Verificação de estado `s3_multipart_sink.is_some()` |
| **AC-007** | Selagem automática e reinicialização da célula ao atingir $128\text{ MB}$ | Arquivo adicionado a `sealed_datafiles` e `current_mass_bytes == 0` |
| **AC-008** | Absorção por Ward-Steiner com penalidade $\lambda_{\text{hull}}$ de células residuais | Teste com 1 célula super-crítica e 1 sub-crítica |
| **AC-009** | Consolidação em arquivo único para batches residuais sem vizinho viável | Execução de job com total $< 64\text{ MB}$ gerando exatamente 1 arquivo |
| **AC-010** | Zero regressões no workspace KinePlex (`cargo test --workspace`) | Cobertura global mantida $\ge 95\%$ |
