# Interface Contract: Laguerre-Kantorovich Optimal Distributor API

**Contract ID**: `KINEPLEX-MK-001`  
**Crate**: `kineplex-core`  
**Module**: `laguerre`  

---

## 1. Assinatura Pública do Distribuidor

```rust
pub struct LaguerreOptimalDistributor {
    // Campos privados
}

impl LaguerreOptimalDistributor {
    /// Constrói uma nova instância do distribuidor com o store, bucket S3, prefixo,
    /// baricentros iniciais e hiperparâmetros de Monge-Kantorovich.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        s3_bucket: String,
        base_prefix: String,
        initial_seeds: Vec<[f64; 4]>,
        config: KantorovichDistributionConfig,
    ) -> Self;

    /// TEOREMA 1: Estima a massa física comprimida ZSTD analisando a entropia condicional
    /// de Markov de 1ª ordem H(X_t | X_{t-1}) acoplada à taxa de sobrevivência RLE.
    /// Complexidade: O(min(N, 4096)) tempo, O(1) espaço (pilha).
    #[inline(always)]
    pub fn estimate_markov_zstd_mass(batch: &RecordBatch) -> u64;

    /// Roteia o lote pela célula de Laguerre de menor custo adimensionalizado,
    /// atualiza o baricentro de Banach e aplica promoção preguiçosa para S3.
    pub async fn route_and_distribute(
        &mut self,
        batch: RecordBatch,
        lp_signature: [f64; 4],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// TEOREMA 3: Coalescência Geodésica Terminal de Ward-Steiner com penalidade
    /// contra dilatação da Bounding Box do Iceberg e selagem de todos os writers.
    pub async fn finalize_optimal_distribution(
        mut self,
    ) -> Result<Vec<(String, ParquetMetaData)>, Box<dyn std::error::Error + Send + Sync>>;
}
```

---

## 2. Contratos de Pré e Pós-Condições

### `estimate_markov_zstd_mass(batch: &RecordBatch) -> u64`
- **Pré-condições**:
  - `batch` pode ser vazio ou conter colunas arbitrárias.
- **Pós-condições**:
  - Retorna valor $\le \text{batch.get\_array\_memory\_size()}$.
  - Se a primeira coluna for `Float64Array`, a estimativa varia entre $0.05 \times \text{raw}$ e $1.0 \times \text{raw}$.
  - Se não for `Float64Array` ou tamanho insuficiente ($\le 16$ bytes), retorna exatamente $\frac{\text{raw}}{4}$.

### `route_and_distribute(batch: RecordBatch, lp_signature: [f64; 4])`
- **Pré-condições**:
  - `lp_signature` contém valores reais válidos (não-NaN).
- **Pós-condições**:
  - A célula vencedora $k^*$ tem sua massa incrementada: $M_{k^*} \leftarrow M_{k^*} + m_i$.
  - Se $M_{k^*} \ge 64\text{ MB}$ e o writer estava inativo, abre `AsyncArrowWriter` e descarrega a arena.
  - Se $M_{k^*} \ge 128\text{ MB}$, o writer é fechado assincronamente e registrado em `sealed_datafiles`.
  - A soma dos potenciais duais $\sum_{k=1}^K \psi_k = 0$ é preservada.

### `finalize_optimal_distribution(self) -> Result<Vec<(String, ParquetMetaData)>, ...>`
- **Pré-condições**:
  - Invocado apenas no encerramento do processamento do fluxo de dados.
- **Pós-condições**:
  - Nenhuma célula órfã permanece pendente em RAM (`staged_arena` vazias).
  - Todos os arquivos retornados em `sealed_datafiles` possuem tamanho $\ge 64\text{ MB}$, a menos que o volume total do job seja $< 64\text{ MB}$, caso em que exatamente 1 arquivo é retornado.
