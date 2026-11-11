# Research & Architectural Decisions: Riemannian Cascades, Lakehouse Commit & SIMD Dispatcher (FT-100)

## 1. Contexto e Objetivos Técnicos
A especificação FT-100 consolida a camada analítica de ponta a ponta do KinePlex, resolvendo três problemas centrais em motores de dados distribuídos:
1. Erro exponencial na estimativa de custo de junções complexas sob *data skew* severo;
2. Conflitos de concorrência e desperdício de I/O no commit simultâneo em catálogos abertos (Apache Iceberg e Delta Lake);
3. Dependência de compilação estática para otimizações vetorizadas avançadas (AVX-512 vs AVX2 vs Graviton NEON).

---

## 2. Decisões Arquiteturais e Análise de Trade-offs

### Decisão 1: Curvatura de Ollivier-Ricci no Grafo MEMO vs Estimativas de Histogramas Unidimensionais
- **Decisão**: Utilizar a Curvatura de Ollivier-Ricci $\kappa(x, y) = 1 - \frac{W_1(m_x, m_y)}{d_G(x, y)}$ baseada no transporte ótimo de Wasserstein $W_1$ entre partições para guiar a substituição física no CBO.
- **Racional**:
  - Motores tradicionais (Catalyst do Spark, Calcite, Orca) assumem independência estatística ($P(A \land B) = P(A)P(B)$), falhando catastroficamente em dados financeiros correlacionados e enviesados (*skew*).
  - A Curvatura de Ollivier-Ricci mede diretamente a dispersão topológica: $\kappa > 0.15$ indica sobreposição compacta em $L^p$ (viabilizando merge join co-localizado com zero rede); $\kappa < -0.15$ indica divergência hiperbólica (árvore com nós concentradores/hubs), exigindo broadcast de hubs e particionamento especial (*Hyperbolic Skew Split*).
  - O tensor de custo geodésico aplica a penalidade $\exp(-\bar{\kappa})$, podando ramos hiperbólicos com enforcers caros no início da busca Bellman-Ford.
- **Alternativas Rejeitadas**:
  - *Histogramas unidimensionais com Dynamic Partition Pruning*: Não capturam correlações multivariadas não-lineares.
  - *Hipercubos estatísticos exatos*: Consomem memória exponencial e tornam o planejamento lento ($> 1\text{ s}$). A curvatura de Wasserstein é avaliada em $< 50\text{ ns}$ aproveitando as assinaturas métricas do índice `.khvp`.

### Decisão 2: Monoide Abeliano Livre e Teorema do Diamante SSI vs Locks e Retries de Dados
- **Decisão**: Modelar o ciclo de escrita como um elemento do Monoide Abeliano Livre $(\mathcal{M}, \oplus, \mathbf{0})$ e resolver concorrência de commit via Teorema do Diamante:
  $$\mathcal{R}(T_{\text{own}}) \cap \mathcal{W}(T_{\text{ext}}) = \emptyset \implies T_{\text{own}} \circ T_{\text{ext}} \equiv T_{\text{ext}} \circ T_{\text{own}}$$
- **Racional**:
  - Em sistemas lakehouse convencionais, conflitos de escrita concorrente geram `ConcurrentModificationException`, forçando re-escrita integral dos dados ou locks distribuídos pessimistas via DynamoDB/ZooKeeper.
  - Como o KinePlex já mantém os limites hiper-retangulares $[\vec{\ell}_k, \vec{u}_k]$ e estatísticas dos arquivos Parquet de 128 MB em RAM, a validação de interseção geométrica com transações concorrentes é executada em $\mathcal{O}(\log K)$ na VP-Tree.
  - Se a interseção for vazia, as ações comutam sem conflito semântico e o commit é re-baseado em $< 2\text{ ms}$ em ambos os catálogos (Iceberg v2 e Delta Lake) a partir do mesmo estado em memória.
- **Alternativas Rejeitadas**:
  - *Duplicação de motores de gravação*: Gravar separadamente em Iceberg e depois em Delta Lake dobraria o custo de rede e I/O de storage. O funtor duplo $\Psi_{\text{Iceberg}}$ e $\Psi_{\text{Delta}}$ elimina qualquer redundância.
  - *Two-Phase Locking (2PL)*: Introduz contenção severa e degrada a latência de streaming contínuo.

### Decisão 3: Álgebra de Cauchy em $\mathbb{F}_2[x]$ e Dispatch Dinâmico de 4 Tiers em Runtime
- **Decisão**: Implementar a multiplicação polinomial sem transporte $Q(x) \otimes_{\mathbb{F}_2} \texttt{0xFFFF\_FFFF\_FFFF\_FFFF} \bmod x^{64}$ com resolução de microarquitetura em tempo de execução via *runtime feature detection*.
- **Racional**:
  - Compilar binários separados por arquitetura (`target-cpu=native`) inviabiliza contêineres portáveis para nuvem heterogênea (ex.: instâncias AWS EC2 Intel Ice Lake com AVX-512 vs instâncias AMD EPYC com AVX2 vs instâncias Graviton com ARM64).
  - A identidade algébrica no corpo finito $\mathbb{F}_2[x]$ permite mapear o cálculo de `QuoteMask` em 1 instrução vetorial em todas as plataformas modernas:
    - x86 AVX-512: `_mm_clmulepi64_si128` + máscaras `__mmask64` ($> 12\text{ GB/s}$).
    - x86 AVX2: `_mm256_movemask_epi8` + `_mm_clmulepi64_si128` ($> 6\text{ GB/s}$).
    - ARM64: `vmull_p64` da extensão Crypto/PMULL ($> 8\text{ GB/s}$).
    - Fallback Universal: Árvore logarítmica de 6 shifts em `u64` ($> 2.5\text{ GB/s}$).
  - A detecção dinâmica no boot do nó elimina falhas de instrução (*SIGILL*) mantendo o máximo desempenho possível do silício subjacente.
- **Alternativas Rejeitadas**:
  - *Máquinas de estado escalares com condicionais*: Perdem até 30% dos ciclos de clock por branch mispredictions.
  - *JIT dinâmico em tempo de execução*: Overhead inaceitável de compilação em warm-up de workers efêmeros.
