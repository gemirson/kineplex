# Feature Specification: Otimizador Cascades Riemanniano, Commit Unificado Lakehouse e Dispatch Dinâmico em $\mathbb{F}_2[x]$ (FT-100)

**Feature Branch**: `004-cascades-lakehouse-simd`  
**Created**: 2026-11-11  
**Status**: Complete  
**Input**: Especificar a tríade analítica fundamental do KinePlex composta por: (1) Otimizador Lógico/Físico Cascades CBO governado por Curvatura de Ollivier-Ricci no espaço de planos MEMO, (2) Protocolo de Commit Transacional Unificado Dual para Apache Iceberg v2 e Delta Lake via Álgebra de Monoides e isolamento serializável comutativo (SSI) sem re-execução de dados, e (3) Motor de Dispatch Dinâmico SIMD em $\mathbb{F}_2[x]$ adaptativo por microarquitetura (AVX-512 VPCLMULQDQ, AVX2, ARM NEON PMULL e SWAR).

---

## 1. Visão Geral e Fundamentação Algébrico-Geométrica

O KinePlex opera como um motor de processamento distribuído zero-copy onde o hardware físico, o fluxo de dados em rede e a álgebra de armazenamento convergem em um único pipeline sem atrito. A especificação FT-100 estabelece a formalização matemática e microarquitetural de três eixos que sustentam a inteligência analítica do sistema:

```
+-------------------------------------------------------------------------------------------------+
|                                    KINEPLEX CORE PIPELINE (FT-100)                              |
|                                                                                                 |
|  [ Eixo 1: Riemannian Cascades CBO ]                                                            |
|  - Grafo Ponderado Discreto G = (V, E, \mu)                                                     |
|  - Curvatura de Ollivier-Ricci: \kappa(x, y) = 1 - W_1(m_x, m_y) / d_G(x, y)                    |
|  - Seleção Física: Laguerre Merge (\kappa > 0.15) | SIMD Hash (\approx 0) | Skew Split (< -0.15)   |
|  - Poda Bellman-Ford com Custo Geodésico: J(e, P) com Enforcer Penalizado por exp(-\kappa)     |
|                                    │                                                            |
|                                    ▼                                                            |
|  [ Eixo 3: Dispatcher SIMD Dinâmico em GF(2)[x] ]                                               |
|  - Produto Polinomial sem Transporte: Q(x) (x) 0xFFFF_FFFF_FFFF_FFFF mod x^64 = P(x)            |
|  - Tier 1: AVX-512BW + VPCLMULQDQ (_mm512_cmpeq_epi8_mask + _mm_clmulepi64_si128) > 12 GB/s     |
|  - Tier 2: AVX2 + PCLMULQDQ (_mm256_movemask_epi8 + pack_u64)                                   |
|  - Tier 3: ARM64 Graviton NEON + PMULL (vmull_p64)                                              |
|  - Tier 4: Fallback Universal SWAR (6 shifts logarítmicos em u64)                               |
|                                    │                                                            |
|                                    ▼                                                            |
|  [ Eixo 2: Monoide Unificado de Commit Lakehouse (Iceberg & Delta) ]                            |
|  - Monoide Abeliano Livre (M, (+), 0) de Ações Atômicas Add(uri, m_k, N, l_k, u_k, \nu_k)       |
|  - Projeção Iceberg v2: \Psi_Iceberg(M) -> ManifestTree Avro + Reticulado Completo [L_c, U_c]  |
|  - Projeção Delta Lake: \Psi_Delta(M) -> _delta_log JSON com PutIfAbsent condicional            |
|  - Resolução de Conflitos SSI com Teorema do Diamante: R(T_own) \cap W(T_ext) = \emptyset      |
|    Commit Concorrente Comutativo em < 2 ms sem regravação de Parquet                           |
+-------------------------------------------------------------------------------------------------+
```

---

## 2. Eixo 1: Matemática do Otimizador Lógico/Físico (Riemannian Cascades CBO)

### 2.1 Diagnóstico das Falhas em Otimizadores Convencionais
Em otimizadores estilo Volcano/Cascades (como Apache Spark Catalyst, Apache Calcite e Orca), o modelo de custo apoia-se em histogramas unidimensionais com a hipótese ingênua de independência estatística:
$$P(A \land B) = P(A) \cdot P(B)$$
Em cargas financeiras reais com forte correlação não-linear e *data skew* severo entre chaves de junção, o erro de estimativa de cardinalidade acumula-se exponencialmente ($\mathcal{O}(2^k)$ em $k$ junções), resultando em seleções desastrosas de planos físicos (ex.: *Broadcast Joins* de tabelas massivas estourando memória RAM ou *Shuffle Hash Joins* com partições de centenas de gigabytes colidindo em um único nó).

### 2.2 Curvatura de Ollivier-Ricci como Preditor de Estrangulamento e Skew
O KinePlex trata o espaço de exploração de planos MEMO como um **Grafo Ponderado Riemanniano Discreto** $G = (V, E, \mu)$. Para dois conjuntos de dados ou partições $x, y \in V$ conectados por uma relação de junção ou aresta de transição $(x, y) \in E$, associamos medidas de probabilidade locais $m_x, m_y \in \mathcal{P}(V)$ induzidas pelas assinaturas métricas de norma $L^p$ registradas no índice `.khvp`.

A **Curvatura de Ollivier-Ricci** ao longo da aresta $(x, y)$ quantifica a convergência geodésica das vizinhanças via distância de transporte ótimo de Wasserstein $W_1(m_x, m_y)$ em relação à distância métrica no grafo $d_G(x, y)$:
$$\kappa(x, y) := 1 - \frac{W_1(m_x, m_y)}{d_G(x, y)}$$

A topologia da curvatura dita deterministicamente a regra de substituição física exata:

1. **Curvatura Positiva ($\kappa(x, y) > 0.15 \implies W_1 < d_G$) — Cluster Esférico / Co-Localizado**:
   - As distribuições de $x$ e $y$ possuem alta sobreposição espacial e co-localidade métrica.
   - **Ação do Otimizador**: Seleciona `LaguerreCoLocatedMergeJoin` sem rede e sem re-particionamento, operando diretamente nas células de potência já alinhadas pelo `LaguerreOptimalDistributor`.

2. **Curvatura Nula ($|\kappa(x, y)| \le 0.15 \implies W_1 \approx d_G$) — Espaço Euclidiano Plano**:
   - Distribuição uniforme sem gargalos e sem correlações anômalas.
   - **Ação do Otimizador**: Aplica o `SimdPartitionedHashJoin` padrão vetorizado via AVX-512.

3. **Curvatura Negativa ($\kappa(x, y) < -0.15 \implies W_1 > d_G$) — Geometria Hiperbólica / Gargalo em Árvore**:
   - Indica divergência exponencial de arestas, caracterizando um *hub* topológico ou *heavy-tail skew* extremo (ex.: um único custodiante ou ativo concentra milhões de operações enquanto os demais possuem volume insignificante).
   - **Ação do Otimizador**: Veta terminantemente o *Shuffle Join* tradicional e dispara a regra de transformação `HyperbolicSkewBroadcastJoin`, isolando o *hub* em *broadcast* com *Quotient Coalesce* local.

### 2.3 O Funcional de Custo Geodésico no Grafo AND-OR (MEMO)
Para cada grupo $g \in \mathcal{G}$ do MEMO, o custo de um operador físico $e \in g$ com propriedades físicas exigidas $\mathcal{P} = (\Pi, \mathcal{O}, \mathcal{A})$ é calculado pelo Tensor de Custo:
$$\mathcal{J}(e, \mathcal{P}) = \sum_{j \in \{\text{CPU}, \text{IO}, \text{NET}\}} \omega_j \, \Phi_j(e) + \sum_{k=1}^{m} \Big[ \mathcal{J}^*(g_k, \mathcal{P}_k) + \Lambda_{\text{enforcer}}(\mathcal{P}_k \to \mathcal{P}) \cdot \exp\!\big(-\bar{\kappa}(g_k)\big) \Big]$$
Onde $\exp(-\bar{\kappa}(g_k))$ penaliza severamente transições de propriedades físicas sobre arestas com curvatura negativa. A equação de Bellman-Ford poda ramos cuja cota inferior satisfaça $\underline{\mathcal{J}}(e) \ge \mathcal{J}_{\text{best}}(g)$ (*Branch-and-Bound Pruning*).

---

## 3. Eixo 2: Matemática da Unificação Transacional (Apache Iceberg & Delta Lake)

### 3.1 Álgebra de Monoides de Estado
Ao concluir a ingestão e particionamento em arquivos Parquet de $128\text{ MB}$, o KinePlex possui as estruturas de metadados em RAM. Modelamos as transações como elementos de um **Monoide Abeliano Livre** $(\mathcal{M}, \oplus, \mathbf{0})$ sobre o alfabeto de ações atômicas:
$$a_k = \text{Add}\Big(\text{uri}_k, \; m_k, \; N_{\text{rows}}, \; \vec{\ell}_k, \; \vec{u}_k, \; \vec{\nu}_k, \; \Phi_{L^p}(k)\Big), \qquad r_j = \text{Remove}\big(\text{uri}_j, \; t_{\text{del}}\big)$$
Onde $\vec{\ell}_k, \vec{u}_k$ são os vetores de limites inferiores e superiores (*lower/upper bounds*) e $\vec{\nu}_k$ é a contagem de nulos por coluna.

### 3.2 Funtores de Projeção Homomórfica ($\Psi_{\text{Iceberg}}$ e $\Psi_{\text{Delta}}$)
A partir do mesmo estado em memória $T \in \mathcal{M}$, projetamos zero-copy para ambos os formatos abertos de lakehouse:
1. **Projeção Apache Iceberg ($\Psi_{\text{Iceberg}}: \mathcal{M} \to \text{ManifestTree}$)**:
   - Serializa as ações $\{a_1, \dots, a_K\}$ diretamente em um buffer Avro em memória (`ManifestFile`).
   - Computa o resumo de limites como o Supremo ($\bigvee$) e Ínfimo ($\bigwedge$) no Reticulado Completo (*Complete Lattice*) de cada coluna:
     $$L_{\text{manifest}}^{(c)} = \bigwedge_{k=1}^{K} \ell_k^{(c)}, \qquad U_{\text{manifest}}^{(c)} = \bigvee_{k=1}^{K} u_k^{(c)}$$
   - Atualiza a `ManifestList` e executa *Compare-And-Swap* (CAS) no ponteiro `metadata_location`.
2. **Projeção Delta Lake ($\Psi_{\text{Delta}}: \mathcal{M} \to \Delta\text{Log}$)**:
   - Converte cada $a_k$ na linha JSON canônica `AddFile { path, size, modificationTime, dataChange: true, stats: JSON(...) }`.
   - Grava atomicamente no S3 via `PutIfAbsent` condicional (`If-None-Match: *`) em `_delta_log/{version:020}.json`.

### 3.3 Resolução de Conflitos SSI por Comutatividade (Propriedade de Diamante)
Se uma transação externa concorrente $T_{\text{ext}}$ comitar o snapshot $S_{n+1}$ antes da transação local $T_{\text{own}}$, o KinePlex aplica o **Teorema de Isolamento Serializável Comutativo**:
$$\mathcal{R}(T_{\text{own}}) \cap \mathcal{W}(T_{\text{ext}}) = \emptyset \implies T_{\text{own}} \circ T_{\text{ext}} \equiv T_{\text{ext}} \circ T_{\text{own}}$$
Como os hiper-retângulos $[\vec{\ell}_f, \vec{u}_f]$ residem em RAM e estão indexados na VP-Tree métrica, a interseção geométrica é resolvida em tempo $\mathcal{O}(\log K)$. Sendo vazia, o commit é re-baseado e consolidado em $< 2\text{ ms}$, eliminando o reprocessamento de dados.

---

## 4. Eixo 3: Matemática do Dispatch Dinâmico SIMD em $\mathbb{F}_2[x]$

### 4.1 Identidade Algébrica da Multiplicação sem Transporte (Carry-Less)
Para processar delimitadores e aspas em arquivos de texto a taxas $\ge 10\text{ GB/s}$ por núcleo, modelamos o bloco de 64 bytes como um polinômio $Q(x) \in \mathbb{F}_2[x]$ de grau $\le 63$, onde $q_i = 1$ se o byte $i$ for `"` (0x22).

A máscara de aspas acumulada $P(x) = \sum_{i=0}^{63} p_i x^i$ corresponde à soma cumulativa módulo 2 (Prefix-XOR):
$$p_i = \bigoplus_{k=0}^{i} q_k \pmod 2$$
No corpo finito $\mathbb{F}_2[x]$, essa operação equivale à multiplicação polinomial sem transporte pelo elemento neutro de todos os bits ligados $U(x) = \texttt{0xFFFF\_FFFF\_FFFF\_FFFF}_{16}$:
$$P(x) = \Big( Q(x) \otimes_{\mathbb{F}_2} \texttt{0xFFFF\_FFFF\_FFFF\_FFFF} \Big) \bmod x^{64}$$
A propagação entre blocos de 64 bytes adjacentes é resolvida em 1 instrução:
$$P_{\text{global}} = P \oplus \text{prev\_in\_quote}, \qquad \text{prev\_in\_quote}_{\text{next}} = \big(\text{i64::from}(P_{\text{global}}) \gg 63\big) \text{ as u64}$$

### 4.2 Arquitetura de 4 Tiers de Dispatch Dinâmico

| Tier | Microarquitetura | Instruções Utilizadas | Vazão Esperada |
|---|---|---|---|
| **Tier 1** | x86_64 AVX-512BW + VPCLMULQDQ | `_mm512_loadu_si512`, `_mm512_cmpeq_epi8_mask`, `_mm_clmulepi64_si128` | $> 12\text{ GB/s}$ |
| **Tier 2** | x86_64 AVX2 + PCLMULQDQ | `_mm256_loadu_si256`, `_mm256_cmpeq_epi8`, `_mm256_movemask_epi8`, `_mm_clmulepi64_si128` | $> 6\text{ GB/s}$ |
| **Tier 3** | ARM64 AWS Graviton3/4 (NEON + Crypto) | `vmull_p64` sobre registradores `poly64_t` | $> 8\text{ GB/s}$ |
| **Tier 4** | Universal Fallback (SWAR de 64 bits) | Redução logarítmica de 6 shifts/XORs em `u64` | $> 2.5\text{ GB/s}$ |

---

## 5. Cenários de Usuário e Testes de Aceitação

### User Story 1 — Otimização CBO Guiada por Curvatura de Ollivier-Ricci (Priority: P1)
Como planejador distribuído de consultas do KinePlex, preciso selecionar operadores de junção físicos (Laguerre Co-Located Merge, SIMD Hash ou Hyperbolic Skew Split) com base na curvatura $\kappa(x, y)$ calculada a partir de assinaturas de Wasserstein $W_1$, impedindo degradações por skew de dados.

- **Acceptance Scenario 1.1**:
  - **Given** duas tabelas com alinhamento de Laguerre prévio e $\kappa(x, y) = 0.45 > 0.15$,
  - **When** avaliado pelo `KinePlexCascadesOptimizer::select_optimal_join_plan`,
  - **Then** o operador retornado DEVE ser `LaguerreCoLocatedMergeJoin` com enforcer nulo.
- **Acceptance Scenario 1.2**:
  - **Given** uma relação de junção com heavy skew e $\kappa(x, y) = -0.60 < -0.15$,
  - **When** avaliado pelo otimizador,
  - **Then** o plano DEVE selecionar `HyperbolicSkewBroadcastJoin` com custo inferior ao `SimdPartitionedHashJoin`.

### User Story 2 — Commit Transacional Unificado Dual Lakehouse (Priority: P2)
Como nó gravador de storage analítico, preciso registrar lotes Parquet de $128\text{ MB}$ simultaneamente no catálogo Apache Iceberg v2 e no log Delta Lake sem duplicação de I/O, resolvendo concorrência através da propriedade comutativa de diamante.

- **Acceptance Scenario 2.1**:
  - **Given** uma ação `UnifiedDataFileAction` com limites $[0.0, 100.0]$,
  - **When** serializada para Delta Lake via `to_delta_log_json_line`,
  - **Then** o JSON gerado DEVE conter o formato canônico de log com campos `numRecords`, `minValues`, `maxValues` e `nullCount`.
- **Acceptance Scenario 2.2**:
  - **Given** uma transação externa concorrente adicionando arquivos com limites $[200.0, 300.0]$ e uma leitura local em $[0.0, 100.0]$,
  - **When** executada a validação `can_commute_concurrent_commit`,
  - **Then** o retorno DEVE ser `true` permitindo o rebase atômico em $< 2\text{ ms}$ sem retentar a gravação dos dados.

### User Story 3 — Dispatcher SIMD Dinâmico em $\mathbb{F}_2[x]$ (Priority: P3)
Como motor de ingestão KineTxt, preciso selecionar em tempo de execução a melhor instrução de produto polinomial sem transporte disponível no silício da CPU (AVX-512, AVX2, ARM NEON ou SWAR) para extrair máscaras estruturais de 64 bytes.

- **Acceptance Scenario 3.1**:
  - **Given** um bloco de 64 bytes contendo aspas e delimitadores,
  - **When** processado por `compute_quote_mask_dispatch`,
  - **Then** os bits correspondentes a delimitadores dentro de aspas DEVEM ser anulados (`valid_delimiters_mask == 0`), preservando delimitadores externos.
- **Acceptance Scenario 3.2**:
  - **Given** uma CPU x86_64 com AVX-512 ou AVX2,
  - **When** executada a rotina de detecção em tempo de execução,
  - **Then** o dispatch dinâmico DEVE selecionar o Tier apropriado sem falhas de instrução (*SIGILL*).

---

## 6. Casos de Borda e Falhas Críticas

1. **Aresta com Distância Métrica Nula ou Quase Zero ($d_G(x, y) < 10^{-9}$)**:
   O cálculo de $\kappa$ deve aplicar `denom = metric_distance_dg.max(1e-9)` para evitar divisão por zero em partições idênticas.
2. **Conflito de Concorrência Real com Sobreposição Geodésica**:
   Se $\mathcal{R}(T_{\text{own}}) \cap \mathcal{W}(T_{\text{ext}}) \ne \emptyset$, `can_commute_concurrent_commit` deve retornar `false`, disparando o caminho seguro de re-execução controlada (SSI Abort).
3. **Blocos de Texto com Aspas Abertas na Fronteira de 64 Bytes**:
   O estado `next_prev_in_quote` deve carregar fielmente `0xFFFF_FFFF_FFFF_FFFF` para o bloco seguinte via shift aritmético `(quote_mask as i64) >> 63`.
4. **Hardware Heterogêneo sem Extensões Vetoriais**:
   O Tier 4 SWAR logarítmico deve garantir corretude bit-a-bit idêntica aos Tiers AVX/NEON em qualquer processador.

---

## 7. Requisitos do Sistema

- **FR-001**: O sistema DEVE fornecer o módulo `riemannian_cascades_cbo` contendo o enum `PhysicalJoinOperator` e a struct `GraphEdgeStatistics`.
- **FR-002**: O cálculo da curvatura de Ollivier-Ricci DEVE seguir estritamente $\kappa(x, y) = 1 - \frac{W_1(m_x, m_y)}{d_G(x, y)}$.
- **FR-003**: O otimizador DEVE selecionar `LaguerreCoLocatedMergeJoin` quando $\kappa > 0.15$ e o particionamento estiver alinhado.
- **FR-004**: O otimizador DEVE selecionar `HyperbolicSkewBroadcastJoin` quando $\kappa < -0.15$ e o custo for inferior ao hash join.
- **FR-005**: O tensor de custo geodésico DEVE aplicar o amplificador $(- \kappa).\exp().\text{clamp}(0.2, 20.0)$ sobre as transições de enforcers.
- **FR-006**: O sistema DEVE fornecer o módulo `unified_lakehouse_commit` contendo a struct `UnifiedDataFileAction`.
- **FR-007**: A struct `UnifiedDataFileAction` DEVE implementar a serialização JSON compatível com a especificação Delta Lake Protocol v2.
- **FR-008**: O sistema DEVE implementar o teste de interseção de hiper-retângulos multidimensionais para validação comutativa SSI.
- **FR-009**: O método `can_commute_concurrent_commit` DEVE validar a comutatividade de commits sem necessitar regravação de arquivos Parquet.
- **FR-010**: O sistema DEVE fornecer o módulo `simd_gf2_dispatcher` contendo a struct `StructuralMask64`.
- **FR-011**: O sistema DEVE implementar o fallback universal `prefix_xor_swar_64` com 6 passos de shift/XOR logarítmicos.
- **FR-012**: Em arquiteturas x86_64, o sistema DEVE implementar `prefix_xor_clmul_x86` com a instrução PCLMULQDQ.
- **FR-013**: Em arquiteturas x86_64 com AVX2, o sistema DEVE implementar `extract_structural_mask_avx2`.
- **FR-014**: Em arquiteturas x86_64 com AVX-512BW, o sistema DEVE implementar `extract_structural_mask_avx512`.
- **FR-015**: A função `compute_quote_mask_dispatch` DEVE selecionar dinamicamente em runtime a extensão de hardware mais avançada presente na CPU.

---

## 8. Critérios de Sucesso e Métricas de Performance

- **SC-001**: O cálculo da Curvatura de Ollivier-Ricci e seleção de plano físico no CBO DEVE executar em $< 50\text{ ns}$ por aresta no MEMO.
- **SC-002**: A validação geométrica de comutatividade SSI entre hiper-retângulos DEVE executar em $< 100\text{ ns}$ para até 1.000 arquivos concorrentes.
- **SC-003**: A serialização de linha de log JSON do Delta Lake DEVE ser compatível e livre de alocações excedentes.
- **SC-004**: O cálculo da máscara de aspas em $\mathbb{F}_2[x]$ via CLMUL / AVX-512 DEVE processar 64 bytes em tempo $\le 3\text{ ciclos de clock}$.
- **SC-005**: A suíte de testes de regressão automatizada DEVE cobrir 100% dos cenários de teste unitário e de integração dos 3 eixos.
- **SC-006**: Cobertura de testes do workspace mantida em $\ge 95\%$ e total conformidade com a Constituição do KinePlex.
