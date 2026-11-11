# Feature Specification: KineTxt — Driver Nativo SIMD & Branchless para Arquivos Texto/Posicionais e Delimitados (FT-099)

**Feature Branch**: `003-kinetxt-driver`  
**Created**: 2026-11-11  
**Status**: Completed  
**Input**: Projetar o driver nativo de leitura de arquivos texto/posicionais e delimitados (.txt, .csv, .cnab, .fix, flat files legados de custódia) para o KineHaus + Quimera-Ignis, eliminando instanciação de strings por linha, branch mispredictions caractere a caractere e conversão lenta de ASCII para ponto flutuante, materializando diretamente em Apache Arrow (BanachFrame) a > 6 GB/s por core.

---

## 1. Visão Geral e Fundamentação Física / Algébrica

### 1.1 Contexto e Diagnóstico dos Três Crimes de Performance da JVM
Sistemas tradicionais de processamento de dados (ex.: `spark.read.text`, `spark.read.csv`, Hadoop, Pandas) cometem três gargalos severos de arquitetura ao processar arquivos texto e delimitados em larga escala:
1. **Instanciação Massiva de `java.lang.String` no Heap**: Cada linha e coluna gera um objeto individual encapsulado com ponteiros, cabeçalhos de objeto (16-24 bytes de overhead) e arrays de caracteres. Em arquivos de dezenas de gigabytes, isso induz pressão extrema sobre o Garbage Collector (GC pauses de múltiplos segundos) e satura a largura de banda da memória DDR.
2. **Branch Misprediction em Loops Escalares**: O parsing caractere a caractere via máquinas de estados com condicionais (`if (c == ';' || c == '\n')`) provoca taxas de erro de predição de desvio (*branch misprediction*) entre 15% e 30% na CPU, forçando o esvaziamento contínuo dos pipelines super-escalares modernos (15 a 20 ciclos de penalidade por erro).
3. **Conversão Lenta de ASCII para Float/Decimal (`Double.parseDouble` / `strtod`)**: Algoritmos de conversão escalar tratam dígitos um a um com multiplicações sequenciais e divisões lentas por potências de 10, limitando a vazão a menos de 150-250 MB/s por núcleo.

### 1.2 A Solução Arquitetural: KineTxt
O **KineTxt** trata o arquivo bruto proveniente do `io_uring` / S3 como um fluxo contínuo de vetores de bytes no corpo de Galois $\mathbb{F}_2^8$, extraindo a topologia sintática através de máscaras de bits AVX2 / AVX-512 e projetando os dados diretamente nos buffers contíguos de memória do Apache Arrow (`OffsetBuffer`, `ScalarBuffer`, `StringArray`, `Float64Array`, `Int64Array`) sem nenhuma alocação intermediária de strings na heap.

---

## 2. A Arquitetura de 4 Estágios do Driver KineTxt

```
+-----------------------------------------------------------------------------------------+
|                                    KineTxt Pipeline                                     |
|                                                                                         |
|  [ S3 / MinIO / io_uring ] ---> Buffers Pré-Registrados (Janelas de 64 MB / 4096B)      |
|                                         |                                               |
|                                         v                                               |
|  +-----------------------------------------------------------------------------------+  |
|  | Estágio 1: Bisseção Assíncrona de Fronteira (Speculative Midpoint Seek)           |  |
|  | - Divide arquivos multi-gigabyte em P blocos sem escanear do byte 0              |  |
|  | - memchr vetorizado AVX2/AVX-512 avança <= 256B até o próximo '\n'                 |  |
|  +-----------------------------------------------------------------------------------+  |
|                                         |                                               |
|                                         v                                               |
|  +-----------------------------------------------------------------------------------+  |
|  | Estágio 2: Descoberta Estrutural em GF(2) (Two-Stage Structural Indexing)         |  |
|  | - vpcmpeqb gera máscaras de delimitadores e quebras de linha em u64               |  |
|  | - QuoteMask = CLMUL(Q, 0xFFFF_FFFF_FFFF_FFFF) em 1 ciclo (vpclmulqdq)             |  |
|  | - ValidDelims = RawDelims AND NOT QuoteMask (Zero if!)                             |  |
|  +-----------------------------------------------------------------------------------+  |
|                                         |                                               |
|                                         v                                               |
|  +-----------------------------------------------------------------------------------+  |
|  | Estágio 3: Parsing Numérico Branchless SWAR (SIMD Within A Register)              |  |
|  | - Carrega 8 bytes ASCII diretamente em u64                                        |  |
|  | - Conversão paralela em 4 ciclos de clock sem loops e sem branching               |  |
|  | - Eisel-Lemire ultra-rápido para mantissas e floats normalizados                  |  |
|  +-----------------------------------------------------------------------------------+  |
|                                         |                                               |
|                                         v                                               |
|  +-----------------------------------------------------------------------------------+  |
|  | Estágio 4: Materialização Zero-Copy em Apache Arrow (BanachFrame)                 |  |
|  | - Empréstimo direto dos bytes da arena de I/O para Arrow StringArray               |  |
|  | - Vetorização por Rayon de colunas numéricas Float64Array / Int64Array            |  |
|  +-----------------------------------------------------------------------------------+  |
|                                         |                                               |
|                                         v                                               |
|  [ RecordBatch Arrow ] ===> [ BanachFrame ] ===> [ MK-EMD Laguerre ] ===> [ Parquet ]   |
+-----------------------------------------------------------------------------------------+
```

### 2.1 Estágio 1: Particionamento de Blocos (Chunk Slicing)
- **Bisseção Assíncrona de Fronteira (*Speculative Midpoint Seek*)**: Para processar arquivos massivos (ex.: 10 GB a 1 TB), o arquivo é fatiado em $P$ partições com base em Byte-Ranges aproximados calculados analiticamente.
- O início de cada partição subsequente avança especulativamente $\le 256\text{ bytes}$ utilizando `memchr` vetorizado (`vpcmpeqb` / `_mm512_cmpeq_epi8_mask`) até encontrar a primeira quebra de linha `\n`, alinhando a fronteira em $O(1)$ sem escanear o arquivo desde o byte 0.

### 2.2 Estágio 2: Descoberta Estrutural em $\text{GF}(2)$ e CLMUL
O desafio fundamental em arquivos delimitados financeiros é identificar se um separador (`;`, `,`, `|`, `\t`) é estrutural ou se está contido como texto dentro de aspas duplas (`"..."`) sem recorrer a um laço de repetição com booleano sequencial (`bool inside_quotes`).

No KineTxt, essa propriedade é computada analiticamente no anel de polinômios sobre o corpo finito de Galois $\mathbb{F}_2[x]$:
1. Para cada bloco de 64 bytes $B = (b_0, \dots, b_{63})$, constrói-se o vetor de bits $Q \in \texttt{u64}$, onde o $i$-ésimo bit é $1$ se $b_i = \texttt{'\"'}$.
2. A condição de estar "dentro de aspas" no byte $i$ é dada pelo somatório cumulativo módulo 2 (*Prefix-XOR*) de todos os bits de $Q$ até a posição $i$:
   $$\text{QuoteMask}(i) = \bigoplus_{k=0}^i Q_k$$
3. Em nível de microarquitetura, o Prefix-XOR de 64 bits corresponde à multiplicação livre de transporte (*Carry-Less Multiplication* — instrução `vpclmulqdq` ou redução rápida de prefixo) entre $Q$ e o polinômio com todos os bits ligados ($\texttt{0xFFFF\_FFFF\_FFFF\_FFFF}_{16}$):
   $$\text{QuoteMask} = \text{CLMUL}(Q, \, \texttt{0xFFFF\_FFFF\_FFFF\_FFFF})$$
4. A máscara de delimitadores sintáticos verdadeiros é obtida em 1 ciclo por álgebra booleana sem nenhum desvio:
   $$\text{ValidDelims} = \text{RawDelims} \;\land\; \neg\,\text{QuoteMask}$$
5. As posições dos delimitadores são extraídas utilizando instruções de contagem de zeros (`tzcnt` / `trailing_zeros`) e limpeza do bit menos significativo (`blsr` / `mask &= mask - 1`).

### 2.3 Estágio 3: Parsing Numérico Branchless SWAR
Valores monetários e operacionais em arquivos de custódia (CNAB 240/400, Selic, B3, logs FIX) são representados como sequências ASCII de dígitos numéricos contíguos (ex.: `"0001254389"` representando `12543.89`).

Em vez do laço tradicional `val = val * 10 + (c - '0')`, o KineTxt carrega 8 bytes ASCII simultaneamente em um registrador de 64 bits (`u64`) e executa a redução aritmética vetorial de SWAR (*SIMD Within A Register*):
1. Subtração paralela de `'0'` (`0x3030_3030_3030_3030`) em todos os 8 bytes.
2. Agrupamento de pares adjacentes em palavras de 16 bits: $(d_0 \times 10 + d_1)$.
3. Agrupamento de pares de 2 dígitos em palavras duplas de 32 bits: $(d_{01} \times 100 + d_{23})$.
4. Combinação dos blocos de 4 dígitos no inteiro final de 8 dígitos: $(d_{0123} \times 10000 + d_{4567})$.
5. Aplicação direta da escala implícita (ex.: $0.01$ para centavos) ou conversão Eisel-Lemire para ponto flutuante IEEE 754 de 64 bits.

### 2.4 Estágio 4: Materialização Zero-Copy em Arrow
- Para colunas de texto (`Utf8`), os bytes brutos da arena contígua são emprestados diretamente para os buffers de valores (`Buffer`) e deslocamentos (`OffsetBuffer<i32>`) do `StringArray` do Apache Arrow, eliminando qualquer criação de `String` intermediária.
- As colunas numéricas (`Float64Array`, `Int64Array`) são materializadas em buffers alinhados utilizando iteradores paralelos em Rayon sobre fatias contíguas.

---

## 3. Modos de Operação do Motor

### 3.1 Modo 1: Leitor Posicional de Largura Fixa (`FixedWidthPositionalScanner`)
- Projetado para arquivos de custódia de mainframe (CNAB 240/400, Selic, Cetip, arquivos de bolsa B3).
- Linhas possuem tamanho fixo de stride ($L$ bytes).
- O acesso a qualquer campo da linha $k$ e coluna com intervalo $[s..e]$ é realizado em tempo constante $O(1)$:
  $$\text{FieldOffset}(k) = k \cdot L + s$$
- **Eliminação Total de Escaneamento**: Não é necessário procurar delimitadores nem quebras de linha (`\n`); os loops operam por paralelização de colunas diretamente indexadas no buffer.

### 3.2 Modo 2: Leitor Delimitado Vetorizado (`DelimitedSimdScanner`)
- Projetado para arquivos delimitados por caractere (`;`, `,`, `|`, `\t`).
- Escaneia em blocos de 64 bytes gerando vetores de quebras de linha e delimitadores em registradores de 64 bits.
- Extrai as colunas selecionadas sem criar vetores intermediários de tokens por linha.

---

## 4. User Scenarios & Testing *(mandatory)*

### User Story 1 - Leitura Posicional de Alta Velocidade (CNAB / Mainframe) (Priority: P1) 🎯 MVP
Como nó de processamento analítico (KineHaus / Quimera-Ignis), preciso processar arquivos posicionais de custódia financeira de largura fixa com stride constante $O(1)$, materializando os campos diretamente em buffers Arrow `RecordBatch` a uma taxa superior a 6 GB/s por núcleo.

- **Why this priority**: É o formato predominante em liquidação financeira e custódia bancária (CNAB 240/400 e B3), onde a eliminação do escaneamento de quebra de linha gera o maior ganho líquido de vazão.
- **Independent Test**: Benchmark e teste unitário com buffer sintético CNAB de 1.000.000 linhas de 240 bytes medindo tempo de processamento, integridade dos valores numéricos e zero alocações de `String`.

**Acceptance Scenarios**:
1. **Given** um arquivo posicional com stride de 240 bytes contendo colunas `Float64` com escala implícita (centavos), `Int64` e `Utf8`,  
   **When** processado pelo `parse_fixed_width_to_arrow`,  
   **Then** o resultado é um `RecordBatch` Arrow válido com contagem correta de linhas, tipos estritos e sem alocação de objetos `String` individuais.
2. **Given** um campo numérico de 8 dígitos `"00012543"`,  
   **When** submetido à rotina `parse_8_digits_swar`,  
   **Then** o retorno deve ser exatamente `12543` em tempo de execução inferior a 5 nanossegundos.

---

### User Story 2 - Leitura Delimitada Branchless com CLMUL Quote-Masking (Priority: P2)
Como motor analítico de ingestão de dados, preciso analisar arquivos CSV/.TXT delimitados contendo delimitadores protegidos por aspas duplas, extraindo os delimitadores válidos em tempo constante via álgebra de Galois sem branch mispredictions.

- **Why this priority**: Arquivos CSV financeiros frequentemente contêm descrições textuais com separadores internos (ex.: `"PETR4;Lote 100"`), onde leitores ingênuos falham ou sofrem até 30% de perda de ciclos de CPU por desvios incorretos.
- **Independent Test**: Teste de parsing com linhas contendo delimitadores dentro e fora de aspas; verificação de que `QuoteMask` desconsidera separadores protegidos e preserva a integridade das colunas.

**Acceptance Scenarios**:
1. **Given** uma linha delimitada `"123;\"Valor;Com;Separador\";456.78"`,  
   **When** processada pelo `parse_delimited_chunk_to_arrow` com delimitador `;`,  
   **Then** exatamente 3 colunas são identificadas (`123`, `Valor;Com;Separador`, `456.78`) sem truncamento ou corrupção de offsets.
2. **Given** um bloco de 64 bytes contendo aspas e delimitadores,  
   **When** calculado `ValidDelims = RawDelims & !QuoteMask`,  
   **Then** nenhum bit correspondente a delimitador interno de aspas permanece ativo.

---

### User Story 3 - Bisseção Assíncrona e Particionamento Zero-Copy para Rayon (Priority: P3)
Como orquestrador paralelo de ingestão, preciso dividir um buffer de arquivo texto contíguo de 64 MB a 10 GB em $P$ fatias perfeitamente alinhadas em quebras de linha `\n`, sem escanear o buffer a partir do byte zero.

- **Why this priority**: Permite que múltiplos núcleos processem paralelamente porções distintas do mesmo arquivo em memória compartilhada sem contenção e sem dependência de parsing prévio.
- **Independent Test**: Dividir um buffer arbitrário em 8 chunks e verificar que todas as fatias iniciam imediatamente após um `\n` e terminam em um `\n`, sem perda de registros nas fronteiras.

**Acceptance Scenarios**:
1. **Given** um buffer de texto de 10 MB e solicitação de 4 chunks,  
   **When** executado `split_txt_buffer_into_aligned_chunks`,  
   **Then** são retornadas 4 fatias contíguas cuja soma dos comprimentos é exatamente igual ao comprimento total do buffer original.

---

### User Story 4 - Pipeline End-to-End: io_uring -> KineTxt -> BanachFrame -> Parquet (Priority: P4)
Como pipeline integrado do KinePlex, preciso conectar o leitor KineTxt à saída do motor de rede `io_uring` e alimentar o distribuidor de Laguerre-Kantorovich para gravação em arquivos Parquet no Apache Iceberg.

- **Why this priority**: Valida a convergência do ecossistema e a ausência de gargalos em todo o caminho de ingestão analítica.
- **Independent Test**: Ingerir arquivo texto, transformar em Arrow `RecordBatch`, alimentar o `LaguerreOptimalDistributor` e verificar a geração de arquivos Parquet ZSTD no storage.

**Acceptance Scenarios**:
1. **Given** dados brutos obtidos via blocos de 64 MB da arena de I/O,  
   **When** o `KineTxtDriver` emite o `RecordBatch`,  
   **Then** o lote é aceito pelo `LaguerreOptimalDistributor` e particionado de acordo com a massa ótima sem cópias intermediárias.

---

## 5. Casos de Borda e Falhas Críticas

1. **Campos Numéricos com Espaços em Branco / Zero-Padding**:  
   Campos posicionais frequentemente contêm espaços à esquerda ou zeros de preenchimento (`" 123.45"` ou `"0000123"`). O parser `fast_parse_f64_ascii` deve saltar espaços e tratar sinais sem branching.
2. **Aspas Não-Balanceadas em Fatias Paralelas**:  
   Caso uma fatia arbitrária termine no meio de uma string entre aspas, o algoritmo de bisseção de fronteira deve verificar o estado de aspas acumulado no ponto de corte ou exigir que aspas não atravessem quebras de linha (padrão RFC 4180 estrito para streaming).
3. **Última Linha sem Caractere `\n` de Finalização (EOF Limpo)**:  
   O particionador e o leitor delimitado devem obrigatoriamente tratar a cauda final do arquivo caso o último byte não seja `\n`.
4. **Linhas com Stride Incompleto no Modo Posicional**:  
   Se `raw_txt.len() % stride != 0`, os bytes residuais devem ser validados ou descartados com aviso explícito de integridade de layout.

---

## 6. Requisitos do Sistema *(mandatory)*

### 6.1 Requisitos Funcionais

- **FR-001**: O sistema DEVE fornecer o driver `KineTxtDriver` com suporte unificado a dois modos operacionais: Delimitado (`DelimitedSimdScanner`) e Posicional de Largura Fixa (`FixedWidthPositionalScanner`).
- **FR-002**: No modo posicional de largura fixa, o sistema DEVE indexar campos em tempo constante $O(1)$ (`base + row * stride + offset`) sem escanear delimitadores ou quebras de linha.
- **FR-003**: No modo delimitado, o sistema DEVE processar blocos de 64 bytes gerando máscaras de bits de 64 bits para delimitadores e quebras de linha, iterando com instruções de contagem de zeros (`tzcnt`).
- **FR-004**: O sistema DEVE calcular a máscara de aspas (`QuoteMask`) sem laços condicionais via multiplicação livre de transporte (CLMUL / Prefix-XOR em $\mathbb{F}_2[x]$): $\text{ValidDelims} = \text{RawDelims} \land \neg\,\text{QuoteMask}$.
- **FR-005**: O sistema DEVE implementar o algoritmo SWAR de 4 ciclos para conversão direta de 8 dígitos ASCII contíguos em inteiro `u64` sem loops ou chamadas a `strtod`.
- **FR-006**: O parser numérico DEVE suportar especificação de escala decimal implícita (ex.: $0.01$ para centavos) e detecção de ponto/vírgula decimal.
- **FR-007**: O sistema DEVE materializar colunas de texto diretamente nos buffers `OffsetBuffer` e `Buffer` do Apache Arrow (`StringArray`) emprestando fatias contíguas da arena de memória sem alocação de instâncias individuais de `String`.
- **FR-008**: O sistema DEVE materializar colunas numéricas em `Float64Array` e `Int64Array` do Apache Arrow paralelizadas via Rayon.
- **FR-009**: O sistema DEVE fornecer a rotina `split_txt_buffer_into_aligned_chunks` dividindo buffers em $P$ partições alinhadas em quebras de linha `\n` via busca especulativa vetorizada.
- **FR-010**: O módulo DEVE ser integrado na crate analítica de dados (`kineplex-df` / `kineplex-core`), compatível com o ecossistema `BanachFrame` e sinks de armazenamento `LaguerreOptimalDistributor`.
- **FR-011**: O hot-path de conversão numérica e extração de delimitadores DEVE ser estritamente zero-allocation na heap.
- **FR-012**: O driver DEVE expor o enum `TxtColumnSpec` permitindo tipagem mista (`Float64`, `Int64`, `Utf8`) por índice ou intervalo posicional.

### 6.2 Entidades Principais de Domínio

1. **`TxtColumnSpec`**: Enumeração que define a projeção de cada coluna:
   - `Float64 { name: String, col_index: usize, fixed_range: Option<(usize, usize)>, scale: f64 }`
   - `Int64 { name: String, col_index: usize, fixed_range: Option<(usize, usize)> }`
   - `Utf8 { name: String, col_index: usize, fixed_range: Option<(usize, usize)> }`
2. **`KineTxtDriver`**: Motor principal contendo delimitador (`u8`), stride opcional (`Option<usize>`) e lista de especificações `schema_specs`.
3. **`FixedWidthPositionalScanner`**: Lógica de execução $O(1)$ para arquivos tabulares de custódia e mainframe.
4. **`DelimitedSimdScanner`**: Lógica de escaneamento vetorizado de 64 bytes com descarte de aspas em $\text{GF}(2)$ e Rayon.

---

## 7. Critérios de Sucesso e Desempenho *(mandatory)*

### 7.1 Critérios Mensuráveis

- **SC-001**: **Taxa de Processamento Sintático**: Atingir taxa de extração sintática $\ge 6\text{ GB/s}$ por núcleo de CPU para arquivos posicionais de largura fixa em memória RAM.
- **SC-002**: **Taxa de Parsing Delimitado**: Atingir vazão $\ge 2.5\text{ GB/s}$ por núcleo para arquivos CSV/delimitados com suporte a aspas.
- **SC-003**: **Zero Alocação de Strings**: Zero instâncias de `String` no heap para o corpo dos dados durante o processo de parsing e materialização em `StringArray`.
- **SC-004**: **Latência de Conversão SWAR**: Conversão de 8 dígitos ASCII em inteiro `u64` em menos de 5 nanossegundos em regime estável.
- **SC-005**: **Materialização Direta em Arrow**: Produzir um `RecordBatch` Apache Arrow válido e diretamente consumível pelo `BanachFrame` e pelo `LaguerreOptimalDistributor`.
- **SC-006**: **Cobertura de Testes**: Manter cobertura de testes automatizados $\ge 95\%$ em todos os componentes do driver KineTxt.

---

## 8. Suposições e Dependências

- **Arquitetura de CPU Alvo**: x86_64 com suporte a AVX2 / AVX-512 e ARM64 com suporte a NEON.
- **Formatos Suportados**: Arquivos texto delimitados por caractere ASCII (`,`, `;`, `|`, `\t`) e arquivos posicionais de mainframe com stride fixo (CNAB 240, CNAB 400, Selic, FIX, B3).
- **Crates e Dependências**:
  - `arrow-array = "46.0"`
  - `arrow-buffer = "46.0"`
  - `arrow-schema = "46.0"`
  - `rayon = "1.8"`
  - `bytemuck = "1.14"`
- **Conformidade com a Constituição**: Aderência estrita às regras de SDD em `AGENTES.MD`.
