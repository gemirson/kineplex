# Research & Architectural Decisions: KineTxt Driver (FT-099)

## 1. Contexto e Objetivos Técnicos
Substituir a leitura baseada em JVM (`spark.read.text`, `spark.read.csv`) por um motor vetorial nativo em Rust capaz de processar arquivos massivos (.txt, .csv, .cnab, .fix) a > 6 GB/s por núcleo, materializando diretamente em `RecordBatch` do Apache Arrow com zero alocações de `String` no heap.

---

## 2. Decisões Arquiteturais e Análise de Trade-offs

### Decisão 1: Indexação Estrutural em $\text{GF}(2)$ via Prefix-XOR Polinomial (CLMUL)
- **Decisão**: Utilizar álgebra no corpo finito $\mathbb{F}_2[x]$ para calcular a máscara de aspas (`QuoteMask`) e validar delimitadores em 1 ciclo por bloco de 64 bytes.
- **Racional**:
  - Em arquivos delimitados com texto contendo separadores (ex.: `"PETR4;Lote 100"`), máquinas de estados condicionais (`if inside_quotes`) induzem 15% a 30% de branch misprediction na CPU.
  - O cálculo da soma cumulativa módulo 2 ($\bigoplus_{k=0}^i Q_k$) equivale à multiplicação de polinômios sem transporte (Carry-Less Multiplication — `vpclmulqdq` ou redução rápida de prefixos).
  - A operação $\text{ValidDelims} = \text{RawDelims} \land \neg\,\text{QuoteMask}$ elimina completamente qualquer condicional.
- **Alternativas Rejeitadas**:
  - *Máquina de Estados Finita (DFA) escalar*: Causa gargalo severo de execução sequencial e stalling de pipeline.
  - *Regex padrão*: Ordem de grandeza mais lenta (~50 MB/s).

### Decisão 2: Parsing Numérico Branchless SWAR (SIMD Within A Register)
- **Decisão**: Carregar 8 bytes ASCII diretamente em `u64` Little/Big-Endian e converter para inteiro binário em 4 ciclos sem loops.
- **Racional**:
  - Arquivos bancários e de custódia (CNAB 240/400, B3, BACEN) utilizam inteiros com zero-padding e escala fixa (ex.: `"0001254389"` = R$ 12.543,89).
  - A subtração paralela `0x3030303030303030` e redução por shifts e multiplicações em registradores de 64 bits converte 8 dígitos em $< 5\text{ ns}$.
- **Alternativas Rejeitadas**:
  - *`strtod` / `parse::<f64>()` padrão*: Incorre em loops escalares e alocações desnecessárias.

### Decisão 3: Acesso Direto com Stride $O(1)$ no Modo Posicional
- **Decisão**: Para arquivos posicionais de mainframe com largura fixa ($L$ bytes por linha), calcular offsets diretamente como $base + row \times L + offset$ sem escanear delimitadores ou quebras de linha `\n`.
- **Racional**:
  - Elimina 100% das leituras de controle sintático; o Rayon divide as linhas pelo número de threads e cada thread processa diretamente seus offsets de memória.
- **Alternativas Rejeitadas**:
  - *Dividir por `\n` antes de ler colunas*: Desperdiça ciclos de CPU procurando caracteres que já estão em posições geometricamente previsíveis.

### Decisão 4: Empréstimo Direto de Fatias para Arrow `StringArray`
- **Decisão**: Construir `StringArray` a partir de `OffsetBuffer<i32>` e `Buffer` preenchidos com fatias contíguas da arena de I/O.
- **Racional**:
  - Elimina a criação de instâncias de `String` individuais. Toda a coluna é um único buffer contíguo de bytes com um índice de deslocamento.
- **Alternativas Rejeitadas**:
  - *Criar `Vec<String>` e converter para Arrow*: Destruiria a taxa de throughput por excesso de alocações e fragmentação do heap.
