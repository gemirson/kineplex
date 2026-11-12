# Catalyst Algébrico: Especificação e Relatório de Validação Empírica

Status: **Implementado e Aprovado nos Gates de Não-Regressão (38/38 testes passando)**.

## 1. Resumo do Entendimento e Escopo
- **Módulo:** `kineplex-df/src/catalyst/algebra/` ativado sob demanda via `.with_algebraic(true)`.
- **Fundamento Matemático Defensável:**
  1. **Canonicalização por Ação de Grupo (`canon.rs`):** Redução à forma normal de operadores comutativos (`Add`, `Mul`, `And`, `Or`, `Eq`, `Neq`) para unificar órbitas de equivalência no CSE.
  2. **Interpretação Abstrata & Conexão de Galois (`absdom.rs`):** Reticulado de intervalos $(\mathcal{I}, \sqsubseteq, \sqcap, \sqcup, \bot, \top)$ com conexão de Galois $(\wp(\mathbb{Z}), \alpha, \gamma, \mathcal{I})$ para decisão estática de predicados e poda de filtros/ramos contraditórios e tautológicos.
  3. **Modelo de Custo de Silício para Materialização de Colunas:** Evita materializar subexpressões baratas onde o custo de ler/gravar na memória excede a reavaliação na CPU.

---

## 2. Resultados Consolidados no Gate de Desempenho (`algebra_gate`)

Medições realizadas com 41 iterações pareadas por carga, aquecimento prévio e fixação de afinidade de CPU (`taskset -c 2-5` no Intel Core i5-9400):

| Carga de Trabalho | Técnica Algébrica Aplicada | Tempo de Execução (off → on) | Ganho Relativo | Tempo de Compilação | Veredito |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **W1 (Pipeline Baremetal)** | Identidades e preservação neutra | $2.09\text{ ms} \to 2.08\text{ ms}$ | $-0.17\%$ (Neutro) | $2.1\mu\text{s} \to 3.7\mu\text{s}$ | ✅ Aprovado |
| **W2 (Subexpr Comutadas Baratas)** | Poda de materialização antieconômica | $7.48\text{ ms} \to 7.52\text{ ms}$ | $+0.62\%$ (Neutro) | $3.3\mu\text{s} \to 6.2\mu\text{s}$ | ✅ Aprovado |
| **W3 (Filtro com Gt)** | Preservação de canais sem ganho | $2.32\text{ ms} \to 2.32\text{ ms}$ | $+0.33\%$ (Neutro) | $0.9\mu\text{s} \to 1.7\mu\text{s}$ | ✅ Aprovado |
| **W4 (Subexpr Cara Comutada)** | Canonicalização + CSE com Custo | $14.84\text{ ms} \to 11.24\text{ ms}$ | **-27.11%** (até **-30.07%**) | $4.4\mu\text{s} \to 12.9\mu\text{s}$ | ✅ Aprovado |
| **W5 (Filtro Tautológico)** | Conexão de Galois (AbsDom) | $9.29\text{ ms} \to 4.41\text{ ms}$ | **-52.73%** (até **-53.20%**) | $1.4\mu\text{s} \to 1.6\mu\text{s}$ | ✅ Aprovado |

---

## 3. Principais Descobertas e Decisões de Engenharia
1. **O "Paradoxo do CSE Cego":** Materializar `id + b` em coluna SoA para economizar 1 ciclo de adição custava $+18\%$ de tempo de execução devido ao tráfego de memória de 8 MB. O modelo de custo agora exige que `(count - 1) * eval_cost > CSE_MATERIALIZE_COST`.
2. **Canonicalização Direcionada:** Só substitui a projeção por sua forma canônica se o número de subexpressões lucrativas no CSE aumentar estritamente.
3. **Poda e Simplificação via Galois (`absdom`):** Produziu um ganho de até **52.7%** na eliminação de filtros tautológicos sem risco de falso positivo, consumindo meros 200 nanossegundos a mais de compilação.
4. **Zero-Clone na Contagem de Subexpressões:** Otimizado de `HashMap<Expression, usize>` para `HashMap<&Expression, usize>`, reduzindo pela metade a latência de compilação.
