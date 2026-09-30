# Backlog técnico proposto para prontidão de 95%

## Contexto e objetivo

Este documento registra um **backlog proposto** para elevar a prontidão do KinePlex de aproximadamente **70–80%** para pelo menos **95%**.

Escopo deste artefato:

- detalhar features acionáveis para implementação futura;
- priorizar por risco e impacto de prontidão;
- definir critérios verificáveis para encerramento.

Este documento **não declara implementação concluída** das features abaixo.

## Evidência existente x backlog proposto

### Evidência existente (já observável no repositório)

- Workspace Rust com `kineplex-core`, `kineplex-net`, `kineplex-node`, `kineplex-sdk`, `kineplex-sdk-macros`, `kineplex-cli`.
- Control Plane com submissão/validação/alocação de grafos.
- Runtime Wasmtime com fuel, limite de memória e cache de módulos.
- Gossip/SWIM com telemetria e integração de roteamento.
- Driver kernel out-of-tree com FT-066..FT-070 documentadas.
- Harness E2E em `bench/e2e/` com limitação explícita de data plane não conectado de ponta a ponta.

### Backlog proposto (não implementado neste trabalho)

As features FT-071..FT-088 abaixo representam propostas para fechamento dos gaps de prontidão.

## Matriz priorizada de features por domínio

| Domínio | Features propostas | Prioridade dominante | Impacto estimado na prontidão |
|---|---|---|---|
| Execução real do data plane distribuído | FT-071, FT-072 | P0 | Muito alto |
| Segurança, autenticação, autorização e isolamento multi-tenant | FT-073, FT-074 | P0 | Alto |
| Confiabilidade, persistência, idempotência, recuperação e rejoin | FT-075, FT-076 | P0 | Muito alto |
| Contratos de protocolo e compatibilidade de versões | FT-077, FT-078 | P1 | Alto |
| Observabilidade, SLOs e métricas reais | FT-079, FT-080 | P1 | Alto |
| Performance e benchmarks reproduzíveis | FT-081, FT-082 | P1 | Médio-alto |
| Build/testes do driver kernel e compatibilidade Linux | FT-083, FT-084 | P1 | Médio-alto |
| CI/CD, release, deploy e rollback | FT-085, FT-086 | P1 | Alto |
| Documentação operacional e runbooks | FT-087, FT-088 | P2 | Médio |

## Backlog detalhado (FT-071..FT-088)

### FT-071 — Data plane distribuído fim a fim (Receptor → Wasm → agregação → Terminal)

- **Problema:** o fluxo distribuído real ainda não fecha o caminho completo de execução com output terminal produzido.
- **Escopo:** conectar estágio receptor, invocação Wasm, estágio de agregação e terminal com passagem de dados Arrow/IPC entre nós alocados.
- **Arquivos/módulos prováveis:** `kineplex-node/src/control.rs`, módulos de execução no `kineplex-node`, `kineplex-core/src/wasm.rs`, `bench/e2e/job.yaml`.
- **Critérios de aceite verificáveis:**
  - execução de um grafo de referência com saída terminal materializada;
  - `bench/e2e/run_e2e.py` não falhar por ausência de `part-*.parquet` quando data plane estiver habilitado;
  - evidência de processamento entre nós distintos no cluster.
- **Dependências:** FT-072, FT-075, FT-077.
- **Prioridade:** P0.
- **Risco:** alto (integração distribuída e acoplamento entre plano de controle e execução).
- **Definição de pronto:** fluxo E2E executando com evidência rastreável de cada estágio no pipeline distribuído.

### FT-072 — Materialização real de saída Parquet no Terminal

- **Problema:** o benchmark E2E declara ausência de produção de Parquet no data plane real.
- **Escopo:** implementar writer/flush de partições Parquet no terminal de execução com convenção estável de saída.
- **Arquivos/módulos prováveis:** estágio terminal no `kineplex-node`, integração Arrow/Parquet no `kineplex-core`, `bench/e2e/run_e2e.py`.
- **Critérios de aceite verificáveis:**
  - produção de `part-*.parquet` em diretório de output previsto;
  - leitura válida do arquivo produzido por verificador do harness;
  - checksum final consistente com baseline funcional do job.
- **Dependências:** FT-071, FT-081.
- **Prioridade:** P0.
- **Risco:** médio-alto (consistência de output, particionamento e flush em falhas).
- **Definição de pronto:** terminal produzindo artefato Parquet consumível pelo fluxo E2E.

### FT-073 — Autenticação mTLS e identidade de nó/cliente

- **Problema:** submissão/alocação e tráfego interno ainda sem política explícita de autenticação forte ponta a ponta.
- **Escopo:** autenticar clientes de submit e nós do cluster por identidade criptográfica com rotação controlada.
- **Arquivos/módulos prováveis:** `kineplex-node/src/control.rs`, camada de transporte no `kineplex-net`, configuração de bootstrap de nós.
- **Critérios de aceite verificáveis:**
  - requisições sem credencial válida rejeitadas com código explícito;
  - nó não autenticado não participa de alocação/roteamento;
  - testes de integração cobrindo handshake válido/inválido.
- **Dependências:** FT-077, FT-085.
- **Prioridade:** P0.
- **Risco:** alto (compatibilidade operacional e gestão de certificados).
- **Definição de pronto:** autenticação obrigatória habilitada para control plane e comunicação interna crítica.

### FT-074 — Autorização por tenant e isolamento de recursos do runtime Wasm

- **Problema:** limite global de execução ainda não garante isolamento robusto multi-tenant (CPU/memória/cache/fuel por tenant).
- **Escopo:** quotas por tenant, políticas de admissão e limites por execução/conjunto de execuções concorrentes.
- **Arquivos/módulos prováveis:** `kineplex-core/src/wasm.rs`, scheduler/executor do `kineplex-node`, metadados de submissão no `kineplex-sdk`.
- **Critérios de aceite verificáveis:**
  - tenant excedendo quota recebe rejeição controlada;
  - execuções de tenants distintos não ultrapassam orçamento alocado;
  - métricas por tenant expostas para auditoria.
- **Dependências:** FT-073, FT-079.
- **Prioridade:** P0.
- **Risco:** alto (fairness, regressão de throughput e política de cache).
- **Definição de pronto:** isolamento configurável e observável em testes de contenção.

### FT-075 — Idempotência de submissão e persistência transacional de estado de grafo

- **Problema:** risco de reprocessamento em retries e perda de estado em reinício.
- **Escopo:** chave de idempotência por submissão e armazenamento durável do ciclo de vida de grafo/alocações.
- **Arquivos/módulos prováveis:** `kineplex-node/src/control.rs`, camada de estado persistente no `kineplex-node`.
- **Critérios de aceite verificáveis:**
  - mesma requisição idempotente não cria múltiplos grafos ativos;
  - reinício de nó preserva estado de grafos em progresso;
  - testes de falha simulada validam recuperação sem duplicação.
- **Dependências:** FT-077, FT-085.
- **Prioridade:** P0.
- **Risco:** alto (consistência entre memória e persistência).
- **Definição de pronto:** retries seguros e recuperação de estado comprovada em cenário de reinício.

### FT-076 — Recuperação automática de execução e cenário shutdown/rejoin multi-nó

- **Problema:** cenário de shutdown/rejoin em cluster ainda não foi medido/validado de forma objetiva.
- **Escopo:** replanejamento após falha de nó, retomada de etapas afetadas e validação de rejoin.
- **Arquivos/módulos prováveis:** `kineplex-net/src/gossip.rs`, `kineplex-net/src/routing.rs`, `kineplex-node/src/control.rs`, `bench/e2e/`.
- **Critérios de aceite verificáveis:**
  - teste de derrubar/reinserir nó durante execução sem corromper resultado final;
  - tempo de convergência de membership registrado;
  - ausência de duplicidade de processamento após rejoin.
- **Dependências:** FT-071, FT-075, FT-079.
- **Prioridade:** P0.
- **Risco:** alto (falhas parciais e convergência distribuída).
- **Definição de pronto:** execução resiliente demonstrada em cenário de falha controlada.

### FT-077 — Contrato versionado de protocolo (Control Plane e Gossip payload)

- **Problema:** evolução de mensagens sem contrato formal forte aumenta risco de incompatibilidade.
- **Escopo:** versionamento explícito, negociação mínima e política de depreciação para APIs de controle e payloads de cluster.
- **Arquivos/módulos prováveis:** `kineplex-node/src/control.rs`, `kineplex-net/src/gossip.rs`, schemas de payload.
- **Critérios de aceite verificáveis:**
  - mensagens com versão incompatível falham com erro determinístico;
  - testes de compatibilidade cruzada entre versões suportadas;
  - documentação de matriz de compatibilidade publicada.
- **Dependências:** FT-078.
- **Prioridade:** P1.
- **Risco:** médio-alto (custos de manutenção de versões simultâneas).
- **Definição de pronto:** contratos explicitamente versionados e cobertos por testes de compatibilidade.

### FT-078 — Suite de compatibilidade SDK/CLI/Node por versão

- **Problema:** não há garantia objetiva contínua de interoperabilidade entre versões de cliente e servidor.
- **Escopo:** matriz de teste de compatibilidade para `kineplex-sdk`, `kineplex-cli` e `kineplex-node`.
- **Arquivos/módulos prováveis:** testes de integração no workspace, pipelines CI.
- **Critérios de aceite verificáveis:**
  - matriz mínima N/N-1 validada em CI;
  - bloqueio de release ao quebrar compatibilidade declarada;
  - relatório de compatibilidade anexado ao processo de release.
- **Dependências:** FT-077, FT-085.
- **Prioridade:** P1.
- **Risco:** médio (aumento do custo de testes).
- **Definição de pronto:** compatibilidade declarada e validada automaticamente.

### FT-079 — Observabilidade operacional de ponta a ponta com métricas de prontidão

- **Problema:** campos críticos permanecem sem medição real no E2E e não há painel fechado de prontidão para decisão de go/no-go.
- **Escopo:** instrumentar métricas de pipeline real (latência por estágio, p95 E2E, CPU de serialização/rede, retries, falhas de alocação).
- **Arquivos/módulos prováveis:** `kineplex-node/src/control.rs`, execução no `kineplex-node`, `kineplex-core/src/metrics.rs`, `bench/e2e/run_e2e.py`.
- **Critérios de aceite verificáveis:**
  - métricas obrigatórias publicadas em Prometheus/OTLP;
  - relatório E2E preenchendo campos hoje `null` sem valores fabricados;
  - dashboard com estado de SLO por ambiente.
- **Dependências:** FT-071, FT-072.
- **Prioridade:** P1.
- **Risco:** médio.
- **Definição de pronto:** métricas operacionais necessárias para prontidão disponíveis e consumíveis.

### FT-080 — SLOs formais e alertas acionáveis (erro, latência, disponibilidade)

- **Problema:** ausência de SLO formal dificulta declarar 95% com critério objetivo.
- **Escopo:** definir SLI/SLO e alertas com janelas, metas e política de erro orçamentário.
- **Arquivos/módulos prováveis:** documentação em `docs/`, regras de monitoramento, integração de métricas.
- **Critérios de aceite verificáveis:**
  - documento de SLO versionado com SLI mensuráveis;
  - alertas para violação de orçamento de erro configurados;
  - revisão operacional aprovada para uso em homolog.
- **Dependências:** FT-079.
- **Prioridade:** P1.
- **Risco:** médio.
- **Definição de pronto:** SLOs acordados e monitorados com gatilho operacional.

### FT-081 — Benchmarks reproduzíveis do data plane real com metodologia fixa

- **Problema:** metas existem, mas medições reproduzíveis de produção ainda não estão estabelecidas.
- **Escopo:** padronizar protocolo de benchmark (ambiente, dataset, aquecimento, repetição, variáveis controladas) para data plane ativo.
- **Arquivos/módulos prováveis:** `bench/e2e/`, `docs/geometry-stability-v0.2.md`, pipelines de benchmark.
- **Critérios de aceite verificáveis:**
  - execução reproduzível com relatório versionado por rodada;
  - variabilidade reportada (mínimo p50/p95 e intervalo);
  - registro explícito do ambiente de execução.
- **Dependências:** FT-071, FT-072, FT-079.
- **Prioridade:** P1.
- **Risco:** médio.
- **Definição de pronto:** benchmark repetível com método padronizado e resultados auditáveis.

### FT-082 — Validação de ganho SIMD e custo de serialização em carga real

- **Problema:** ganho SIMD em produção e fração de CPU em serialização/rede ainda não medidos.
- **Escopo:** medir impacto SIMD on/off e perfil de CPU por estágio em execução distribuída real.
- **Arquivos/módulos prováveis:** `kineplex-core` (hot paths geométricos), `bench/e2e/`, métricas de runtime.
- **Critérios de aceite verificáveis:**
  - experimento controlado com comparação SIMD habilitado/desabilitado;
  - captura da fração de CPU por categoria (rede/serialização/execução);
  - publicação de relatório técnico sem extrapolação não medida.
- **Dependências:** FT-079, FT-081.
- **Prioridade:** P1.
- **Risco:** médio.
- **Definição de pronto:** evidência quantitativa de desempenho em ambiente representativo.

### FT-083 — Build real do driver contra matriz de kernels Linux suportados

- **Problema:** validação estrutural atual não substitui compilação real contra headers de kernels alvo.
- **Escopo:** pipeline de build do módulo out-of-tree para versões de kernel suportadas.
- **Arquivos/módulos prováveis:** `kineplex-kernel/README.md`, scripts/pipelines de CI.
- **Critérios de aceite verificáveis:**
  - build do módulo passando na matriz de versões Linux declarada;
  - artefatos de build publicados para auditoria;
  - falhas de compatibilidade bloqueando release.
- **Dependências:** FT-085.
- **Prioridade:** P1.
- **Risco:** médio-alto (variação de ABI interna do kernel).
- **Definição de pronto:** compatibilidade de build comprovada em matriz de kernels declarada.

### FT-084 — Testes funcionais do driver (KUnit + smoke io_uring/debugfs)

- **Problema:** falta validação contínua de comportamento do driver em ambiente de execução real.
- **Escopo:** adicionar suíte mínima funcional para io_uring command path, debugfs e invariantes geométricas críticas.
- **Arquivos/módulos prováveis:** `kineplex-kernel/*`, infraestrutura de teste kernel.
- **Critérios de aceite verificáveis:**
  - KUnit executado em CI de kernel com resultado versionado;
  - smoke test de `IORING_OP_URING_CMD` do driver concluído;
  - leitura de debugfs validada sem travamento.
- **Dependências:** FT-083.
- **Prioridade:** P1.
- **Risco:** médio.
- **Definição de pronto:** suíte funcional do driver estável em execução automatizada.

### FT-085 — Pipeline CI/CD de release com promoção por ambientes e gates de prontidão

- **Problema:** falta pipeline de release orientado a critérios de prontidão e bloqueio objetivo.
- **Escopo:** formalizar gates de promoção `develop -> homolog -> main` com checklist técnico de prontidão.
- **Arquivos/módulos prováveis:** workflows em `.github/workflows/`, `docs/branching-flow.md`.
- **Critérios de aceite verificáveis:**
  - promoção bloqueada quando gate crítico falha;
  - evidência de execução dos gates por ambiente;
  - critérios de prontidão vinculados à decisão de release.
- **Dependências:** FT-078, FT-083, FT-086.
- **Prioridade:** P1.
- **Risco:** médio.
- **Definição de pronto:** processo de release reproduzível e controlado por gates.

### FT-086 — Estratégia de deploy progressivo e rollback validada

- **Problema:** ausência de procedimento validado de rollback aumenta risco operacional.
- **Escopo:** definir e testar estratégia de canary/rolling com rollback automático/manual e critérios de reversão.
- **Arquivos/módulos prováveis:** documentação operacional em `docs/`, pipelines e scripts de deploy.
- **Critérios de aceite verificáveis:**
  - plano de rollback testado em ambiente de homolog;
  - gatilhos de rollback ligados a SLO/erro orçamentário;
  - tempo de reversão registrado em evidência operacional.
- **Dependências:** FT-080, FT-085.
- **Prioridade:** P1.
- **Risco:** médio-alto.
- **Definição de pronto:** rollback praticável e exercitado com evidência.

### FT-087 — Runbooks operacionais de incidente e recuperação

- **Problema:** falta documentação operacional prescritiva para incidentes e recuperação reduz confiabilidade em produção.
- **Escopo:** runbooks de falha de nó, queda de cluster, degradação de latência, falha de output e recuperação.
- **Arquivos/módulos prováveis:** `docs/` (novo material operacional), referência a observabilidade/SLO.
- **Critérios de aceite verificáveis:**
  - runbooks versionados com pré-condições, passos e validação pós-incidente;
  - exercícios de mesa (tabletop) com ações registradas;
  - vínculo de runbook com alertas e métricas existentes.
- **Dependências:** FT-079, FT-080.
- **Prioridade:** P2.
- **Risco:** médio.
- **Definição de pronto:** operação possui procedimentos executáveis e revisados.

### FT-088 — Guia de operação segura multi-tenant e baseline de hardening

- **Problema:** ausência de baseline consolidada de hardening e operação segura em multi-tenant.
- **Escopo:** consolidar política de configuração segura, limites, gestão de chaves/certificados e checklist de auditoria operacional.
- **Arquivos/módulos prováveis:** `docs/`, configurações de deployment, referências de runtime/rede.
- **Critérios de aceite verificáveis:**
  - checklist de hardening versionado e aplicado por ambiente;
  - política de rotação e revogação de identidade documentada;
  - revisão de segurança operacional registrada.
- **Dependências:** FT-073, FT-074.
- **Prioridade:** P2.
- **Risco:** médio.
- **Definição de pronto:** baseline de segurança operacional adotada como gate de entrada em produção.

## Roadmap em fases e impacto estimado na prontidão

> Estimativas abaixo representam impacto esperado de fechamento de lacunas, não evidência de resultado já obtido.

### Fase 1 — Fechamento do caminho de execução real (alvo ~85–88%)

- FT-071, FT-072, FT-075.
- Resultado esperado: pipeline distribuído funcional com output terminal materializado e proteção básica contra duplicidade/restart.

### Fase 2 — Resiliência e segurança de base (alvo ~90–92%)

- FT-073, FT-074, FT-076, FT-077.
- Resultado esperado: operação autenticada, isolamento multi-tenant inicial e comportamento previsível em falha/rejoin.

### Fase 3 — Mensuração real e performance reprodutível (alvo ~93–94%)

- FT-079, FT-080, FT-081, FT-082.
- Resultado esperado: decisões orientadas por SLO e benchmark reproduzível em data plane real.

### Fase 4 — Industrialização operacional (alvo >=95%)

- FT-078, FT-083, FT-084, FT-085, FT-086, FT-087, FT-088.
- Resultado esperado: cadeia de release/deploy/rollback e operação com runbooks + compatibilidade Linux comprovada.

## Critérios objetivos para declarar prontidão >=95%

Para declarar 95%, todos os critérios abaixo devem estar satisfeitos com evidência versionada:

1. **Execução distribuída real:** fluxo Receptor → Wasm → agregação → Terminal executando com output Parquet produzido.
2. **Confiabilidade:** cenário de shutdown/rejoin multi-nó executado com recuperação sem corrupção de resultado.
3. **Segurança:** autenticação obrigatória e isolamento de quotas multi-tenant ativos e testados.
4. **Compatibilidade:** contratos versionados com suíte N/N-1 passando em CI.
5. **Observabilidade/SLO:** p95 E2E, erro e uso de recursos monitorados com alertas operacionais.
6. **Performance:** benchmark reproduzível com metodologia estável e relatório técnico versionado.
7. **Kernel/Linux:** build + testes funcionais mínimos do driver em matriz de kernel suportada.
8. **Operação/Release:** pipeline de promoção com gates, deploy progressivo e rollback exercitado.
9. **Documentação operacional:** runbooks de incidente e recuperação aprovados.

## Itens explicitamente fora do escopo deste trabalho

- Implementar as features FT-071..FT-088.
- Rodar benchmark E2E Docker completo.
- Compilar/testar módulo kernel contra headers reais neste ambiente.
- Declarar metas de SLO/benchmark como atingidas sem medição real.
- Alterar comportamento de produção do runtime, rede, control plane ou driver.

## Rastreabilidade dos gaps observáveis no estado atual

- `docs/geometry-stability-v0.2.md`: declara que data plane físico não está implementado, output Parquet não produzido e campos de medição críticos ainda não preenchidos.
- `bench/e2e/` (`README.md`, `run_e2e.py`): harness E2E existe, mas documenta ausência de execução distribuída completa até o terminal.
- `kineplex-node/src/control.rs`: há submissão/validação/alocação, porém sem evidência no próprio módulo de pipeline distribuído fim a fim com materialização de resultado.
- `kineplex-core/src/wasm.rs`: sandbox Wasmtime robusto (fuel/memória/cache/ABI), porém sem resolver sozinho requisitos de isolamento multi-tenant e política operacional de quotas por tenant.
- `kineplex-net/src/gossip.rs`: membership/telemetria e payload versionado existentes, com gap de contrato de compatibilidade ampliado e segurança operacional para ambientes multi-versão.
- `kineplex-kernel/README.md`: FT-066..FT-070 descritas; ainda depende de validação contínua em matriz real de kernels e testes funcionais automatizados de integração.

