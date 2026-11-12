# Feature Specification: HTTP/3 QUIC + io_uring com Controle PID em Malha Fechada (FT-098)

**Feature Branch**: `002-quic-iouring-pid`  
**Created**: 2026-11-10  
**Status**: Draft  
**Input**: Substituir a pilha TCP/TLS + HTTP/2 tradicional por HTTP/3 sobre QUIC (UDP) acoplado diretamente ao io_uring do kernel Linux — e governado por um Controlador PID Discreto de Malha Fechada — eliminando gargalos de I/O entre Quimera-Ignis, armazenamento S3/MinIO e microsserviços VPC.

---

## 1. Visão Geral e Fundamentação Física

### 1.1 Contexto e Diagnóstico do Gargalo Atual
Na infraestrutura tradicional baseada em TCP/TLS + HTTP/2:
1. **Head-of-Line (HoL) Blocking no Transporte**: Embora o HTTP/2 forneça multiplexação lógica de streams em nível de aplicação, a camada de transporte TCP subjacente enxerga um único fluxo ordenado de bytes. A perda de um único pacote IP (ex.: descarte temporário na fila da interface de rede AWS ENA) interrompe o avanço de todos os streams multiplexados na mesma conexão até a conclusão da retransmissão TCP.
2. **Custo de Troca de Contexto e Syscalls**: O paradigma `epoll_wait` + `sendmsg`/`recvmsg` exige múltiplas transições entre o anel de proteção de usuário (*Ring 3*) e o kernel (*Ring 0*). Sob taxas de milhões de pacotes por segundo (pps), as mitigações de segurança de microarquitetura (Spectre/Meltdown/KPTI) consomem até 25-35% dos ciclos de CPU apenas em interrupções e trocas de tabela de páginas.
3. **Bufferbloat e Descoordenação da Janela**: Algoritmos de congestionamento tradicionais de TCP baseados em perda (como CUBIC) inflam desnecessariamente os anéis de fila (Tx/Rx queues) do driver ENA e buffers de socket do kernel, induzindo cauda longa de latência ($p99 > 50\text{ ms}$).

### 1.2 A Variável de Processo Medida $y(t_k)$ (Esforço Híbrido de Latência-Pressão)
Em vez de depender exclusivamente do RTT ou da taxa de perda de pacotes (que é um indicador tardio de colapso de fila e induz *throttling* HTTP 503 SlowDown no S3/MinIO), definimos a variável de processo adimensional $y(t_k)$ que combina o gradiente de atraso de fila do QUIC com a taxa de ocupação instantânea do anel `io_uring`:

$$y(t_k) = \alpha_{\text{rtt}} \left( \frac{\text{RTT}_{\text{smoothed}}(t_k) - \text{RTT}_{\min}}{\text{RTT}_{\min}} \right) + (1 - \alpha_{\text{rtt}}) \left( \frac{Q_{\text{in-flight}}(t_k)}{Q_{\text{ring\_cap}}} \right)$$

Onde:
- $\alpha_{\text{rtt}} \in (0, 1)$ é o fator de ponderação de acoplamento (valor nominal de projeto: $0.70$);
- $\text{RTT}_{\min}$ é o atraso de propagação físico mínimo do canal na VPC (tipicamente $400\,\mu\text{s}$ a $1.5\,\text{ms}$ na região AWS `sa-east-1`);
- $\text{RTT}_{\text{smoothed}}(t_k)$ é a estimativa suavizada do RTT calculada continuamente pela máquina de estados QUIC;
- $Q_{\text{in-flight}}(t_k)$ é o número instantâneo de descritores de submissão (SQEs) e pacotes em trânsito sem conclusão;
- $Q_{\text{ring\_cap}}$ é a capacidade nominal do anel de submissão do `io_uring`.

**Interpretação Física:**
Quando $\text{RTT}_{\text{smoothed}} \approx \text{RTT}_{\min}$, os buffers dos roteadores e interfaces da VPC estão vazios (regime laminar). Se $\text{RTT}_{\text{smoothed}}$ começa a divergir de $\text{RTT}_{\min}$, o termo derivativo do controlador detecta o enchimento do buffer antes de ocorrer qualquer perda de datagrama UDP ou descarte na placa ENA, estabilizando a operação exatamente no **Ponto de Kleinrock** (máxima vazão de 20+ Gbps sem indução de bufferbloat).

### 1.3 Lei de Controle PID na Forma de Velocidade com Filtro Derivativo ($N$) e Anti-Windup
Seja $r^*$ o setpoint ótimo adimensional de operação (valor nominal padrão $r^* = 0.25$, mantendo $25\%$ de margem elástica sobre o RTT propagativo mínimo). O erro de rastreamento é dado por:

$$e_k = r^* - y(t_k)$$

A variável de atuação $u_k \in [u_{\min}, u_{\max}]$ determina o orçamento de submissão de datagramas no `io_uring` (Pacing Rate em $\text{bytes}/\mu\text{s}$ ou pacotes/ciclo de amostragem):

$$u_k = \text{sat}_{[u_{\min}, u_{\max}]} \Big( P_k + I_k + D_k \Big)$$

Os três termos são calculados com complexidade $O(1)$ a cada lote de conclusões (CQEs) com período $\Delta t_k = t_k - t_{k-1}$:

1. **Termo Proporcional ($P_k$):**
   $$P_k = K_p \, e_k$$
   Reage instantaneamente a desvios transitórios na janela em voo.

2. **Termo Integral com Clamping Anti-Windup via Back-Calculation ($I_k$):**
   $$I_k = I_{k-1} + \left( K_i \, e_k + K_{aw} (u_{k-1}^{\text{sat}} - u_{k-1}^{\text{raw}}) \right) \Delta t_k$$
   Onde $u_{k-1}^{\text{sat}}$ é o valor saturado no intervalo $[u_{\min}, u_{\max}]$ e $u_{k-1}^{\text{raw}} = P_{k-1} + I_{k-1} + D_{k-1}$.  
   *Efeito:* O termo integral descobre autonomamente a capacidade assintótica exata do enlace de rede (desde instâncias AWS Glue/EC2 G.1X de 10 Gbps até nós G.4X de 40 Gbps) sem necessidade de parametrização manual de topologia.

3. **Termo Derivativo Filtrado sobre a Medida ($D_k$ — Derivative-on-Measurement):**
   $$D_k = \left( \frac{2 \tau_d - \Delta t_k}{2 \tau_d + \Delta t_k} \right) D_{k-1} - \left( \frac{2 K_d}{2 \tau_d + \Delta t_k} \right) \big( y(t_k) - y(t_{k-1}) \big)$$
   Onde $\tau_d$ é a constante de tempo do filtro passa-baixas de 1ª ordem ($N = \frac{1}{\tau_d}$), que rejeita o jitter de alta frequência inerente à rede sem comprometer a resposta transitória.  
   *Prevenção de Derivative Kick:* Como o termo derivativo atua exclusivamente sobre a variação da saída $-\Delta y(t_k)$ (e não sobre $\Delta e_k$), elimina-se completamente o impulso espúrio (*derivative kick*) quando o setpoint $r^*$ é reajustado dinamicamente via canal MPSC pelo `LaguerreOptimalDistributor`.

---

## 2. Arquitetura do Motor io_uring + QUIC (`quinn-proto` + `io-uring`)

Para extrair performance *bare-metal* no Linux (Kernel 5.10+ / 6.1+ presente no Amazon Linux 2023 e AWS Glue 5.0), o subsistema não utiliza o runtime padrão de sockets assíncronos do Tokio/mio. Em vez disso, implementa um loop de I/O de alta densidade baseado em quatro otimizações diretas de kernel:

```
+-----------------------------------------------------------------------------------------+
|                                 Motor de Transporte KinePlex                             |
|                                                                                         |
|  [ LaguerreDistributor ] ---> [ Canal MPSC: Ajuste r* ]                                 |
|            |                                                                            |
|            v                                                                            |
|  [ Quinn-Proto QUIC ] <--------------------+ (Pacing / u_k)                             |
|            |                               |                                            |
|   (Super-buffers 64 KB)          [ Controlador PID ]                                   |
|            v                               ^                                            |
|  +-------------------+                     |                                            |
|  | io_uring SQ Ring  |                     |  Amostragem y(t_k)                         |
|  | (SQPOLL / Fixed)  |                     |  (Smoothed RTT + Q_inflight)               |
|  +-------------------+                     |                                            |
|            |                               |                                            |
|      (Zero-Syscall)                        |                                            |
|            v                               +--------------------+                       |
|     [ Kernel Linux ] <------------------------------------+     |                       |
|            |                                              |     |                       |
|   +---------------------------------------+               |     |                       |
|   | 1. IORING_REGISTER_BUFFERS (HugePages)|               |     |                       |
|   | 2. IORING_REGISTER_FILES (Fixed Sock) |               |     |                       |
|   | 3. UDP_SEGMENT (Hardware GSO/GRO ENA) |               |     |                       |
|   | 4. Batching Governado por u_k         |               |     |                       |
|   +---------------------------------------+               |     |                       |
|            |                                              |     |                       |
|            v                                              v     v                       |
|     [ AWS ENA NIC ] ==========================> [ io_uring CQ Ring ]                    |
|       (20+ Gbps)                                   (CQE Batch)                          |
+-----------------------------------------------------------------------------------------+
```

### 2.1 Buffers Pré-Registrados (`IORING_REGISTER_BUFFERS`)
- Uma arena de memória contígua alinhada em páginas de $2\text{ MB}$ (HugePages ou `mmap` alinhado a 4096 bytes com `MADV_HUGEPAGE`) é alocada e registrada no anel `io_uring` na inicialização do serviço.
- A arena é particionada em slots fixos de $64\text{ KB}$.
- As operações de transmissão e recepção utilizam os descritores `opcode::WriteFixed` e `opcode::ReadFixed` (ou `RecvMsgMulti`), eliminando a necessidade de o kernel executar a chamada de pinning `get_user_pages()` a cada datagrama.

### 2.2 Descritores de Arquivo Registrados (`IORING_REGISTER_FILES`)
- O file descriptor do socket UDP subjacente é fixado na tabela interna de arquivos do `io_uring` através de `types::Fixed(0)`.
- Elimina a contenção de atomicidade e a sobrecarga de incremento/decremento de contador de referências de arquivo (`fget()` / `fput()`) no Virtual File System (VFS) do Linux a cada despacho.

### 2.3 Segmentação em Hardware (`UDP_SEGMENT` — GSO e `UDP_GRO`)
- Em vez de submeter um SQE individual para cada pacote QUIC de $1350\text{ bytes}$ (MTU padrão seguro contra fragmentação IP), o motor agrega e monta super-buffers de até $64\text{ KB}$ contendo aproximadamente 48 datagramas QUIC concatenados.
- O campo de controle do socket define `UDP_SEGMENT = 1350`.
- O adaptador de rede (AWS ENA) realiza o fatiamento físico dos pacotes diretamente no circuito ASIC de hardware (Generic Segmentation Offload — GSO) na saída, e agrega datagramas recebidos na entrada via Generic Receive Offload (GRO).

### 2.4 Batching Governado pelo PID (`io_uring_enter` / `submit_and_wait`)
- O valor da variável de atuação $u_k$ calculado pela lei PID dita rigorosamente quantos SQEs podem ser enfileirados no Submission Queue antes de invocar o avanço do anel.
- Em modo SQPOLL (`IORING_SETUP_SQPOLL`), o thread de kernel drena os SQEs sem nenhuma chamada `io_uring_enter` enquanto o produtor mantiver ritmo contínuo.
- O controlador PID programa o temporizador de *pacing* entre rajadas, impedindo a injeção espúria de pacotes que sobrecarregue os buffers de saída da ENA.

---

## 3. User Scenarios & Testing *(mandatory)*

### User Story 1 - Roteamento HTTP/3 sobre QUIC com Zero-Syscall via io_uring (Priority: P1)
Como motor de computação analítica distribuída (Quimera-Ignis), preciso transmitir blocos de dados Arrow/Parquet para nós de armazenamento e microsserviços via HTTP/3 sobre UDP utilizando anéis `io_uring` em modo SQPOLL e buffers pré-registrados, para que o throughput atinja saturação de linha sem overhead de transição de contexto de CPU.

- **Why this priority**: É o alicerce de infraestrutura de transporte de zero-cópia e zero-syscall necessário para sustentar 20+ Gbps por nó.
- **Independent Test**: Pode ser verificado isoladamente através de um loopback/benchmark local de socket UDP sobre `io_uring SQPOLL`, medindo a contagem de syscalls via `strace -c` e validando zero chamadas `io_uring_enter` em regime estável.

**Acceptance Scenarios**:
1. **Given** um anel `io_uring` com `IORING_SETUP_SQPOLL`, `IORING_REGISTER_BUFFERS` em arena de 2 MB particionada em slots de 64 KB, e socket fixado via `IORING_REGISTER_FILES`,  
   **When** múltiplos streams HTTP/3 transmitem simultaneamente blocos colunares,  
   **Then** os pacotes UDP são despachados sem chamadas `enter` no espaço do usuário e sem bloqueio entre streams caso ocorra perda de datagramas em um stream isolado.
2. **Given** uma conexão HTTP/3 transmitindo continuamente a 10 Gbps,  
   **When** monitorado via `perf stat` ou `strace`,  
   **Then** a taxa de transições de contexto (*context switches*) e *syscalls* no thread de I/O em regime permanente deve ser inferior a 0.01% do volume de pacotes transferidos.

---

### User Story 2 - Estabilização de Fila e Vazão no Ponto de Kleinrock via Controlador PID (Priority: P2)
Como operador de rede e orquestrador de transporte, preciso que a taxa de injeção de pacotes no anel `io_uring` seja governada pelo controlador PID híbrido ($y(t_k)$) com anti-windup e filtro derivativo, para evitar bufferbloat e manter a ocupação de fila no Ponto de Kleinrock sob tráfego heterogêneo.

- **Why this priority**: Evita que rajadas de submissão sobrecarreguem as filas do driver de rede (ENA) ou os buffers do kernel, eliminando oscilações de latência ($p99$).
- **Independent Test**: Injetar tráfego com degrau abrupto de carga e introduzir restrição sintética de banda; medir a convergência da variável de estado $y(t_k) \to r^*$ e verificar a ausência de saturação descontrolada no termo integral ($I_k$).

**Acceptance Scenarios**:
1. **Given** o controlador PID configurado com setpoint $r^* = 0.25$, ganhos $(K_p, K_i, K_d)$, constante $\tau_d$ e coeficiente de anti-windup $K_{aw}$,  
   **When** uma rajada gera elevação do gradiente de latência ou ocupação de anel tal que $y(t_k) > r^*$,  
   **Then** a lei de controle reduz imediatamente a taxa de atuação $u_k$ proporcionalmente à variação derivativa e integral, estabilizando $y(t_k)$ dentro da tolerância de $\pm 5\%$ em menos de 3 RTTs sem derivative kick.
2. **Given** um canal estrangulado onde a taxa calculada atinge o limite inferior $u_{\min}$,  
   **When** a condição de saturação se mantém por dezenas de ciclos,  
   **Then** o termo anti-windup por back-calculation congela a expansão de $I_k$, prevenindo sobre-elevação (*overshoot*) quando o link voltar a liberar banda.

---

### User Story 3 - Resiliência e Multiplexação Imune a Head-of-Line Blocking (Priority: P3)
Como aplicação consumidora de dados distribuídos, preciso recuperar blocos analíticos em 64 streams QUIC paralelos sobre a mesma conexão física, de modo que a perda arbitrária de pacotes em um stream não atrase a entrega dos blocos íntegros dos demais streams.

- **Why this priority**: Garante que nós distribuídos recebendo chunks Parquet/Arrow de partições distintas processem imediatamente lotes finalizados sem esperar por retransmissões alheias.
- **Independent Test**: Simular 5% de perda aleatória de pacotes via `tc-netem` e medir a taxa de entrega de streams independentes comparada a uma conexão HTTP/2 tradicional equivalente.

**Acceptance Scenarios**:
1. **Given** 32 streams paralelos transportando blocos analíticos independentes,  
   **When** 10% dos pacotes do Stream #3 são descartados na rede,  
   **Then** os Streams #1, #2 e #4..32 completam a transmissão e deserialização sem acréscimo de latência decorrente do Stream #3.

---

### User Story 4 - Integração de Ponta a Ponta com Ignis e Storage S3/MinIO (Priority: P4)
Como pipeline analítico KinePlex, preciso direcionar requisições HTTP/3 para endpoints de storage (S3 compatível / MinIO / nós Ignis remotos) utilizando a pilha acoplada `io_uring` + QUIC e super-buffers de 64 KB com `UDP_SEGMENT`, reduzindo a latência ponta a ponta de leitura/escrita em pelo menos 30% em relação ao HTTP/2.

- **Why this priority**: É a aplicação fim que valida a integração do subsistema com o ecossistema KinePlex.
- **Independent Test**: Executar pipeline de upload e download de múltiplos arquivos Parquet de 64 MB contra MinIO/S3 e comparar latência média, $p99$ e consumo de CPU contra a pilha TCP legado.

**Acceptance Scenarios**:
1. **Given** uma instância MinIO/S3 habilitada para HTTP/3 na mesma VPC,  
   **When** o KinePlex despacha 100 blocos colunares concorrentes em super-buffers agregados,  
   **Then** a taxa efetiva de dados atinge $\ge 20\text{ Gbps}$ e a CPU do host opera com pelo menos 35% menos ciclos por GB transferido.

---

## 4. Casos de Borda e Falhas Críticas

1. **Esgotamento do Anel de Submissão (`EBUSY` / SQ Ring Full)**:  
   Se o produtor tentar submeter mais pacotes do que a capacidade do anel `io_uring`, o controlador PID deve detectar a pressão via $Q_{\text{in-flight}} / Q_{\text{ring\_cap}}$ e limitar $u_k \le \text{SQ\_AVAILABLE}$. O subsistema deve aplicar backpressure cooperativo ao invés de descartar buffers.
2. **Perda de Privilégio ou Falha no Kernel SQPOLL (`EPERM` / `ENOSYS`)**:  
   Caso o kernel não possua suporte a `IORING_SETUP_SQPOLL` ou falte permissão (`CAP_SYS_ADMIN`), o subsistema deve reportar erro estruturado e permitir fallback seguro para modo dirigido por eventos com notificação explícita nos logs de auditoria.
3. **Instabilidade Numérica do Controlador PID por Amostragem Irregular**:  
   Se o intervalo $\Delta t_k \to 0$ devido a conclusões ultra-rápidas em rajada, o termo derivativo pode oscilar. Uma cota inferior $\Delta t_{\min} = 1\,\mu\text{s}$ e a constante de filtro $\tau_d$ evitam singularidades numéricas.
4. **Variações Bruscas de RTT da VPC (Jitter da ENA / Migração de Rota)**:  
   Alterações na infraestrutura física que alterem o RTT base disparam o rastreador de mínimo de $\text{RTT}_{\min}$ para recalibrar o gradiente relativo da variável $y(t_k)$.
5. **Reconfiguração Dinâmica de Setpoint sem Derivative Kick**:  
   Quando o `LaguerreOptimalDistributor` emitir novo setpoint $r^*$ via canal MPSC, a formulação *Derivative-on-Measurement* garante que nenhuma derivada de erro seja aplicada na transição, prevenindo colapsos momentâneos de vazão.

---

## 5. Requisitos do Sistema *(mandatory)*

### 5.1 Requisitos Funcionais

- **FR-001**: O sistema DEVE fornecer um motor de transporte HTTP/3 baseado em QUIC (UDP) desacoplado do runtime assíncrono padrão de sockets POSIX do Tokio.
- **FR-002**: O sistema DEVE interagir com o kernel Linux utilizando anéis de `io_uring` configurados preferencialmente com `IORING_SETUP_SQPOLL` para despachos sem chamadas de sistema no espaço de usuário.
- **FR-003**: O sistema DEVE pré-alocar e registrar arenas contíguas de memória (`IORING_REGISTER_BUFFERS`) alinhadas em 2 MB e particionadas em slots de 64 KB, despachando via `opcode::WriteFixed` e recebendo via `opcode::ReadFixed` / `RecvMsgMulti`.
- **FR-004**: O sistema DEVE registrar o descritor de socket UDP no anel `io_uring` (`IORING_REGISTER_FILES`) via `types::Fixed(0)`, eliminando overhead de `fget()`/`fput()` no VFS do Linux.
- **FR-005**: O sistema DEVE suportar agregação de super-buffers de até 64 KB com segmentação em hardware (`UDP_SEGMENT` / GSO) e desagregação na recepção (`UDP_GRO`).
- **FR-006**: O sistema DEVE calcular a variável de processo adimensional $y(t_k) = \alpha_{\text{rtt}} \left( \frac{\text{RTT}_{\text{smoothed}} - \text{RTT}_{\min}}{\text{RTT}_{\min}} \right) + (1 - \alpha_{\text{rtt}}) \left( \frac{Q_{\text{in-flight}}}{Q_{\text{ring\_cap}}} \right)$.
- **FR-007**: O sistema DEVE implementar o Controlador PID Discreto na forma de velocidade calculando o orçamento de injeção $u_k = \text{sat}_{[u_{\min}, u_{\max}]}(P_k + I_k + D_k)$.
- **FR-008**: O termo integral $I_k$ DEVE implementar clamping anti-windup com compensação por back-calculation: $I_k = I_{k-1} + (K_i e_k + K_{aw}(u_{k-1}^{\text{sat}} - u_{k-1}^{\text{raw}})) \Delta t_k$.
- **FR-009**: O termo derivativo $D_k$ DEVE ser calculado estritamente sobre a medida (Derivative-on-Measurement) com filtro passa-baixas de 1ª ordem $\tau_d$: $D_k = \frac{2\tau_d - \Delta t_k}{2\tau_d + \Delta t_k} D_{k-1} - \frac{2 K_d}{2\tau_d + \Delta t_k} (y(t_k) - y(t_{k-1}))$.
- **FR-010**: O sistema DEVE prover um canal MPSC desacoplado permitindo que o `LaguerreOptimalDistributor` ajuste dinamicamente o setpoint $r^*$ sem induzir *derivative kick*.
- **FR-011**: O sistema DEVE tratar independentemente cada stream QUIC, assegurando imunidade a Head-of-Line Blocking entre streams multiplexados.
- **FR-012**: O hot-path de empacotamento, pacing e submissão DEVE ser estritamente zero-allocation na heap durante regime estável.
- **FR-013**: O sistema DEVE expor telemetria detalhada em tempo real contendo $y(t_k)$, $e_k$, $P_k$, $I_k$, $D_k$, $u_k$, $Q_{\text{in-flight}}$ e RTTs observados.
- **FR-014**: O sistema DEVE implementar fallback gracioso e log de auditoria explícito caso o ambiente de execução não disponha de suporte a `io_uring SQPOLL`.

### 5.2 Entidades Principais de Domínio

1. **`PidControlParameters`**: Estrutura imutável de hiperparâmetros contendo os ganhos $(K_p, K_i, K_d)$, ganho anti-windup $K_{aw}$, constante do filtro passa-baixas $\tau_d$, limites de saturação $[u_{\min}, u_{\max}]$, peso $\alpha_{\text{rtt}}$ e cota inferior de amostragem $\Delta t_{\min}$.
2. **`HybridPressureMetric`**: Estado contínuo que armazena a evolução de $y(t_k)$, $y(t_{k-1})$, $\text{RTT}_{\min}$ e $Q_{\text{in-flight}}$.
3. **`PidControllerState`**: Registrador de estado da lei de controle contendo o integrador $I_k$, derivador filtrado $D_k$, última atuação $u_{k-1}^{\text{sat}}$, $u_{k-1}^{\text{raw}}$ e timestamp da última conclusão $t_{k-1}$.
4. **`RegisteredMemoryArena`**: Gerenciador de memória com buffer contíguo alinhado em HugePages (2 MB) particionado em blocos de 64 KB registrados no kernel via `IORING_REGISTER_BUFFERS`.
5. **`IoUringQuicEngine`**: Orquestrador central que integra `quinn-proto` e `io-uring`, despachando super-buffers GSO via `WriteFixed` no socket fixado `Fixed(0)` e governando o batching por $u_k$.

---

## 6. Critérios de Sucesso e Desempenho *(mandatory)*

### 6.1 Critérios Mensuráveis

- **SC-001**: **Throughput de Rede**: Atingir taxa sustentada de transmissão $\ge 20\text{ Gbps}$ em instâncias AWS EC2/Glue sob interfaces ENA em conexões ponto a ponto.
- **SC-002**: **Zero-Syscall Steady State**: Redução de $\ge 99.5\%$ nas chamadas de sistema no thread de I/O em regime permanente sob carga com `IORING_SETUP_SQPOLL`.
- **SC-003**: **Mitigação de Head-of-Line Blocking**: Em cenários com $2\%$ de perda induzida de pacotes, a latência de transferência de streams íntegros multiplexados deve ser $\le 10\%$ superior à latência sem perdas.
- **SC-004**: **Estabilidade no Ponto de Kleinrock (Zero Bufferbloat)**: O percentil de latência $p99$ de entrega de pacotes não deve exceder $1.25 \times \text{RTT}_{\min}$ sob carga nominal.
- **SC-005**: **Eficiência de CPU**: Redução de no mínimo $35\%$ nos ciclos de CPU gastos por gigabyte transferido em comparação à implementação HTTP/2 sobre TCP.
- **SC-006**: **Cobertura de Testes e Confiabilidade**: Manter cobertura de testes automatizados $\ge 95\%$ no novo módulo de transporte de rede e zero vazamento de descritores ou buffers.

---

## 7. Suposições e Dependências

- **Kernel do Sistema Operacional**: Linux versão $\ge 5.10$ / $\ge 6.1$ LTS (Amazon Linux 2023, AWS Glue 5.0).
- **Recursos de Hardware**: Adaptadores AWS ENA compatíveis com UDP GSO/GRO e suporte a múltiplas filas de interrupção.
- **Crates e Dependências**:
  - `io-uring` para manipulação segura e de baixo nível dos anéis de submissão e conclusão;
  - `quinn-proto` para o protocolo QUIC puro e determinístico sem runtime embutido de socket;
  - `bytemuck` para conversão zero-cópia entre buffers brutos e fatias registradas.
- **Ciclo de Vida de Memória**: Toda memória registrada deve permanecer pinada e válida durante toda a existência do anel `io_uring`.
