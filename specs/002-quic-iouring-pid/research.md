# Research & Architectural Decisions: HTTP/3 QUIC + io_uring com Controle PID (FT-098)

## 1. Contexto e Objetivos Técnicos
A substituição da pilha TCP/HTTP/2 por HTTP/3 sobre QUIC acoplado diretamente ao `io_uring` visa eliminar os dois maiores gargalos de I/O na VPC da AWS sob instâncias EC2 e AWS Glue 5.0 (Kernel Linux 5.10+ / 6.1+):
1. **Head-of-Line (HoL) Blocking** entre streams independentes;
2. **Sobrecarga de Syscalls e Trocas de Contexto** (`epoll_wait`, `sendmsg`, `recvmsg`).

---

## 2. Decisões Arquiteturais e Análise de Trade-offs

### Decisão 1: Abstração de QUIC Determinística (`quinn-proto` vs `quinn` runtime completo)
- **Decisão**: Utilizar a crate `quinn-proto` diretamente, sem o runtime padrão baseado em Tokio UdpSocket de `quinn`.
- **Racional**:
  - O `quinn` padrão amarra os eventos a sockets assíncronos convencionais via Tokio, o que reintroduziria chamadas de sistema `epoll` e syscalls de socket POSIX (`recvmsg`/`sendmsg`).
  - O `quinn-proto` é uma máquina de estados pura, livre de I/O (*sans I/O*). Ele aceita pacotes brutos recebidos, atualiza a máquina de estados interna e produz datagramas a serem transmitidos sem alocações e sem chamadas de sistema.
  - Isso viabiliza o acoplamento exato aos descritores de submissão do anel `io_uring`.
- **Alternativas Rejeitadas**:
  - *`quinn` (com runtime Tokio)*: Rejeitado por incorrer em trocas de contexto de kernel a cada pacote.
  - *`s2n-quic`*: Excelente biblioteca da AWS, porém seu motor io_uring é interno e menos configurável para acoplamento customizado com controlador PID discreto de pacing em tempo real.

### Decisão 2: Submissão e Recepção Zero-Syscall via `io-uring` (Linux 5.10+ / 6.1+)
- **Decisão**: Configurar anel `io_uring` com `IORING_SETUP_SQPOLL` e buffers registrados (`IORING_REGISTER_BUFFERS`).
- **Racional**:
  - `IORING_SETUP_SQPOLL`: O kernel dedica uma thread de kernel (`io_uring-sq`) para realizar polling contínuo na fila de submissão (SQ). Enquanto houver pacotes para despachar, o espaço de usuário escreve na fila compartilhada em memória sem executar a syscall `io_uring_enter(2)`.
  - `IORING_REGISTER_BUFFERS`: Permite ao kernel pinar páginas previamente. Elimina o custo de `get_user_pages()` a cada datagrama UDP.
  - `IORING_REGISTER_FILES`: Registra o file descriptor do socket UDP como `Fixed(0)`, removendo a contenção atômica de `fget()`/`fput()` no VFS do Linux.
- **Alternativas Rejeitadas**:
  - *Mio / Epoll tradicional*: Incorre em até 250.000 transições de contexto por segundo a 20 Gbps.
  - *AF_XDP (XDP sockets)*: Requer privilégios `CAP_NET_ADMIN` / `root` irrestritos e drivers de placas dedicados, incompatível com ambientes de contêineres e nós gerenciados (AWS Glue / ECS).

### Decisão 3: Segmentação em Hardware via GSO (`UDP_SEGMENT`)
- **Decisão**: Montar super-buffers de até 64 KB agregando múltiplos datagramas QUIC (segmentos de 1350 bytes) e utilizar `UDP_SEGMENT` do Linux.
- **Racional**:
  - Reduz em até $48\times$ o número de SQEs e CQEs transitando no anel `io_uring`.
  - O controlador de interface de rede AWS ENA divide os super-buffers fisicamente em datagramas individuais no hardware, liberando ciclos preciosos de CPU.
- **Alternativas Rejeitadas**:
  - *Submissão datagrama a datagrama (1350B por SQE)*: Satura o anel de submissão do io_uring em altas taxas de dados (acima de 1.5 Mpps).

### Decisão 4: Formulação do Controlador PID em Tempo Discreto
- **Decisão**: Utilizar PID na Forma de Velocidade com Variável de Processo Híbrida $y(t_k)$, Anti-Windup por Back-Calculation e Derivada sobre a Medida (*Derivative-on-Measurement*).
- **Racional**:
  - **Variável Híbrida $y(t_k)$**: O RTT mede o canal fim-a-fim, enquanto $Q_{\text{in-flight}}$ mede a pressão imediata do anel local. Juntos, detectam saturação no Ponto de Kleinrock antes de ocorrer perda.
  - **Back-Calculation Anti-Windup**: $I_k = I_{k-1} + (K_i e_k + K_{aw}(u_{k-1}^{\text{sat}} - u_{k-1}^{\text{raw}})) \Delta t_k$ descobre a capacidade do link e impede sobre-elevação (*overshoot*) quando $u_k$ atinge os limites $[u_{\min}, u_{\max}]$.
  - **Derivative-on-Measurement**: $D_k$ depende de $-\Delta y(t_k)$ e não de $\Delta e_k$. Evita o impulso violento (*derivative kick*) quando o setpoint $r^*$ é ajustado via canal MPSC pelo `LaguerreOptimalDistributor`.
  - **Filtro Passa-Baixas $\tau_d$**: Rejeita jitter de alta frequência do RTT de rede.
- **Alternativas Rejeitadas**:
  - *Algoritmo BBR v2 / CUBIC padrão de TCP*: Projetados para espaço de usuário com chamadas síncronas de socket; não modulam a taxa de injeção em anéis SQ de baixo nível em tempo real.
  - *PID Clássico de Posição sem Anti-Windup*: Causa instabilidade severa quando o buffer da placa de rede atinge capacidade máxima.

---

## 3. Compatibilidade e Isolamento de Plataforma

| Componente | Requisito Mínimo | Plataforma Alvo Primária | Fallback / Modo Degradação |
|---|---|---|---|
| Kernel Linux | 5.10+ | 6.1 LTS (Amazon Linux 2023 / AWS Glue 5.0) | Fallback para submissão dirigida por eventos com aviso estruturado em log |
| `io_uring SQPOLL` | Suportado no kernel com `CAP_SYS_ADMIN` | Instâncias EC2 nativas / Glue workers | Submissão com `io_uring_enter` em batch |
| `UDP_SEGMENT` | Kernel >= 4.18 | AWS ENA habilitado | Segmentação de software (emissão de múltiplos datagramas QUIC) |
| Arena de Memória | HugePages 2MB / `mmap` | Alinhamento 4096B com `MADV_HUGEPAGE` | Memória alinhada padrão no heap fixado |
