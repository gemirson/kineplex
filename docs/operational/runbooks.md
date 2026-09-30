# Runbooks Operacionais de Incidente e Recuperação - FT-087

## Visão Geral

Este documento contém runbooks para cenários de falha comuns no KinePlex, com procedimentos pré-condições, passos e validação pós-incidente.

---

## Runbook 1: Falha de Nó

### Descrição
Quando um nó do cluster falha ou fica indisponível.

### Pré-condições
- [ ] Acesso ao cluster
- [ ] Ferramentas de monitoramento
- [ ] Permissão para reiniciar serviços

### Detecção
```
ALERTA: node_failure
Node node-3 não responde há 2 minutos
Métrica: healthy_nodes < threshold
```

### Passos de Recuperação

#### 1. Confirmar falha
```bash
# Verificar status do nó
kubectl get nodes
kineplex-cli cluster status

# Ver logs do nó
journalctl -u kineplex-node -n 100
```

#### 2. Identificar grafos afetados
```bash
# Listar grafos em execução
kineplex-cli list --status running

# Identificar grafos no nó falho
kineplex-cli list --node node-3
```

#### 3. Replanejar execuções
```bash
# Reexecutar grafos afetados
for graph_id in $(kineplex-cli list --node node-3 --format json | jq -r '.[].id'); do
  kineplex-cli resubmit --graph-id $graph_id
done
```

#### 4. Remover nó do cluster
```bash
# Atualizar membership
kineplex-cli cluster remove-node node-3
```

#### 5. Verificar saúde do cluster
```bash
# Verificar convergência
kineplex-cli cluster health
```

### Validação Pós-incidente
- [ ] Cluster com número esperado de nós
- [ ] Grafos重新execução completados
- [ ] Sem duplicidade de processamento
- [ ] Métricas de membership atualizadas

---

## Runbook 2: Queda de Cluster

### Descrição
Quando o cluster inteiro fica indisponível.

### Pré-condições
- [ ] Acesso root aos servidores
- [ ] Backup do estado do cluster
- [ ] Procedimento de recovery documentado

### Detecção
```
ALERTA: cluster_down
Nenhum nó respondendo há 5 minutos
```

### Passos de Recuperação

#### 1. Verificar infraestrutura
```bash
# Verificar rede
ping -c 3 control-plane-node

# Verificar discos
df -h
```

#### 2. Reiniciar nós do cluster
```bash
# Iniciar serviço em cada nó
systemctl start kineplex-node

# Verificar status
kineplex-cli cluster status
```

#### 3. Verificar estado persistente
```bash
# Verificar dados de grafos pendentes
ls -la /var/lib/kineplex/graph-state/

# Verificar idempotency keys
cat /var/lib/kineplex/idempotency-keys.json
```

#### 4. Recomeçar execuções pendentes
```bash
# Listar grafos pendentes
kineplex-cli list --status pending

# Reprocessar
kineplex-cli resubmit-all
```

### Validação Pós-incidente
- [ ] Todos os nós no estado Ready
- [ ] Membership convergido
- [ ] Grafos pendentes processados
- [ ] Sem perda de dados

---

## Runbook 3: Degradação de Latência

### Descrição
Quando a latência excede o SLO por período prolongado.

### Detecção
```
ALERTA: high_latency
Latência p95 > 500ms por 5 minutos
```

### Passos de Diagnóstico

#### 1. Identificar gargalo
```bash
# Ver métricas por estágio
curl -s localhost:9090/api/v1/query?query=kineplex_stage_latency_ms | jq

# Ver utilização de recursos
kubectl top nodes
```

#### 2. Verificar rede
```bash
# Ver latência entre nós
for node in node-1 node-2 node-3; do
  ping -c 10 $node
done
```

#### 3. Verificar Wasm runtime
```bash
# Ver métricas de execução
curl -s localhost:9090/metrics | grep wasm

# Ver cache
curl -s localhost:9090/metrics | grep cache
```

### Ações Corretivas

| Gargalo | Ação |
|---------|------|
| CPU | Escalar nós |
| Memória | Aumentar limite |
| Rede | Verificar roteamento |
| Wasm | Limpar cache |

### Validação
- [ ] Latência retornou ao normal
- [ ] Sem novos alertas de latência

---

## Runbook 4: Falha de Output

### Descrição
Quando o terminal não produz arquivo Parquet de saída.

### Detecção
```
ALERTA: no_output
Nenhum arquivo part-*.parquet após 5 minutos
```

### Passos de Diagnóstico

#### 1. Verificar execução
```bash
# Ver status do grafo
kineplex-cli status <graph-id>

# Ver logs do terminal stage
kubectl logs -l stage=terminal
```

#### 2. Verificar armazenamento
```bash
# Ver espaço em disco
df -h /output

# Ver permissões
ls -la /output
```

#### 3. Verificar Parquet writer
```bash
# Verificar erros
curl -s localhost:8080/debug/pprof/trace | grep parquet
```

### Recuperação
```bash
# Regenerar output
kineplex-cli regenerate-output --graph-id <id>
```

---

## Runbook 5: Recuperação de Desastre (DR)

### Descrição
Recuperação após perda completa de dados ou cluster.

### Pré-condições
- [ ] Backup disponível
- [ ] Disaster Recovery site
- [ ] RTO < 4 horas

### Passos

#### 1. Restaurar estado
```bash
# Restaurar banco de dados
./restore-db.sh --backup /backup/latest

# Restaurar configuração
./restore-config.sh
```

#### 2. Recriar cluster
```bash
# Criar novos nós
kineplex-cli cluster init --nodes node-1,node-2,node-3

# Verificar saúde
kineplex-cli cluster health
```

#### 3. Validar integridade
```bash
# Verificar grafos
./validate-state.sh
```

### Validação DR
- [ ] RTO < 4 horas
- [ ] Dados consistentes
- [ ] Operações normais

---

##关联到 Alertas e Métricas

| Runbook | Alerta | Métrica |
|---------|--------|---------|
| Falha de nó | node_failure | healthy_nodes |
| Queda de cluster | cluster_down | cluster_availability |
| Degradação | high_latency | latency_p95 |
| Falha output | no_output | parquet_files_created |