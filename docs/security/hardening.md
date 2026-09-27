# Guia de Operação Segura Multi-tenant e Baseline de Hardening - FT-088

## Visão Geral

Este documento consolida a política de configuração segura, limites, gestão de chaves/certificados e checklist de auditoria operacional para ambientes multi-tenant do KinePlex.

## Checklist de Hardening

### 1. Autenticação e Autorização

- [ ] **mTLS habilitado para comunicação interna**
  ```yaml
  mtls:
    enabled: true
    client_cert_required: true
    min_tls_version: "1.3"
  ```

- [ ] **API authentication obrigatório**
  ```yaml
  auth:
    api_key_required: true
    token_expiry_hours: 24
  ```

- [ ] **RBAC configurado por tenant**
  ```yaml
  rbac:
    tenant_isolation: true
    role_per_tenant: true
  ```

### 2. Isolamento de Recursos

- [ ] **Cotas por tenant configuradas**
  ```yaml
  tenant_quotas:
    max_concurrent_graphs: 10
    max_memory_mb: 4096
    max_fuel_per_execution: 1000000
    rate_limit_per_minute: 100
  ```

- [ ] **Limites de execução Wasm enforced**
  ```yaml
  wasm_limits:
    max_memory_mb: 512
    max_fuel: 1000000
    max_execution_time_ms: 30000
  ```

- [ ] **Isolamento de rede por tenant**
  ```yaml
  network_isolation:
    vpc_per_tenant: true
    private_subnets: true
  ```

### 3. Criptografia

- [ ] **Dados em repouso criptografados**
  ```bash
  # Verificar criptografia de disco
  lsblk -o NAME,FSTYPE,CRYPT /dev/sda
  ```

- [ ] **Dados em trânsito TLS 1.3**
  ```yaml
  tls:
    min_version: "1.3"
    cipher_suites:
      - TLS_AES_256_GCM_SHA384
      - TLS_CHACHA20_POLY1305_SHA256
  ```

- [ ] **Secrets em vault**
  ```bash
  # Usar vault para secrets
  export VAULT_ADDR="https://vault.internal"
  ```

### 4. Logging e Auditoria

- [ ] **Logs de auditoria habilitados**
  ```yaml
  audit:
    log_all_requests: true
    log_auth_attempts: true
    log_graph_executions: true
    retention_days: 365
  ```

- [ ] **Logs centralizados**
  ```yaml
  logging:
    backend: "elasticsearch"
    level: "info"
    format: "json"
  ```

- [ ] **Máscara de dados sensíveis**
  ```yaml
  data_masking:
    tenant_id: "mask"
    credentials: "mask"
    pii: "mask"
  ```

## Política de Rotação de Credenciais

### Certificados

| Tipo | Validade | Rotação |
|------|----------|---------|
| Server cert | 90 dias | 30 dias antes do expiry |
| Client cert | 30 dias | 7 dias antes do expiry |
| CA cert | 1 ano | 60 dias antes do expiry |

### Procedure de Rotação

```bash
# 1. Gerar novo certificado
./generate_cert.sh --type client --tenant tenant-1

# 2. Distribuir para nós
./distribute_certs.sh --nodes node-1,node-2,node-3

# 3. Verificar conexão
curl -v https://kineplex.internal/health

# 4. Revogar certificado antigo
./revoke_cert.sh --serial <old_serial>
```

### Revogação de Emergência

```bash
# Revogação imediata
./emergency_revoke.sh --tenant tenant-1 --reason "security_incident"

# Verificar certificados revogados
./check_revocation.sh
```

## Política de Gestão de Keys

### Idempotency Keys

```yaml
idempotency:
  ttl_hours: 24
  max_keys_per_tenant: 1000
  key_format: "uuid_v4"
```

### API Keys

```yaml
api_keys:
  rotation_days: 30
  max_keys_per_tenant: 5
  prefix: "kp_live_"
```

## Auditoria Operacional

### Verificações Diárias

- [ ] Verificar certificados próximos do expiry
- [ ] Revisar logs de autenticação falhada
- [ ] Verificar uso de quota por tenant
- [ ] Confirmar backup executado

### Verificações Semanais

- [ ] Revisar access patterns suspeitos
- [ ] Verificar compliance de hardening
- [ ] Testar procedure de revogação
- [ ] Atualizar documentação de segurança

### Verificações Mensais

- [ ] Audit log review
- [ ] Penetration testing
- [ ] Security patch review
- [ ] Compliance checklist

## Registro de Revisão de Segurança

| Data | Revisor | Alterações | Status |
|------|---------|------------|--------|
| 2024-01-15 | Team | Baseline inicial | Aprovado |
|            |         |            |         |

## Baseline de Configuração Segura

```yaml
# Base configuration - não modificar diretamente
# Herdar via config management

production_hardening:
  # Autenticação
  auth:
    enabled: true
    mtls_required: true
    api_key_required: true
    token_expiry: 86400
    
  # Rede
  network:
    tls_min_version: "1.3"
    ciphers: ["TLS_AES_256_GCM_SHA384"]
    enforce_https: true
    
  # Recursos
  quotas:
    tenant:
      max_concurrent: 10
      max_memory_mb: 4096
      max_fuel: 1000000
      
  # Logging
  audit:
    enabled: true
    retention_days: 365
    log_graphs: true
    log_auth: true
```

## Validação

Para FT-088, os critérios são:
- [ ] Checklist de hardening versionado por ambiente
- [ ] Política de rotação documentada
- [ ] Procedure de revogação testada
- [ ] Registro de revisão de segurança
- [ ] Baseline adotado como gate de produção