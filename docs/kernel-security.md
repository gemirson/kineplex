# Segurança do device geométrico — FT-096

## Política

O módulo `kineplex_geo.ko` aplica `CAP_NET_ADMIN` a todas as entradas controladas por processo:

| Entrada | Verificação | Falha sem capability |
|---|---|---|
| `open("/dev/kinegeo")` | `capable(CAP_NET_ADMIN)` | `-EPERM` |
| `ioctl` (`GET_INFO`, telemetria e reset) | `capable(CAP_NET_ADMIN)` | `-EPERM` |
| `mmap` do tensor | `capable(CAP_NET_ADMIN)` | `-EPERM` |
| `io_uring` `uring_cmd` | `capable(CAP_NET_ADMIN)` antes de qualquer comando | `-EPERM` |

O callback `uring_cmd` é deny-by-default: mesmo um processo autorizado recebe `-EOPNOTSUPP` enquanto não houver um opcode de produção especificado e validado. Isso evita que uma ABI incompleta seja exposta como superfície de injeção.

A inicialização do módulo não representa uma chamada de processo comum: o carregamento do LKM já é controlado pelo mecanismo de módulos do Linux (`CAP_SYS_MODULE`/política de assinatura). A política de acesso ao motor, depois que o device existe, é exclusivamente `CAP_NET_ADMIN`.

## Erros e validação

A UAPI não retorna sucesso parcial. O caminho de capability é executado antes de copiar dados, consultar estado NUMA, mapear páginas ou interpretar comandos. O processo não privilegiado deve observar exatamente `EPERM`.

O teste de segurança deve ser executado em uma VM Linux com o módulo carregado por uma etapa administrativa:

```sh
# setup privilegiado
sudo insmod kineplex_geo.ko nic_ifname=eth0

# tentativa sem CAP_NET_ADMIN; deve imprimir EPERM
./tests/kernel/capability_smoke.sh /dev/kinegeo

# cleanup privilegiado
sudo rmmod kineplex_geo
```

O teste não deve ser executado com `sudo`, nem receber capability ambient/permitted. Em hosts com file capabilities, confirme com `getcap` que o binário de teste não possui `cap_net_admin`.

## Threat model e limites

- Um tenant não recebe acesso direto ao device; o acesso deve ser mediado pelo serviço privilegiado.
- `mmap` aceita somente o tamanho exato do tensor e remapeia uma página previamente alocada localmente ao NUMA da NIC.
- Comandos desconhecidos retornam `ENOTTY`/`EOPNOTSUPP`, sem executar ponteiros ou offsets fornecidos pelo usuário.
- O módulo deve ser assinado e carregado por uma política de secure boot no ambiente de produção.
- O resultado `EPERM` deve ser comprovado por syscall em kernel suportado; análise estática sozinha não satisfaz o critério de aceite.
