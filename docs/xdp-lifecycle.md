# Ciclo de vida eBPF/XDP — FT-095

## Ownership

`tools/kineplex_geo_loader` é o proprietário do `bpf_link` retornado por `bpf_program__attach_xdp`. O link **não é pinado** em bpffs. Portanto, o kernel libera o link quando o processo fecha o descritor; isso cobre saída normal, `SIGINT`, `SIGTERM`, `SIGHUP` e encerramento inesperado do processo.

A ordem de cleanup é determinística:

1. parar o loop de telemetria;
2. destruir `bpf_link` com `bpf_link__destroy` — detach explícito;
3. fechar `bpf_object`;
4. fechar `/dev/kinegeo`.

## Proteção contra unload do módulo

O loader mantém `/dev/kinegeo` aberto durante todo o lease do link. Como o device pertence ao módulo, o contador de referências do módulo impede um `rmmod` inseguro enquanto o loader está ativo. A operação correta é enviar `SIGTERM` ao loader; ele destaca o XDP antes de liberar o device.

Isso evita a situação em que o programa XDP continua ativo sem o contrato de ABI do driver. O loader também consulta `KINEPLEX_GEO_IOC_GET_INFO` em cada ciclo; qualquer falha ou incompatibilidade de ABI causa detach imediato.

## Segurança operacional

- O programa BPF não é pinado nem aceito de tenant.
- O caminho de produção deve carregar somente artefato assinado e verificado por CI.
- O loader falha se a interface não existir, se `/dev/kinegeo` não estiver disponível ou se a ABI divergir.
- O attach é feito com `bpf_program__attach_xdp`, e o erro de attach encerra sem deixar link parcialmente instalado.
- O detach é idempotente no caminho de saída.

## Limite verificável

Um crash do processo do loader fecha o FD do link e o kernel remove o programa automaticamente. Um crash do próprio kernel não pode ser tratado por código de usuário; deve ser coberto por watchdog, kexec/crash recovery e testes de kernel. A validação em ambiente alvo deve registrar `bpftool link show` antes/depois e comprovar que não há link residual.
