# KinePlex Geometric Kernel Driver v0.3

O módulo `kineplex_geo.ko` implementa as FT-093 a FT-096. Ele é um módulo Linux out-of-tree; o checkout não depende de uma árvore Rust-for-Linux completa, portanto as APIs de alocação e segurança usam as APIs estáveis do kernel C (`dev_to_node`, `kmem_cache_alloc_node`, `alloc_pages_node`, `capable`). A política é equivalente e pode ser portada para Rust-for-Linux quando o target kernel adotar essa árvore.

## Componentes

- `src/kineplex_geo_numa.c`: descobre o NUMA da NIC e usa `__GFP_THISNODE` tanto no slab quanto nas páginas remapeáveis.
- `src/kineplex_geo_telemetry.c`: contadores `alloc_percpu`, atualizados somente no CPU corrente; snapshots somam no caminho de controle.
- `src/kineplex_geo_main.c`: device `/dev/kinegeo`, UAPI, mmap, ioctl, capability enforcement e callback `uring_cmd` deny-by-default.
- `../bpf/kineplex_geo_xdp.bpf.c`: programa XDP com `BPF_MAP_TYPE_PERCPU_ARRAY`.
- `../tools/kineplex_geo_loader.c`: attach/detach via libbpf e lifetime não pinado do `bpf_link`.

## Build

```sh
# Em uma VM Linux com headers do kernel em execução:
make -C kineplex-kernel KDIR=/lib/modules/$(uname -r)/build
make -C tools bpf
make -C tools loader # requer libbpf-dev e pkg-config
```

O módulo exige uma NIC com NUMA node válido. Para carregar:

```sh
sudo insmod kineplex-kernel/kineplex_geo.ko nic_ifname=eth0
```

Se `dev_to_node(eth0)` retornar `NUMA_NO_NODE`, o módulo falha com `ENODEV`; não há fallback remoto silencioso.

## Validação local sem kernel headers

```sh
tests/kernel/static_validation.sh
```

Esse teste verifica contratos de fonte, compila o teste de capability e compila o objeto BPF quando clang suporta o target BPF. Ele não substitui o build do módulo, KUnit, insmod, attach XDP ou `perf c2c`.

## Evidência exigida no ambiente alvo

1. `cat /sys/module/kineplex_geo/parameters/nic_ifname` e `ioctl(GET_INFO)` devem mostrar o mesmo NUMA node da NIC.
2. `perf c2c record`/`perf c2c report` no caminho do tensor deve ser executado com carga RX representativa; a análise deve confirmar zero acessos remotos no caminho crítico.
3. `bpftool map show` deve identificar o mapa como per-CPU e `bpftool link show` deve mostrar o link apenas enquanto o loader estiver ativo.
4. Enviar `SIGTERM` ou matar o loader deve remover o link; executar `bpftool link show` depois não pode mostrar link residual.
5. `tests/kernel/capability_smoke.sh /dev/kinegeo` deve retornar `EPERM` como usuário sem `CAP_NET_ADMIN`.
6. Os resultados devem incluir kernel release, modelo da NIC, máscara NUMA, número de CPUs RX, hash do `.ko` e hash do objeto BPF.
