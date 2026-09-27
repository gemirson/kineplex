# KinePlex Kernel Driver (v0.3)

This directory contains the Linux out-of-tree drivers for the KinePlex kernel features. It is deliberately separate from the Rust userspace workspace: Linux kernel code cannot link against `std`, Tokio, Arrow, or Wasmtime.

---

## 1. Drivers e Módulos

### `kineplex.ko` — Driver Geométrico e Roteamento (FT-066 a FT-070)

- **FT-066 — Resolução Geodésica Assíncrona:** O worker de geometria consome telemetria através de `kineplex_geometry_update_telemetry()`, recalculando a métrica Q16.16 e símbolos de Christoffel em uma workqueue de alta prioridade (`WQ_HIGHPRI | WQ_UNBOUND | WQ_MEM_RECLAIM`). Cada passo de integração chama `cond_resched()` e rejeita trabalhos maiores que `KINEPLEX_MAX_GEODESIC_STEPS`.
- **FT-067 — Alocação Slab para Homotopias:** Objetos de deformação transiente são alocados a partir do `kmem_cache` customizado `kineplex_homotopy_cache` e zerados antes da publicação. A destruição do módulo garante liberação sem vazamentos.
- **FT-068 — Sandbox Matemático Anti-Pânico:** Todas as divisões passam por helpers Q16.16 seguros. A inversão de matriz métrica 3x3 rejeita determinantes nulos com `-EDOM`.
- **FT-069 — Comando io_uring para Roteamento:** O módulo registra `/dev/kineplex` com operações `.uring_cmd` (`IORING_OP_URING_CMD` com seletor `IORING_OP_KINEPLEX_ROUTE`).
- **FT-070 — Debugfs de Topologia Global:** Cria `/sys/kernel/debug/kineplex/curvature_tensor` e `/sys/kernel/debug/kineplex/active_geodesics` utilizando RCU para leitura não bloqueante.

### `kineplex_geo.ko` — Controle Geométrico, NUMA e XDP (FT-093 a FT-096)

- **FT-093 — Alocação NUMA Estrita:** `src/kineplex_geo_numa.c` descobre o nó NUMA da NIC e usa `__GFP_THISNODE` no slab e páginas remapeáveis.
- **FT-094 — Telemetria XDP per-CPU:** `src/kineplex_geo_telemetry.c` contadores `alloc_percpu` atualizados exclusivamente na CPU corrente.
- **FT-095 — Gerenciamento de Ciclo de Vida eBPF:** `../bpf/kineplex_geo_xdp.bpf.c` e `../tools/kineplex_geo_loader.c` para attach/detach de `bpf_link` não pinado.
- **FT-096 — Enforcement de CAP_NET_ADMIN:** `src/kineplex_geo_main.c` com dispositivo `/dev/kinegeo`, ioctl, mmap e validação de `capable(CAP_NET_ADMIN)`.

---

## 2. Compilação

Para compilar contra os headers do kernel instalado:

```sh
# Compilação dos módulos
make -C /lib/modules/$(uname -r)/build M=$PWD CONFIG_KINEPLEX=m CONFIG_KINEPLEX_KUNIT_TEST=m modules

# Ou utilizando o Makefile local:
make KDIR=/lib/modules/$(uname -r)/build

# Compilar ferramentas auxiliares e BPF:
make -C ../tools bpf
make -C ../tools loader # requer libbpf-dev e pkg-config
```

### Validação estática sem kernel headers:
```sh
../tests/kernel/static_validation.sh
```
