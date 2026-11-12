# SPDX-License-Identifier: GPL-2.0

CLANG ?= clang
ARCH_INC := $(shell [ -d /usr/include/x86_64-linux-gnu ] && echo "-I/usr/include/x86_64-linux-gnu")
BPF_CFLAGS ?= -O2 -g -target bpf -D__TARGET_ARCH_x86 $(ARCH_INC)
UAPI_INC := -I../kineplex-kernel/include/uapi

.PHONY: all bpf loader clean

all: bpf loader

bpf: ../bpf/kineplex_geo_xdp.bpf.o

../bpf/kineplex_geo_xdp.bpf.o: ../bpf/kineplex_geo_xdp.bpf.c ../bpf/bpf_helpers.h
	$(CLANG) $(BPF_CFLAGS) -I../bpf -c $< -o $@

loader: kineplex_geo_loader

kineplex_geo_loader: kineplex_geo_loader.c
	$(CC) -O2 -g -Wall -Wextra -Werror $(UAPI_INC) \
		$< -o $@ $$(pkg-config --cflags --libs libbpf)

clean:
	$(RM) ../bpf/kineplex_geo_xdp.bpf.o kineplex_geo_loader
