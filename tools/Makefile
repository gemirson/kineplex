# SPDX-License-Identifier: GPL-2.0

CLANG ?= clang
CC ?= cc
BPF_CFLAGS ?= -O2 -g -target bpf -D__TARGET_ARCH_x86
MULTIARCH ?= $(shell uname -m)-linux-gnu
ifneq ($(wildcard /usr/include/$(MULTIARCH)/asm/types.h),)
    BPF_CFLAGS += -I/usr/include/$(MULTIARCH)
endif
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
