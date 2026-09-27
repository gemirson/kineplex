# SPDX-License-Identifier: GPL-2.0
# Out-of-tree KinePlex geometric engine module.

obj-m += kineplex_geo.o
kineplex_geo-y := \
	src/kineplex_geo_main.o \
	src/kineplex_geo_numa.o \
	src/kineplex_geo_telemetry.o

ccflags-y += -I$(src)/include

KERNELRELEASE ?= $(shell uname -r)
KDIR ?= /lib/modules/$(KERNELRELEASE)/build
PWD := $(shell pwd)

.PHONY: all modules clean install

all modules:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean

install: modules
	$(MAKE) -C $(KDIR) M=$(PWD) modules_install
