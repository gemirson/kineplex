# SPDX-License-Identifier: GPL-2.0
# Out-of-tree Kbuild for the KinePlex geometry driver.

obj-$(CONFIG_KINEPLEX) += kineplex.o
kineplex-y := kineplex_main.o kineplex_geometry.o kineplex_math.o kineplex_homotopy.o kineplex_uring.o kineplex_debugfs.o

obj-$(CONFIG_KINEPLEX_KUNIT_TEST) += kineplex_kunit.o

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
