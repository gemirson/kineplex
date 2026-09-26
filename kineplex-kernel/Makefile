# Out-of-tree Kbuild for the KinePlex geometry driver.
obj-$(CONFIG_KINEPLEX) += kineplex.o
kineplex-y := kineplex_main.o kineplex_geometry.o

obj-$(CONFIG_KINEPLEX_KUNIT_TEST) += kineplex_kunit.o

ccflags-y += -I$(src)/include
