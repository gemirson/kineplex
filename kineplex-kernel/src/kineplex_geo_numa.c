// SPDX-License-Identifier: GPL-2.0
/* FT-093: strict NUMA placement for metric tensor memory. */

#include <linux/gfp.h>
#include <linux/mm.h>
#include <linux/numa.h>
#include <linux/pagemap.h>
#include <linux/slab.h>

#include "kineplex_geo_internal.h"

static bool kineplex_geo_valid_node(int node)
{
	return node >= 0 && node_possible(node);
}

int kineplex_geo_numa_init(struct kineplex_geo_context *ctx,
			   struct device *nic_device)
{
	int node;

	if (!ctx || !nic_device)
		return -EINVAL;

	/* dev_to_node is the source of truth for the NIC's physical locality. */
	node = dev_to_node(nic_device);
	if (!kineplex_geo_valid_node(node))
		return -ENODEV;

	ctx->nic_device = nic_device;
	ctx->numa_node = node;
	ctx->tensor_order = get_order(KINEPLEX_GEO_TENSOR_BYTES);
	mutex_init(&ctx->lock);

	ctx->tensor_cache = kmem_cache_create(
		KINEPLEX_GEO_TENSOR_CACHE_NAME,
		KINEPLEX_GEO_TENSOR_BYTES,
		0,
		SLAB_HWCACHE_ALIGN,
		NULL);
	if (!ctx->tensor_cache)
		return -ENOMEM;

	/* __GFP_THISNODE prohibits a fallback to a remote NUMA node. */
	ctx->tensor_object = kmem_cache_alloc_node(
		ctx->tensor_cache,
		GFP_KERNEL | __GFP_ZERO | __GFP_THISNODE,
		node);
	if (!ctx->tensor_object)
		goto err_cache;

	ctx->tensor_page = alloc_pages_node(
		node,
		GFP_KERNEL | __GFP_ZERO | __GFP_THISNODE,
		ctx->tensor_order);
	if (!ctx->tensor_page)
		goto err_object;

	return 0;

err_object:
	kmem_cache_free(ctx->tensor_cache, ctx->tensor_object);
	ctx->tensor_object = NULL;
err_cache:
	kmem_cache_destroy(ctx->tensor_cache);
	ctx->tensor_cache = NULL;
	return -ENOMEM;
}

void kineplex_geo_numa_destroy(struct kineplex_geo_context *ctx)
{
	if (!ctx)
		return;

	if (ctx->tensor_page) {
		__free_pages(ctx->tensor_page, ctx->tensor_order);
		ctx->tensor_page = NULL;
	}

	if (ctx->tensor_object && ctx->tensor_cache) {
		kmem_cache_free(ctx->tensor_cache, ctx->tensor_object);
		ctx->tensor_object = NULL;
	}

	if (ctx->tensor_cache) {
		kmem_cache_destroy(ctx->tensor_cache);
		ctx->tensor_cache = NULL;
	}

	ctx->nic_device = NULL;
	ctx->numa_node = NUMA_NO_NODE;
}

int kineplex_geo_numa_info(const struct kineplex_geo_context *ctx,
			   struct kineplex_geo_device_info *info)
{
	if (!ctx || !info || !kineplex_geo_valid_node(ctx->numa_node))
		return -ENODEV;

	info->abi_version = KINEPLEX_GEO_ABI_VERSION;
	info->numa_node = (u32)ctx->numa_node;
	info->tensor_order = ctx->tensor_order;
	info->reserved = 0;
	info->tensor_bytes = KINEPLEX_GEO_TENSOR_BYTES;
	return 0;
}

int kineplex_geo_numa_mmap(struct kineplex_geo_context *ctx,
			   struct vm_area_struct *vma)
{
	unsigned long requested = vma->vm_end - vma->vm_start;
	unsigned long expected = PAGE_SIZE << ctx->tensor_order;
	int ret;

	if (!ctx || !vma || !ctx->tensor_page)
		return -ENODEV;
	if (requested != expected)
		return -EINVAL;

	/* The page was allocated with __GFP_THISNODE; never remap another page. */
	vma->vm_flags |= VM_DONTEXPAND | VM_DONTDUMP;
	ret = remap_pfn_range(vma,
			      vma->vm_start,
			      page_to_pfn(ctx->tensor_page),
			      expected,
			      vma->vm_page_prot);
	return ret;
}
