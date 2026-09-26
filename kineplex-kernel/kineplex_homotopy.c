// SPDX-License-Identifier: GPL-2.0
#include <linux/atomic.h>
#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/slab.h>

#include "include/kineplex.h"

static struct kmem_cache *kineplex_homotopy_cache;
static atomic64_t homotopy_allocations = ATOMIC64_INIT(0);
static atomic64_t homotopy_frees = ATOMIC64_INIT(0);
static atomic_t homotopy_in_use = ATOMIC_INIT(0);

int kineplex_homotopy_init(void)
{
	if (kineplex_homotopy_cache)
		return -EEXIST;
	kineplex_homotopy_cache = kmem_cache_create("kineplex_homotopy_cache",
						 sizeof(struct kineplex_homotopy),
						 0, SLAB_HWCACHE_ALIGN, NULL);
	if (!kineplex_homotopy_cache)
		return -ENOMEM;
	atomic64_set(&homotopy_allocations, 0);
	atomic64_set(&homotopy_frees, 0);
	atomic_set(&homotopy_in_use, 0);
	return 0;
}
EXPORT_SYMBOL_GPL(kineplex_homotopy_init);

void kineplex_homotopy_exit(void)
{
	struct kmem_cache *cache = kineplex_homotopy_cache;

	if (!cache)
		return;
	/* Every caller must release before module unload; fail loudly if not. */
	if (WARN_ON(atomic_read(&homotopy_in_use) != 0))
		return;
	kineplex_homotopy_cache = NULL;
	kmem_cache_destroy(cache);
}
EXPORT_SYMBOL_GPL(kineplex_homotopy_exit);

struct kineplex_homotopy *kineplex_homotopy_alloc(gfp_t flags)
{
	struct kineplex_homotopy *homotopy;
	struct kmem_cache *cache = READ_ONCE(kineplex_homotopy_cache);

	if (unlikely(!cache))
		return NULL;
	homotopy = kmem_cache_alloc(cache, flags);
	if (!homotopy)
		return NULL;
	memset(homotopy, 0, sizeof(*homotopy));
	atomic64_inc(&homotopy_allocations);
	atomic_inc(&homotopy_in_use);
	return homotopy;
}
EXPORT_SYMBOL_GPL(kineplex_homotopy_alloc);

void kineplex_homotopy_free(struct kineplex_homotopy *homotopy)
{
	struct kmem_cache *cache = READ_ONCE(kineplex_homotopy_cache);

	if (unlikely(!homotopy || !cache))
		return;
	kmem_cache_free(cache, homotopy);
	atomic64_inc(&homotopy_frees);
	atomic_dec(&homotopy_in_use);
}
EXPORT_SYMBOL_GPL(kineplex_homotopy_free);

void kineplex_homotopy_stats(u64 *allocations, u64 *frees, u32 *in_use)
{
	if (allocations)
		*allocations = atomic64_read(&homotopy_allocations);
	if (frees)
		*frees = atomic64_read(&homotopy_frees);
	if (in_use)
		*in_use = (u32)atomic_read(&homotopy_in_use);
}
EXPORT_SYMBOL_GPL(kineplex_homotopy_stats);
