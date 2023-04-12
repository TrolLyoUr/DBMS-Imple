// bufpool.c ... buffer pool implementation

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include "bufpool.h"

static unsigned int nvb = 0;

int pageInPool(BufPool pool, UINT table, UINT64 page)
{
	int i;
	for (i = 0; i < pool->nbufs; i++)
	{
		if (table == pool->bufs[i].table_oid && page == pool->bufs[i].page_id)
		{
			return i;
		}
	}
	return -1;
}

// Interface Functions

// initBufPool(nbufs,strategy)
// - initialise a buffer pool with nbufs
// - buffer pool uses supplied replacement strategy

BufPool initBufPool()
{
	BufPool newPool;

	Conf *cf = get_conf();
	UINT nbufs = cf->buf_slots;
	UINT num_partitions = floor((nbufs) / 2);

	newPool = malloc(sizeof(struct bufPool));
	assert(newPool != NULL);
	newPool->nbufs = nbufs;
	newPool->strategy = cf->buf_policy;
	newPool->bufs = malloc(nbufs * (sizeof(struct buffer)));
	assert(newPool->bufs != NULL);
	newPool->num_partitions = num_partitions;
	newPool->parts = malloc(num_partitions * 2 * (sizeof(struct hashPartition)));
	assert(newPool->parts != NULL);

	int i;
	for (i = 0; i < nbufs; i++)
	{
		newPool->bufs[i].table_oid = 0;
		newPool->bufs[i].page_id = 0;
		newPool->bufs[i].pin = 0;
		newPool->bufs[i].use = 0;
		newPool->bufs[i].data = malloc(cf->page_size - sizeof(UINT64)); // size of array equal to the number of tuples can fit in a page);
		newPool->bufs[i].data = NULL;
	}
	for (i = 0; i < num_partitions * 2; i++)
	{
		newPool->parts[i].table_oid = 0;
		newPool->parts[i].part_id = 0;
		newPool->parts[i].num_tuples = 0;
		newPool->parts[i].num_pages = 0;
		newPool->parts[i].page_ids = NULL;
	}
	return newPool;
}

int grabNextSlot(BufPool pool)
{
	int slot;
	while (true)
	{
		if (pool->bufs[nvb].pin == 0 && pool->bufs[nvb].use == 0)
		{
			slot = nvb;
			nvb = (nvb + 1) % pool->nbufs;
			return slot;
		}
		else
		{
			if (pool->bufs[nvb].use > 0)
				pool->bufs[nvb].use--;
			nvb = (nvb + 1) % pool->nbufs;
		}
	}
}

void release_page(BufPool pool, UINT table, UINT64 page)
{
	int i;
	i = pageInPool(pool, table, page);
	assert(i >= 0);
	pool->bufs[i].pin = 0;
}

void freeBufPool(BufPool pool)
{
	int i;
	for (i = 0; i < pool->nbufs; i++)
	{
		free(pool->bufs[i].data);
	}
	free(pool->bufs);
	free(pool);
}