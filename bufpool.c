// bufpool.c ... buffer pool implementation

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include "bufpool.h"

static unsigned int nvb = 0;

// Helper Functions (private)

// pageInBuf(pool,slot)
// - return the name of the page current stored in specified slot

// static UINT64 pageInBuf(BufPool pool, int slot)
// {
// 	UINT64 pid;
// 	pid = pool->bufs[slot].page_id;
// 	if (pid == 0)
// 		return -1;
// 	else
// 		return pid;
// }

// pageInPool(BufPool pool, char rel, int page)
// - check whether page from rel is already in the pool
// - returns the slot containing this page, else returns -1

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

	newPool = malloc(sizeof(struct bufPool));
	assert(newPool != NULL);
	newPool->nbufs = nbufs;
	newPool->strategy = cf->buf_policy;
	newPool->bufs = malloc(nbufs * (sizeof(struct buffer)));
	assert(newPool->bufs != NULL);
	newPool->parts = malloc(nbufs * (sizeof(struct hashPartition)));

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
	return newPool;
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

// Functions for hash join
int request_page_hash(BufPool pool, UINT table, UINT64 page, UINT partition_num)
{
	int slot;
	pool->nrequests++;
	slot = pageInPool(pool, table, page);
	if (slot >= 0)
	{
		pool->nhits++;
		pool->bufs[slot].pin++;
	}
	// showPoolState(pool); // for debugging
	return slot;
}