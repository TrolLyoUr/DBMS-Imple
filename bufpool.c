// bufpool.c ... buffer pool implementation

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include "bufpool.h"

static unsigned int clock = 0;

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
	UINT oid;
	UINT64 pid;
	oid = table;
	pid = page;
	for (i = 0; i < pool->nbufs; i++)
	{
		if (oid == pool->bufs[i].table_oid && pid == pool->bufs[i].page_id)
		{
			return i;
		}
	}
	return -1;
}

// removeFirstFree(pool)
// - use the first slot on the free list
// - the slot is removed from the free list
//   by moving all later elements down

int removeFirstFree(BufPool pool)
{
	int v, i;
	assert(pool->nfree > 0);
	v = pool->freeList[0];
	for (i = 0; i < pool->nfree - 1; i++)
		pool->freeList[i] = pool->freeList[i + 1];
	pool->nfree--;
	return v;
}

// getNextSlot(pool)
// - finds the "best" unused buffer pool slot
// - "best" is determined by the replacement strategy
// - if the replaced page is dirty, write it out
// - initialise the chosen slot to hold the new page
// - if there are no available slots, return -1

int grabNextSlot(BufPool pool)
{
	int slot;
	int i;
	bool free_pin;

	// get next available according to cycle counter
	slot = -1;
	free_pin = true;
	while (free_pin)
	{
		free_pin = false;
		for (i = 0; i < pool->nbufs; i++)
		{
			int s;
			s = (clock + i) % pool->nbufs;
			if (pool->bufs[s].pin == 0 && pool->bufs[s].use == 0)
			{
				slot = s;
				break;
			}
			else
			{
				if (pool->bufs[s].pin == 0)
					free_pin = true;
				if (pool->bufs[s].use > 0)
					pool->bufs[s].use--;
			}
		}
	}
	if (slot >= 0)
		clock = slot;

	if (slot >= 0 && pool->bufs[slot].dirty)
	{
		pool->nwrites++;
		pool->bufs[slot].dirty = false;
	}
	return slot;
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
	newPool->nrequests = 0;
	newPool->nreleases = 0;
	newPool->nhits = 0;
	newPool->nreads = 0;
	newPool->nwrites = 0;
	newPool->nfree = nbufs;
	newPool->nused = 0;
	newPool->freeList = malloc(nbufs * sizeof(UINT));
	assert(newPool->freeList != NULL);
	newPool->usedList = malloc(nbufs * sizeof(UINT));
	assert(newPool->usedList != NULL);
	newPool->bufs = malloc(nbufs * (sizeof(struct buffer)));
	assert(newPool->bufs != NULL);

	int i;
	for (i = 0; i < nbufs; i++)
	{
		newPool->bufs[i].table_oid = 0;
		newPool->bufs[i].page_id = 0;
		newPool->bufs[i].pin = 0;
		newPool->bufs[i].use = 0;
		newPool->bufs[i].dirty = false;
		newPool->bufs[i].data = malloc(cf->page_size - sizeof(UINT64)); // size of array equal to the number of tuples can fit in a page);
		newPool->freeList[i] = i;
		newPool->usedList[i] = -1;
	}
	return newPool;
}

int request_page(BufPool pool, UINT table, UINT64 page)
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

void release_page(BufPool pool, UINT table, UINT64 page)
{
	pool->nreleases++;

	int i;
	i = pageInPool(pool, table, page);
	assert(i >= 0);
	// increment use counter
	if (pool->bufs[i].use < 255)
		pool->bufs[i].use++;
	pool->bufs[i].pin--;
}

// showPoolUsage(pool)
// - prints statistics counters for buffer pool

void showPoolUsage(BufPool pool)
{
	assert(pool != NULL);
	printf("#requests: %d\n", pool->nrequests);
	printf("#releases: %d\n", pool->nreleases);
	printf("#hits    : %d\n", pool->nhits);
	printf("#reads   : %d\n", pool->nreads);
	printf("#writes  : %d\n", pool->nwrites);
}

void freeBufPool(BufPool pool)
{
	int i;
	for (i = 0; i < pool->nbufs; i++)
	{
		free(pool->bufs[i].data);
	}
	free(pool->bufs);
	free(pool->freeList);
	free(pool->usedList);
	free(pool);
}