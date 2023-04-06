// bufpool.h ... buffer pool interface

#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>
#include "db.h"

// one buffer
typedef struct buffer
{
    UINT table_oid;
    UINT64 page_id;
    UINT pin;
    INT8 use;
    bool dirty;
    char *data;
} buffer;

// collection of buffers + stats
typedef struct bufPool
{
    UINT nbufs; // how many buffers
    char *strategy;
    UINT nrequests; // stats counters
    UINT nreleases;
    UINT nhits;
    UINT nreads;
    UINT nwrites;
    UINT *freeList; // list of completely unused bufs
    UINT nfree;
    UINT *usedList; // implements replacement strategy
    UINT nused;
    buffer *bufs;
} bufPool;

typedef struct bufPool *BufPool;

BufPool initBufPool();
int request_page(BufPool, UINT, UINT64);
void release_page(BufPool, UINT, UINT64);
int pageInPool(BufPool, UINT, UINT64);
int removeFirstFree(BufPool);
int grabNextSlot(BufPool);
void showPoolUsage(BufPool);
void showPoolState(BufPool);
void freeBufPool(BufPool);
