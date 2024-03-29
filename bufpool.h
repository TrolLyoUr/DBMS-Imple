// bufpool.h ... buffer pool interface

#include <stdint.h>
#include <stdio.h>
#include "db.h"

// one buffer
typedef struct buffer
{
    UINT table_oid;
    UINT64 page_id;
    UINT pin;
    INT8 use;
    char *data;
} buffer;

// collection of buffers + stats
typedef struct bufPool
{
    UINT nbufs; // how many buffers
    char *strategy;
    buffer *bufs;
    UINT num_partitions; // number of partitions for Grace hash join
} bufPool;

typedef struct bufPool *BufPool;

BufPool initBufPool();
int grabNextSlot(BufPool);
void release_page(BufPool, UINT, UINT64);
int pageInPool(BufPool, UINT, UINT64);
void freeBufPool(BufPool);
