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

typedef struct hashPartition
{
    UINT table_oid;
    UINT part_id;
    UINT64 num_tuples;
    UINT64 num_pages;
    UINT *page_ids;
} hashPartition;

// collection of buffers + stats
typedef struct bufPool
{
    UINT nbufs; // how many buffers
    char *strategy;
    buffer *bufs;
    UINT num_partitions;  // number of partitions for Grace hash join
    hashPartition *parts; // array of hashPartitions for both tables being joined
} bufPool;

typedef struct bufPool *BufPool;

BufPool initBufPool();
int grabNextSlot(BufPool);
void release_page(BufPool, UINT, UINT64);
int pageInPool(BufPool, UINT, UINT64);
void freeBufPool(BufPool);
