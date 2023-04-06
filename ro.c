#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ro.h"
#include "db.h"
#include "bufpool.h"
#include "filecache.h"

BufPool pool;
Conf *cf_curr;
Database *db_curr;
FileCache *file_cache;

void init()
{
    // do some initialization here.

    // example to get the Conf pointer
    cf_curr = get_conf();

    // example to get the Database pointer
    db_curr = get_db();

    pool = initBufPool();
    file_cache = init_file_cache(cf_curr->file_limit);
    printf("init() is invoked.\n");
}

void release()
{
    // optional
    // do some end tasks here.
    // free space to avoid memory leak
    freeBufPool(pool);
    free_file_cache(file_cache);
    printf("release() is invoked.\n");
}

_Table *sel(const UINT idx, const INT cond_val, const char *table_name)
{

    printf("sel() is invoked.\n");
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    // Find the table with the given table name
    Table *table = find_table_by_name(table_name, db_curr);
    if (table == NULL)
    {
        printf("Table not found: %s\n", table_name);
        exit(1);
    }

    if (idx >= table->nattrs)
    {
        printf("Invalid attribute index: %u\n", idx);
        exit(1);
    }

    // Calculate the offset of the attribute in each tuple
    UINT attr_offset = idx * sizeof(INT);

    // Read the table file
    FILE *table_fp = get_file_pointer(file_cache, table->oid, db_curr->path);
    if (table_fp == NULL)
    {
        perror("Fail to open the table file.\n");
        exit(-1);
    }

    UINT tuples_per_page = (cf_curr->page_size - sizeof(UINT64)) / (sizeof(INT) * table->nattrs);
    UINT64 page_id = 0;
    int slot = 0;

    // Intialize the result table
    UINT ntuples = 0;
    _Table *result = malloc(sizeof(_Table) + ntuples * sizeof(Tuple));
    result->nattrs = table->nattrs;

    // Iterate through the pages
    while (fread(&page_id, sizeof(UINT64), 1, table_fp) == 1)
    {
        // Request the page from pool
        slot = request_page(pool, table->oid, page_id);
        if (slot == -1)
        { // page is not already in pool
            if (pool->nfree > 0)
            {
                slot = removeFirstFree(pool);
            }
            else
            {
                slot = grabNextSlot(pool);
                log_release_page(pool->bufs[slot].page_id); // log release
            }
            if (slot < 0)
            {
                fprintf(stderr, "Failed to find slot for Table:%s page:%ld\n", table->name, page_id);
                exit(1);
            }
            pool->nreads++;
            pool->bufs[slot].table_oid = table->oid;
            pool->bufs[slot].page_id = page_id;
            pool->bufs[slot].pin = 1;
            pool->bufs[slot].use = 0;
            pool->bufs[slot].dirty = 0;
            // Read the page from the disk
            fread(pool->bufs[slot].data, cf_curr->page_size - sizeof(UINT64), 1, table_fp);
            log_read_page(page_id); // log read
        }
        else
        { // page is already in pool
            fseek(table_fp, cf_curr->page_size - sizeof(UINT64), SEEK_CUR);
        }

        // Iterate through the tuples in the page
        for (UINT i = 0; i < tuples_per_page; i++)
        {
            // Examine the specific attribute from the tuple
            INT attr_value;

            memcpy(&attr_value, pool->bufs[slot].data + i * sizeof(INT) * table->nattrs + attr_offset, sizeof(INT));
            if (attr_value == cond_val)
            {
                // Store the tuple as result
                ++ntuples;
                result = realloc(result, sizeof(_Table) + ntuples * sizeof(Tuple));
                Tuple t = malloc(sizeof(INT) * result->nattrs);
                result->tuples[ntuples - 1] = t;
                memcpy(t, pool->bufs[slot].data + i * sizeof(INT) * table->nattrs, sizeof(INT) * result->nattrs);
            }
        }
        release_page(pool, table->oid, page_id);
    }
    rewind(table_fp);
    result->ntuples = ntuples;
    return result;

    // return NULL;
}

_Table *join(const UINT idx1, const char *table1_name, const UINT idx2, const char *table2_name)
{

    printf("join() is invoked.\n");
    // write your code to join two tables
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    // Retrieve table pointers
    Table *table1 = find_table_by_name(table1_name, db_curr);
    Table *table2 = find_table_by_name(table2_name, db_curr);

    if (table1 == NULL || table2 == NULL)
    {
        printf("Table not found: %s or %s\n", table1_name, table2_name);
        return NULL;
    }

    if (idx1 >= table1->nattrs || idx2 >= table2->nattrs)
    {
        printf("Invalid attribute index: %u or %u\n", idx1, idx2);
        return NULL;
    }

    // Calculate the number of pages for each table
    UINT tuples_per_page1 = (cf_curr->page_size - sizeof(UINT64)) / (sizeof(INT) * table1->nattrs);
    UINT tuples_per_page2 = (cf_curr->page_size - sizeof(UINT64)) / (sizeof(INT) * table2->nattrs);
    UINT npages1 = (table1->ntuples + tuples_per_page1 - 1) / tuples_per_page1;
    UINT npages2 = (table2->ntuples + tuples_per_page2 - 1) / tuples_per_page2;

    // Initialize the result table
    UINT ntuples = 0;
    UINT nattrs = table1->nattrs + table2->nattrs;
    _Table *result = malloc(sizeof(_Table) + ntuples * sizeof(Tuple));
    result->nattrs = nattrs;

    // Check if buffer slots are enough
    if (cf_curr->buf_slots >= npages1 + npages2)
    {
        // In-memory join (sort-merge join or hash join)
        // Implement either sort-merge join or hash join here
    }
    else
    {
        // Naive nested for-loop join
        return naive_nested_loop_join(idx1, table1, idx2, table2);
    }

    return NULL;
}

Table *find_table_by_name(const char *table_name, Database *db)
{
    for (UINT i = 0; i < db->ntables; i++)
    {
        if (strcmp(db->tables[i].name, table_name) == 0)
        {
            return &db->tables[i];
        }
    }
    return NULL;
}
