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
        slot = pageInPool(pool, table->oid, page_id);
        if (slot == -1)
        { // page is not already in pool
            slot = grabNextSlot(pool);
            log_release_page(pool->bufs[slot].page_id); // log release
            if (slot < 0)
            {
                fprintf(stderr, "Failed to find slot for Table:%s page:%ld\n", table->name, page_id);
                exit(1);
            }
            pool->bufs[slot].table_oid = table->oid;
            pool->bufs[slot].page_id = page_id;
            pool->bufs[slot].pin = 1;
            pool->bufs[slot].use = 1;
            // Read the page from the disk
            fread(pool->bufs[slot].data, cf_curr->page_size - sizeof(UINT64), 1, table_fp);
            log_read_page(page_id); // log read
        }
        else
        { // page is already in pool
            if (pool->bufs[slot].use < 255)
                pool->bufs[slot].use++;
            pool->bufs[slot].pin = 1;
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
        // Grace hash join
        return grace_hash_join(idx1, table1, idx2, table2);
    }
    else
    {
        // Naive nested for-loop join
        // Determine which table to be the outer loop
        if (npages1 > npages2)
            return naive_nested_loop_join(idx1, table1, idx2, table2);
        else
        {
            return naive_nested_loop_join(idx2, table2, idx1, table1);
        }
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

_Table *block_nested_loop_join(const UINT idx1, Table *table1, const UINT idx2, Table *table2)
{
    // Calculate the offset of the attribute
    UINT attr_offset1 = idx1 * sizeof(INT);
    UINT attr_offset2 = idx2 * sizeof(INT);

    // N-1 pages for table 1 into buffers
    UINT pages_per_block = cf_curr->buf_slots - 1;

    // Open the table file
    FILE *table_fp1 = get_file_pointer(file_cache, table1->oid, db_curr->path);
    if (table_fp1 == NULL)
    {
        perror("Fail to open the table file.\n");
        exit(-1);
    }
    FILE *table_fp2 = get_file_pointer(file_cache, table2->oid, db_curr->path);
    if (table_fp2 == NULL)
    {
        perror("Fail to open the table file.\n");
        exit(-1);
    }

    UINT tuples_per_page1 = (cf_curr->page_size - sizeof(UINT64)) / (sizeof(INT) * table1->nattrs);
    UINT tuples_per_page2 = (cf_curr->page_size - sizeof(UINT64)) / (sizeof(INT) * table2->nattrs);
    UINT64 page_id1 = 0;
    UINT64 page_id2 = 0;
    int slot1 = 0;
    int slot2 = 0;

    // Intialize the result table
    UINT ntuples = 0;
    _Table *result = malloc(sizeof(_Table) + ntuples * sizeof(Tuple));
    result->nattrs = table1->nattrs + table2->nattrs;

    // Iterate through the pages of table1
    while (1)
    {
        for (UINT i = 0; i < pages_per_block; i++)
        {
            if (fread(&page_id1, sizeof(UINT64), 1, table_fp1) == 1)
            {
                // Request the page from pool
                slot1 = pageInPool(pool, table1->oid, page_id1);
                if (slot1 == -1)
                { // page is not already in pool
                    slot1 = grabNextSlot(pool);
                    log_release_page(pool->bufs[slot1].page_id); // log release
                    if (slot1 < 0)
                    {
                        fprintf(stderr, "Failed to find slot for Table:%s page:%ld\n", table1->name, page_id1);
                        exit(1);
                    }
                    pool->bufs[slot1].table_oid = table1->oid;
                    pool->bufs[slot1].page_id = page_id1;
                    pool->bufs[slot1].pin = 1;
                    pool->bufs[slot1].use = 1;
                    // Read the page from the disk
                    fread(pool->bufs[slot1].data, cf_curr->page_size - sizeof(UINT64), 1, table_fp1);
                    log_read_page(page_id1); // log read
                }
                else
                { // page is already in pool
                    fseek(table_fp1, cf_curr->page_size - sizeof(UINT64), SEEK_CUR);
                }
            }
            else
            {
                break;
            }
        }

        // Iterate through the pages of table2
        while (fread(&page_id2, sizeof(UINT64), 1, table_fp2) == 1)
        {
            // Request the page from pool
            slot2 = pageInPool(pool, table2->oid, page_id2);
            if (slot2 == -1)
            { // page is not already in pool
                slot2 = grabNextSlot(pool);
                log_release_page(pool->bufs[slot2].page_id); // log release
                if (slot2 < 0)
                {
                    fprintf(stderr, "Failed to find slot for Table:%s page:%ld\n", table2->name, page_id2);
                    exit(1);
                }
                pool->bufs[slot2].table_oid = table2->oid;
                pool->bufs[slot2].page_id = page_id2;
                pool->bufs[slot2].pin = 1;
                pool->bufs[slot2].use = 1;
                // Read the page from the disk
                fread(pool->bufs[slot2].data, cf_curr->page_size - sizeof(UINT64), 1, table_fp2);
                log_read_page(page_id2); // log read
            }
            else
            { // page is already in pool
                fseek(table_fp2, cf_curr->page_size - sizeof(UINT64), SEEK_CUR);
            }

            // Iterate through the tuples of table1
            for (UINT i = 0; i < tuples_per_page1; i++)
            {
                // Check if the tuple is valid
                if (pool->bufs[slot1].data + (i * table1->nattrs * sizeof(INT)) == 0)
                {
                    continue;
                }

                // Iterate through the tuples of table2
                for (UINT j = 0; j < tuples_per_page2; j++)
                {
                    // Check if the tuple is valid
                    if (pool->bufs[slot2].data + (j * table2->nattrs * sizeof(INT)) == 0)
                    {
                        continue;
                    }

                    // Check if the two tuples are joinable
                    if (memcmp(pool->bufs[slot1].data + (i * table1->nattrs * sizeof(INT) + attr_offset1),
                               pool->bufs[slot2].data + (j * table2->nattrs * sizeof(INT) + attr_offset2),
                               sizeof(INT)) == 0)
                    {
                        // Allocate space for the new tuple
                        ntuples++;
                        result = realloc(result, sizeof(_Table) + result->ntuples * sizeof(Tuple));

                        // Copy the attributes from the two tuples as result
                        Tuple t = malloc(sizeof(INT) * result->nattrs);
                        result->tuples[ntuples - 1] = t;
                        memcpy(t,
                               pool->bufs[slot1].data + (i * table1->nattrs * sizeof(INT)),
                               table1->nattrs * sizeof(INT));
                        memcpy(t + table1->nattrs * sizeof(INT),
                               pool->bufs[slot2].data + (j * table2->nattrs * sizeof(INT)),
                               table2->nattrs * sizeof(INT));
                    }
                }
            }
            release_page(pool, table2->oid, page_id2);
        }
        rewind(table_fp2);
        release_page(pool, table1->oid, page_id1);
    }
    rewind(table_fp1);
    result->ntuples = ntuples;
    return result;
}

_Table *grace_hash_join(_Table *table1, _Table *table2, int attr1, int attr2)
{
    unsigned int num_partitions = (pool->nbufs - 2) / 2;
    UINT64 cur_page_id;

    // Partition the tables
    _Table **table1_partitions = partition_table(table1, attr1, num_partitions, simple_hash);
    _Table **table2_partitions = partition_table(table2, attr2, num_partitions, simple_hash);

    // Initialize result table
    _Table *result = malloc(sizeof(_Table));
    result->nattrs = table1->nattrs + table2->nattrs;
    result->ntuples = 0;

    // Perform join for each pair of partitions
    for (unsigned int i = 0; i < num_partitions; i++)
    {
        _Table *partition1 = table1_partitions[i];
        _Table *partition2 = table2_partitions[i];

        // Load partitions into buffer pool
        for (UINT64 j = 0; j < partition1->ntuples; j++)
        {
            cur_page_id = j;
            request_page(pool, i, cur_page_id);
        }

        // Perform join
        for (UINT64 j = 0; j < partition2->ntuples; j++)
        {
            cur_page_id = j;
            int buf_slot = request_page(buf_pool, i, cur_page_id);

            // Check if the tuples in partition2 match any tuple in partition1
            for (UINT64 k = 0; k < partition1->ntuples; k++)
            {
                if (partition2->tuples[j][attr2] == buf_pool->bufs[buf_slot].data[k * sizeof(INT)])
                {
                    // Add joined tuple to the result table
                    result->ntuples++;
                    result->tuples = realloc(result->tuples, result->ntuples * sizeof(Tuple));

                    Tuple new_tuple = malloc(sizeof(INT) * result->nattrs);
                    memcpy(new_tuple, partition1->tuples[k], sizeof(INT) * table1->nattrs);
                    memcpy(new_tuple + table1->nattrs, partition2->tuples[j], sizeof(INT) * table2->nattrs);

                    result->tuples[result->ntuples - 1] = new_tuple;
                }
            }

            // Release the buffer pool slot
            release_page(buf_pool, i, cur_page_id);
        }

        // Release partition1 from the buffer pool
        for (UINT64 j = 0; j < partition1->ntuples; j++)
        {
            cur_page_id = j;
            release_page(buf_pool, i, cur_page_id);
        }
    }

    // Cleanup
    free_partitions(table1_partitions, num_partitions);
    free_partitions(table2_partitions, num_partitions);
    freeBufPool(buf_pool);

    return result;
}

typedef UINT64 (*hash_function)(INT, UINT64);

_Table **partition_table(_Table *table, int attr, UINT64 num_partitions, hash_function hash_func, BufPool buf_pool)
{
    // Allocate memory for the partition tables
    _Table **partitions = malloc(num_partitions * sizeof(_Table *));

    // Initialize the partition tables
    for (UINT64 i = 0; i < num_partitions; i++)
    {
        partitions[i] = malloc(sizeof(_Table));
        partitions[i]->nattrs = table->nattrs;
        partitions[i]->ntuples = 0;
        partitions[i]->tuples[0] = NULL;
    }

    // Partition the input table
    for (UINT64 i = 0; i < table->ntuples; i++)
    {
        Tuple tuple = table->tuples[i];
        INT attr_value = tuple[attr];
        UINT64 partition_idx = hash_func(attr_value, num_partitions);

        // Add the tuple to the corresponding partition
        _Table *partition = partitions[partition_idx];
        partition->ntuples++;

        // Request a page from the buffer pool for the partition
        UINT64 cur_page_id = partition->ntuples - 1;
        int slot = request_page(buf_pool, partition_idx, cur_page_id);

        // Write the tuple to the page in the buffer pool
        memcpy(buf_pool->bufs[slot].data, tuple, sizeof(Tuple));

        // Release the page back to the buffer pool
        release_page(buf_pool, partition_idx, cur_page_id);
    }

    return partitions;
}

// Simple hash function for attribute values
UINT64 hash_func(UINT64 key, UINT64 num_partitions)
{
    const double A = 0.6180339887; // Constant A should be an irrational number between 0 and 1
    double fractional_part = key * A - (int64_t)(key * A);
    return (UINT64)(num_partitions * fractional_part);
}
