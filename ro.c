#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "ro.h"
#include "db.h"
#include "bufpool.h"
#include "filecache.h"
#include "hashtab.h"

BufPool pool;
Conf *cf_curr;
Database *db_curr;
FileCache *file_cache;
HashTable *ht;

typedef UINT64 (*hash_function)(UINT64, UINT);

// Function Declarations
Table *find_table_by_name(const char *table_name, Database *db);
_Table *block_nested_loop_join(const UINT idx1, Table *table1, const UINT idx2, Table *table2);
_Table *join(const UINT idx1, const char *table1_name, const UINT idx2, const char *table2_name);
_Table *sel(const UINT idx, const INT cond_val, const char *table_name);
_Table *grace_hash_join(const UINT attr1, Table *table1, const UINT attr2, Table *table2);
_Table **partition_table(Table *table, int attr, hash_function hash_func, int table_idx);
UINT64 hash_func(UINT64 key, UINT num_partitions);
void free_partitions(_Table **partitions, UINT num_partitions);
void free_result_table(_Table *result_table);

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

Table *find_table_by_name(const char *table_name, Database *db)
{
    for (UINT i = 0; i < db->ntables; i++)
    {
        if (strcmp(db->tables[i].name, table_name) == 0)
        {
            return (&db->tables[i]);
        }
    }
    return NULL;
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

    // Check if buffer slots are enough
    if (cf_curr->buf_slots >= npages1 + npages2)
    {
        // In-memory join (sort-merge join or hash join)
        // Implement either sort-merge join or hash join here
        // Grace hash join
        printf("Grace hsah join() is invoked.\n");
        return grace_hash_join(idx1, table1, idx2, table2);
    }
    else
    {
        // Naive nested for-loop join
        // Determine which table to be the outer loop
        printf("Block nested join() is invoked.\n");
        if (npages1 < npages2)
            return block_nested_loop_join(idx1, table1, idx2, table2);
        else
        {
            return block_nested_loop_join(idx2, table2, idx1, table1);
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
    UINT npages1 = (table1->ntuples + tuples_per_page1 - 1) / tuples_per_page1;
    UINT npages2 = (table2->ntuples + tuples_per_page2 - 1) / tuples_per_page2;
    UINT tuples_left1 = table1->ntuples % tuples_per_page1 ? table1->ntuples % tuples_per_page1 : tuples_per_page1;
    UINT tuples_left2 = table2->ntuples % tuples_per_page2 ? table2->ntuples % tuples_per_page2 : tuples_per_page2;
    UINT curr_page_count1 = 0;
    UINT curr_page_count2 = 0;
    UINT64 page_id1 = 0;
    UINT64 page_id2 = 0;
    int slot1 = 0;
    int slot2 = 0;

    // Intialize the result table
    _Table *result = malloc(sizeof(_Table));
    result->nattrs = table1->nattrs + table2->nattrs;
    UINT ntuples = 0;

    // Iterate through the pages of table1
    bool end_of_table1 = false;
    int outer_loop_slot[pages_per_block];
    int latest_slot = 0;
    int loop_count = 0;
    while (!end_of_table1)
    {
        loop_count++;
        for (UINT i = 0; i < pages_per_block; i++)
        {
            if (fread(&page_id1, sizeof(UINT64), 1, table_fp1) == 1)
            {
                // Request the page from pool
                slot1 = pageInPool(pool, table1->oid, page_id1);
                if (slot1 == -1)
                { // page is not already in pool
                    slot1 = grabNextSlot(pool);
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
                outer_loop_slot[i] = slot1;
                latest_slot = i;
            }
            else
            {
                latest_slot = i - 1;
                end_of_table1 = true;
                break;
            }
        }

        if (latest_slot == -1)
            break;

        // Iterate through the pages of table2
        while (fread(&page_id2, sizeof(UINT64), 1, table_fp2) == 1)
        {
            // Request the page from pool
            slot2 = pageInPool(pool, table2->oid, page_id2);
            if (slot2 == -1)
            { // page is not already in pool
                slot2 = grabNextSlot(pool);
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
                curr_page_count2++;
            }
            else
            { // page is already in pool
                fseek(table_fp2, cf_curr->page_size - sizeof(UINT64), SEEK_CUR);
            }
            // Iterate through the pages of table1 block
            for (UINT i = 0; i <= latest_slot; i++)
            {
                slot1 = outer_loop_slot[i];
                curr_page_count1 = (loop_count - 1) * pages_per_block + i + 1;

                // Iterate through the tuples of table1
                for (UINT j = 0; j < tuples_per_page1; j++)
                {
                    // Breake if the end of the table is reached
                    if (curr_page_count1 == npages1 && j == tuples_left1)
                    {
                        break;
                    }

                    // Iterate through the tuples of table2
                    for (UINT k = 0; k < tuples_per_page2; k++)
                    {
                        // Breake if the end of the table is reached
                        if (curr_page_count2 == npages2 && k == tuples_left2)
                        {
                            break;
                        }

                        // Check if the two tuples are joinable
                        if (memcmp(pool->bufs[slot1].data + (j * table1->nattrs * sizeof(INT) + attr_offset1),
                                   pool->bufs[slot2].data + (k * table2->nattrs * sizeof(INT) + attr_offset2),
                                   sizeof(INT)) == 0)
                        {
                            // Allocate space for the new tuple
                            ++ntuples;
                            result = realloc(result, sizeof(_Table) + ntuples * sizeof(Tuple));

                            // Copy the attributes from the two tuples as result
                            Tuple t = malloc(sizeof(INT) * result->nattrs);
                            result->tuples[ntuples - 1] = t;

                            for (int i = 0; i < table1->nattrs; i++)
                            {
                                ((INT *)t)[i] = ((INT *)(pool->bufs[slot1].data + (j * table1->nattrs * sizeof(INT))))[i];
                            }

                            for (int i = 0; i < table2->nattrs; i++)
                            {
                                ((INT *)t)[table1->nattrs + i] = ((INT *)(pool->bufs[slot2].data + (k * table2->nattrs * sizeof(INT))))[i];
                            }
                        }
                    }
                }
            }
            release_page(pool, table2->oid, page_id2);
        }
        rewind(table_fp2);
        curr_page_count2 = 0;
        for (UINT i = 0; i <= latest_slot; i++)
        {
            pool->bufs[outer_loop_slot[i]].pin = 0; // release the pages of outer loop
        }
    }
    rewind(table_fp1);
    result->ntuples = ntuples;
    return result;
}

_Table *grace_hash_join(const UINT attr1, Table *table1, const UINT attr2, Table *table2)
{
    // Calculate the offset of the attribute
    // UINT attr_offset1 = attr1 * sizeof(INT);
    // UINT attr_offset2 = attr2 * sizeof(INT);

    // Partition the tables
    _Table **table1_partitions = partition_table(table1, attr1, hash_func, 0);
    _Table **table2_partitions = partition_table(table2, attr2, hash_func, 1);

    // Initialize result table
    _Table *result = malloc(sizeof(_Table));
    result->nattrs = table1->nattrs + table2->nattrs;
    result->ntuples = 0;

    // Perform join for each pair of partitions
    for (UINT i = 0; i < pool->num_partitions; i++)
    {
        _Table *partition1 = table1_partitions[i];
        _Table *partition2 = table2_partitions[i];

        // Create and initialize hash table for the current partition
        ht = initHashTable(50);

        // Load partitions and hashing attribute into hash table
        for (UINT64 j = 0; j < partition1->ntuples; j++)
        {
            insertIntoHashTable(ht, partition1->tuples[j], partition1->tuples[j][attr1]);
        }

        // Perform join
        for (UINT64 j = 0; j < partition2->ntuples; j++)
        {
            // Get the tuple from the partition in hashtable
            UINT64 index = hashFunction(partition2->tuples[j][attr2], ht->size);
            HashEntry *entry = ht->table[index];
            while (entry != NULL)
            {
                if ((entry->tuple[attr1]) == (partition2->tuples[j][attr2]))
                {
                    // Allocate space for the new tuple
                    result->ntuples++;
                    result = realloc(result, sizeof(_Table) + result->ntuples * sizeof(Tuple));

                    // Copy the attributes from the two tuples as result
                    Tuple t = malloc(sizeof(INT) * result->nattrs);
                    result->tuples[result->ntuples - 1] = t;

                    for (int i = 0; i < table1->nattrs; i++)
                    {
                        ((INT *)t)[i] = ((INT *)entry->tuple)[i];
                    }

                    for (int i = 0; i < table2->nattrs; i++)
                    {
                        ((INT *)t)[table1->nattrs + i] = ((INT *)partition2->tuples[j])[i];
                    }
                }
                entry = entry->next;
            }
        }
        freeHashTable(ht);
    }

    // Cleanup
    free_partitions(table1_partitions, pool->num_partitions);
    free_partitions(table2_partitions, pool->num_partitions);

    return result;
}

_Table **partition_table(Table *table, int attr, hash_function hash_func, int table_idx)
{
    // Allocate memory for the partition tables
    _Table **partitions = malloc(pool->num_partitions * sizeof(_Table *));

    // Initialize the partition tables
    for (UINT i = 0; i < pool->num_partitions; i++)
    {
        partitions[i] = malloc(sizeof(_Table));
        partitions[i]->nattrs = table->nattrs;
        partitions[i]->ntuples = 0;
    }
    // Open the table file
    FILE *table_fp = get_file_pointer(file_cache, table->oid, db_curr->path);
    if (table_fp == NULL)
    {
        perror("Fail to open the table file.\n");
        exit(-1);
    }

    // Partition the table by hashing the attribute
    UINT64 tuples_per_page = (cf_curr->page_size - sizeof(UINT64)) / (table->nattrs * sizeof(INT));
    UINT npages = (table->ntuples + tuples_per_page - 1) / tuples_per_page;
    UINT tuples_left = table->ntuples % tuples_per_page ? table->ntuples % tuples_per_page : tuples_per_page;
    UINT curr_page_count = 0;
    UINT64 page_id = 0;

    while (fread(&page_id, sizeof(UINT64), 1, table_fp) == 1)
    {
        // Read the page into buffer pool
        int buf_slot = pageInPool(pool, table->oid, page_id);

        if (buf_slot == -1)
        { // page is not already in pool
            buf_slot = grabNextSlot(pool);
            if (buf_slot < 0)
            {
                fprintf(stderr, "Failed to find slot for Table:%s page:%ld\n", table->name, page_id);
                exit(1);
            }
            pool->bufs[buf_slot].table_oid = table->oid;
            pool->bufs[buf_slot].page_id = page_id;
            pool->bufs[buf_slot].pin = 1;
            pool->bufs[buf_slot].use = 1;
            // Read the page from the disk
            fread(pool->bufs[buf_slot].data, cf_curr->page_size - sizeof(UINT64), 1, table_fp);
            log_read_page(page_id); // log read
        }
        else
        { // page is already in pool
            fseek(table_fp, cf_curr->page_size - sizeof(UINT64), SEEK_CUR);
        }
        curr_page_count++;

        // Iterate through the tuples of the page
        for (UINT64 i = 0; i < tuples_per_page; i++)
        {
            // Break if all tuples have been processed
            if (curr_page_count == npages && i == tuples_left)
            {
                break;
            }

            // Hash the attribute value
            UINT part_id = hash_func(pool->bufs[buf_slot].data[i * table->nattrs * sizeof(INT) + attr * sizeof(INT)], pool->num_partitions);

            // Add the tuple to the corresponding partition
            _Table *partition = partitions[part_id];

            partition->ntuples++;
            partition = realloc(partition, sizeof(_Table) + partition->ntuples * sizeof(Tuple));

            Tuple tuple = malloc(sizeof(INT) * table->nattrs);
            partition->tuples[partition->ntuples - 1] = tuple;
            memcpy(tuple, pool->bufs[buf_slot].data + (i * table->nattrs * sizeof(INT)), sizeof(INT) * table->nattrs);

            partitions[part_id] = partition;
        }
    }
    rewind(table_fp);
    return partitions;
}

// Simple hash function for attribute values
UINT64 hash_func(UINT64 key, UINT num_partitions)
{
    const double A = 0.6180339887; // Constant A should be an irrational number between 0 and 1
    double fractional_part = key * A - (int64_t)(key * A);
    return (UINT64)(num_partitions * fractional_part);
}

void free_partitions(_Table **partitions, UINT num_partitions)
{
    if (partitions == NULL)
    {
        return;
    }

    // Iterate through each partition table
    for (UINT i = 0; i < num_partitions; i++)
    {
        _Table *partition = partitions[i];

        if (partition != NULL)
        {
            // Free the memory used by each tuple in the partition
            for (UINT64 j = 0; j < partition->ntuples; j++)
            {
                free(partition->tuples[j]);
            }

            // Free the memory used by the partition table structure
            free(partition);
        }
    }

    // Free the memory used by the partitions array
    free(partitions);
}

void free_result_table(_Table *result_table)
{
    if (result_table == NULL)
    {
        return;
    }

    // Free the memory allocated to each tuple in the table
    for (UINT i = 0; i < result_table->ntuples; i++)
    {
        free(result_table->tuples[i]);
    }

    // Free the memory allocated to the result table
    free(result_table);
}
