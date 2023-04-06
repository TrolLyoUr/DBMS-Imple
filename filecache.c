#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "db.h"
#include "filecache.h"

FileCache *init_file_cache(UINT limit)
{
    FileCache *cache = malloc(sizeof(FileCache));
    cache->size = 0;
    cache->limit = limit;
    cache->head = NULL;
    cache->tail = NULL;
    return cache;
}

void free_file_cache(FileCache *cache)
{
    FileCacheEntry *entry = cache->head;
    while (entry != NULL)
    {
        FileCacheEntry *next = entry->next;
        fclose(entry->file_ptr);
        free(entry);
        entry = next;
    }
    free(cache);
}

FILE *get_file_pointer(FileCache *cache, UINT oid, const char *path)
{
    FileCacheEntry *entry = cache->head;
    while (entry != NULL)
    {
        if (entry->oid == oid)
        {
            // Move entry to the head of the list
            if (entry->prev != NULL)
            {
                entry->prev->next = entry->next;
                if (entry->next != NULL)
                {
                    entry->next->prev = entry->prev;
                }
                else
                {
                    cache->tail = entry->prev;
                }
                entry->prev = NULL;
                entry->next = cache->head;
                cache->head->prev = entry;
                cache->head = entry;
            }
            return entry->file_ptr;
        }
        entry = entry->next;
    }

    // File not found in cache, open it and add it to the cache
    char table_path[200];
    sprintf(table_path, "%s/%u", path, oid);
    FILE *file_ptr = fopen(table_path, "rb");
    if (file_ptr == NULL)
    {
        perror("Fail to open the table file.\n");
        exit(-1);
    }
    log_open_file(oid);

    entry = malloc(sizeof(FileCacheEntry));
    entry->oid = oid;
    entry->file_ptr = file_ptr;
    entry->prev = NULL;
    entry->next = cache->head;
    if (cache->head != NULL)
    {
        cache->head->prev = entry;
    }
    else
    {
        cache->tail = entry;
    }
    cache->head = entry;
    cache->size++;

    // Evict the least recently used entry if cache size exceeds the limit
    if (cache->size > cache->limit)
    {
        FileCacheEntry *evicted = cache->tail;
        cache->tail = evicted->prev;
        cache->tail->next = NULL;
        fclose(evicted->file_ptr);
        log_close_file(evicted->oid);
        free(evicted);
        cache->size--;
    }

    return file_ptr;
}
