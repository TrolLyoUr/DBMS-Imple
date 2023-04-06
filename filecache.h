#include <stdint.h>
#include <stdio.h>
#include "db.h"

typedef struct FileCacheEntry
{
    UINT oid;
    FILE *file_ptr;
    struct FileCacheEntry *prev;
    struct FileCacheEntry *next;
} FileCacheEntry;

typedef struct FileCache
{
    UINT size;
    UINT limit;
    FileCacheEntry *head;
    FileCacheEntry *tail;
} FileCache;

FileCache *init_file_cache(UINT limit);
void free_file_cache(FileCache *cache);
FILE *get_file_pointer(FileCache *cache, UINT, const char *);