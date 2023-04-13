#include <stdlib.h>
#include "db.h"

// A single entry in the hash table
typedef struct HashEntry
{
    Tuple tuple;
    struct HashEntry *next;
} HashEntry;

// The hash table structure
typedef struct HashTable
{
    UINT64 size;       // Size of the hash table (number of buckets)
    HashEntry **table; // Pointer to an array of pointers to HashEntry
} HashTable;

HashTable *initHashTable(UINT64);
UINT64 hashFunction(UINT attr, UINT64 tableSize);
void insertIntoHashTable(HashTable *ht, Tuple tuple, UINT attr);
void freeHashTable(HashTable *ht);