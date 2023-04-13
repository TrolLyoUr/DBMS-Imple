#include <stdlib.h>
#include "hashtab.h"

HashTable *initHashTable(UINT64 size)
{
    // Initialize a hash table with a fixed size
    HashTable *ht = (HashTable *)malloc(sizeof(HashTable));
    ht->size = size;
    ht->table = (HashEntry **)calloc(size, sizeof(HashEntry *));

    return ht;
}

UINT64 hashFunction(UINT attr, UINT64 tableSize)
{
    return (UINT64)attr % tableSize;
}

void insertIntoHashTable(HashTable *ht, Tuple tuple, UINT attr)
{
    UINT64 index = hashFunction(attr, ht->size);

    // Check if the bucket is empty
    if (ht->table[index] == NULL)
    {
        ht->table[index] = (HashEntry *)malloc(sizeof(HashEntry));
        ht->table[index]->tuple = tuple;
        ht->table[index]->next = NULL;
    }
    else
    {
        // Bucket is not empty, insert the new entry at the beginning of the linked list
        HashEntry *newEntry = (HashEntry *)malloc(sizeof(HashEntry));
        newEntry->tuple = tuple;
        newEntry->next = ht->table[index];
        ht->table[index] = newEntry;
    }
}

void freeHashTable(HashTable *ht)
{
    if (ht == NULL)
    {
        return;
    }

    // Iterate through each bucket in the hash table
    for (UINT64 i = 0; i < ht->size; i++)
    {
        HashEntry *entry = ht->table[i];
        while (entry != NULL)
        {
            HashEntry *temp = entry;
            entry = entry->next;
            // Free the memory used by the current entry
            free(temp);
        }
    }

    // Free the memory used by the table array
    free(ht->table);

    // Free the memory used by the hash table structure
    free(ht);
}