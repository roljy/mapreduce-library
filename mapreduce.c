// mapreduce.c
// Tawfeeq Mannan

// library includes
#include <stdio.h>      // printf
#include <stdlib.h>     // malloc, free
#include <string.h>     // strcmp
#include <stdbool.h>    // true/false
#include <pthread.h>    // pthread_mutex_t, etc...

// user includes
#include "mapreduce.h"
#include "threadpool.h"


typedef struct pair_t
{
    char *key;
    char *value;
    pair_t *next;
} pair_t;


typedef struct partition_t
{
    // TODO do we need the size?
    unsigned int size;          // # of pairs in partition
    pair_t *head;               // linked list of key-value pairs
    pthread_mutex_t lock;       // lock to protect concurrent writes
} partition_t;


// global vars (shared data)
unsigned int num_partitions;
ThreadPool_t *threadpool;  // worker thread pool
partition_t *partitions;  // array of partitions


/**
 * Run the MapReduce framework
 * 
 * @param file_count # of files (i.e. input splits)
 * @param file_names array of filenames
 * @param mapper function pointer to the map function
 * @param reducer function pointer to the reduce function
 * @param num_workers # of threads in the thread pool
 * @param num_parts # of partitions to be created
 */
void MR_Run(unsigned int file_count,
            char *file_names[],
            Mapper mapper,
            Reducer reducer, 
            unsigned int num_workers,
            unsigned int num_parts)
{
    if (num_workers == 0) { printf("No worker threads!\n"); return; }
    if (num_parts == 0) { printf("No partitions\n"); return; }

    // create the thread pool and partition array
    threadpool = ThreadPool_create(num_workers);
    partitions = malloc(sizeof(partition_t) * num_parts);
    for (int i = 0; i < num_parts; i++)
    {
        partitions[i].size = 0;
        partitions[i].head = NULL;
        pthread_mutex_init(&partitions[i].lock, NULL);
    }
    num_partitions = num_parts;

    // TODO run the mapper

    // TODO run the reducer
}


/**
 * Write a specifc map output, a <key, value> pair, to a partition
 * 
 * @param key output key
 * @param value output value
 */
void MR_Emit(char *key, char *value)
{
    pair_t *newPair = malloc(sizeof(pair_t));
    newPair->key = key;
    newPair->value = value;

    unsigned int part_idx = MR_Partitioner(key, num_partitions);
    // to write pair into partition, enter critical section
    pthread_mutex_lock(&partitions[part_idx].lock);

    // find where to insert the key for ascending order
    pair_t *prev = partitions[part_idx].head;
    if (prev == NULL || strcmp(key, prev->key) < 0)
    {
        // this key must go at the very beginning
        newPair->next = prev;
        partitions[part_idx].head = newPair;
    }
    else
    {
        while (prev->next != NULL && strcmp(key, prev->next->key) < 0)    
        {
            // the next key is still >= this one, try the one after that
            prev = prev->next;
        }
        // now prev is the pair that belongs right before our new one
        newPair->next = prev->next;
        prev->next = newPair;
    }
    partitions[part_idx].size++;

    // end critical section, we're done writing now
    pthread_mutex_unlock(&partitions[part_idx].lock);
}


/**
 * Hash a mapper's output to determine the partition that will hold it
 * 
 * Using the DJB2 Hash algorithm for this implementation
 * 
 * @param key key of a specifc map output
 * @param num_partitions total # of partitions
 * 
 * @return Index of the partition
 */
unsigned int MR_Partitioner(char *key, unsigned int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
