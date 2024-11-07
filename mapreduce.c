// mapreduce.c
// Tawfeeq Mannan

// library includes
#define _GNU_SOURCE
#include <stdio.h>      // printf
#include <stdlib.h>     // malloc, free, qsort
#include <string.h>     // strcmp, strdup
#include <stdbool.h>    // true/false
#include <sys/stat.h>   // stat
#include <pthread.h>    // pthread_mutex_t, etc...

// user includes
#include "mapreduce.h"
#include "threadpool.h"


typedef struct pair_t
{
    char *key;
    char *value;
    struct pair_t *next;
} pair_t;


typedef struct partition_t
{
    unsigned int size;          // # of pairs in partition
    pair_t *head;               // linked list of key-value pairs
    pthread_mutex_t lock;       // lock to protect concurrent writes
} partition_t;


// global vars (shared data)
unsigned int num_partitions;
ThreadPool_t *threadpool;  // worker thread pool
partition_t *partitions;  // array of partitions


/**
 * @brief Comparison function for mapper input files, based on file size
 * 
 * @param file1 Filename for the 1st comparison arg
 * @param file2 Filename for the 2nd comparison arg
 * @return -1 if file1<file2, 1 if file1>file2, 0 if equal
 */
int compare_mapper_files(const char *file1, const char *file2)
{
    struct stat sb1, sb2;
    if (stat(file1, &sb1) == -1) return 1;  // fallback if stat fails
    if (stat(file2, &sb2) == -1) return -1;
    return (sb1.st_size > sb2.st_size) - (sb1.st_size < sb2.st_size);
}


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
    for (unsigned int i = 0; i < num_parts; i++)
    {
        partitions[i].size = 0;
        partitions[i].head = NULL;
        pthread_mutex_init(&partitions[i].lock, NULL);
    }
    num_partitions = num_parts;

    // sort the input filenames by ascending file size
    char **sorted_file_names = malloc(sizeof(char *) * file_count);
    for (unsigned int i = 0; i < file_count; i++)
        sorted_file_names[i] = file_names[i];
    qsort(sorted_file_names,
          file_count,
          sizeof(char *),
          (int (*)(const void *, const void *)) compare_mapper_files);

    // run the mapper
    for (unsigned int i = 0; i < file_count; i++)
    {
        ThreadPool_add_job(threadpool,
                           (void (*)(void *)) mapper,
                           sorted_file_names[i]);
    }
    ThreadPool_check(threadpool);
    // mapper is done now
    free(sorted_file_names);

    // TODO run the reducer

    // destroy the threadpool and free memory when done
    ThreadPool_destroy(threadpool);
    free(partitions);
}


/**
 * Write a specifc map output, a <key, value> pair, to a partition
 * 
 * Note that the key-value pair consists of newly allocated strings,
 * must be freed alongside the pair.
 * 
 * @param key output key
 * @param value output value
 */
void MR_Emit(char *key, char *value)
{
    pair_t *newPair = malloc(sizeof(pair_t));
    newPair->key = strdup(key);
    newPair->value = strdup(value);

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


/**
 * Run the reducer callback function for each <key, (list of values)> 
 * retrieved from a partition
 * 
 * @param threadarg pointer to a hidden args object
 */
void MR_Reduce(void *threadarg)
{
    // TODO implement MR_Reduce
}


/**
 * Get the next value of the given key in the partition
 * 
 * @param key key of the values being reduced
 * @param partition_idx index of the partition containing this key
 * 
 * @return Value of the next <key, value> pair if its key is the current key,
 *         otherwise NULL
 */
char *MR_GetNext(char *key, unsigned int partition_idx)
{
    // TODO implement MR_GetNext
    return NULL;
}
