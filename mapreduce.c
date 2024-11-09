// mapreduce.c
// Tawfeeq Mannan

// library includes
#define _GNU_SOURCE
#include <stdio.h>      // printf
#include <stdlib.h>     // malloc, free, qsort
#include <string.h>     // strcmp, strdup, strlen
#include <stdbool.h>    // true/false
#include <sys/stat.h>   // stat
#include <pthread.h>    // pthread_mutex_t, etc...

// user includes
#include "mapreduce.h"
#include "threadpool.h"


typedef struct pair_t
{
    char *key;                  // key to index by
    char *value;                // value associated with a key
    struct pair_t *next;        // pointer to next item in linked list
} pair_t;


typedef struct partition_t
{
    unsigned int size;          // total size of kv pairs in partition
    pair_t *head;               // linked list of key-value pairs
                                // TODO create hash table of starting indices
    pthread_mutex_t lock;       // lock to protect concurrent writes
} partition_t;


// global vars (shared data)
unsigned int num_partitions;    // no. of partitions (needed by MR_Emit)
ThreadPool_t *threadpool;       // worker thread pool
partition_t *partitions;        // array of partitions
Reducer global_reducer;         // reducer function (needed by MR_Reduce)


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
 * @brief Comparison function for the reducer partition indices, based on size
 * 
 * @param idx1 Pointer to 1st partition index
 * @param idx2 Pointer to 2nd partition index
 * @return int -1 if LHS<RHS, 1 if LHS>RHS, 0 if equal
 */
int compare_partitions(const unsigned int *idx1, const unsigned int *idx2)
{
    return (partitions[*idx1].size > partitions[*idx2].size)
            - (partitions[*idx1].size < partitions[*idx2].size);
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

    // sort the partition indices by ascending partition size
    unsigned int *sorted_part_idxs = malloc(sizeof(unsigned int) * num_parts);
    for (unsigned int i = 0; i < num_parts; i++)
        sorted_part_idxs[i] = i;
    qsort(sorted_part_idxs,
          num_parts,
          sizeof(unsigned int),
          (int (*)(const void *, const void *)) compare_partitions);

    // run 1 reduction job per partition (job func is MR_Reduce)
    global_reducer = reducer;
    for (unsigned int i = 0; i < num_parts; i++)
    {
        ThreadPool_add_job(threadpool,
                           (void (*)(void *)) MR_Reduce,
                           &sorted_part_idxs[i]);
    }
    ThreadPool_check(threadpool);
    // reducer is done now
    free(sorted_part_idxs);

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
    pair_t *prev = NULL, *curr = partitions[part_idx].head;
    while (curr != NULL && strcmp(key, curr->key) >= 0)    
    {
        // our new key is not yet less than this one, keep traversing
        prev = curr;
        curr = curr->next;
    }
    // curr belongs right after this new one, prev right before (except NULL)
    newPair->next = curr;
    if (prev == NULL)
        partitions[part_idx].head = newPair;  // new beginning
    else
        prev->next = newPair;

    // increase partition size counter by combined kv size.
    // add 2 extra bytes for the null terminators not included by strlen
    partitions[part_idx].size += strlen(key) + strlen(value) + 2;

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
 * Within a thread, run the reducer callback function for each
 * <key, (list of values)> retrieved from a partition
 * 
 * @param threadarg pointer to the partition index (unsigned int) to reduce
 */
void MR_Reduce(void *threadarg)
{
    unsigned int partition_idx = *((unsigned int *) threadarg);
    char *current_key = NULL;
    while (partitions[partition_idx].head != NULL)
    {
        // reduce all the keys matching the current head
        current_key = strdup(partitions[partition_idx].head->key);
        global_reducer(current_key, partition_idx);
        free(current_key);
    }
}


/**
 * Get the next value of the given key in the partition, and pop it out
 * 
 * Note: while the popped pair and key are freed, the caller is responsible
 * for freeing the returned value.
 * 
 * @param key key of the values being reduced
 * @param partition_idx index of the partition containing this key
 * 
 * @return Value of the next <key, value> pair if its key is the current key,
 *         otherwise NULL
 */
char *MR_GetNext(char *key, unsigned int partition_idx)
{
    // traversing this partition is critical, lock it from writes
    pthread_mutex_lock(&partitions[partition_idx].lock);

    // search the linked list until first match, or quit early if overshot
    pair_t *prev = NULL, *curr = partitions[partition_idx].head;
    while (curr != NULL && strcmp(key, curr->key) > 0)    
    {
        // our desired key is still too large, keep traversing
        prev = curr;
        curr = curr->next;
    }
    // now either curr is a good key or we've overshot
    if (curr == NULL || strcmp(key, curr->key) != 0)
    {
        pthread_mutex_unlock(&partitions[partition_idx].lock);
        return NULL;  // went too far, key doesn't exist
    }

    // pop the pair out of the partition
    if (prev == NULL)
        partitions[partition_idx].head = curr->next;  // new beginning
    else
        prev->next = curr->next;
    
    // decrease partition size by the total kv size for this pair
    char *value = curr->value;
    partitions[partition_idx].size -= strlen(key) + strlen(value) + 2;

    // free the pair and return the value
    free(curr->key);
    free(curr);
    pthread_mutex_unlock(&partitions[partition_idx].lock);
    return value;
}
