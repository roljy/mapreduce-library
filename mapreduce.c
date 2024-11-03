// mapreduce.c
// Tawfeeq Mannan

// library includes
#include <stdio.h>      // printf
#include <stdlib.h>     // malloc, free
#include <stdbool.h>    // true/false

// user includes
#include "mapreduce.h"
#include "threadpool.h"


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
    // create the thread pool
    if (num_workers == 0) { printf("No worker threads!\n"); return; }
    ThreadPool_t *threadpool = ThreadPool_create(num_workers);

    // TODO run the mapper

    // TODO run the reducer
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
