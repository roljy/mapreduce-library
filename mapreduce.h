// mapreduce.h
// Tawfeeq Mannan

#ifndef _MAPREDUCE_H
#define _MAPREDUCE_H

// function pointer typedefs
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, unsigned int partition_idx);


/**
 * Run the MapReduce framework
 * 
 * @param file_count number of files (i.e. input splits)
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
            unsigned int num_parts);


/**
 * Write a specifc map output, a <key, value> pair, to a partition
 * 
 * @param key output key
 * @param value output value
 */
void MR_Emit(char *key, char *value);


/**
 * Hash a mapper's output to determine the partition that will hold it
 * 
 * @param key key of a specifc map output
 * @param num_partitions total # of partitions
 * 
 * @return Index of the partition
 */
unsigned int MR_Partitioner(char *key, unsigned int num_partitions);


/**
 * Run the reducer callback function for each <key, (list of values)> 
 * retrieved from a partition
 * 
 * @param threadarg pointer to a hidden args object
 */
void MR_Reduce(void *threadarg);


/**
 * Get the next value of the given key in the partition
 * 
 * @param key key of the values being reduced
 * @param partition_idx index of the partition containing this key
 * 
 * @return Value of the next <key, value> pair if its key is the current key,
 *         otherwise NULL
 */
char *MR_GetNext(char *key, unsigned int partition_idx);


#endif  // _MAPREDUCE_H
