// threadpool.c
// Tawfeeq Mannan

// library includes
#include <stdlib.h>     // malloc
#include <pthread.h>    // pthread_create, ...

// user includes
#include "threadpool.h"


/**
 * Create a new ThreadPool object
 * 
 * @param num # of threads to create
 * 
 * @return Pointer to the newly created ThreadPool object
 */
ThreadPool_t *ThreadPool_create(unsigned int num)
{
    ThreadPool_t *tp = malloc(sizeof(ThreadPool_t));

    // explicitly null-initialize the job queue
    tp->jobs.size = 0;
    tp->jobs.head = NULL;

    // create the array of threads, each running the thread start routine
    tp->threads = malloc(sizeof(pthread_t) * num);
    for (int i = 0; i < num; i++)
    {
        pthread_create(&(tp->threads[i]), NULL, (void *) Thread_run, tp);
    }

    return tp;
}
