// threadpool.c
// Tawfeeq Mannan

// library includes
#include <stdio.h>      // printf
#include <stdlib.h>     // malloc
#include <pthread.h>    // pthread_create, ...

// user includes
#include "threadpool.h"


/**
 * C style constructor for creating a new ThreadPool object
 * 
 * @param num # of threads to create
 * 
 * @return Pointer to the newly created ThreadPool object
 */
ThreadPool_t *ThreadPool_create(unsigned int num)
{
    ThreadPool_t *tp = malloc(sizeof(ThreadPool_t));
    tp->num_threads = num;

    // explicitly null-initialize the job queue
    tp->jobs.size = 0;
    tp->jobs.head = NULL;
    tp->jobs.tail = NULL;

    // create the array of threads, each running the thread start routine
    tp->threads = malloc(sizeof(pthread_t) * num);
    for (int i = 0; i < num; i++)
    {
        pthread_create(&(tp->threads[i]), NULL, (void *) Thread_run, tp);
    }

    return tp;
}


/**
 * C style destructor to destroy a ThreadPool object
 * 
 * @param tp pointer to the ThreadPool object to be destroyed
 */
void ThreadPool_destroy(ThreadPool_t *tp)
{
    if (tp == NULL) return;

    if (tp->jobs.size != 0)
    {
        printf("Jobs still pending!\n");
        return;
    }

    for (int i = 0; i < tp->num_threads; i++)
    {
        // cancel each thread now. should have no effect if they're alr done
        pthread_cancel(tp->threads[i]);
        // wait to join with the thread to ensure total destruction
        pthread_join(tp->threads[i], NULL);
    }

    free(tp->threads);
    free(tp);
    return;
}


/**
 * Push a job to the ThreadPool's job queue
 * 
 * @param tp pointer to the ThreadPool object
 * @param func function pointer that will be called by the serving thread
 * @param arg arguments for that function
 * 
 * @return True on success, otherwise false
 */
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg)
{
    // create the new job struct
    ThreadPool_job_t *newJob = malloc(sizeof(ThreadPool_job_t));
    if (newJob == NULL) return false;
    newJob->func = func;
    newJob->arg = arg;
    newJob->next = NULL;

    // attach the job to the tail of the queue
    // TODO protect the queue with a lock while pushing to it
    if (tp->jobs.size == 0)
        tp->jobs.head = newJob;
    else
        tp->jobs.tail->next = newJob;
    tp->jobs.tail = newJob;
    tp->jobs.size++;

    return true;
}


/**
 * Pop a job from the job queue of the ThreadPool object
 * 
 * @param tp pointer to the ThreadPool object
 * 
 * @return Next job to run
 */
ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp)
{
    // TODO write-protect the queue
    ThreadPool_job_t *nextJob = tp->jobs.head;
    if (nextJob != NULL)
    {
        tp->jobs.size--;
        tp->jobs.head = nextJob->next;
        if (tp->jobs.size == 0)  // this was the last job
            tp->jobs.tail = NULL;
    }
    return nextJob;
}
