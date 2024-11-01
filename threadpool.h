// threadpool.h
// Tawfeeq Mannan

#ifndef _THREADPOOL_H
#define _THREADPOOL_H

#include <pthread.h>
#include <stdbool.h>

typedef void (*thread_func_t)(void *arg);


typedef struct ThreadPool_job_t
{
    thread_func_t func;             // function pointer
    void *arg;                      // arguments for that function
    struct ThreadPool_job_t *next;  // pointer to the next job in the queue
    // TODO add other members as needed
} ThreadPool_job_t;


typedef struct
{
    unsigned int size;              // no. jobs in the queue
    ThreadPool_job_t *head;         // pointer to the first (shortest) job
    // TODO add other members as needed
} ThreadPool_job_queue_t;


typedef struct
{
    pthread_t *threads;             // pointer to the array of thread handles
    ThreadPool_job_queue_t jobs;    // queue of jobs waiting for a thread to run
    // TODO add other members as needed
} ThreadPool_t;


/**
 * C style constructor for creating a new ThreadPool object
 * 
 * @param num # of threads to create
 * 
 * @return Pointer to the newly created ThreadPool object
 */
ThreadPool_t *ThreadPool_create(unsigned int num);


/**
 * C style destructor to destroy a ThreadPool object
 * 
 * @param tp pointer to the ThreadPool object to be destroyed
 */
void ThreadPool_destroy(ThreadPool_t *tp);


/**
 * Add a job to the ThreadPool's job queue
 * 
 * @param tp pointer to the ThreadPool object
 * @param func function pointer that will be called by the serving thread
 * @param arg arguments for that function
 * 
 * @return True on success, otherwise false
 */
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg);


/**
 * Get a job from the job queue of the ThreadPool object
 * 
 * @param tp pointer to the ThreadPool object
 * 
 * @return Next job to run
 */
ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp);


/**
 * Start routine of each thread in the ThreadPool Object.
 * In a loop, check the job queue, get a job (if any) and run it.
 * 
 * @param tp pointer to the ThreadPool object containing this thread
 */
void *Thread_run(ThreadPool_t *tp);


/**
 * Ensure that all threads are idle and the job queue is empty before returning
 * 
 * @param tp pointer to the ThreadPool object that will be destroyed
 */
void ThreadPool_check(ThreadPool_t *tp);


#endif  // _THREADPOOL_H
