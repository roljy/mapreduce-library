// threadpool.c
// Tawfeeq Mannan

// library includes
#include <stdlib.h>     // malloc, free
#include <stdbool.h>    // true/false
#include <pthread.h>    // pthread_create, ...

// user includes
#include "threadpool.h"


/**
 * @brief C style constructor for creating a new ThreadPool object
 * 
 * @param num # of threads to create
 * 
 * @return Pointer to the newly created ThreadPool object
 */
ThreadPool_t *ThreadPool_create(unsigned int num)
{
    ThreadPool_t *tp = malloc(sizeof(ThreadPool_t));
    tp->num_threads = num;
    tp->running = malloc(sizeof(pthread_mutex_t) * num);
    pthread_mutex_init(&tp->master_busy, NULL);

    // explicitly null-initialize the job queue
    tp->jobs.size = 0;
    pthread_mutex_init(&tp->jobs.lock, NULL);
    pthread_cond_init(&tp->jobs.empty, NULL);
    pthread_cond_init(&tp->jobs.notEmpty, NULL);
    tp->jobs.head = NULL;
    tp->jobs.tail = NULL;

    // create the array of threads, each running the thread start routine
    tp->threads = malloc(sizeof(pthread_t) * num);
    pthread_mutex_lock(&tp->master_busy);  // begin batch thread creation
    for (int i = 0; i < num; i++)
    {
        pthread_mutex_init(&tp->running[i], NULL);
        pthread_create(&tp->threads[i], NULL, (void *) Thread_run, tp);
    }
    pthread_mutex_unlock(&tp->master_busy);

    return tp;
}


/**
 * @brief C style destructor to destroy a ThreadPool object
 * 
 * @param tp pointer to the ThreadPool object to be destroyed
 */
void ThreadPool_destroy(ThreadPool_t *tp)
{
    if (tp == NULL) return;

    ThreadPool_check(tp);

    for (int i = 0; i < tp->num_threads; i++)
    {
        // cancel each thread now. should have no effect if they're alr done
        pthread_cancel(tp->threads[i]);
        // wait to join with the thread to ensure resource reclamation
        pthread_join(tp->threads[i], NULL);
    }

    free(tp->threads);
    free(tp->running);
    free(tp);
    return;
}


/**
 * @brief Push a job to the ThreadPool's job queue
 * 
 * For now, jobs are ordered FCFS. It is the parent's responsibility to order
 * them for SJF since tasks may be popped while still being pushed.
 * 
 * @param tp pointer to the ThreadPool object
 * @param func function pointer that will be called by the serving thread
 * @param arg arguments for that function
 * 
 * @return True on success, otherwise false
 */
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg)
{
    if (tp == NULL) return false;

    // create the new job struct
    ThreadPool_job_t *newJob = malloc(sizeof(ThreadPool_job_t));
    if (newJob == NULL) return false;
    newJob->func = func;
    newJob->arg = arg;
    newJob->next = NULL;

    // attach the job to the tail of the queue (critical section)
    // TODO implement SJF?
    pthread_mutex_lock(&tp->jobs.lock);
    if (tp->jobs.size == 0)
        tp->jobs.head = newJob;
    else
        tp->jobs.tail->next = newJob;
    tp->jobs.tail = newJob;
    // wake up all worker threads who may have been blocked on empty queue
    if (++tp->jobs.size == 1)
        pthread_cond_broadcast(&tp->jobs.notEmpty);
    pthread_mutex_unlock(&tp->jobs.lock);

    return true;
}


/**
 * @brief Pop a job from the job queue of the ThreadPool object
 * 
 * @param tp pointer to the ThreadPool object
 * 
 * @return Next job to run
 */
ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp)
{
    if (tp == NULL) return NULL;

    // pop the first job from head of queue (critical section)
    pthread_mutex_lock(&tp->jobs.lock);
    while (tp->jobs.size == 0)  // relinquish lock if no jobs right now
        pthread_cond_wait(&tp->jobs.notEmpty, &tp->jobs.lock);

    // if we are here, there is a job available, and we have the lock
    ThreadPool_job_t *nextJob = tp->jobs.head;
    tp->jobs.size--;
    tp->jobs.head = nextJob->next;
    if (tp->jobs.size == 0)  // this was the last job
    {
        tp->jobs.tail = NULL;
        pthread_cond_signal(&tp->jobs.empty);
    }
    // ! write lock is not released. caller must release it !

    return nextJob;
}


/**
 * @brief Start routine of each thread in the ThreadPool Object.
 * 
 * In a loop, check the job queue, get a job (if any) and run it.
 * 
 * @param tp pointer to the ThreadPool object containing this thread
 */
void *Thread_run(ThreadPool_t *tp)
{
    if (tp == NULL) return NULL;

    pthread_t tid = pthread_self();
    int thread_index = 0;
    // get the index of this thread in the thread pool.
    // must wait for parent to finish creating all threads to avoid racing,
    // but we can release it immediately since the actual search isnt critical
    pthread_mutex_lock(&tp->master_busy);
    pthread_mutex_unlock(&tp->master_busy);
    for (int i = 0; i < tp->num_threads; i++)
    {
        if (tp->threads[i] == tid)
        {
            thread_index = i;
            break;
        }
    }

    while (true)
    {
        ThreadPool_job_t *job = ThreadPool_get_job(tp);  // block until pop

        pthread_mutex_lock(&tp->running[thread_index]);  // signal thread busy
        pthread_mutex_unlock(&tp->jobs.lock);  // end of queue critical section

        if (job->func == NULL)
            return NULL;  // kill the thread if receive an empty job
        job->func(job->arg);
        free(job);  // once we've run the task nobody will need it again

        pthread_mutex_unlock(&tp->running[thread_index]);
    }
}


/**
 * @brief Ensure all threads idle and job queue is empty before returning
 * 
 * @param tp pointer to the ThreadPool object containing this thread
 */
void ThreadPool_check(ThreadPool_t *tp)
{
    if (tp == NULL) return;

    pthread_mutex_lock(&tp->jobs.lock);
    while (tp->jobs.size > 0)  // relinquish lock if still unclaimed jobs
        pthread_cond_wait(&tp->jobs.empty, &tp->jobs.lock);
    
    // if we are here, there are no more unclaimed jobs, and we have the lock
    for (int i = 0; i < tp->num_threads; i++)
        pthread_mutex_lock(&tp->running[i]);  // wait for each thread to finish

    // now that every thread is done, we can release all resources
    for (int i = 0; i < tp->num_threads; i++)
        pthread_mutex_unlock(&tp->running[i]);
    pthread_mutex_unlock(&tp->jobs.lock);
    return;  // signal to caller that threadpool is idle
}
