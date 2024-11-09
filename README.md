# Multithreaded MapReduce Library

MapReduce is traditionally a framework for data processing on distributed
clusters, but that doesn't stop us from taking advantage of the multithreading
capabilities of modern multi-core processors. This lightweight library allows
users to run their own MapReduce applications by simply invoking two functions
(MR_Emit and MR_GetNext) in their own Map and Reduce functions, then calling
MR_Run to begin the processing. The library takes care of threadpool management
and task scheduling, ensuring that data is not corrupted by conflicting writes.


## How to Run

`make wordcount` to build the wordcount executable, an example application
showing the MapReduce library in action.

For just the library object files, `make threadpool.o` and `make mapreduce.o`
are sufficient. These are prerequistite to wordcount or other applications.

For memory leak checking, `make valgrind` will run a debug build in valgrind.


## Design

The mutual exclusion and thread safety of this library was guaranteed using two
main synchronization primitives in pthreads: mutexes and condition variables
(ie. pthread_mutex_t [sometimes more than 1] and pthread_cond_t).

These were added as attributes of the ThreadPool_t, ThreadPool_job_queue_t, and
partition_t structs; this way each relevant structure had associated locks
and/or condition variables responsible for guaranteeing mutual exclusion in
critical sections. For example, the job queue has its own "lock" mutex, meaning
that only one thread can add or remove a job from the queue at a given time.
Likewise, the threadpool has an array of "busy" mutexes, one for each array,
so that the master thread may know when the threads are done their jobs by
acquiring every thread's busy mutex once it's been released. The job queue
also has condition variables for when the queue is both empty and not empty,
so that appropriate threads may wake up based on the queue's condition.
The intermediate partition structs also have their own write locks, thus
preventing multiple mapper threads from writing conflicting data concurrently.

The intermediate partition structures used to pass key-value pairs from the
mapper output to the reducer input were implemented as their own struct,
modelled as linked lists. Each pair_t struct consists of a key and value,
as well as a pointer to the next pair in the list. The partition_t struct has
a write lock, a size counter (so that the partitions can be sorted by size
before reducer jobs are submitted), and a pointer to the head of the list.
The linked list is always kept in sorted ascending order by key so that
pairs with the same key will always appear together in the list (this order
is enforced during pair insertion).

Although the threadpool library itself schedules submitted jobs using a
first-come, first-served (FCFS) policy, the overall mapreduce framework runs
a shortest job first (SJF) scheduling policy by sorting both map and reduce
jobs before submitting them as jobs to the threadpool. This was done because
the threadpool may begin popping jobs from the queue before all have been
added, meaning that if they were not added in SJF order to begin with then 
longer jobs that arrived earlier may be run first.


## Testing

This library was tested with the provided example wordcount application
throughout its entire development.

Since the threadpool library was implemented first, the first iteration of
MR_Run simply created and destroyed a threadpool without submitting any tasks.
This was then run in valgrind to validate memory safety. The map phase was then
developed and tested on its own, printing out the contents of the intermediate
partitions instead of invoking the reducer. Again, valgrind validated that
running the map jobs alone didn't produce any unforseen memory leaks.

Finally, the reduce phase was implemented, and with this, the entire
wordcount program was validated using both default values of 5 threads and
10 output partitions, as well as custom values (particularly limit-testing
the extreme case of 1 thread and/or 1 partition). Output values were
checked for correctness, and valgrind confirmed that there were no memory
leaks during the execution.
