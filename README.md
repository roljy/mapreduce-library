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

TODO


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
