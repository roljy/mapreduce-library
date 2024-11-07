CC = gcc
CFLAGS = -Wall -Werror -std=c11 -pthread
DBFLAGS = -g -O0
.PHONY: clean valgrind

wordcount: distwc.o mapreduce.o threadpool.o
	$(CC) $(CFLAGS) $^ -o $@

valgrind: db_wordcount
	valgrind --tool=memcheck --leak-check=yes --fair-sched=yes ./$< ./sample_inputs/sample1.txt ./sample_inputs/sample2.txt

db_wordcount: db_threadpool.o db_mapreduce.o db_distwc.o
	$(CC) $(CFLAGS) $(DBFLAGS) $^ -o $@

threadpool.o: threadpool.c
	$(CC) $(CFLAGS) -c $^ -o $@

mapreduce.o: mapreduce.c
	$(CC) $(CFLAGS) -c $^ -o $@

distwc.o: distwc.c
	$(CC) $(CFLAGS) -c $^ -o $@

db_%.o: %.c
	$(CC) $(CFLAGS) $(DBFLAGS) -c $^ -o $@

clean:
	rm -f wordcount *.o
