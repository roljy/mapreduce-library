CC = gcc
CFLAGS = -Wall -std=c11
.PHONY: clean

wordcount: distwc.o mapreduce.o threadpool.o
	$(CC) $(CFLAGS) $^ -o $@

threadpool.o: threadpool.c
	$(CC) $(CFLAGS) -c $^ -o $@

mapreduce.o: mapreduce.c
	$(CC) $(CFLAGS) -c $^ -o $@

distwc.o: distwc.c
	$(CC) $(CFLAGS) -c $^ -o $@

clean:
	rm -f wordcount *.o
