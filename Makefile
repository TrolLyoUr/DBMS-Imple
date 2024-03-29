# Makefile for COMP9315 23T1 Assignment 2

CC=gcc
CFLAGS=-std=gnu99 -Wall -g
OBJS=main.o ro.o db.o bufpool.o filecache.o hashtab.o
BINS=main

main: $(OBJS)
	$(CC) -std=gnu99 -o main $(OBJS)

main.o: ro.h db.h

ro.o: ro.h db.h bufpool.h filecache.h hashtab.h

db.o: db.h

bufpool.o: bufpool.h db.h

filecache.o: filecache.h db.h

hashtab.o: hashtab.h db.h

clean:
	rm -f $(BINS) *.o
