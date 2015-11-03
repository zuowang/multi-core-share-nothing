CC=g++
RELEASE_CFLAGS=-O3 -Wall
DEBUG_CFLAGS=-ggdb -gdwarf-2 -Wall
CLIBS=-lpthread -lrt

all: release \
     debug

release: aggregation.cc rand.cc rand.h
	$(CC) $(RELEASE_CFLAGS) $(AVX) -o release_agg aggregation.cc rand.cc $(CLIBS)
debug: aggregation.cc rand.cc rand.h
	$(CC) $(DEBUG_CFLAGS) $(AVX) -o debug_agg aggregation.cc rand.cc $(CLIBS) 
clean:
	rm -f release_agg debug_agg
