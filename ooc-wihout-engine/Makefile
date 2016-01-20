# Compiler options
CC = g++

CFLAGS = -O3 -DHE_PROFILER_ENABLE
LFLAGS = -L.. -lhe-profiler -lenergymon-default -lhbs-pow -lboost_system -lboost_filesystem -lconfig++ -ltcmalloc_minimal

BIN_DIR=./bin

all: preprocess2 pagerank

bindir:
	mkdir -p ${BIN_DIR}

pagerank: pagerank.cpp bindir
	$(CC) $(CFLAGS) -o ${BIN_DIR}/pagerank pagerank.cpp $(LFLAGS)
	@echo Finish generating pagerank ...

preprocess2: preprocess2.cpp bindir
	$(CC) -O3 -fopenmp -o ${BIN_DIR}/preprocess2 preprocess2.cpp -lstxxl -lpthread -lconfig++
	@echo Finish generating preprocess ...

clean:
	rm -rf ${BIN_DIR}
