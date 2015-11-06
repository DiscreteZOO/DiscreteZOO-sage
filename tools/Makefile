.PHONY: all

NAUTY=/Users/katja/Nauty

all: benchmarking

benchmarking: benchmarking.cpp
	clang++ -Wall -Wextra -Winline -Wshadow -pedantic -std=c++11 $(NAUTY)/nauty.a -I$(NAUTY) $^ -o $@

clean:
	-rm benchmarking
