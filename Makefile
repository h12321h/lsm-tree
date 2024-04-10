
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall -g

all: correctness persistence

correctness: kvstore.o correctness.o memtable.o sstable.o skiplist.o sstheader.o vlog.o bloomfilter.o
	$(LINK.o) $^ -o $@

persistence: kvstore.o correctness.o memtable.o sstable.o skiplist.o sstheader.o vlog.o bloomfilter.o
	$(LINK.o) $^ -o $@

clean:
	-rm -f correctness persistence *.o
	-rm -rf data/level-0

r:
	-rm -rf data/level-0
