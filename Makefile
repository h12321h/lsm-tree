
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall -g

all: correctness persistence

correctness: kvstore.o correctness.o memtable.o sstable.o skiplist.o sstheader.o vlog.o bloomfilter.o
	$(LINK.o) $^ -o $@

persistence: kvstore.o persistence.o memtable.o sstable.o skiplist.o sstheader.o vlog.o bloomfilter.o
	$(LINK.o) $^ -o $@

clean:r
	-rm -f correctness persistence *.o

r:
	-rm -rf data/level-0
	-rm -f data/vlog
	-rm -f error_log.txt
