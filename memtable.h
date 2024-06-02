//
// Created by 丁牧云 on 2024/4/2.
//

#ifndef LSMKV_HANDOUT_MEMTABLE_H
#define LSMKV_HANDOUT_MEMTABLE_H
#include"skiplist.h"
#include"vlog.h"
#include"sstable.h"
#include <list>
#include <cstdint>
using namespace std;

class SSTable;
class MemTable {
private:
    SkipList *skiplist;

public:
    MemTable() ;

    ~MemTable() ;

    void put(uint64_t key, const string &val) ;

    string get(uint64_t key) const ;

    void del(uint64_t key);

    void scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string> > &list) const ;

    void reset() ;

    SSTable* change2SSTable(string dir,VLog *vlog) ;

    int getSize(){
        return skiplist->size;
    }
};


#endif //LSMKV_HANDOUT_MEMTABLE_H
