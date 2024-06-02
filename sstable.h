//
// Created by 丁牧云 on 2024/4/3.
//

#ifndef LSMKV_HANDOUT_SSTABLE_H
#define LSMKV_HANDOUT_SSTABLE_H
#include<iostream>
#include"sstheader.h"
#include"bloomfilter.h"
#include"vlog.h"
#include"memtable.h"
#include <cstdint>
using namespace std;
class KVStore;
class SSTable {
friend class MemTable;
friend class KVStore;
private:
    struct Node{
        uint64_t key;
        uint64_t offset;
        uint32_t vlen;
        Node(){};
    };
    Node *data;//缓存的元组,数组
    string filename;
    SSTheader *header;
    BloomFilter *filter;
    VLog *vlog;

public:
    SSTable(){}
    SSTable(const string &filename,VLog *vlog);
    ~SSTable();
    void put(uint64_t key, const string &val);
    string get(uint64_t key) const;
    void scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string> > &list) const;
    void writeSSTable();
    void loadSSTable();
    uint64_t getTimeStamp(){
        return header->timeStamp;
    }
    uint64_t getNum(){
        return header->num;
    }
    uint64_t getMinKey(){
        return header->min_key;
    }
    uint64_t getMaxKey(){
        return header->max_key;
    }
    Node* getData(){
        return data;
    }
    string getFilename(){
        return filename;
    }


    uint64_t gcGet(uint64_t key);//用于gc时对比，返回偏移量(偏移量是当前key+当前entry后)

    void merge(SSTable *s1,SSTable *s2);
    void split(SSTable *big);
};


#endif //LSMKV_HANDOUT_SSTABLE_H
