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
using namespace std;
class SSTable {
friend class MemTable;
private:
    struct Node{
        uint64_t key;
        uint64_t offset;
        uint32_t vlen;
        Node(){};
    };
    Node *data;//缓存的元组
    string filename;
    SSTheader *header;
    BloomFilter *filter;
    VLog *vlog;

public:
    SSTable();
    SSTable(const string &filename,int timeStamp,VLog *vlog);
    ~SSTable();
    void put(uint64_t key, const string &val);
    string get(uint64_t key) const;
    void scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string> > &list) const;
    void writeSSTable();
    void loadSSTable();
    int getTimeStamp(){
        return header->timeStamp;
    }

};


#endif //LSMKV_HANDOUT_SSTABLE_H
