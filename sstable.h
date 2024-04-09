//
// Created by 丁牧云 on 2024/4/3.
//

#ifndef LSMKV_HANDOUT_SSTABLE_H
#define LSMKV_HANDOUT_SSTABLE_H
#include<iostream>
#include"sstheader.h"
#include"bloomfilter.h"
#include"vlog.h"
using namespace std;
class SSTable {
private:
    struct Node{
        uint64_t key;
        uint64_t offset;
        uint32_t vlen;
        Node(){};
    };
    Node *data;
    string filename;
    uint64_t size;
    SSTheader *header;
    BloomFilter *filter;
    VLog *vlog;

public:
    SSTable();
    SSTable(const string &filename,int timeStamp,string vlog_name);
    ~SSTable();
    void updateHeader();
    void put(uint64_t key, const string &val);
    string get(uint64_t key) const;
    void loadSSTable();

};


#endif //LSMKV_HANDOUT_SSTABLE_H
