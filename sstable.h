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
    string filename;
    uint64_t size;
    SSTheader *header;
    BloomFilter *filter;
    VLog *vlog;
public:
    SSTable();
    SSTable(const string &filename,int timeStamp);
    ~SSTable();
    void updateHeader();
    void put(uint64_t key, const string &val);

};


#endif //LSMKV_HANDOUT_SSTABLE_H
