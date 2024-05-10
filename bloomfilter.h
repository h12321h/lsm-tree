//
// Created by 丁牧云 on 2024/4/3.
//

#ifndef LSMKV_HANDOUT_BLOOMFILTER_H
#define LSMKV_HANDOUT_BLOOMFILTER_H
#include<iostream>
#include"MurmurHash3.h"
using namespace std;
//BF
#define BLOOMFILTER_SIZE 8192

class BloomFilter {
private:
    bool *hash;
public:
    BloomFilter();
    ~BloomFilter();
    void set(uint64_t key);
    bool get(uint64_t key);
    void readFilter(string filename,int offset);
    void writeFilter(string filename,int offset);
};


#endif //LSMKV_HANDOUT_BLOOMFILTER_H
