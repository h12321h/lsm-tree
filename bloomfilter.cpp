//
// Created by 丁牧云 on 2024/4/3.
//

#include "bloomfilter.h"
#include<fstream>
#define K 3
BloomFilter::BloomFilter() {
    hash = new bool[BLOOMFILTER_SIZE];
    for (int i = 0; i < BLOOMFILTER_SIZE; i++) {
        hash[i] = false;
    }
}

BloomFilter::~BloomFilter() {
    delete[] hash;
}

void BloomFilter::set(uint64_t key) {
    for(int k=1;k<=K;k++){
        uint64_t hashvalue[2] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), k*10+10, hashvalue);
        hash[hashvalue[1]%BLOOMFILTER_SIZE]=1;
    }
    return;
}

bool BloomFilter::get(uint64_t key) {
    for(int k=1;k<=K;k++){
        uint64_t hashvalue[2] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), k*10+10, hashvalue);
        if(hash[hashvalue[1]%BLOOMFILTER_SIZE]==0){
            return false;
        }
    }
    return true;
}

void BloomFilter::readFilter(string filename,int offset){
    ifstream in(filename,ios::binary);
    if(!in){
        cout<<"open file error"<<endl;
        return;
    }
    in.seekg(offset,ios::beg);
    in.read((char*)hash,sizeof(bool)*BLOOMFILTER_SIZE);
    in.close();
}

void BloomFilter::writeFilter(string filename,int offset){
    ofstream out(filename,ios::binary|ios::app|ios::out);
    if(!out){
        cout<<"open file error"<<endl;
        return;
    }
    out.seekp(offset,ios::beg);
    out.write((char*)hash,sizeof(bool)*BLOOMFILTER_SIZE);
    out.close();
}
