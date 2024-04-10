//
// Created by 丁牧云 on 2024/4/3.
//

#ifndef LSMKV_HANDOUT_SSTHEADER_H
#define LSMKV_HANDOUT_SSTHEADER_H
#include<iostream>
using namespace std;

class SSTheader {
public:
    string filename;
    uint64_t timeStamp;  // 时间戳的序列号
    uint64_t num;  // 该SSTable中的键值对数目
    uint64_t min_key;
    uint64_t max_key;

    SSTheader();
    SSTheader(const string &filename);
    ~SSTheader();
    void setTimeStamp(uint64_t timeStamp);
    void setNum(uint64_t num);
    void addNum(uint64_t addnum);
    void setMinKey(uint64_t key);
    void setMaxKey(uint64_t key);
    bool readHeader(uint64_t offset);
    void writeHeader(uint64_t offset);
};


#endif //LSMKV_HANDOUT_SSTHEADER_H
