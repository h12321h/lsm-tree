//
// Created by 丁牧云 on 2024/4/4.
//

#ifndef LSMKV_HANDOUT_VLOG_H
#define LSMKV_HANDOUT_VLOG_H
#include<iostream>
#include<fstream>
using namespace std;

class VLog {
private:
    string filename;
    uint64_t tail;
    uint64_t haed;

public:
    VLog(){};
    VLog(const string &filename){
        this->filename = filename;
    };
    ~VLog();
    uint64_t write(const uint64_t key,const uint32_t vlen,const string &value);
    string read(const uint64_t offset,const uint32_t vlen);
    string getFilename(){
        return filename;
    }
};


#endif //LSMKV_HANDOUT_VLOG_H
