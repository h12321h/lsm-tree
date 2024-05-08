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
    uint64_t head;

public:
    VLog(){};
    VLog(const string &filename);
    ~VLog();
    uint64_t write(const uint64_t key,const uint32_t vlen,const string &value);
    string read(const uint64_t offset,uint32_t vlen=0);
    uint64_t readKey(uint64_t &offset);

    string getFilename(){
        return filename;
    }
    uint64_t getTail(){
        return tail;
    }
    void setTail(uint64_t tail){
        this->tail=tail;
    }
    void initTail();

    void reset(){
        tail=0;
        head=0;
    }

};


#endif //LSMKV_HANDOUT_VLOG_H
