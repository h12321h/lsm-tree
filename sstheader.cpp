//
// Created by 丁牧云 on 2024/4/3.
//

#include "sstheader.h"
#include<fstream>

SSTheader::SSTheader() {
}

SSTheader::SSTheader(const string &filename) {
    this->filename=filename;
    num=0;
    min_key=UINT64_MAX;
    max_key=0;
}

SSTheader::~SSTheader() {
}

void SSTheader::setTimeStamp(uint64_t timeStamp){
    this->timeStamp=timeStamp;
}

void SSTheader::setNum(uint64_t num){
    this->num=num;
}

void SSTheader::addNum(uint64_t addnum){
    this->num+=addnum;
}

void SSTheader::setMinKey(uint64_t key){
    this->min_key=min(this->min_key,key);
}

void SSTheader::setMaxKey(uint64_t key){
    this->max_key=max(this->max_key,key);
}

bool SSTheader::readHeader(uint64_t offset){
    ifstream in(filename,ios::binary);
    if(!in){
        cout<<"open file error"<<endl;
        return false;
    }
    in.seekg(offset,ios::beg);
    in.read((char*)&timeStamp,sizeof(uint64_t));
    in.read((char*)&num,sizeof(uint64_t));
    in.read((char*)&min_key,sizeof(uint64_t));
    in.read((char*)&max_key,sizeof(uint64_t));
    in.close();
    return true;
}

bool SSTheader::writeHeader(uint64_t offset){
    ofstream out(filename,ios::binary);
    if(!out){
        cout<<"open file error"<<endl;
        return false;
    }
    out.seekp(offset,ios::beg);
    out.write((char*)&timeStamp,sizeof(uint64_t));
    out.write((char*)&num,sizeof(uint64_t));
    out.write((char*)&min_key,sizeof(uint64_t));
    out.write((char*)&max_key,sizeof(uint64_t));
    out.close();
    return true;
}

