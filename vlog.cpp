//
// Created by 丁牧云 on 2024/4/4.
//

#include "vlog.h"
#include<fstream>

VLog::VLog() {

}

VLog::~VLog() {

}

int VLog::write(const uint64_t key, const uint32_t vlen,const string value) {
    ofstream out(filename,ios::binary);
    if(!out){
        cout<<"open file error"<<endl;
        return -1;
    }
    uint64_t offset=out.tellp();
    out.seekp(0,ios::end);
    out.write("oxff",sizeof("0xff"));
    out.write("",sizeof()
    out.write((char*)&key,sizeof(uint64_t));
    out.write((char*)&vlen,sizeof(uint32_t));
    out.write(value.c_str(),value.size());
    out.close();
    return offset+sizeof(uint64_t)+sizeof(uint32_t);
}

string VLog::read(const uint64_t offset, const uint32_t vlen) {
    ifstream in(filename,ios::binary);
    if(!in){
        cout<<"open file error"<<endl;
        return "";
    }
    in.seekg(offset,ios::beg);

    char *buf=new char[vlen];
    in.read(buf,vlen);
    in.close();
    return string(buf,vlen);
}


