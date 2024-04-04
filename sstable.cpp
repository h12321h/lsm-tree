//
// Created by 丁牧云 on 2024/4/3.
//

#include "sstable.h"
#include<fstream>

SSTable::SSTable(const string &filename,int timeStamp){
    this->filename=filename;
    header=new SSTheader(filename);
    header->setTimeStamp(timeStamp);
    vlog=new VLog("/data/vlog");
}

void SSTable::put(uint64_t key, const string &val){
    uint32_t vlen=val.size();
    uint64_t offset=vlog->write(key,vlen,val);
    header->addNum(1);
    header->setMinKey(key);
    header->setMaxKey(key);
    ofstream out(filename,ios::binary);
    out.seekp(0,ios::end);
    out.write((char*)&offset,sizeof(uint64_t));
    out.write((char*)&offset,sizeof(uint64_t));
    out.write((char*)vlen,sizeof(uint32_t));
    out.close();
}

void SSTable::updateHeader(){
    header->writeHeader(0);
}
