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
    uint64_t offset=vlog->write(key,vlen,val);//写入vlog,返回偏移量
    header->addNum(1);//键值对数目+1
    header->setMinKey(key);
    header->setMaxKey(key);
    ofstream out(filename,ios::binary);//写入sstable
    out.seekp(0,ios::end);
    out.write((char*)&key,sizeof(uint64_t));//key
    out.write((char*)&offset,sizeof(uint64_t));//offset
    out.write((char*)vlen,sizeof(uint32_t));//vlen
    out.close();
}

void SSTable::updateHeader(){
    header->writeHeader(0);
}
