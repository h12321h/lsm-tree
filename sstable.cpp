//
// Created by 丁牧云 on 2024/4/3.
//

#include "sstable.h"
#include<fstream>

SSTable::SSTable(const string &filename,int timeStamp,string vlog_name){
    this->filename=filename;
    header=new SSTheader(filename);
    header->setTimeStamp(timeStamp);
    vlog=new VLog(vlog_name);
    filter=new BloomFilter();
}

SSTable::~SSTable(){
    delete header;
    delete vlog;
    delete filter;
    delete[] data;
}

string SSTable::get(uint64_t key) const{
    if(!filter->get(key))//bloomfilter判断是否存在
        return "";
    uint64_t left=0,right=header->num-1;
    while(left<=right){//二分查找
        uint64_t mid=(left+right)/2;
        if(data[mid].key==key&&data[mid].vlen>0){
            return vlog->read(data[mid].offset,data[mid].vlen);
        }
        else if(data[mid].key<key){
            left=mid+1;
        }
        else{
            right=mid-1;
        }
    }
    return "";//没找到
}

void SSTable::put(uint64_t key, const string &val){
    uint32_t vlen=val.size();
    uint64_t offset=0;
    if(val=="~DELETED~"){//被删除
        vlen=0;
    }else{
        offset=vlog->write(key,vlen,val);//写入vlog,返回偏移量
        filter->set(key);//bloomfilter
    }
    header->addNum(1);//键值对数目+1
    header->setMinKey(key);//todo优化
    header->setMaxKey(key);
    ofstream out(filename,ios::binary|ios::app|ios::out);//写入sstable
    if (!out.is_open()) {
        std::cerr << "Failed to open file." << std::endl;
        return;
    }
    out.seekp(0,ios::end);
    std::streampos position = out.tellp();
 //   std::cout << filename<<"SST position: " << position << std::endl;
    out.write((char*)&key,sizeof(uint64_t));//key
    out.write((char*)&offset,sizeof(uint64_t));//offset
    out.write((char*)&vlen,sizeof(uint32_t));//vlen
    out.flush();
    out.close();
}

void SSTable::updateHeader(){
    header->writeHeader(0);
}

void SSTable::updateFilter(){
    filter->writeFilter(filename,32);
}

void SSTable::loadSSTable(){
    ifstream in(filename,ios::binary);
    if(!in){
        cout<<"open file error"<<endl;
        return;
    }
    in.seekg(0,ios::beg);
    header->readHeader(0);
    data=new Node[header->num];
    filter->readFilter(filename,32);
    int i=0;
    while(!in.eof()){
        uint64_t key,offset;
        uint32_t vlen;
        in.read((char*)&key,sizeof(uint64_t));
        in.read((char*)&offset,sizeof(uint64_t));
        in.read((char*)&vlen,sizeof(uint32_t));
        data[i].key=key;
        data[i].offset=offset;
        data[i].vlen=vlen;
        i++;
    }
    in.close();
}
