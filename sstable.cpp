//
// Created by 丁牧云 on 2024/4/3.
//

#include "sstable.h"
#include<fstream>

SSTable::SSTable(const string &filename,int timeStamp,VLog *vlog){
   // cout<<vlog->getFilename()<<endl;
    this->filename=filename;
    header=new SSTheader(filename);
    header->setTimeStamp(timeStamp);
    this->vlog=vlog;
   // cout<<"create sstable"<<endl;

    //cout<<this->vlog->getFilename()<<endl;
    //vlog=new VLog(vlog_name);
    filter=new BloomFilter();
}

SSTable::~SSTable(){
    delete[] data;
    delete filter;
    delete header;
}

string SSTable::get(uint64_t key) const{
    if(key>header->max_key||key<header->min_key)
        return "";
    if(!filter->get(key)){//bloomfilter判断是否存在
       // cout<<"filter not found"<<endl;
        return "";
    }  
    int left=0,right=header->num-1;
    while(left<=right&&right>=0&&left<header->num){//二分查找
        uint64_t mid=(left+right)/2;
        if(data[mid].key==key){
            if(data[mid].vlen==0)
                return "~DELETED~";
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
    }
    filter->set(key);//bloomfilter
    header->setMinKey(key);//todo优化
    header->setMaxKey(key);
    data[header->num].key=key;
    data[header->num].offset=offset;
    data[header->num].vlen=vlen;
    header->num++;
}

void SSTable::scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string> > &list) const{
    if(key1>header->max_key)
        return ;
    if(key2<header->min_key)
        return ;
    long left=0,right=header->num-1;
    long start=header->num,end=0;
    //二分查找，找到第一个大于等于key1
    while(left<=right&&right>=0&&left<header->num){
        uint64_t mid=(left+right)/2;
        if(data[mid].key>=key1){
            right=mid-1;
            start=mid;
        }else{
            left=mid+1;
        }
    }
    //二分查找，找到最后一个小于等于key2
    //left=0;
    right=header->num-1;
    while(left<=right&&right>=0&&left<header->num){
        uint64_t mid=(left+right)/2;
        if(data[mid].key<=key2){
            left=mid+1;
            end=mid;
        }else{
            right=mid-1;
        }
    }
    for(int i=start;i<=end;i++){
        if(data[i].vlen>0){
            list.push_back(make_pair(data[i].key,vlog->read(data[i].offset,data[i].vlen)));
        }
    }
}

void SSTable::writeSSTable(){
    header->writeHeader(0);
    filter->writeFilter(filename,32);
    ofstream out(filename,ios::binary|ios::app|ios::out);
    if(!out){
        cout<<"open file error"<<endl;
        return;
    }
    out.seekp(0,ios::end);
    for(int i=0;i<header->num;i++){
        out.write((char*)&data[i].key,sizeof(uint64_t));
        out.write((char*)&data[i].offset,sizeof(uint64_t));
        out.write((char*)&data[i].vlen,sizeof(uint32_t));
    }
    out.close();
}

void SSTable::loadSSTable(){
    ifstream in(filename,ios::binary|ios::in);
    if(!in){
        cout<<"open file error"<<endl;
        return;
    }
    header->readHeader(0);
    data=new Node[header->num];
    filter->readFilter(filename,32);
    in.seekg(32+8192,ios::beg);
    for(int i=0;i<header->num;i++){
        uint64_t key,offset;
        uint32_t vlen;
        in.read((char*)&key,sizeof(uint64_t));
        in.read((char*)&offset,sizeof(uint64_t));
        in.read((char*)&vlen,sizeof(uint32_t));
        data[i].key=key;
        data[i].offset=offset;
        data[i].vlen=vlen;
    }
    in.close();
}
