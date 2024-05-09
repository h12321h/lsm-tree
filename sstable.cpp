//
// Created by 丁牧云 on 2024/4/3.
//
#include "sstable.h"
#include<fstream>
#include <chrono>
#include <cstdint>
#include <ctime>

uint64_t generateTimestamp() {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
}


SSTable::SSTable(const string &filename,VLog *vlog){
   // cout<<vlog->getFilename()<<endl;
    this->filename=filename;
    header=new SSTheader(filename);
    //时间戳
    uint64_t timeStamp = generateTimestamp();
    //cout<<timeStamp<<endl;
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
    if(key>header->max_key||key<header->min_key){
        if(key==39680)
           // cout<<"超范围"<<endl;
        return "";
    }
        
    if(!filter->get(key)){//bloomfilter判断是否存在
    if(key==39680)
        cout<<"filter not found"<<endl;
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

uint64_t SSTable::gcGet(uint64_t key){
    if(key>header->max_key||key<header->min_key)
        return 0;
    if(!filter->get(key)){//bloomfilter判断是否存在
       // cout<<"filter not found"<<endl;
        return 0;
    }  
    int left=0,right=header->num-1;
    while(left<=right&&right>=0&&left<header->num){//二分查找
        uint64_t mid=(left+right)/2;
        if(data[mid].key==key){
            if(data[mid].vlen==0)
                return 1;//1表示被删除
            return data[mid].offset+3+sizeof(uint64_t)+sizeof(uint32_t)+data[mid].vlen;
        }
        else if(data[mid].key<key){
            left=mid+1;
        }
        else{
            right=mid-1;
        }
    }
    return 0;//没找到
}

//合并两个sstable的data，其他的不管
void SSTable::merge(SSTable *s1,SSTable *s2){
    header->setMinKey(min(s1->getMinKey(),s2->getMinKey()));
    header->setMaxKey(max(s1->getMaxKey(),s2->getMaxKey()));
    header->setNum(s1->getNum()+s2->getNum());
    data=new Node[header->num];
    uint64_t i=0,j=0;
    uint64_t size=0;
    while(i<s1->getNum()&&j<s2->getNum()){
        // if(s1->data[i].key==39680||s2->data[j].key==39680)
        //     cout<<"merge 39680"<<endl;
         if(s1->data[i].key<s2->data[j].key){
        //     if(s1->data[i].key==39680||s2->data[j].key==39680)
        //         cout<<"merge< 39680"<<endl;
            data[size++]=s1->data[i];
            i++;
        }else if(s1->data[i].key==s2->data[j].key){
            // if(s1->data[i].key==39680||s2->data[j].key==39680)
            //     cout<<"merge= 39680"<<endl;
           // cout<<"same"<<endl;
           // cout<<s1->getTimeStamp()<<" "<<s2->getTimeStamp()<<endl;
            if(s1->getTimeStamp()>s2->getTimeStamp())
                data[size++]=s1->data[i];
            else
                data[size++]=s2->data[j];
            //header->addNum(-1);
            i++;
            j++;
        }else{
            // if(s1->data[i].key==39680||s2->data[j].key==39680)
            //     cout<<"merge> 39680"<<endl;
            data[size++]=s2->data[j];
            j++;
        }
    }
    while(i<s1->getNum()){
        // if(s1->data[i].key==39680)
        //     cout<<"merge i 39680"<<endl;
        data[size++]=s1->data[i];
        i++;
    }
    while(j<s2->getNum()){
        // if(s1->data[j].key==39680)
        //     cout<<"merge j 39680"<<endl;
        data[size++]=s2->data[j];
        j++;
    }
    header->setNum(size);
    delete s1;
    delete s2;
    return ;
}

//拆分，拆成MAX_MEM_SIZE大小的
void SSTable::split(SSTable *big){
    if(big->getNum()<408)
        header->setNum(big->getNum());
    else
        header->setNum(408);
    
   // cout<<"split minkey:"<<this->getMinKey()<<endl;
   // cout<<"split maxkey:"<<this->getMaxKey()<<endl;
    data=new Node[header->num];
    int i=0;
    while(i<header->num){
        data[i]=big->data[i];
        // if(data[i].key==39680)
        //     cout<<"split put 39680"<<endl;
        header->setMinKey(data[i].key);
        header->setMaxKey(data[i].key);
        filter->set(data[i].key);
        i++;
    }
    big->header->setNum(big->getNum()-header->num);
    Node *tmp=new Node[big->getNum()];
    for(int j=0;j<big->getNum();j++){
        tmp[j]=big->data[i];
        // if(tmp[j].key==39680)
        //     cout<<"split putto j 39680"<<endl;
        i++;
    }
    //delete big->data;
    big->data=tmp;
    writeSSTable();
    return ;
}