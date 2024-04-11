//
// Created by 丁牧云 on 2024/4/2.
//

#include "memtable.h"
#include"sstable.h"
#include<iostream>
#include<fstream>
#include <filesystem>
#include <string>
using namespace std;

MemTable::MemTable() {
    skiplist=new SkipList();
}

MemTable::~MemTable() {
    delete skiplist;
}

void MemTable::put(uint64_t key, const string &val) {
    skiplist->put(key,val);
}

string MemTable::get(uint64_t key) const {
    return skiplist->get(key);
}

void MemTable::del(uint64_t key) {
    skiplist->put(key,"~DELETED~");
}

void MemTable::scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string> > &list) const {
    skiplist->scan(key1,key2,list);
}

void MemTable::reset() {
    delete skiplist;
    skiplist=new SkipList();
}

void MemTable::change2SSTable(string dir,string vlog_name) {//todo 直接转存为缓存
    string path=dir+"/level-0";//存储路径//todo
    if(!filesystem::exists(path))//创建目录
        filesystem::create_directory(path);
    size_t file_count = std::distance(filesystem::directory_iterator(path), filesystem::directory_iterator{});//文件数-时间戳
    string filename=path+"/"+to_string(file_count+1)+".sst";//文件名
    SSTable *sst=new SSTable(filename,file_count+1,vlog_name);//创建sst
    sst->data=new SSTable::Node[skiplist->size];
    for(auto it=skiplist->head->forward[1];it!= nullptr;it=it->forward[1]){
       // cout<<it->key<<" "<<it->val<<endl;
        sst->put(it->key,it->val);
    }
    sst->writeSSTable();
    //cout<<"header:"<<sst->header->min_key<<" "<<sst->header->max_key<<endl;
    reset();
    delete sst;
    return;
}

