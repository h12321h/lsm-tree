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

void MemTable::change2SSTable() {
    string path="/data/level-0";
    if(!filesystem::exists(path))
        filesystem::create_directory(path);
    size_t file_count = std::distance(filesystem::directory_iterator(path), filesystem::directory_iterator{});
    string filename=path+"/"+to_string(file_count+1)+".sst";
    SSTable sst= SSTable(filename,file_count+1);
    //遍历跳表
    for(auto it=skiplist->head->forward[0];it!= nullptr;it=it->forward[0]){
        sst.put(it->key,it->val);
    }
    sst.updateHeader();
    reset();
}

