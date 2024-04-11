//
// Created by 丁牧云 on 2024/4/2.
//

#ifndef LSMKV_HANDOUT_SKIPLIST_H
#define LSMKV_HANDOUT_SKIPLIST_H
#include <list>
#include<iostream>
#include <vector>
using namespace std;
const int MAX_LEVEL=16;
const double P=0.5;
class SkipList {
public:
    struct NODE{
        uint64_t  key;
        string val;
        std::vector<NODE*> forward;
        NODE(){}
        NODE(uint64_t key,string value,int level):key(key),val(value),forward(level, nullptr){}
        ~NODE(){}
    };
    uint64_t level;
    double p;
    NODE *head;

    uint64_t size;
    SkipList(){
        level=1;
        this->p=P;
        head= new NODE(0,"",MAX_LEVEL);
        size=0;
    }
    ~SkipList();
    void put(uint64_t key, const string &val);
    string get(uint64_t key) const;
    void scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string> > &list) const;
    void reset();
};


#endif //LSMKV_HANDOUT_SKIPLIST_H
