//
// Created by 丁牧云 on 2024/4/2.
//
#include "skiplist.h"

void SkipList::put(uint64_t key, const string &val) {
    NODE **update=new NODE*[MAX_LEVEL];
    NODE *t=this->head;
    for(int i=this->level;i>=1;i--){
        while(t->forward[i]!= nullptr && t->forward[i]->key < key)
            t=t->forward[i];
        update[i]=t;
    }
    if(t->forward[1]!= nullptr && t->forward[1]->key==key){
        t->forward[1]->val=val;
        return;
    }

    int lvl=1;
    while((double)std::rand() / RAND_MAX < p && lvl<MAX_LEVEL-1)
        lvl++;

    if(lvl>this->level){
        for(int i=lvl;i>this->level;i--) {
            update[i] = this->head;
        }
        this->level=lvl;
    }
    NODE *newnode=new NODE(key,val,lvl+1);
    for(int i=1;i<=lvl;i++){
        newnode->forward[i]=update[i]->forward[i];
        update[i]->forward[i]=newnode;
    }
    size++;
    delete[] update;
}
string SkipList::get(uint64_t key) const {
    NODE *t=this->head;
    for(int i=level;i>=1;i--){
        while(t->forward[i]!= nullptr && t->forward[i]->key<key)
            t=t->forward[i];
        if(t->forward[i]!= nullptr&&t->forward[i]->key==key){
            if(t->forward[i]->val=="~DELETED~")
                return "not found";
            return t->forward[i]->val;
        }
    }
    return "not found";
}

void SkipList::scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, std::string> > &list) const {
    NODE *t=this->head;
    for(int i=level;i>=1;i--){
        while(t->forward[i]!= nullptr && t->forward[i]->key<key1)
            t=t->forward[i];
    }
    t=t->forward[1];
    while(t!= nullptr && t->key<=key2){
        list.push_back(make_pair(t->key,t->val));
        t=t->forward[1];
    }
}

void SkipList::reset() {
    NODE *t=this->head->forward[1];
    while(t!= nullptr){
        NODE *tmp=t;
        t=t->forward[1];
        delete tmp;
    }
    for(int i=1;i<MAX_LEVEL;i++)
        this->head->forward[i]= nullptr;
    this->level=1;
}

SkipList::~SkipList() {

}


