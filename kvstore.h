#pragma once

#include "kvstore_api.h"
#include "memtable.h"
#include "sstable.h"
#include"vlog.h"

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
    string dir;
    string vlog_name;
    MemTable *mem;
    struct SSTCache{
        SSTable *sstable;
        int level;
        int timeStamp;
        SSTCache *next;
        SSTCache(SSTable *sstable,int level,int timeStamp,SSTCache *next= nullptr){
            this->sstable=sstable;
            this->level=level;
            this->timeStamp=timeStamp;
            this->next=next;
        }
    };
    SSTCache *sstListHead;

public:
	KVStore(const std::string &dir, const std::string &vlog_name);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;
};
