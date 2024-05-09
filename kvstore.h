#pragma once

#include "kvstore_api.h"
#include "memtable.h"
#include "sstable.h"
#include"vlog.h"
#include"utils.h"

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
    string dir;//存储路径
    string vlog_name;
    VLog *vlog;
    MemTable *mem;
    struct SSTCache{
        SSTable *sstable;
        int level;
        int file;
        SSTCache *next;
        SSTCache(SSTable *sstable,int level,int file,SSTCache *next= nullptr){
            this->sstable=sstable;
            this->level=level;
            this->file=file;
            this->next=next;
        }
        ~SSTCache(){
            delete sstable;
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

    void laodSSTCache();
    uint64_t gcGet(uint64_t key);//用于gc时对比，返回偏移量(偏移量是当前key+当前entry后)
    void compaction();

    void deleteSSTCache(int deleteLevel,int deleteFile);
    void insertSSTCache(SSTCache *newCache);

    int fileCount(int level);
    int maxFileName(int level);
    int fileNameNum(string filename);
    int fileNameLevel(string filename);

};
