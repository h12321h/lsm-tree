#include "kvstore.h"
#include <string>
#include <filesystem>

const int MAX_MEM_SIZE=408;

KVStore::KVStore(const std::string &dir, const std::string &vlog_name) : KVStoreAPI(dir, vlog_name)
{
    this->dir=dir;
    this->vlog=new VLog(vlog_name);
    //this->vlog_name=vlog_name;
    mem = new MemTable();
    if(!filesystem::exists(dir))
        filesystem::create_directory(dir);
    sstListHead= nullptr;
}

KVStore::~KVStore()
{
    delete mem;
    //todo
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if(mem->getSize()>MAX_MEM_SIZE){
        SSTable *newSSTable=mem->change2SSTable(dir,vlog);
        SSTCache *newCache=new SSTCache(newSSTable,0,newSSTable->getTimeStamp(),sstListHead);//todo compact
        sstListHead=newCache;
    }
    mem->put(key,s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	//find in memtable
    string res=mem->get(key);
    if(res=="~DELETED~")
        return "";
    if(res!="")
        return res;
    
    //load sstable
    //遍历level，以及level下的sstable
    int currentLevel;
    int currentTimeStamp;
    if(sstListHead== nullptr){
        currentLevel=0;
        currentTimeStamp=0;
    }else{
        currentLevel=sstListHead->level;
        currentTimeStamp=sstListHead->timeStamp;
    }
    while(true){
        string path=dir+"/level-"+to_string(currentLevel);
        if(!filesystem::exists(path))//检查是否有这个level
            break;
        size_t file_count = std::distance(filesystem::directory_iterator(path), filesystem::directory_iterator{});//看这个level下有多少文件
        for(int j=currentTimeStamp+1;j<=file_count;j++){
            string filename=path+"/"+to_string(j)+".sst";
            SSTable *sst= new SSTable(filename,j,this->vlog);
            sst->loadSSTable();
           // cout<<"load new "<<filename<<" "<<sst->header->min_key<<" "<<sst->header->max_key<<endl;
            SSTCache *newCache=new SSTCache(sst,currentLevel,j,sstListHead);
            sstListHead=newCache;
        }
        currentLevel++;
    }

    //find in sstList
    SSTCache *p=sstListHead;
    while(p!=nullptr){
        res=p->sstable->get(key);
        if(res=="~DELETED~")
            return "";
        if(res!="")
            return res;
        p=p->next;
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    if(this->get(key)==""){
       // cout<<key<<endl;
        return 0;
    }
        
    put(key,"~DELETED~");
    return 1;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    cout<<"reset"<<endl;
    mem->reset();
    SSTCache *p=sstListHead;
    while(p!= nullptr){
        SSTCache *q=p;
        p=p->next;
        delete q;
    }
    sstListHead= nullptr;
    filesystem::remove_all(dir);
    filesystem::create_directory(dir);
    filesystem::remove_all(vlog->getFilename());
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    //scan memtable
    mem->scan(key1,key2,list);

    //load sstable
    int currentLevel;
    int currentTimeStamp;
    if(sstListHead== nullptr){
        currentLevel=0;
        currentTimeStamp=0;
    }else{
        currentLevel=sstListHead->level;
        currentTimeStamp=sstListHead->timeStamp;
    }
    while(true){
        string path=dir+"/level-"+to_string(currentLevel);
        if(!filesystem::exists(path))//检查是否有这个level
            break;
        size_t file_count = std::distance(filesystem::directory_iterator(path), filesystem::directory_iterator{});//看这个level下有多少文件
        for(int j=currentTimeStamp+1;j<=file_count;j++){
            string filename=path+"/"+to_string(j)+".sst";
            SSTable *sst= new SSTable(filename,j,vlog);
            sst->loadSSTable();
            SSTCache *newCache=new SSTCache(sst,currentLevel,j,sstListHead);
            sstListHead=newCache;
        }
        currentLevel++;
    }

    //scan sstList
    SSTCache *p=sstListHead;
    while(p!=nullptr){
        p->sstable->scan(key1,key2,list);
        p=p->next;
    }
    //sort list
    list.sort();//todo 优化
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{

}