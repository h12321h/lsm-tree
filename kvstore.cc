#include "kvstore.h"
#include<cmath>
#include <string>
#include <filesystem>

const int MAX_MEM_SIZE = 408 ;

KVStore::KVStore(const std::string &dir, const std::string &vlog_name) : KVStoreAPI(dir, vlog_name)
{
    this->dir = dir;
    // this->vlog_name=vlog_name;
    mem = new MemTable();
    if (!filesystem::exists(dir))
        filesystem::create_directory(dir);
    sstListHead = nullptr;
    this->vlog = new VLog(vlog_name);
    laodSSTCache();
}

KVStore::~KVStore()
{
    SSTable *newSSTable = mem->change2SSTable(dir, vlog);
    SSTCache *newCache = new SSTCache(newSSTable, fileNameLevel(newSSTable->getFilename()), fileNameNum(newSSTable->getFilename()), sstListHead); // todo compact
    sstListHead = newCache;
    compaction();
    delete vlog;
    SSTCache *p = sstListHead;
    while (p != nullptr)
    {
        SSTCache *q = p;
        p = p->next;
        delete q;
    }
    delete mem;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (mem->getSize() >=MAX_MEM_SIZE)
    {
        SSTable *newSSTable = mem->change2SSTable(dir, vlog);
        SSTCache *newCache = new SSTCache(newSSTable, fileNameLevel(newSSTable->getFilename()), fileNameNum(newSSTable->getFilename()), sstListHead); // todo compact
        sstListHead = newCache;
        compaction();
    }
    mem->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    // find in memtable
    string res = mem->get(key);
   // if(key==39680)
      //  cout<<res<<endl;
    if (res == "~DELETED~")
        return "";
    if (res != "")
        return res;

    // load sstable ？？
    // sstListHead = nullptr;
    // laodSSTCache();

    // find in sstList
    SSTCache *p = sstListHead;
    while (p != nullptr)
    {
        res = p->sstable->get(key);
        // if(key==39680)
        //     cout<<res<<endl;
        if (res == "~DELETED~")
            return "";
        if (res != "")
            return res;
        p = p->next;
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    if (this->get(key) == "")
    {
        // cout<<key<<endl;
        return 0;
    }

    put(key, "~DELETED~");
    return 1;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    //cout << "reset" << endl;
    mem->reset();
    SSTCache *p = sstListHead;
    while (p != nullptr)
    {
        SSTCache *q = p;
        p = p->next;
        delete q;
    }
    sstListHead = nullptr;
    filesystem::remove_all(dir);
    filesystem::create_directory(dir);
    utils::rmfile(vlog->getFilename());
    vlog->reset();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    // scan memtable
    mem->scan(key1, key2, list);

    // load sstable
    //laodSSTCache();

    // scan sstList
    SSTCache *p = sstListHead;
    while (p != nullptr)
    {
        p->sstable->scan(key1, key2, list);
        p = p->next;
    }
    // sort list
    list.sort(); // todo 优化
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
    uint64_t tail = vlog->getTail();
    uint64_t offset = tail;
   // cout <<"tail"<< tail << endl;
    while (offset < tail + chunk_size)
    {
        string s = vlog->read(offset);
        uint64_t key = vlog->readKey(offset);
        uint64_t findOffset = gcGet(key);
        if (findOffset == offset)
        {
            // cout<<"same"<<endl;
            //cout << s << endl;
            put(key, s);
        }
    }
   // cout <<"chunk_size"<< chunk_size << endl;
   // cout <<"offset"<< offset << endl;
    SSTable *newSSTable = mem->change2SSTable(dir, vlog);
    SSTCache *newCache = new SSTCache(newSSTable, fileNameLevel(newSSTable->getFilename()), fileNameNum(newSSTable->getFilename()), sstListHead); // todo compact
    sstListHead = newCache;
    compaction();
    utils::de_alloc_file(vlog->getFilename(), tail, offset - tail); // 打洞
    vlog->setTail(offset);
    return;
}

void KVStore::laodSSTCache()
{
    // load sstable
    int currentLevel=0;
    while (true)
    {
        string path = dir + "/level-" + to_string(currentLevel);
        if (!filesystem::exists(path)) // 检查是否有这个level
            break;
        int maxFile = maxFileName(currentLevel);
        for (int j = maxFile; j >=0; j--)
        {
            string filename = path + "/" + to_string(j) + ".sst";
            //检查是否有这个文件
            if (!filesystem::exists(filename))
                continue;
            SSTable *sst = new SSTable(filename, vlog);
            sst->loadSSTable();
            SSTCache *newCache = new SSTCache(sst, currentLevel, j, nullptr);
            insertSSTCache(newCache);
        }
        currentLevel++;
    }
    return;
}

uint64_t KVStore::gcGet(uint64_t key)
{ // 0代表无需再次插入
    if (mem->get(key) != "")
        return 0;
    uint64_t offset;
    // load sstable
   // laodSSTCache();

    // find in sstList
    SSTCache *p = sstListHead;
    while (p != nullptr)
    {
        offset = p->sstable->gcGet(key);
        if (offset == 1) // 1表示被删除
            return 0;
        if (offset > 0)
            return offset;
        p = p->next;
    }
    return 0;
}

void KVStore::compaction(){
  //  cout<<endl;
    int checkLevel=0;//从level0开始检查有没有爆
    for(;;checkLevel++){
      //  cout<<"level:"<<checkLevel<<endl;
        string path = dir + "/level-" + to_string(checkLevel);
        if (!filesystem::exists(path)) // 检查是否有这个level
            break;
        int levelFileCount=fileCount(checkLevel);
       // cout<<"levelFileCout:"<<levelFileCount<<endl;
        
        if(levelFileCount>pow(2,checkLevel+1)){
           // cout<<"compact level:"<<checkLevel<<endl;
            vector<SSTable*> compactList;//要合并的文件
            //统计覆盖区间
            uint64_t min_key=UINT64_MAX;
            uint64_t max_key=0;
            SSTCache *p = sstListHead;
            if(checkLevel==0){//处理level0
                while (p != nullptr)
                {
                    if(p->level==0){
                      //  cout<<p->sstable->getFilename()<<endl;
                        if(p->sstable->getMinKey()<min_key)
                            min_key=p->sstable->getMinKey();
                        if(p->sstable->getMaxKey()>max_key)
                            max_key=p->sstable->getMaxKey();
                        compactList.push_back(p->sstable);
                        deleteSSTCache(p->level,p->file);
                        utils::rmfile(p->sstable->getFilename());
                       // cout<<"delete:"<<p->sstable->getFilename()<<endl;
                    }
                    p = p->next;
                }
            }else{//处理其他level
                //计算要处理多少个文件
                int num=levelFileCount-pow(2,checkLevel+1);
                int i=0;
                while (p != nullptr)
                {
                    if(p->level==checkLevel){
                        i++;
                        if(i>num){
                            if(p->sstable->getMinKey()<min_key)
                                min_key=p->sstable->getMinKey();
                            if(p->sstable->getMaxKey()>max_key)
                                max_key=p->sstable->getMaxKey();
                            compactList.push_back(p->sstable);
                            deleteSSTCache(p->level,p->file);
                            utils::rmfile(p->sstable->getFilename());
                        }
                    }
                    p = p->next;
                }
            }

            //cout<<"min_key:"<<min_key<<"  max_key:"<<max_key<<endl;


            //查找下一层交集的文件
            int nextLevel=checkLevel+1;
            p=sstListHead;
            while (p != nullptr)
            {
                if(p->level==nextLevel){
                    if(!(p->sstable->getMaxKey()<min_key||p->sstable->getMinKey()>max_key)){
                        compactList.push_back(p->sstable);
                        deleteSSTCache(p->level,p->file);
                        utils::rmfile(p->sstable->getFilename());
                    }
                }
                p = p->next;
            }

            string nextdir=dir+"/level-"+to_string(checkLevel+1);
            if (!filesystem::exists(nextdir))
                filesystem::create_directory(nextdir);

            //归并合并 从compactList中取出两个文件合并
            while(compactList.size()>1){
                //cout<<"merge"<<endl;
                SSTable *sst1=compactList.front();
                compactList.erase(compactList.begin());
                SSTable *sst2=compactList.front();
                compactList.erase(compactList.begin());
                SSTable *newSSTable = new SSTable(nextdir+"/"+to_string(maxFileName(checkLevel+1)+1)+".sst",vlog);
                newSSTable->merge(sst1,sst2);
                compactList.insert(compactList.begin(),newSSTable);
                //compactList.push_back(newSSTable);
            }
            //拆分
            SSTable *bigSSTable = compactList.back();
            
            compactList.pop_back();
            while(bigSSTable->getNum()>0){
                SSTable *newSSTable = new SSTable(nextdir+"/"+to_string(maxFileName(checkLevel+1)+1)+".sst",vlog);
                newSSTable->split(bigSSTable);
                insertSSTCache(new SSTCache(newSSTable,checkLevel+1,maxFileName(checkLevel+1),nullptr));
            }
            delete bigSSTable;
        }
    }
}

void KVStore::deleteSSTCache(int deleteLevel,int deleteFile){
   // cout<<deleteLevel<<" "<<deleteFile<<endl;
    SSTCache *p = sstListHead;
    SSTCache *q = nullptr;
    while (p != nullptr)
    {
        //cout<<"trydelete:"<<p->level<<" "<<p->file<<endl;
        if(p->level==deleteLevel&&p->file==deleteFile){
            if(q==nullptr){
                sstListHead=p->next;
            }else{
                q->next=p->next;
            }
            return ;
        }else{
            q=p;
            p=p->next;
        }
    }
    return ;
}
void KVStore::insertSSTCache(SSTCache *newCache){
    SSTCache *p = sstListHead;
    SSTCache *q = nullptr;
    while (p != nullptr)
    {
        if(p->level>newCache->level){
            if(q==nullptr){
                newCache->next=sstListHead;
                sstListHead=newCache;
            }else{
                q->next=newCache;
                newCache->next=p;
            }
            return;
        }else if(p->level==newCache->level){
            if(p->file<newCache->file){
                if(q==nullptr){
                    newCache->next=sstListHead;
                    sstListHead=newCache;
                }else{
                    q->next=newCache;
                    newCache->next=p;
                }
                return;
            }
        }
        q=p;
        p=p->next;
    }
    if(q==nullptr)
        sstListHead=newCache;
    else
        q->next=newCache;
    return;
}

int KVStore::fileCount(int level){
    string path = dir + "/level-" + to_string(level);
    if (!filesystem::exists(path)) // 检查是否有这个level
        return 0;
    size_t file_count = std::distance(filesystem::directory_iterator(path), filesystem::directory_iterator{}); // 看这个level下有多少文件
    return file_count;
}

//返回由数字组成的文件名中 文件名数字最大的值
int KVStore::maxFileName(int level){
    string path = dir + "/level-" + to_string(level);
    int max_value = 0;
    for (const auto& entry : filesystem::directory_iterator(path)) {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().stem().string(); // 获取不带扩展名的文件名
            // 将文件名转换为整数
            int current_value = std::stoi(filename);
            if (current_value > max_value) {
                max_value = current_value;
            }
        }
    }
    return max_value;
}

int KVStore::fileNameNum(string filename){
    // 找到最后一个 '/' 的位置
    size_t lastSlashPos = filename.find_last_of('/');
    // 从最后一个 '/' 后面的位置开始截取字符串
    filename = filename.substr(lastSlashPos + 1);
    string num=filename.substr(0,filename.find('.'));
    //cout<<num<<endl;
    return stoi(num);
}

int KVStore::fileNameLevel(string filename){
    // 找到最后一个 '-' 的位置
    size_t lastSlashPos = filename.find_last_of('-');
    filename = filename.substr(lastSlashPos + 1);
    string num=filename.substr(0,filename.find('/'));
   // cout<<num<<endl;
    return stoi(num);
}
