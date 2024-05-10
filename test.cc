#include <iostream>
#include <unordered_map>
#include <chrono>
#include <vector>
#include <random>
#include<cmath>
#include"kvstore.h"
#include <map>
#include <atomic>
#include <thread>
#include <mutex>
#include <fstream>

const int NUM_OPERATIONS = 1000000;  // 总的操作次数
const int KEY_RANGE = 10000;         // 键的范围


KVStore db("./data", "./data/vlog");

std::vector<int> generateRandomKeys(int range, int numKeys) {
    std::vector<int> keys;
    keys.reserve(numKeys);
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> dist(1, range);
    for (int i = 0; i < numKeys; ++i) {
        keys.push_back(dist(gen));
    }
    return keys;
}


template <typename Func>
double measureThroughput(Func&& func, int numOps) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    return numOps / elapsed.count();  // 吞吐量 = 操作数 / 时间
}

template <typename Func>
double measureLatency(Func&& func, int numOps) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> elapsed = end - start;
    return elapsed.count() / numOps;  // 平均时延 = 总时间 / 操作数
}

void testPUT(const std::vector<int>& keys,const int &num) {
    for (int i=0;i<num;i++){
        db.put(keys[i], std::string(256, '1'));  // 写入操作
    }
}

void testGET(const std::vector<int>& keys,const int &num) {
    for (int i=0;i<num;i++) {
        db.get(keys[i]);  // 读取操作
    }
}

void testSCAN(const std::vector<int>& keys,const int &num) { 
    for (int i=0;i<num;i++) {
    std::list<std::pair<uint64_t, std::string>> list;
   // cout<<min(keys[i],keys[i+1])<<" "<<max(keys[i],keys[i+1])<<endl;
        db.scan(min(keys[i],keys[i+1]),max(keys[i],keys[i+1]),list);  // 读取操作
    }
}


void testDEL(const std::vector<int>& keys,const int &num) {
    for (int i=0;i<num;i++) {
        db.del(keys[i]);  // 删除操作
    }
}

void testPUTthrou(const std::vector<int>& keys,const int &num,const int &value){
    for (int i=0;i<num;i++){
        db.put(keys[i], std::string(value, '1'));  // 写入操作
    }
}

std::map<long long, int> requests_per_sec;
std::mutex data_mutex;

// 模拟单线程处理PUT请求
void process_put_requests(int duration) {
    auto start_time = std::chrono::system_clock::now();
    long long elapsed_half_seconds = 0;
    uint64_t key = 0;

    while (elapsed_half_seconds < duration) {
        auto current_time = std::chrono::system_clock::now();
        elapsed_half_seconds = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time).count() / 250;
        db.put(key, std::string(256, '1'));  // 写入操作
        key++;
        {
            std::lock_guard<std::mutex> guard(data_mutex);
            requests_per_sec[elapsed_half_seconds]++;
        }

        // 模拟一个PUT请求的处理
        // 你可以根据实际的请求处理逻辑调整这里的代码

    }
}

// 保存数据到CSV文件
void save_data_to_file(const std::string& filename) {
    std::ofstream file(filename);
    file << "Time (Seconds), Requests\n";
    for (const auto& [sec, count] : requests_per_sec) {
        file << sec << "," << count << "\n";
    }
    file.close();
}


int main() {
    // // 生成随机键
    // std::vector<int> keys = generateRandomKeys(KEY_RANGE, 16384+2);    
    // for(int i=128;i<=16384;i*=2){

    //     // 测试 PUT 操作
    //     double putLatency = measureLatency([&]() { testPUT(keys,i); }, i);
    //     // 测试 GET 操作
    //     double getLatency = measureLatency([&]() { testGET(keys,i); }, i);
    //     // 测试 SCAN 操作
    //     double scanLatency = measureLatency([&]() { testSCAN(keys,i); }, i);
    //     // 测试 DEL 操作
    //     double delLatency = measureLatency([&]() { testDEL(keys,i); }, i);

    //     db.reset();

    //     std::cout<<i<<std::endl;
    //     std::cout << "PUT Latency: " << putLatency <<  std::endl;
    //     std::cout << "GET Latency: " << getLatency <<  std::endl;
    //     std::cout << "SCAN Latency: " << scanLatency<< std::endl;
    //     std::cout << "DEL Latency: " << delLatency <<  std::endl;
    // }

    // // 生成随机键
    // std::vector<int> keys = generateRandomKeys(KEY_RANGE, 8192+2);
    // for(int i=128;i<=16384;i*=2){
    //     // 测试 PUT 操作的吞吐量
    //     double putThroughput = measureThroughput([&]() { testPUTthrou(keys,8192,i); }, 8192);
    //     // 测试 GET 操作的吞吐量
    //     double getThroughput = measureThroughput([&]() { testGET(keys,8192); }, 8192);
    //     // 测试 SCAN 操作的吞吐量
    //     double scanThroughput = measureThroughput([&]() { testSCAN(keys,8192); }, 8192);
    //     // 测试 DEL 操作的吞吐量
    //     double delThroughput = measureThroughput([&]() { testDEL(keys,8192); }, 8192);

    //     db.reset();

    //     std::cout<<i<<std::endl;
    //     std::cout << "PUT throughput: " << putThroughput <<  std::endl;
    //     std::cout << "GET throughput: " << getThroughput <<  std::endl;
    //     std::cout << "SCAN throughput: " << scanThroughput << std::endl;
    //     std::cout << "DEL throughput: " << delThroughput <<  std::endl;

    // }

    // // 缓存的时延
    // // 生成随机键
    // std::vector<int> keys = generateRandomKeys(KEY_RANGE, 8192+2);
    // double putLatency = measureLatency([&]() { testPUT(keys,8192); }, 8192);
    // double getLatency = measureLatency([&]() { testGET(keys,8192); }, 8192);
    // cout<<"Get Latency: "<<getLatency<<endl;

    // int duration_seconds = 60; // 持续时间为10秒

    // // 开始PUT请求的处理过程
    // process_put_requests(duration_seconds);

    // // 保存数据
    // save_data_to_file("throughput_data.csv");
    // std::cout << "Data has been saved to throughput_data.csv" << std::endl;

    // 生成随机键
    std::vector<int> keys = generateRandomKeys(KEY_RANGE, 8192+2);
    // 测试 PUT 操作
    double putLatency = measureLatency([&]() { testPUT(keys,8192); }, 8192);
    // 测试 GET 操作
    double getLatency = measureLatency([&]() { testGET(keys,8192); }, 8192);

    std::cout << "PUT Latency: " << putLatency <<  std::endl;
    std::cout << "GET Latency: " << getLatency <<  std::endl;

    return 0;
}