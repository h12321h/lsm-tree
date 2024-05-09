#include <iostream>
#include <unordered_map>
#include <chrono>
#include <vector>
#include <random>
#include"kvstore.h"

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

void testPUT(const std::vector<int>& keys) {
    for (auto key : keys) {
        db.put(key, std::string(key, '3'));  // 写入操作
    }
}

void testGET(const std::vector<int>& keys) {
    for (auto key : keys) {
        db.get(key);  // 读取操作
    }
}

void testDEL(const std::vector<int>& keys) {
    for (auto key : keys) {
        db.del(key);  // 删除操作
    }
}



int main() {
    // 生成随机键
    std::vector<int> keys = generateRandomKeys(KEY_RANGE, NUM_OPERATIONS);

    // 测试 PUT 操作的吞吐量
    double putThroughput = measureThroughput([&]() { testPUT(keys); }, NUM_OPERATIONS);
    // 测试 GET 操作的吞吐量
    double getThroughput = measureThroughput([&]() { testGET(keys); }, NUM_OPERATIONS);
    // 测试 DEL 操作的吞吐量
    double delThroughput = measureThroughput([&]() { testDEL(keys); }, NUM_OPERATIONS);

    db.reset();

    // 测试 PUT 操作的平均时延
    double putLatency = measureLatency([&]() { testPUT(keys); }, NUM_OPERATIONS);
    // 测试 GET 操作的平均时延
    double getLatency = measureLatency([&]() { testGET(keys); }, NUM_OPERATIONS);
    // 测试 DEL 操作的平均时延
    double delLatency = measureLatency([&]() { testDEL(keys); }, NUM_OPERATIONS);

    std::cout << "PUT throughput: " << putThroughput << " ops/s, latency: " << putLatency << " us/op" << std::endl;
    std::cout << "GET throughput: " << getThroughput << " ops/s, latency: " << getLatency << " us/op" << std::endl;
    std::cout << "DEL throughput: " << delThroughput << " ops/s, latency: " << delLatency << " us/op" << std::endl;


    return 0;
}