//
// Created by 丁牧云 on 2024/4/4.
//

#include "vlog.h"
#include <fstream>
#include "utils.h"
#include <vector>

VLog::VLog(const string &filename)
{
    this->filename = filename;
    //预创建vlog文件,并计算文件大小给head
    ofstream ofs(filename,std::ios::binary | std::ios::out|std::ios::app);
    head = ofs.tellp();
    cout<<"head"<<head<<endl;
    ofs.close();
    initTail();
};

VLog::~VLog()
{
}

uint64_t VLog::write(const uint64_t key, const uint32_t vlen, const string &value)
{
    ofstream out(filename, ios::binary | ios::app | ios::out);
    if (!out)
    {
        cout << "open vlog file error" << endl;
        return -1;
    }
    out.seekp(0, ios::end);
    uint64_t offset = out.tellp();
    // cout<<"vlog:offset"<<offset<<endl;
    char magic = 0xff;
    out.write(&magic, sizeof(char)); // magic

    // checksum
    vector<unsigned char> data;
    // 将key转换为字节序列
    for (int i = 7; i >= 0; --i)
    {
        data.push_back((key >> (i * 8)) & 0xFF);
    }
    // 将vlen转换为字节序列
    for (int i = 3; i >= 0; --i)
    {
        data.push_back((vlen >> (i * 8)) & 0xFF);
    }
    // 添加value
    data.insert(data.end(), value.begin(), value.end());
    uint16_t checksum = utils::crc16(data);

    out.write((char *)&checksum, sizeof(uint16_t));

    out.write((char *)&key, sizeof(uint64_t));  // key
    out.write((char *)&vlen, sizeof(uint32_t)); // vlen
    out.write(value.data(), vlen);              // value
    head = out.tellp();
    out.close();
    return offset; // 返回偏移量，偏移量是从开始符号算的
}

string VLog::read(const uint64_t offset, uint32_t vlen)
{
    ifstream in(filename, ios::binary | ios::in);
    if (!in)
    {
        cout << "open vlog file error" << endl;
        return "";
    }

    if (vlen == 0)
    {
        in.seekg(offset + 3 + sizeof(uint64_t), ios::beg);
        in.read((char *)&vlen, sizeof(uint32_t));
    }
    in.seekg(offset + 3 + sizeof(uint64_t) + sizeof(uint32_t), ios::beg);

    std::string s(vlen, '\0'); // 创建一个足够大的字符串并初始化为0

    // 读取整个文件到字符串
    in.read(&s[0], vlen);
    return s;
}

uint64_t VLog::readKey(uint64_t &offset)
{
    ifstream in(filename, ios::binary | ios::in);
    if (!in)
    {
        cout << "open vlog file error" << endl;
        return -1;
    }
    offset += 3;
    in.seekg(offset, ios::beg);
    uint64_t key;
    in.read((char *)&key, sizeof(uint64_t)); // 获得key

    offset += 8;
    in.seekg(offset, ios::beg);
    uint32_t vlen;
    in.read((char *)&vlen, sizeof(uint32_t)); // 获得vlen

    offset += 4 + vlen; // 调整偏移量

    return key;
}

void VLog::initTail()
{
    tail = utils::seek_data_block(filename);
    //cout << filename << endl;
    if (tail == -1)
    {
        tail=0;
        cout << "seek data block error" << endl;
        return;
    }

    ifstream in(filename, ios::binary | ios::in);
    if (!in)
    {
        cout << "open vlog file error" << endl;
        return;
    }

    in.seekg(0, std::ios::end);
    head = in.tellg();

    while (tail + 15 < head)
    {
        // 校对magic
        char magic;
        in.seekg(tail, ios::beg);
        in.read(&magic, sizeof(char));
        if (magic == 0xff)
        {
            // 校对checksum
            uint64_t key;
            in.seekg(tail + 3, ios::beg);
            in.read((char *)&key, sizeof(uint64_t)); // 获得key

            uint32_t vlen;
            in.seekg(tail + 11, ios::beg);
            in.read((char *)&vlen, sizeof(uint32_t)); // 获得vlen

            string value;
            if (tail + 15 + vlen <= head)
            {
                in.seekg(tail + 15, ios::beg);
                value.resize(vlen);
                in.read(&value[0], vlen); // 获得value
            }

            // 计算当前的checksum
            vector<unsigned char> data;
            // 将key转换为字节序列
            for (int i = 7; i >= 0; --i)
            {
                data.push_back((key >> (i * 8)) & 0xFF);
            }
            // 将vlen转换为字节序列
            for (int i = 3; i >= 0; --i)
            {
                data.push_back((vlen >> (i * 8)) & 0xFF);
            }
            // 添加value
            data.insert(data.end(), value.begin(), value.end());
            uint16_t checksum = utils::crc16(data);

            // 读出当前存储的checksum
            uint16_t check;
            in.seekg(tail + 1, ios::beg);
            in.read((char *)&check, sizeof(uint16_t)); // 获得checksum

            if (check == checksum)
                break;
        }
        tail += 1;
    }
    cout << tail << endl;
    return;
}
