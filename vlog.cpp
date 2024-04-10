//
// Created by 丁牧云 on 2024/4/4.
//

#include "vlog.h"
#include<fstream>
#include"utils.h"
#include<vector>

VLog::~VLog() {

}

int VLog::write(const uint64_t key, const uint32_t vlen,const string value) {
    ofstream out(filename,ios::binary|ios::app|ios::out);
    if(!out){
        cout<<"open file error"<<endl;
        return -1;
    }
    uint64_t offset=out.tellp();
    //cout<<"vlog:offset"<<offset<<endl;
    out.seekp(0,ios::end);
    char magic=0xff;
    out.write(&magic,sizeof(char));//magic

    //checksum
    vector<unsigned char> data;
    // 将key转换为字节序列
    for (int i = 7; i >= 0; --i) {
        data.push_back((key >> (i * 8)) & 0xFF);
    }
    // 将vlen转换为字节序列
    for (int i = 3; i >= 0; --i) {
        data.push_back((vlen >> (i * 8)) & 0xFF);
    }
    // 添加value
    data.insert(data.end(), value.begin(), value.end());
    uint16_t checksum=utils::crc16(data);

    out.write((char*)&checksum,sizeof(uint16_t));

    out.write((char*)&key,sizeof(uint64_t));//key
    out.write((char*)&vlen,sizeof(uint32_t));//vlen
    out.write(value.c_str(),value.size());//value
    out.close();
    return offset+3+sizeof(uint64_t)+sizeof(uint32_t);//返回偏移量
}

string VLog::read(const uint64_t offset, const uint32_t vlen) {
    ifstream in(filename,ios::binary|ios::in);
    if(!in){
        cout<<"open file error"<<endl;
        return "";
    }
    in.seekg(offset,ios::beg);

    char *buf=new char[vlen];
    in.read(buf,vlen);
    in.close();
    string s(buf,vlen);
    //cout<<s<<endl;
    return s;
}


