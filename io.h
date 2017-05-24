#include <iostream>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/file.hpp>
#include <chrono>
#include <unordered_map>


class Timer {
private:
    std::chrono::steady_clock::time_point begin;
public:
    Timer() {
        begin = std::chrono::steady_clock::now();
    }
    void finish() {
        std::cout << "Time taken (sec): "
                  << std::chrono::duration_cast<std::chrono::seconds> (std::chrono::steady_clock::now() - begin).count()
                  << "\n"
                  << std::endl;
    }
};


class UuidMap {
public:
    std::unordered_map<std::string, int> map;
    int get_uid(std::string &uuid) {
        int uid;
        auto pair = map.find(uuid);
        if (pair != map.end()) {
            uid = pair->second;
        } else {
            uid = map.size();
            map.insert(make_pair(uuid, uid));
        }
        return uid;
    }

    std::unordered_map<std::string, int>* data() {
        return &map;
    };

};


class CsvGzReader {
public:
    boost::iostreams::filtering_istream inbuf;
    std::string header;

    CsvGzReader(std::string filename) {
        boost::iostreams::file_source file(filename, std::ios_base::in | std::ios_base::binary);
        inbuf.push(boost::iostreams::gzip_decompressor());
        inbuf.push(file);
        std::getline(inbuf, header);
        std::cout << "HEADER: "<< header << std::endl;
    }

    bool getline(std::string *line, char delimeter = '\n') {
        if (std::getline(inbuf, *line, delimeter)) {
            return true;
        } else {
            return false;
        }
    }

    operator bool() {
        return !inbuf.eof();
    }
};