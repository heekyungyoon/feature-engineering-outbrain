#include <iostream>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/file.hpp>
#include <chrono>

class timer {
private:
    std::chrono::steady_clock::time_point begin;
public:
    timer() {
        begin = std::chrono::steady_clock::now();
    }
    void finish() {
        std::cout << std::chrono::duration_cast<std::chrono::seconds>
                          (std::chrono::steady_clock::now() - begin).count()
                  << "\n"
                  << std::endl;
    }
};


class csvgz_reader {
public:
    boost::iostreams::filtering_istream inbuf;
    std::string header;

    csvgz_reader(std::string filename) {
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