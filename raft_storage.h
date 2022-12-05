#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);

    ~raft_storage();

    // Lab3: Your code here
    void store_metadata(int term, int voteFor);

    void store_log(const log_entry<command> &log);

    void restore_metadata(int &currentTerm, int &votedFor);

    void restore_log(std::vector <log_entry<command>> &log);

private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string file_dir, metadata_path, log_path;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
    file_dir = dir;
    metadata_path = file_dir + "/metadata.log";
    log_path = file_dir + "/log.log";
}

template<typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
}

template<typename command>
void raft_storage<command>::store_metadata(int term, int voteFor) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);
    std::ofstream out(metadata_path);
    if (out.is_open()) {
        out << term << " " << voteFor << std::endl;
        out.close();
    }
}

template<typename command>
void raft_storage<command>::store_log(const log_entry<command> &log) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);
    std::ofstream out(log_path, std::ios::out | std::ios::app | std::ios::binary);
    if (out.is_open()) {
        out.write((char *) &log.term, sizeof(int));
        out.write((char *) &log.id, sizeof(int));

        // use interface of command
        int size = log.cmd.size();
        out.write((char *) &size, sizeof(int));

        char *buf = new char[size + 1];
        (log.cmd).serialize(buf, size);
        out.write(buf, size);

        delete[] buf;
        out.close();
    }
}

template<typename command>
void raft_storage<command>::restore_metadata(int &term, int &voteFor) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);
    std::ifstream in(metadata_path);
    if (in.is_open()) {
        in >> term >> voteFor;
        in.close();
    }
}

template<typename command>
void raft_storage<command>::restore_log(std::vector <log_entry<command>> &log) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);
    std::ifstream in(log_path, std::ios::in | std::ios::binary);
    if (in.is_open()) {
        int term, id;
        command cmd;
        while (in.read((char *) &term, sizeof(int))) {
            in.read((char *) &id, sizeof(int));
            int size;
            char *buf;
            in.read((char *) &size, sizeof(int));
            buf = new char[size + 1];
            in.read(buf, size);
            cmd.deserialize(buf, size);
            while (id >= (int) log.size()) log.push_back(log_entry<command>());
            log[id] = log_entry<command>(cmd, term, id);
        }
        in.close();
    };
}

#endif // raft_storage_h