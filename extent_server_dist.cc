#include "extent_server_dist.h"

chfs_raft *extent_server_dist::leader() const {
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0) {
        return this->raft_group->nodes[0];
    } else {
        return this->raft_group->nodes[leader];
    }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_CRT;
    cmd.type = type;
    int term, index;
    bool flag = leader()->new_command(cmd, term, index);
    assert(flag); // leader() is not leader!
    std::unique_lock <std::mutex> lck(cmd.res->mtx);
    while (!cmd.res->done)
        cmd.res->cv.wait(lck);
    id = cmd.res->id;
    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_PUT;
    cmd.id = id;
    cmd.buf = buf;
    int term, index;
    bool flag = leader()->new_command(cmd, term, index);
    assert(flag); // leader() is not leader!
    std::unique_lock <std::mutex> lck(cmd.res->mtx);
    while (!cmd.res->done)
        cmd.res->cv.wait(lck);
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_GET;
    cmd.id = id;
    int term, index;
    bool flag = leader()->new_command(cmd, term, index);
    assert(flag); // leader() is not leader!
    std::unique_lock <std::mutex> lck(cmd.res->mtx);
    while (!cmd.res->done)
        cmd.res->cv.wait(lck);
    buf = cmd.res->buf;
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_GETA;
    cmd.id = id;
    int term, index;
    bool flag = leader()->new_command(cmd, term, index);
    assert(flag); // leader() is not leader!
    std::unique_lock <std::mutex> lck(cmd.res->mtx);
    while (!cmd.res->done)
        cmd.res->cv.wait(lck);
    a = cmd.res->attr;
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_RMV;
    cmd.id = id;
    int term, index;
    bool flag = leader()->new_command(cmd, term, index);
    assert(flag); // leader() is not leader!
    std::unique_lock <std::mutex> lck(cmd.res->mtx);
    while (!cmd.res->done)
        cmd.res->cv.wait(lck);
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist() {
    delete this->raft_group;
}