#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() : cmd_tp(CMD_NONE), res(std::make_shared<result>()) {
    // Lab3: Your code here
    res->start = std::chrono::system_clock::now();
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
        cmd_tp(cmd.cmd_tp), type(cmd.type), id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}

chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const {
    // Lab3: Your code here
    switch (cmd_tp) {
        case CMD_NONE: {
            return sizeof(command_type);
        }
        case CMD_CRT: {
            return sizeof(command_type) + sizeof(type);
        }
        case CMD_PUT: {
            return sizeof(command_type) + sizeof(id) + sizeof(int) + buf.size();
        }
        case CMD_GET:
        case CMD_GETA:
        case CMD_RMV: {
            return sizeof(command_type) + sizeof(id);
        }
    }
    assert(false);
    return 0;
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    *(reinterpret_cast<command_type *>(buf_out)) = cmd_tp;
    buf_out += sizeof(command_type);
    switch (cmd_tp) {
        case CMD_NONE: {
            return;
        }
        case CMD_CRT: {
            *(reinterpret_cast<uint32_t *>(buf_out)) = type;
            return;
        }
        case CMD_PUT: {
            *(reinterpret_cast<extent_protocol::extentid_t *>(buf_out)) = id;
            buf_out += sizeof(id);
            int sz = (int) buf.size();
            *(reinterpret_cast<int *>(buf_out)) = sz;
            buf_out += sizeof(int);
            memcpy(buf_out, buf.c_str(), sz);
//            printf("serialize PUT totsize=%d size=%d\n",size, sz);
            return;
        }
        case CMD_GET:
        case CMD_GETA:
        case CMD_RMV: {
            *(reinterpret_cast<extent_protocol::extentid_t *>(buf_out)) = id;
            return;
        }
    }
    assert(false);
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    char *in = const_cast<char * >(buf_in);
    cmd_tp = *(reinterpret_cast<command_type *>(in));
    in += sizeof(command_type);
    switch (cmd_tp) {
        case CMD_NONE: {
            return;
        }
        case CMD_CRT: {
            type = *(reinterpret_cast<uint32_t *>(in));
            return;
        }
        case CMD_PUT: {
            id = *(reinterpret_cast<extent_protocol::extentid_t *>(in));
            in += sizeof(id);
            int sz = *(reinterpret_cast<int *>(in));
//            printf("deserialize PUT totsize=%d size=%d\n",size, sz);
            in += sizeof(int);
            buf = std::string(in, sz);
            return;
        }
        case CMD_GET:
        case CMD_GETA:
        case CMD_RMV: {
            id = *(reinterpret_cast<extent_protocol::extentid_t *>(in));
            return;
        }
    }
    assert(false);
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    m << (int) cmd.cmd_tp << cmd.type << cmd.id << cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int cmd_tp;
    u >> cmd_tp >> cmd.type >> cmd.id >> cmd.buf;
    cmd.cmd_tp = (chfs_command_raft::command_type) cmd_tp;
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(chfs_cmd.res->mtx);
    chfs_cmd.res->start = std::chrono::system_clock::now();
    int t;
    switch (chfs_cmd.cmd_tp) {
        case chfs_command_raft::command_type::CMD_NONE: {
            break;
        }
        case chfs_command_raft::command_type::CMD_CRT: {
            es.create(chfs_cmd.type, chfs_cmd.res->id);
            break;
        }
        case chfs_command_raft::command_type::CMD_PUT: {
            es.put(chfs_cmd.id, chfs_cmd.buf, t);
            break;
        }
        case chfs_command_raft::command_type::CMD_GET: {
            es.get(chfs_cmd.id, chfs_cmd.res->buf);
            break;
        }
        case chfs_command_raft::command_type::CMD_GETA: {
            es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
            break;
        }
        case chfs_command_raft::command_type::CMD_RMV: {
            es.remove(chfs_cmd.id, t);
            break;
        }
        default:
            assert(false);
    }
    chfs_cmd.res->done = true;
    chfs_cmd.res->cv.notify_all();
    return;
}


