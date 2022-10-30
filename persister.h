#ifndef persister_h
#define persister_h

#include <fcntl.h>

#include <fstream>
#include <iostream>
#include <mutex>
#include <vector>

#include "extent_server.h"
#include "rpc.h"

#define MAX_LOG_SZ 131072

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires.
 *
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT
 * command.
 * 3. each chfs_commands contains transaction ID, command type, and other
 * information.
 * 4. you can treat a chfs_command as a log entry.
 */

typedef unsigned long long txid_t;
enum cmd_type {
  CMD_BEGIN = 0,
  CMD_COMMIT,
  CMD_CREATE,
  CMD_PUT,
  CMD_GET,
  CMD_GETATTR,
  CMD_REMOVE,
  CMD_DEFAULT
};
class chfs_command {
 public:
  cmd_type cmdTy;
  txid_t txid = 0;
  // constructor
  chfs_command(cmd_type cmd) : cmdTy(cmd), txid(0) {}
  chfs_command(txid_t id) : cmdTy(CMD_DEFAULT), txid(id) {}
  chfs_command(txid_t id, cmd_type cmd) : cmdTy(cmd), txid(id) {}
  virtual void save_log(std::ofstream& out) = 0;
  virtual void read_log(std::ifstream& in) = 0;
  virtual void print() = 0;
};

class chfs_command_begin : public chfs_command {
 public:
  chfs_command_begin() : chfs_command(CMD_BEGIN) {}
  chfs_command_begin(txid_t id) : chfs_command(id, CMD_BEGIN) {}

  void save_log(std::ofstream& out) {
    out.write(reinterpret_cast<char*>(&cmdTy), sizeof(cmdTy));
    out.write(reinterpret_cast<char*>(&txid), sizeof(txid));
  }

  void read_log(std::ifstream& in) {
    in.read(reinterpret_cast<char*>(&txid), sizeof(txid));
  }

  void print() {}
};

class chfs_command_commit : public chfs_command {
 public:
  chfs_command_commit() : chfs_command(CMD_COMMIT) {}
  chfs_command_commit(txid_t id) : chfs_command(id, CMD_COMMIT) {}

  void save_log(std::ofstream& out) {
    out.write(reinterpret_cast<char*>(&cmdTy), sizeof(cmdTy));
    out.write(reinterpret_cast<char*>(&txid), sizeof(txid));
  }

  void read_log(std::ifstream& in) {
    in.read(reinterpret_cast<char*>(&txid), sizeof(txid));
  }

  void print() {}
};

class chfs_command_create : public chfs_command {
 public:
  // 表示创建类型与创建后返回的inum
  uint32_t type, inum;
  chfs_command_create() : chfs_command(CMD_CREATE) {}
  chfs_command_create(txid_t id, uint32_t ty, uint32_t inum_)
      : chfs_command(id, CMD_CREATE), type(ty), inum(inum_) {}

  void save_log(std::ofstream& out) {
    out.write(reinterpret_cast<char*>(&cmdTy), sizeof(cmdTy));
    out.write(reinterpret_cast<char*>(&txid), sizeof(txid));
    out.write(reinterpret_cast<char*>(&inum), sizeof(inum));
    out.write(reinterpret_cast<char*>(&type), sizeof(type));
  }

  void read_log(std::ifstream& in) {
    in.read(reinterpret_cast<char*>(&txid), sizeof(txid));
    in.read(reinterpret_cast<char*>(&inum), sizeof(inum));
    in.read(reinterpret_cast<char*>(&type), sizeof(type));
  }
  void print() {
    printf("create read_log txid=%llu inum=%u type=%u\n", txid, inum, type);
  }
};

class chfs_command_put : public chfs_command {
 public:
  uint32_t inum, size;
  std::string str;
  chfs_command_put() : chfs_command(CMD_PUT) {}
  chfs_command_put(txid_t id, uint32_t inum_, uint32_t sz, const std::string& s)
      : chfs_command(id, CMD_PUT), inum(inum_), size(sz), str(s) {}

  void save_log(std::ofstream& out) {
    out.write(reinterpret_cast<char*>(&cmdTy), sizeof(cmdTy));
    out.write(reinterpret_cast<char*>(&txid), sizeof(txid));
    out.write(reinterpret_cast<char*>(&inum), sizeof(inum));
    out.write(reinterpret_cast<char*>(&size), sizeof(size));
    assert(size == str.size());
    // for (int i = 0; i < size; i++) assert(str[i] != 0);
    out.write(str.c_str(), size);
    // printf("save_log put  txid=%d inum =%d size=%d\n", txid, inum, size);
  }

  void read_log(std::ifstream& in) {
    in.read(reinterpret_cast<char*>(&txid), sizeof(txid));
    in.read(reinterpret_cast<char*>(&inum), sizeof(inum));
    in.read(reinterpret_cast<char*>(&size), sizeof(size));
    char* ch = new char[size + 1];
    in.read(ch, size);
    ch[size] = 0;
    str = ch;
    if (str.size() != size) std::cout << str.size() << " " << size << std::endl;
    // assert(str.size() == size);
    delete ch;
  }
  void print() {
    printf("put  txid=%lld inum =%d size=%d\n", txid, inum, size);
    // std::cout << str << std::endl;
    // std::cout.flush();
  }
};

class chfs_command_remove : public chfs_command {
 public:
  uint32_t inum;
  chfs_command_remove() : chfs_command(CMD_REMOVE) {}
  chfs_command_remove(txid_t id, uint32_t inum_)
      : chfs_command(id, CMD_REMOVE), inum(inum_) {}

  void save_log(std::ofstream& out) {
    out.write(reinterpret_cast<char*>(&cmdTy), sizeof(cmdTy));
    out.write(reinterpret_cast<char*>(&txid), sizeof(txid));
    out.write(reinterpret_cast<char*>(&inum), sizeof(inum));
  }

  void read_log(std::ifstream& in) {
    // auto ptr = dynamic_cast<chfs_command_remove*>(p);
    // assert(ptr != nullptr);
    in.read(reinterpret_cast<char*>(&txid), sizeof(txid));
    in.read(reinterpret_cast<char*>(&inum), sizeof(inum));
  }

  void print() {}
};
// 定义一个指针类型
typedef chfs_command* chfs_command_ptr;

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to
 * persist and recover data.
 *
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
// 要什么模板类！
// 为了不重定义只能放里面了
class chfs_persister {
 public:
  // restored log data
  std::vector<chfs_command*> log_entries;

  chfs_persister(const std::string& dir) {
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
  }
  ~chfs_persister() {
    // Your code here for lab2A
    // for (auto x : log_entries) delete x;
  }

  // persist data into solid binary file
  // You may modify parameters in these functions
  void append_log(chfs_command* log) {
    // Your code here for lab2A
    log_entries.push_back(log);

    std::ofstream out(file_path_logfile,
                      std::ofstream::app | std::ofstream::binary);
    log->save_log(out);
    out.close();
  }
  void checkpoint() {
    // Your code here for lab2A
  }

  // restore data from solid binary file
  // You may modify parameters in these functions
  // 从文件中恢复log到log_entries中
  void restore_logdata() {
    // Your code here for lab2A
    std::ifstream in(file_path_logfile, std::ifstream::binary);
    cmd_type cmdTy;
    while (in.read(reinterpret_cast<char*>(&cmdTy), sizeof(cmdTy))) {
      chfs_command* log = nullptr;
      switch (cmdTy) {
        case CMD_BEGIN:
          log = new chfs_command_begin();
          assert(log->cmdTy == CMD_BEGIN);
          break;
        case CMD_COMMIT:
          log = new chfs_command_commit();
          assert(log->cmdTy == CMD_COMMIT);
          break;
        case CMD_CREATE:
          log = new chfs_command_create();
          assert(log->cmdTy == CMD_CREATE);
          break;
        case CMD_PUT:
          log = new chfs_command_put();
          assert(log->cmdTy == CMD_PUT);
          break;
        case CMD_REMOVE:
          log = new chfs_command_remove();
          assert(log->cmdTy == CMD_REMOVE);
          break;
        default:
          assert(false);
          break;
      }
      log->read_log(in);
      // log->print();
      log_entries.push_back(log);
      // std::cout << "[in restore log] log address:" << log << std::endl;
      // printf("log size:%ld\n", log_entries.size());
    }
    in.close();
  }
  void restore_checkpoint() {
    // Your code here for lab2A
  }

 private:
  std::mutex mtx;
  std::string file_dir;
  std::string file_path_checkpoint;
  std::string file_path_logfile;
};

// using chfs_persister = persister<chfs_command*>;

#endif  // persister_h