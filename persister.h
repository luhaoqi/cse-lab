#ifndef persister_h
#define persister_h

#include <fcntl.h>

#include <fstream>
#include <iostream>
#include <mutex>
#include <vector>

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
  virtual ~chfs_command() = default;
};

class chfs_command_begin : public chfs_command {
 public:
  chfs_command_begin() : chfs_command(CMD_BEGIN) {}
  chfs_command_begin(txid_t id) : chfs_command(id, CMD_BEGIN) {}
  virtual ~chfs_command_begin() = default;

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
  virtual ~chfs_command_commit() = default;

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
  virtual ~chfs_command_create() = default;

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
  virtual ~chfs_command_put() = default;

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
    std::cout << "txid, inum, size: " << txid << " " << inum << " " << size
              << std::endl;
    char* ch = new char[size + 1];
    in.read(ch, size);
    ch[size] = 0;
    // 非常关键 中间可能有'\0' ，因为offset可能大于size
    str = std::string(ch, size);
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
  virtual ~chfs_command_remove() = default;

  void save_log(std::ofstream& out) {
    out.write(reinterpret_cast<char*>(&cmdTy), sizeof(cmdTy));
    out.write(reinterpret_cast<char*>(&txid), sizeof(txid));
    out.write(reinterpret_cast<char*>(&inum), sizeof(inum));
  }

  void read_log(std::ifstream& in) {
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
  chfs_command_create* checkpoint_create[INODE_NUM + 5];
  chfs_command_put* checkpoint_put[INODE_NUM + 5];

  chfs_persister(const std::string& dir) {
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
    memset(checkpoint_create, 0, sizeof(checkpoint_create));
    memset(checkpoint_put, 0, sizeof(checkpoint_put));
  }
  ~chfs_persister() {
    // Your code here for lab2A
    // 存储checkpoint_entries
  }

  // 把checkpoint中的数据存储到checkpoint.bin
  void save_checkpoint() {
    std::ofstream out(file_path_checkpoint,
                      std::ofstream::trunc | std::ofstream::binary);

    // 保存checkpoint_entries
    chfs_command_create* log_create = nullptr;
    chfs_command_put* log_put = nullptr;
    for (int i = 1; i <= INODE_NUM; i++) {
      if ((log_create = checkpoint_create[i]) != nullptr) {
        // save create log
        log_create->save_log(out);
      }
      assert(i == 1 ||
             !(log_create == nullptr && checkpoint_put[i] != nullptr));
      if ((log_put = checkpoint_put[i]) != nullptr) {
        // put
        log_put->save_log(out);
      }
    }
    out.close();
  }

  // persist data into solid binary file
  // You may modify parameters in these functions
  void append_log(chfs_command* log) {
    // Your code here for lab2A
    // 只加入vector 不修改log.bin commit就checkpoint
    printf("append_log type=%d\n", log->cmdTy);
    log_entries.push_back(log);
    if (log->cmdTy == CMD_COMMIT) checkpoint();

    // std::ofstream out(file_path_logfile,
    //                   std::ofstream::app | std::ofstream::binary);
    // log->save_log(out);
    // out.close();
  }

  // 把checkpoint_entries中的内容与log_entries中的内容合并
  void checkpoint() {
    // Your code here for lab2A
    printf("checkpoint\n");
    int sz = log_entries.size();
    assert(sz >= 2);
    assert(log_entries[0]->cmdTy == CMD_BEGIN);
    assert(log_entries[sz - 1]->cmdTy == CMD_COMMIT);
    uint32_t inum = 0;
    for (int i = 1; i < sz - 1; i++) {
      auto log = log_entries[i];
      switch (log->cmdTy) {
        case CMD_CREATE: {
          auto p = dynamic_cast<chfs_command_create*>(log);
          inum = p->inum;
          printf("CREATE i=%d inum =%u add=%p\n", i, inum,
                 checkpoint_create[inum]);
          assert(checkpoint_create[inum] == nullptr);
          checkpoint_create[inum] = p;
          break;
        }
        case CMD_PUT: {
          auto p = dynamic_cast<chfs_command_put*>(log);
          inum = p->inum;
          printf("CREATE i=%d inum =%u add=%p\n", i, inum,
                 checkpoint_put[inum]);
          assert(inum == 1 || checkpoint_create[inum] != nullptr);
          if (checkpoint_put[inum] != nullptr) {
            delete checkpoint_put[inum];
          }
          checkpoint_put[inum] = p;
          break;
        }
        case CMD_REMOVE: {
          auto p = dynamic_cast<chfs_command_remove*>(log);
          inum = p->inum;
          assert(checkpoint_create[inum] != nullptr);
          delete checkpoint_create[inum];
          if (checkpoint_put[inum] != nullptr) {
            delete checkpoint_put[inum];
          }
          checkpoint_create[inum] = nullptr;
          checkpoint_put[inum] = nullptr;
        } break;
        default:
          // checkpoint.bin中没有其他类型的log
          // 包括BEGIN、COMMIT
          assert(false);
          break;
      }
    }
    log_entries.clear();
    save_checkpoint();
  }

  // restore data from solid binary file
  // You may modify parameters in these functions
  // 从文件中恢复log到log_entries中
  // 因为一个commit就checkpoint 所以没有restore_data的需求
  void restore_logdata() {
    // Your code here for lab2A
  }

  // 开机的时候读取恢复数据
  // save_checkpoint保证没有重复数据
  void restore_checkpoint() {
    // Your code here for lab2A
    std::ifstream in(file_path_checkpoint, std::ifstream::binary);
    cmd_type cmdTy;
    while (in.read(reinterpret_cast<char*>(&cmdTy), sizeof(cmdTy))) {
      switch (cmdTy) {
        case CMD_CREATE: {
          auto log = new chfs_command_create();
          assert(log->cmdTy == CMD_CREATE);
          log->read_log(in);
          assert(log->inum > 1 && log->inum <= INODE_NUM);
          assert(checkpoint_create[log->inum] == nullptr);
          checkpoint_create[log->inum] = log;
          break;
        }
        case CMD_PUT: {
          auto log = new chfs_command_put();
          assert(log->cmdTy == CMD_PUT);
          log->read_log(in);
          assert(log->inum >= 1 && log->inum <= INODE_NUM);
          assert(log->inum == 1 || checkpoint_create[log->inum] != nullptr);
          checkpoint_put[log->inum] = log;
          break;
        }
        default:
          // checkpoint.bin中没有其他类型的log
          // 包括BEGIN、COMMIT、REMOVE
          assert(false);
          break;
      }
      // log->print();
    }
    in.close();
  }

 private:
  std::mutex mtx;
  std::string file_dir;
  std::string file_path_checkpoint;
  std::string file_path_logfile;
};

// using chfs_persister = persister<chfs_command*>;

#endif  // persister_h