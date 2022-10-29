#ifndef persister_h
#define persister_h

#include <fcntl.h>

#include <fstream>
#include <iostream>
#include <mutex>

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

class chfs_command {
 public:
  enum cmd_type {
    CMD_BEGIN = 0,
    CMD_COMMIT,
    CMD_CREATE,
    CMD_PUT,
    CMD_GET,
    CMD_GETATTR,
    CMD_REMOVE,

  };

  cmd_type type = CMD_BEGIN;
  txid_t txid = 0;
  // create need
  uint32_t create_type, inum;
  // put need (remove reuse old var)
  uint32_t size_old, size_new;
  std::string s_old, s_new;

  // constructor
  chfs_command(cmd_type ty, txid_t id) : type(ty), txid(id) {}

  // uint64_t size() const {
  //   uint64_t s = sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint32_t) * 4 +
  //                sizeof(s_old) + sizeof(s_new);
  //   return s;
  // }
};
/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to
 * persist and recover data.
 *
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template <typename command>
class persister {
 public:
  persister(const std::string& file_dir);
  ~persister();

  // persist data into solid binary file
  // You may modify parameters in these functions
  void append_log(const command& log);
  void checkpoint();

  // restore data from solid binary file
  // You may modify parameters in these functions
  void restore_logdata();
  void restore_checkpoint();

  // restored log data
  std::vector<command> log_entries;

 private:
  std::mutex mtx;
  std::string file_dir;
  std::string file_path_checkpoint;
  std::string file_path_logfile;
};

template <typename command>
persister<command>::persister(const std::string& dir) {
  // DO NOT change the file names here
  file_dir = dir;
  file_path_checkpoint = file_dir + "/checkpoint.bin";
  file_path_logfile = file_dir + "/logdata.bin";
}

template <typename command>
persister<command>::~persister() {
  // Your code here for lab2A
}

template <typename command>
void persister<command>::append_log(const command& log) {
  // Your code here for lab2A
  log_entries.push_back(log);
  std::ofstream out(file_path_logfile,
                    std::ofstream::app | std::ofstream::binary);
  out.write(reinterpret_cast<const char*>(&log.type), sizeof(log.type));
  out.write(reinterpret_cast<const char*>(&log.txid), sizeof(log.txid));
  switch (log.type) {
    case chfs_command::CMD_CREATE:
      out.write(reinterpret_cast<const char*>(&log.inum), sizeof(log.inum));
      out.write(reinterpret_cast<const char*>(&log.create_type),
                sizeof(log.create_type));
      break;
    case chfs_command::CMD_PUT:
      out.write(reinterpret_cast<const char*>(&log.inum), sizeof(log.inum));
      out.write(reinterpret_cast<const char*>(&log.size_old),
                sizeof(log.size_old));
      out.write(reinterpret_cast<const char*>(&log.size_new),
                sizeof(log.size_new));
      out.write(reinterpret_cast<const char*>(log.s_old.c_str()), log.size_old);
      out.write(reinterpret_cast<const char*>(log.s_new.c_str()), log.size_new);
      break;
    case chfs_command::CMD_REMOVE:
      out.write(reinterpret_cast<const char*>(&log.inum), sizeof(log.inum));
      out.write(reinterpret_cast<const char*>(&log.size_old),
                sizeof(log.size_old));
      out.write(reinterpret_cast<const char*>(log.s_old.c_str()), log.size_old);
      break;
    default:
      break;
  }
}

template <typename command>
void persister<command>::checkpoint() {
  // Your code here for lab2A
}

template <typename command>
void persister<command>::restore_logdata() {
  // Your code here for lab2A
  std::ifstream in(file_path_logfile, std::ios::binary);
  if (in.is_open()) {
    chfs_command::cmd_type type;
    in.read(reinterpret_cast<char*>(&type), sizeof(type));
    txid_t txid;
    in.read(reinterpret_cast<char*>(&txid), sizeof(txid));
    std::cout << "restore logdada: type=" << type << " txid=" << txid
              << std::endl;

    chfs_command cmd(type, txid);

    uint32_t create_type, inum;
    uint32_t size_old, size_new;
    char *s_old, *s_new;
    switch (type) {
      case chfs_command::CMD_CREATE:
        in.read(reinterpret_cast<char*>(&inum), sizeof(inum));
        cmd.inum = inum;
        in.read(reinterpret_cast<char*>(&create_type), sizeof(create_type));
        cmd.create_type = create_type;
        break;
      case chfs_command::CMD_PUT:
        in.read(reinterpret_cast<char*>(&inum), sizeof(inum));
        cmd.inum = inum;
        in.read(reinterpret_cast<char*>(&size_old), sizeof(size_old));
        in.read(reinterpret_cast<char*>(&size_new), sizeof(size_new));
        cmd.size_old = size_old;
        cmd.size_new = size_new;
        s_old = new char[size_old + 1];
        s_new = new char[size_new + 1];
        in.read(reinterpret_cast<char*>(&s_old), size_old);
        in.read(reinterpret_cast<char*>(&s_new), size_new);
        s_old[size_old] = s_new[size_new] = 0;
        cmd.s_old = s_old;
        cmd.s_new = s_new;
        delete s_old;
        delete s_new;
        break;
      case chfs_command::CMD_REMOVE:
        in.read(reinterpret_cast<char*>(&inum), sizeof(inum));
        cmd.inum = inum;
        in.read(reinterpret_cast<char*>(&size_old), sizeof(size_old));
        cmd.size_old = size_old;
        s_old = new char[size_old + 1];
        in.read(reinterpret_cast<char*>(&s_old), size_old);
        s_old[size_old] = 0;
        cmd.s_old = s_old;
        delete s_old;
        break;
      default:
        break;
    }
    log_entries.push_back(cmd);
  }
};

template <typename command>
void persister<command>::restore_checkpoint(){
    // Your code here for lab2A

};

using chfs_persister = persister<chfs_command>;

#endif  // persister_h