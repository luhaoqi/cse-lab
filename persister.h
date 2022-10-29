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

 private:
  std::mutex mtx;
  std::string file_dir;
  std::string file_path_checkpoint;
  std::string file_path_logfile;

  // restored log data
  std::vector<command> log_entries;
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
  std::cout << "append log" << std::endl;
  std::ofstream out(file_path_logfile, std::ofstream::app);
  out.write(reinterpret_cast<const char*>(&log.type), sizeof(log.type));
  out.write(reinterpret_cast<const char*>(&log.txid), sizeof(log.txid));
  switch (log.type) {
    case chfs_command::CMD_BEGIN:
    case chfs_command::CMD_COMMIT:
      break;
    case chfs_command::CMD_CREATE:
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
void persister<command>::restore_logdata(){
    // Your code here for lab2A

};

template <typename command>
void persister<command>::restore_checkpoint(){
    // Your code here for lab2A

};

using chfs_persister = persister<chfs_command>;

#endif  // persister_h