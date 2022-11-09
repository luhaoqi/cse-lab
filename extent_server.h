// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <map>
#include <string>

#include "extent_protocol.h"
#include "inode_manager.h"
#include "persister.h"

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  chfs_persister *_persister;
  // std::map<uint32_t, uint32_t> inode_map;

 public:
  extent_server();

  int create(uint32_t type, extent_protocol::extentid_t &id);
  int create_pos(uint32_t type, uint32_t pos, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);

  // lab2a add begin and commit
  // 至少一个参数+一个引用
  int begin(int, int &);
  int commit(int, int &);

  // get global transaction ID
  // 关于为什么写在这里:别的地方都编译错误
  class global_txid {
    txid_t txid = 0;

   public:
    txid_t get_next_txid() { return ++txid; }
    txid_t get_txid() { return txid; }
    void set_txid(txid_t id) { txid = id; }
  } txid_manager;

  // Your code here for lab2A: add logging APIs
  void append_log(chfs_command_ptr cmd) {
    // in lab2B not save log
    //_persister->append_log(cmd);
  }
};

#endif
