// the extent server implementation

#include "extent_server.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <sstream>

#include "persister.h"

// get global transaction ID
// 关于为什么写在这里:别的地方都编译错误
class global_txid {
  txid_t txid = 0;

 public:
  txid_t get_next_txid() { return ++txid; }
  void set_txid(txid_t id) { txid = id; }
} txid_manager;

extent_server::extent_server() {
  im = new inode_manager();
  _persister = new chfs_persister("log");  // DO NOT change the dir name here

  // inode1初始化的时候加载 需要预先设定
  inode_map[1] = 1;
  // Your code here for Lab2A: recover data on startup
  _persister->restore_logdata();

  printf("log size:%ld\n", _persister->log_entries.size());

  int num = 0;
  size_t sz = _persister->log_entries.size();
  for (size_t i = 0; i < sz; i++) {
    chfs_command *log = _persister->log_entries[i];
    ++num;
    // if (i % 10 == 0) printf("REDO LOG---%d----\n", num);
    // // assert(_persister->log_entries[i] <= 0xFFFFFFFFFFFF);
    // printf("log address(x): %p\n", log);
    // log->print();
    redo_log(log);
    // printf("REDO     ----------   END\n");
  }
}

void extent_server::redo_log(chfs_command *log) {
  // printf("log address(log): %p\n", log);
  cmd_type cmdTy = log->cmdTy;
  // printf("%d\n", cmdTy);
  switch (cmdTy) {
    case CMD_BEGIN:
    case CMD_COMMIT:
      break;
    case CMD_CREATE: {
      // printf("CREATE-------\n");
      auto p = dynamic_cast<chfs_command_create *>(log);
      assert(p != nullptr);
      extent_protocol::extentid_t inum;
      create(p->type, inum);
      inode_map[p->inum] = inum;
    } break;
    case CMD_PUT: {
      // printf("PUT-------\n");
      auto p = dynamic_cast<chfs_command_put *>(log);
      assert(p != nullptr);
      int r;
      assert(p->str.size() == p->size);
      put(inode_map[p->inum], p->str, r);
    } break;
    case CMD_REMOVE: {
      // printf("REMOVE-------\n");
      auto p = dynamic_cast<chfs_command_remove *>(log);
      assert(p != nullptr);
      int r;
      remove(inode_map[p->inum], r);
    } break;
    default:
      assert(false);
      break;
  }
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id) {
  // alloc a new inode and return inum
  // printf("extent_server: create inode\n");
  // printf("[in create] %u\n", type);
  id = im->alloc_inode(type);

  // Lab2A: add create log into persist
  // append log
  txid_t txid = txid_manager.get_next_txid();
  chfs_command_ptr cmd_ptr = new chfs_command_create(txid, type, id);
  _persister->append_log(cmd_ptr);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &) {
  id &= 0x7fffffff;

  const char *cbuf = buf.c_str();
  int size = buf.size();
  // printf("in [put] before write_file\n");
  im->write_file(id, cbuf, size);
  // printf("in [put] after write_file\n");
  // Lab2A: add create log into persist
  // append log
  txid_t txid = txid_manager.get_next_txid();
  chfs_command *cmd_ptr = new chfs_command_put(txid, id, size, buf);
  _persister->append_log(cmd_ptr);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf) {
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id,
                           extent_protocol::attr &a) {
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;

  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &) {
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;

  im->remove_file(id);

  // Lab2A: add create log into persist
  // append log
  txid_t txid = txid_manager.get_next_txid();
  chfs_command *cmd_ptr = new chfs_command_remove(txid, id);
  _persister->append_log(cmd_ptr);

  return extent_protocol::OK;
}
