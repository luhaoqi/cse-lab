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

  // Your code here for Lab2A: recover data on startup
  _persister->restore_logdata();
  for (const auto &x : _persister->log_entries) redo_log(x);
}

void extent_server::redo_log(const chfs_command &cmd) {
  std::cout << "redo log: type=" << cmd.type << std::endl;
  extent_protocol::extentid_t inum = cmd.inum;
  int x;
  switch (cmd.type) {
    case chfs_command::CMD_CREATE:
      create(cmd.create_type, inum);
      break;
    case chfs_command::CMD_PUT:
      assert(cmd.s_new.size() != cmd.size_new);
      put(inum, cmd.s_new, x);
      break;
    case chfs_command::CMD_REMOVE:
      remove(inum, x);
      break;
    default:
      break;
  }
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id) {
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);
  // add log
  chfs_command cmd(chfs_command::CMD_CREATE, txid_manager.get_next_txid());
  cmd.create_type = type;
  cmd.inum = id;
  _persister->append_log(cmd);

  // Lab2A: add create log into persist
  // _persister->append_log(chfs_command::CMD_CREATE);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &) {
  id &= 0x7fffffff;

  // record old value
  chfs_command cmd(chfs_command::CMD_PUT, txid_manager.get_next_txid());
  cmd.inum = id;
  get(id, cmd.s_old);
  cmd.size_old = cmd.s_old.size();

  const char *cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);

  // add log
  cmd.s_new = buf;
  cmd.size_new = size;
  _persister->append_log(cmd);

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

  // record old value
  chfs_command cmd(chfs_command::CMD_PUT, txid_manager.get_next_txid());
  cmd.inum = id;
  get(id, cmd.s_old);
  cmd.size_old = cmd.s_old.size();

  im->remove_file(id);

  // add log
  _persister->append_log(cmd);

  return extent_protocol::OK;
}
