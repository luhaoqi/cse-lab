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

extent_server::extent_server() {
  im = new inode_manager();
  _persister = new chfs_persister("log");  // DO NOT change the dir name here

  // Your code here for Lab2A: recover data on startup
  _persister->restore_checkpoint();
  // 从checkpoint_entries 复原state
  printf("redo begin\n");
  chfs_command_create *log_create = nullptr;
  chfs_command_put *log_put = nullptr;
  for (int i = 1; i <= INODE_NUM; i++) {
    if ((log_create = _persister->checkpoint_create[i]) != nullptr) {
      // create
      extent_protocol::extentid_t inum = 0;
      assert(log_create->inum != (uint32_t)1);
      assert(log_create->inum == (uint32_t)i);
      create_pos(log_create->type, log_create->inum, inum);
      assert((uint32_t)inum == log_create->inum);
    }
    if ((log_put = _persister->checkpoint_put[i]) != nullptr) {
      // put
      printf("redo put inum=%d\n", log_put->inum);
      int r = 0;
      put(log_put->inum, log_put->str, r);
    }
  }
  _persister->log_entries.clear();
  printf("redo end\n");
}

// 普通的 create
int extent_server::create(uint32_t type, extent_protocol::extentid_t &id) {
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  // printf("[in create] %u\n", type);
  id = im->alloc_inode(type);
  printf("in [create] alloc id=%llu pos=%u\n", id, 0);

  // Lab2A: add create log into persist
  // append log
  txid_t txid = txid_manager.get_txid();
  chfs_command_ptr cmd_ptr = new chfs_command_create(txid, type, id);
  append_log(cmd_ptr);

  return extent_protocol::OK;
}

// 带pos的版本 用于恢复
int extent_server::create_pos(uint32_t type, uint32_t pos,
                              extent_protocol::extentid_t &id) {
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  // printf("[in create] %u\n", type);
  id = im->alloc_inode(type, pos);
  printf("in [create] alloc id=%llu pos=%u\n", id, pos);

  // Lab2A: add create log into persist
  // append log
  txid_t txid = txid_manager.get_txid();
  chfs_command_ptr cmd_ptr = new chfs_command_create(txid, type, id);
  append_log(cmd_ptr);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &) {
  printf("extent_server: put %lld size=%lu\n", id, buf.size());
  id &= 0x7fffffff;

  const char *cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  // Lab2A: add create log into persist
  // append log
  txid_t txid = txid_manager.get_txid();
  chfs_command *cmd_ptr = new chfs_command_put(txid, id, size, buf);
  append_log(cmd_ptr);

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
  txid_t txid = txid_manager.get_txid();
  chfs_command *cmd_ptr = new chfs_command_remove(txid, id);
  append_log(cmd_ptr);

  return extent_protocol::OK;
}

int extent_server::begin(int, int &) {
  txid_t txid = txid_manager.get_next_txid();
  chfs_command *cmd_ptr = new chfs_command_begin(txid);
  append_log(cmd_ptr);

  return extent_protocol::OK;
}

int extent_server::commit(int, int &) {
  txid_t txid = txid_manager.get_txid();
  chfs_command *cmd_ptr = new chfs_command_commit(txid);
  append_log(cmd_ptr);

  return extent_protocol::OK;
}
