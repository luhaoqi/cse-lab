#ifndef chfs_client_h
#define chfs_client_h

#include <string>

#include "lock_client.h"
#include "lock_protocol.h"
// #include "chfs_protocol.h"
#include <vector>

#include "extent_client.h"

class chfs_client {
  extent_client *ec;
  lock_client *lc;

 public:
  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    chfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

 public:
  chfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);
  bool issymbolic(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum, const char *);
  int mkdir(inum, const char *, mode_t, inum &);
  int symlink(inum parent, const char *name, const char *link, inum &ino_out);
  int readlink(inum ino, std::string &data);

  // txid_t begin_transaction();
  // void commit_transaction(txid_t txid);

  /** you may need to add symbolic link related methods here.*/
};

// 仿照std::lock_guard 在生命周期结束的时候自动释放锁
class LockGuard {
 public:
  LockGuard(lock_client *lc, lock_protocol::lockid_t lid) : lc(lc), lid(lid) {
    lc->acquire(lid);
  }
  ~LockGuard() { lc->release(lid); }

 private:
  lock_client *lc;
  lock_protocol::lockid_t lid;
};

#endif
