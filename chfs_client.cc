// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <sstream>

#include "extent_client.h"

chfs_client::chfs_client(std::string extent_dst) {
  ec = new extent_client(extent_dst);
  if (ec->put(1, "") != extent_protocol::OK)
    printf("error init root dir\n");  // XYB: init root dir
}

chfs_client::inum chfs_client::n2i(std::string n) {
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string chfs_client::filename(inum inum) {
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool chfs_client::isfile(inum inum) {
  extent_protocol::attr a;

  if (ec->getattr(inum, a) != extent_protocol::OK) {
    printf("error getting attr\n");
    return false;
  }

  if (a.type == extent_protocol::T_FILE) {
    printf("isfile: %lld is a file\n", inum);
    return true;
  }
  printf("isfile: %lld is not a file\n", inum);
  return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool chfs_client::isdir(inum inum) {
  extent_protocol::attr a;

  if (ec->getattr(inum, a) != extent_protocol::OK) {
    printf("error getting attr\n");
    return false;
  }

  if (a.type == extent_protocol::T_DIR) {
    printf("isdir: %lld is a dir\n", inum);
    return true;
  }
  printf("isdir: %lld is not a dir\n", inum);
  return false;
}

bool chfs_client::issymbolic(inum inum) {
  extent_protocol::attr a;

  if (ec->getattr(inum, a) != extent_protocol::OK) {
    printf("error getting attr\n");
    return false;
  }

  if (a.type == extent_protocol::T_SYMBOLIC_LINK) {
    printf("issymbolic: %lld is a symbolic link\n", inum);
    return true;
  }
  printf("issymbolic: %lld is not a symbolic link\n", inum);
  return false;
}

int chfs_client::getfile(inum inum, fileinfo &fin) {
  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
  return r;
}

int chfs_client::getdir(inum inum, dirinfo &din) {
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

release:
  return r;
}

#define EXT_RPC(xx)                                          \
  do {                                                       \
    if ((xx) != extent_protocol::OK) {                       \
      printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
      r = IOERR;                                             \
      goto release;                                          \
    }                                                        \
  } while (0)

// Only support set size of attr
int chfs_client::setattr(inum ino, size_t size) {
  int r = OK;

  /*
   * your code goes here.
   * note: get the content of inode ino, and modify its content
   * according to the size (<, =, or >) content length.
   */

  std::string buf;
  if ((r = ec->get(ino, buf)) != OK) return r;
  if (buf.length() == size) return r;
  buf.resize(size);
  if ((r = ec->put(ino, buf)) != OK) return r;
  return r;
}

int chfs_client::create(inum parent, const char *name, mode_t mode,
                        inum &ino_out) {
  int r = OK;

  /*
   * your code goes here.
   * note: lookup is what you need to check if file exist;
   * after create file or dir, you must remember to modify the parent
   * infomation.
   */
  bool found;
  if ((r = lookup(parent, name, found, ino_out)) != OK) return r;
  if (found) return EXIST;

  // create new inode
  if ((r = ec->create(extent_protocol::T_FILE, ino_out)) != OK) return r;

  // add an entry into parent
  std::string buf;
  if ((r = ec->get(parent, buf)) != OK) return r;
  buf.append(std::string(name) + "/" + filename(ino_out) + "/");
  if ((r = ec->put(parent, buf)) != OK) return r;

  return r;
}

int chfs_client::mkdir(inum parent, const char *name, mode_t mode,
                       inum &ino_out) {
  int r = OK;

  /*
   * your code goes here.
   * note: lookup is what you need to check if directory exist;
   * after create file or dir, you must remember to modify the parent
   * infomation.
   */

  bool found;
  if ((r = lookup(parent, name, found, ino_out)) != OK) return r;
  if (found) return EXIST;

  // create new inode
  if ((r = ec->create(extent_protocol::T_DIR, ino_out)) != OK) return r;

  // add an entry into parent
  std::string buf;
  if ((r = ec->get(parent, buf)) != OK) return r;
  buf.append(std::string(name) + "/" + filename(ino_out) + "/");
  if ((r = ec->put(parent, buf)) != OK) return r;

  return r;
}

int chfs_client::lookup(inum parent, const char *name, bool &found,
                        inum &ino_out) {
  int r = OK;

  std::list<dirent> list;
  if ((r = readdir(parent, list)) != OK) {
    return r;
  }
  for (auto x : list) {
    if (x.name == name) {
      found = true;
      ino_out = x.inum;
      return r;
    }
  }
  found = false;

  return r;
}

int chfs_client::readdir(inum dir, std::list<dirent> &list) {
  // my directory format: name/inum/name/inum/name/inum...
  int r = OK;
  // printf("readdir in dir %016llx\n", dir);
  std::string buf;
  extent_protocol::attr a;
  if (ec->getattr(dir, a) != OK) {
    r = IOERR;
    return r;
  }

  if (a.type != extent_protocol::T_DIR) {
    r = NOENT;
    return r;
  }

  if (ec->get(dir, buf) != OK) {
    r = IOERR;
    return r;
  }

  size_t name_start = 0;
  size_t name_end = buf.find('/');
  while (name_end != std::string::npos) {
    std::string name = buf.substr(name_start, name_end - name_start);
    size_t inum_start = name_end + 1;
    size_t inum_end = buf.find('/', inum_start);
    std::string inum = buf.substr(inum_start, inum_end - inum_start);

    struct dirent entry;
    entry.name = name;
    entry.inum = n2i(inum);

    list.push_back(entry);

    name_start = inum_end + 1;
    name_end = buf.find('/', name_start);
  }

  return r;
}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data) {
  int r = OK;

  /*
   * your code goes here.
   * note: read using ec->get().
   */

  std::string buf;
  if ((r = ec->get(ino, buf)) != OK) return r;

  if ((size_t)off > buf.length())
    data = "";
  else
    data = buf.substr(off, size);

  return r;
}

int chfs_client::write(inum ino, size_t size, off_t off, const char *data,
                       size_t &bytes_written) {
  int r = OK;

  /*
   * your code goes here.
   * note: write using ec->put().
   * when off > length of original file, fill the holes with '\0'.
   */

  std::string buf, str(data, size);
  if ((r = ec->get(ino, buf)) != OK) return r;

  if ((size_t)off > buf.length()) {
    // size_t len = buf.length();
    buf.resize(off + size);
    buf.replace(off, size, str);
    bytes_written = size;  // Why size ? buf.length()-len 不能通过测试
  } else {
    buf.replace(off, size, str);
    bytes_written = size;
  }

  if ((r = ec->put(ino, buf)) != OK) return r;

  return r;
}

int chfs_client::unlink(inum parent, const char *name) {
  int r = OK;

  /*
   * your code goes here.
   * note: you should remove the file using ec->remove,
   * and update the parent directory content.
   */

  bool found = false;
  inum inum;
  if ((r = lookup(parent, name, found, inum)) != OK || !found) {
    printf("unlink empty file!");
    return r;
  }

  if ((r = ec->remove(inum)) != OK) {
    return r;
  }

  std::string buf;
  if ((r = ec->get(parent, buf)) != OK) return r;
  size_t erase_start = buf.find(name);
  size_t erase_after = buf.find('/', buf.find('/', erase_start) + 1);
  buf.erase(erase_start, erase_after - erase_start + 1);

  if ((r = ec->put(parent, buf)) != OK) return r;

  return r;
}

int chfs_client::symlink(inum parent, const char *name, const char *link,
                         inum &ino_out) {
  int r = OK;

  // check if existed
  bool found = false;
  chfs_client::inum t;
  if ((r = lookup(parent, name, found, t)) != OK) return r;
  if (found) return EXIST;

  // pick an inum and init the symlink
  if ((r = ec->create(extent_protocol::T_SYMBOLIC_LINK, ino_out) != OK))
    return r;
  if ((r = ec->put(ino_out, std::string(link))) != OK) return r;

  // add an entry into parent
  std::string buf;
  if ((r = ec->get(parent, buf)) != OK) return r;
  buf.append(std::string(name) + "/" + filename(ino_out) + "/");
  if ((r = ec->put(parent, buf)) != OK) return r;
  return r;
}

int chfs_client::readlink(inum ino, std::string &data) {
  int r = OK;

  std::string buf;
  if ((r = ec->get(ino, buf)) != OK) return r;

  data = buf;

  return r;
}
