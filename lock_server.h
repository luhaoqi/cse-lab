// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <condition_variable>
#include <string>
#include <thread>

#include "lock_client.h"
#include "lock_protocol.h"
#include "rpc.h"

class lock_server {
 protected:
  int nacquire;
  std::map<lock_protocol::lockid_t, bool> lck_manager;
  std::condition_variable cv;
  // 对map的锁，可以实现不同inum的并行，可以扩展成一个map多个锁，增加并行程度
  std::mutex mtx;

  // pthread_mutex_t mutex;
  // static pthread_cond_t cond;

 public:
  lock_server();
  ~lock_server(){};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif