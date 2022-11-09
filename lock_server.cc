// the lock server implementation

#include "lock_server.h"

#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>

#include <sstream>

lock_server::lock_server() : nacquire(0) {}

lock_protocol::status lock_server::stat(int clt, lock_protocol::lockid_t lid,
                                        int &r) {
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status lock_server::acquire(int clt, lock_protocol::lockid_t lid,
                                           int &r) {
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2B part2 code goes here
  {
    std::unique_lock<std::mutex> lck(mtx);
    // 该锁还没有使用过 返回该锁 true表示上锁
    if (lck_manager.find(lid) == lck_manager.end()) {
      lck_manager[lid] = true;
    } else {
      // while循环 防止虚假唤醒
      while (lck_manager[lid]) cv.wait(lck);
      lck_manager[lid] = true;
    }
  }
  return ret;
}

lock_protocol::status lock_server::release(int clt, lock_protocol::lockid_t lid,
                                           int &r) {
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2B part2 code goes here
  {
    std::unique_lock<std::mutex> lck(mtx);
    if (lck_manager.find(lid) == lck_manager.end()) {
      ret = lock_protocol::NOENT;
    } else {
      lck_manager[lid] = false;
      cv.notify_one();
    }
  }
  return ret;
}