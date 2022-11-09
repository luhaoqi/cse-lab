// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>

#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  rpcc *cl;

 public:
  extent_client(std::string dst);

  extent_protocol::status create(uint32_t type,
                                 extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid,
                              std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid,
                                  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);

  extent_protocol::status begin();
  extent_protocol::status commit();
  // txid_t get_next_txid() { return es->txid_manager.get_next_txid(); }
  // void append_log(chfs_command_ptr cmd) { es->append_log(cmd); }
};

#endif
