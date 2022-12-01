#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <chrono>

#include "./rpc/rpc.h"
#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(),

    "state_machine must inherit from raft_state_machine");

    static_assert(std::is_base_of<raft_command, command>(),

    "command must inherit from raft_command");

    friend class thread_pool;

#define TEST
#ifdef TEST
#define RAFT_LOG(fmt, args...) \
    do {                       \
    } while (0);

#else
#define RAFT_LOG(fmt, args...)                                                                                   \
     do {                                                                                                         \
         auto now =                                                                                               \
             std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                 std::chrono::system_clock::now().time_since_epoch())                                             \
                 .count();                                                                                        \
         printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
     } while (0);
#endif
public:
    raft(
            rpcs *rpc_server,
            std::vector<rpcc *> rpc_clients,
            int idx,
            raft_storage<command> *storage,
            state_machine *state);

    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

    /* ----Persistent state on all server----  */
    int voteFor;    //candidateId that received vote in currentTerm (or null if none, here -1 for null)
    std::vector<log_entry<command>> log;    //log entries; each entry contains command for state machine, and term when entry was received by leader
    std::vector<bool> persistVote;          //persist all votes from other clients

    /* ---- Volatile state on all server----  */
    int commitIndex;    //index of highest log entry known to be committed (initialized to 0, increases monotonically)
    int lastApplied;    //index of highest log entry applied to state machine
    long long election_timer;  // use to check timeout in election

    /* ---- Volatile state on leader----  */
    std::vector<int> nextIndex; // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    std::vector<int> matchIndex; // for each server, index of highest log entry known to be replicated on server


private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);

    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);

    void
    handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);

    void
    handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();

    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();

    void run_background_election();

    void run_background_commit();

    void run_background_apply();

    // Your code here:
    // 获取随机数
    static int get_random(int low, int high) {
        return rand() % (high - low + 1) + low;
    }

    // 返回当前时间，以毫秒为单位
    static long long get_current_time() {
        using namespace std::chrono;
        system_clock::time_point now = system_clock::now(); // 获取当前时间点
        nanoseconds d = now.time_since_epoch();
        milliseconds milliSeconds = duration_cast<milliseconds>(d);
        return milliSeconds.count();
    }

    void convert_follower();

    void convert_candidate();

    void convert_leader();
};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage,
                                   state_machine *state) :
        storage(storage),
        state(state),
        rpc_server(server),
        rpc_clients(clients),
        my_id(idx),
        stopped(false),
        role(follower),
        current_term(0),
        background_election(nullptr),
        background_ping(nullptr),
        background_commit(nullptr),
        background_apply(nullptr),
        voteFor(-1),
        log(std::vector<log_entry<command>>()),
        commitIndex(0),
        lastApplied(0) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    election_timer = get_current_time();
    /* Notice that the first log index is 1 instead of 0. To simplify the programming, you can append an empty log entry to the logs at the very beginning. And since the 'lastApplied' index starts from 0, the first empty log entry will never be applied to the state machine.*/
    log.push_back(log_entry<command>());
    log[0].term = log[0].id = 0;

}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);
    if (is_leader(current_term)) {
        log_entry<command> current_log = log_entry<command>(cmd, current_term, (int) log.size());
        log.push_back(current_log);
        term = current_term;
        index = log.size() - 1;
        RAFT_LOG("new_command, my_id: %d, term: %d, index: %d", my_id, term, index);
        return true;
    }
    return false;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);

    if (args.term < current_term) {
        reply.term = current_term;
        reply.voteGranted = false;
    } else {
        if (current_term < args.term) {
            current_term = args.term;
            if (role != follower) convert_follower();
        }
        if ((voteFor == -1 || voteFor == args.candidateId)
            && (log.back().term < args.term ||
                (log.back().term == args.term && (int) (log.size() - 1) <= args.lastLogIndex))) {
            // if not vote or already vote for it or candidate's log is at least as complex as receiver's log, vote it
            reply.term = current_term;
            reply.voteGranted = true;
            voteFor = args.candidateId;
        } else {
            reply.term = current_term;
            reply.voteGranted = false;
        }
    }

    return raft_rpc_status::OK;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg,
                                                             const request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);
    RAFT_LOG("RPC request_vote : candidate: %d, voter: %d, voteGranted: %d", arg.candidateId, target,
             reply.voteGranted);
    election_timer = get_current_time();

    if (reply.term > current_term) {
        current_term = reply.term;
        convert_follower();
    } else if (reply.voteGranted) {
        persistVote[target] = true;
    }
    return;
}

template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);
    if (arg.term < current_term) {
        reply.success = false;
        reply.term = current_term;
        return raft_rpc_status::OK;
    }
    election_timer = get_current_time();
    if (arg.entries.size() == 0) {
        //heartbeat;
        RAFT_LOG("node %d get heartbeat from %d", my_id, arg.leaderId);
        convert_follower();
        current_term = arg.term;
        reply.term = current_term;
        reply.success = true;
    }
    return raft_rpc_status::OK;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg,
                                                               const append_entries_reply &reply) {
    // Lab3: Your code here
    return;
}

template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg,
                                                                 const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    RAFT_LOG("my_id: %d, term: %d, role: %d", my_id, current_term, role);
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        // sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        // get current time
        long long current_time = get_current_time();
        if (role == follower) {
            // rules for follower
            int timeout_election = get_random(150, 300);
            if (current_time - election_timer > timeout_election) {
                convert_candidate();
            }
        } else if (role == candidate) {
            int sum = 0;
            for (auto x: persistVote) sum += x;
            if (sum > num_nodes() / 2) {
                convert_leader();
            } else {
                // election timeout elapses: start new election
                if (current_time - election_timer > 1000) {
                    convert_candidate();
                }
            }

        } else if (role == leader) {
//            break;
        }
    }
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /*
        while (true) {
            if (is_stopped()) return;
            // Lab3: Your code here
        }    
        */

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /*
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
    }    
    */
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        if (role == leader) {
            for (int i = 0; i < num_nodes(); i++)
                if (i != my_id) {
                    append_entries_args<command> args(current_term, my_id); // heartbeat;

                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::convert_follower() {
    RAFT_LOG("convert_follower my_id: %d, term: %d, role: %d", my_id, current_term, role);
    role = follower;
    voteFor = -1;
    election_timer = get_current_time();
}

template<typename state_machine, typename command>
void raft<state_machine, command>::convert_candidate() {
    RAFT_LOG("convert_candidate my_id: %d, term: %d, role: %d", my_id, current_term, role);
    role = candidate;
    // start election
    current_term++;
    voteFor = my_id;
    election_timer = get_current_time();
    // assign all persistVote to false
    persistVote.assign(num_nodes(), false);
    persistVote[my_id] = true;

    request_vote_args args(current_term, my_id, log.back().term, log.size() - 1);
    // ask for voting
    for (int i = 0; i < num_nodes(); i++)
        if (i != my_id)
            thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
}

template<typename state_machine, typename command>
void raft<state_machine, command>::convert_leader() {
    RAFT_LOG("convert_leader my_id: %d, term: %d, role: %d", my_id, current_term, role);
    role = leader;
//  nextIndex.assign(num_nodes(), log.size());
//  matchIndex.assign(num_nodes(), 0);
}


#endif // raft_h