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
#include <set>

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

//#define DEBUG
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
         printf("[%lld][%s:%d][node %d term %d] " fmt "\n", now - 1669969483153LL, __FILE__, __LINE__, my_id, current_term, ##args); \
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
    // Please make sure all the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all the background threads are joined in this method.
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
    std::set<int> persistVote;          //persist all votes from other clients

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
    assert(log.size() == 1);

    // restore
    storage->restore_metadata(current_term, voteFor);
    storage->restore_log(log);
    RAFT_LOG("node %d restore: term:%d, voteFor:%d, losSize:%d", my_id, current_term, voteFor, (int) log.size())
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
    RAFT_LOG("stop")
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

    RAFT_LOG("start")
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
//        nextIndex[my_id] = log.size();
        matchIndex[my_id] = log.size() - 1;

        //store to storage
        storage->store_log(current_log);

        term = current_term;
        index = log.size() - 1;
        RAFT_LOG("new_command, my_id: %d, term: %d, index: %d", my_id, term, index)
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
    reply.term = current_term;
    if (args.term < current_term) {
        reply.voteGranted = false;
    } else {
        bool change = false;
        if (current_term < args.term) {
            current_term = args.term;
            convert_follower();
            change = true;
        }
        if ((voteFor == -1 || voteFor == args.candidateId)
            && (log.back().term < args.lastLogTerm ||
                (log.back().term == args.lastLogTerm && (int) (log.size() - 1) <= args.lastLogIndex))) {
            // if not vote or already vote for it or candidate's log is at least as complex as receiver's log, vote it
            reply.voteGranted = true;
            voteFor = args.candidateId;
            change = true;
        } else {
            reply.voteGranted = false;
        }
        //store when change
        if (change) storage->store_metadata(current_term, voteFor);
    }
    RAFT_LOG("[node,term,index]: [%d,%d,%d] get vote RPC from [%d,%d,%d], [granted]:%d ", my_id, log.back().term,
             (int) log.size() - 1, args.candidateId, args.lastLogTerm, args.lastLogIndex, reply.voteGranted)
    return raft_rpc_status::OK;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg,
                                                             const request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);
    if (role != candidate || current_term > arg.term) return;
    RAFT_LOG("RPC request_vote : candidate: %d, voter: %d, voteGranted: %d", arg.candidateId, target,
             reply.voteGranted)
//    election_timer = get_current_time();

    if (reply.term > current_term) {
        current_term = reply.term;
        convert_follower();
    } else if (reply.voteGranted) {
        persistVote.insert(target);
        if ((int) persistVote.size() > num_nodes() / 2) {
            convert_leader();
        }
    }
    return;
}

template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);
    reply.term = current_term;
    //  Reply false if term < currentTerm
    if (arg.term < current_term) {
        reply.success = false;
        return raft_rpc_status::OK;
    }
    if (arg.term > current_term) {
        current_term = arg.term;
        storage->store_metadata(current_term, voteFor);
    }
    election_timer = get_current_time();
    if (arg.entries.size() == 0) {
        //heartbeat;
        convert_follower();
        reply.success = true;
        if (arg.leaderCommit > commitIndex) {
            commitIndex = std::min(arg.leaderCommit, (int) log.size() - 1);
        }
        RAFT_LOG("node %d get heartbeat from %d my_commitIndex: %d", my_id, arg.leaderId, commitIndex)
    } else {
        // true append entries RPC

        //Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
        if ((int) log.size() < arg.prevLogIndex || log[arg.prevLogIndex].term != arg.prevLogTerm) {
            reply.success = false;
            return raft_rpc_status::OK;
        }
        //  If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
        size_t i;
        for (i = arg.prevLogIndex + 1; i < arg.entries.size() && i < log.size(); i++) {
            if (arg.entries[i].term != log[i].term) break;
        }
        // pop logs until index is i (i also pop)
        while (log.size() > i)
            log.pop_back();
        // Append any new entries not already in the log
        while (i < arg.entries.size()) {
            log.push_back(arg.entries[i]);
            // store when change
            storage->store_log(arg.entries[i]);
            i++;
        }
        assert(log.size() == arg.entries.size());
        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (arg.leaderCommit > commitIndex) {
            commitIndex = std::min(arg.leaderCommit, (int) log.size() - 1);
        }
        reply.success = true;
    }
    return raft_rpc_status::OK;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg,
                                                               const append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock <std::mutex> lock(mtx);

    election_timer = get_current_time();

    if (reply.term > current_term) {
        // convert to follower
        current_term = reply.term;
        convert_follower();
    } else if (reply.success) {
        // append entries successfully

        // update matchIndex[target]
        matchIndex[node] = std::max(matchIndex[node], (int) arg.entries.size() - 1);
        nextIndex[node] = matchIndex[node] + 1;
        // update commitIndex, which means we should commit until this log
        std::vector<int> tmp = matchIndex;
        sort(tmp.begin(), tmp.end());
        // 升序 (size + 1) / 2  (4:2, 5:3) 这里是第几个，id需要-1
        int new_commitIndex = tmp[(tmp.size() + 1) / 2 - 1];
        for (int i = new_commitIndex; i >= 0; i--)
            if (log[i].term == current_term) {
                commitIndex = std::max(commitIndex, i);
                break;
            } else if (log[i].term < current_term) break;

        if (!arg.entries.empty())
            RAFT_LOG("handle_append_entries_reply(success) my_id: %d, id: %d, log_id:%d, commitIndex: %d",
                     my_id, node, arg.prevLogIndex + 1, commitIndex)
    } else {
        // append entries fails
        // we simply transfer all the logs here
        nextIndex[node] = 1;
        RAFT_LOG("handle_append_entries_reply(fail) my_id: %d, id: %d, commitIndex: %d", my_id, node, commitIndex)
    }
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
    // periodically check the liveness of the leader.

    // Work for followers and candidates.

    RAFT_LOG("my_id: %d, term: %d, role: %d", my_id, current_term, role)
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        // sleep to avoid busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        // get current time
        long long current_time = get_current_time();
        if (role == follower) {
            // rules for follower
            int timeout_election = get_random(300, 500);
            if (current_time - election_timer > timeout_election) {
                convert_candidate();
            }
        } else if (role == candidate) {
            // election timeout elapses: start new election
            int timeout_election = get_random(800, 1000);
            if (current_time - election_timer > timeout_election) {
                convert_follower();
            }
        } else if (role == leader) {
//            break;
        }
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // periodically send logs to the follower.

    // Only work for the leader.
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        if (is_leader(current_term)) {
            matchIndex[my_id] = log.size() - 1;
            for (int i = 0; i < num_nodes(); i++) {
                // there are new logs to append
                if (i != my_id && (int) log.size() > nextIndex[i]) {
                    // send args, specially if nextIndex[i] == 1, then send log[0] and term of it is 0
                    append_entries_args<command> arg(current_term, my_id, nextIndex[i] - 1,
                                                     log[nextIndex[i] - 1].term,
                                                     log, commitIndex);
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                    RAFT_LOG(
                            "run_background_commit  my_id: %d, my_log_max_id:%d, node=%d, nextIndex[node]: %d, CommitIndex: %d",
                            my_id, (int) log.size() - 1,
                            i, nextIndex[i], commitIndex)
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // periodically apply committed logs the state machine

    // Work for all the nodes.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        if (commitIndex > lastApplied) {
            RAFT_LOG("run_background_apply my_id: %d, role: %d, commitIndex: %d, lastApplied: %d", my_id, role,
                     commitIndex, lastApplied)
            for (int i = lastApplied + 1; i <= commitIndex; i++) {
                state->apply_log(log[i].cmd);
            }
            lastApplied = commitIndex;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // periodically send empty append_entries RPC to the followers.

    // Only work for the leader.
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        if (is_leader(current_term)) {
            RAFT_LOG("send heartbeat my_id=%d num=%d", my_id, num_nodes())
            for (int i = 0; i < num_nodes(); i++)
                if (i != my_id) {
                    append_entries_args<command> args(current_term, my_id, commitIndex); // heartbeat;
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
}

/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::convert_follower() {
    RAFT_LOG("convert_follower my_id: %d, term: %d, role: %d", my_id, current_term, role)
    role = follower;
    voteFor = -1;
    election_timer = get_current_time();
    //store when change
    storage->store_metadata(current_term, voteFor);
}

template<typename state_machine, typename command>
void raft<state_machine, command>::convert_candidate() {
    role = candidate;
    // start election
    current_term++;
    voteFor = my_id;
    election_timer = get_current_time();
    RAFT_LOG("convert_candidate my_id: %d, term: %d, role: %d", my_id, current_term, role)

    // clear persistVote
    persistVote.clear();
    persistVote.insert(my_id);

    //store when change
    storage->store_metadata(current_term, voteFor);

    request_vote_args args(current_term, my_id, log.size() - 1, log.back().term);
    // ask for voting
    for (int i = 0; i < num_nodes(); i++)
        if (i != my_id)
            thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
}

template<typename state_machine, typename command>
void raft<state_machine, command>::convert_leader() {
    role = leader;
    RAFT_LOG("convert_leader my_id:%d, term:%d", my_id, current_term)
    nextIndex.assign(num_nodes(), 1);
    matchIndex.assign(num_nodes(), 0);
    matchIndex[my_id] = log.size() - 1;
}


#endif // raft_h