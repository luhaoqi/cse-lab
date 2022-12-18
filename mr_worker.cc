#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>

#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
};

int Hash(const string &str) {
    unsigned int hashVal = 0;
    for (const char &ch: str) {
        hashVal = hashVal * 131 + (int) ch;
    }
    return hashVal % REDUCER_COUNT;
}

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector <KeyVal> Map(const string &filename, const string &content) {
    // Copy your code from mr_sequential.cc here.
    stringstream ss(content);
    map<string, int> cnt;
    char c;
    string word = "";
    while (ss.get(c)) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
            word += c;
        } else {
            if (word.length() > 0) {
                cnt[word]++;
                word = "";
            }
        }
    }
    // 如果最后有一个word
    if (word.length() > 0) {
        cnt[word]++;
        word = "";
    }
    vector <KeyVal> vec;
    for (auto &t: cnt) {
        vec.push_back(KeyVal{t.first, to_string(t.second)});
    }
    return vec;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector <string> &values) {
    // Copy your code from mr_sequential.cc here.
    int tot = 0;
    for (auto &v: values)
        tot += atoi(v.c_str());
    return to_string(tot);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);

typedef string (*REDUCEF)(const string &key, const vector <string> &values);

class Worker {
public:
    Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

    void doWork();

private:
    void doMap(int index, const string &filename);

    void doReduce(int index, int num);

    void doSubmit(mr_tasktype taskType, int index);

    mutex mtx;
    int id;

    rpcc *cl;
    std::string basedir;
    MAPF mapf;
    REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf) {
    // id generator
    this->basedir = dir;
    if (basedir[basedir.length() - 1] != '/')
        basedir += '/';
    this->id = rand();

    this->mapf = mf;
    this->reducef = rf;

    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    this->cl = new rpcc(dstsock);
    if (this->cl->bind() < 0) {
        printf("mr worker: call bind error\n");
    }
}

void Worker::doMap(int index, const string &filename) {
    // Lab4: Your code goes here.
    string intermediate_file_pre = basedir + "mr-" + to_string(index) + '-';
    // read file and Map it
    string content;
    getline(ifstream(filename), content, '\0');
    vector <KeyVal> KVA = Map(filename, content);
    // process KVA
    vector <string> reduce(REDUCER_COUNT);
    for (const KeyVal &x: KVA) {
        // cal which reduce to put
        int reducerId = Hash(x.key);
        reduce[reducerId] += x.key + ' ' + x.val + '\n';
    }
    // generate intermediate_file
    for (int i = 0; i < REDUCER_COUNT; ++i) {
        const string &s = reduce[i];
        if (!s.empty()) {
            string intermediate_file_path = intermediate_file_pre + to_string(i);
            ofstream out(intermediate_file_path);
            out << s;
            out.close();
        }
    }
}

void Worker::doReduce(int index, int num) {
    // Lab4: Your code goes here.
    // cal the word count
    map<string, int> cnt;
    for (int i = 0; i < num; ++i) {
        string filepath = basedir + "mr-" + to_string(i) + '-' + to_string(index);
        ifstream in(filepath);
        if (!in.is_open()) {
            continue;
        }
        string key, value;
        while (in >> key >> value)
            cnt[key] += atoi(value.c_str());
        in.close();
    }
    // save output
    string content;
    for (const auto &x: cnt)
        content += x.first + ' ' + to_string(x.second) + '\n';

    // save to individual files
    ofstream out(basedir + "mr-out-" + to_string(index));
    out << content << endl;
    out.close();
}

void Worker::doSubmit(mr_tasktype taskType, int index) {
    bool b;
    mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
    if (ret != mr_protocol::OK) {
        fprintf(stderr, "submit task failed\n");
        exit(-1);
    }
}

void Worker::doWork() {
    printf("i'm worker %d\n", id);
    for (;;) {

        //
        // Lab4: Your code goes here.
        // Hints: send asktask RPC call to coordinator
        // if mr_tasktype::MAP, then doMap and doSubmit
        // if mr_tasktype::REDUCE, then doReduce and doSubmit
        // if mr_tasktype::NONE, meaning currently no work is needed, then sleep
        //
        mr_protocol::AskTaskResponse res;
        // ask task from coordinator
        cl->call(mr_protocol::asktask, id, res);
        printf("%d %d\n", res.task_type, res.index);
        switch ((mr_tasktype) res.task_type) {
            case MAP:
                printf("worker(%d) receive map task\n", id);
                doMap(res.index, res.filename);
                doSubmit(MAP, res.index);
                break;
            case REDUCE:
                printf("worker(%d) receive reduce task\n", id);
                doReduce(res.index, res.num);
                doSubmit(REDUCE, res.index);
                break;
            case NONE:
                printf("worker(%d) receive none task\n", id);
                sleep(1);
                break;
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
        exit(1);
    }

    MAPF mf = Map;
    REDUCEF rf = Reduce;

    Worker w(argv[1], argv[2], mf, rf);
    w.doWork();

    return 0;
}

