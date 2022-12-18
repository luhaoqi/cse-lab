#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab4: Your definition here.
        int task_type;
        int index;  // id of task
        int num;    // reducer need to know how many files need to process
        string filename;
	};

    friend marshall &operator<<(marshall &m, const AskTaskResponse &res) {
        return m << res.task_type << res.filename << res.index << res.num;
    }

    friend unmarshall &operator>>(unmarshall &u, AskTaskResponse &res) {
        return u >> res.task_type >> res.filename >> res.index >> res.num;
    }

	struct AskTaskRequest {
		// Lab4: Your definition here.
        int id; //
	};

	struct SubmitTaskResponse {
		// Lab4: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab4: Your definition here.
	};

};

#endif

