#include "config.h"

// system mode: 0: eager, 1: lazy, 2: 2pc, 3: single
int SYSTEM_MODE = 2;
int LOCAL_BATCH_TXN_SIZE = 100;
// WORKLOAD_MODE:
//   0: SmallBank
//   1: TPCC
//   2: YCSB (脚本式：每事务固定 10 个 key)
//   4: SQL  (StartDatabaseSQL，通过 socket 输入 SQL)
//   5: Interactive Bench (StartInteractiveBench，通过 socket 按行输入 begin/commit/abort/<key>,<is_partition>,<is_write>)
int WORKLOAD_MODE = 2;  
int ComputeNodeCount = 8;   
bool use_rdma = false;      // TODO
int thread_num_per_node = 1;
int PARALLEL_PAGE_FETCH = 0;
int TUPLE_CONFLICT_PRECHECK = 0;
double WR_TXN_RATE = 0.8;
double LOCAL_TRASACTION_RATE = 0.8;
uint64_t ATTEMPTED_NUM = 1000;
double CrossNodeAccessRatio = 0.1;
int LOCK_MODE = NO_WAIT;
int delay_time = 0;
double LongTxnRate = 0.10;
double HYBRID_SKEW_THRESHOLD = 0.5;
int HOT_KEY_TOP_N = 100;
#include <atomic>
#include <cstdint>
std::atomic<int64_t> tuple_precheck_pass_count{0};
std::atomic<int64_t> tuple_precheck_reject_count{0};
