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

// Affinity-driven repartitioning defaults (off unless JSON enables it)
bool        enable_affinity              = false;
int         affinity_aggregator_tick_ms  = 50;
int         affinity_partition_cycle_ms  = 5000;
int         affinity_migration_tick_ms   = 200;
int         affinity_migration_batch     = 50;
int         affinity_migration_workers   = 1;
bool        affinity_enable_batch_migration = true;
double      affinity_edge_min_weight     = 2.0;
double      affinity_edge_decay_factor    = 0.5;
int         affinity_assignment_ttl_epochs = 30;
int         affinity_max_vertices        = 5000000;
int         affinity_shuffle_barrier_ms  = 30000;
int         affinity_uds_recv_timeout_ms = 30000;
double      affinity_repart_itr          = 1000.0;
double      affinity_ubvec               = 1.05;
double      affinity_max_changed_vertices_ratio = 1.0;
std::string affinity_sidecar_uds_path    = "/tmp/wookong_parmetis.sock";
int         affinity_timeseries_tick_ms  = 1000;
std::string affinity_timeseries_csv_path = "affinity_timeseries.csv";
bool        affinity_timeseries_when_disabled = false;

bool        affinity_auto_spawn_sidecar       = true;
std::string affinity_sidecar_binary_path      = "./parmetis_sidecar/parmetis_sidecar";
std::string affinity_sidecar_hostfile_path    = "/tmp/wookong_affinity_hostfile";
std::string affinity_sidecar_mpirun_bin       = "mpirun";
std::string affinity_sidecar_mpirun_extra_args = "";
std::string affinity_sidecar_log_path         = "affinity_sidecar.log";
