#pragma once
#include <cstddef>
#include <gflags/gflags.h>
#include <string>
#include <type_traits>
#include <utility>

#if __has_include(<brpc/socket_mode.h>)
#include <brpc/socket_mode.h>
#define BRPC_HAS_SOCKET_MODE_HEADER 1
#else
#define BRPC_HAS_SOCKET_MODE_HEADER 0
#endif

template <typename T, typename = void>
struct BrpcHasSocketMode : std::false_type {};

template <typename T>
struct BrpcHasSocketMode<T, std::void_t<decltype(std::declval<T&>().socket_mode)>> : std::true_type {};

template <typename T, typename = void>
struct BrpcHasUseRdma : std::false_type {};

template <typename T>
struct BrpcHasUseRdma<T, std::void_t<decltype(std::declval<T&>().use_rdma)>> : std::true_type {};

template <typename T>
inline void SetBrpcRdmaOption(T& options, bool enabled) {
#if BRPC_HAS_SOCKET_MODE_HEADER
    if constexpr (BrpcHasSocketMode<T>::value) {
        options.socket_mode = enabled ? brpc::SOCKET_MODE_RDMA : brpc::SOCKET_MODE_TCP;
        return;
    }
#endif
    if constexpr (BrpcHasUseRdma<T>::value) {
        options.use_rdma = enabled;
    }
}

#define SET_BRPC_RDMA_OPTION(options, enabled) SetBrpcRdmaOption((options), (enabled))

/*********************** For common **********************/
// Max data item size.
// 8: smallbank
// 40: tatp
// 664: tpcc
// 1024:ycsb
// 40: micro-benchmark

// ! pay attention: need modify this when use different workload
// Max data item size.
// 8: smallbank
// 664: tpcc
// 1008: yscb

#define BUFFER_LENGTH 2097152
#define NUM_BUFFER_PARTITION 16

enum class TsPhase{
    BEGIN = 0,          // 初始化
    RUNNING = 1,        // 在时间片内
    SWITCHING = 2       // 切换阶段
};

#define ComputeNodeBufferPageSize 262144 // 262144*4KB = 1GB
// #define ComputeNodeBufferPageSize 2621440 // for leap

inline size_t NormalizePageLockTableCapacity(size_t requested_capacity) {
    const size_t legacy_page_id_capacity = static_cast<size_t>(ComputeNodeBufferPageSize);
    return requested_capacity < legacy_page_id_capacity ? legacy_page_id_capacity
                                                        : requested_capacity;
}

#define BufferFusionSize ComputeNodeBufferPageSize
#define PartitionDataSize (ComputeNodeBufferPageSize / ComputeNodeCount)
#define MaxComputeNodeCount 128

// 定义算法版本 0:baseline, 1:lazy release, 2: phase switch-baseline 3: phase switch-lazy release 4: delay release 5: phase switch-delay release
extern int SYSTEM_MODE;

extern int LOCAL_BATCH_TXN_SIZE;

// 定义所跑的workload 0:smallbank 1:tpcc
extern int WORKLOAD_MODE;

extern bool use_rdma;
extern int ComputeNodeCount;
extern int thread_num_per_node;
extern int PARALLEL_PAGE_FETCH;
extern int TUPLE_CONFLICT_PRECHECK;
extern double WR_TXN_RATE;
extern double LOCAL_TRASACTION_RATE;
extern uint64_t ATTEMPTED_NUM;
extern double CrossNodeAccessRatio;
extern int delay_time;
extern double LongTxnRate;

// SYSTEM_MODE == 4 (2PC + Lazy 混合) 下的偏斜度阈值：
// 当一个事务访问的 key 中「热 key 占比」>= 该阈值时，走 2PC 提交；否则走 Lazy 提交。
// 取值范围 [0.0, 1.0]，0.0 = 全部走 2PC，1.0 = 全部走 Lazy。
extern double HYBRID_SKEW_THRESHOLD;

// Zipfian 模式下「头部热点 key 数量」：
// 把 zipfian 访问的前 HOT_KEY_TOP_N 个索引（即 idx < HOT_KEY_TOP_N）视为热点 key。
// 取值范围 [1, +∞)。仅在 zipfian (use_zipfian == 1) 模式下使用。
extern int HOT_KEY_TOP_N;

#define MaxPartitionCount 1024

#define ThreadPoolSizePerWorker 2 // 每个worker线程池的大小
// 定义计算节点的各个阶段
enum class Phase {PARTITION, GLOBAL, SWITCH_TO_PAR, SWITCH_TO_GLOBAL, BEGIN};
enum class OperationType {READ, WRITE};

// #define PartitionPhaseDuration 30000 // us, 1000us = 1ms
// #define GlobalPhaseDuration 10000 // us, 1000us = 1ms
#define EpochOptCount 1000 // 一个epoch中操作的次数
#define EpochTime 100
#define EarlyStopEpoch 1

#define HHH 6
#define KKK 5

#define RAFT false     // 存储层是否使用RAFT保证容错

#define AsyncCommit2pc false // 对于2PC的提交阶段, 是否采用异步处理

#define RunOperationTime 500 // us, 1000us = 1ms

#define NetworkLatency 0 // us, 1000us = 1ms , 1000000us = 1s

#define BatchTimeStamp 200 

#define UniformHot false 

#define LongTxnSize 10 // 长事务大小

#define WrongPrediction 0

#define SINGLE_MISS_CACHE_RATE 0.875 // 7/8

#define ATOM_FETCH_ADD(dest, value) __sync_fetch_and_add(&(dest), value)

enum lock_mode_type {NO_WAIT = 0, WAIT_DIE = 1 };
extern int LOCK_MODE;

/*********************** Affinity-driven repartitioning (论文实验) **********************/
// All disabled by default. Enable via config/compute_node_config.json -> "affinity": { "enable": true, ... }
extern bool        enable_affinity;             // master switch
extern int         affinity_aggregator_tick_ms; // default 50
extern int         affinity_partition_cycle_ms; // default 5000
extern int         affinity_migration_tick_ms;  // default 200
extern int         affinity_migration_batch;    // default 50
extern int         affinity_migration_workers;  // default 1 — number of MigrationLoop drainer threads per compute node
extern double      affinity_edge_min_weight;    // default 2.0 — drop singleton edges before partitioning
extern double      affinity_edge_decay_factor;  // default 0.5 — per-epoch EWMA decay on aggregator's accumulated graph; 1.0 = infinite memory, 0.0 = reset every epoch (old behaviour)
extern int         affinity_assignment_ttl_epochs; // default 30 — drop AssignmentTable entries untouched for this many epochs; 0 = no TTL (unbounded growth)
extern int         affinity_max_vertices;       // default 5,000,000 safety cap
extern int         affinity_shuffle_barrier_ms; // default 30000 EdgeShuffler barrier timeout
extern int         affinity_uds_recv_timeout_ms;// default 30000 sidecar UDS recv timeout
extern double      affinity_repart_itr;         // default 1000.0 ParMETIS itr (edgecut vs migration cost)
extern double      affinity_ubvec;              // default 1.05 ParMETIS ubvec (allowable partition size imbalance; >1.0 = tolerance, 1.05 = 5%)
extern double      affinity_max_changed_vertices_ratio; // default 1.0 disables the anti-thrash assignment gate
extern std::string affinity_sidecar_uds_path;   // default "/tmp/wookong_parmetis.sock"
extern int         affinity_timeseries_tick_ms; // default 1000 (per-second sample for affinity_timeseries.csv)
extern std::string affinity_timeseries_csv_path;// default "affinity_timeseries.csv"

// Sidecar auto-spawn knobs. When enabled, the leader compute_server
// (machine_id == 0) fork+execs `mpirun` on startup so the operator no longer
// has to run affinity_sidecar_launch.sh by hand. Other ranks rely on the
// PartitionerLoop UDS retry to wait for the local sidecar to come up.
extern bool        affinity_auto_spawn_sidecar;     // default true
extern std::string affinity_sidecar_binary_path;    // default "./parmetis_sidecar/parmetis_sidecar" (relative to cwd)
extern std::string affinity_sidecar_hostfile_path;  // default "/tmp/wookong_affinity_hostfile"
extern std::string affinity_sidecar_mpirun_bin;     // default "mpirun" (looked up via PATH)
extern std::string affinity_sidecar_mpirun_extra_args; // default "" (e.g. "--mca btl_tcp_if_include eth0")
extern std::string affinity_sidecar_log_path;       // default "affinity_sidecar.log" (mpirun child stdout/stderr)
