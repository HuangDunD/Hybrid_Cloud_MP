// author: hcy
// date: 2024.6.21
#pragma once
#include <cassert>
#include <unistd.h>
#include <list>
#include <queue>
#include <set>
#include <utility>
#include <condition_variable>
#include <mutex>
#include <brpc/channel.h>
#include "fiber/scheduler.h"
#include "fiber/thread.h"
#include "unordered_map"
#include "atomic"
#include "chrono"
#include "algorithm"
#include "iterator"

#include "util/json_config.h"
#include "storage/storage_service.pb.h"
#include "remote_page_table/remote_page_table.pb.h"
#include "LPLM/local_page_lock.h"
#include "LPLM/local_ER_page_lock.h"
#include "LPLM/local_LR_page_lock.h"
#include "bufferpool.h" 
#include "config.h"
#include "connection/meta_manager.h"
#include "core/storage/sm_meta.h"

struct Page_request_info{
    page_id_t page_id;
    OperationType operation_type;
    std::chrono::_V2::high_resolution_clock::time_point start_time; // 记录开始时间
};

struct Txn_request_info{
    // int txn_type;
    uint64_t seed;
    timespec start_time; // 记录开始时间
};

class ComputeNode {
friend class ComputeServer;
friend class ComputeNodeServiceImpl;
public:
    /*
        1. 初始化和 Lock Fusion 以及 storage node 的 brpc 连接
        2. 初始化本地的 Lock
        3. 根据不同的 WORKLOAD，创建不同的表(元数据)，然后为每张表创建各自的本地缓冲池，并把这个表的部分页从存储中加载到本地内存(预加载)
    */
    ComputeNode(int nodeid, std::string remote_server_ip, int remote_server_port, MetaManager* meta_manager = nullptr) :node_id(nodeid), meta_manager_(meta_manager) {
        // connect to remote pagetable&bufferpool server
        brpc::ChannelOptions options;
        SET_BRPC_RDMA_OPTION(options, use_rdma);
        ts_cnt = node_id;       // 初始分配的时间片设置成 node_id
        ts_cnt_hot = node_id;
        if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            std::cout << "Node : " << node_id << " allocate a timestamp , id = " << ts_cnt << "\n";
        }
        // options.timeout_ms = 5000; // 5s超时
        options.timeout_ms = 0x7fffffff; // 2147483647ms
        std::string remote_node = remote_server_ip + ":" + std::to_string(remote_server_port);
        if (page_table_channel.Init(remote_node.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to init channel";
            exit(1);
        }
        brpc::ChannelOptions options2;
        SET_BRPC_RDMA_OPTION(options2, use_rdma);
        options2.timeout_ms = 0x7fffffff; // 2147483647ms
        std::string storage_node = meta_manager->remote_storage_nodes[0].ip + ":" + std::to_string(meta_manager->remote_storage_nodes[0].port);
        if (storage_channel.Init(storage_node.c_str(), &options2) != 0) {
            LOG(ERROR) << "Fail to init channel";
            exit(1);
        }

        // 配置每个表的缓冲区大小
        size_t table_pool_size_cfg = 0; 
        size_t blink_buffer_pool_cfg = 0;
        size_t fsm_pool_size_cfg = 0;
        size_t partition_size_cfg = 0;
        bool has_table_pool_size_cfg = false;
        bool push_page_with_scheduler_cfg = false;
        bool generate_log_cfg = true;
        {
            std::string config_filepath = "../../config/compute_node_config.json";
            auto json_config = JsonConfig::load_file(config_filepath);
            auto pool_size_node = json_config.get("table_buffer_pool_size_per_table");
            auto index_pool_size = json_config.get("index_buffer_pool_size_per_table");
            auto fsm_pool_size = json_config.get("fsm_buffer_pool_size_per_table");
            auto partition_size = json_config.get("partition_size_per_table");
            auto ts_time_node = json_config.get("ts_time");
            auto push_page_with_scheduler_node = json_config.get("push_page_with_scheduler");
            auto push_page_scheduler_threads_node = json_config.get("push_page_scheduler_threads");
            auto generate_log_node = json_config.get("local_compute_node").get("generate_log");
            auto hybrid_skew_threshold_node = json_config.get("hybrid_skew_threshold");
            auto hot_key_top_n_node = json_config.get("hot_key_top_n");
            if (pool_size_node.exists() && pool_size_node.is_int64()) {
                table_pool_size_cfg = (size_t)pool_size_node.get_int64();
                has_table_pool_size_cfg = true;
            }
            if (index_pool_size.exists() && index_pool_size.is_int64()){
                blink_buffer_pool_cfg = (size_t)index_pool_size.get_int64();
            }
            if (fsm_pool_size.exists() && fsm_pool_size.is_int64()){
                fsm_pool_size_cfg = (size_t)fsm_pool_size.get_int64();
            }
            if (partition_size.exists() && partition_size.is_int64()){
                partition_size_cfg = (size_t)partition_size.get_int64();
            }
            if (ts_time_node.exists() && ts_time_node.is_int64()){
                ts_time = (int)ts_time_node.get_int64();
            }
            if (push_page_with_scheduler_node.exists() && push_page_with_scheduler_node.is_int64()){
                push_page_with_scheduler_cfg = (push_page_with_scheduler_node.get_int64() != 0);
            }
            if (push_page_scheduler_threads_node.exists() && push_page_scheduler_threads_node.is_int64()){
                push_page_scheduler_threads = std::max(1, (int)push_page_scheduler_threads_node.get_int64());
            }
            if (generate_log_node.exists() && generate_log_node.is_int64()){
                generate_log_cfg = (generate_log_node.get_int64() != 0);
            }
            // SYSTEM_MODE == 4：根据事务热点偏斜度动态选择 2PC / Lazy 提交的阈值
            if (hybrid_skew_threshold_node.exists()){
                if (hybrid_skew_threshold_node.is_double()){
                    HYBRID_SKEW_THRESHOLD = hybrid_skew_threshold_node.get_double();
                } else if (hybrid_skew_threshold_node.is_int64()){
                    HYBRID_SKEW_THRESHOLD = (double)hybrid_skew_threshold_node.get_int64();
                }
                if (HYBRID_SKEW_THRESHOLD < 0.0) HYBRID_SKEW_THRESHOLD = 0.0;
                if (HYBRID_SKEW_THRESHOLD > 1.0) HYBRID_SKEW_THRESHOLD = 1.0;
            }
            // Zipfian 模式下，把访问索引在 [0, HOT_KEY_TOP_N) 的 key 视为热点 key
            if (hot_key_top_n_node.exists() && hot_key_top_n_node.is_int64()){
                HOT_KEY_TOP_N = (int)hot_key_top_n_node.get_int64();
                if (HOT_KEY_TOP_N < 1) HOT_KEY_TOP_N = 1;
            }
            std::cout << "Table BufferPool Size Per Table : " << table_pool_size_cfg << "\n";
            std::cout << "Index BufferPool Size Per Table : " << blink_buffer_pool_cfg << "\n";
            if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
                std::cout << "TS Time Slice : " << ts_time/1000 << "ms" << "\n";
            }

            // Affinity-driven repartitioning section (optional, disabled by default)
            auto affinity_section = json_config.get("affinity");
            if (affinity_section.exists()) {
                auto enable_node              = affinity_section.get("enable");
                auto agg_tick_node            = affinity_section.get("aggregator_tick_ms");
                auto part_cycle_node          = affinity_section.get("partition_cycle_ms");
                auto mig_tick_node            = affinity_section.get("migration_tick_ms");
                auto mig_batch_node           = affinity_section.get("migration_batch");
                auto mig_workers_node         = affinity_section.get("migration_workers");
                auto edge_min_w_node          = affinity_section.get("edge_min_weight");
                auto edge_decay_node          = affinity_section.get("edge_decay_factor");
                auto asn_ttl_node             = affinity_section.get("assignment_ttl_epochs");
                auto max_vert_node            = affinity_section.get("max_vertices");
                auto shuffle_barrier_node     = affinity_section.get("shuffle_barrier_ms");
                auto uds_recv_to_node         = affinity_section.get("uds_recv_timeout_ms");
                auto repart_itr_node          = affinity_section.get("repart_itr");
                auto ubvec_node               = affinity_section.get("ubvec");
                auto sidecar_uds_path_node    = affinity_section.get("sidecar_uds_path");
                auto ts_tick_node             = affinity_section.get("timeseries_tick_ms");
                auto ts_path_node             = affinity_section.get("timeseries_csv_path");
                auto auto_spawn_node          = affinity_section.get("auto_spawn_sidecar");
                auto sc_bin_node              = affinity_section.get("sidecar_binary_path");
                auto sc_hostfile_node         = affinity_section.get("sidecar_hostfile_path");
                auto sc_mpirun_bin_node       = affinity_section.get("sidecar_mpirun_bin");
                auto sc_mpirun_args_node      = affinity_section.get("sidecar_mpirun_extra_args");
                auto sc_log_node              = affinity_section.get("sidecar_log_path");
                if (enable_node.exists()) {
                    if (enable_node.is_bool())      enable_affinity = enable_node.get_bool();
                    else if (enable_node.is_int64()) enable_affinity = (enable_node.get_int64() != 0);
                }
                if (agg_tick_node.exists() && agg_tick_node.is_int64())          affinity_aggregator_tick_ms  = (int)agg_tick_node.get_int64();
                if (part_cycle_node.exists() && part_cycle_node.is_int64())      affinity_partition_cycle_ms  = (int)part_cycle_node.get_int64();
                if (mig_tick_node.exists() && mig_tick_node.is_int64())          affinity_migration_tick_ms   = (int)mig_tick_node.get_int64();
                if (mig_batch_node.exists() && mig_batch_node.is_int64())        affinity_migration_batch     = (int)mig_batch_node.get_int64();
                if (mig_workers_node.exists() && mig_workers_node.is_int64()) {
                    int v = (int)mig_workers_node.get_int64();
                    if (v < 1) v = 1;
                    affinity_migration_workers = v;
                }
                if (edge_min_w_node.exists()) {
                    if (edge_min_w_node.is_double())     affinity_edge_min_weight = edge_min_w_node.get_double();
                    else if (edge_min_w_node.is_int64()) affinity_edge_min_weight = (double)edge_min_w_node.get_int64();
                }
                if (edge_decay_node.exists()) {
                    if (edge_decay_node.is_double())     affinity_edge_decay_factor = edge_decay_node.get_double();
                    else if (edge_decay_node.is_int64()) affinity_edge_decay_factor = (double)edge_decay_node.get_int64();
                }
                if (asn_ttl_node.exists() && asn_ttl_node.is_int64())            affinity_assignment_ttl_epochs = (int)asn_ttl_node.get_int64();
                if (max_vert_node.exists() && max_vert_node.is_int64())          affinity_max_vertices        = (int)max_vert_node.get_int64();
                if (shuffle_barrier_node.exists() && shuffle_barrier_node.is_int64()) affinity_shuffle_barrier_ms  = (int)shuffle_barrier_node.get_int64();
                if (uds_recv_to_node.exists() && uds_recv_to_node.is_int64())    affinity_uds_recv_timeout_ms = (int)uds_recv_to_node.get_int64();
                if (repart_itr_node.exists()) {
                    if (repart_itr_node.is_double())     affinity_repart_itr = repart_itr_node.get_double();
                    else if (repart_itr_node.is_int64()) affinity_repart_itr = (double)repart_itr_node.get_int64();
                }
                if (ubvec_node.exists()) {
                    if (ubvec_node.is_double())     affinity_ubvec = ubvec_node.get_double();
                    else if (ubvec_node.is_int64()) affinity_ubvec = (double)ubvec_node.get_int64();
                }
                if (sidecar_uds_path_node.exists() && sidecar_uds_path_node.is_str()) affinity_sidecar_uds_path = sidecar_uds_path_node.get_str();
                if (ts_tick_node.exists() && ts_tick_node.is_int64()) affinity_timeseries_tick_ms = (int)ts_tick_node.get_int64();
                if (ts_path_node.exists() && ts_path_node.is_str())   affinity_timeseries_csv_path = ts_path_node.get_str();
                if (auto_spawn_node.exists() && auto_spawn_node.is_int64())     affinity_auto_spawn_sidecar       = (auto_spawn_node.get_int64() != 0);
                if (sc_bin_node.exists() && sc_bin_node.is_str())               affinity_sidecar_binary_path      = sc_bin_node.get_str();
                if (sc_hostfile_node.exists() && sc_hostfile_node.is_str())     affinity_sidecar_hostfile_path    = sc_hostfile_node.get_str();
                if (sc_mpirun_bin_node.exists() && sc_mpirun_bin_node.is_str()) affinity_sidecar_mpirun_bin       = sc_mpirun_bin_node.get_str();
                if (sc_mpirun_args_node.exists() && sc_mpirun_args_node.is_str()) affinity_sidecar_mpirun_extra_args = sc_mpirun_args_node.get_str();
                if (sc_log_node.exists() && sc_log_node.is_str())               affinity_sidecar_log_path         = sc_log_node.get_str();
                if (enable_affinity) {
                    std::cout << "Affinity repartitioning ENABLED."
                              << " agg=" << affinity_aggregator_tick_ms << "ms"
                              << " part=" << affinity_partition_cycle_ms << "ms"
                              << " mig=" << affinity_migration_tick_ms << "ms"
                              << " batch=" << affinity_migration_batch
                              << " workers=" << affinity_migration_workers
                              << " sidecar_uds=" << affinity_sidecar_uds_path << "\n";
                    std::cout << "WARN: affinity migration is non-recoverable; do NOT kill -9 mid-experiment.\n";
                }
            }
        }

        push_page_with_scheduler = (SYSTEM_MODE == 1 || SYSTEM_MODE == 4) && push_page_with_scheduler_cfg && generate_log_cfg;

        meta_manager_->initParSize();
        int table_num = meta_manager_->GetTableNum();

        // 负载模式下，先把每张表(及其 BLink/FSM) 的实际页面数取出来，
        // 用于按照实际大小给本地锁表 / 缓冲区分配容量。
        std::vector<int> max_page_per_table;
        std::copy(meta_manager->page_num_per_table.begin(), meta_manager->page_num_per_table.end(),
                    std::back_inserter(max_page_per_table));

        // 锁表仍按 page_id 直接索引。负载模式先按实际页面数估算，再规范化到
        // legacy page_id 域，避免 page_id 与紧凑容量语义不一致。
        auto calc_lock_capacity = [](int page_num) -> size_t {
            return NormalizePageLockTableCapacity((size_t)std::max(0, page_num) + 10);
        };
        auto calc_fsm_capacity = [](int page_num) -> size_t {
            return NormalizePageLockTableCapacity(
                std::max((size_t)std::max(0, page_num) + 10, (size_t)2048));
        };

        if (SYSTEM_MODE == 0){
            // eager
            eager_local_page_lock_tables.resize(30000);
            for(int i = 0; i < table_num ; i++){
                eager_local_page_lock_tables[i] = new ERLocalPageLockTable(
                    calc_lock_capacity(max_page_per_table[i]));
                eager_local_page_lock_tables[i + 10000] = new ERLocalPageLockTable(
                    calc_lock_capacity(max_page_per_table[i + 10000]));
                eager_local_page_lock_tables[i + 20000] = new ERLocalPageLockTable(
                    calc_fsm_capacity(max_page_per_table[i + 20000]));
            }
        }else if (SYSTEM_MODE == 1 || SYSTEM_MODE == 4){
            // lazy
            lazy_local_page_lock_tables.resize(30000);
            for(int i = 0; i < table_num; i++){
                lazy_local_page_lock_tables[i] = new LRLocalPageLockTable(
                    calc_lock_capacity(max_page_per_table[i]));
                lazy_local_page_lock_tables[i + 10000] = new LRLocalPageLockTable(
                    calc_lock_capacity(max_page_per_table[i + 10000]));
                lazy_local_page_lock_tables[i + 20000] = new LRLocalPageLockTable(
                    calc_fsm_capacity(max_page_per_table[i + 20000]));
            }
            // 尝试一下，验证下线程去推页面的性能如何
            if (push_page_with_scheduler){
                push_page_scheduler = new Scheduler(push_page_scheduler_threads , true , "PushPageScheduler");
                push_page_scheduler->start();
            }
        }else if (SYSTEM_MODE == 2){
            // 2pc
            local_page_lock_tables.reserve(table_num);
            lazy_local_page_lock_tables.resize(30000);
            for(int i = 0; i < table_num; i++){
                local_page_lock_tables.emplace_back(new LocalPageLockTable(
                    calc_lock_capacity(max_page_per_table[i])));
                lazy_local_page_lock_tables[i + 10000] = new LRLocalPageLockTable(
                    calc_lock_capacity(max_page_per_table[i + 10000]));
                lazy_local_page_lock_tables[i + 20000] = new LRLocalPageLockTable(
                    calc_fsm_capacity(max_page_per_table[i + 20000]));
            }
        }else if (SYSTEM_MODE == 3){
            assert(false);
            // TODO
            local_page_lock_tables.reserve(table_num);
            for(int i = 0; i < table_num; i++){
                local_page_lock_tables.emplace_back(new LocalPageLockTable(
                    calc_lock_capacity(max_page_per_table[i])));
            }
        } else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            local_page_lock_tables.reserve(table_num);
            lazy_local_page_lock_tables.reserve(30000);
            for (int i = 0 ; i < table_num ; i++){
                local_page_lock_tables.emplace_back(new LocalPageLockTable(
                    calc_lock_capacity(max_page_per_table[i])));
            }

            for (int i = 0 ; i < table_num ; i++){
                lazy_local_page_lock_tables[i + 10000] = new LRLocalPageLockTable(
                    calc_lock_capacity(max_page_per_table[i + 10000]));
                lazy_local_page_lock_tables[i + 20000] = new LRLocalPageLockTable(
                    calc_fsm_capacity(max_page_per_table[i + 20000]));
            }
        } else {
            assert(false);
        }
        for (int i = 0 ; i < table_num ; i++){
            // TPCC 负载比较特殊，某些表的页面数量太少了，所以按照页面数量来分区
            if (WORKLOAD_MODE == 1){
                meta_manager_->par_size_per_table[i] = meta_manager_->page_num_per_table[i] / ComputeNodeCount;
                meta_manager_->par_size_per_table[i + 10000] = meta_manager_->page_num_per_table[i + 10000] / ComputeNodeCount;
                meta_manager_->par_size_per_table[i + 20000] = meta_manager_->page_num_per_table[i + 20000] / ComputeNodeCount;
                // std::cout << meta_manager_->par_size_per_table[i] << " " << meta_manager_->par_size_per_table[i + 10000] << " " << meta_manager_->par_size_per_table[i + 20000] << "\n";
            } else {
                // 固定大小切分
                meta_manager_->par_size_per_table[i] = partition_size_cfg;
                meta_manager_->par_size_per_table[i + 10000] = meta_manager_->page_num_per_table[i + 10000] / ComputeNodeCount;
                meta_manager_->par_size_per_table[i + 20000] = 100;
            }

            local_buffer_pools = std::vector<BufferPool*>(30000 , nullptr);

            for(int table_id = 0; table_id < table_num ; table_id++) {
                assert(table_pool_size_cfg > 0 && blink_buffer_pool_cfg > 0 && fsm_pool_size_cfg > 0);
                // 表本体
                local_buffer_pools[table_id] = new BufferPool(table_pool_size_cfg , (size_t)max_page_per_table[table_id]);
                // BLink 树
                local_buffer_pools[table_id + 10000] = new BufferPool(blink_buffer_pool_cfg , (size_t)max_page_per_table[table_id + 10000]);
                // FSM 
                local_buffer_pools[table_id + 20000] = new BufferPool(fsm_pool_size_cfg , 5000);
            }
        }

        // par_size_per_table 与 page_cache 已就绪，构建每个节点的真实 key 列表，
        // 供 zipfian 等 key-level 热点访问模式使用（兼容 random_generate 模式）。
        meta_manager_->BuildNodeKeyLists(ComputeNodeCount);

        if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            int thread_num = 5;
            {
                std::string config_filepath = "../../config/compute_node_config.json";
                auto json_config = JsonConfig::load_file(config_filepath);
                auto client_conf = json_config.get("local_compute_node");
                if(client_conf.exists()) {
                     thread_num = (int)client_conf.get("thread_num_per_machine").get_int64();
                }
            }
            std::stringstream ss;
            ss << "node" << node_id << " scheduler";
            // 多的一个线程来调度时间片轮转
            scheduler = new Scheduler(thread_num + 1, false , "TS_Scheduler");
            scheduler->enableTimeSliceScheduling(ComputeNodeCount);
            scheduler->start();
        }


        // 不知道干啥的，这里先给他设置 100 吧
        local_buffer_pool = new BufferPool(ComputeNodeBufferPageSize , 100);
        fetch_remote_cnt = 0;
        fetch_allpage_cnt = 0;
        fetch_from_remote_cnt = 0;
        fetch_from_storage_cnt = 0;
        fetch_from_local_cnt = 0;
        evict_page_cnt = 0;
        lock_remote_cnt = 0;
        hit_delayed_release_lock_cnt = 0;
        latency_vec = std::vector<double>();
    }

    ComputeNode(int nodeid, std::string remote_server_ip, int remote_server_port, MetaManager* meta_manager , std::string db_name , int thread_num)
        : node_id(nodeid), meta_manager_(meta_manager){
        // SQL 模式目前只支持 lazy
        assert(SYSTEM_MODE == 1);
        brpc::ChannelOptions options;
        SET_BRPC_RDMA_OPTION(options, use_rdma);

        // 初始化和 metaserver 的连接
        options.timeout_ms = 0x7fffffff; // 2147483647ms
        std::string remote_node = remote_server_ip + ":" + std::to_string(remote_server_port);
        if (page_table_channel.Init(remote_node.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to init channel";
            exit(1);
        }

        // 初始化和 storage_server 的连接
        brpc::ChannelOptions options2;
        SET_BRPC_RDMA_OPTION(options2, use_rdma);
        options2.timeout_ms = 0x7fffffff; // 2147483647ms
        std::string storage_node = meta_manager->remote_storage_nodes[0].ip + ":" + std::to_string(meta_manager->remote_storage_nodes[0].port);
        if (storage_channel.Init(storage_node.c_str(), &options2) != 0) {
            LOG(ERROR) << "Fail to init channel";
            exit(1);
        }

        // 配置每个表的缓冲区大小
        size_t table_pool_size_cfg = 0; 
        size_t blink_buffer_pool_cfg = 0;
        size_t fsm_pool_size_cfg = 0;
        size_t partition_size_cfg = 0;
        {
            std::string config_filepath = "../../config/compute_node_config.json";
            auto json_config = JsonConfig::load_file(config_filepath);

            auto pool_size_node = json_config.get("table_buffer_pool_size_per_table");
            auto index_pool_size = json_config.get("index_buffer_pool_size_per_table");
            auto fsm_pool_size = json_config.get("fsm_buffer_pool_size_per_table");
            auto partition_size = json_config.get("partition_size_per_table");

            if (pool_size_node.exists() && pool_size_node.is_int64()) {
                table_pool_size_cfg = (size_t)pool_size_node.get_int64();
            }
            if (index_pool_size.exists() && index_pool_size.is_int64()){
                blink_buffer_pool_cfg = (size_t)index_pool_size.get_int64();
            }
            if (fsm_pool_size.exists() && fsm_pool_size.is_int64()){
                fsm_pool_size_cfg = (size_t)fsm_pool_size.get_int64();
            }
            if (partition_size.exists() && partition_size.is_int64()){
                partition_size_cfg = (size_t)partition_size.get_int64();
            }
            std::cout << "Table BufferPool Size Per Table : " << table_pool_size_cfg << "\n";
            std::cout << "Index BufferPool Size Per Table : " << blink_buffer_pool_cfg << "\n";
        }

        // std::cout << "Open DB : " << db_name << "\n";

        // meta_manager_->initParSize();
        // 先预设只有一个数据库，打开的就是这个数据库
        // 通知存储层，打开此数据库
        storage_service::StorageService_Stub storage_stub(&storage_channel);
        brpc::Controller cntl;
        storage_service::OpendbRequest open_db_request;
        storage_service::OpendbResponse open_db_response;
        open_db_request.set_db_name(db_name);
        open_db_request.set_node_id(nodeid);
        storage_stub.OpenDb(&cntl , &open_db_request , &open_db_response , NULL);
        if (cntl.Failed()){
            LOG(ERROR) << "Fail To Open DB , DB_Name = " << db_name;
            assert(false);
        }

        // 目前这个数据库里的表的数量
        int table_num = open_db_response.table_num();
        for (int i = 0 ; i < open_db_response.table_names_size() ; i++){
            table_names.emplace_back(open_db_response.table_names(i));
        }

        std::cout << "OpenDB : " << db_name << " Table Num = " << table_num << " Error Code = " << open_db_response.error_code() << "\n";

        // 根据目前表的数量，去初始化锁表和缓冲区
        if (SYSTEM_MODE == 1){
            lazy_local_page_lock_tables.resize(30000);
            for(int i = 0; i < table_num; i++){
                lazy_local_page_lock_tables[i] = new LRLocalPageLockTable();
                lazy_local_page_lock_tables[i + 10000] = new LRLocalPageLockTable();
                lazy_local_page_lock_tables[i + 20000] = new LRLocalPageLockTable();
            }
        }else {
            assert(false);
        }

        meta_manager_->initParSize();
        // 直接一口气全初始化了
        for (int i = 0 ; i < 10000 ; i++){
            meta_manager_->par_size_per_table[i] = partition_size_cfg;
            // 暂时先让 FSM 和 BLink 的页面分区大小为 100
            meta_manager_->par_size_per_table[i + 10000] = 100;
            meta_manager_->par_size_per_table[i + 20000] = 100;
        }

        local_buffer_pools = std::vector<BufferPool*>(30000 , nullptr);
        for(int table_id = 0; table_id < table_num ; table_id++) {
            assert(table_pool_size_cfg > 0 && blink_buffer_pool_cfg > 0 && fsm_pool_size_cfg > 0);
            // 表本体
            pool_size_per_table = table_pool_size_cfg;
            pool_size_per_blink = blink_buffer_pool_cfg;
            pool_size_per_fsm = fsm_pool_size_cfg;
            local_buffer_pools[table_id] = new BufferPool(table_pool_size_cfg , 10000);
            // BLink 树
            local_buffer_pools[table_id + 10000] = new BufferPool(blink_buffer_pool_cfg , 10000);
            // FSM 
            local_buffer_pools[table_id + 20000] = new BufferPool(fsm_pool_size_cfg , 5000);
        }

        // 调度器，负责处理事务，先初始化，后续再往里边添加线程
        scheduler = new Scheduler("SQL_Scheduler");
    }

    ~ComputeNode(){
        delete local_page_lock_table;
        delete local_buffer_pool;
    }

    // 远程通知此节点释放数据页（不主动释放数据页策略下）
    int PendingPage(page_id_t page_id, bool xpending, table_id_t table_id) {
      int unlock_remote = 0;
      assert(lazy_local_page_lock_tables[table_id] != nullptr);
      return lazy_local_page_lock_tables[table_id]->GetLock(page_id)->Pending(node_id, xpending);
    }

    // 远程通知此节点获取页面控制权成功
    void NotifyPushPageSuccess(table_id_t table_id, page_id_t page_id) {
        assert(lazy_local_page_lock_tables[table_id] != nullptr);
        lazy_local_page_lock_tables[table_id]->GetLock(page_id)->RemotePushPageSuccess();
    }

    // 远程通知此节点获取页面控制权成功
    void NotifyLockPageSuccess(table_id_t table_id, page_id_t page_id, bool xlock , bool is_newest) {
        assert(lazy_local_page_lock_tables[table_id] != nullptr);
        lazy_local_page_lock_tables[table_id]->GetLock(page_id)->RemoteNotifyLockSuccess(xlock, is_newest);
    }


    Page *fetch_page(table_id_t table_id , page_id_t page_id){
        return local_buffer_pools[table_id]->fetch_page(page_id);
    }

    // 不知道缓冲区里有没有，尝试去缓冲区里拿一下页面
    Page *try_fetch_page(table_id_t table_id , page_id_t page_id){
        return local_buffer_pools[table_id]->try_fetch_page(page_id);
    }

    void unpin_page(table_id_t table_id , page_id_t page_id ){
        local_buffer_pools[table_id]->unpin_page(page_id);
    }

    std::string try_fetch_page_ret_string(table_id_t table_id , page_id_t page_id){
        return local_buffer_pools[table_id]->try_fetch_page_ret_string(page_id);
    }

    const std::atomic<int> &getFetchRemoteCnt() const {
        return fetch_remote_cnt;
    }


    inline int getNodeID() { return node_id; }

    inline LocalPageLockTable* getLocalPageLockTable() { return local_page_lock_table; }
    inline LocalPageLockTable *getLocalPageLockTables(table_id_t table_id) {return local_page_lock_tables[table_id];}
    inline ERLocalPageLockTable* getEagerPageLockTable() { return eager_local_page_lock_table; }
    inline LRLocalPageLockTable* getLazyPageLockTable() { return lazy_local_page_lock_table; }
    inline LRLocalPageLockTable* getLazyPageLockTable(table_id_t table_id) { return lazy_local_page_lock_tables[table_id]; }
    inline BufferPool* getBufferPool() { return local_buffer_pool; }
    inline BufferPool* getBufferPoolByIndex(int index) { return local_buffer_pools[index]; }

public:
    inline int get_fetch_remote_cnt() {
        return fetch_remote_cnt.load();
    }
    inline int get_fetch_allpage_cnt() {
        return fetch_allpage_cnt.load();
    }
    inline int get_fetch_from_remote_cnt() {
        return fetch_from_remote_cnt.load();
    }
    inline int get_fetch_from_storage_cnt() {
        return fetch_from_storage_cnt.load();
    }
    inline int get_fetch_from_local_cnt() {
        return fetch_from_local_cnt.load();
    }
    inline int get_evict_page_cnt() {
        return evict_page_cnt.load();
    }
    inline int get_lock_remote_cnt() {
        return lock_remote_cnt.load();
    }
    inline int get_hit_delayed_release_lock_cnt() {
        return hit_delayed_release_lock_cnt.load();
    }
    inline std::vector<double>& get_latency_vec() {
        return latency_vec;
    }
    inline std::vector<std::pair<Page_request_info, double>>& get_latency_pair_vec() {
        return latency_pair_vec;
    }
    inline void add_partition_cnt(){
        partition_cnt++;
    }
    inline void add_global_cnt(){
        global_cnt++;
    }
    inline std::atomic<int>& get_partition_cnt(){
        return partition_cnt;
    }
    inline std::atomic<int>& get_global_cnt(){
        return global_cnt;
    }
    inline Phase get_phase(){
        return phase;
    }
    inline std::queue<Txn_request_info>& get_partitioned_txn_queue(){
        return partitioned_txn_queue;
    }
    inline std::queue<Txn_request_info>& get_global_txn_queue(){
        return global_txn_queue;
    }
    inline void setNodeRunning(bool running){
        is_running = running;
    }
    inline MetaManager* getMetaManager(){
        return meta_manager_;
    }

    node_id_t get_node_id() const {
        return node_id;
    }
    
    void set_page_dirty(table_id_t table_id , page_id_t page_id , bool flag){
        if (flag){
            dirty_pages.insert(std::make_pair(table_id , page_id));
        }else {
            dirty_pages.erase(std::make_pair(table_id , page_id));
        }
    }

    void set_page_dirty_hot(table_id_t table_id , page_id_t page_id , bool flag){
        if (flag){
            dirty_pages_hot.insert(std::make_pair(table_id , page_id));
        }else{
            dirty_pages.erase(std::make_pair(table_id , page_id));
        }
    }

    void waitRemoteOK(){
        std::unique_lock<std::mutex> lk(switch_mtx_hot);
        // assert(success_return == false);

        // 等待远程通知我，热点页面轮到你了
        switch_hot_cv.wait(lk , [this]{
            return success_return;
        });

        success_return = false;
    }
    void notifyRemoteOK(){
        success_return = true;
        switch_hot_cv.notify_one();
    }

    TsPhase getPhase() {
        RWMutex::ReadLock lock(rw_mutex);
        return ts_phase;
    }
    TsPhase getPhaseNoBlock() const {
        return ts_phase;
    }

    TsPhase getPhaseHotNoBlock() const {
        return ts_phase_hot;
    }

    Scheduler* getScheduler() {
        return scheduler;
    }

    // 交互模式下需要在使用 scheduler 之前确保已经初始化好 (简单构造器
    // 默认不会初始化非 SYSTEM_MODE 12/13 的 scheduler)
    // 注意: Scheduler::addThread / sql_run 内部 assert(m_name == "SQL_Scheduler")，
    // 因此这里直接复用同名调度器以走相同的 fiber 调度路径。
    void ensureScheduler(const std::string& name = "SQL_Scheduler") {
        if (scheduler == nullptr) {
            scheduler = new Scheduler(name);
        }
    }

    Scheduler* getPushPageScheduler(){
        return push_page_scheduler;
    }

    std::vector<int> getSchedulerThreadIds() {
        if (scheduler)
            return scheduler->getThreadIds();
        return std::vector<int>();
    }

    void setPhase(TsPhase val) {
        RWMutex::WriteLock lock(rw_mutex);
        ts_phase = val;
    }

    void setPhaseHot(TsPhase val){
        RWMutex::WriteLock lock(rw_mutex_hot);
        ts_phase_hot = val;
    }

    void addPhase(){
        RWMutex::WriteLock lock(rw_mutex);
        ts_cnt = (ts_cnt + 1) % ComputeNodeCount;
    }
    int get_ts_cnt() {
        RWMutex::ReadLock lock(rw_mutex);
        return ts_cnt;
    }


public:
    // DBMeta，缓存 DB 信息
    DBMeta db_meta;
    std::vector<std::string> table_names;


private:
    node_id_t node_id;

    brpc::Channel page_table_channel; //建立rpc通信的channel, 与remote_server
    brpc::Channel storage_channel; //建立rpc通信的channel, 与storage_server

    MetaManager* meta_manager_;

    // 本地数据结构
    LocalPageLockTable* local_page_lock_table = nullptr;
    ERLocalPageLockTable* eager_local_page_lock_table = nullptr;
    LRLocalPageLockTable* lazy_local_page_lock_table = nullptr;

    // 本地的缓冲池，每个表一个 BufferPool
    std::vector<BufferPool*> local_buffer_pools;
    BufferPool* local_buffer_pool;

    std::unordered_map<table_id_t ,int> buffer_pool_index;
    std::vector<LocalPageLockTable*> local_page_lock_tables;
    std::vector<ERLocalPageLockTable*> eager_local_page_lock_tables;
    std::vector<LRLocalPageLockTable*> lazy_local_page_lock_tables;

    // for phase switch
    std::queue<Page_request_info> partitioned_page_queue;  // 访问本地逻辑分区的数据页
    std::queue<Page_request_info> global_page_queue;  // 访问全局逻辑分区的数据页
    std::queue<Txn_request_info> partitioned_txn_queue;  // 访问本地逻辑分区的事务
    std::queue<Txn_request_info> global_txn_queue;  // 访问全局逻辑分区的事务

    // 时间片轮转 SYSTEM_MODE = 12 和 13 用的
    Phase phase = Phase::BEGIN;
    bool is_running = true;
    Scheduler* scheduler = nullptr;

    Scheduler *push_page_scheduler = nullptr;
    

public:
    bool* threads_switch; //每个线程的阶段切换状态，数组
    bool* threads_finish; //每个线程的运行结束状态，数组
    bool is_phase_switch_finish = false;

public: // for star
    std::unordered_map<table_id_t, std::unordered_map<page_id_t, std::list<page_id_t>::iterator>*> local_page_set;
    std::unordered_map<table_id_t, std::list<page_id_t>*> lru_page_list;

public:
    std::condition_variable phase_switch_cv;
    std::mutex phase_switch_mutex;
    // phase switch 统计
    std::atomic<int> partition_cnt{0};
    std::atomic<int> global_cnt{0};
    std::atomic<int> stat_partition_cnt{0};
    std::atomic<int> stat_global_cnt{0};
    std::atomic<int> stat_commit_partition_cnt{0};
    std::atomic<int> stat_commit_global_cnt{0};
    std::atomic<int> stat_hit{0};
    std::atomic<int> stat_miss{0};
    double partition_tps = 0;
    double global_tps = 0;
    int partition_ms = EpochTime * (1-CrossNodeAccessRatio);
    int global_ms = EpochTime * CrossNodeAccessRatio;
    int max_par_txn_queue;
    int max_global_txn_queue;
    int epoch = 0;
    int pool_size_per_table = 100;
    int pool_size_per_blink = 100;
    int pool_size_per_fsm = 100;

    bool push_page_with_scheduler = false;
    int push_page_scheduler_threads = 3;

    // 时间戳阶段转化用的
    std::atomic<TsPhase> ts_phase{TsPhase::BEGIN};
    std::atomic<TsPhase> ts_phase_hot{TsPhase::BEGIN};
    std::atomic<int> ts_cnt{0};                 // 当前节点处于哪个时间片中
    std::atomic<int> ts_cnt_hot{0};
    RWMutex rw_mutex;
    RWMutex rw_mutex_hot;      
    std::condition_variable ts_switch_cond;
    int ts_time = 1000000;  // 默认 100ms
    // TS: 统计当前正在进行中的 fetch 数，切片切换时等待其归零以保证安全
    std::atomic<int> ts_inflight_fetch{0};
    std::atomic<int> ts_inflight_fetch_hot{0};
    std::mutex switch_mtx;
    std::condition_variable switch_hot_cv;
    bool success_return = false;
    std::mutex switch_mtx_hot;
    std::set<std::pair<table_id_t , page_id_t>> dirty_pages;
    std::set<std::pair<table_id_t , page_id_t>> dirty_pages_hot;

    // for delay release
    bool check_delay_release_finish = false;
    bool release_delay_lock_finish = false;

    // 统计信息
    std::atomic<int> fetch_remote_cnt;
    std::atomic<int> fetch_allpage_cnt;
    std::atomic<int> fetch_from_remote_cnt;
    std::atomic<int> fetch_from_storage_cnt;
    std::atomic<int> fetch_from_local_cnt;
    std::atomic<int> evict_page_cnt;
    
private:
    std::atomic<int> lock_remote_cnt;
    std::atomic<int> hit_delayed_release_lock_cnt;
    std::vector<double> latency_vec;
    std::vector<std::pair<Page_request_info, double>> latency_pair_vec;
};
