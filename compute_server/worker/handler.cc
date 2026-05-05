// Author: Chunyue Huang
// Copyright (c) 2024

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <future>
#include <sched.h>
#include <unistd.h>
#include <vector>
#include <cstring>

#include "config.h"
#include "handler.h"
#include "compute_server/server.h"
#include "connection/meta_manager.h"
#include "cache/index_cache.h"
#include "util/json_config.h"
#include "worker.h"
#include "fiber/scheduler.h"
#include "workload/ycsb/ycsb_db.h"

#include "affinity/aggregator.h"
#include "affinity/affinity_timeseries.h"
#include "affinity/edge_shuffler.h"
#include "affinity/migration_worker.h"
#include "affinity/partitioner.h"
#include "affinity/sample_buffer.h"
#include "affinity/sidecar_supervisor.h"

#define LISTEN_PORT_BEGIN 9095

std::atomic<uint64_t> tx_id_generator;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> ab_rate;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::set<double> fetch_remote_vec;
std::set<double> fetch_all_vec;
std::set<double> lock_remote_vec;
std::set<double> fetch_from_remote_vec;
std::set<double> fetch_from_storage_vec;
std::set<double> fetch_from_local_vec;
std::set<double> evict_page_vec;
std::set<double> total_outputs;
std::vector<double> lock_durations;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;
double all_time = 0;
double tx_begin_time = 0,tx_exe_time = 0,tx_fetch_exe_time = 0,tx_commit_time = 0,tx_abort_time = 0,tx_update_time = 0,tx_commit_fetch_page_time = 0;
double tx_get_timestamp_time1=0, tx_get_timestamp_time2=0, tx_write_commit_log_time=0, tx_write_commit_log_time2=0, tx_write_prepare_log_time=0, tx_write_backup_log_time=0;
double TxWaitAbortLogTime = 0;
int single_txn =0, distribute_txn=0;
int hybrid_2pc_commit_count = 0, hybrid_lazy_commit_count = 0;

std::atomic<int64_t> global_commit_log_count{0};
std::atomic<int64_t> global_prepare_log_count{0};
std::atomic<int64_t> global_backup_log_count{0};

namespace {

node_id_t g_bench_machine_id_override = INVALID_NODE_ID;

bool IsSmallBankBench(const std::string& bench_name) {
  return bench_name == "smallbank" || bench_name == "smallbank_aff";
}

bool EnvEnabled(const char* name) {
  const char* value = std::getenv(name);
  return value != nullptr && std::string(value) != "0";
}

int SelectWorkerCpu(node_id_t machine_id, t_id_t thread_idx, t_id_t thread_num_per_machine) {
  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (sched_getaffinity(0, sizeof(cpu_set_t), &allowed) != 0) {
    return static_cast<int>(thread_idx);
  }

  int allowed_count = CPU_COUNT(&allowed);
  if (allowed_count <= 0) {
    return static_cast<int>(thread_idx);
  }

  const bool offset_by_machine = EnvEnabled("WOOKONG_CPU_OFFSET_BY_MACHINE");
  int target_ordinal = static_cast<int>(thread_idx);
  if (offset_by_machine) {
    target_ordinal += static_cast<int>(machine_id) * static_cast<int>(thread_num_per_machine);
  }
  target_ordinal %= allowed_count;

  int ordinal = 0;
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (!CPU_ISSET(cpu, &allowed)) {
      continue;
    }
    if (ordinal == target_ordinal) {
      return cpu;
    }
    ++ordinal;
  }
  return static_cast<int>(thread_idx);
}

void StartAffinityRuntimeIfEnabled(ComputeServer* compute_server,
                                   const std::vector<std::string>& compute_ips,
                                   node_id_t machine_id,
                                   std::vector<std::thread>& affinity_threads) {
  if (!enable_affinity) {
    return;
  }

  affinity::Init();
  LOG(WARNING) << "[affinity] migration is non-recoverable: do not kill -9"
               << " mid-experiment (BLink is not WAL-persistent).";
  affinity::SpawnSidecarsIfLeader(compute_ips, machine_id);
  affinity_threads.emplace_back([compute_server] { affinity::AggregatorLoop(compute_server); });
  affinity_threads.emplace_back([compute_server] { affinity::EdgeShufflerLoop(compute_server); });
  affinity_threads.emplace_back([compute_server] { affinity::PartitionerLoop(compute_server); });
  affinity_threads.emplace_back([compute_server] { affinity::MigrationLoop(compute_server); });
  affinity_threads.emplace_back([compute_server] { affinity::TimeseriesLoop(compute_server); });
}

void StopAffinityRuntimeIfEnabled(std::vector<std::thread>& affinity_threads) {
  if (!enable_affinity) {
    return;
  }

  affinity::RequestAggregatorStop();
  affinity::RequestShufflerStop();
  affinity::RequestPartitionerStop();
  affinity::RequestMigrationStop();
  affinity::RequestTimeseriesStop();
  affinity::StopSidecars();
  for (auto& t : affinity_threads) {
    if (t.joinable()) {
      t.join();
    }
  }
}

}  // namespace

void Handler::ConfigureComputeNodeRunSQL(){
  // 配置节点数量
  std::string config_file = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_file);
  auto local_compute_node = json_config.get("local_compute_node");
  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  ComputeNodeCount = static_cast<int>(remote_compute_ips.size());
  assert(ComputeNodeCount > 0);
  auto parallel_page_fetch = local_compute_node.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
  }
  auto tuple_conflict_precheck = local_compute_node.get("tuple_conflict_precheck");
  if (tuple_conflict_precheck.exists() && tuple_conflict_precheck.is_int64()) {
    TUPLE_CONFLICT_PRECHECK = (int)tuple_conflict_precheck.get_int64();
  }

  // 目前 SQL 模式只支持 lazy_release 策略
  SYSTEM_MODE = 1;
}


void Handler::ConfigureComputeNodeRunBench(int argc, char* argv[]) {
  std::string config_file = "../../config/compute_node_config.json";
  std::string system_name = std::string(argv[2]);

  // 根据输入的参数，配置一些东西
  if (argc == 7) {
    std::string s2 = "sed -i 's/^[[:space:]]*\"thread_num_per_machine\".*/    \"thread_num_per_machine\": " + std::string(argv[3]) + ",/' " + config_file;
    thread_num_per_node = std::stoi(argv[3]);
    // 这里的协程数量目前没啥用，设置为 1 即可
    std::string s3 = "sed -i 's/^[[:space:]]*\"coroutine_num\".*/    \"coroutine_num\": 1,/' " + config_file;
    system(s2.c_str());
    system(s3.c_str());
    WR_TXN_RATE = std::stod(argv[4]);
    LOCAL_TRASACTION_RATE = std::stod(argv[5]);
    CrossNodeAccessRatio = 1 - LOCAL_TRASACTION_RATE;

    g_bench_machine_id_override = static_cast<node_id_t>(std::stoi(argv[6]));

    // 如果是 YCSB 负载，那修改 config 文件，来修改只读事务的比例
    if (std::string(argv[1]) == "ycsb") {
        std::string ycsb_config = "../../config/ycsb_config.json";
        int write_p = (int)(WR_TXN_RATE * 100);
        int read_p = 100 - write_p;
        // 注意保留缩进和逗号
        std::string s_read = "sed -i 's/^[[:space:]]*\"read_percent\".*/    \"read_percent\": " + std::to_string(read_p) + ",/' " + ycsb_config;
        std::string s_write = "sed -i 's/^[[:space:]]*\"write_percent\".*/    \"write_percent\": " + std::to_string(write_p) + ",/' " + ycsb_config;
        
        // 执行 sed 命令
        int ret1 = system(s_read.c_str());
        int ret2 = system(s_write.c_str());
        if (ret1 != 0 || ret2 != 0) {
            std::cerr << "Failed to update ycsb_config.json" << std::endl;
            assert(false);
        }
    }
  }

  // Customized test without modifying configs
  int txn_system_value = 0;
  if (system_name.find("eager") != std::string::npos) {
    txn_system_value = 0;
  } else if (system_name.find("lazy") != std::string::npos) {
    txn_system_value = 1;
  } else if (system_name.find("2pc") != std::string::npos) {
    txn_system_value = 2;
  } else if (system_name.find("single") != std::string::npos) {
    txn_system_value = 3;
  } else if (system_name.find("ts_sep_hot") != std::string::npos){
    txn_system_value = 13;
  } else if (system_name.find("ts_sep") != std::string::npos) {
    txn_system_value = 12;
  } else if (system_name.find("mix") != std::string::npos){
    // lazy 和 2pc 混合的模式
    txn_system_value = 4;
  }else {
    assert(false);
  }
  SYSTEM_MODE = txn_system_value;

  // read compute node count
  auto json_config = JsonConfig::load_file(config_file);
  auto local_compute_node = json_config.get("local_compute_node");
  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  ComputeNodeCount = static_cast<int>(remote_compute_ips.size());
  assert(ComputeNodeCount > 0);
  auto parallel_page_fetch = local_compute_node.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
  }
  auto tuple_conflict_precheck = local_compute_node.get("tuple_conflict_precheck");
  if (tuple_conflict_precheck.exists() && tuple_conflict_precheck.is_int64()) {
    if (SYSTEM_MODE == 1) TUPLE_CONFLICT_PRECHECK = (int)tuple_conflict_precheck.get_int64();
  }

  
  std::cout << "SYSTEM_MODE = " << SYSTEM_MODE << "\n";
  std::cout << "TUPLE_CONFLICT_PRECHECK = " << TUPLE_CONFLICT_PRECHECK << "\n";
  std::string s = "sed -i 's/^[[:space:]]*\"txn_system\".*/    \"txn_system\": " + std::to_string(txn_system_value) + ",/' " + config_file;
  system(s.c_str());
  return;
}

void Handler::StartDatabaseSQL(node_id_t node_id , int thread_num, int sys_mode , const std::string db_name){
  WORKLOAD_MODE = 4;
  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  node_id_t machine_num = static_cast<node_id_t>(remote_compute_ips.size());  // 节点数量
  assert(machine_num > 0);
  auto parallel_page_fetch = client_conf.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
  }
  auto tuple_conflict_precheck = client_conf.get("tuple_conflict_precheck");
  if (tuple_conflict_precheck.exists() && tuple_conflict_precheck.is_int64()) {
    TUPLE_CONFLICT_PRECHECK = (int)tuple_conflict_precheck.get_int64();
  }

  assert(node_id >= 0 && node_id < machine_num);
  tx_id_generator = 0;

  auto thread_arr = new std::thread[thread_num];
  auto* index_cache = new IndexCache();
  auto* page_cache = new PageCache();
  auto* global_meta_man = new MetaManager("", index_cache , page_cache , node_id , sys_mode);
  auto* param_arr = new struct thread_params[thread_num];

  std::string remote_server_ip = global_meta_man->remote_server_nodes[0].ip;
  int remote_server_port = global_meta_man->remote_server_nodes[0].port;
  std::string remote_storage_ip = global_meta_man->remote_storage_nodes[0].ip;
  int remote_storage_port = global_meta_man->remote_storage_nodes[0].port;

  auto* compute_node = new ComputeNode(node_id, remote_server_ip, remote_server_port, global_meta_man , db_name , thread_num);
  
  std::vector<std::string> compute_ips(machine_num);
  std::vector<int> compute_ports(machine_num);
  for (node_id_t i = 0; i < machine_num; i++) {
    compute_ips[i] = global_meta_man->remote_compute_nodes[i].ip;
    compute_ports[i] = global_meta_man->remote_compute_nodes[i].port;
  }

  auto* compute_server = new ComputeServer(compute_node, compute_ips, compute_ports);  

  sleep(3);

  socket_start_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);

  int server_fd, new_socket;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);

  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
      perror("socket failed");
      exit(EXIT_FAILURE);
  }

  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
      perror("setsockopt");
      exit(EXIT_FAILURE);
  }
  int node_port = LISTEN_PORT_BEGIN + node_id;
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(node_port);

  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
      perror("bind failed");
      exit(EXIT_FAILURE);
  }

  if (listen(server_fd, 20) < 0) {
      perror("listen");
      exit(EXIT_FAILURE);
  }

  // 启动后台线程：自适应刷新策略（1000条日志或100ms触发）
  std::thread log_flush_thread([compute_server]() {
      
      while (true) {
          // 等待触发信号或超时 
          compute_server->WaitLogFlushTrigger(compute_server->GetLogFlushIntervalMs());
          
          compute_server->LogFlush();
      }
      
      // 线程退出前最后一次刷新
      compute_server->LogFlush();
      std::cout << "Log flush thread terminated";
  });
  log_flush_thread.detach();

  std::cout << ">>> Server listening on " << node_port << ". Mode: One Thread Per Connection." << std::endl;

  while(true){
    // 等待新连接进来
    if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
        perror("accept");
        continue;
    }    
    std::cout << "New Connection!\n";
    // 用于生成全局唯一的线程 ID (在多机环境下通常配合 machine_id)
    // 这里作为简单的累加计数器
    std::atomic<int> dynamic_thread_counter(0);

    int thread_id = compute_node->getScheduler()->addThread();
    thread_params param;
    param.thread_global_id = (node_id * thread_num) + thread_id;
    param.thread_id = thread_id;
    param.machine_id = node_id;
    param.coro_num = 1;
    param.bench_name = "";
    param.index_cache = index_cache;
    param.page_cache = page_cache;
    param.global_meta_man = global_meta_man;
    param.compute_server = compute_server;
    param.thread_num_per_machine = thread_num;
    param.total_thread_num = thread_num * machine_num;

    std::atomic<bool> init_finish(false);
    auto task = [&](){
      initThread(&param , nullptr , nullptr , nullptr);
      init_finish = true;
    };

    compute_node->getScheduler()->schedule(task , thread_id);
    // 等线程初始化完成
    while(!init_finish){
      usleep(10);
    }

    compute_node->getScheduler()->schedule([new_socket](){
      RunSQL(new_socket);
      close(new_socket);
      // 连接断开后，需要回收线程资源
      Scheduler::setJobFinish(true);
    }, thread_id);
  }

  socket_finish_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);
}

// =====================================================================
// 交互式负载模式 (WORKLOAD_MODE == 5)
// 与 StartDatabaseSQL 类似:
//   - 监听 TCP 端口，每个连接绑定一个独立的工作线程
//   - 该线程跑一个长事务会话(由用户通过 begin/commit/abort 控制)
//   - 但分发的 worker 是 RunInteractiveYCSB，而不是 RunSQL
// 监听端口为 INTERACTIVE_LISTEN_PORT_BEGIN + node_id, 避免与 SQL 模式冲突。
// =====================================================================
#define INTERACTIVE_LISTEN_PORT_BEGIN 9115
void Handler::StartInteractiveBench(node_id_t node_id , int thread_num , int sys_mode , const std::string bench_name){
  // 当前仅支持 ycsb / smallbank
  if (bench_name != "ycsb" && bench_name != "smallbank") {
    LOG(FATAL) << "[Interactive] Unsupported bench_name=" << bench_name
               << " (supported: ycsb, smallbank)";
    assert(false);
  }

  // 复用 benchmark 的 WORKLOAD_MODE 编码：smallbank=0, ycsb=2。
  // 这样 ComputeServer::InitTableNameMeta 等下游逻辑可以直接走 benchmark 路径，
  // 不需要再为 "interactive" 单独维护一份表名映射。
  if (bench_name == "smallbank") {
    WORKLOAD_MODE = 0;
  } else {
    WORKLOAD_MODE = 2;
  }
  std::cout << "[Interactive] WORKLOAD_MODE = " << WORKLOAD_MODE
            << " bench=" << bench_name << " sys_mode=" << sys_mode << "\n";

  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  node_id_t machine_num = static_cast<node_id_t>(remote_compute_ips.size());
  assert(machine_num > 0);
  auto parallel_page_fetch = client_conf.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
  }
  auto tuple_conflict_precheck = client_conf.get("tuple_conflict_precheck");
  if (tuple_conflict_precheck.exists() && tuple_conflict_precheck.is_int64()) {
    TUPLE_CONFLICT_PRECHECK = (int)tuple_conflict_precheck.get_int64();
  }

  assert(node_id >= 0 && node_id < machine_num);
  tx_id_generator = 0;

  auto* index_cache = new IndexCache();
  auto* page_cache = new PageCache();
  // bench_name 传给 MetaManager 后会触发 PrefetchIndex(0) 等元数据加载
  auto* global_meta_man = new MetaManager(bench_name, index_cache , page_cache , node_id , sys_mode);

  std::string remote_server_ip = global_meta_man->remote_server_nodes[0].ip;
  int remote_server_port = global_meta_man->remote_server_nodes[0].port;

  // 注意：交互模式走与 benchmark 相同的轻量构造器(不调用 OpenDb)。
  // 因为 ycsb / smallbank 的表是由存储层在启动阶段直接加载的(load_table)，
  // 不存在于 sm_manager 的 SQL 字典里；调用 OpenDb 只会建出空库导致后续访问越界。
  auto* compute_node = new ComputeNode(node_id, remote_server_ip, remote_server_port, global_meta_man);
  // 简单构造器只在 SYSTEM_MODE==12/13 时初始化 scheduler，这里强制确保有一个可用的
  compute_node->ensureScheduler("SQL_Scheduler");

  std::vector<std::string> compute_ips(machine_num);
  std::vector<int> compute_ports(machine_num);
  for (node_id_t i = 0; i < machine_num; i++) {
    compute_ips[i] = global_meta_man->remote_compute_nodes[i].ip;
    compute_ports[i] = global_meta_man->remote_compute_nodes[i].port;
  }

  auto* compute_server = new ComputeServer(compute_node, compute_ips, compute_ports);
  std::vector<std::thread> affinity_threads;
  StartAffinityRuntimeIfEnabled(compute_server, compute_ips, node_id, affinity_threads);

  sleep(3);

  socket_start_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);

  // 监听 socket
  int server_fd, new_socket;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);

  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket failed");
    exit(EXIT_FAILURE);
  }
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
    perror("setsockopt");
    exit(EXIT_FAILURE);
  }

  int node_port = INTERACTIVE_LISTEN_PORT_BEGIN + node_id;
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(node_port);

  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
    perror("bind failed");
    exit(EXIT_FAILURE);
  }
  if (listen(server_fd, 20) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }

  // 后台日志刷新线程(与 SQL 模式相同)
  std::thread log_flush_thread([compute_server]() {
    while (true) {
      compute_server->WaitLogFlushTrigger(compute_server->GetLogFlushIntervalMs());
      compute_server->LogFlush();
    }
    compute_server->LogFlush();
    std::cout << "Log flush thread terminated";
  });
  log_flush_thread.detach();

  // 每节点最大并发连接数；超过则立刻回 BUSY 并关闭。
  // 默认 200，可通过环境变量 INTERACTIVE_MAX_CONN 覆盖。
  int max_conn = 200;
  if (const char* env = std::getenv("INTERACTIVE_MAX_CONN")) {
    int v = std::atoi(env);
    if (v > 0) max_conn = v;
  }
  static std::atomic<int> active_conn_cnt{0};

  std::cout << ">>> Interactive bench server listening on " << node_port
            << ". One Thread Per Connection. max_conn=" << max_conn << std::endl;

  while (true) {
    if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
      perror("accept");
      continue;
    }

    // 连接数限流：若当前活跃连接已达上限，回 BUSY 后立刻关闭
    int cur = active_conn_cnt.fetch_add(1) + 1;
    if (cur > max_conn) {
      active_conn_cnt.fetch_sub(1);
      const char* busy_msg = "BUSY: too many connections, max=";
      std::string msg = std::string(busy_msg) + std::to_string(max_conn) + "\n";
      send(new_socket, msg.c_str(), msg.size(), 0);
      close(new_socket);
      std::cout << "[Interactive] connection rejected (limit " << max_conn << ")\n";
      continue;
    }
    std::cout << "[Interactive] new connection (active=" << cur << "/" << max_conn << ")\n";

    int thread_id = compute_node->getScheduler()->addThread();
    thread_params param;
    param.thread_global_id = (node_id * thread_num) + thread_id;
    param.thread_id = thread_id;
    param.machine_id = node_id;
    param.coro_num = 1;
    // 注意: 这里特意置为空字符串，让 initThread 走「啥都不做」的兜底分支，
    // 避免触发 ycsb_client/zipfan_gens 等针对 benchmark 模式的初始化。
    // 交互模式中 key 由用户显式传入，不需要这些。
    param.bench_name = "";
    param.index_cache = index_cache;
    param.page_cache = page_cache;
    param.global_meta_man = global_meta_man;
    param.compute_server = compute_server;
    param.thread_num_per_machine = thread_num;
    param.total_thread_num = thread_num * machine_num;

    std::atomic<bool> init_finish(false);
    auto init_task = [&](){
      initThread(&param , nullptr , nullptr , nullptr);
      init_finish = true;
    };
    compute_node->getScheduler()->schedule(init_task , thread_id);
    while (!init_finish) usleep(10);

    compute_node->getScheduler()->schedule([new_socket, bench_name](){
      RunInteractiveBench(new_socket, bench_name);
      close(new_socket);
      active_conn_cnt.fetch_sub(1);
      Scheduler::setJobFinish(true);
    }, thread_id);
  }

  socket_finish_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);
}

void Handler::GenThreads(std::string bench_name) {
  if (IsSmallBankBench(bench_name)) {
      WORKLOAD_MODE = 0;
  } else if(bench_name == "tpcc") {
      WORKLOAD_MODE = 1;
  } else if (bench_name == "ycsb"){
      WORKLOAD_MODE = 2;
  }else {
      LOG(FATAL) << "Unsupported benchmark name: " << bench_name;
      assert(false);
  }

  std::cout << "WORKLOAD_MODE = " << WORKLOAD_MODE << "\n";
  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  node_id_t machine_num = static_cast<node_id_t>(remote_compute_ips.size());
  assert(machine_num > 0);
  node_id_t machine_id = (g_bench_machine_id_override != INVALID_NODE_ID)
      ? g_bench_machine_id_override
      : (node_id_t)client_conf.get("machine_id").get_int64();
  auto parallel_page_fetch = client_conf.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
  }
  auto tuple_conflict_precheck = client_conf.get("tuple_conflict_precheck");
  if (tuple_conflict_precheck.exists() && tuple_conflict_precheck.is_int64()) {
    TUPLE_CONFLICT_PRECHECK = (int)tuple_conflict_precheck.get_int64();
  }
  std::cout << "starting primary , machine id = " << machine_id << " machine num = " << machine_num << "\n";
  t_id_t thread_num_per_machine = (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    thread_num_per_machine++;
  }
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();

  LOCAL_BATCH_TXN_SIZE = (int)client_conf.get("batch_size").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);

  /* Start working */
  tx_id_generator = 0;  // Initial transaction id == 0

  // ljTag
  auto thread_arr = new std::thread[thread_num_per_machine];
  auto* index_cache = new IndexCache();
  auto* page_cache = new PageCache();
  auto* global_meta_man = new MetaManager(bench_name, index_cache , page_cache , machine_id , SYSTEM_MODE);
  auto* param_arr = new struct thread_params[thread_num_per_machine];

  // Create a compute node object
  std::string remote_server_ip = global_meta_man->remote_server_nodes[0].ip;
  int remote_server_port = global_meta_man->remote_server_nodes[0].port;
  std::string remote_storage_ip = global_meta_man->remote_storage_nodes[0].ip;
  int remote_storage_port = global_meta_man->remote_storage_nodes[0].port;

  auto* compute_node = new ComputeNode(machine_id, remote_server_ip, remote_server_port, global_meta_man);
  std::vector<std::string> compute_ips(machine_num);
  std::vector<int> compute_ports(machine_num);
  for (node_id_t i = 0; i < machine_num; i++) {
    compute_ips[i] = global_meta_man->remote_compute_nodes[i].ip;
    compute_ports[i] = global_meta_man->remote_compute_nodes[i].port;
  }

  auto* compute_server = new ComputeServer(compute_node, compute_ips, compute_ports);
  std::vector<std::thread> affinity_threads;

  if (compute_server->IsLogEnabled()) {
    std::thread log_flush_thread([compute_server]() {
      while (compute_server->log_flush_running.load()) {
          compute_server->WaitLogFlushTrigger(compute_server->GetLogFlushIntervalMs());
          compute_server->LogFlush();
      }
      compute_server->LogFlush();
      std::cout << "Log flush thread terminated";
    });
    log_flush_thread.detach();
  }

  StartAffinityRuntimeIfEnabled(compute_server, compute_ips, machine_id, affinity_threads);

  // ComputeServer 启动是用另外一个线程启动的， 这里等待一下启动
  if (WORKLOAD_MODE == 0){
    std::this_thread::sleep_for(std::chrono::seconds(10)); 
  }else if (WORKLOAD_MODE == 1){
    // TPCC needs more time to initialize tables (check table_exist for 11 tables)
    std::this_thread::sleep_for(std::chrono::seconds(40)); 
  }else if (WORKLOAD_MODE == 2){
    std::this_thread::sleep_for(std::chrono::seconds(5)); 
  }else {
    assert(false);
  }


  // Send TCP requests to remote servers here, and the remote server establishes a connection with the compute node
  socket_start_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);
  // std::cout << "finish start client\n";
  
  SmallBank* smallbank_client = nullptr;
  TPCC* tpcc_client = nullptr;
  YCSB *ycsb_client = nullptr;

  if (IsSmallBankBench(bench_name)) {
    std::string config_path = "../../config/" + bench_name + "_config.json";
    auto config = JsonConfig::load_file(config_path);
    auto conf = config.get(bench_name);
    int hot_rate = conf.get("num_hot_rate").get_int64();
    int use_zipfian = conf.get("use_zipfian").get_int64();
    bool enable_workload_affinity = bench_name == "smallbank_aff";
    double affinity_txn_ratio = conf.get("affinity_txn_ratio").get_double(0.0);

    assert(hot_rate > 0 && hot_rate < 100);
    smallbank_client = new SmallBank(nullptr,
                                     hot_rate,
                                     use_zipfian,
                                     enable_workload_affinity,
                                     affinity_txn_ratio,
                                     bench_name);
    total_try_times.resize(SmallBank_TX_TYPES, 0);
    total_commit_times.resize(SmallBank_TX_TYPES, 0);
  } else if(bench_name == "tpcc") {
    tpcc_client = new TPCC(nullptr);
    total_try_times.resize(TPCC_TX_TYPES, 0);
    total_commit_times.resize(TPCC_TX_TYPES, 0);
  } else if (bench_name == "ycsb"){
    std::string config_path = "../../config/ycsb_config.json";
    auto config = JsonConfig::load_file(config_path);
    int record_cnt = config.get("ycsb").get("num_record").get_int64();
    int hot_cnt = config.get("ycsb").get("num_hot_record").get_int64();
    int use_zipfian = config.get("ycsb").get("use_zipfian").get_int64();
    int read_cnt = config.get("ycsb").get("read_percent").get_int64();
    int write_cnt = config.get("ycsb").get("write_percent").get_int64();
    int field_len = config.get("ycsb").get("field_len").get_int64();
    int tx_hot_rate = config.get("ycsb").get("TX_HOT").get_int64();
    double zipf_theta = config.get("ycsb").get("zipf_theta").get_double(0.70);
    assert(hot_cnt < record_cnt);
    assert(use_zipfian == 1 || use_zipfian == 0);
    assert(read_cnt + write_cnt == 100);
    assert(field_len > 0);
    assert(tx_hot_rate > 0 && tx_hot_rate < 100);
    assert(zipf_theta == -1.0 || (zipf_theta >= 0.0 && zipf_theta < 1.0) || zipf_theta >= 40.0);
    std::vector<int> page_num_per_node;
    std::vector<int> node_key_counts;
    auto* mm = compute_server->get_node()->getMetaManager();
    for (int i = 0 ; i < ComputeNodeCount ; i++){
      page_num_per_node.emplace_back(mm->GetPageNumPerNode(i , 0 , ComputeNodeCount));
      // 真实 key 数量（来自 PrefetchIndex 后构建的节点 key 列表），用于 zipfian 的 n
      node_key_counts.emplace_back((int)mm->GetNodeKeys(0, i).size());
    }
    bool random_generate = mm->GetRandomGenerate();
    ycsb_client = new YCSB(nullptr , record_cnt , hot_cnt , use_zipfian , page_num_per_node , node_key_counts , read_cnt , write_cnt , field_len , tx_hot_rate , zipf_theta , random_generate);
    total_try_times.resize(YCSB_TX_TYPES, 0);
    total_commit_times.resize(YCSB_TX_TYPES, 0);
  }else {
    LOG(FATAL) << "Unsupported benchmark name: " << bench_name;
  }


  std::atomic<int> init_finish_cnt(0);
  t_id_t i = 0;
  
  for (; i < thread_num_per_machine; i++) { 
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].thread_id = i;
    param_arr[i].machine_id = machine_id;
    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].index_cache = index_cache;
    param_arr[i].page_cache = page_cache;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].compute_server = compute_server;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = thread_num_per_machine * machine_num;
    if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
      std::vector<int> thread_ids = compute_node->getSchedulerThreadIds();
      if (i < thread_ids.size()) {
          // 先初始化一下调度器里面的每一个线程
          auto task = [&, i](){
            initThread(&param_arr[i] , smallbank_client , tpcc_client , ycsb_client);
            init_finish_cnt++;
          };
          compute_node->getScheduler()->schedule(task , thread_ids[i]);
          // std::cout << "Thread ID " << i << " = " << thread_ids[i] << "\n";
      } else {
          assert(false);
      }
    }else{
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  smallbank_client,
                                  tpcc_client,
                                  ycsb_client);
      /* Pin thread i to hardware thread i */
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      const int worker_cpu = SelectWorkerCpu(machine_id, i, thread_num_per_machine);
      CPU_SET(worker_cpu, &cpuset);
      int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
      if (rc != 0) {
        LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
      }
    }
  }
  
  if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3 || SYSTEM_MODE == 4){
    for (t_id_t i = 0; i < thread_num_per_machine; i++) {
      if (thread_arr[i].joinable()) {
        thread_arr[i].join();
        std::cout << "thread " << i << " joined" << std::endl;
      }
    }
  } else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    // 等待协程调度器里面的线程池中的每个线程初始化
    while (init_finish_cnt < thread_num_per_machine ){
      usleep(1000);
    }
    compute_node->getScheduler()->schedule([compute_server]{
      compute_server->ts_switch_phase(compute_server->get_node()->ts_time);
    });
    if (SYSTEM_MODE == 13){
      compute_node->getScheduler()->schedule([compute_server]{
        // 先用 10ms 试试
        compute_server->ts_switch_phase_hot_new(20000);
      });
    }
    std::vector<int> thread_ids = compute_node->getSchedulerThreadIds();
    std::cout << "coro num = " << coro_num << "\n";
    compute_server->set_alive_fiber_cnt(coro_num);
    RunWorkLoad(compute_server, bench_name , -1 , coro_num);

    while (true){
      usleep(10000);
      if (compute_server->get_alive_fiber_cnt() == 0){
        break;
      }
    }

    // 最后，统计一下信息：
    for (int i = 0 ; i < thread_num_per_machine ; i++){
        auto task = [&](){
          CaculateInfo(compute_server);
          init_finish_cnt--;
        };
        compute_node->getScheduler()->schedule(task , thread_ids[i]);
        // std::cout << "Thread ID " << i << " = " << thread_ids[i] << "\n";
    }
    // compute_node->getScheduler()->stop();

    while(true){
      usleep(10000);
      if (init_finish_cnt == 0){
        break;
      }
    }
  }else {
    assert(false);
  }

  // Shut down the affinity pipeline BEFORE we start the cross-node finish
  // barrier. Migration worker touches BLink + FSM tree and fetches pages via
  // GPLM; if it keeps running past the workers' join, it races with the
  // shutdown sequence on this node (FSM "Could not find leaf page" warnings)
  // and can wedge on fetch RPCs once peers enter Shutdown() too. Quiescing
  // here — with StopSidecars before the joins so UDS reads in PartitionerLoop
  // unblock on EOF — keeps the rest of the shutdown path single-threaded.
  StopAffinityRuntimeIfEnabled(affinity_threads);

  std::cout << "All workers DONE, Waiting for all compute nodes to finish..." << std::endl;

  // 统计compute server中的统计信息
  tx_update_time = compute_server->tx_update_time;

  if(SYSTEM_MODE == 1){
    // 该线程结束, 释放持有的页锁
    // compute_server->rpc_lazy_release_all_page();
  }


  // Wait for all compute nodes to finish
  socket_finish_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);
  compute_server->Shutdown();
  bool is_shutdown_coordinator = true;
  if (!global_meta_man->remote_compute_nodes.empty()) {
    node_id_t min_node_id = global_meta_man->remote_compute_nodes[0].node_id;
    for (const auto& node : global_meta_man->remote_compute_nodes) {
      if (node.node_id < min_node_id) {
        min_node_id = node.node_id;
      }
    }
    is_shutdown_coordinator = (machine_id == min_node_id);
  }
  if (is_shutdown_coordinator) {
    if (!compute_server->ShutdownStorageServer()) {
      LOG(ERROR) << "Fail to shutdown storage server";
    }
    // The meta finish barrier above already tells remote_node that this round
    // is over. In non-interactive script runs the remote process exits on its
    // own after socket_finish_server(), so a second brpc shutdown RPC here
    // only races with a clean exit and creates noisy false errors.
  }

  std::cout << "All compute nodes have finished";

  std::ofstream result_file("delay_fetch_remote.txt");
  result_file << "fetch_all: " << *fetch_all_vec.rbegin() << std::endl;
  result_file << "fetch_remote: " << *fetch_remote_vec.rbegin() << std::endl;
  result_file << "lock_remote: " << *lock_remote_vec.rbegin() << std::endl;
  delete[] param_arr;
  delete global_meta_man;
  if (smallbank_client) delete smallbank_client;
  if(tpcc_client) delete tpcc_client;
}
