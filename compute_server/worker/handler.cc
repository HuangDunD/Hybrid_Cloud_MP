#include <bvar/passive_status.h>
// Author: Chunyue Huang
// Copyright (c) 2024

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>
#include <future>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <vector>
#include <cstring>

#include "config.h"
#include "common/rpc_transport.h"
#include "handler.h"
#include "dtx/benchmark_stats.h"
#include "compute_server/server.h"
#include "connection/meta_manager.h"
#include "cache/index_cache.h"
#include "util/json_config.h"
#include "worker.h"
#include "fiber/scheduler.h"
#include "workload/ycsb/ycsb_db.h"

#define LISTEN_PORT_BEGIN 9095

// 供 run.cc 汇总输出锁等待统计（WAIT_DETECT）用的全局句柄
ComputeServer* g_compute_server = nullptr;
std::atomic<uint64_t> tx_id_generator;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> ab_rate;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
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

// Shared reporting only. Call with workload workers stopped; worker-local DTX
// accumulators belong to the worker lifecycle and must also start fresh.
static void ResetBenchmarkStats(ComputeServer* server) {
  extern benchmark_stats::Totals benchmark_node_stats;
  server->ResetBenchmarkStats();
  benchmark_node_stats = {};
  tid_vec.clear();
  attemp_tp_vec.clear();
  tp_vec.clear();
  ab_rate.clear();
  medianlat_vec.clear();
  taillat_vec.clear();
  fetch_all_vec.clear();
  lock_remote_vec.clear();
  fetch_from_remote_vec.clear();
  fetch_from_storage_vec.clear();
  fetch_from_local_vec.clear();
  evict_page_vec.clear();
  total_outputs.clear();
  lock_durations.clear();
  std::fill(total_try_times.begin(), total_try_times.end(), 0);
  std::fill(total_commit_times.begin(), total_commit_times.end(), 0);
  all_time = 0;
  tx_begin_time = 0;
  tx_exe_time = 0;
  tx_fetch_exe_time = 0;
  tx_commit_time = 0;
  tx_abort_time = 0;
  tx_update_time = 0;
  tx_commit_fetch_page_time = 0;
  tx_get_timestamp_time1 = 0;
  tx_get_timestamp_time2 = 0;
  tx_write_commit_log_time = 0;
  tx_write_commit_log_time2 = 0;
  tx_write_prepare_log_time = 0;
  tx_write_backup_log_time = 0;
  TxWaitAbortLogTime = 0;
  single_txn = 0;
  distribute_txn = 0;
  hybrid_2pc_commit_count = 0;
  hybrid_lazy_commit_count = 0;
  global_commit_log_count = 0;
  global_prepare_log_count = 0;
  global_backup_log_count = 0;
}

static void LoadLockWaitConfig(const std::string& config_file) {
  auto json_config = JsonConfig::load_file(config_file);
  auto local_compute_node = json_config.get("local_compute_node");
  auto lock_mode = local_compute_node.get("lock_mode");
  if (lock_mode.exists() && lock_mode.is_int64()) {
    int mode = (int)lock_mode.get_int64();
    // 目前只支持两种：NO_WAIT + WAIT_DETECT
    if (mode != NO_WAIT && mode != WAIT_DETECT) {
      std::cerr << "[warn] unsupported lock_mode = " << mode << " in " << config_file
                << ", fallback to WAIT_DETECT" << std::endl;
      assert(false);
    }
    LOCK_MODE = mode;
  }
  auto wait_timeout = local_compute_node.get("lock_wait_timeout_ms");
  if (wait_timeout.exists() && wait_timeout.is_int64()) {
    LOCK_WAIT_TIMEOUT_MS = (int)wait_timeout.get_int64();
  }
  auto check_interval = local_compute_node.get("deadlock_check_interval_ms");
  if (check_interval.exists() && check_interval.is_int64()) {
    DEADLOCK_CHECK_INTERVAL_MS = (int)check_interval.get_int64();
  }
  std::cout << "LOCK_MODE = " << LOCK_MODE << " (0=NO_WAIT,1=WAIT_DETECT)"
            << " , deadlock_check_interval_ms = " << DEADLOCK_CHECK_INTERVAL_MS
            << " , lock_wait_timeout_ms(watchdog) = " << LOCK_WAIT_TIMEOUT_MS << std::endl;
}

void Handler::ConfigureComputeNodeRunSQL(){
  // 配置节点数量
  std::string config_file = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_file);
  ConfigureRpcTransport(json_config.get("use_rdma").get_bool(false));
  auto local_compute_node = json_config.get("local_compute_node");
  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  ComputeNodeCount = static_cast<int>(remote_compute_ips.size());
  assert(ComputeNodeCount > 0);
  assert(ComputeNodeCount <= MaxComputeNodeCount);
  auto parallel_page_fetch = local_compute_node.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
  }

  LoadLockWaitConfig(config_file);
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

    {
      std::string s1 = "sed -i 's/^[[:space:]]*\"machine_id\".*/    \"machine_id\": " + std::string(argv[6]) + ",/' " + config_file;
      system(s1.c_str());
    }

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
  } else if (system_name.find("mix") != std::string::npos){
    // lazy 和 2pc 混合的模式
    txn_system_value = 4;
  }else {
    assert(false);
  }
  SYSTEM_MODE = txn_system_value;

  // read compute node count
  auto json_config = JsonConfig::load_file(config_file);
  ConfigureRpcTransport(json_config.get("use_rdma").get_bool(false));
  auto local_compute_node = json_config.get("local_compute_node");
  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  ComputeNodeCount = static_cast<int>(remote_compute_ips.size());
  assert(ComputeNodeCount > 0);
  assert(ComputeNodeCount <= MaxComputeNodeCount);
  auto parallel_page_fetch = local_compute_node.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
  }
  LoadLockWaitConfig(config_file);

  
  std::cout << "SYSTEM_MODE = " << SYSTEM_MODE << "\n";
  std::string s = "sed -i 's/^[[:space:]]*\"txn_system\".*/    \"txn_system\": " + std::to_string(txn_system_value) + ",/' " + config_file;
  system(s.c_str());
  return;
}

namespace {
bool BootstrapSqlSend(int fd, const std::string& sql) {
    size_t offset = 0;
    while (offset < sql.size()) {
        const ssize_t sent = send(fd, sql.data() + offset, sql.size() - offset, 0);
        if (sent <= 0) return false;
        offset += static_cast<size_t>(sent);
    }
    return true;
}

bool BootstrapSqlExec(int fd, const std::string& sql) {
    if (!BootstrapSqlSend(fd, sql)) return false;
    char buffer[8192];
    const ssize_t received = recv(fd, buffer, sizeof(buffer) - 1, 0);
    if (received <= 0) return false;
    std::string reply(buffer, static_cast<size_t>(received));
    if (reply.find("fail") != std::string::npos ||
        reply.find("Abort") != std::string::npos ||
        reply.find("Syntax") != std::string::npos ||
        reply.find("error") != std::string::npos) {
        std::cerr << "[SQL] bootstrap statement failed: " << sql
                  << " -> " << reply << std::endl;
        return false;
    }
    return true;
}

// SQL mode is a simple single-process client/server. The listener is already
// active when this detached client runs; the normal accept loop serves it.
void BootstrapYcsbTable(int node_port) {
    int fd = -1;
    for (int attempt = 0; attempt < 50; ++attempt) {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) break;
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(node_port);
        CHECK(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
        if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0) break;
        close(fd);
        fd = -1;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (fd < 0) {
        std::cerr << "[SQL] cannot connect to local SQL server for YCSB bootstrap" << std::endl;
        return;
    }

    std::string create = "create table ycsb_user_table (id int, magic int";
    for (int field = 0; field < 10; ++field)
        // SQL's record-size limit (RM_MAX_RECORD_SIZE=1080) includes the
        // DataItem header; 10 x 90 bytes + magic still fits, whereas the
        // benchmark's non-SQL 10 x 100 value layout does not.
        create += ", file_" + std::to_string(field) + " char(90)";
    create += ", primary key(id));";
    if (!BootstrapSqlExec(fd, create) || !BootstrapSqlExec(fd, "BEGIN;")) {
        close(fd);
        return;
    }
    for (int row = 0; row < 1000; ++row) {
        std::string insert = "insert into ycsb_user_table values (" + std::to_string(row) + ", 125";
        for (int field = 0; field < 10; ++field) {
            insert += ", 'ycsb_" + std::to_string(row) + "_" + std::to_string(field) + "'";
        }
        insert += ");";
        if (!BootstrapSqlExec(fd, insert)) {
            BootstrapSqlExec(fd, "ABORT;");
            close(fd);
            return;
        }
    }
    if (!BootstrapSqlExec(fd, "COMMIT;")) {
        close(fd);
        return;
    }
    close(fd);
    std::cout << "[SQL] initialized ycsb_user_table with 1000 rows" << std::endl;
}
}  // namespace

void Handler::StartDatabaseSQL(node_id_t node_id , int thread_num, int sys_mode , const std::string db_name){
  WORKLOAD_MODE = 4;
  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  node_id_t machine_num = static_cast<node_id_t>(remote_compute_ips.size());  // 节点数量
  assert(machine_num > 0);
  // 同上：元组用 1 字节记录 holder 节点，故节点数上限 256（id 0 ~ 255）
  assert(machine_num <= MaxComputeNodeCount);
  auto parallel_page_fetch = client_conf.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
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
  g_compute_server = compute_server;
  // Expose the same engine counters to SQL integration tests and operators.
  bvar::PassiveStatus<uint64_t> sql_lsn("sql_wal_lsn", [](void* s) -> uint64_t {
      return static_cast<ComputeServer*>(s)->CurrentLsn();
  }, compute_server);
  bvar::PassiveStatus<uint64_t> sql_durable_lsn("sql_durable_wal_lsn", [](void* s) -> uint64_t {
      return static_cast<ComputeServer*>(s)->GetPersistedLSN();
  }, compute_server);
  bvar::PassiveStatus<uint64_t> sql_undo_pages("sql_undo_pages", [](void* s) -> uint64_t {
      return static_cast<ComputeServer*>(s)->Mvcc().Undo().AllocatedPages();
  }, compute_server);
  bvar::PassiveStatus<uint64_t> sql_undo_evictions("sql_undo_evictions", [](void* s) -> uint64_t {
      return static_cast<ComputeServer*>(s)->UndoEvictions();
  }, compute_server);
  bvar::PassiveStatus<uint64_t> sql_heap_evictions("sql_heap_evictions", [](void* s) -> uint64_t {
      return static_cast<ComputeServer*>(s)->main_data_evictions_.load();
  }, compute_server);
  bvar::PassiveStatus<uint64_t> sql_writer_queue_depth_max("sql_writer_queue_depth_max", [](void* s) -> uint64_t {
      return static_cast<ComputeServer*>(s)->lock_wait_mgr()->stat_wq_depth_max.load();
  }, compute_server);
  // The SQL mode has no benchmark warm-up phase. Make sure this node's RPC
  // server (used by create-table notifications and page-lock RPCs) is live
  // before the remote-server barrier and before bootstrap DDL.
  compute_server->WaitReady();

  sleep(3);

  compute_server->WaitForClusterStart();

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
  auto port_base = json_config.get("sql_listen_port_base");
  int node_port = (port_base.exists() ? port_base.get_int64() : LISTEN_PORT_BEGIN) + node_id;
  CHECK(node_port > 0 && node_port < 65536);
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

  if (node_id == 0) {
      if (!compute_server->table_exist("ycsb_user_table")) {
          std::cout << "[SQL] bootstrapping ycsb_user_table: 1000 rows" << std::endl;
          std::thread([node_port]() { BootstrapYcsbTable(node_port); }).detach();
      } else {
          std::cout << "[SQL] ycsb_user_table already exists; skip bootstrap" << std::endl;
      }
  }

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
  // 同上：元组用 1 字节记录 holder 节点，故节点数上限 256（id 0 ~ 255）
  assert(machine_num <= MaxComputeNodeCount);
  auto parallel_page_fetch = client_conf.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
  }
  LoadLockWaitConfig(config_filepath);   // 锁等待配置（与 benchmark/SQL 入口一致）

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
  // 简单构造器不会初始化 scheduler，这里强制确保有一个可用的
  compute_node->ensureScheduler("SQL_Scheduler");

  std::vector<std::string> compute_ips(machine_num);
  std::vector<int> compute_ports(machine_num);
  for (node_id_t i = 0; i < machine_num; i++) {
    compute_ips[i] = global_meta_man->remote_compute_nodes[i].ip;
    compute_ports[i] = global_meta_man->remote_compute_nodes[i].port;
  }

  auto* compute_server = new ComputeServer(compute_node, compute_ips, compute_ports);
  g_compute_server = compute_server;

  sleep(3);

  compute_server->WaitForClusterStart();

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
  if (bench_name == "smallbank") {
      WORKLOAD_MODE = 0;
  } else if(bench_name == "tpcc") {
      WORKLOAD_MODE = 1;
  } else if (bench_name == "ycsb"){
      WORKLOAD_MODE = 2;
  } else if (oltp::Enabled(bench_name)) {
      WORKLOAD_MODE = bench_name == "tatp" ? oltp::TatpMode : oltp::SysbenchMode;
  }else {
      LOG(FATAL) << "Unsupported benchmark name: " << bench_name;
      assert(false);
  }

  std::cout << "WORKLOAD_MODE = " << WORKLOAD_MODE << "\n";
  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  auto number_or = [&](const char* key, double fallback) {
    auto value = client_conf.get(key);
    if (!value.exists()) return fallback;
    return value.is_int64() ? static_cast<double>(value.get_int64()) : value.get_double();
  };
  benchmark_stats::runtime.Configure(client_conf.get("benchmark_run_mode").get_str("count"),
      number_or("benchmark_run_seconds", 60), number_or("benchmark_prewarm_seconds", 10));
  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  node_id_t machine_num = static_cast<node_id_t>(remote_compute_ips.size());
  assert(machine_num > 0);
  // 同上：元组用 1 字节记录 holder 节点，故节点数上限 256（id 0 ~ 255）
  assert(machine_num <= MaxComputeNodeCount);
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  auto parallel_page_fetch = client_conf.get("parallel_page_fetch");
  if (parallel_page_fetch.exists() && parallel_page_fetch.is_int64()) {
    PARALLEL_PAGE_FETCH = (int)parallel_page_fetch.get_int64();
  }
  std::cout << "starting primary , machine id = " << machine_id << " machine num = " << machine_num << "\n";
  t_id_t thread_num_per_machine = (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
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
  g_compute_server = compute_server;

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

  compute_server->WaitReady();

  SmallBank* smallbank_client = nullptr;
  TPCC* tpcc_client = nullptr;
  YCSB *ycsb_client = nullptr;

  if (bench_name == "smallbank") {
    std::string config_path = "../../config/smallbank_config.json";
    auto config = JsonConfig::load_file(config_path);
    int hot_rate = config.get("smallbank").get("num_hot_rate").get_int64();
    int use_zipfian = config.get("smallbank").get("use_zipfian").get_int64();

    assert(hot_rate > 0 && hot_rate < 100);
    auto reuse_prob = config.get("smallbank").get("id_reuse_prob");
    double id_reuse_prob = reuse_prob.is_int64() ? static_cast<double>(reuse_prob.get_int64()) : reuse_prob.get_double(0.0);
    int id_reuse_window_size = config.get("smallbank").get("id_reuse_window_size").get_int64(64);
    smallbank_client = new SmallBank(nullptr , hot_rate , use_zipfian, false,
                                    id_reuse_prob, id_reuse_window_size);
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
    int write_owner_count = config.get("ycsb").get("write_owner_count").get_int64(0);
    auto reuse_prob = config.get("ycsb").get("id_reuse_prob");
    double id_reuse_prob = reuse_prob.is_int64() ? static_cast<double>(reuse_prob.get_int64()) : reuse_prob.get_double(0.0);
    int id_reuse_window_size = config.get("ycsb").get("id_reuse_window_size").get_int64(64);
    assert(hot_cnt < record_cnt);
    assert(use_zipfian == 1 || use_zipfian == 0);
    assert(read_cnt + write_cnt == 100);
    assert(field_len > 0);
    assert(tx_hot_rate > 0 && tx_hot_rate < 100);
    assert(zipf_theta == -1.0 || (zipf_theta >= 0.0 && zipf_theta < 1.0) || zipf_theta >= 40.0);
    auto* mm = compute_server->get_node()->getMetaManager();
    bool random_generate = mm->GetRandomGenerate();
    ycsb_client = new YCSB(nullptr , record_cnt , hot_cnt , use_zipfian , read_cnt , write_cnt , field_len , tx_hot_rate , zipf_theta , random_generate, write_owner_count,
                           id_reuse_prob, id_reuse_window_size);
    total_try_times.resize(YCSB_TX_TYPES, 0);
    total_commit_times.resize(YCSB_TX_TYPES, 0);
  } else if (oltp::Enabled(bench_name)) {
    total_try_times.resize(oltp::Types(bench_name).size());
    total_commit_times.resize(total_try_times.size());
  }else {
    LOG(FATAL) << "Unsupported benchmark name: " << bench_name;
  }


  auto reset_storage_stats = [&] {
    if (machine_id == 0) {
      storage_service::StorageService_Stub stub(compute_server->get_storage_channel());
      storage_service::ResetBenchmarkStatsRequest request;
      storage_service::ResetBenchmarkStatsResponse response;
      brpc::Controller cntl;
      stub.ResetBenchmarkStats(&cntl, &request, &response, nullptr);
      CHECK(!cntl.Failed()) << "Reset storage statistics: " << cntl.ErrorText();
    }
  };
  compute_server->WaitForClusterStart([&] {
    ResetBenchmarkStats(compute_server);
    compute_node->page_cost_stats_.ResetPageCostStats();
    reset_storage_stats();
    std::cout << "Benchmark statistics and page cost statistics reset" << std::endl;
  });

  std::atomic<int> bench_done(0);
  t_id_t i = 0;

  // 改用 Scheduler + Fiber。
  // Scheduler 起 thread_num_per_machine 条线程，每条线程挂一个【钉线程】的 fiber 跑
  // run_thread —— thread_local 初始化必须发生在目标线程上，所以 fiber 不能漂。
  Scheduler* bench_sched = new Scheduler(thread_num_per_machine, false, "BenchScheduler");
  bench_sched->start();
  std::vector<int> bench_tids = bench_sched->getThreadIds();
  assert((int)bench_tids.size() >= (int)thread_num_per_machine);

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
    // 把 run_thread 作为 fiber 钉到对应线程上跑
    bench_sched->schedule([&, i]() {
        run_thread(&param_arr[i], smallbank_client, tpcc_client, ycsb_client);
        bench_done.fetch_add(1, std::memory_order_relaxed);
    }, bench_tids[i]);
  }
  
  // Reporting is driven by this control thread, even when every worker is waiting.
  while (benchmark_stats::runtime.ready.load() < thread_num_per_machine) usleep(1000);
  auto& runtime = benchmark_stats::runtime;
  runtime.Start();
  if (runtime.prewarm_seconds > 0) {
    std::cout << "Prewarm started: " << runtime.prewarm_seconds << " seconds" << std::endl;
    while (runtime.paused.load() < thread_num_per_machine) usleep(1000);
    runtime.prewarm_attempts = runtime.attempts.load();
    runtime.prewarm_commits = runtime.commits.load();
    compute_server->prepare_notifications_.Drain();
    compute_server->pending_releases_.Drain();
    CHECK_EQ(socket_finish_client(global_meta_man->remote_server_nodes[0].ip,
        global_meta_man->remote_server_meta_port, false,
        [&] { compute_server->LogFlush(); }, [&] {
          // Baseline for measured-only page-cost analysis; model history stays intact.
          std::ofstream prewarm_costs("page_cost_prewarm.tsv");
          compute_node->page_cost_stats_.DumpCosts(prewarm_costs);
          prewarm_costs.close();
          ResetBenchmarkStats(compute_server);
          runtime.phase = benchmark_stats::Runtime::ResetWorkers;
          while (runtime.reset_done.load() < thread_num_per_machine) usleep(1000);
          reset_storage_stats();
        }), 0);
    runtime.Start();
    runtime.phase = benchmark_stats::Runtime::Measure;
    std::cout << "Prewarm finished: " << runtime.prewarm_attempts
              << " attempts; benchmark statistics reset; page cost history retained" << std::endl;
  }
  std::cout << "Measurement started" << std::endl;
  uint64_t last_sample = runtime.start_ns.load(), last_commits = 0, last_aborts = 0;
  auto sample = [&](uint64_t now) {
    const auto commits = runtime.commits.load();
    const auto aborts = runtime.aborts.load();
    const double seconds = static_cast<double>(now - last_sample) / 1e9;
    if (seconds <= 0) return;
    const double tps = (commits - last_commits) / seconds;
    const double completed_tps = (commits + aborts - last_commits - last_aborts) / seconds;
    const auto wall_now = std::chrono::system_clock::now();
    const auto wall_time = std::chrono::system_clock::to_time_t(wall_now);
    const auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(
        wall_now.time_since_epoch()) % 1000;
    std::tm local_time{};
    localtime_r(&wall_time, &local_time);
    std::ostringstream row;
    row << '[' << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S") << '.'
        << std::setfill('0') << std::setw(3) << milliseconds.count() << "] [Node " << machine_id
        << "] [Commit TPS] " << std::fixed << std::setprecision(2) << tps
        << " txn/sec [Completed TPS] " << completed_tps
        << " txn/sec (total commits: " << commits << ", total aborts: " << aborts << ')';
    // The runner streams stdout into nodeN/compute.out without printing it.
    std::cout << row.str() << std::endl;
    last_sample = now; last_commits = commits; last_aborts = aborts;
  };
  constexpr uint64_t kTpsReportIntervalNs = 2'000'000'000ULL;
  while (bench_done.load(std::memory_order_relaxed) < (int)thread_num_per_machine) {
    const auto now = benchmark_stats::NowNs();
    if (now - last_sample >= kTpsReportIntervalNs) sample(now);
    usleep(1000);
  }
  sample(benchmark_stats::NowNs());
  runtime.Stop();
  std::cout << "all bench fibers finished" << std::endl;
  
  std::cout << "[Benchmark drain] Node " << machine_id
            << ": workers finished, waiting for all compute nodes" << std::endl;

  // 统计compute server中的统计信息
  tx_update_time = compute_server->tx_update_time;
  
  // Wait for all compute nodes to finish
  // Keep the same drain barrier for both switch values: this node may have
  // participated in a durable-Prepare transaction coordinated by another node.
  const bool drain_2pc_logs = SYSTEM_MODE == 2 || SYSTEM_MODE == 4;
  if (drain_2pc_logs) {
    compute_server->prepare_notifications_.Drain();
    compute_server->pending_releases_.Drain();
  }
  CHECK(socket_finish_client(global_meta_man->remote_server_nodes[0].ip,
                            global_meta_man->remote_server_meta_port, drain_2pc_logs) == 0);
  if (drain_2pc_logs) {
    // Every coordinator has now joined its Commit RPCs, including requests to
    // this participant from workers that finished later than our own workers.
    compute_server->LogFlush();
    CHECK(socket_finish_client(global_meta_man->remote_server_nodes[0].ip,
                              global_meta_man->remote_server_meta_port) == 0);
  }
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
    std::cout << "[Benchmark drain] Node " << machine_id
              << ": waiting for storage to replay durable WAL and flush pages" << std::endl;
    if (!compute_server->ShutdownStorageServer()) {
      LOG(ERROR) << "Fail to shutdown storage server";
      std::abort(); // failed replay/drain cannot produce a successful experiment
    }
    std::cout << "[Benchmark drain] Node " << machine_id
              << ": storage shutdown complete, shutting down remote server" << std::endl;
    if (!compute_server->ShutdownRemoteServer()) {
      LOG(ERROR) << "Fail to shutdown remote server";
    }
  }

  std::cout << "All compute nodes have finished" << std::endl;

  delete[] param_arr;
  delete global_meta_man;
  if (smallbank_client) delete smallbank_client;
  if(tpcc_client) delete tpcc_client;
}
