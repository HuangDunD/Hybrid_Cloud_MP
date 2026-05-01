// Author: Chunyue Huang
// Copyright (c) 2024

#include <butil/logging.h>
#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <brpc/channel.h>
#include "meta_manager.h"
#include "base/data_item.h"
#include "util/json_config.h"
#include "util/bitmap.h"
#include "storage/storage_service.pb.h"

MetaManager::MetaManager(std::string bench_name, IndexCache* index_cache , PageCache* page_cache , int node_id , int system_mode) 
  : index_cache_(index_cache), page_cache_(page_cache){
  // init table name and table id map
  if (bench_name == "smallbank") {
    table_name_map[0] = "smallbank_savings";
    table_name_map[1] = "smallbank_checking";

  } else if (bench_name == "tpcc") {
    table_name_map[0] = "tpcc_warehouse";
    table_name_map[1] = "tpcc_district";
    table_name_map[2] = "tpcc_customer";
    table_name_map[3] = "tpcc_customerhistory";
    table_name_map[4] = "tpcc_ordernew";
    table_name_map[5] = "tpcc_order";
    table_name_map[6] = "tpcc_orderline";
    table_name_map[7] = "tpcc_item";
    table_name_map[8] = "tpcc_stock";
    table_name_map[9] = "tpcc_customerindex";
    table_name_map[10] = "tpcc_orderindex";
  }else if (bench_name == "ycsb"){
    table_name_map[0] = "ycsb_user_table";
  }else if (bench_name == ""){
    // SQL
    
  }else {
    assert(false);
  }

  // Read config json file
  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto local_node = json_config.get("local_compute_node");
  local_machine_id = node_id;
  txn_system = system_mode;

  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  auto remote_compute_ports = compute_nodes.get("remote_compute_node_port");
  auto remote_compute_meta_ports = compute_nodes.get("remote_compute_node_meta_port");

  auto server_nodes = json_config.get("remote_server_nodes");
  auto remote_server_ips = server_nodes.get("remote_server_node_ips");
  auto remote_server_ports = server_nodes.get("remote_server_node_port");
  auto remote_server_meta_ports = server_nodes.get("remote_server_node_meta_port");
  remote_server_meta_port = remote_server_meta_ports.get(0).get_int64();
  
  auto storage_nodes = json_config.get("remote_storage_nodes");
  auto remote_storage_ips = storage_nodes.get("remote_storage_node_ips");               
  auto remote_storage_ports = storage_nodes.get("remote_storage_node_rpc_port");            
  auto remote_storage_meta_ports = storage_nodes.get("remote_storage_node_meta_port");  

  // Get remote machine's compute store meta via TCP
  for (size_t index = 0; index < remote_compute_ips.size(); index++) {
    std::string remote_ip = remote_compute_ips.get(index).get_str();
    int remote_port = (int)remote_compute_ports.get(index).get_int64();
    remote_compute_nodes.push_back(RemoteNode{.node_id = (int32_t)index, .ip = remote_ip, .port = remote_port});
  }

  // Get remote machine's server store meta via TCP
  for (size_t index = 0; index < remote_server_ips.size(); index++) {
    std::string remote_ip = remote_server_ips.get(index).get_str();
    int remote_port = (int)remote_server_ports.get(index).get_int64();
    remote_server_nodes.push_back(RemoteNode{.node_id = 0, .ip = remote_ip, .port = remote_port});
  }

  // Get remote machine's storage store meta via TCP
  for (size_t index = 0; index < remote_storage_ips.size(); index++) {
    std::string remote_ip = remote_storage_ips.get(index).get_str();
    int remote_meta_port = (int)remote_storage_meta_ports.get(index).get_int64();
    node_id_t remote_machine_id;
    if (bench_name != ""){
      // 如果是非 SQL 模式，需要从存储层获取到初始化的页面数量
      remote_machine_id = GetRemoteStorageMeta(remote_ip, remote_meta_port);
      if (remote_machine_id == -1) {
        LOG(ERROR) << "Thread " << std::this_thread::get_id() << " GetAddrStoreMeta() failed!, remote_machine_id = -1" << std::endl;
        assert(false);
      }
      // 校验：计算层指定的 bench_name 必须与存储层启动时的 workload 一致，
      // 否则两侧表结构 / 页面布局对不上，会导致访问越界或数据错乱。
      if (!storage_workload_str_.empty() && storage_workload_str_ != bench_name) {
        LOG(FATAL) << "Compute node started with bench_name='" << bench_name
                   << "' but storage server reports workload='" << storage_workload_str_
                   << "'. They must match. Aborting.";
        std::cerr << "[FATAL] Compute/Storage workload mismatch: compute='" << bench_name
                  << "' storage='" << storage_workload_str_ << "'\n";
        assert(false);
      }
    }
    
    int remote_port = (int)remote_storage_ports.get(index).get_int64();
    remote_storage_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port});
  }

  // prefetch index
  if (bench_name != ""){
    for (auto& table : table_name_map) {
      // table.first ：table_id
      PrefetchIndex(table.first);
    }
    for(auto &item : index_cache_->getRidsMap()) {
        table_id_t item_table_id = item.first;
        for(auto it : item.second) {
            itemkey_t it_key = it.first;
            Rid it_rid = it.second;
            page_cache_->Insert(item_table_id,it_rid.page_no_,it_key);
        }
    }
  }
}

// 在 PrefetchIndex 与 par_size_per_table 都已就绪后调用，构造 [table_id][node_id] -> 真实 key 列表。
// 节点拥有的「页面集合」由分区策略决定：物理 page p (1-based) 归属节点
//     ((p-1) % (node_count * partition_size)) / partition_size
// 该划分与 GetPageNumPerNode / 各 workload 中的 page 反映射保持一致。
// 节点上的 key 列表 = 该节点拥有的所有页面里的全部真实 key（按 page_id, slot 顺序追加）。
void MetaManager::BuildNodeKeyLists(int node_count) {
  if (node_count <= 0) return;
  if (!page_cache_) return;
  node_key_lists_.clear();
  const auto& pc = page_cache_->getPageCache();
  for (const auto& [table_id, page_map] : pc) {
    int partition_size = (table_id < (int)par_size_per_table.size()) ? par_size_per_table[table_id] : 0;
    if (partition_size <= 0) {
      // 该表未参与分区（如 BLink/FSM 内部表），跳过
      continue;
    }
    // 收集该表所有 page_id，按升序遍历，保证 key 列表内部按 (page, slot) 顺序，
    // zipfian 索引 0 自然对应「物理排布最前的一条 key」，热点位置稳定。
    std::vector<page_id_t> pages;
    pages.reserve(page_map.size());
    for (const auto& kv : page_map) pages.push_back(kv.first);
    std::sort(pages.begin(), pages.end());
    auto& per_node = node_key_lists_[table_id];
    per_node.assign(node_count, std::vector<itemkey_t>());
    int chunk = node_count * partition_size;
    for (page_id_t p : pages) {
      int owner = (int)(((p - 1) % chunk) / partition_size);
      if (owner < 0 || owner >= node_count) continue;
      const auto& keys = page_map.at(p);
      auto& dst = per_node[owner];
      dst.insert(dst.end(), keys.begin(), keys.end());
    }
  }
}

node_id_t MetaManager::GetRemoteStorageMeta(std::string& remote_ip, int remote_port) {
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
    return -1;
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
    close(client_socket);
    return -1;
  }
  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
    close(client_socket);
    return -1;
  }

  /* --------------- Receiving Storage metadata ----------------- */
  size_t meta_size = (size_t)1024;
  char* recv_buf = (char*)malloc(meta_size);
  auto retlen = recv(client_socket, recv_buf, meta_size, 0);
  if (retlen < 0) {
    LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    return -1;
  }
  char ack[] = "[ACK]hash_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);
  close(client_socket);
  char* snooper = recv_buf;
  // Now only recieve the machine id
  node_id_t remote_machine_id = *((node_id_t*)snooper);
  if (remote_machine_id >= MAX_REMOTE_NODE_NUM) {
    LOG(FATAL) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
  }
  snooper += sizeof(remote_machine_id);

  int table_num = *((int*)snooper);
  snooper += sizeof(int);
  int* init_page_num_per_table = (int*)snooper;
  snooper += table_num * sizeof(int);
  int record_per_page = *((int*)snooper);
  snooper += sizeof(int);
  // 接收 random_generate 标志（由存储层根据 storage_node_config.json 决定）
  int random_generate_flag = *((int*)snooper);
  snooper += sizeof(int);
  random_generate_ = (random_generate_flag != 0);
  // 接收 workload 标识（用于校验计算层与存储层启动方式是否一致）
  // 与存储层 PrepareStorageMeta 中 kWorkloadStrLen 必须保持一致
  constexpr int kWorkloadStrLen = 32;
  storage_workload_str_.assign(snooper, strnlen(snooper, kWorkloadStrLen));
  snooper += kWorkloadStrLen;
  page_num_per_table = std::vector<int>(30000 , 0);
  assert(table_num % 3 == 0);
  assert(table_num > 0);
  int real_table = table_num / 3;
  // std::cout << "Real Table Cnt = " << real_table << "\n";
  for(int i = 0; i < real_table ; i++) {
      page_num_per_table[i] = init_page_num_per_table[i];
      page_num_per_table[i + 10000] = init_page_num_per_table[i + real_table];
      page_num_per_table[i + 20000] = init_page_num_per_table[i + real_table * 2];

      // std::cout << "[MetaManager] Table ID = " << i << " Raw Page Num = " << page_num_per_table[i] 
      //     << " BLink Raw Page Num = " << page_num_per_table[i + 10000]
      //     << " FSM Raw Page Num = " << page_num_per_table[i + 20000] 
      //     << "\n";
  }
  
  assert(*(uint64_t*)snooper == MEM_STORE_META_END);
  free(recv_buf);
  return remote_machine_id;
}

// ljTag
void MetaManager::PrefetchIndex(const int &table_id) {
  brpc::Channel index_channel;
  // Init Brpc channel
  brpc::ChannelOptions options;
  options.timeout_ms = 0x7FFFFFFF;
  std::string storage_ips = remote_storage_nodes[0].ip;
  int storage_port = remote_storage_nodes[0].port;
  std::string storage_node = storage_ips + ":" + std::to_string(storage_port);
  if(index_channel.Init(storage_node.c_str(), &options) != 0) {
    LOG(FATAL) << "Fail to initialize channel to " << storage_node;
    assert(false);
  }
  brpc::Controller cntl;
  storage_service::GetBatchIndexRequest request;
  storage_service::GetBatchIndexResponse response;
  std::string table_name = table_name_map[table_id];
  request.set_table_name(table_name);
  int batch_id = 0;
  while (true){
    request.set_batch_id(batch_id);
    request.set_table_name(table_name);
    storage_service::StorageService_Stub stub(&index_channel);
    stub.PrefetchIndex(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
      LOG(FATAL) << "Fail to prefetch index";
      assert(false);
    }
    assert(response.itemkey_size() == response.pageid_size());
    assert(response.pageid_size() == response.slotid_size());
    size_t index_size = response.itemkey_size();
    
    for (int i = 0; i < response.itemkey_size(); i++) {
      // std::cout << table_id << " " << table_id << " key : " << i << " page_no = " << response.pageid(i) << "\n";
      index_cache_->Insert(table_id, response.itemkey(i), Rid{response.pageid(i), response.slotid(i)});
    }
    if (index_size < BATCH_INDEX_PREFETCH_SIZE) {
      break;
    }
    batch_id++;
    cntl.Reset();
  }
}

Rid MetaManager::Fetchrid(const int &table_id,itemkey_t key){
  return index_cache_->Search(table_id,key);
}
