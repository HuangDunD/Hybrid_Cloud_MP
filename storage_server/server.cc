// Author: Huang Chunyue 
// Copyright (c) 2024

#include "server.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>
#include <butil/logging.h>

#include "util/json_config.h"
#include "util/bitmap.h"
#include "storage/sm_manager.h"

// 计算给定 record_size 时每页可容纳的记录数（与 RmManager::create_file 保持一致）
static int compute_records_per_page(int record_size) {
  return (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1)
         / (1 + (record_size + (int)sizeof(itemkey_t)) * BITMAP_WIDTH);
}

// 检查给定的若干张表的数据文件是否已生成且数据页数量正确。
// expected_pages[i] == -1 表示不校验确切页数，仅要求 num_pages_ > 1（至少装载过数据）。
// 同时要求每张表对应的 _bl 文件存在。
static bool data_files_already_built(RmManager* rm_manager,
                                     const std::vector<std::string>& tables,
                                     const std::vector<int>& expected_pages,
                                     const std::vector<int>& expected_record_sizes = {}) {
  auto* dm = rm_manager->get_diskmanager();
  for (size_t i = 0; i < tables.size(); ++i) {
    const std::string& name = tables[i];
    if (!dm->is_file(name)) {
      std::cout << "[storage] data file missing: " << name << ", will regenerate\n";
      return false;
    }
    if (!dm->is_file(name + "_bl")) {
      std::cout << "[storage] index file missing: " << name + "_bl" << ", will regenerate\n";
      return false;
    }
    auto fh = rm_manager->open_file(name);
    int actual = (int)fh->get_file_hdr().num_pages_;
    int actual_rs = (int)fh->get_file_hdr().record_size_;
    rm_manager->close_file(fh.get());
    if (expected_pages[i] >= 0) {
      if (actual != expected_pages[i]) {
        std::cout << "[storage] page count mismatch on " << name
                  << ": actual=" << actual << " expected=" << expected_pages[i]
                  << ", will regenerate\n";
        return false;
      }
    } else {
      if (actual <= 1) {
        std::cout << "[storage] page count too small on " << name
                  << ": actual=" << actual << ", will regenerate\n";
        return false;
      }
    }
    if (i < expected_record_sizes.size() && expected_record_sizes[i] >= 0) {
      if (actual_rs != expected_record_sizes[i]) {
        std::cout << "[storage] record_size mismatch on " << name
                  << ": actual=" << actual_rs << " expected=" << expected_record_sizes[i]
                  << " (DataItem layout changed?), will regenerate\n";
        return false;
      }
    }
  }
  return true;
}

void LoadData(node_id_t machine_id,
                      node_id_t machine_num,  // number of memory nodes
                      std::string& workload,
                      RmManager* rm_manager) {
  std::cout << "Begin Init Data...\n";
  // 读取存储节点配置中的 random_generate 选项
  bool random_generate = false;
  {
    std::string storage_config_path = "../../config/storage_node_config.json";
    auto storage_cfg = JsonConfig::load_file(storage_config_path);
    auto local_node_cfg = storage_cfg.get("local_storage_node");
    random_generate = (bool)local_node_cfg.get("random_generate").get_bool();
  }

  if (workload == "smallbank") {
    // 计算 SmallBank 期望页数：tuple_size = sizeof(DataItem) + sizeof(smallbank_savings_val_t)
    std::string sb_config_path = "../../config/smallbank_config.json";
    auto sb_cfg = JsonConfig::load_file(sb_config_path);
    int num_accounts = (int)sb_cfg.get("smallbank").get("num_accounts").get_uint64();
    int sb_tuple = sizeof(DataItem) + sizeof(smallbank_savings_val_t);
    int sb_rpp  = compute_records_per_page(sb_tuple);
    int sb_exp  = (num_accounts + sb_rpp - 1) / sb_rpp;
    int sb_chk_tuple = sizeof(DataItem) + sizeof(smallbank_checking_val_t);
    if (data_files_already_built(rm_manager,
                                 {"smallbank_savings", "smallbank_checking"},
                                 {sb_exp, sb_exp},
                                 {sb_tuple, sb_chk_tuple})) {
      std::cout << "[SmallBank] data files already built (pages=" << sb_exp
                << "), skip generation.\n";
    } else {
      SmallBank smallbank_server(rm_manager , 50 , 0 , random_generate);
      smallbank_server.LoadTable(machine_id, machine_num);
      // smallbank_server.VerifyData(); // 已禁用：跳过数据正确性校验
    }
  } else if (workload == "tpcc") {
    // TPCC 涉及 11 张表，期望页数依赖配置（warehouse / district / customer 等），
    // 这里采用宽松校验：所有数据文件存在且 num_pages_ > 1
    std::vector<std::string> tpcc_tables = {
      "tpcc_warehouse", "tpcc_district", "tpcc_customer", "tpcc_customerhistory",
      "tpcc_ordernew", "tpcc_order", "tpcc_orderline", "tpcc_item",
      "tpcc_stock", "tpcc_customerindex", "tpcc_orderindex"
    };
    std::vector<int> tpcc_exp(tpcc_tables.size(), -1);
    if (data_files_already_built(rm_manager, tpcc_tables, tpcc_exp)) {
      std::cout << "[TPCC] data files already built, skip generation.\n";
    } else {
      TPCC tpcc_server(rm_manager , random_generate);
      tpcc_server.LoadTable(machine_id, machine_num);
      // tpcc_server.VerifyData(); // 已禁用：跳过数据正确性校验
    }
  } else if (workload == "ycsb"){
    std::string config_path = "../../config/ycsb_config.json";
    auto config = JsonConfig::load_file(config_path);
    int record_cnt = config.get("ycsb").get("num_record").get_int64();
    // YCSB 期望页数：tuple_size = sizeof(DataItem) + sizeof(ycsb_user_table_val)
    int y_tuple = sizeof(DataItem) + sizeof(ycsb_user_table_val);
    int y_rpp   = compute_records_per_page(y_tuple);
    int y_exp   = (record_cnt + y_rpp - 1) / y_rpp;
    if (data_files_already_built(rm_manager, {"ycsb_user_table"}, {y_exp}, {y_tuple})) {
      std::cout << "[YCSB] data files already built (pages=" << y_exp
                << "), skip generation.\n";
    } else {
      YCSB ycsb_server(rm_manager , record_cnt , -1 , 0 , std::vector<int>{} , std::vector<int>{} , 10 , 90 , 100 , 60 , 0.70 , random_generate);
      ycsb_server.LoadTable();
      // ycsb_server.VerifyData(); // 已禁用：跳过数据正确性校验
    }
  } else{
    LOG(ERROR) << "Unsupported workload: " << workload;
    assert(false);
  }
}

void Server::SendMeta(node_id_t machine_id, size_t compute_node_num, std::string workload) {
  // Prepare LockTable meta
  char* storage_meta_buffer = nullptr;
  size_t total_meta_size = 0;
  PrepareStorageMeta(machine_id, workload, &storage_meta_buffer, total_meta_size);
  assert(storage_meta_buffer != nullptr);
  assert(total_meta_size != 0);

  // Send memory store meta to all the compute nodes via TCP
  for (int index = 0; index < compute_node_num; index++) {
    SendStorageMeta(storage_meta_buffer, total_meta_size);
  }
  free(storage_meta_buffer);
}

void Server::PrepareStorageMeta(node_id_t machine_id, std::string workload, char** storage_meta_buffer, size_t& total_meta_size) {
  // Get LockTable meta
  int table_num;

  // 我们项目里，每个表的 B+ 树和 FSM 都视为一个单独的表
  // 所以这里 table_num 是物理上的表数量，用户看到的数量是 table_num / 3
  // 也就是 smallbank 两张，saving 和 checking，tpcc 11 张，ycsb 1 张 user_table
  if(workload == "smallbank") {
    table_num = 6;
  }else if(workload == "tpcc") {
    table_num = 33;
  } else if (workload == "ycsb"){
    table_num = 3;
  }else {
    assert(false);
  }
  
  std::vector<int> init_page_num_per_table(table_num, 0);
  int record_per_page;
  // 读取 random_generate 配置（YCSB 等负载根据该参数决定 key 的生成模式）
  int random_generate_flag = 0;
  {
    std::string storage_config_path = "../../config/storage_node_config.json";
    auto storage_cfg = JsonConfig::load_file(storage_config_path);
    auto local_node_cfg = storage_cfg.get("local_storage_node");
    random_generate_flag = local_node_cfg.get("random_generate").get_bool() ? 1 : 0;
  }
  // meta 布局：[table_num][init_page_num_per_table * table_num][record_per_page][random_generate_flag][workload_str(kWorkloadStrLen)]
  // 末尾追加 workload 标识符（定长，'\0' 填充），让计算层启动时校验自身与存储层启动方式是否一致。
  constexpr int kWorkloadStrLen = 32;
  int storage_meta_len = sizeof(int) + table_num * sizeof(int) + sizeof(int) + sizeof(int) + kWorkloadStrLen;
  std::vector<char> storage_meta(storage_meta_len);
  // 预先准备 workload 字符串
  char workload_buf[kWorkloadStrLen];
  memset(workload_buf, 0, sizeof(workload_buf));
  // 限制：超过 31 字节会被截断
  strncpy(workload_buf, workload.c_str(), kWorkloadStrLen - 1);

  if(workload == "smallbank") {
    std::vector<std::string> sb_tables = {"smallbank_savings", "smallbank_checking"};
    for(int i = 0; i < 2; ++i) {
        // 1. Data Table (ID: i)
        std::unique_ptr<RmFileHandle> table_file = rm_manager_->open_file(sb_tables[i]);
        init_page_num_per_table[i] = table_file->get_file_hdr().num_pages_;
        // std::cout << "File Size = " << max_page_num_per_table[i] << "\n";
        if(i == 0) record_per_page = table_file->get_file_hdr().num_records_per_page_;

        // 2. B-Link Tree (ID: i + 4)
        std::string bl_name = sb_tables[i] + "_bl";
        int bl_size = rm_manager_->get_diskmanager()->get_file_size(bl_name);
        init_page_num_per_table[i + 2] = (bl_size == -1) ? 0 : (bl_size / PAGE_SIZE);

        // 3. FSM
        std::string fsm_name = sb_tables[i] + "_fsm";
        // int fsm_size = rm_manager_->get_diskmanager()->get_file_size(fsm_name);
        // max_page_num_per_table[i + 4] = (fsm_size == -1) ? 0 : (fsm_size / PAGE_SIZE);
    }

    // Fill storage meta
    memcpy(storage_meta.data(), &table_num, sizeof(int));
    memcpy(storage_meta.data() + sizeof(int), init_page_num_per_table.data(), table_num * sizeof(int));
    memcpy(storage_meta.data() + sizeof(int) + table_num * sizeof(int), &record_per_page, sizeof(int));
    memcpy(storage_meta.data() + sizeof(int) + table_num * sizeof(int) + sizeof(int), &random_generate_flag, sizeof(int));
    memcpy(storage_meta.data() + sizeof(int) + table_num * sizeof(int) + sizeof(int) + sizeof(int), workload_buf, kWorkloadStrLen);

  }else if(workload == "tpcc") {
      std::vector<std::string> tpcc_tables = {
        "tpcc_warehouse", "tpcc_district", "tpcc_customer", "tpcc_customerhistory",
        "tpcc_ordernew", "tpcc_order", "tpcc_orderline", "tpcc_item",
        "tpcc_stock", "tpcc_customerindex", "tpcc_orderindex"
      };

      for(int i = 0; i < 11; ++i) {
          // 1. Data Table (ID: i)
          std::unique_ptr<RmFileHandle> table_file = rm_manager_->open_file(tpcc_tables[i]);
          init_page_num_per_table[i] = table_file->get_file_hdr().num_pages_;
          if(i == 0) record_per_page = table_file->get_file_hdr().num_records_per_page_;

          // 2. B-Link Tree (ID: i + 22)
          std::string bl_name = tpcc_tables[i] + "_bl";
          int bl_size = rm_manager_->get_diskmanager()->get_file_size(bl_name);
          init_page_num_per_table[i + 11] = (bl_size == -1) ? 0 : (bl_size / PAGE_SIZE);

          // std::cout << "Table Init Page Num = " << init_page_num_per_table[i]
          //           << " BLink Init Page Num = " << init_page_num_per_table[i + 11]
          //           << " FSM "
          //           << " \n";

          // 3. FSM
          std::string fsm_name = tpcc_tables[i] + "_fsm";
          // int fsm_size = rm_manager_->get_diskmanager()->get_file_size(fsm_name);
          // max_page_num_per_table[i + 22] = (fsm_size == -1) ? 0 : (fsm_size / PAGE_SIZE);
      }

      // Fill storage meta
      memcpy(storage_meta.data(), &table_num, sizeof(int));
      memcpy(storage_meta.data() + sizeof(int), init_page_num_per_table.data(), table_num * sizeof(int));
      memcpy(storage_meta.data() + sizeof(int) + table_num * sizeof(int), &record_per_page, sizeof(int));
      memcpy(storage_meta.data() + sizeof(int) + table_num * sizeof(int) + sizeof(int), &random_generate_flag, sizeof(int));
      memcpy(storage_meta.data() + sizeof(int) + table_num * sizeof(int) + sizeof(int) + sizeof(int), workload_buf, kWorkloadStrLen);
  } else if (workload == "ycsb"){
    std::string ycsb_table = "ycsb_user_table";
    for (int i = 0 ; i < 1 ; i++){
        std::unique_ptr<RmFileHandle> table_file = rm_manager_->open_file(ycsb_table);
        init_page_num_per_table[i] = table_file->get_file_hdr().num_pages_;
        if(i == 0) record_per_page = table_file->get_file_hdr().num_records_per_page_;

        // 2. B-Link Tree (ID: i + 22)
        std::string bl_name = ycsb_table + "_bl";
        int bl_size = rm_manager_->get_diskmanager()->get_file_size(bl_name);
        init_page_num_per_table[i + 1] = (bl_size == -1) ? 0 : (bl_size / PAGE_SIZE);
        
        // std::cout << "Init Page Num = " << init_page_num_per_table[i] << " " << "Init BLink = " << init_page_num_per_table[i + 1] << "\n";
        // 3. FSM
        std::string fsm_name = ycsb_table + "fsm";
        // int fsm_size = rm_manager_->get_diskmanager()->get_file_size(fsm_name);
        // max_page_num_per_table[i + 22] = (fsm_size == -1) ? 0 : (fsm_size / PAGE_SIZE);
        init_page_num_per_table[i + 2] = 0;
    }
    memcpy(storage_meta.data(), &table_num, sizeof(int));
    memcpy(storage_meta.data() + sizeof(int), init_page_num_per_table.data(), table_num * sizeof(int));
    memcpy(storage_meta.data() + sizeof(int) + table_num * sizeof(int), &record_per_page, sizeof(int));
    memcpy(storage_meta.data() + sizeof(int) + table_num * sizeof(int) + sizeof(int), &random_generate_flag, sizeof(int));
    memcpy(storage_meta.data() + sizeof(int) + table_num * sizeof(int) + sizeof(int) + sizeof(int), workload_buf, kWorkloadStrLen);
  } else {
    LOG(ERROR) << "Unsupported workload: " << workload;
    assert(false);
  }

  total_meta_size = sizeof(machine_id) + storage_meta_len + sizeof(MEM_STORE_META_END);

  *storage_meta_buffer = (char*)malloc(total_meta_size);

  char* local_buf = *storage_meta_buffer;

  // Fill primary hash meta
  *((node_id_t*)local_buf) = machine_id;
  local_buf += sizeof(machine_id);
  
  memcpy(local_buf, storage_meta.data(), storage_meta_len);

  local_buf += storage_meta_len;
  // EOF
  *((uint64_t*)local_buf) = MEM_STORE_META_END;
}

void Server::SendStorageMeta(char* hash_meta_buffer, size_t& total_meta_size) {
  //> Using TCP to send hash meta
  /* --------------- Initialize socket ---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(local_meta_port_);    // change host little endian to big endian
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);  // change host "0.0.0.0" to big endian
  int listen_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (listen_socket < 0) {
    LOG(ERROR) << "Server creates socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  // std::cout << "Server creates socket success";
  if (bind(listen_socket, (const struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    LOG(ERROR) << "Server binds socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  // std::cout << "Server binds socket success";
  int max_listen_num = 10;
  if (listen(listen_socket, max_listen_num) < 0) {
    LOG(ERROR) << "Server listens error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  // std::cout << "Server listens success";
  int from_client_socket = accept(listen_socket, NULL, NULL);
  // int from_client_socket = accept(listen_socket, (struct sockaddr*) &client_addr, &client_socket_length);
  if (from_client_socket < 0) {
    LOG(ERROR) << "Server accepts error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  // std::cout << "Server accepts success";

  /* --------------- Sending hash metadata ----------------- */
  auto retlen = send(from_client_socket, hash_meta_buffer, total_meta_size, 0);
  if (retlen < 0) {
    LOG(ERROR) << "Server sends hash meta error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  // std::cout << "Server sends hash meta success";
  size_t recv_ack_size = 100;
  char* recv_buf = (char*)malloc(recv_ack_size);
  recv(from_client_socket, recv_buf, recv_ack_size, 0);
  if (strcmp(recv_buf, "[ACK]hash_meta_received_from_client") != 0) {
    std::string ack(recv_buf);
    LOG(ERROR) << "Client receives hash meta error. Received ack is: " << ack;
  } else {
    std::cout << "Connect Success" << std::endl;
  }

  free(recv_buf);
  close(from_client_socket);
  close(listen_socket);
}

bool Server::Run() {
  // Now server just waits for user typing quit to finish
  // Server's CPU is not used during one-sided RDMA requests from clients
  printf("====================================================================================================\n");
  printf(
      "Server now runs as a disaggregated mode. No CPU involvement during RDMA-based transaction processing\n"
      "Type c to run another round, type q if you want to exit :)\n");
  while (true) {
    char ch;
    scanf("%c", &ch);
    if (ch == 'q') {
      return false;
    } else if (ch == 'c') {
      return true;
    } else {
      printf("Type c to run another round, type q if you want to exit :)\n");
    }
    usleep(2000);
  }
}

int main(int argc, char* argv[]) {
    // 默认以 SQL 交互模式启动
    std::string mode;
    if (argc == 1){
      std::cerr << "Please Input Mode\n";
      std::cerr << "Mode : sql , smallbank , tpcc , ycsb\n";
      std::cerr << "Example : ./storage_pool sql\n";
      exit(-1);
    } else if (argc > 2){
      std::cerr << "Error\n";
      exit(-1);
    }

    mode = std::string(argv[1]);
    if (mode != "smallbank" && mode != "tpcc" && mode != "ycsb" && mode != "sql"){
      std::cerr << "Invalid Mode\n";
      std::cerr << "Mode : sql , smallbank , tpcc , ycsb\n";
      exit(-1);
    }

    std::string log_path = "./storageserver.log" + std::to_string(getpid()); // 设置日志路径

    if (std::ifstream(log_path)) { std::remove(log_path.c_str()); }
    ::logging::LoggingSettings log_setting;  // 创建LoggingSetting对象进行设置
    log_setting.log_file = log_path.c_str(); // 设置日志路径
    log_setting.logging_dest = logging::LOG_TO_FILE; // 设置日志写到文件，不写的话不生效
    ::logging::InitLogging(log_setting);     // 应用日志设置

    // Configure of this server
    std::string config_filepath = "../../config/storage_node_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto local_node = json_config.get("local_storage_node");
    node_id_t machine_num = (node_id_t)local_node.get("machine_num").get_int64();
    node_id_t machine_id = (node_id_t)local_node.get("machine_id").get_int64();
    assert(machine_id >= 0 && machine_id < machine_num);
    int local_rpc_port = (int)local_node.get("local_rpc_port").get_int64();
    int local_meta_port = (int)local_node.get("local_meta_port").get_int64();
    bool use_rdma = (bool)local_node.get("use_rdma").get_bool();
    auto compute_config = JsonConfig::load_file("../../config/compute_node_config.json");
    auto compute_nodes = compute_config.get("remote_compute_nodes");
    auto compute_node_ips = compute_nodes.get("remote_compute_node_ips");
    size_t compute_node_num = compute_node_ips.size();

    std::vector<std::string> compute_ip_list;
    std::vector<int> compute_ports_list;
    for(size_t i=0; i<compute_node_ips.size(); i++){
      compute_ip_list.push_back(compute_nodes.get("remote_compute_node_ips").get(i).get_str());
      compute_ports_list.push_back(compute_nodes.get("remote_compute_node_port").get(i).get_int64());
    }

    auto disk_manager = std::make_shared<DiskManager>();

    auto buffer_mgr = std::make_shared<StorageBufferPoolManager>(RM_BUFFER_POOL_SIZE, disk_manager.get());
    auto rm_manager = std::make_shared<RmManager>(disk_manager.get(), buffer_mgr.get());

    SmManager *sm_manager = new SmManager(rm_manager.get() , rm_manager->get_bufferPoolManager());

    auto log_replay = std::make_shared<LogReplay>(disk_manager.get() , sm_manager , mode); 
    auto log_manager = std::make_shared<LogManager>(disk_manager.get(), log_replay.get());

    if (mode == "sql"){
      auto server = std::make_shared<Server>(machine_id, local_rpc_port, local_meta_port, use_rdma, 
                      compute_node_num, compute_ip_list, compute_ports_list,
                      disk_manager.get(), log_manager.get(), rm_manager.get(), sm_manager);
    } else {
      // 如果是负载模式，那就硬加载数据到存储里
      LoadData(machine_id, machine_num, mode, rm_manager.get());
      auto server = std::make_shared<Server>(machine_id, local_rpc_port, local_meta_port, use_rdma, 
                      compute_node_num, compute_ip_list, compute_ports_list,
                      disk_manager.get(), log_manager.get(), rm_manager.get(), mode);
    }
    
    return 0;
}
