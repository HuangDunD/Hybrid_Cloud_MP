// Author: Chunyue Huang
// Copyright (c) 2024

#pragma once

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>
#include "config.h"
#include "common.h"

extern int global_txn_participants_1;
extern int global_txn_participants_multi;

extern std::atomic<int64_t> global_commit_log_count;
extern std::atomic<int64_t> global_prepare_log_count;
extern std::atomic<int64_t> global_backup_log_count;

class Handler {
 public:
  Handler() {}
  // For macro-benchmark
  static void ConfigureComputeNodeRunBench(int argc, char* argv[]);
  static void ConfigureComputeNodeRunSQL();
  void GenThreads(std::string bench_name);
  static void StartDatabaseSQL(node_id_t node_id , int thread_num , int sys_mode , const std::string db_name);
  void OutputResult(std::string bench_name, std::string system_name);


};
