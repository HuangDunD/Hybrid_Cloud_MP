#pragma once

#include <atomic>
#include <cstdint>
#include <set>
#include <vector>

#include "common.h"

extern std::atomic<uint64_t> tx_id_generator;

extern std::vector<double> lock_durations;
extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> ab_rate;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;
extern std::set<double> fetch_remote_vec;
extern std::set<double> fetch_all_vec;
extern std::set<double> lock_remote_vec;
extern std::set<double> fetch_from_remote_vec;
extern std::set<double> fetch_from_storage_vec;
extern std::set<double> fetch_from_local_vec;
extern std::set<double> evict_page_vec;
extern std::set<double> fetch_three_vec;
extern std::set<double> fetch_four_vec;
extern std::set<double> total_outputs;
extern std::vector<uint64_t> total_try_times;
extern std::vector<uint64_t> total_commit_times;

extern double all_time;
extern double tx_begin_time;
extern double tx_exe_time;
extern double tx_commit_time;
extern double tx_abort_time;
extern double tx_update_time;
extern double tx_get_timestamp_time1;
extern double tx_get_timestamp_time2;
extern double tx_write_commit_log_time;
extern double tx_write_commit_log_time2;
extern double tx_write_prepare_log_time;
extern double tx_write_backup_log_time;
extern double tx_fetch_exe_time;
extern double tx_fetch_commit_time;
extern double tx_fetch_abort_time;
extern double tx_release_exe_time;
extern double tx_release_commit_time;
extern double tx_release_abort_time;

extern int single_txn;
extern int distribute_txn;
