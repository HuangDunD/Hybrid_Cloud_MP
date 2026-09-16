#include "benchmark_stats.h"

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
std::set<double> fetch_three_vec;
std::set<double> fetch_four_vec;
std::set<double> total_outputs;
std::vector<double> lock_durations;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;

double all_time = 0;
double tx_begin_time = 0;
double tx_exe_time = 0;
double tx_commit_time = 0;
double tx_abort_time = 0;
double tx_update_time = 0;
double tx_get_timestamp_time1 = 0;
double tx_get_timestamp_time2 = 0;
double tx_write_commit_log_time = 0;
double tx_write_commit_log_time2 = 0;
double tx_write_prepare_log_time = 0;
double tx_write_backup_log_time = 0;
double tx_fetch_exe_time = 0;
double tx_fetch_commit_time = 0;
double tx_fetch_abort_time = 0;
double tx_release_exe_time = 0;
double tx_release_commit_time = 0;
double tx_release_abort_time = 0;

int single_txn = 0;
int distribute_txn = 0;
