// Author: Chunyue Huang
// Copyright (c) 2024

#include "worker/handler.h"
#include "worker/worker.cc" // 包含worker.cc文件
#include <brpc/channel.h>
#include <thread>
#include <iomanip>

extern int single_txn, distribute_txn;

// Entrance to run threads that spawn coroutines as coordinators to run distributed transactions
int main(int argc, char* argv[]) {

    std::string log_path = "./computeserver.log" + std::to_string(getpid()); // 设置日志路径

    if (std::ifstream(log_path)) { std::remove(log_path.c_str()); }
    ::logging::LoggingSettings log_setting;  // 创建LoggingSetting对象进行设置
    log_setting.log_file = log_path.c_str(); // 设置日志路径
    log_setting.logging_dest = logging::LOG_TO_FILE; // 设置日志写到文件，不写的话不生效
    ::logging::InitLogging(log_setting);     // 应用日志设置

    if (argc == 3){
        // SQL 模式，需要以下几个参数： 
        // 1. 当前节点 id
        // 2. 打开的数据库
        Handler handler;
        handler.ConfigureComputeNodeRunSQL();

        int node_id = std::stoi(argv[1]);
        std::string db_name = std::string(argv[2]);

        handler.StartDatabaseSQL(node_id , thread_num , SYSTEM_MODE , db_name);
    }else if (argc == 7) {
        // 负载运行模式
        Handler handler;
        handler.ConfigureComputeNodeRunBench(argc, argv);
        handler.GenThreads(std::string(argv[1]));

        std::cout << "Time taken by function: " << all_time / thread_num_per_node << "s" << std::endl;
        double throughtput = 0;
        for(auto tp: tp_vec) {
            throughtput += tp;
        }
        std::cout << "Throughtput: " << throughtput << " Transactions Per Second\n";
        // double fetch_remote_ratio = *fetch_remote_vec.rbegin() / *fetch_all_vec.rbegin();
        // std::cout << "Fetch Page From remote ratio: " << fetch_remote_ratio << std::endl;
        double lock_ratio = *lock_remote_vec.rbegin() / *fetch_all_vec.rbegin();
        std::cout << "Lock ratio: " << lock_ratio << std::endl;
        
        // 从远程计算节点、存储节点和本地缓存拉取的统计
        double total_fetch = *fetch_from_remote_vec.rbegin() + *fetch_from_storage_vec.rbegin() + *fetch_from_local_vec.rbegin();
        double from_remote_ratio = total_fetch > 0 ? *fetch_from_remote_vec.rbegin() / total_fetch : 0;
        double from_storage_ratio = total_fetch > 0 ? *fetch_from_storage_vec.rbegin() / total_fetch : 0;
        double from_local_ratio = total_fetch > 0 ? *fetch_from_local_vec.rbegin() / total_fetch : 0;
        const auto fetch_remote_cnt = static_cast<long long>(*fetch_from_remote_vec.rbegin());
        const auto fetch_storage_cnt = static_cast<long long>(*fetch_from_storage_vec.rbegin());
        const auto fetch_local_cnt = static_cast<long long>(*fetch_from_local_vec.rbegin());
        std::cout << std::fixed << std::setprecision(2);
        if (SYSTEM_MODE != 2){
            std::cout << "Fetch Page from remote compute count: " << fetch_remote_cnt << " (" << from_remote_ratio * 100 << "%)" << std::endl;
            std::cout << "Fetch Page from storage count : " << fetch_storage_cnt << " (" << from_storage_ratio * 100 << "%)" << std::endl;
            std::cout << "Fetch Page from local BufferPool count : " << fetch_local_cnt << " (" << from_local_ratio * 100 << "%)" << std::endl;
        }
        std::cout << "Evicted pages: " << *evict_page_vec.rbegin() << std::endl;
        
        if(std::string(argv[1]) == "smallbank") {
            for (int i = 0; i < SmallBank_TX_TYPES; i++) {
                std::cout << "SmallBank Type : " << SmallBank_TX_NAME[i] << " Total Transactions : " << total_try_times[i] << " Commit Transactions : " << total_commit_times[i] << " Abort Ratio : " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
            }
        } else if(std::string(argv[1]) == "tpcc") {
            for (int i = 0; i < TPCC_TX_TYPES; i++) {
                std::cout << "TPCC : " << TPCC_TX_NAME[i] << " Total Transactions : " << total_try_times[i] << " Commit Transactions : " << total_commit_times[i] << " Abort Ratio : " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
            }
        } else if(std::string(argv[1]) == "ycsb") {
            for (int i = 0; i < YCSB_TX_TYPES; i++) {
                std::cout << "YCSB : Total Run Transaction : " << total_try_times[i] << " Commit Transactions : " << total_commit_times[i] << " Abort Ratio : " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
            }
        }else {
            assert(false);
        }

        std::cout << "tx_begin_time: " << tx_begin_time << std::endl;
        std::cout << "tx_exe_time: " << tx_exe_time << std::endl;
        double wait_log_flush_time = (double)global_wait_log_flush_time_ns / 1000000000.0;
        std::cout << "wait_log_flush_time: " << wait_log_flush_time << std::endl;
        std::cout << "wait_log_flush_push_page_time: " << (double)global_wait_log_flush_push_page_time_ns / 1000000000.0 << std::endl;
        std::cout << "wait_log_flush_count: " << global_wait_log_flush_count << std::endl;
        std::cout << "ownership_transfer_count: " << ownership_transfer_count << std::endl;
        std::cout << "ownership_transfer_time_total: " << (double)ownership_transfer_time_total / 1000000000.0 << std::endl;
        double avg_ownership_transfer_time_ms = 0.0;
        if (ownership_transfer_count > 0) {
            avg_ownership_transfer_time_ms = ((double)ownership_transfer_time_total / (double)ownership_transfer_count) / 1000000.0;
        }
        std::cout << "ownership_transfer_time_avg_ms: " << avg_ownership_transfer_time_ms << std::endl;
        std::cout << "notify_push_page_count: " << global_notify_push_page_count << std::endl;
        std::cout << "notify_push_page_time: " << (double)global_notify_push_page_time_ns / 1000000000.0 << std::endl;
        std::cout << "log_flush_count: " << global_log_flush_count << std::endl;
        std::cout << "log_flush_time: " << (double)global_log_flush_total_time_ns / 1000000000.0  << std::endl;
        std::cout << "log_flush_to_lock_done_time: " << (double)global_log_flush_to_lock_done_time_ns / 1000000000.0 << std::endl;
        std::cout << "log_flush_to_max_lsn_time: " << (double)global_log_flush_to_max_lsn_time_ns / 1000000000.0 << std::endl;
        std::cout << "log_flush_to_serialize_done_time: " << (double)global_log_flush_to_serialize_done_time_ns / 1000000000.0 << std::endl;
        std::cout << "log_flush_storage_rpc_time: " << (double)global_log_flush_storage_rpc_time_ns / 1000000000.0 << std::endl;
        std::cout << "log_flush_update_persist_lsn_time: " << (double)global_log_flush_update_persist_lsn_time_ns / 1000000000.0 << std::endl;
        std::cout << "log_flush_avg_batch_size: " << (global_log_flush_count > 0 ? (double)global_log_flush_total_batch_size / global_log_flush_count : 0) << std::endl;
        std::cout << "log_flush_max_batch_size: " << global_log_flush_max_batch_size << std::endl;
        std::cout << "log_flush_total_count: " << global_log_flush_total_batch_size << std::endl; 

        std::cout << "tx_commit_time: " << tx_commit_time << std::endl;
        std::cout << "tx_abort_time: " << tx_abort_time << std::endl;
        std::cout << "TxWaitAbortLogTime: " << TxWaitAbortLogTime << std::endl;
        std::cout << "commit_log_count: " << global_commit_log_count << std::endl;
        std::cout << "prepare_log_count: " << global_prepare_log_count << std::endl;
        std::cout << "backup_log_count: " << global_backup_log_count << std::endl;
        std::cout << "update_log_count: " << global_update_log_count << std::endl;
        std::cout << "lazy_getpage_dire: " << lazy_getpage_dire << std::endl;
        std::cout << "lazy_getpage_wait: " << lazy_getpage_wait << std::endl;
        std::cout << std::defaultfloat;

        std::ofstream result_file("result.txt");

        result_file << "total_time_seconds=" << all_time / thread_num_per_node <<std::endl;
        result_file << "throughput=" << throughtput << std::endl;
        // result_file << fetch_remote_ratio << std::endl;
        result_file << "lock_ratio=" << lock_ratio << std::endl;
        result_file << "fetch_from_remote_count=" << *fetch_from_remote_vec.rbegin() << std::endl;
        result_file << "fetch_from_storage_count=" << *fetch_from_storage_vec.rbegin() << std::endl;
        result_file << "fetch_from_local_count=" << *fetch_from_local_vec.rbegin() << std::endl;
        result_file << "evicted_pages_count=" << *evict_page_vec.rbegin() << std::endl;
        result_file << "from_remote_ratio=" << from_remote_ratio << std::endl;
        result_file << "from_storage_ratio=" << from_storage_ratio << std::endl;
        result_file << "from_local_ratio=" << from_local_ratio << std::endl;
        // result_file << p50_latency << std::endl;
        // result_file << p90_latency << std::endl;
        if(std::string(argv[1]) == "smallbank") {
            for (int i = 0; i < SmallBank_TX_TYPES; i++) {
                result_file << SmallBank_TX_NAME[i] << "_try_commit=" << total_try_times[i] << " " << total_commit_times[i] << std::endl;
            }
            for (int i = 0; i < SmallBank_TX_TYPES; i++) {
                double rr = 0.0;
                if (total_try_times[i] > 0) {
                    rr = (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i];
                }
                result_file << SmallBank_TX_NAME[i] << "_rollback_rate=" << rr << std::endl;
            }
        } else if(std::string(argv[1]) == "tpcc") {
            for (int i = 0; i < TPCC_TX_TYPES; i++) {
                result_file << TPCC_TX_NAME[i] << "_try_commit=" << total_try_times[i] << " " << total_commit_times[i] << std::endl;
            }
            for (int i = 0; i < TPCC_TX_TYPES; i++) {
                double rr = 0.0;
                if (total_try_times[i] > 0) {
                    rr = (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i];
                }
                result_file << TPCC_TX_NAME[i] << "_rollback_rate=" << rr << std::endl;
            }
        } else if(std::string(argv[1]) == "ycsb") {
            for (int i = 0; i < YCSB_TX_TYPES; i++) {
                result_file << "ycsb_tx" << "_try_commit=" << total_try_times[i] << " " << total_commit_times[i] << std::endl;
            }
            for (int i = 0; i < YCSB_TX_TYPES; i++) {
                double rr = 0.0;
                if (total_try_times[i] > 0) {
                    rr = (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i];
                }
                result_file << "ycsb_tx" << i << "_rollback_rate=" << rr << std::endl;
            }
        } else {
            assert(false);
        }

        result_file << "tx_begin_time=" << tx_begin_time << std::endl;
        result_file << "tx_exe_time=" << tx_exe_time << std::endl;
        result_file << "tx_fetch_exe_time=" << tx_fetch_exe_time << std::endl;
        result_file << "wait_log_flush_time=" << wait_log_flush_time << std::endl;
        result_file << "wait_log_flush_push_page_time=" << (double)global_wait_log_flush_push_page_time_ns / 1000000000.0 << std::endl;
        result_file << "wait_log_flush_count=" << global_wait_log_flush_count << std::endl;
        result_file << "ownership_transfer_count=" << ownership_transfer_count << std::endl;
        result_file << "ownership_transfer_time_total=" << (double)ownership_transfer_time_total / 1000000000.0 << std::endl;
        result_file << "notify_push_page_count=" << global_notify_push_page_count << std::endl;
        result_file << "notify_push_page_time=" << (double)global_notify_push_page_time_ns / 1000000000.0 << std::endl;
        result_file << "log_flush_count=" << global_log_flush_count << std::endl;
        result_file << "log_flush_time=" << (double)global_log_flush_total_time_ns / 1000000000.0 << std::endl;
        result_file << "log_flush_to_lock_done_time=" << (double)global_log_flush_to_lock_done_time_ns / 1000000000.0 << std::endl;
        result_file << "log_flush_to_max_lsn_time=" << (double)global_log_flush_to_max_lsn_time_ns / 1000000000.0 << std::endl;
        result_file << "log_flush_to_serialize_done_time=" << (double)global_log_flush_to_serialize_done_time_ns / 1000000000.0 << std::endl;
        result_file << "log_flush_storage_rpc_time=" << (double)global_log_flush_storage_rpc_time_ns / 1000000000.0 << std::endl;
        result_file << "log_flush_update_persist_lsn_time=" << (double)global_log_flush_update_persist_lsn_time_ns / 1000000000.0 << std::endl;
        result_file << "log_flush_avg_batch=" << (global_log_flush_count > 0 ? (double)global_log_flush_total_batch_size / global_log_flush_count : 0) << std::endl;
        result_file << "log_flush_max_batch=" << global_log_flush_max_batch_size << std::endl;
        result_file << "log_flush_total_batch=" << global_log_flush_total_batch_size << std::endl;
        result_file << "tx_commit_time=" << tx_commit_time << std::endl;
        result_file << "tx_abort_time=" << tx_abort_time << std::endl;
        result_file << "TxWaitAbortLogTime=" << TxWaitAbortLogTime << std::endl;
        // result_file << tx_update_time << std::endl;
        result_file << "wait_commit_log_time=" << (double)global_wait_commit_log_time_ns / 1000000000.0 << std::endl;
        result_file << "wait_prepare_log_time=" << (double)global_wait_prepare_log_time_ns / 1000000000.0 << std::endl;
        result_file << "wait_backup_log_time=" << (double)global_wait_backup_log_time_ns / 1000000000.0 << std::endl;
        result_file << "commit_log_count=" << global_commit_log_count << std::endl;
        result_file << "prepare_log_count=" << global_prepare_log_count << std::endl;
        result_file << "backup_log_count=" << global_backup_log_count << std::endl;
        result_file << "tx_write_commit_log_time=" << tx_write_commit_log_time << std::endl;
        result_file << "tx_write_commit_log_time2=" << tx_write_commit_log_time2 << std::endl;
        result_file << "tx_write_prepare_log_time=" << tx_write_prepare_log_time << std::endl;
        result_file << "tx_write_backup_log_time=" << tx_write_backup_log_time << std::endl;
        result_file << "tx_get_timestamp_time1=" << tx_get_timestamp_time1 << std::endl;
        result_file << "tx_get_timestamp_time2=" << tx_get_timestamp_time2 << std::endl;
        result_file << "twopc_remote_fetch_time=" << (double)twopc_remote_fetch_time_ns / 1000000000.0 << std::endl;
        result_file << "twopc_remote_fetch_count=" << twopc_remote_fetch_count << std::endl;
        result_file << "update_log_count=" << global_update_log_count << std::endl;
        result_file << "single_txn_count=" << single_txn << std::endl;
        result_file << "distribute_txn_count=" << distribute_txn << std::endl;

        if (ownership_transfer_count > 0) {
            avg_ownership_transfer_time_ms = ((double)ownership_transfer_time_total / (double)ownership_transfer_count) / 1000000.0;
        }
        result_file << "ownership_transfer_time_avg_ms=" << avg_ownership_transfer_time_ms << std::endl;
        result_file << "lazy_getpage_dire=" << lazy_getpage_dire << std::endl;
        result_file << "lazy_getpage_wait=" << lazy_getpage_wait << std::endl;


        result_file.close();
    }else {
        std::cerr << "启动参数错误\n";
        assert(false);
    }


}
