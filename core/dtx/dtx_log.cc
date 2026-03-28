// Author: huangdund
// Copyright (c) 2023

#include <brpc/channel.h>
#include <cstdlib>
#include <cstring>
#include "data_item.h"
#include "dtx/dtx.h"
#include "storage/storage_service.pb.h"
#include "storage/log_record.h" 
#include "record/record.h"

static void LogOnRPCDone(storage_service::LogWriteResponse* response, brpc::Controller* cntl) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<storage_service::LogWriteResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        // RPC失败了. response里的值是未定义的，勿用。
        LOG(ERROR) << "Fail to send log: " << cntl->ErrorText();
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理。
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

// 把一条表示事务结束的日志加入到日志集合中
void DTX::TxOver(LLSN commit_lsn){
    cnt_commit_log++;
    assert(txn_log != nullptr);
    // commit log
    BatchEndLogRecord* batch_end_log = new BatchEndLogRecord(txn_log->batch_id_, global_meta_man->local_machine_id, tx_id);
    // LLSN log_lsn = compute_server->generate_next_llsn_with_lock();
    batch_end_log->lsn_ = commit_lsn;
    compute_server->AddToLog(batch_end_log); 

    // 等待 Commit Log 落盘
    compute_server->wait_log_flush(commit_lsn);
}


// Build a new-page log and stash it into temp_log
NewPageLogRecord* DTX::GenNewPageLog(table_id_t table_id,
                                     int request_pages) {
    assert(false);

    assert(txn_log != nullptr);
    std::string table_name = compute_server->getTableNameByTableID(table_id);

    NewPageLogRecord* log = new NewPageLogRecord(txn_log->batch_id_,
                                                 global_meta_man->local_machine_id,
                                                 tx_id,
                                                 table_id,
                                                 table_name,
                                                 request_pages);
    // 同时写入节点共享的log_records和事务的txn_log
    compute_server->AddToLog(log);  // 写入节点共享的log_records
    // txn_log->logs.push_back(log);   // 也写入txn_log，用于事务提交时发送
    return log;
}

FSMUpdateLogRecord* DTX::GenFSMUpdateLog(table_id_t table_id,
                                         uint32_t page_id,
                                         uint32_t free_space,
                                         const std::string& table_name) {
    assert(txn_log != nullptr);

    auto* log = new FSMUpdateLogRecord(txn_log->batch_id_,
                                       global_meta_man->local_machine_id,
                                       tx_id,
                                       table_id,
                                       table_name,
                                       page_id,
                                       free_space);
    txn_log->logs.push_back(log);
    return log;
}
 
    
