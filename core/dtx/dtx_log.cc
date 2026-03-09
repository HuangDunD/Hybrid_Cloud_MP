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
void DTX::TxOver(){
    assert(txn_log != nullptr);
    // commit log
    BatchEndLogRecord* batch_end_log = new BatchEndLogRecord(txn_log->batch_id_, global_meta_man->local_machine_id, tx_id);
    
    LLSN log_lsn = compute_server->generate_next_llsn_with_lock();
    batch_end_log->lsn_ = log_lsn;

    compute_server->AddToLog(batch_end_log); 

    // 最后，需要等这个事务相关的日志全都落盘
    // compute_server->wait_log_flush(max_lsn);
    // 等待 Commit Log 落盘
    compute_server->wait_log_flush(log_lsn);
    max_lsn = 0;
}

// Build a unified update log and stash it into temp_log
LLSN DTX::GenUpdateLog(DataItem* item,
                                   itemkey_t *key,
                                   Rid rid,
                                   const void* value,
                                   RmPageHdr* pagehdr) {
    assert(txn_log != nullptr);

    const size_t item_size = item->GetSerializeSize();
    char* item_buf = (char*)malloc(item_size);
    memcpy(item_buf, (char*)item, sizeof(DataItem));
    memcpy(item_buf + sizeof(DataItem), value, item->value_size);

    itemkey_t pri_key;
    // 如果这个表没有设置主键，那主键位置填充负无穷
    if (key == nullptr){
        // 负无穷
        pri_key = (itemkey_t)(-1);
    }else{
        pri_key = *key;
    }
    RmRecord new_record(pri_key, item_size, item_buf);
    free(item_buf);

    std::string table_name = compute_server->getTableNameByTableID(item->table_id);

    UpdateLogRecord* log = new UpdateLogRecord(txn_log->batch_id_,
                                               global_meta_man->local_machine_id,
                                               tx_id,
                                               new_record,
                                               rid,
                                               table_name,
                                               nullptr);
    log->prev_lsn_ = pagehdr->LLSN_;
    LLSN lsn = compute_server->UpdatePageLLSN(pagehdr);
    log->lsn_ = lsn;
    compute_server->AddToLogNoBlock(log);

    assert(max_lsn <= lsn);
    max_lsn = lsn;
    return log->lsn_;
}

LLSN DTX::GenInsertLog(DataItem* item,
                                  itemkey_t* key,
                                  const void* value,
                                  const Rid& rid,
                                  RmPageHdr* pagehdr) {
    assert(txn_log != nullptr);

    const size_t item_size = item->GetSerializeSize();
    char* item_buf = (char*)malloc(item_size);
    memcpy(item_buf, (char*)item, sizeof(DataItem));
    memcpy(item_buf + sizeof(DataItem), value, item->value_size);

    itemkey_t pri_key;
    if (key == nullptr){
        pri_key = (itemkey_t)(-1);
    }else{
        pri_key = *key;
    }

    RmRecord new_record(pri_key, item_size, item_buf);
    free(item_buf);

    table_id_t table_id = item->table_id;
    std::string table_name = compute_server->getTableNameByTableID(table_id);

    InsertLogRecord* log = new InsertLogRecord(txn_log->batch_id_,
                                               global_meta_man->local_machine_id,
                                               tx_id,
                                               new_record,
                                               rid.page_no_,
                                               rid.slot_no_,
                                               table_name);
    log->prev_lsn_ = pagehdr->LLSN_;
    LLSN lsn = compute_server->UpdatePageLLSN(pagehdr);
    log->lsn_ = lsn;
    compute_server->AddToLogNoBlock(log);

    assert(max_lsn <= lsn);
    max_lsn = lsn;

    return log->lsn_;
}

LLSN DTX::GenDeleteLog(table_id_t table_id,
                                   itemkey_t* key,
                                   int page_no,
                                   int slot_no,
                                   RmPageHdr* pagehdr) {
    assert(txn_log != nullptr);
    std::string table_name = compute_server->getTableNameByTableID(table_id);

    DeleteLogRecord* log = new DeleteLogRecord(txn_log->batch_id_,
                                               global_meta_man->local_machine_id,
                                               tx_id,
                                               table_id,
                                               table_name,
                                               page_no,
                                               slot_no);
    log->prev_lsn_ = pagehdr->LLSN_;
    LLSN lsn = compute_server->UpdatePageLLSN(pagehdr);
    log->lsn_ = lsn;
    compute_server->AddToLogNoBlock(log);

    assert(max_lsn <= lsn);
    max_lsn = lsn;
    return log->lsn_;
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
 
    