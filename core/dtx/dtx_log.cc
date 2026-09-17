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
// 返回 false 表示刷新硬超时：BatchEnd 与数据日志仍在队列中由重试机制继续发送，
// 事务结局未知（数据已写入页面且锁已释放，无法回滚，也不能确认已持久）。
// 调用方必须把该事务按"结果未知"处理，不得当成功、也不得盲目重试。
bool DTX::AddLogToTxn(){
    bool flushed = true;
    if(txn_log == nullptr){
        txn_log = new TxnLog();
    }
    // commit log
    BatchEndLogRecord* batch_end_log = new BatchEndLogRecord(txn_log->batch_id_, global_meta_man->local_machine_id, tx_id);
    
    commit_log_ticket = compute_server->AddToLog(batch_end_log);
    if (!compute_server->WaitLogReceipt(commit_log_ticket)) {
        // 硬超时：日志仍在队列中由重试机制继续发送。此时数据已写入页面且锁已释放，
        // 事务无法回滚。结局未知：既不能向调用方确认提交，也不能重试。
        flushed = false;
        LOG(ERROR) << "TxCommit log persistence timeout, tx_id=" << tx_id
                   << " max_lsn=" << max_lsn << " (logs remain queued for retry) OUTCOME=UNKNOWN";
    }
    max_lsn = 0;
    return flushed;
}

bool DTX::AddAbortEndToTxn() {
    if (!txn_log) txn_log = new TxnLog();
    auto* end = new AbortLogRecord(txn_log->batch_id_, global_meta_man->local_machine_id, tx_id);
    end->log_type_ = LogType::ABORTEND;
    commit_log_ticket = compute_server->AddToLog(end);
    if (!compute_server->WaitLogReceipt(commit_log_ticket)) {
        tx_status = TXStatus::TX_UNKNOWN;
        return false;
    }
    return true;
}

// Build a unified update log and stash it into temp_log
LLSN DTX::GenUpdateLog(DataItem* item,
                                   itemkey_t *key,
                                   Rid rid,
                                   const void* value,
                                   RmPageHdr* pagehdr,
                                   const RmRecord* old_value) {
    //return 0;
    if (txn_log == nullptr) {
        txn_log = new TxnLog();
    }
    
    const size_t item_size = item->GetSerializeSize();
    char* item_buf = (char*)malloc(item_size);
    memcpy(item_buf, (char*)item, sizeof(DataItem));
    memcpy(item_buf + sizeof(DataItem), value, item->value_size);

    itemkey_t pri_key;
    if (key == nullptr){
        // 负无穷
        pri_key = (itemkey_t)(-1);
    }else{
        pri_key = *key;
    }
    RmRecord new_record(pri_key, item_size, item_buf);
    free(item_buf);

    std::string table_name;
    table_id_t table_id = item->table_id;
    // SQL 模式下，通过 db_meta 获取表名字
    if (WORKLOAD_MODE == 4){
        // B+ 树存在 10000 - 20000，FSM 存在 20000 到 30000
        int tab_id = 0;
        if (table_id < 10000){
            tab_id = table_id;
        }else if (table_id < 20000){
            tab_id = table_id - 10000;
        }else if (table_id < 30000){
            tab_id = table_id - 20000;
        }else {
            assert(false);
        }

        std::string tab_name = compute_server->getTableNameFromTableID(tab_id);
        assert(tab_name != "");

        if (table_id >= 10000 && table_id < 20000){
            tab_name += "_bl";
        }else if (table_id >= 20000 && table_id < 30000){
            tab_name += "_fsm";
        }

        table_name = tab_name;
    }else{
        table_name = compute_server->table_name_meta[table_id];
    }

    UpdateLogRecord* log = new UpdateLogRecord(txn_log->batch_id_,
                                               global_meta_man->local_machine_id,
                                               tx_id,
                                               new_record,
                                               rid,
                                               table_name,
                                               old_value);
    log->prev_lsn_ = pagehdr->LLSN_;
    LLSN lsn = compute_server->UpdatePageLLSN(pagehdr);
    log->lsn_ = lsn;
    compute_server->AddToLogNoBlock(log);

    // LOG(INFO) << "GenUpdateLog , table_id = " << table_id << " page_id = " << rid.page_no_ << " slot_no = " << rid.slot_no_ << " new lsn = " << lsn;
    assert(max_lsn <= lsn);
    max_lsn = lsn;
    return lsn;
}

LLSN DTX::GenInsertLog(DataItem* item,
                                  itemkey_t* key,
                                  const void* value,
                                  const Rid& rid,
                                  RmPageHdr* pagehdr) {
    // std::cout << "生成insert日志"<< std::endl;
    if (txn_log == nullptr) {
        txn_log = new TxnLog();
    }

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
    std::string table_name;
    // SQL 模式下，通过 db_meta 获取表名字
    if (WORKLOAD_MODE == 4){
        // B+ 树存在 10000 - 20000，FSM 存在 20000 到 30000
        int tab_id = 0;
        if (table_id < 10000){
            tab_id = table_id;
        }else if (table_id < 20000){
            tab_id = table_id - 10000;
        }else if (table_id < 30000){
            tab_id = table_id - 20000;
        }else {
            assert(false);
        }

        std::string tab_name = compute_server->getTableNameFromTableID(tab_id);
        assert(tab_name != "");

        if (table_id >= 10000 && table_id < 20000){
            tab_name += "_bl";
        }else if (table_id >= 20000 && table_id < 30000){
            tab_name += "_fsm";
        }

        table_name = tab_name;
    }else{
        table_name = compute_server->table_name_meta[table_id];
    }

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
    return lsn;
}

LLSN DTX::GenDeleteLog(table_id_t table_id,
                                   itemkey_t* key,
                                   int page_no,
                                   int slot_no,
                                   RmPageHdr* pagehdr) {
    std::string table_name;
    // SQL 模式下，通过 db_meta 获取表名字
    if (WORKLOAD_MODE == 4){
        // B+ 树存在 10000 - 20000，FSM 存在 20000 到 30000
        int tab_id = 0;
        if (table_id < 10000){
            tab_id = table_id;
        }else if (table_id < 20000){
            tab_id = table_id - 10000;
        }else if (table_id < 30000){
            tab_id = table_id - 20000;
        }else {
            assert(false);
        }

        std::string tab_name = compute_server->getTableNameFromTableID(tab_id);
        assert(tab_name != "");

        if (table_id >= 10000 && table_id < 20000){
            tab_name += "_bl";
        }else if (table_id >= 20000 && table_id < 30000){
            tab_name += "_fsm";
        }

        table_name = tab_name;
    }else{
        table_name = compute_server->table_name_meta[table_id];
    }

    if (txn_log == nullptr) {
        txn_log = new TxnLog();
    }

    DeleteLogRecord* log = new DeleteLogRecord(txn_log->batch_id_,
                                               global_meta_man->local_machine_id,
                                               tx_id,
                                               table_id,
                                               table_name,
                                               page_no,
                                               slot_no);
    if (WORKLOAD_MODE == 2 && key) {
        const auto header = compute_server->get_file_hdr_cached(table_id);
        const int bucket = static_cast<int>(sizeof(RmPageHdr)) + slot_no / BITMAP_WIDTH;
        const char current = reinterpret_cast<const char*>(pagehdr)[bucket];
        RmPageHdr before = *pagehdr;
        ++before.num_records_;
        const char undo_bucket = static_cast<char>(current | (1u << (7 - slot_no % BITMAP_WIDTH)));
        log->set_meta(bucket, current, *pagehdr, header->first_free_page_no_,
                      undo_bucket, before, header->first_free_page_no_);
    }
    log->prev_lsn_ = pagehdr->LLSN_;
    LLSN lsn = compute_server->UpdatePageLLSN(pagehdr);
    log->lsn_ = lsn;

    compute_server->AddToLogNoBlock(log);

    assert(max_lsn <= lsn);
    max_lsn = lsn;
    return lsn;
}

// 表名解析：与 GenUpdateLog/GenInsertLog/GenDeleteLog 的规则一致。
// SQL 模式（WORKLOAD_MODE==4）中 blink 表为 table_id 10000~20000（表名+"_bl"），
// FSM 表为 20000~30000（表名+"_fsm"）；其它模式直接用 table_name_meta
static std::string ResolveTableNameForLog(ComputeServer *compute_server, table_id_t table_id) {
    if (WORKLOAD_MODE == 4){
        // B+ 树存在 10000 - 20000，FSM 存在 20000 到 30000
        int tab_id = 0;
        if (table_id < 10000){
            tab_id = table_id;
        }else if (table_id < 20000){
            tab_id = table_id - 10000;
        }else if (table_id < 30000){
            tab_id = table_id - 20000;
        }else {
            assert(false);
        }

        std::string tab_name = compute_server->getTableNameFromTableID(tab_id);
        assert(tab_name != "");

        if (table_id >= 10000 && table_id < 20000){
            tab_name += "_bl";
        }else if (table_id >= 20000 && table_id < 30000){
            tab_name += "_fsm";
        }

        return tab_name;
    }
    return compute_server->table_name_meta[table_id];
}

LLSN DTX::GenFSMUpdateLog(table_id_t table_id,
                                         uint32_t page_id,
                                         uint32_t free_space,
                                         uint32_t old_free_space,
                                         const std::string& table_name) {
    if (txn_log == nullptr) {
        txn_log = new TxnLog();
    }

    auto* log = new FSMUpdateLogRecord(txn_log->batch_id_,
                                       global_meta_man->local_machine_id,
                                       tx_id,
                                       table_id,
                                       table_name,
                                       page_id,
                                       free_space,
                                       old_free_space);
    // 与其它日志一致：分配 LLSN 并进入节点共享队列，事务提交时
    // wait_log_flush_v2(max_lsn) 保证 FSMUPDATE 随事务一起持久化。
    // 注意：GenLogLSN 持有 log_mtx，由 AddToLogNoBlock 内部解锁
    log->prev_lsn_ = INVALID_LSN;   // 逻辑日志，不挂载页面 LLSN 链
    const LLSN lsn = compute_server->GenLogLSN();
    log->lsn_ = lsn;
    compute_server->AddToLogNoBlock(log);

    assert(max_lsn <= lsn);
    max_lsn = lsn;
    return lsn;
}

LLSN DTX::GenBLinkInsertLog(table_id_t blink_table_id, const itemkey_t &key, const Rid &rid) {
    if (txn_log == nullptr) {
        txn_log = new TxnLog();
    }

    std::string table_name = ResolveTableNameForLog(compute_server, blink_table_id);
    auto* log = new BLinkInsertLogRecord(txn_log->batch_id_,
                                         global_meta_man->local_machine_id,
                                         tx_id,
                                         blink_table_id,
                                         table_name,
                                         key,
                                         rid);
    log->prev_lsn_ = INVALID_LSN;   // 逻辑日志，blink 页面无 LLSN 字段
    const LLSN lsn = compute_server->GenLogLSN();
    log->lsn_ = lsn;
    compute_server->AddToLogNoBlock(log);

    assert(max_lsn <= lsn);
    max_lsn = lsn;
    return lsn;
}

LLSN DTX::GenBLinkDeleteLog(table_id_t blink_table_id, const itemkey_t &key, const Rid &rid) {
    if (txn_log == nullptr) {
        txn_log = new TxnLog();
    }

    std::string table_name = ResolveTableNameForLog(compute_server, blink_table_id);
    auto* log = new BLinkDeleteLogRecord(txn_log->batch_id_,
                                         global_meta_man->local_machine_id,
                                         tx_id,
                                         blink_table_id,
                                         table_name,
                                         key,
                                         rid);
    log->prev_lsn_ = INVALID_LSN;
    const LLSN lsn = compute_server->GenLogLSN();
    log->lsn_ = lsn;
    compute_server->AddToLogNoBlock(log);

    assert(max_lsn <= lsn);
    max_lsn = lsn;
    return lsn;
}

void DTX::UpdateFSMWithLog(table_id_t table_id, uint32_t page_id, uint32_t free_space) {
    // 先修改 FSM 页面（X 锁页面），返回旧空间值；类别未变则无需日志
    uint32_t old_free = compute_server->update_page_space(table_id, page_id, free_space);
    if (old_free == 0xFFFFFFFFu) {
        return;
    }
    const table_id_t fsm_table_id = table_id + 20000;
    std::string fsm_table_name = ResolveTableNameForLog(compute_server, fsm_table_id);
    GenFSMUpdateLog(fsm_table_id, page_id, free_space, old_free, fsm_table_name);
}

// 把这个事务的全部日志序列化成一个字符串，写入到存储层中
void DTX::SendLogToStoragePool(uint64_t bid, brpc::CallId* cid, int urgent){
    // 添加模拟延迟
    //usleep(100); // 100us
    storage_service::StorageService_Stub stub(storage_log_channel);
    brpc::Controller* cntl = new brpc::Controller();
    storage_service::LogWriteRequest request;
    storage_service::LogWriteResponse* response = new storage_service::LogWriteResponse();

    txn_log->batch_id_ = bid;
    // std::cout << "发送日志，batch_id: " << bid << std::endl;
    request.set_log(txn_log->get_log_string());
    request.set_urgent(urgent);

    // 在这里改成异步发送
    *cid = cntl->call_id();

    stub.LogWrite(cntl, &request, response, brpc::NewCallback(LogOnRPCDone, response, cntl));

    // ! 在程序外部同步

    // clear the logs
    txn_log->logs.clear();
}
 
    