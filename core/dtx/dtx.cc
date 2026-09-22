// Author: Chunyue Huang
// Copyright (c) 2024

#include "dtx/dtx.h"
#include "config.h"
#include "workload/ycsb/ycsb_db.h"
#include "record.h"

DTX::DTX(ComputeServer *server , brpc::Channel *data_channel , brpc::Channel *log_channel , brpc::Channel *server_channel , TxnLog *txn_l2og){
    tx_id = 0;
    compute_server = server;
    tx_status = TXStatus::TX_INIT;

    storage_data_channel = data_channel; 
    storage_log_channel = log_channel; 
    remote_server_channel = server_channel;
    txn_log = txn_l2og;
}

DTX::DTX(MetaManager* meta_man,
         t_id_t tid,
         t_id_t l_tid,
         coro_id_t coroid,
         CoroutineScheduler* sched,
         IndexCache* _index_cache,
         PageCache* _page_cache,
         ComputeServer* server,
         brpc::Channel* data_channel, 
         brpc::Channel* log_channel,
         brpc::Channel* server_channel,
         ThreadPool* thd_pool,
         TxnLog* txn_log0,
         CoroutineScheduler* sched_0,
         int* using_which_coro_sched_) {
  // Transaction setup
  tx_id = 0;
  t_id = tid;           // thread_ID(Gloabl)
  local_t_id = l_tid;   // thread_ID(Local)
  coro_id = coroid;
  coro_sched = sched;
  global_meta_man = meta_man;
  compute_server = server;
  tx_status = TXStatus::TX_INIT;
  // thread_remote_log_offset_alloc = remote_log_offset_allocator;
  index_cache = _index_cache;
  page_cache = _page_cache;

  storage_data_channel = data_channel; 
  storage_log_channel = log_channel; 
  remote_server_channel = server_channel;
  txn_log = txn_log0;
  thread_pool = thd_pool;
}

/*
    每个事务都需要一个开始时间戳
    如果每个事务都像远程请求一个全局时间戳，那开销太大了
    因此设置一个 BatchTimeStamp，让远程给我分配 100 个连续的时间戳
    然后我内部自己再去消化这 100 个时间戳
*/
timestamp_t DTX::GetTimestampRemote() {
  timestamp_t ret;
  if(local_timestamp % BatchTimeStamp != 0){
    ret = local_timestamp++;
    return ret;
  }                         
  // Get timestamp from remote
  timestamp_service::TimeStampService_Stub stub(remote_server_channel);
  timestamp_service::GetTimeStampRequest request;
  timestamp_service::GetTimeStampResponse response;
  brpc::Controller cntl;
  assert(remote_server_channel);
  stub.GetTimeStamp(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Fail to get timestamp from remote";
    return 0;
  }
  local_timestamp = response.timestamp() * BatchTimeStamp;
  ret = local_timestamp++;
  return ret;
}

void DTX::ReleaseSPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id){
    if(SYSTEM_MODE == 0) {
        compute_server->rpc_release_s_page(table_id,page_id);
    } else if(SYSTEM_MODE == 1){
        compute_server->rpc_lazy_release_s_page(table_id,page_id);
    }else if(SYSTEM_MODE == 2){
        compute_server->local_release_s_page(table_id,page_id);
    }else if(SYSTEM_MODE == 3){
        compute_server->single_release_s_page(table_id,page_id);
    }else if(SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
        compute_server->rpc_ts_release_s_page(table_id , page_id);
    }else assert(false);
}

void DTX::ReleaseXPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id){
   if(SYSTEM_MODE == 0) {
        compute_server->rpc_release_x_page(table_id,page_id);
    } 
    else if(SYSTEM_MODE == 1){
        compute_server->rpc_lazy_release_x_page(table_id,page_id);
    }
    else if(SYSTEM_MODE == 2){
        compute_server->local_release_x_page(table_id,page_id);
    }
    else if(SYSTEM_MODE == 3){
        compute_server->single_release_x_page(table_id,page_id);
    }else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
        compute_server->rpc_ts_release_x_page(table_id , page_id);
        // if (compute_server->is_hot_page(table_id, page_id)){
        //     compute_server->rpc_lazy_release_x_page(table_id , page_id);
        // } else {
        //     compute_server->rpc_ts_release_x_page(table_id , page_id);
        // }
    }
    else assert(false);
    
}

DataItem* DTX::GetDataItemFromPageRO(table_id_t table_id, char* data, Rid rid , RmFileHdr::ptr file_hdr , itemkey_t& item_key){
    if (rid.page_no_ <= 0 || rid.slot_no_ < 0 || rid.slot_no_ >= file_hdr->num_records_per_page_ ||
        file_hdr->record_size_ < static_cast<int>(sizeof(DataItem)) || file_hdr->bitmap_size_ <= 0 ||
        sizeof(RmPageHdr) + file_hdr->bitmap_size_ +
            static_cast<uint64_t>(file_hdr->num_records_per_page_) * (file_hdr->record_size_ + sizeof(itemkey_t)) > PAGE_SIZE)
        throw std::runtime_error("invalid heap layout or READ RID");
    const char* bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    const char* tuple = bitmap + file_hdr->bitmap_size_ + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
    itemkey_t actual_key;
    memcpy(&actual_key, tuple, sizeof(actual_key));
    const auto* item = reinterpret_cast<const DataItem*>(tuple + sizeof(itemkey_t));
    if (!Bitmap::is_set(bitmap, rid.slot_no_) || actual_key != item_key || item->table_id != table_id ||
        item->valid != 1 || item->user_insert != 0 || item->lock != UNLOCKED ||
        item->value_size != file_hdr->record_size_ - static_cast<int>(sizeof(DataItem))) {
        // R2 诊断：读校验拒绝必须可见——打印 RID 与实测/期望各字段
        LOG(WARNING) << "[read-reject-RO] table=" << table_id
                     << " rid=(" << rid.page_no_ << "," << rid.slot_no_ << ")"
                     << " bitmap_set=" << Bitmap::is_set(bitmap, rid.slot_no_)
                     << " actual_key=" << actual_key << " expect_key=" << item_key
                     << " item_table=" << item->table_id
                     << " valid=" << (int)item->valid
                     << " user_insert=" << (int)item->user_insert
                     << " lock=" << item->lock
                     << " value_size=" << item->value_size
                     << " expect_value_size=" << (file_hdr->record_size_ - (int)sizeof(DataItem))
                     << " " << [&]() -> std::string {
                            if (SYSTEM_MODE != 1 || !compute_server || !compute_server->get_node() ||
                                table_id < 0 || rid.page_no_ < 0) return "lplm{n/a}";
                            LRLocalPageLockTable* lplm =
                                compute_server->get_node()->getLazyPageLockTable(table_id);
                            if (!lplm) return "lplm{n/a}";
                            return lplm->GetLock(rid.page_no_)->DumpState();
                        }();
        throw std::runtime_error("READ rejected stale RID, uncommitted row or malformed value");
    }
    workload_read_copy.reset(new DataItem(table_id, item->value_size));
    memcpy(workload_read_copy->value, tuple + sizeof(itemkey_t) + sizeof(DataItem), item->value_size);
    workload_read_copy->version = item->version;
    workload_read_copy->prev_lsn = item->prev_lsn;
    return workload_read_copy.get();
}

// 从页面里读取数据，Load 到 itemPtr 里并返回
DataItem* DTX::GetDataItemFromPageRW(table_id_t table_id, char* data, Rid rid , RmFileHdr::ptr file_hdr , itemkey_t& item_key){
    if (WORKLOAD_MODE == 2 && (rid.page_no_ <= 0 || rid.slot_no_ < 0 ||
        rid.slot_no_ >= file_hdr->num_records_per_page_ || file_hdr->bitmap_size_ <= 0 ||
        file_hdr->record_size_ < static_cast<int>(sizeof(DataItem)) ||
        sizeof(RmPageHdr) + file_hdr->bitmap_size_ + static_cast<uint64_t>(file_hdr->num_records_per_page_) *
            (file_hdr->record_size_ + sizeof(itemkey_t)) > PAGE_SIZE))
        throw std::runtime_error("invalid heap layout or write RID");
    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));

    DataItem* disk_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    itemkey_t *disk_key = reinterpret_cast<itemkey_t*>(tuple);
    if (WORKLOAD_MODE == 2 && SYSTEM_MODE == 1 &&
        (!Bitmap::is_set(bitmap, rid.slot_no_) || *disk_key != item_key || disk_item->table_id != table_id ||
         disk_item->valid != 1 || disk_item->user_insert > 1 ||
         disk_item->value_size != file_hdr->record_size_ - static_cast<int>(sizeof(DataItem)))) {
        LOG(WARNING) << "[read-reject-RW] table=" << table_id
                     << " rid=(" << rid.page_no_ << "," << rid.slot_no_ << ")"
                     << " bitmap_set=" << Bitmap::is_set(bitmap, rid.slot_no_)
                     << " actual_key=" << *disk_key << " expect_key=" << item_key
                     << " item_table=" << disk_item->table_id
                     << " valid=" << (int)disk_item->valid
                     << " user_insert=" << (int)disk_item->user_insert
                     << " lock=" << disk_item->lock
                     << " value_size=" << disk_item->value_size
                     << " expect_value_size=" << (file_hdr->record_size_ - (int)sizeof(DataItem));
        throw std::runtime_error("write rejected stale RID, invalid row or malformed value");
    }
    disk_item->value = (uint8_t*)reinterpret_cast<char*>(disk_item) + sizeof(DataItem);
    item_key = *disk_key;

    return disk_item;
}

DataItem* DTX::GetDataItemFromPage(table_id_t table_id , Rid rid , char *data , RmFileHdr::ptr file_hdr , itemkey_t &pri_key , bool is_w){
    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));

    DataItem *disk_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    disk_item->value = reinterpret_cast<uint8_t*>(disk_item) + sizeof(DataItem);
    
    pri_key = *reinterpret_cast<itemkey_t*>(tuple);

    if (!is_w){
        // TODO
        UndoDataItem(disk_item);
    }
    return disk_item;
}

LLSN GetLLSNFromPageRW(char* data){
    RmPageHdr* page_hdr = reinterpret_cast<RmPageHdr*>(data + OFFSET_PAGE_HDR);
    return page_hdr->LLSN_;
}

DataItem* DTX::UndoDataItem(DataItem* item) {
  // auto prev_lsn = item->prev_lsn;
  // while(start_ts < item->version) {
  //   // Undo the data item
  //   // UndoLog();
  // }
  return item;
}

void DTX::Abort() {
  tx_status = TXStatus::TX_ABORT;
}

bool DTX::AcquireWorkloadKeys() {
    if (WORKLOAD_MODE != 2 || SYSTEM_MODE != 1) return true;
    if (!workload_locked_keys.empty()) throw std::runtime_error("repeated TxExe is unsupported");
    for (const auto* set : {&read_only_set, &read_write_set, &insert_set, &delete_set})
        for (const auto& item : *set) workload_locked_keys.emplace_back(item.second.item_ptr->table_id, item.first);
    std::sort(workload_locked_keys.begin(), workload_locked_keys.end());
    if (std::adjacent_find(workload_locked_keys.begin(), workload_locked_keys.end()) != workload_locked_keys.end()) {
        workload_locked_keys.clear();
        workload_error = "UNSUPPORTED_REPEATED_KEY_IN_TRANSACTION";
        return false;
    }
    timestamp_service::WorkloadLockRequest request;
    request.set_node_id(global_meta_man->local_machine_id);
    request.set_tx_id(tx_id);
    if (bench_control::enabled()) request.set_generation(bench_control::run_id());
    for (const auto& key : workload_locked_keys) {
        auto* target = request.add_keys();
        target->set_table_id(key.first);
        target->set_key(key.second);
    }
    brpc::Controller controller;
    controller.set_timeout_ms(5000);
    controller.set_max_retry(0);
    timestamp_service::WorkloadLockResponse response;
    timestamp_service::TimeStampService_Stub stub(remote_server_channel);
    stub.WorkloadLock(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        tx_status = TXStatus::TX_UNKNOWN;
        throw std::runtime_error("workload key admission unknown: " + controller.ErrorText());
    }
    if (!response.granted()) {
        workload_locked_keys.clear();
        workload_error = "KEY_CONFLICT";
        return false;
    }
    return true;
}

void DTX::ReleaseWorkloadKeys() {
    if (workload_locked_keys.empty()) return;
    timestamp_service::WorkloadLockRequest request;
    request.set_node_id(global_meta_man->local_machine_id);
    request.set_tx_id(tx_id);
    request.set_release(true);
    if (bench_control::enabled()) request.set_generation(bench_control::run_id());
    for (const auto& key : workload_locked_keys) {
        auto* target = request.add_keys();
        target->set_table_id(key.first);
        target->set_key(key.second);
    }
    brpc::Controller controller;
    controller.set_timeout_ms(5000);
    controller.set_max_retry(0);
    timestamp_service::WorkloadLockResponse response;
    timestamp_service::TimeStampService_Stub stub(remote_server_channel);
    stub.WorkloadLock(&controller, &request, &response, nullptr);
    if (controller.Failed() || !response.granted())
        throw std::runtime_error("workload key release unconfirmed");
    workload_locked_keys.clear();
}
