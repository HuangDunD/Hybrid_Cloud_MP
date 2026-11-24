// Author: Chunyue Huang
// Copyright (c) 2024

#include <butil/logging.h>
#include <ctime>
#include <future>
#include "common.h"
#include "config.h"
#include "dtx/dtx.h"
#include "dtx/mvcc_2pc.h"
#include "mvcc/row.h"
#include "exception.h"
#include "record.h"
#include "rm_file_handle.h"
#include "workload/ycsb/ycsb_db.h"

bool DTX::TxExe(bool fail_abort){
  CHECK(WORKLOAD_MODE != 4) << "SQL uses its own executor";
  if (compute_server->IsLazyWorkload()) return ExecuteLazy(fail_abort);
  if (SYSTEM_MODE == 4) {
    // The same cost-model inputs as before are evaluated before page access;
    // the selected branch still fixes the plan of this attempt.
    DecideMixWithCostModel();
    if (!is_distribute_txn) return ExecuteLazy(fail_abort);
  }
  if (compute_server->IsMvccWorkload()) {
    if (tx_status == TXStatus::TX_ABORTING) {
      if (fail_abort) TxAbortWorkLoad();
      return false;
    }
    tx_status = TXStatus::TX_EXE;
  }
  const auto stats_begin = benchmark_stats::NowNs();
  compute_server->OnTxnExecuted();
  // 存储要真正去读和写的任务
  FetchTasks ro_fetch_tasks, rw_fetch_tasks;

  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  // 读操作
  for (size_t i=0; i<read_only_set.size(); i++) {
    DataSetItem& item = read_only_set[i].second;
    itemkey_t item_key = read_only_set[i].first;
    if (!item.is_fetched) {
      // Get data index
      Rid rid = RidForUnlock(item.item_ptr->table_id , item_key);
      if(rid.page_no_ == -1) {
        // Data not found
        if (mvcc_snapshot_) {
          item.item_ptr->valid = 0;
          item.is_fetched = true;
        } else {
          read_only_set.erase(read_only_set.begin() + i);
          --i;
        }
        tx_status = TXStatus::TX_VAL_NOTFOUND; // Value not found
        continue;
      }
      ro_fetch_tasks.emplace_back(i, rid);
    }
  }

  // Update 操作
  for (size_t i = 0; i < read_write_set.size(); i++) {
    DataSetItem& item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;

    if (!item.is_fetched && !item.missing_rid) {
      // Get data index
      Rid rid = RidForUnlock(item.item_ptr->table_id , item_key);
      if(rid.page_no_ == -1) {
        // Data not found
        if (mvcc_snapshot_) {
          item.missing_rid = true;
          item.item_ptr->valid = 0;
        } else {
          read_write_set.erase(read_write_set.begin() + i);
          --i;
        }
        tx_status = TXStatus::TX_VAL_NOTFOUND; // Value not found
        continue;
      }
      rw_fetch_tasks.emplace_back(i, rid);
    }
  }

  struct timespec fetch_exe_start_time, fetch_exe_end_time;
  clock_gettime(CLOCK_REALTIME, &fetch_exe_start_time);

  // 真正执行任务，也就是真正地去读写页面
  // Business callbacks may depend on earlier reads/updates in this TxExe.
  const bool has_updates = std::any_of(rw_fetch_tasks.begin(), rw_fetch_tasks.end(),
      [&](const auto& task) { return bool(read_write_set[task.first].second.update); });
  const bool use_parallel_fetch = (PARALLEL_PAGE_FETCH != 0) && !has_updates;
  if (mvcc_snapshot_) {
    Fetch2pcMvccBatch(ro_fetch_tasks, false, use_parallel_fetch);
    Fetch2pcMvccBatch(rw_fetch_tasks, true, use_parallel_fetch);
  } else {
    std::vector<std::future<void>> futures;
    for (auto& task : ro_fetch_tasks) {
      size_t idx = task.first;
      Rid rid = task.second;
      auto fetch_task = [&, idx, rid](){
          {
            DataSetItem& item = read_only_set[idx].second;
            itemkey_t item_key = read_only_set[idx].first;

            if(SYSTEM_MODE == 0 || SYSTEM_MODE == 3){
              auto data = compute_server->FetchSPage(item.item_ptr->table_id, rid.page_no_, -1, benchmark_stats::Execute);

              RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(item.item_ptr->table_id);
              *item.item_ptr = *GetDataItemFromPageRO(item.item_ptr->table_id, data, rid , file_hdr , item_key);

              item.is_fetched = true;
              ReleaseSPage(item.item_ptr->table_id, rid.page_no_); // release the page
            }
        }
      };
      if (use_parallel_fetch){
        assert(thread_pool);
        futures.emplace_back(thread_pool->enqueue(fetch_task));
      } else {
        assert(!thread_pool || has_updates);
        fetch_task();
      }
    }

    for (auto& task : rw_fetch_tasks) {
      // 如果插入或者删除出问题了，那写操作也没必要执行了
      if (tx_status == TXStatus::TX_ABORTING){
        break;
      }

      size_t idx = task.first;
      Rid rid = task.second;
      auto fetch_task = [&, idx, rid](){
        {
            DataSetItem& item = read_write_set[idx].second;
            itemkey_t item_key = read_write_set[idx].first;

            // Fetch data from storage
            if(SYSTEM_MODE == 0 || SYSTEM_MODE == 3){
              while (true) {
              Page *page = compute_server->FetchXPage(item.item_ptr->table_id, rid.page_no_, -1, benchmark_stats::Execute);
              if (!compute_server->ResolveFinishedWriter(item.item_ptr->table_id, rid, tx_id,
                      start_ts, compute_server->getNodeID(), page)) {
                  compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_);
                  tx_status = TXStatus::TX_ABORTING;
                  return;
              }
              char *data = page->get_data();
              DataItem* orginal_item = nullptr;

              RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(item.item_ptr->table_id);
              orginal_item = GetDataItemFromPageRW(item.item_ptr->table_id, data, rid , file_hdr , item_key);
              *item.item_ptr = *orginal_item;

              if(orginal_item->lock == UNLOCKED) {
                orginal_item->lock = EXCLUSIVE_LOCKED;
                RecordLockedRid(item.item_ptr->table_id, item_key, rid);
                // 在元组内记录加锁事务的 ID，后续访问可据此识别“本事务自持写锁”
                orginal_item->timeStamp = start_ts;
                orginal_item->txn_home_node = (uint8_t)compute_server->get_node()->getNodeID();
                orginal_item->lock_wait_record_node = (uint8_t)compute_server->get_node()->getNodeID();
                page->set_dirty(true);
                // GenUpdateLog(orginal_item , &item_key , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);
                LLSN page_new_lsn = compute_server->AddLockLog(tx_id, item.item_ptr->table_id, rid, EXCLUSIVE_LOCKED, (RmPageHdr*)data, start_ts, compute_server->get_node()->getNodeID(), compute_server->get_node()->getNodeID());

                ApplyWriteOperation(item, item_key, rid, page, orginal_item);
                compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_); // release the page
              } else if(orginal_item->lock == EXCLUSIVE_LOCKED &&
                        orginal_item->timeStamp == start_ts &&
                        orginal_item->txn_home_node == compute_server->getNodeID()) {
                // LOG(INFO) << "Is me Add , table_id = " << item.item_ptr->table_id << " page_id = " << rid.page_no_ << " timeStamp = " << start_ts;
                // 写锁是本事务自己加的，允许继续
                // 【修复】自持锁分支：可能是"过户接手"来的锁，这里补登记 Rid（幂等）
                RecordLockedRid(item.item_ptr->table_id, item_key, rid);
                ApplyWriteOperation(item, item_key, rid, page, orginal_item);
                compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_);
              } else{
                // LOG(INFO) << "Is Other Add , table_id = " << item.item_ptr->table_id << " page_id = " << rid.page_no_ << " timeStamp = " << orginal_item->timeStamp;
                // Eager/single conflicts abort without entering the Lazy wait path.
                compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_); // release the page
                tx_status = TXStatus::TX_ABORTING; // Transaction is aborting due to lock conflict
                return;
              }
              break;  // 加锁成功（或自持锁重入）：退出重检循环
              }
              item.is_fetched = true;
            }
            ApplyWriteOperation(item, item_key, rid);
        }
      };
      if (use_parallel_fetch){
        futures.emplace_back(thread_pool->enqueue(fetch_task));
      } else {
        fetch_task();
      }
    }

    for (auto& fut : futures) {
      // 并行地去拉取页面
      assert(use_parallel_fetch);
      fut.get();
    }
  }

  clock_gettime(CLOCK_REALTIME, &fetch_exe_end_time);
  tx_fetch_exe_time += (fetch_exe_end_time.tv_sec - fetch_exe_start_time.tv_sec) + (double)(fetch_exe_end_time.tv_nsec - fetch_exe_start_time.tv_nsec) / 1000000000;

  clock_gettime(CLOCK_REALTIME, &end_time);
  tx_exe_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;

  // Step 4: Check if the transaction is still valid
  stats_attempt.Add(benchmark_stats::Execute, stats_begin);
  if (tx_status == TXStatus::TX_ABORTING) {
    if (fail_abort) TxAbortWorkLoad();
    return false;
  }
  return true;
}

// Called before releasing an already fetched page; never fetches another page.
void DTX::ApplyWriteOperation(DataSetItem& item, itemkey_t key, const Rid& rid,
                              Page* page, DataItem* disk_item) {
  CHECK(!compute_server->IsLazyWorkload() && !(SYSTEM_MODE == 4 && !is_distribute_txn))
      << "Lazy writes must append undo before installing a row";
  if (!item.update) return;
  const bool install_here = page &&
      !(SYSTEM_MODE == 2 || (SYSTEM_MODE == 4 && is_distribute_txn));
  if (!install_here) {
    // Owner-side execution retains Prepare/Commit writeback. Repeated accesses
    // within this transaction must nevertheless see earlier callback results.
    for (const auto& entry : read_write_set) {
      if (entry.first == key && entry.second.item_ptr->table_id == item.item_ptr->table_id &&
          entry.second.update_done) {
        if (item.item_ptr != entry.second.item_ptr)
          memcpy(item.item_ptr->value, entry.second.item_ptr->value, item.item_ptr->value_size);
        break;
      }
    }
  }
  auto before = std::make_shared<DataItem>(*item.item_ptr);
  item.update(*item.item_ptr);
  item.update = {};
  item.update_done = true;
  if (install_here) {
    workload_undo_.emplace_back(WType::UPDATE_TUPLE, item.item_ptr->table_id, rid, before, key);
    memcpy(reinterpret_cast<char*>(disk_item) + sizeof(DataItem),
           item.item_ptr->value, item.item_ptr->value_size);
    page->set_dirty(true);
    compute_server->AddUpdateLog(tx_id, disk_item, &key, rid,
        reinterpret_cast<char*>(disk_item) + sizeof(DataItem),
        reinterpret_cast<RmPageHdr*>(page->get_data()));
    item.value_installed = true;
  }
  // Commit deduplicates by record; keep its existing first-entry selection valid.
  for (auto& entry : read_write_set) {
    auto& other = entry.second;
    if (entry.first == key && other.item_ptr->table_id == item.item_ptr->table_id &&
        other.is_fetched) {
      if (other.item_ptr != item.item_ptr)
        memcpy(other.item_ptr->value, item.item_ptr->value, item.item_ptr->value_size);
      other.value_installed = item.value_installed;
    }
  }
}

void DTX::UndoWorkloadUpdates() {
  CHECK(!compute_server->IsLazyWorkload() && !(SYSTEM_MODE == 4 && !is_distribute_txn))
      << "Lazy rollback must read the transaction undo chain";
  // Only callbacks installed on the ownership-transfer path need early undo.
  // Restore values before the existing abort code releases the record locks.
  for (auto it = workload_undo_.rbegin(); it != workload_undo_.rend(); ++it) {
    const auto table = it->GetTableID();
    const auto rid = it->GetRid();
    auto key = it->GetKey();
    Page* page = compute_server->FetchXPage(table, rid.page_no_, -1, benchmark_stats::Abort);
    auto* disk = GetDataItemFromPageRW(table, page->get_data(), rid,
                                      compute_server->get_file_hdr_cached(table), key);
    memcpy(reinterpret_cast<char*>(disk) + sizeof(DataItem),
           it->GetDataItem()->value, it->GetDataItem()->value_size);
    page->set_dirty(true);
    compute_server->AddUpdateLog(tx_id, disk, &key, rid,
        reinterpret_cast<char*>(disk) + sizeof(DataItem),
        reinterpret_cast<RmPageHdr*>(page->get_data()));
    compute_server->ReleaseXPage(table, rid.page_no_);
  }
  workload_undo_.clear();
}

bool DTX::TxCommit(){
  const auto stats_begin = benchmark_stats::NowNs();
  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  // Lazy/ownership-transfer commits have one actual writer: the home node.
  const bool twopc = SYSTEM_MODE == 2 || (SYSTEM_MODE == 4 && is_distribute_txn);
  const auto stats_path = [&] {
    if (compute_server->IsMvccWorkload() && twopc) {
      if (mvcc_changed_writers_.empty()) return benchmark_stats::ReadOnly;
      if (mvcc_changed_writers_.size() > 1) return benchmark_stats::MultiWriter;
      return *mvcc_changed_writers_.begin() == compute_server->getNodeID()
                 ? benchmark_stats::SingleLocal : benchmark_stats::SingleRemote;
    }
    if (twopc) {
      if (write_participants.empty()) return benchmark_stats::ReadOnly;
      if (write_participants.size() > 1) return benchmark_stats::MultiWriter;
      return *write_participants.begin() == compute_server->getNodeID()
                 ? benchmark_stats::SingleLocal : benchmark_stats::SingleRemote;
    }
    if (read_write_set.empty() && insert_set.empty() && delete_set.empty())
      return benchmark_stats::ReadOnly;
    return benchmark_stats::SingleLocal;
  };
  bool commit_status = false;
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
    commit_status = TxCommitSingle();
  } else if(SYSTEM_MODE == 2){
    // Multi-writer 2PC selects durable Prepare or the original protocol
    // using local_compute_node.two_pc_variant.
    commit_status = Tx2PCCommit();
  }else if (SYSTEM_MODE == 4){
    // 2PC + Lazy 混合模式：根据 DecideCommitMode() 的结果分流
    if (is_distribute_txn){
      commit_status = Tx2PCCommit();
      hybrid_2pc_commit_cnt++;
    }else {
      commit_status = TxCommitSingle();
      hybrid_lazy_commit_cnt++;
    }
  }else {
    assert(false);
  }
  if (commit_status && !compute_server->IsMvccWorkload()) {
    compute_server->lock_wait_mgr()->PublishTxnStatus(start_ts,
        {compute_node_service::TXN_COMMITTED, commit_ts});
  }
  clock_gettime(CLOCK_REALTIME, &end_time);
  tx_commit_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  // 动态指标：提交计数（配合 Abort() 里的回滚计数，给运行时的 TPS(avg)/Abort Cnt 用）
  if (commit_status) compute_server->OnTxnCommitted();
  const auto commit_path = stats_path();
  const unsigned stats_writers = compute_server->IsMvccWorkload() && twopc
      ? mvcc_changed_writers_.size()
      : (twopc ? write_participants.size()
               : (commit_path == benchmark_stats::ReadOnly ? 0 : 1));
  stats_attempt.CommitFinished(stats_totals, commit_path, stats_begin, stats_writers);
  stats_attempt.Finish(stats_totals, SYSTEM_MODE, is_distribute_txn, commit_status);
  return commit_status;
}

bool DTX::TxCommitSingleSQL() {
  CHECK(WORKLOAD_MODE == 4 && SYSTEM_MODE == 1 && mvcc_snapshot_);
  return CommitLazy();
}

bool DTX::TxCommitSingle() {
  CHECK(WORKLOAD_MODE != 4) << "SQL uses TxCommitSingleSQL";
  if (compute_server->IsLazyWorkload() ||
      (SYSTEM_MODE == 4 && !is_distribute_txn)) return CommitLazy();
  struct timespec start_time, end_ts_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  commit_ts = GetTimestampRemote(); // 先拿到一个全局的时间戳
  clock_gettime(CLOCK_REALTIME, &end_ts_time);
  // 把这次获取全局时间戳的耗时给记录下来，加入到 tx...
  tx_get_timestamp_time2 += (end_ts_time.tv_sec - start_time.tv_sec) + (double)(end_ts_time.tv_nsec - start_time.tv_nsec) / 1000000000;

  // 去重：同一 tuple 在 read_write_set 中可能出现多次（zipfian 高偏斜下常见）。
  auto unique_indices = UniqueRWIndices();
  const bool deferred_cleanup =
      (SYSTEM_MODE == 4 && !is_distribute_txn) &&
      !unique_indices.empty() && insert_set.empty() && delete_set.empty() &&
      std::all_of(unique_indices.begin(), unique_indices.end(), [&](size_t i) {
          return read_write_set[i].second.value_installed;
      });
  if (deferred_cleanup) {
    const auto log_begin = benchmark_stats::NowNs();
    TxCommitOver();
    tx_write_commit_log_time += (benchmark_stats::NowNs() - log_begin) / 1e9;
    compute_server->lock_wait_mgr()->FinishDeferredTxn(start_ts, commit_ts,
        compute_server->getNodeID(), compute_server->get_compute_channel());
    for (size_t i : unique_indices) {
      const auto& entry = read_write_set[i];
      const auto rid_it = locked_locks_.find(LockLedgerKey{entry.second.item_ptr->table_id, entry.first});
      if (rid_it == locked_locks_.end()) continue; // optional cleanup never searches the index
      compute_server->TryCleanCommittedRecord(entry.second.item_ptr->table_id,
          rid_it->second, entry.first,
          tx_id, start_ts, commit_ts);
    }
    tx_status = TXStatus::TX_COMMIT;
    return true;
  }
  for(size_t k = 0 ; k < unique_indices.size() ; k++){
    size_t i = unique_indices[k];
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;
    assert(data_item.is_fetched);
    Rid rid = RidForUnlock(data_item.item_ptr->table_id , item_key);
    assert(rid.page_no_ >= 0);

    struct timespec start_time1, end_time1;
    PendingHandoff lw_handoff = PrepareTupleUnlock(data_item.item_ptr->table_id, rid);
    clock_gettime(CLOCK_REALTIME, &start_time1);
    Page* x_page = compute_server->FetchXPage(data_item.item_ptr->table_id, rid.page_no_, -1, benchmark_stats::Commit);
    char *data = x_page->get_data();
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_commit_fetch_page_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(data_item.item_ptr->table_id);
    orginal_item = GetDataItemFromPageRW(data_item.item_ptr->table_id, data, rid , file_hdr , item_key);

    // 把元组的锁给释放，并标记版本号
    orginal_item->version = commit_ts;
    orginal_item->commitTimeStamp = commit_ts;
    const bool lw_handed =
        UnlockTupleWithHandoff(data_item.item_ptr->table_id, rid, orginal_item, start_ts, &lw_handoff);
    if (!lw_handed) {
      orginal_item->lock = UNLOCKED;
    }
    // 把改过的信息给写回去
    if (!data_item.value_installed) {
      memcpy(reinterpret_cast<char*>(orginal_item) + sizeof(DataItem), data_item.item_ptr->value, data_item.item_ptr->value_size);
    }

    x_page->set_dirty(true);
    LLSN page_new_lsn;

    struct timespec log_start, log_end;
    clock_gettime(CLOCK_REALTIME, &log_start);
    page_new_lsn = compute_server->AddUpdateLog(tx_id , orginal_item, &item_key, rid , (char*)data_item.item_ptr->value,(RmPageHdr*)data , commit_ts);
    clock_gettime(CLOCK_REALTIME, &log_end);
    tx_write_commit_log_time += (log_end.tv_sec - log_start.tv_sec) + (double)(log_end.tv_nsec - log_start.tv_nsec) / 1000000000;

    struct timespec start_time2, end_time2;
    clock_gettime(CLOCK_REALTIME, &start_time2);
    compute_server->ReleaseXPage(data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time2);
    if (lw_handed) DeliverHandoff(lw_handoff);

    if (k == unique_indices.size() - 1) {
      struct timespec log_start, log_end;
      clock_gettime(CLOCK_REALTIME, &log_start);
      TxCommitOver();
      clock_gettime(CLOCK_REALTIME, &log_end);
      tx_write_commit_log_time += (log_end.tv_sec - log_start.tv_sec) + (double)(log_end.tv_nsec - log_start.tv_nsec) / 1000000000;
    }
  }

  // WAIT_DETECT：本事务锁已全部释放，唤醒等待本事务锁的本地/远程 waiter
  compute_server->lock_wait_mgr()->FinishTx(start_ts);
  tx_status = TXStatus::TX_COMMIT;
  return true;
}

void DTX::TxAbortSQL() {
  CHECK(WORKLOAD_MODE == 4 && SYSTEM_MODE == 1 && mvcc_snapshot_);
  AbortLazy();
}

void DTX::TxAbortWorkLoad() {
  CHECK(WORKLOAD_MODE != 4) << "SQL uses TxAbortSQL";
  if (compute_server->IsLazyWorkload() ||
      (SYSTEM_MODE == 4 && !is_distribute_txn)) { AbortLazy(); return; }
  if (compute_server->IsMvccWorkload()) { Abort2PCMvcc(); return; }
  const auto stats_begin = benchmark_stats::NowNs();
  // std::cout << "TxAbort\n";
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
  UndoWorkloadUpdates();
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 3){
    // 按 (table_id, page_no, slot_no) 去重后再解锁。
    // read_write_set 里同一个元组可能出现多条（例如 YCSB 一次抽 10 个 key 不去重，
    // 同一个 key 被写位置抽中两次），而解锁会把锁【过户】给下一个等待者：第一次解锁
    // 之后队列 owner 已经换成接手者，第二次再解同一个元组就会撞上
    // PrepareHandoff 里的 assert(q.owner_holder_ts == holder_ts)。
    // commit/2PC 路径一直用 UniqueRWIndices() 去重，这里之前漏了。
    auto unique_indices = UniqueRWIndices();
    // [DEBUG 日志已关] 这里原来是 abort_begin 取证（rw_size/fetched/unique 统计 + LwTrace）。
    // 恢复调试：把下面这几行取消注释即可。
    // if (LOCK_WAIT_TRACE) {
    //   size_t lw_fetched = 0;
    //   for (const auto& e : read_write_set) if (e.second.is_fetched) ++lw_fetched;
    //   LwTrace("abort_begin", "tx=", start_ts, "rw_size=", read_write_set.size(),
    //           "fetched=", lw_fetched, "unique=", unique_indices.size());
    // }
    for (size_t i : unique_indices) {
      DataSetItem& data_item = read_write_set[i].second;
      itemkey_t item_key = read_write_set[i].first;

      // item.is_fetched 代表了这个元组已经被修改过了，需要回退对其的修改操作
      if(data_item.is_fetched){
        // this data item is fetched and locked
        Rid rid = RidForUnlock(data_item.item_ptr->table_id , item_key);
// [DEBUG 日志已关] LwTrace("abort_unlock", "tx=", start_ts, "i=", i, "item_key=", item_key,
//                "table=", data_item.item_ptr->table_id, "page=", rid.page_no_,
//                "slot=", rid.slot_no_);
        struct timespec start_time1, end_time1;
        PendingHandoff lw_handoff = PrepareTupleUnlock(data_item.item_ptr->table_id, rid);
        clock_gettime(CLOCK_REALTIME, &start_time1);
        Page *x_page = compute_server->FetchXPage(data_item.item_ptr->table_id, rid.page_no_, -1, benchmark_stats::Abort);
        char *data = x_page->get_data();
        clock_gettime(CLOCK_REALTIME, &end_time1);
        DataItem* orginal_item = nullptr;

        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(data_item.item_ptr->table_id);
        orginal_item = GetDataItemFromPageRW(data_item.item_ptr->table_id, data, rid  , file_hdr , item_key);

        // assert(orginal_item->key == data_item.item_ptr->key);
        // assert(orginal_item->lock == EXCLUSIVE_LOCKED);
        const bool lw_handed =
            UnlockTupleWithHandoff(data_item.item_ptr->table_id, rid, orginal_item, start_ts, &lw_handoff);
        if (!lw_handed) {
          orginal_item->lock = UNLOCKED;
        }
        struct timespec start_time2, end_time2;
        clock_gettime(CLOCK_REALTIME, &start_time2);

        x_page->set_dirty(true);
        // GenUpdateLog(orginal_item , &item_key , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);
        LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , orginal_item , &item_key , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);
        ReleaseXPage(data_item.item_ptr->table_id, rid.page_no_);
        clock_gettime(CLOCK_REALTIME, &end_time2);
        if (lw_handed) DeliverHandoff(lw_handoff);
      }
    }
  // WAIT_DETECT：本事务锁已全部释放，唤醒等待本事务锁的本地/远程 waiter
  compute_server->lock_wait_mgr()->FinishTx(start_ts);
    struct timespec abort_log_start_time, abort_log_end_time;
    clock_gettime(CLOCK_REALTIME, &abort_log_start_time);
    TxAbortOver();
    clock_gettime(CLOCK_REALTIME, &abort_log_end_time);
    TxWaitAbortLogTime += (abort_log_end_time.tv_sec - abort_log_start_time.tv_sec) +
                          (double)(abort_log_end_time.tv_nsec - abort_log_start_time.tv_nsec) / 1000000000;
  } else if(SYSTEM_MODE == 2){
    struct timespec abort_log_start_time, abort_log_end_time;
    clock_gettime(CLOCK_REALTIME, &abort_log_start_time);
    if(participants.size() == 1 && compute_server->get_node()->getNodeID() == *participants.begin())
      Tx2PCAbortLocal();
    else
      Tx2PCAbortAll();

    clock_gettime(CLOCK_REALTIME, &abort_log_end_time);
    TxWaitAbortLogTime += (abort_log_end_time.tv_sec - abort_log_start_time.tv_sec) +
                          (double)(abort_log_end_time.tv_nsec - abort_log_start_time.tv_nsec) / 1000000000;
  } else if (SYSTEM_MODE == 4){
    struct timespec abort_log_start_time, abort_log_end_time;
    clock_gettime(CLOCK_REALTIME, &abort_log_start_time);

    if (is_distribute_txn){
      Tx2PCAbortAll();
    }else {
      Tx2PCAbortLocal();
    }

    clock_gettime(CLOCK_REALTIME, &abort_log_end_time);
    TxWaitAbortLogTime += (abort_log_end_time.tv_sec - abort_log_start_time.tv_sec) +
                          (double)(abort_log_end_time.tv_nsec - abort_log_start_time.tv_nsec) / 1000000000;
  }else {
    assert(false);
  }
  compute_server->lock_wait_mgr()->PublishTxnStatus(start_ts,
      {compute_node_service::TXN_ABORTED, 0});
  tx_status = TXStatus::TX_ABORT;
  clock_gettime(CLOCK_REALTIME, &end_time);
  tx_abort_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  Abort();
  stats_attempt.Add(benchmark_stats::Abort, stats_begin);
  stats_attempt.Finish(stats_totals, SYSTEM_MODE, is_distribute_txn, false);
}

bool DTX::TxPrepare(prepared_2pc::Requests& requests, bool storage_notification){
  CHECK(SYSTEM_MODE == 2 || SYSTEM_MODE == 4);
  CHECK(compute_server->IsLogEnabled()) << "Durable Prepare requires generate_log=1";
  CHECK(insert_set.empty() && delete_set.empty()) << "Prepared 2PC supports existing-record UPDATE only";
  CHECK(mvcc_snapshot_);
  const auto& context = mvcc_snapshot_->context();
  const auto writes = MvccWriteSet(true);
  CHECK(writes.size() > 1);
  std::vector<node_id_t> participants;
  for (const auto& write : writes) {
    const auto node = compute_server->get_node_id_by_page_id(write.table, write.rid.page_no_);
    if (requests.count(node) == 0) participants.push_back(node);
    const auto& item = *read_write_set[write.index].second.item_ptr;
    CHECK(!item.user_insert && item.value != nullptr);
    auto& request = requests[node];
    auto* prepared = request.add_writes();
    prepared->mutable_item_id()->set_table_id(write.table);
    prepared->mutable_item_id()->set_page_no(write.rid.page_no_);
    prepared->mutable_item_id()->set_slot_id(write.rid.slot_no_);
    prepared->set_table_name(compute_server->getTableNameByTableID(write.table));
    prepared->set_key(write.key);
    prepared->set_operation(twopc_service::PreparedWrite::UPDATE);
    prepared->set_deleted(item.valid == 0);
    auto hdr = compute_server->get_file_hdr_cached(write.table);
    CHECK(item.value_size == hdr->record_size_ - sizeof(DataItem));
    prepared->set_value(item.value, item.value_size);
  }
  CHECK(requests.size() == participants.size() && requests.size() > 1);
  std::sort(participants.begin(), participants.end());
  twopc_service::TxnIdentity identity;
  identity.set_coordinator_node(compute_server->get_node()->getNodeID());
  identity.set_coordinator_incarnation(compute_server->coordinator_incarnation_);
  identity.set_local_tx_id(tx_id);
  for (auto& entry : requests) {
    CHECK(write_participants.count(entry.first) == 1);
    auto& request = entry.second;
    request.set_transaction_id(tx_id);
    request.set_protocol_version(storage_notification ? prepared_2pc::kProtocolVersion
                                                     : prepared_2pc::kParticipantReplyVersion);
    *request.mutable_identity() = identity;
    mvcc_2pc::EncodeContext(context, request.mutable_mvcc());
    request.set_start_ts(start_ts);
    request.set_participant_node(entry.first);
    for (auto node : participants) request.add_participants(node);
  }
  return true;
}

bool DTX::Tx2PCCommit(){
  // P5 routes every benchmark 2PC / mix-distributed commit through the unified
  // MVCC path; the old timestamped commit is no longer a reachable branch.
  CHECK(compute_server->IsMvccWorkload());
  return Commit2PCMvcc();
}

void DTX::Tx2PCCommitLocal(){
  // 去重：同一 tuple 可能被加入 read_write_set 多次（zipfian 高偏斜下常见），
  // 第二次 unlock 会触发 assert(item->lock == EXCLUSIVE_LOCKED)。
  auto unique_indices = UniqueRWIndices();
  for(size_t k = 0; k < unique_indices.size(); k++){
    size_t i = unique_indices[k];
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;

    Rid rid = RidForUnlock(data_item.item_ptr->table_id , item_key);

    node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);
    assert(node_id == compute_server->get_node()->getNodeID());

    struct timespec start_time1, end_time1;
    PendingHandoff lw_handoff = PrepareTupleUnlock(data_item.item_ptr->table_id, rid);
    clock_gettime(CLOCK_REALTIME, &start_time1);
    Page* page = nullptr;
    if (SYSTEM_MODE == 2){
      page = compute_server->local_fetch_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }else if (SYSTEM_MODE == 4){
      page = compute_server->rpc_lazy_fetch_x_page(data_item.item_ptr->table_id , rid.page_no_, benchmark_stats::Commit);
    }else {
      assert(false);
    }


    char* data = page->get_data();
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_commit_fetch_page_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(data_item.item_ptr->table_id);
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_+ sizeof(itemkey_t));

    itemkey_t *disk_key = reinterpret_cast<itemkey_t*>(tuple);
    DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    assert(*disk_key == item_key);
    assert(item->lock == EXCLUSIVE_LOCKED);

    memcpy(tuple + sizeof(itemkey_t) + sizeof(DataItem) , data_item.item_ptr->value , data_item.item_ptr->value_size);
    item->commitTimeStamp = commit_ts;
    const bool lw_handed =
        UnlockTupleWithHandoff(data_item.item_ptr->table_id, rid, item, start_ts, &lw_handoff);
    if (!lw_handed) {
      item->lock = UNLOCKED; // unlock the data
    }

    item->value = (uint8_t*)reinterpret_cast<char*>(item) + sizeof(DataItem);
    page->set_dirty(true);
    LLSN page_new_lsn;
    page_new_lsn = compute_server->AddUpdateLog(tx_id , item , disk_key , rid , tuple + sizeof(itemkey_t) + sizeof(DataItem) , (RmPageHdr*)(data) , commit_ts);

    if (SYSTEM_MODE == 2){
      compute_server->get_node()->getLocalPageLockTables(data_item.item_ptr->table_id)->GetLock(rid.page_no_)->set_newest_lsn(page_new_lsn);
      compute_server->local_release_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }else {
      compute_server->rpc_lazy_release_x_page(data_item.item_ptr->table_id , rid.page_no_);
    }
    if (lw_handed) DeliverHandoff(lw_handoff);

    if (k == unique_indices.size() - 1){
      // 刷一个事务结束的日志下去，同时等待这个事务相关的日志全部落盘
      TxCommitOver();
    }
  }
  // WAIT_DETECT：本事务锁已全部释放，唤醒等待本事务锁的本地/远程 waiter
  compute_server->lock_wait_mgr()->FinishTx(start_ts);
}

void DTX::Tx2PCCommitAll(){
  // 2pc 方法的commit阶段
  // node_id_t：节点ID，table_id + Rid：元组位置，char*：元组数据
  std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map;
  // 远端 Commit handler 会 assert(item->lock == EXCLUSIVE_LOCKED)，重复项会失败，需去重。
  for (size_t i : UniqueRWIndices()){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t key = read_write_set[i].first;
    Rid rid = RidForUnlock(data_item.item_ptr->table_id , key);
    node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);
    auto write_data = reinterpret_cast<char*>(data_item.item_ptr.get()->value);
    node_data_map[node_id].push_back(std::make_pair(std::make_pair(data_item.item_ptr->table_id, rid), write_data));
  }

#if AsyncCommit2pc
  // 异步提交
  two_latency_c = compute_server->Commit_2pc(node_data_map, tx_id, commit_ts, false);
#else
  // 同步提交
  two_latency_c = compute_server->Commit_2pc(node_data_map, tx_id, commit_ts, true);
#endif
  // WAIT_DETECT：本事务锁已全部释放，唤醒等待本事务锁的本地/远程 waiter
  compute_server->lock_wait_mgr()->FinishTx(start_ts);
  return;
}

void DTX::Tx2PCAbortLocal(){
  // write log。去重避免对同一 tuple 重复 unlock。
  for (size_t i : UniqueRWIndices()){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key =  read_write_set[i].first;
    Rid rid = RidForUnlock(data_item.item_ptr->table_id , item_key);
    node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);

    PendingHandoff lw_handoff = PrepareTupleUnlock(data_item.item_ptr->table_id, rid);
    Page* page = nullptr;
    if (SYSTEM_MODE == 2){
      assert(node_id == compute_server->get_node()->getNodeID());
      page = compute_server->local_fetch_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }else if (SYSTEM_MODE == 4){
      page = compute_server->rpc_lazy_fetch_x_page(data_item.item_ptr->table_id, rid.page_no_, benchmark_stats::Abort);
    }else {
      assert(false);
    }

    assert(page);
    char* data = page->get_data();
    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(data_item.item_ptr->table_id);
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
    DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    // 重定位下 item->value
    item->value = (uint8_t*)reinterpret_cast<char*>(item) + sizeof(DataItem);
    assert(item->lock == EXCLUSIVE_LOCKED);
    // don't write the data
    const bool lw_handed =
        UnlockTupleWithHandoff(data_item.item_ptr->table_id, rid, item, start_ts, &lw_handoff);
    if (!lw_handed) {
      item->lock = UNLOCKED; // unlock the data
    }

    page->set_dirty(true);
    LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , item , &item_key , rid , (char*)item + sizeof(DataItem) , (RmPageHdr*)(data));
    if (SYSTEM_MODE == 2){
      compute_server->get_node()->getLocalPageLockTables(data_item.item_ptr->table_id)->GetLock(rid.page_no_)->set_newest_lsn(page_new_lsn);
      compute_server->local_release_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }else {
      compute_server->rpc_lazy_release_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }
    if (lw_handed) DeliverHandoff(lw_handoff);
  }

  // WAIT_DETECT：本事务锁已全部释放，唤醒等待本事务锁的本地/远程 waiter
  compute_server->lock_wait_mgr()->FinishTx(start_ts);
  TxAbortOver();
}

void DTX::Tx2PCAbortAll(){
  // 2pc 方法的abort阶段。远端 Abort handler 会 assert lock，需去重。
  std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map;
  for (size_t i : UniqueRWIndices()){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;
    Rid rid = RidForUnlock(data_item.item_ptr->table_id , item_key);
    node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id ,  rid.page_no_);
    node_data_map[node_id].push_back(std::make_pair(data_item.item_ptr->table_id, rid));
  }
#if AsyncCommit2pc
  // 异步abort
  compute_server->Abort_2pc(node_data_map, tx_id, false);
#else
  // 同步abort
  compute_server->Abort_2pc(node_data_map, tx_id, true);
#endif
  // WAIT_DETECT：本事务锁已全部释放，唤醒等待本事务锁的本地/远程 waiter
  compute_server->lock_wait_mgr()->FinishTx(start_ts);
  return;
}
