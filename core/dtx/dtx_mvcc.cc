#include "dtx/dtx.h"
#include "mvcc/row.h"

// 单节点 Lazy 的读写、提交和回滚入口。快照在 TxBegin 时固定，读取期间不再取新 SCN。
// 这里的 start_ts 用作行锁持有者的身份；可见性比较使用 mvcc_snapshot_ 中的快照 SCN。
// 记录这份历史属于哪一行。除了页号和槽号，还带上表号、主键，读历史时一起核对。
undo::RowId DTX::MvccIdentity(table_id_t table, const Rid& rid, itemkey_t key) const {
    CHECK(table >= 0 && rid.page_no_ > 0 && rid.slot_no_ >= 0);
    return {uint32_t(table), uint32_t(rid.page_no_), uint32_t(rid.slot_no_), key};
}
namespace {
// 比较业务副本和上次读取或安装时的基准，只关心业务值、删除标记。
// 锁字段变化不算业务更新；仅仅拿过写锁，不应该因此生成 undo。
bool Changed(const DataSetItem& item) {
    CHECK(item.mvcc_original && item.item_ptr->value_size == int(item.mvcc_original->value.size()));
    return item.item_ptr->valid != !item.mvcc_original->version.deleted ||
           memcmp(item.item_ptr->value, item.mvcc_original->value.data(), item.item_ptr->value_size) != 0;
}
}  // namespace

// 找本事务尚未写回主行的修改，让旧的“先取副本、再直接改值”路径也能读到自己的写入。
// 同一行可能有多个条目，从后往前找最近的已修改副本；找不到就交给主行和 undo 读取流程。
// 这里只构造供本事务读取的行，不分配 undo，也不修改共享页面。
std::optional<undo::RowImage> DTX::PrivateMvcc(table_id_t table, itemkey_t key) const {
    const auto aliases = mvcc_write_aliases_.find({table, key});
    if (aliases == mvcc_write_aliases_.end()) return std::nullopt;
    for (auto it = aliases->second.rbegin(); it != aliases->second.rend(); ++it) {
        const auto& item = read_write_set[*it].second;
        if (Changed(item)) {
            auto row = *item.mvcc_original;
            row.value.assign(reinterpret_cast<char*>(item.item_ptr->value), item.item_ptr->value_size);
            row.version = {mvcc_snapshot_->context().id, false, item.item_ptr->valid == 0};
            return row;
        }
    }
    return std::nullopt;
}

// 执行读写集合中尚未处理的条目。读取先看自己的私有修改，否则按固定快照找可见版本；
// 写入交给 FetchMvccWrite 取得行锁、检查 SI 冲突，并执行更新回调。
// fail_abort=false 时只标记失败并返回，由调用方稍后回滚；这个事务仍然不能提交。
// 找不到行或读到可见墓碑时，用 TX_VAL_NOTFOUND 报告，不当作事务冲突。
bool DTX::ExecuteLazy(bool fail_abort) {
    CHECK(mvcc_snapshot_ && (SYSTEM_MODE == 1 || (SYSTEM_MODE == 4 && !is_distribute_txn)));
    if (tx_status == TXStatus::TX_ABORTING) {
        if (fail_abort) TxAbortWorkLoad();
        return false; // 上一次执行已经失败，不能靠再次 TxExe(false) 把事务变回可提交状态。
    }
    CHECK(insert_set.empty() && delete_set.empty())
        << "P3 supports existing-row updates and valid tombstones";
    const auto began = benchmark_stats::NowNs();
    compute_server->OnTxnExecuted();
    tx_status = TXStatus::TX_EXE;
    for (auto& entry : read_only_set) {
        auto& item = entry.second;
        if (item.is_fetched) continue;
        const auto table = item.item_ptr->table_id;
        const auto key = entry.first;
        const auto rid = RidForUnlock(table, key);
        if (rid.page_no_ == INVALID_PAGE_ID) {
            item.item_ptr->valid = 0;
            item.is_fetched = true;
            tx_status = TXStatus::TX_VAL_NOTFOUND;
            continue;
        }
        auto private_row = PrivateMvcc(table, key);
        undo::RowImage row;
        if (private_row)
            row = *private_row;
        else {
            const auto hdr = compute_server->get_file_hdr_cached(table);
            auto* data = compute_server->FetchSPage(table, rid.page_no_, -1, benchmark_stats::Execute);
            row = mvcc::CopyRow(data, *hdr, MvccIdentity(table, rid, key));
            // 当前值和版本信息已经复制出来，先放主页面锁，再去查可能需要取页的 undo 链。
            ReleaseSPage(table, rid.page_no_);
            row = compute_server->Mvcc().VisibleRow(*mvcc_snapshot_, std::move(row));
        }
        mvcc::CopyToItem(*item.item_ptr, row);
        item.is_fetched = true;
        if (row.version.deleted) tx_status = TXStatus::TX_VAL_NOTFOUND;
    }
    for (size_t index = 0; index < read_write_set.size(); ++index) {
        auto& entry = read_write_set[index];
        auto& item = entry.second;
        if (item.is_fetched) continue;
        const auto rid = RidForUnlock(item.item_ptr->table_id, entry.first);
        if (rid.page_no_ == INVALID_PAGE_ID) {
            item.item_ptr->valid = 0;
            item.is_fetched = true;
            tx_status = TXStatus::TX_VAL_NOTFOUND;
            continue;
        }
        if (!FetchMvccWrite(index, entry.first, rid)) {
            tx_status = TXStatus::TX_ABORTING;
            break;
        }
    }
    stats_attempt.Add(benchmark_stats::Execute, began);
    tx_exe_time += (benchmark_stats::NowNs() - began) / 1e9;
    if (tx_status == TXStatus::TX_ABORTING) {
        if (fail_abort) TxAbortWorkLoad();
        return false;
    }
    return true;
}

// 为这一行取得写权限，并检查它是否已被别人在本事务快照之后提交过。
// 正常锁冲突按 WAIT_DETECT 配置等待或返回 false；SI 冲突也返回 false，已拿到的锁由 Abort 释放。
// 有更新回调就立即执行并安装到主行；没有回调则先交出私有副本，后续修改在提交时写回。
bool DTX::FetchMvccWrite(size_t index, itemkey_t key, const Rid& rid) {
    auto& item = read_write_set[index].second;
    const auto table = item.item_ptr->table_id;
    const auto id = MvccIdentity(table, rid, key);
    const auto hdr = compute_server->get_file_hdr_cached(table);
    const auto home = compute_server->getNodeID();
    if (!compute_server->Mvcc().Undo().AdmitNewWrite(mvcc_snapshot_->context().id)) {
        mvcc_undo_full = true;
        return false;
    }
    auto private_row = PrivateMvcc(table, key);
    undo::RowImage current;
    for (;;) {
        // 页 X 权限保护锁字段的检查和修改；留下的行写锁负责阻止其他事务覆盖这一行。
        Page* page = compute_server->FetchXPage(table, rid.page_no_, -1, benchmark_stats::Execute);
        const bool resolved = compute_server->ResolveFinishedWriter(table, rid, tx_id, start_ts,
                                                                    home, page);
        CHECK(resolved);
        auto* disk = mvcc::RowAt(page->get_data(), *hdr, id);
        if (disk->lock == UNLOCKED ||
            (disk->lock == EXCLUSIVE_LOCKED && disk->timeStamp == start_ts && disk->txn_home_node == home)) {
            if (disk->lock == UNLOCKED) {
                disk->lock = EXCLUSIVE_LOCKED;
                disk->timeStamp = start_ts;
                disk->txn_home_node = home;
                disk->lock_wait_record_node = home;
                page->set_dirty(true);
                compute_server->AddLockLog(tx_id, table, rid, EXCLUSIVE_LOCKED,
                                           reinterpret_cast<RmPageHdr*>(page->get_data()), start_ts, home, home);
            }
            RecordLockedRid(table, key, rid);
            current = mvcc::CopyRow(page->get_data(), *hdr, id);
            // 这里只放页锁，行写锁仍归本事务。是否能写要看版本创建者的提交 SCN。
            ReleaseXPage(table, rid.page_no_);
            if (!compute_server->Mvcc().WriteAllowed(*mvcc_snapshot_, current)) {
                mvcc_write_conflict = true;
                return false;
            }
            break;
        }
        CHECK(disk->lock == EXCLUSIVE_LOCKED && disk->timeStamp > 0);
        const auto holder = disk->timeStamp;
        const auto home = disk->txn_home_node;
        const auto queue = disk->lock_wait_record_node;
        ReleaseXPage(table, rid.page_no_);
        if (LOCK_MODE != WAIT_DETECT) return false;
        auto result = compute_server->lock_wait_mgr()->WaitForLock(
            start_ts, compute_server->getNodeID(), holder, home, queue,
            compute_server->get_compute_channel(), table, rid.page_no_, rid.slot_no_, key);
        if (result != LockWaitResult::GRANTED_RETRY) return false;
    }
    if (private_row) current = *private_row;
    mvcc::CopyToItem(*item.item_ptr, current);
    item.mvcc_original = current;
    item.is_fetched = true;
    mvcc_write_aliases_[{table, key}].push_back(index);
    if (current.version.deleted) {
        tx_status = TXStatus::TX_VAL_NOTFOUND;
        return true;
    }
    if (item.update) {
        item.update(*item.item_ptr);
        item.update = {};
        item.update_done = true;
        return InstallMvcc(item, key, rid);
    }
    return true;
}

// 把一次实际修改安装到共享主行，调用前必须已经持有该行的写锁。
// 顺序是：复制旧行 → 保存完整 undo → 安装新值、创建者、删除标记和 undo 链头 → 记录主行 redo。
// 这里只完成覆盖，不代表事务已经提交；其他事务仍按自己的快照决定是否沿 undo 读取。
bool DTX::InstallMvcc(DataSetItem& item, itemkey_t key, const Rid& rid) {
    CHECK(mvcc_snapshot_ && item.is_fetched && item.mvcc_original);
    const auto table = item.item_ptr->table_id;
    // 同一行可能被加入读写集合多次。安装后同步当前及更早条目的副本和基准，
    // 避免提交时又把旧副本写回去；后面的条目保留自己的后续修改。
    auto synchronize = [&](const undo::RowImage& installed) {
        for (size_t index : mvcc_write_aliases_.at({table, key})) {
            auto& view = read_write_set[index].second;
            mvcc::CopyToItem(*view.item_ptr, installed);
            view.mvcc_original = installed;
            view.value_installed = true;
            if (&view == &item) break;
        }
    };
    const auto id = MvccIdentity(table, rid, key);
    const auto hdr = compute_server->get_file_hdr_cached(table);
    Page* page = compute_server->FetchXPage(table, rid.page_no_, -1, benchmark_stats::Execute);
    auto* disk = mvcc::RowAt(page->get_data(), *hdr, id);
    CHECK(disk->lock == EXCLUSIVE_LOCKED && disk->timeStamp == start_ts &&
          disk->txn_home_node == compute_server->getNodeID());
    auto before = mvcc::CopyRow(page->get_data(), *hdr, id);
    // 行写锁继续保留，但写 undo 可能需要等 WAL 或重新取页，不占着主页面锁做这些等待。
    ReleaseXPage(table, rid.page_no_);
    CHECK(compute_server->Mvcc().WriteAllowed(*mvcc_snapshot_, before));
    CHECK(item.item_ptr->value_size == int(before.value.size()) && item.item_ptr->valid <= 1);
    auto after = before;
    after.value.assign(reinterpret_cast<char*>(item.item_ptr->value), item.item_ptr->value_size);
    after.version.deleted = item.item_ptr->valid == 0;
    if (before.value == after.value && before.version.deleted == after.version.deleted) {
        synchronize(before);
        return true;  // 只加锁或最终值没有变化，不追加 undo，也不改变版本创建者。
    }
    // Append 会先把 undo 页的 redo 放入本节点 WAL，主行引用它的 redo 必须排在后面。
    const auto ref = compute_server->Mvcc().Undo().Append(mvcc_snapshot_->context().id, before);
    if (ref.empty()) {
        mvcc_undo_full = true;
        return false;
    }
    after.previous = ref;
    after.version.creator = mvcc_snapshot_->context().id;
    after.version.initial = false;
    page = compute_server->FetchXPage(table, rid.page_no_, -1, benchmark_stats::Execute);
    disk = mvcc::RowAt(page->get_data(), *hdr, id);
    CHECK(disk->lock == EXCLUSIVE_LOCKED && disk->timeStamp == start_ts &&
          disk->txn_home_node == compute_server->getNodeID());
    // 中间只放过页锁，没有交出行写权限，所以主行的业务版本必须仍是刚才复制的那一份。
    CHECK(undo::EncodeRow(mvcc::CopyRow(page->get_data(), *hdr, id)) == undo::EncodeRow(before))
        << "row changed while writer retained permission";
    mvcc::InstallRow(*disk, after);
    page->set_dirty(true);
    const auto lsn =
        compute_server->AddUpdateLog(tx_id, disk, &key, rid, reinterpret_cast<char*>(disk) + sizeof(DataItem),
                                     reinterpret_cast<RmPageHdr*>(page->get_data()), 0, UpdateHistory::Install);
    CHECK(ref.page.writer == uint32_t(compute_server->getNodeID()) && lsn > ref.lsn)
        << "same-writer WAL order: undo redo must precede the main-row redo";
    // 只登记已经安装到主行的覆盖，Abort 从这里沿事务链逐条退回。
    mvcc_tail_ = ref;
    ++mvcc_writes_;
    ReleaseXPage(table, rid.page_no_);
    synchronize(after);
    return true;
}

// 提交前先补写尚未安装的私有修改，再封口 undo，等待本事务所需的 WAL 持久化。
// 有实际覆盖才通过 SCN 协议发布提交；只读或只加锁的事务走无业务写入的结束路径。
// 提交发布完成后，才通知等待者并尝试清理行锁；没能立即清掉的物理锁仍按 Lazy 方式处理。
bool DTX::CommitLazy() {
    CHECK(mvcc_snapshot_);
    if (tx_status == TXStatus::TX_ABORTING) {
        AbortLazy();
        return false;
    }
    for (auto& entry : read_write_set) {
        auto& item = entry.second;
        if (item.is_fetched && item.mvcc_original && Changed(item) &&
            !InstallMvcc(item, entry.first, RidForUnlock(item.item_ptr->table_id, entry.first))) {
            tx_status = TXStatus::TX_ABORTING;
            AbortLazy();
            return false;
        }
    }
    auto& runtime = compute_server->Mvcc();
    runtime.Undo().Finish(mvcc_snapshot_->context().id);
    uint64_t durable = 0;
    if (!locked_locks_.empty()) {
        // 等尾标记落盘，就覆盖了此前入队的 undo、主行和锁日志，仍使用现有批量刷新。
        auto* end = new BatchEndLogRecord(0, compute_server->getNodeID(), tx_id);
        durable = compute_server->AppendLogAndWait(end, benchmark_stats::CommitLog);
    }
    commit_ts = runtime.Complete(*mvcc_snapshot_, true, mvcc_writes_ > 0, durable);
    mvcc_snapshot_.reset();
    // 事务已经完成发布，后续写者此后才能接手这些行。
    compute_server->lock_wait_mgr()->FinishDeferredTxn(
        start_ts, commit_ts, compute_server->getNodeID(), compute_server->get_compute_channel());
    for (const auto& entry : locked_locks_)
        compute_server->TryCleanCommittedRecord(entry.first.table_id, entry.second, entry.first.key, tx_id,
                                                start_ts, commit_ts);
    tx_status = TXStatus::TX_COMMIT;
    return true;
}

// 按实际覆盖的逆序回滚：每次从 undo 取旧行，重新取得主页面，恢复业务值和版本信息。
// 同一行改过多次也要逐次退回；页面即使已被淘汰，也通过正常取页路径恢复，不依赖旧内存副本。
// 只改了私有副本、尚未安装的内容不在这条链上，无需对主行做回滚。
// 回滚日志落盘并公布 ABORTED 后，才释放或交接行锁，最后清理锁等待关系和事务统计。
void DTX::AbortLazy() {
    if (tx_status == TXStatus::TX_ABORT) return;
    CHECK(mvcc_snapshot_);
    const auto began = benchmark_stats::NowNs();
    auto& runtime = compute_server->Mvcc();
    auto ref = mvcc_tail_;
    for (uint64_t sequence = mvcc_writes_; sequence > 0; --sequence) {
        CHECK(!ref.empty());
        const auto record = runtime.Undo().ReadRollback(ref, mvcc_snapshot_->context().id, sequence);
        const auto& old = record.old;
        const Rid rid{int(old.id.page), int(old.id.slot)};
        auto key = old.id.key;
        const auto hdr = compute_server->get_file_hdr_cached(old.id.table);
        Page* page = compute_server->FetchXPage(old.id.table, rid.page_no_, -1, benchmark_stats::Abort);
        auto* disk = mvcc::RowAt(page->get_data(), *hdr, old.id);
        CHECK(disk->lock == EXCLUSIVE_LOCKED && disk->timeStamp == start_ts &&
              disk->txn_home_node == compute_server->getNodeID());
        auto current = mvcc::CopyRow(page->get_data(), *hdr, old.id);
        CHECK(current.version.creator == mvcc_snapshot_->context().id && current.previous == ref);
        // 恢复旧值、旧创建者、删除标记和旧链头，当前锁持有者及等待信息保持不变。
        mvcc::InstallRow(*disk, old);
        page->set_dirty(true);
        compute_server->AddUpdateLog(tx_id, disk, &key, rid, reinterpret_cast<char*>(disk) + sizeof(DataItem),
                                     reinterpret_cast<RmPageHdr*>(page->get_data()));
        ReleaseXPage(old.id.table, rid.page_no_);
        // previous_txn 串起本事务的修改顺序；old.previous 是同一行的历史，不能拿来遍历事务。
        ref = record.previous_txn;
    }
    CHECK(ref.empty());
    runtime.Undo().Finish(mvcc_snapshot_->context().id);
    uint64_t durable = 0;
    if (!locked_locks_.empty()) {
        durable = compute_server->AppendLogAndWait(
            new BatchEndLogRecord(0, compute_server->getNodeID(), tx_id), benchmark_stats::AbortLog);
    }
    runtime.Complete(*mvcc_snapshot_, false, mvcc_writes_ > 0, durable);
    mvcc_snapshot_.reset();
    for (const auto& entry : locked_locks_) {
        const auto table = entry.first.table_id;
        const auto rid = entry.second;
        const auto id = MvccIdentity(table, rid, entry.first.key);
        auto handoff = PrepareTupleUnlock(table, rid);
        const auto hdr = compute_server->get_file_hdr_cached(table);
        Page* page = compute_server->FetchXPage(table, rid.page_no_, -1, benchmark_stats::Abort);
        auto* disk = mvcc::RowAt(page->get_data(), *hdr, id);
        CHECK(disk->lock == EXCLUSIVE_LOCKED && disk->timeStamp == start_ts &&
              disk->txn_home_node == compute_server->getNodeID());
        const bool handed = UnlockTupleWithHandoff(table, rid, disk, start_ts, &handoff);
        if (!handed) disk->lock = UNLOCKED;
        page->set_dirty(true);
        compute_server->AddLockLog(tx_id, table, rid, disk->lock,
                                   reinterpret_cast<RmPageHdr*>(page->get_data()), disk->timeStamp,
                                   disk->txn_home_node, disk->lock_wait_record_node);
        ReleaseXPage(table, rid.page_no_);
        if (handed) DeliverHandoff(handoff);
    }
    compute_server->lock_wait_mgr()->FinishTx(start_ts, TxnStatus{compute_node_service::TXN_ABORTED, 0});
    Abort();
    stats_attempt.Add(benchmark_stats::Abort, began);
    stats_attempt.Finish(stats_totals, SYSTEM_MODE, false, false);
}

DataItemPtr DTX::SqlVisibleRow(undo::RowImage row) {
    CHECK(WORKLOAD_MODE == 4 && mvcc_snapshot_);
    row = compute_server->Mvcc().VisibleRow(*mvcc_snapshot_, std::move(row));
    if (row.version.deleted) return nullptr;
    auto item = std::make_shared<DataItem>(row.id.table);
    mvcc::CopyToItem(*item, row);
    return item;
}

void DTX::SqlWrite(table_id_t table, itemkey_t key, Rid rid, bool insert,
                   const std::function<void(DataItem&)>& change) {
    CHECK(WORKLOAD_MODE == 4 && mvcc_snapshot_);
    AddToReadWriteSet(std::make_shared<DataItem>(table), key, {});
    const auto index = read_write_set.size() - 1;
    if (FetchMvccWrite(index, key, rid)) {
        auto& entry = read_write_set[index].second;
        if (insert && entry.item_ptr->valid)
            throw std::runtime_error("Abort: duplicate primary key");
        CHECK(insert || entry.item_ptr->valid);
        change(*entry.item_ptr);
        if (InstallMvcc(entry, key, rid)) return;
    }
    tx_status = TXStatus::TX_ABORTING;
    if (mvcc_undo_full) throw std::runtime_error("Abort: undo capacity exhausted");
    throw std::runtime_error(mvcc_write_conflict ? "Abort: SI write conflict" : "Abort: writer conflict");
}

Rid DTX::ReserveSqlSlot(table_id_t table, itemkey_t& key, bool indexed, const RmFileHdr& file) {
    auto* server = compute_server;
    CHECK(mvcc_snapshot_ && WORKLOAD_MODE == 4);
    if (!server->Mvcc().Undo().AdmitNewWrite(mvcc_snapshot_->context().id))
        throw std::runtime_error("Abort: undo capacity exhausted");
    const int stride = file.record_size_ + sizeof(itemkey_t);
    for (int attempt = 0; ; ++attempt) {
        page_id_t page_id = attempt < 2 ? server->search_free_page(table, stride) : INVALID_PAGE_ID;
        if (page_id == INVALID_PAGE_ID) page_id = server->rpc_create_page(table);
        CHECK(page_id > 0);
        Page* page = server->FetchXPage(table, page_id);
        char* bitmap = page->get_data() + sizeof(RmPageHdr);
        int slot = Bitmap::first_bit(false, bitmap, file.num_records_per_page_);
        if (slot == file.num_records_per_page_) {
            server->update_page_space(table, page_id, 0);
            server->ReleaseXPage(table, page_id);
            continue;
        }
        Rid rid{page_id, slot};
        // The unique index claims the key under its leaf X latch. Until the heap
        // page is released a reader of the new RID cannot see a partial slot.
        if (indexed) {
            auto leaf = server->bl_indexes[table]->insert_entry(&key, rid);
            if (leaf == INVALID_PAGE_ID) {
                server->ReleaseXPage(table, page_id);
                throw std::runtime_error("Abort: concurrent duplicate primary key");
            }
        } else {
            key = (uint64_t(uint32_t(page_id)) << 32) | uint32_t(slot);
        }
        char* tuple = bitmap + file.bitmap_size_ + slot * stride;
        memcpy(tuple, &key, sizeof(key));
        auto* bytes = tuple + sizeof(key);
        DataItem absent(table, file.record_size_ - int(sizeof(DataItem)));
        memset(absent.value, 0, absent.value_size);
        absent.valid = 0;
        absent.lock = EXCLUSIVE_LOCKED;
        absent.timeStamp = start_ts;
        absent.txn_home_node = absent.lock_wait_record_node = server->getNodeID();
        absent.Serialize(bytes);
        Bitmap::set(bitmap, slot);
        auto* hdr = reinterpret_cast<RmPageHdr*>(page->get_data());
        ++hdr->num_records_;
        page->set_dirty(true);
        server->AddInsertLog(tx_id, &absent, &key, absent.value, rid, hdr);
        RecordLockedRid(table, key, rid);
        const int free = Bitmap::getfreeposnum(bitmap, file.num_records_per_page_);
        server->update_page_space(table, page_id, free * stride);
        server->ReleaseXPage(table, page_id);
        return rid;
    }
}
