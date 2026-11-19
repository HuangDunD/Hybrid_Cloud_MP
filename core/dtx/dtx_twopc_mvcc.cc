#include <future>
#include "dtx/dtx.h"
#include "dtx/mvcc_2pc.h"
#include "mvcc/row.h"

namespace {
// Business change only; a lock-only access must not create a version.
bool MvccChanged(const DataSetItem& item) {
    CHECK(item.mvcc_original && item.item_ptr->value_size == int(item.mvcc_original->value.size()));
    return item.item_ptr->valid != !item.mvcc_original->version.deleted ||
           memcmp(item.item_ptr->value, item.mvcc_original->value.data(), item.item_ptr->value_size) != 0;
}
}  // namespace

// The mix branch decision must use the same cost inputs the eager fetch path
// used, but before any page is touched so the Lazy leg can run unchanged.
void DTX::DecideMixWithCostModel() {
    CHECK(SYSTEM_MODE == 4);
    if (!compute_server->CostModelActive() || cost_mode_decided_) return;
    const auto cost_start = page_cost::Model::NowNs();
    auto& model = compute_server->page_cost_model_;
    cost_mode_decided_ = true;
    std::vector<page_cost::Access> accesses;
    auto collect = [&](const auto& set, bool write) {
        for (const auto& entry : set) {
            const auto table = entry.second.item_ptr->table_id;
            const auto rid = RidForUnlock(table, entry.first);
            if (rid.page_no_ == INVALID_PAGE_ID) continue;
            accesses.push_back({table, rid.page_no_, write,
                                compute_server->get_node_id_by_page_id(table, rid.page_no_)});
        }
    };
    if ((WORKLOAD_MODE == 0 || WORKLOAD_MODE == 2) && insert_set.empty() && delete_set.empty()) {
        collect(read_only_set, false);
        collect(read_write_set, true);
    }
    const auto decision = model.Decide(accesses, compute_server->getNodeID(),
        [&](const page_cost::Access& a) {
            return compute_server->get_node()->getLazyPageLockTable(a.table)->GetLock(a.page)
                ->SnapshotForCost(a.write);
        }, cost_start);
    if (!decision.eligible) {
        model.RecordFallback();
        return;
    }
    is_distribute_txn = decision.twopc;
    model.RecordDecision(decision.twopc, decision.lazy_ms, decision.twopc_ms,
                         (page_cost::Model::NowNs() - cost_start) / 1e6, decision.commit_delta_ms);
}

// Worker tasks only fetch bytes. The DTX thread owns registration, the lock
// ledger, aliases, callbacks and the result of this execution.
void DTX::Fetch2pcMvccBatch(const FetchTasks& tasks, bool write, bool parallel) {
    if (tasks.empty()) return;
    auto& items = write ? read_write_set : read_only_set;
    const auto home = compute_server->getNodeID();
    struct Result {
        std::unique_ptr<char[]> bytes;
        std::optional<undo::RowImage> own;
        bool missing = false;
        bool full = false;
    };
    bool aborted = false;
    auto consume = [&](size_t index, const Rid& rid, Result result) {
        auto& [key, item] = items[index];
        const auto table = item.item_ptr->table_id;
        if (result.own) {
            mvcc::CopyToItem(*item.item_ptr, *result.own);
            item.is_fetched = true;
            if (result.own->version.deleted) tx_status = TXStatus::TX_VAL_NOTFOUND;
            return;
        }
        if (!result.bytes) {
            CHECK(write);
            mvcc_undo_full |= result.full;
            aborted = true;
            return;
        }
        auto* row = reinterpret_cast<DataItem*>(result.bytes.get());
        CHECK(row->table_id == table && row->valid <= 1);
        row->value = reinterpret_cast<uint8_t*>(row) + sizeof(DataItem);
        auto prior = write ? PrivateMvcc(table, key) : std::nullopt;
        *item.item_ptr = *row;
        item.is_fetched = true;
        if (!write) {
            CHECK(result.missing == (row->valid == 0));
            if (result.missing) tx_status = TXStatus::TX_VAL_NOTFOUND;
            return;
        }
        // Also record successful locks arriving after another worker's conflict.
        RecordLockedRid(table, key, rid);
        if (!CheckMvccWriteCandidate(item, rid, key)) {
            aborted = true;
            return;
        }
        if (prior) mvcc::CopyToItem(*item.item_ptr, *prior);
        auto& aliases = mvcc_write_aliases_[{table, key}];
        aliases.push_back(index);
        if (!item.item_ptr->valid) {
            item.update = {};
            tx_status = TXStatus::TX_VAL_NOTFOUND;
            return;
        }
        if (!item.update || aborted) return;
        item.update(*item.item_ptr);
        item.update = {};
        item.update_done = true;
        for (size_t alias : aliases) {
            auto& other = items[alias].second.item_ptr;
            if (other == item.item_ptr) continue;
            memcpy(other->value, item.item_ptr->value, other->value_size);
            other->valid = item.item_ptr->valid;
        }
    };

    std::vector<std::future<Result>> pending;
    if (parallel) pending.reserve(tasks.size());
    for (const auto& [index, rid] : tasks) {
        const auto& [key, item] = items[index];
        const auto table = item.item_ptr->table_id;
        auto own = write ? std::nullopt : PrivateMvcc(table, key);
        const auto node = compute_server->get_node_id_by_page_id(table, rid.page_no_);
        auto context = mvcc_snapshot_->context();
        if (!own) {
            participants.insert(node);
            if (write) write_participants.insert(node);
            if (node != home) context = ExportMvccParticipant(node);
        }
        auto fetch = [this, table, rid_copy = rid, key_copy = key, write, node, home,
                      context, own = std::move(own)]() mutable {
            Result result;
            if (own) {
                result.own = std::move(own);
                return result;
            }
            char* data = nullptr;
            if (node == home) {
                if (write) {
                    bool conflict = false;
                    itemkey_t actual_key = 0;
                    data = compute_server->Fetch2pcWriteRow(table, rid_copy, context, tx_id, start_ts,
                        home, nullptr, nullptr, &conflict, &actual_key, &result.full);
                    CHECK(!data || actual_key == key_copy);
                } else {
                    data = compute_server->Fetch2pcReadRow(table, rid_copy, context, &result.missing);
                }
            } else {
                compute_server->Get_2pc_Remote_page(node, table, rid_copy, write, data, tx_id,
                    &context, start_ts, nullptr, nullptr, &result.missing, nullptr, &result.full);
            }
            result.bytes.reset(data);
            return result;
        };
        if (parallel) {
            CHECK(thread_pool);
            pending.push_back(thread_pool->enqueue(std::move(fetch)));
        } else {
            consume(index, rid, fetch());
            if (aborted) break;
        }
    }
    for (size_t i = 0; i < pending.size(); ++i)
        consume(tasks[i].first, tasks[i].second, pending[i].get());
    if (aborted) tx_status = TXStatus::TX_ABORTING;
}

// The participant has already granted the row lock. This check runs before the
// business callback changes the private copy, so a version committed after the
// fixed snapshot aborts instead of being overwritten.
bool DTX::CheckMvccWriteCandidate(DataSetItem& item, const Rid& rid, itemkey_t key) {
    CHECK(mvcc_snapshot_ && item.is_fetched && item.item_ptr != nullptr);
    const auto table = item.item_ptr->table_id;
    auto current = mvcc::RowImageFromItem(*item.item_ptr, MvccIdentity(table, rid, key));
    item.mvcc_original = current;
    const auto check = compute_server->Mvcc().CheckWriteRow(*mvcc_snapshot_, current);
    if (check == mvcc::WriteCheck::SnapshotConflict) {
        mvcc_write_conflict = true;
        return false;
    }
    // The tuple write permission was granted by the participant; an active
    // foreign creator or an aborted chain head would contradict it.
    CHECK(check == mvcc::WriteCheck::Allowed) << "tuple permission and version state disagree";
    if (current.version.deleted) {
        // Visible tombstone: the business callback must not resurrect the row.
        item.update = {};
        tx_status = TXStatus::TX_VAL_NOTFOUND;
    }
    return true;
}

mvcc::Context DTX::ExportMvccParticipant(node_id_t node) {
    CHECK(mvcc_snapshot_);
    const auto home = compute_server->getNodeID();
    CHECK(node != home && node < ComputeNodeCount);
    if (mvcc_exports_.insert(node).second)
        compute_server->Mvcc().Export(*mvcc_snapshot_, node);
    return mvcc_snapshot_->context();
}

std::vector<DTX::MvccWrite> DTX::MvccWriteSet(bool changed_only) {
    CHECK(mvcc_snapshot_);
    std::map<std::tuple<table_id_t, page_id_t, int>, size_t> last;
    for (size_t i = 0; i < read_write_set.size(); ++i) {
        const auto& entry = read_write_set[i];
        if (!entry.second.is_fetched) continue;
        CHECK(entry.second.mvcc_original)
            << "a fetched write item must carry the fixed-snapshot pre-image";
        const auto rid = RidForUnlock(entry.second.item_ptr->table_id, entry.first);
        CHECK(rid.page_no_ != INVALID_PAGE_ID);
        last[std::make_tuple(entry.second.item_ptr->table_id, rid.page_no_, rid.slot_no_)] = i;
    }
    std::vector<MvccWrite> writes;
    for (const auto& entry : last) {
        const size_t i = entry.second;
        if (changed_only && !MvccWriteChanged(i)) continue;
        const auto rid = RidForUnlock(read_write_set[i].second.item_ptr->table_id, read_write_set[i].first);
        writes.push_back({i, rid, read_write_set[i].first, read_write_set[i].second.item_ptr->table_id});
    }
    std::sort(writes.begin(), writes.end(),
              [](const MvccWrite& a, const MvccWrite& b) { return a.index < b.index; });
    return writes;
}

bool DTX::MvccWriteChanged(size_t index) const {
    const auto& item = read_write_set[index].second;
    CHECK(item.is_fetched && item.mvcc_original);
    return MvccChanged(item);
}

std::set<node_id_t> DTX::MvccReleaseNodes(const std::vector<MvccWrite>& writes) const {
    std::set<node_id_t> nodes = mvcc_exports_;
    for (const auto& write : writes)
        nodes.insert(compute_server->get_node_id_by_page_id(write.table, write.rid.page_no_));
    return nodes;
}

std::map<node_id_t, std::vector<twopc_service::ItemID>> DTX::MvccItemMap(
        const std::vector<MvccWrite>& writes) const {
    std::map<node_id_t, std::vector<twopc_service::ItemID>> result;
    for (const auto& write : writes) {
        const auto node = compute_server->get_node_id_by_page_id(write.table, write.rid.page_no_);
        twopc_service::ItemID id;
        id.set_table_id(write.table);
        id.set_page_no(write.rid.page_no_);
        id.set_slot_id(write.rid.slot_no_);
        result[node].push_back(id);
    }
    return result;
}

std::map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> DTX::MvccPairMap(
        const std::vector<MvccWrite>& writes) const {
    std::map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> result;
    for (const auto& write : writes) {
        const auto node = compute_server->get_node_id_by_page_id(write.table, write.rid.page_no_);
        result[node].push_back({write.table, write.rid});
    }
    return result;
}

std::map<node_id_t, std::vector<std::string>> DTX::MvccValueMap(
        const std::vector<MvccWrite>& writes) const {
    std::map<node_id_t, std::vector<std::string>> result;
    for (const auto& write : writes) {
        const auto& item = *read_write_set[write.index].second.item_ptr;
        const auto node = compute_server->get_node_id_by_page_id(write.table, write.rid.page_no_);
        result[node].emplace_back(reinterpret_cast<const char*>(item.value), item.value_size);
    }
    return result;
}

std::map<node_id_t, std::vector<bool>> DTX::MvccDeletedMap(
        const std::vector<MvccWrite>& writes) const {
    std::map<node_id_t, std::vector<bool>> result;
    for (const auto& write : writes) {
        const auto& item = *read_write_set[write.index].second.item_ptr;
        CHECK(item.valid <= 1);
        const auto node = compute_server->get_node_id_by_page_id(write.table, write.rid.page_no_);
        result[node].push_back(item.valid == 0);
    }
    return result;
}

mvcc::Scn DTX::ReserveMvccCommit(const mvcc::Context& context, mvcc::CommitKind kind,
                                 const std::set<mvcc::NodeId>& writers,
                                 const std::map<mvcc::NodeId, uint64_t>& wal,
                                 const std::map<mvcc::NodeId, uint64_t>& prepare,
                                 std::optional<mvcc::WalPosition> decision, bool quorum) {
    auto& runtime = compute_server->Mvcc();
    if (runtime.IsCoordinator())
        return runtime.ReserveTwoPhase(context, kind, writers, wal, prepare, decision, quorum);
    return mvcc::SendReserveTwoPhase(compute_server->get_compute_channel()[runtime.Coordinator()],
                                     context, kind, writers, wal, prepare, decision, quorum);
}

bool DTX::CompleteMvccCommit(const mvcc::Context& context,
                             const std::set<mvcc::NodeId>& installed) {
    auto& runtime = compute_server->Mvcc();
    const auto slot = runtime.IsCoordinator()
        ? runtime.CompleteTwoPhase(context, installed)
        : mvcc::SendCompleteTwoPhase(compute_server->get_compute_channel()[runtime.Coordinator()],
                                     context, installed);
    CHECK(slot > context.snapshot);
    CHECK(slot == commit_ts) << "publication position differs from the reserved position";
    return true;
}

void DTX::Finish2PCMvcc(const mvcc::Context& context, mvcc::State outcome) {
    auto& runtime = compute_server->Mvcc();
    for (node_id_t node : mvcc_exports_) runtime.RemoteFinished(context, node);
    mvcc_exports_.clear();
    // The home may have admitted a write before its first lock attempt failed.
    runtime.SealParticipantUndo(context.id);
    runtime.End(*mvcc_snapshot_, outcome);
    mvcc_snapshot_.reset();
    // All Release replies include delivery of any reserved handoffs. Until
    // then ResolveFinishedWriter must treat these explicitly released locks as active.
    const bool aborted = outcome == mvcc::State::Aborted;
    compute_server->lock_wait_mgr()->FinishTx(start_ts, TxnStatus{
        aborted ? compute_node_service::TXN_ABORTED : compute_node_service::TXN_COMMITTED,
        outcome == mvcc::State::Committed ? commit_ts : 0});
    if (!aborted) tx_status = TXStatus::TX_COMMIT;
}

bool DTX::Commit2PCMvcc() {
    CHECK(mvcc_snapshot_);
    if (tx_status == TXStatus::TX_ABORTING) {
        TxAbortWorkLoad();
        return false;
    }
    auto& runtime = compute_server->Mvcc();
    const auto context = mvcc_snapshot_->context();
    CHECK(context.id.home == compute_server->getNodeID());
    auto locked = MvccWriteSet(false);
    auto changed = MvccWriteSet(true);
    const auto nodes = MvccReleaseNodes(locked);
    auto locked_items = MvccItemMap(locked);
    if (changed.empty()) {
        mvcc_changed_writers_.clear();
        // Only locks changed: no version is published, so nothing may reserve a
        // commit position. Drain the locks with the ordinary terminal path.
        compute_server->ReleaseMvcc_2pc(context, start_ts, 0, locked_items, nodes, {});
        Finish2PCMvcc(context, mvcc::State::ReadOnlyFinished);
        commit_ts = 0;
        return true;
    }
    std::set<mvcc::NodeId> writers;
    mvcc_changed_writers_.clear();
    for (const auto& write : changed) {
        const auto writer = mvcc::NodeId(compute_server->get_node_id_by_page_id(write.table, write.rid.page_no_));
        writers.insert(writer);
        mvcc_changed_writers_.insert(node_id_t(writer));
    }
    auto changed_items = MvccItemMap(changed);
    auto values = MvccValueMap(changed);
    auto deleted = MvccDeletedMap(changed);
    if (writers.size() == 1) {
        // The single-writer shortcut has no Prepare. It installs undo + new
        // value on the actual writer, then the home publishes the position.
        const auto writer = *writers.begin();
        std::map<node_id_t, LLSN> durable;
        const auto install_begin = benchmark_stats::NowNs();
        compute_server->CommitSingleMvcc_2pc(changed_items, values, deleted, context, start_ts,
                                             &durable);
        stats_attempt.commit_part_ns[2] += benchmark_stats::NowNs() - install_begin;
        CHECK(durable.size() == 1 && durable.count(writer));
        std::set<mvcc::NodeId> installed{mvcc::NodeId(writer)};
        const std::map<mvcc::NodeId, uint64_t> wal = {{mvcc::NodeId(writer), durable.at(writer)}};
        const auto publish_begin = benchmark_stats::NowNs();
        commit_ts = runtime.IsCoordinator()
            ? runtime.SubmitTwoPhase(context, mvcc::CommitKind::SingleWriter, writers, wal, {},
                                     std::nullopt, false, installed)
            : mvcc::SendTwoPhaseCommit(compute_server->get_compute_channel()[runtime.Coordinator()],
                                       context, mvcc::CommitKind::SingleWriter, writers, wal, {},
                                       std::nullopt, false, installed);
        stats_attempt.commit_part_ns[1] += benchmark_stats::NowNs() - publish_begin;
        benchmark_stats::scn_wait.Add(benchmark_stats::NowNs() - publish_begin);
        CHECK(commit_ts > context.snapshot);
        compute_server->ReleaseMvcc_2pc(context, start_ts, commit_ts, locked_items, nodes, writers);
        Finish2PCMvcc(context, mvcc::State::Committed);
        return true;
    }

    // Multi-writer: durable Prepare fixes every participant's undo pre-image
    // and new value before one global decision and one commit_scn.
    const bool storage_notification = compute_server->IsCornusPrepareCommitEnabled();
    const bool cornus = compute_server->UsesCornusCommit();
    commit_ts = GetTimestampRemote(); // legacy prepared-envelope nonce, not the commit SCN
    prepared_2pc::Requests requests;
    const auto prepare_begin = benchmark_stats::NowNs();
    CHECK(TxPrepare(requests, storage_notification));
    CHECK(requests.size() == writers.size()) << "prepared set differs from the changed writer set";
    mvcc_prepare_started_ = true;
    std::map<node_id_t, LLSN> prepare_lsn;
    CHECK(compute_server->Prepare_2pc(requests, &prepare_lsn, &stats_attempt.prepare))
        << "Prepare did not collect every durable YES; recovery is not implemented";
    const auto prepare_elapsed = benchmark_stats::NowNs() - prepare_begin;
    stats_attempt.commit_part_ns[0] += prepare_elapsed;
    if (stats_attempt.prepare.count == 0)
        stats_attempt.prepare = {1, prepare_elapsed, 0, prepare_elapsed};
    if (storage_notification) CHECK(prepare_lsn.empty());
    else CHECK(prepare_lsn.size() == writers.size());
    const auto decision_begin = benchmark_stats::NowNs();
    std::optional<mvcc::WalPosition> decision;
    if (!cornus) {
        auto* backup_log = new BatchEndLogRecord(tx_id, compute_server->getNodeID(), tx_id);
        const LLSN backup_lsn = compute_server->AppendLogAndWait(
            backup_log, benchmark_stats::DecisionLog, static_cast<int>(page_cost::LogPhase::Backup));
        decision = mvcc::WalPosition{runtime.Epoch(), context.id.home, backup_lsn};
    }
    const bool quorum = cornus || storage_notification;
    const std::map<node_id_t, uint64_t> wal(prepare_lsn.begin(), prepare_lsn.end());
    const std::map<node_id_t, uint64_t> prepare(prepare_lsn.begin(), prepare_lsn.end());
    const auto kind = cornus ? mvcc::CommitKind::Cornus : mvcc::CommitKind::TwoPhase;
    const std::map<mvcc::NodeId, uint64_t> mvcc_wal(wal.begin(), wal.end());
    const std::map<mvcc::NodeId, uint64_t> mvcc_prepare(prepare.begin(), prepare.end());
    commit_ts = ReserveMvccCommit(context, kind, writers, mvcc_wal, mvcc_prepare, decision, quorum);
    stats_attempt.commit_part_ns[1] += benchmark_stats::NowNs() - decision_begin;
    CHECK(commit_ts > context.snapshot);
    const auto final_begin = benchmark_stats::NowNs();
    std::map<node_id_t, LLSN> installed_durable;
    compute_server->CommitPrepared_2pc(requests, commit_ts, &installed_durable);
    CHECK(installed_durable.size() == writers.size());
    for (node_id_t writer : writers) CHECK(installed_durable.count(writer) == 1);
    const auto publication_begin = benchmark_stats::NowNs();
    CHECK(CompleteMvccCommit(context, writers));
    benchmark_stats::scn_wait.Add(benchmark_stats::NowNs() - publication_begin);
    stats_attempt.commit_part_ns[2] += benchmark_stats::NowNs() - final_begin;

    compute_server->ReleaseMvcc_2pc(context, start_ts, commit_ts, locked_items, nodes, writers, &requests);
    Finish2PCMvcc(context, mvcc::State::Committed);
    return true;
}

void DTX::Abort2PCMvcc() {
    if (tx_status == TXStatus::TX_ABORT) return;
    CHECK(mvcc_snapshot_);
    CHECK(!mvcc_prepare_started_) << "a durable prepared YES cannot be turned into a local Abort";
    const auto began = benchmark_stats::NowNs();
    const auto context = mvcc_snapshot_->context();
    auto locked = MvccWriteSet(false);
    const auto nodes = MvccReleaseNodes(locked);
    auto locked_items = MvccItemMap(locked);
    // 2PC never installs a business value before the irreversible commit
    // decision, so an abortable transaction only holds row locks and private
    // copies. Unlock and seal their undo subsegments; installed versions do not
    // exist to restore.
    compute_server->ReleaseMvcc_2pc(context, start_ts, 0, locked_items, nodes, {});
    Finish2PCMvcc(context, mvcc::State::Aborted);
    Abort();
    stats_attempt.Add(benchmark_stats::Abort, began);
    stats_attempt.Finish(stats_totals, SYSTEM_MODE, is_distribute_txn, false);
}
