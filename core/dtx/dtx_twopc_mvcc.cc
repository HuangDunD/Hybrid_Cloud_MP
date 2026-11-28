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
    const auto cost_start = page_cost::PageCostModel::NowNs();
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
    if ((WORKLOAD_MODE == 0 || WORKLOAD_MODE == 2 || oltp::Enabled(WORKLOAD_MODE)) && insert_set.empty() && delete_set.empty()) {
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
                         (page_cost::PageCostModel::NowNs() - cost_start) / 1e6, decision.commit_delta_ms);
}

// Worker tasks only fetch bytes. The DTX thread owns registration, the lock
// ledger, aliases, callbacks and the result of this execution.
void DTX::Fetch2pcMvccBatch(const FetchTasks& tasks, bool write, bool parallel) {
    if (tasks.empty()) return;
    auto& items = write ? read_write_set : read_only_set;
    const auto home = compute_server->getNodeID();
    struct Result {
        bool si_conflict = false;
        bool needs_status = false;
        std::unique_ptr<char[]> bytes;
        std::optional<undo::RowImage> own;
        bool missing = false;
        bool full = false;
    };
    bool aborted = false;
    auto consume = [&](size_t index, const Rid& rid, Result result) {
        auto& [key, item] = items[index];
        const auto table = item.item_ptr->table_id;
        if (compute_server->UsesKeyRouting() && write && (result.bytes || result.si_conflict))
            participants.insert(item.node_id); // SI may reject after the row lock was granted
        if (result.own) {
            mvcc::CopyToItem(*item.item_ptr, *result.own);
            item.is_fetched = true;
            if (result.own->version.deleted) tx_status = TXStatus::TX_VAL_NOTFOUND;
            return;
        }
        if (result.missing && !result.bytes) {
            item.item_ptr->valid = 0;
            item.missing_rid = write;
            item.is_fetched = !write;
            tx_status = TXStatus::TX_VAL_NOTFOUND;
            return;
        }
        if (!result.bytes) {
            CHECK(write);
            mvcc_write_conflict |= result.si_conflict;
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
        const bool by_key = compute_server->UsesKeyRouting();
        if (!by_key) RecordLockedRid(table, key, rid);
        if (!CheckMvccWriteCandidate(item, rid, key, !by_key || result.needs_status)) {
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
        auto& [key, item] = items[index];
        const auto table = item.item_ptr->table_id;
        auto own = write ? std::nullopt : PrivateMvcc(table, key);
        const bool by_key = compute_server->UsesKeyRouting();
        const auto node = by_key ? compute_server->KeyOwner(table, key)
                                 : compute_server->get_node_id_by_page_id(table, rid.page_no_);
        item.node_id = node;
        if (by_key && node != home && !own) stats_attempt.remote_row_fetch = true;
        auto context = mvcc_snapshot_->context();
        if (write) {
            if (!by_key) participants.insert(node);
            write_participants.insert(node);
            if (node != home) context = ExportMvccParticipant(node);
        }
        auto fetch = [this, table, input_rid = rid, key_copy = key, write, node, home, by_key,
                      context, own = std::move(own)]() mutable {
            Result result;

            if (own) {
                result.own = std::move(own);
                return result;
            }
            char* data = nullptr;
            if (node == home) {
                const auto local_rid = by_key ? compute_server->ResolveKey(table, key_copy) : input_rid;
                if (local_rid.page_no_ == INVALID_PAGE_ID) {
                    result.missing = true;
                    return result;
                }
                if (write) {
                    bool conflict = false;
                    itemkey_t actual_key = 0;
                    auto check = mvcc::WriteCheck::Allowed;
                    data = compute_server->Fetch2pcWriteRow(table, local_rid, context, tx_id, start_ts,
                        home, nullptr, nullptr, &conflict, &actual_key, &result.full, by_key ? &key_copy : nullptr, &check);
                    result.si_conflict = check == mvcc::WriteCheck::SnapshotConflict;
                    result.needs_status = check == mvcc::WriteCheck::NeedStatus;
                    CHECK(!data || actual_key == key_copy);
                } else {
                    data = compute_server->Fetch2pcReadRow(table, local_rid, context, &result.missing);
                }
            } else {
                compute_server->Get_2pc_Remote_page(node, table, input_rid, write, data, tx_id,
                    &context, start_ts, nullptr, nullptr, &result.missing, &result.si_conflict, &result.full,
                    by_key ? &key_copy : nullptr, &result.needs_status);
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
bool DTX::CheckMvccWriteCandidate(DataSetItem& item, const Rid& rid, itemkey_t key, bool check_status) {
    CHECK(mvcc_snapshot_ && item.is_fetched && item.item_ptr != nullptr);
    const auto table = item.item_ptr->table_id;
    // The home uses only logical identity, value and version for keyed private copies.
    const auto id = compute_server->UsesKeyRouting() ? undo::RowId{uint32_t(table), 0, 0, key}
                                                        : MvccIdentity(table, rid, key);
    item.mvcc_original = mvcc::RowImageFromItem(*item.item_ptr, id);
    const auto& current = *item.mvcc_original;
    const auto check = check_status ? compute_server->Mvcc().CheckWriteRow(*mvcc_snapshot_, current)
                                    : mvcc::WriteCheck::Allowed;
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
    if (compute_server->UsesKeyRouting()) {
        std::vector<MvccWrite> writes;
        // Release/abort enumerate the participant ledger. Only prepare/commit
        // need a logical write set, with the final value of each distinct key.
        if (!changed_only) return writes;
        for (const auto& [id, aliases] : mvcc_write_aliases_) {
            const auto index = aliases.back();
            if (MvccWriteChanged(index)) writes.push_back({index, INDEX_NOT_FOUND, id.key, id.table_id});
        }
        std::sort(writes.begin(), writes.end(), [](auto& a, auto& b) { return a.index < b.index; });
        return writes;
    }
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
    if (compute_server->UsesKeyRouting()) {
        const auto home = compute_server->getNodeID();
        if (participants.count(home)) nodes.insert(home);
        return nodes;
    }
    for (const auto& write : writes)
        nodes.insert(read_write_set[write.index].second.node_id);
    return nodes;
}

std::map<node_id_t, std::vector<twopc_service::ItemID>> DTX::MvccItemMap(
        const std::vector<MvccWrite>& writes) const {
    std::map<node_id_t, std::vector<twopc_service::ItemID>> result;
    for (const auto& write : writes) {
        const auto node = read_write_set[write.index].second.node_id;
        twopc_service::ItemID id;
        id.set_table_id(write.table);
        id.set_page_no(write.rid.page_no_);
        id.set_slot_id(write.rid.slot_no_);
        if (SYSTEM_MODE == 4) id.set_key(write.key);
        result[node].push_back(id);
    }
    return result;
}

std::map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> DTX::MvccPairMap(
        const std::vector<MvccWrite>& writes) const {
    std::map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> result;
    for (const auto& write : writes) {
        const auto node = read_write_set[write.index].second.node_id;
        result[node].push_back({write.table, write.rid});
    }
    return result;
}

twopc_service::CommitRequest DTX::SingleWriteRequest(const std::vector<MvccWrite>& writes,
                                                     const mvcc::Context& context) const {
    twopc_service::CommitRequest request;
    request.set_kind(twopc_service::CommitRequest::LEGACY);
    request.set_keys_only(compute_server->UsesKeyRouting());
    request.set_transaction_id(context.id.local);
    request.set_start_ts(start_ts);
    mvcc_2pc::EncodeContext(context, request.mutable_mvcc());
    for (const auto& write : writes) {
        const auto& item = *read_write_set[write.index].second.item_ptr;
        auto* id = request.add_item_id();
        id->set_table_id(write.table);
        if (request.keys_only()) id->set_key(write.key);
        else {
            id->set_page_no(write.rid.page_no_);
            id->set_slot_id(write.rid.slot_no_);
        }
        CHECK(item.valid <= 1);
        request.add_data(item.value, item.value_size);
        request.add_deleted(item.valid == 0);
    }
    return request;
}

std::function<void()> DTX::TakeMvccReleaseCompletion(mvcc::State outcome) {
    // A worker may reuse or destroy its DTX as soon as commit returns.
    auto snapshot = *mvcc_snapshot_;
    mvcc_snapshot_.reset();
    const bool finish_locks = SYSTEM_MODE != 4 || outcome == mvcc::State::Aborted;
    const bool local_write = write_participants.count(snapshot.context().id.home);
    return [server = compute_server, snapshot = std::move(snapshot), local_write, exports = std::exchange(mvcc_exports_, {}),
            outcome, start = start_ts, timestamp = outcome == mvcc::State::Committed ? commit_ts : 0, finish_locks] {
        auto& runtime = server->Mvcc();
        const auto& context = snapshot.context();
        for (const auto peer : exports) runtime.RemoteFinished(context, peer);
        if (local_write) runtime.SealParticipantUndo(context.id);
        runtime.End(snapshot, outcome);
        if (finish_locks) {
            server->lock_wait_mgr()->FinishTx(start, TxnStatus{
                outcome == mvcc::State::Aborted ? compute_node_service::TXN_ABORTED : compute_node_service::TXN_COMMITTED,
                timestamp});
        }
    };
}

bool DTX::Commit2PCMvcc() {
    CHECK(mvcc_snapshot_);
    if (tx_status == TXStatus::TX_ABORTING) {
        TxAbortWorkLoad();
        return false;
    }
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
        commit_ts = 0;
        compute_server->ReleaseMvcc_2pc(context, start_ts, 0, locked_items, nodes, {}, SYSTEM_MODE == 4,
                                       TakeMvccReleaseCompletion(mvcc::State::ReadOnlyFinished));
        tx_status = TXStatus::TX_COMMIT;
        return true;
    }
    std::set<mvcc::NodeId> writers;
    mvcc_changed_writers_.clear();
    for (const auto& write : changed) {
        const auto writer = mvcc::NodeId(read_write_set[write.index].second.node_id);
        writers.insert(writer);
        mvcc_changed_writers_.insert(node_id_t(writer));
    }
    prepared_2pc::Requests requests;
    if (writers.size() == 1) {
        const auto writer = *writers.begin();
        auto request = SingleWriteRequest(changed, context);
        if (writer == context.id.home) {
            const auto install_begin = benchmark_stats::NowNs();
            std::shared_ptr<prepared_2pc::Single> pending;
            const auto install = compute_server->InstallSingleMvccWrites(request, context, pending);
            CHECK(install > 0);
            stats_attempt.commit_part_ns[2] += benchmark_stats::NowNs() - install_begin;
            const auto finalize_begin = benchmark_stats::NowNs();
            const auto durable = CommitMvccWriter(context);
            if (pending) {
                CHECK(durable > install);
                compute_server->single_writes_.Applied(pending, install);
            }
            stats_attempt.commit_part_ns[1] += benchmark_stats::NowNs() - finalize_begin;
        } else {
            commit_ts = compute_server->Mvcc().AllocateCommitTs(context);
            compute_server->RunMvccGate(context.id, context.id.home, "tso_reserved");
            request.set_kind(twopc_service::CommitRequest::REMOTE_SINGLE);
            request.set_commit_ts(commit_ts);
            const auto install_begin = benchmark_stats::NowNs();
            compute_server->CommitRemoteSingleMvcc_2pc(writer, request);
            stats_attempt.commit_part_ns[2] += benchmark_stats::NowNs() - install_begin;
            compute_server->Mvcc().CertifyRemoteCommit(context, commit_ts);
        }
    } else {
        const bool storage_notification = compute_server->IsCornusPrepareCommitEnabled();
        commit_ts = compute_server->Mvcc().AllocateCommitTs(context);
        compute_server->RunMvccGate(context.id, context.id.home, "tso_reserved");
        const auto prepare_begin = benchmark_stats::NowNs();
        CHECK(TxPrepare(requests, storage_notification, changed));
        CHECK(requests.size() == writers.size());
        for (auto writer : writers) CHECK(requests.count(writer));
        mvcc_prepare_started_ = true;
        std::map<node_id_t, LLSN> prepare_lsn;
        CHECK(compute_server->Prepare_2pc(requests, &prepare_lsn, &stats_attempt.prepare))
            << "Prepare did not collect every durable YES";
        const auto prepare_elapsed = benchmark_stats::NowNs() - prepare_begin;
        stats_attempt.commit_part_ns[0] += prepare_elapsed;
        // Storage notifications report all-YES, not individual YES arrivals.
        // Leave that path without a coordinator first/tail sample.
        if (storage_notification) CHECK(prepare_lsn.empty());
        else {
            CHECK(prepare_lsn.size() == writers.size());
            for (auto writer : writers) CHECK(prepare_lsn.at(writer) > 0);
        }
        compute_server->RunMvccGate(context.id, context.id.home, "all_votes");
        if (!compute_server->UsesCornusCommit()) {
            const auto decision_begin = benchmark_stats::NowNs();
            auto* decision = new TwoPhaseDecisionLogRecord(context.id.home, context.id.local, commit_ts);
            compute_server->AppendLogAndWait(decision, benchmark_stats::DecisionLog,
                                             static_cast<int>(page_cost::LogPhase::Backup));
            stats_attempt.commit_part_ns[1] += benchmark_stats::NowNs() - decision_begin;
        }
        // Each durable YES covers its participant's business installs and undo.
        // Ordinary 2PC also has the coordinator decision above.
        compute_server->RunMvccGate(context.id, context.id.home, "decided");
        compute_server->Mvcc().CertifyRemoteCommit(context, commit_ts);
    }
    const auto release_begin = benchmark_stats::NowNs();
    compute_server->ReleaseMvcc_2pc(context, start_ts, commit_ts, locked_items, nodes, writers, SYSTEM_MODE == 4,
                                   TakeMvccReleaseCompletion(mvcc::State::Committed), requests.empty() ? nullptr : &requests);
    stats_attempt.commit_part_ns[2] += benchmark_stats::NowNs() - release_begin;
    tx_status = TXStatus::TX_COMMIT;
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
    compute_server->ReleaseMvcc_2pc(context, start_ts, 0, locked_items, nodes, {}, false,
                                   TakeMvccReleaseCompletion(mvcc::State::Aborted));
    Abort();
    stats_attempt.Add(benchmark_stats::Abort, began);
    stats_attempt.Finish(stats_totals, SYSTEM_MODE, is_distribute_txn, false);
}
