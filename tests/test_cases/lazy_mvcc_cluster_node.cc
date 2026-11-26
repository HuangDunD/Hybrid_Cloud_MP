// One compute process for the P4 multi-node Lazy test. It exposes a line
// protocol on loopback so the Python driver can interleave transactions and
// force page/WAL/ACK conditions explicitly. No correctness decision depends on
// an unattended sleep.
#include "common/fatal_checks.h"
#include "compute_server/worker/handler.h"
#include "dtx/dtx.h"
#include "mvcc/row.h"
#include <arpa/inet.h>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <iterator>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <set>
#include <cstring>
#include <sstream>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {
uint64_t Number(const DataItemPtr& item) {
    CHECK(item->valid && item->value_size >= 8);
    uint64_t result;
    memcpy(&result, item->value, 8);
    return result;
}
void SetNumber(DataItem& item, uint64_t value) {
    CHECK(item.value_size >= 8);
    memcpy(item.value, &value, 8);
}
struct Result {
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    bool ok = false;
    uint64_t commit_ts = 0;
};
struct ClusterNode {
    IndexCache index;
    PageCache pages;
    MetaManager* meta = nullptr;
    ComputeServer* server = nullptr;
    std::thread flusher;
    std::atomic<bool> flusher_running{true};
    std::atomic<bool> flusher_paused{false};
    std::unique_ptr<ThreadPool> fetch_pool;
    std::map<uint64_t, std::unique_ptr<DTX>> txns;
    std::map<uint64_t, std::shared_ptr<Result>> results;
    std::map<uint64_t, bool> force_2pc;
    std::map<uint64_t, bool> force_applied;
    std::atomic<uint64_t> main_fetch_x{0};
    std::mutex install_mutex;
    std::condition_variable install_cv;
    bool install_gate_armed = false;
    bool install_gate_entered = false;
    std::string install_gate_phase;
    int system_mode = 1;
    uint64_t next_txn_handle = 1;
    uint64_t next_result = 1;
    std::mutex control_mutex;
    table_id_t primary = 0;

    ClusterNode(const std::string& bench, int node_id, int mode) : system_mode(mode) {
        primary = bench == "tpcc" ? 2 : 0;
        Handler handler;
        handler.ConfigureComputeNodeRunSQL();
        SYSTEM_MODE = mode;
        if (PARALLEL_PAGE_FETCH) fetch_pool = std::make_unique<ThreadPool>(2, node_id);
        WORKLOAD_MODE = bench == "ycsb" ? 2 : bench == "smallbank" ? 0 : 1;
        meta = new MetaManager(bench, &index, &pages, node_id, SYSTEM_MODE);
        auto* node = new ComputeNode(node_id, meta->remote_server_nodes[0].ip,
                                     meta->remote_server_nodes[0].port, meta);
        std::vector<std::string> ips;
        std::vector<int> ports;
        for (const auto& n : meta->remote_compute_nodes) {
            ips.push_back(n.ip);
            ports.push_back(n.port);
        }
        server = new ComputeServer(node, ips, ports);
        CHECK(server->IsMvccWorkload() && server->IsLogEnabled());
        flusher = std::thread([this] {
            while (flusher_running.load()) {
                if (flusher_paused.load()) {
                    usleep(1000);
                    continue;
                }
                server->WaitLogFlushTrigger(10);
                if (flusher_paused.load()) continue;
                server->LogFlush();
            }
            server->LogFlush();
        });
        server->WaitReady();
        server->WaitForClusterStart();
        server->SetMvccGate([this](const mvcc::TxnId&, mvcc::NodeId node, const char* phase) {
            if (node != static_cast<mvcc::NodeId>(server->getNodeID())) return;
            if (std::string_view(phase) == "main_fetch_x") {
                ++main_fetch_x;
                return;
            }
            std::unique_lock<std::mutex> lock(install_mutex);
            if (!install_gate_armed || install_gate_phase != phase) return;
            install_gate_entered = true;
            install_cv.notify_all();
            install_cv.wait(lock, [this] { return !install_gate_armed; });
        });
    }
    DTX* Get(uint64_t handle) {
        auto it = txns.find(handle);
        CHECK(it != txns.end()) << "unknown transaction handle";
        return it->second.get();
    }
    DTX* Begin() {
        auto tx = std::make_unique<DTX>(meta, 0, 0, 0, &index, &pages, server,
                                        server->get_storage_channel(), server->get_storage_channel(),
                                        server->get_pagetable_channel(), fetch_pool.get());
        tx->TxBegin(1);
        const auto handle = next_txn_handle++;
        force_applied[handle] = false;
        auto* raw = tx.get();
        txns.emplace(handle, std::move(tx));
        return raw;
    }
    uint64_t Handle(DTX* tx) {
        for (auto& entry : txns) if (entry.second.get() == tx) return entry.first;
        CHECK(false) << "transaction not in the control table";
        return 0;
    }
    void MaybeForce(DTX& tx) {
        if (system_mode != 4) return;
        const auto handle = Handle(&tx);
        auto forced = force_2pc.find(handle);
        if (forced == force_2pc.end() || force_applied[handle]) return;
        tx.NoteKeyAccess(forced->second);
        tx.DecideCommitMode();
        force_applied[handle] = true;
    }
    std::string Read(DTX& tx, table_id_t table, itemkey_t key) {
        MaybeForce(tx);
        tx.ClearReadOnlySet();
        auto item = std::make_shared<DataItem>(table);
        tx.AddToReadOnlySet(item, key);
        if (!tx.TxExe()) return "FAIL";
        CHECK((tx.tx_status == TXStatus::TX_VAL_NOTFOUND) == !item->valid);
        if (!item->valid) return "NOTFOUND";
        CHECK(item->value_size >= 8);
        return "VALUE " + std::to_string(Number(item));
    }
    std::string Write(DTX& tx, table_id_t table, itemkey_t key, uint64_t value) {
        MaybeForce(tx);
        auto item = std::make_shared<DataItem>(table);
        tx.AddToReadWriteSet(item, key, [value](DataItem& row) { SetNumber(row, value); });
        if (!tx.TxExe()) return tx.mvcc_undo_full ? "FULL" : (tx.mvcc_write_conflict ? "CONFLICT" : "ABORTING");
        return "OK";
    }
    std::string Delete(DTX& tx, table_id_t table, itemkey_t key) {
        MaybeForce(tx);
        auto item = std::make_shared<DataItem>(table);
        tx.AddToReadWriteSet(item, key, [](DataItem& row) { row.valid = 0; });
        if (!tx.TxExe()) return tx.mvcc_undo_full ? "FULL" : (tx.mvcc_write_conflict ? "CONFLICT" : "ABORTING");
        return "OK";
    }
    std::string Commit(DTX& tx, uint64_t* commit_ts) {
        const bool ok = tx.TxCommit();
        if (ok) *commit_ts = tx.commit_ts;
        return ok ? "OK" : "FAIL";
    }
    undo::RowImage Current(table_id_t table, itemkey_t key) {
        const auto rid = index.Search(table, key);
        CHECK(rid.page_no_ != INVALID_PAGE_ID) << "unknown key";
        const auto hdr = server->get_file_hdr_cached(table);
        auto* data = server->FetchSPage(table, rid.page_no_);
        auto row = mvcc::CopyRow(data, *hdr,
            {uint32_t(table), uint32_t(rid.page_no_), uint32_t(rid.slot_no_), key});
        server->ReleaseSPage(table, rid.page_no_);
        return row;
    }
    void EvictMain(table_id_t table, itemkey_t key) {
        const auto rid = index.Search(table, key);
        CHECK(rid.page_no_ != INVALID_PAGE_ID) << "unknown key";
        auto* pool = server->get_node()->getBufferPool(table);
        if (!pool->is_in_bufferPool({table, rid.page_no_})) return;
        std::set<page_id_t> pages;
        for (const auto& entry : index.getRidsMap().at(table)) pages.insert(entry.second.page_no_);
        for (page_id_t page : pages) {
            // 2PC pages never migrate: a node can only fetch its own partitions.
            if (SYSTEM_MODE == 2 &&
                server->get_node_id_by_page_id(table, page) != server->getNodeID()) continue;
            if (!pool->is_in_bufferPool({table, rid.page_no_})) break;
            server->FetchSPage(table, page);
            server->ReleaseSPage(table, page);
        }
        CHECK(!pool->is_in_bufferPool({table, rid.page_no_})) << "main page did not evict";
    }
    std::string Stats() {
        const auto& commit_log = benchmark_stats::log_details[benchmark_stats::CommitLog];
        std::ostringstream out;
        out << "OK undo_pages=" << server->Mvcc().Undo().AllocatedPages()
            << " undo_evictions=" << server->UndoEvictions()
            << " undo_transfers=" << server->undo_permission_transfers_.load()
            << " undo_remote_fetches=" << server->undo_remote_fetches_.load()
            << " undo_wal_waits=" << server->undo_wal_waits_.load()
            << " history_steps=" << server->Mvcc().HistorySteps()
            << " si_conflicts=" << server->Mvcc().SiConflicts()
            << " lock_conflicts=" << server->mvcc_lock_conflicts_.load()
            << " prepared_contexts=" << server->prepared_txns_.Size()
            << " single_writes=" << server->single_writes_.Size()
            << " finished_participants=" << server->MvccFinishedCount()
            << " capacity_rejects=" << server->Mvcc().Undo().CapacityRejects()
            << " admitted_reservations=" << server->Mvcc().Undo().AdmittedReservations()
            << " tit_waiters=" << server->Mvcc().PendingTitWaiters()
            << " tit_completed_waits=" << benchmark_stats::tit_pending_wait.count.load()
            << " tit_status_rpcs=" << benchmark_stats::tit_status_rpc.count.load()
            << " tso_snapshots=" << benchmark_stats::tso_snapshot_rpc.count.load()
            << " tso_snapshot_ns=" << benchmark_stats::tso_snapshot_rpc.total_ns.load()
            << " tso_commits=" << benchmark_stats::tso_commit_rpc.count.load()
            << " tso_commit_ns=" << benchmark_stats::tso_commit_rpc.total_ns.load()
            << " commit_wal_waits=" << benchmark_stats::log_waits[benchmark_stats::CommitLog].count.load()
            << " commit_wal_samples=" << commit_log.count.load()
            << " commit_wal_rpc_ns=" << commit_log.rpc_ns.load()
            << " commit_wal_write_ns=" << commit_log.write_ns.load()
            << " commit_wal_sync_ns=" << commit_log.sync_ns.load()
            << " persisted_lsn=" << server->GetPersistedLSN();
        return out.str();
    }
    void StartAsync(uint64_t handle, bool commit, uint64_t result_id) {
        auto state = std::make_shared<Result>();
        results[result_id] = state;
        DTX* tx = Get(handle);
        std::thread([this, tx, commit, state] {
            uint64_t commit_ts = 0;
            bool ok = false;
            if (commit) {
                ok = tx->TxCommit();
                commit_ts = tx->commit_ts;
            } else {
                tx->TxAbortWorkLoad();
                ok = tx->tx_status == TXStatus::TX_ABORT;
            }
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->ok = ok;
                state->commit_ts = commit_ts;
                state->done = true;
            }
            state->cv.notify_all();
        }).detach();
    }
    std::string ResultOf(uint64_t id, bool wait) {
        auto it = results.find(id);
        CHECK(it != results.end());
        auto state = it->second;
        std::unique_lock<std::mutex> lock(state->mutex);
        if (wait) {
            state->cv.wait(lock, [&] { return state->done; });
        } else if (!state->done) {
            return "PENDING";
        }
        return std::string(state->ok ? "OK " : "FAIL ") + std::to_string(state->commit_ts);
    }
    void Shutdown() {
        server->pending_releases_.Drain();
        flusher_running.store(false);
        server->NotifyLogFlush();
        flusher.join();
        server->LogFlush();
        CHECK(socket_finish_client(meta->remote_server_nodes[0].ip,
                                   meta->remote_server_meta_port) == 0);
        if (server->getNodeID() == 0) {
            CHECK(server->ShutdownStorageServer());
            // The remote meta barrier may already have made remote_node exit;
            // the barrier itself is the authoritative finish handshake.
            server->ShutdownRemoteServer();
        }
    }
};

std::string Dispatch(ClusterNode& node, const std::string& line, bool* quit) {
    std::istringstream in(line);
    std::string command;
    in >> command;
    if (command == "DRAIN_RELEASES") { node.server->pending_releases_.Drain(); return "OK"; }
    if (command == "MAIN_X_COUNT") return std::to_string(node.main_fetch_x.load());
    if (command == "PING") return "PONG";
    if (command == "BEGIN") {
        auto* tx = node.Begin();
        return "OK " + std::to_string(node.Handle(tx)) + " " +
               std::to_string(tx->SnapshotTs()) + " " + std::to_string(tx->SnapshotLocalId());
    }
    if (command == "FORCE") {
        uint64_t handle; std::string branch;
        in >> handle >> branch;
        CHECK(branch == "2pc" || branch == "lazy");
        node.force_2pc[handle] = branch == "2pc";
        node.force_applied[handle] = false;
        return "OK";
    }
    if (command == "OWNER") {
        int table; uint64_t key;
        in >> table >> key;
        const auto rid = node.index.Search(table, key);
        CHECK(rid.page_no_ != INVALID_PAGE_ID);
        return "OK " + std::to_string(node.server->get_node_id_by_page_id(table, rid.page_no_));
    }
    if (command == "KEY_ON_NODE") {
        int table; int target;
        in >> table >> target;
        unsigned skip = 0;
        in >> skip;
        for (const auto& entry : node.index.getRidsMap().at(table)) {
            if (node.server->get_node_id_by_page_id(table, entry.second.page_no_) != target) continue;
            if (skip == 0) return "OK " + std::to_string(entry.first);
            --skip;
        }
        CHECK(false) << "no key owned by the requested node";
    }
    if (command == "HOLD_INSTALL") {
        std::string phase;
        in >> phase;
        std::lock_guard<std::mutex> lock(node.install_mutex);
        node.install_gate_phase = phase;
        node.install_gate_entered = false;
        node.install_gate_armed = true;
        return "OK";
    }
    if (command == "WAIT_INSTALL_GATE") {
        std::unique_lock<std::mutex> lock(node.install_mutex);
        node.install_cv.wait(lock, [&] { return node.install_gate_entered; });
        return "OK";
    }
    if (command == "RELEASE_INSTALL_GATE") {
        std::lock_guard<std::mutex> lock(node.install_mutex);
        node.install_gate_armed = false;
        node.install_cv.notify_all();
        return "OK";
    }
    if (command == "TIT") {
        uint32_t home; uint64_t tid;
        in >> home >> tid;
        const auto status = node.server->Mvcc().LookupStatus({node.server->Mvcc().Epoch(), home, tid});
        return "OK " + std::to_string(status && status->state == mvcc::State::Committed ? status->commit_ts : 0);
    }
    if (command == "SNAPSHOT") {
        uint64_t handle; in >> handle;
        return "OK " + std::to_string(node.Get(handle)->SnapshotTs());
    }
    if (command == "READ") {
        uint64_t handle; int table; uint64_t key;
        in >> handle >> table >> key;
        return node.Read(*node.Get(handle), table, key);
    }
    if (command == "LOCK_PAIR") {
        uint64_t handle; int table; itemkey_t a, b;
        in >> handle >> table >> a >> b;
        auto& tx = *node.Get(handle);
        node.MaybeForce(tx);
        tx.AddToReadWriteSet(std::make_shared<DataItem>(table), a);
        tx.AddToReadWriteSet(std::make_shared<DataItem>(table), b);
        return tx.TxExe() ? "OK" : "ABORT";
    }
    if (command == "PRIVATE_WRITE") {
        uint64_t handle, value;
        table_id_t table;
        itemkey_t key;
        in >> handle >> table >> key >> value;
        auto& tx = *node.Get(handle);
        auto item = std::make_shared<DataItem>(table);
        tx.AddToReadWriteSet(item, key);
        if (!tx.TxExe()) return "ABORT";
        SetNumber(*item, value);
        return "OK";
    }
    if (command == "WRITE") {
        uint64_t handle; int table; uint64_t key, value;
        in >> handle >> table >> key >> value;
        return node.Write(*node.Get(handle), table, key, value);
    }
    if (command == "DELETE") {
        uint64_t handle; int table; uint64_t key;
        in >> handle >> table >> key;
        return node.Delete(*node.Get(handle), table, key);
    }
    if (command == "COMMIT") {
        uint64_t handle; in >> handle;
        uint64_t commit_ts = 0;
        return node.Commit(*node.Get(handle), &commit_ts) == "OK" ? "OK " + std::to_string(commit_ts) : "FAIL";
    }
    if (command == "ABORT") {
        uint64_t handle; in >> handle;
        auto* tx = node.Get(handle);
        tx->TxAbortWorkLoad();
        return tx->tx_status == TXStatus::TX_ABORT ? "OK" : "FAIL";
    }
    if (command == "COMMIT_ASYNC" || command == "ABORT_ASYNC") {
        uint64_t handle; in >> handle;
        const auto id = node.next_result++;
        node.StartAsync(handle, command == "COMMIT_ASYNC", id);
        return "OK " + std::to_string(id);
    }
    if (command == "RESULT") {
        uint64_t id; in >> id;
        return node.ResultOf(id, true);
    }
    if (command == "TRY_RESULT") {
        uint64_t id; in >> id;
        return node.ResultOf(id, false);
    }
    if (command == "WRITE_LOOP") {
        uint64_t handle, count, base; int table; uint64_t key;
        in >> handle >> table >> key >> count >> base;
        auto* tx = node.Get(handle);
        for (uint64_t i = 0; i < count; ++i) {
            const auto r = node.Write(*tx, table, key, base + i);
            if (r != "OK") return "FAIL " + std::to_string(i) + " " + r;
        }
        return "OK " + std::to_string(count);
    }
    if (command == "READ_LOOP") {
        uint64_t handle, count, expect; int table; uint64_t key;
        in >> handle >> table >> key >> count >> expect;
        auto* tx = node.Get(handle);
        for (uint64_t i = 0; i < count; ++i) {
            const auto r = node.Read(*tx, table, key);
            if (r != "VALUE " + std::to_string(expect))
                return "BAD " + std::to_string(i) + " " + r;
        }
        return "OK " + std::to_string(count);
    }
    if (command == "CURRENT") {
        int table; uint64_t key;
        in >> table >> key;
        const auto row = node.Current(table, key);
        uint64_t value = 0;
        if (row.value.size() >= sizeof(value)) memcpy(&value, row.value.data(), sizeof(value));
        std::ostringstream out;
        out << "OK " << value
            << " initial=" << row.version.initial
            << " deleted=" << row.version.deleted
            << " creator_home=" << row.version.creator.home
            << " creator_local=" << row.version.creator.local
            << " previous_writer=" << row.previous.page.writer
            << " previous_number=" << row.previous.page.number
            << " previous_slot=" << row.previous.slot
            << " previous_lsn=" << row.previous.lsn
            << " value_size=" << row.value.size();
        return out.str();
    }
    if (command == "ROW_CTS") {
        int table; itemkey_t key; in >> table >> key;
        const auto rid = node.index.Search(table, key);
        const auto hdr = node.server->get_file_hdr_cached(table);
        char* data = node.server->FetchSPage(table, rid.page_no_);
        const auto cts = mvcc::RowAt(data, *hdr,
            {uint32_t(table), uint32_t(rid.page_no_), uint32_t(rid.slot_no_), key})->commitTimeStamp;
        node.server->ReleaseSPage(table, rid.page_no_);
        return std::to_string(cts);
    }
    if (command == "LOCAL_X_ROW" || command == "QUEUE_DEPTH") {
        int table; itemkey_t key; in >> table >> key;
        const auto rid = node.index.Search(table, key);
        CHECK(rid.page_no_ != INVALID_PAGE_ID);
        if (command == "QUEUE_DEPTH")
            return std::to_string(node.server->lock_wait_mgr()->WaitQueueDepth({table, rid.page_no_, rid.slot_no_}));
        const auto hdr = node.server->get_file_hdr_cached(table);
        auto* compute = node.server->get_node();
        std::string result = "NO_X";
        compute->getLazyPageLockTable(table)->GetLock(rid.page_no_)->TryWithExclusiveLocal([&] {
            return compute->getBufferPool(table)->TryVisitCachedPage({table, rid.page_no_}, [&](Page* page) {
                const auto* row = mvcc::RowAt(page->get_data(), *hdr,
                    {uint32_t(table), uint32_t(rid.page_no_), uint32_t(rid.slot_no_), key});
                result = row->lock == UNLOCKED ? "FREE" : "LOCKED " + std::to_string(row->timeStamp);
            });
        });
        return result;
    }
    if (command == "WRITE_FIBER") {
        uint64_t handle, key, value; int table;
        in >> handle >> table >> key >> value;
        std::string reply;
        Scheduler scheduler(1, false, "SQL_Scheduler");
        scheduler.start();
        scheduler.schedule([&] {
            reply = node.Write(*node.Get(handle), table, key, value);
            Scheduler::setJobFinish(true);
        });
        scheduler.stop();
        return reply;
    }
    if (command == "FETCH_X_MAIN" || command == "FETCH_S_MAIN") {
        int table; uint64_t key;
        in >> table >> key;
        const auto rid = node.index.Search(table, key);
        CHECK(rid.page_no_ != INVALID_PAGE_ID);
        if (command == "FETCH_X_MAIN") {
            node.server->FetchXPage(table, rid.page_no_);
            node.server->ReleaseXPage(table, rid.page_no_);
        } else {
            node.server->FetchSPage(table, rid.page_no_);
            node.server->ReleaseSPage(table, rid.page_no_);
        }
        return "OK";
    }
    if (command == "EVICT_MAIN") {
        int table; uint64_t key;
        in >> table >> key;
        node.EvictMain(table, key);
        return "OK";
    }
    if (command == "UNDO_CAPACITY") {
        uint64_t pages; in >> pages;
        node.server->Mvcc().Undo().SetCapacityForTest(pages);
        return "OK";
    }
    if (command == "CLEAR_UNDO") {
        node.server->Mvcc().Undo().ClearCache();
        return "OK";
    }
    if (command == "HAS_MAIN") {
        int table; uint64_t key;
        in >> table >> key;
        const auto rid = node.index.Search(table, key);
        CHECK(rid.page_no_ != INVALID_PAGE_ID);
        return node.server->get_node()->getBufferPool(table)->is_in_bufferPool({table, rid.page_no_})
            ? "YES" : "NO";
    }
    if (command == "HAS_UNDO") {
        uint32_t writer, number;
        in >> writer >> number;
        const auto key = undo::PageKey{undo::PageKind::Undo, node.server->Mvcc().Epoch(), writer, number, 1};
        return node.server->get_node()->getBufferPool(UNDO_TABLE_ID)
                   ->is_in_bufferPool({UNDO_TABLE_ID, undo::PageNumber(key)}) ? "YES" : "NO";
    }
    if (command == "KEY") {
        int table; in >> table;
        const auto& rids = node.index.getRidsMap().at(table);
        CHECK(!rids.empty());
        return "OK " + std::to_string(rids.begin()->first);
    }
    if (command == "KEY_AT") {
        int table; size_t index;
        in >> table >> index;
        const auto& rids = node.index.getRidsMap().at(table);
        CHECK(index < rids.size());
        auto it = rids.begin();
        std::advance(it, index);
        return "OK " + std::to_string(it->first);
    }
    if (command == "PAGE_OF") {
        int table; uint64_t key;
        in >> table >> key;
        const auto rid = node.index.Search(table, key);
        CHECK(rid.page_no_ != INVALID_PAGE_ID);
        return "OK " + std::to_string(rid.page_no_);
    }
    if (command == "KEY_OTHER_PAGE") {
        int table; int excluded_page;
        in >> table >> excluded_page;
        for (const auto& entry : node.index.getRidsMap().at(table)) {
            if (entry.second.page_no_ != excluded_page)
                return "OK " + std::to_string(entry.first);
        }
        CHECK(false) << "no key on a different page";
    }
    if (command == "STATS") return node.Stats();
    if (command == "QUERY_STATUS" || command == "LOCAL_STATUS") {
        uint32_t home; uint64_t local;
        in >> home >> local;
        const mvcc::TxnId id{node.server->Mvcc().Epoch(), home, local};
        const auto status = command == "LOCAL_STATUS" ? node.server->Mvcc().Snapshots().Lookup(id)
                                                       : node.server->Mvcc().LookupStatus(id);
        if (!status) return "UNKNOWN";
        const char* label = nullptr;
        switch (status->state) {
            case mvcc::State::Active: label = "ACTIVE"; break;
            case mvcc::State::Committing: label = "COMMITTING"; break;
            case mvcc::State::Committed: label = "COMMITTED"; break;
            case mvcc::State::Aborted: label = "ABORTED"; break;
            case mvcc::State::ReadOnlyFinished: label = "READ_ONLY"; break;
        }
        CHECK(label);
        return std::string("FOUND ") + label + " " + std::to_string(status->commit_ts);
    }
    if (command == "PAUSE_FLUSH") {
        node.flusher_paused.store(true);
        return "OK";
    }
    if (command == "RESUME_FLUSH") {
        node.flusher_paused.store(false);
        node.server->NotifyLogFlush();
        return "OK";
    }
    if (command == "FLUSH") {
        node.server->LogFlush();
        return "OK " + std::to_string(node.server->GetPersistedLSN());
    }
    if (command == "SHUTDOWN") {
        node.Shutdown();
        *quit = true;
        return "OK";
    }
    CHECK(false) << "unknown control command: " << line;
    return "FAIL";
}
}  // namespace

int main(int argc, char** argv) {
    EnableFatalChecks();
    CHECK(argc == 4 || argc == 5)
        << "usage: mvcc_cluster_node <bench> <node_id> <control_port> [system_mode]";
    const std::string bench(argv[1]);
    const int node_id = std::stoi(argv[2]);
    const int control_port = std::stoi(argv[3]);
    const int system_mode = argc == 5 ? std::stoi(argv[4]) : 1;
    ClusterNode node(bench, node_id, system_mode);
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0);
    int one = 1;
    CHECK(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) == 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(control_port);
    CHECK(bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    CHECK(listen(fd, 1) == 0);
    std::cout << "CONTROL_READY " << control_port << std::endl;
    const int client = accept(fd, nullptr, nullptr);
    CHECK(client >= 0);
    FILE* stream = fdopen(client, "r+");
    CHECK(stream != nullptr);
    char buffer[4096];
    bool quit = false;
    while (!quit && fgets(buffer, sizeof(buffer), stream)) {
        std::string line(buffer);
        while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) line.pop_back();
        if (line.empty()) continue;
        const auto response = Dispatch(node, line, &quit);
        fprintf(stream, "%s\n", response.c_str());
        fflush(stream);
    }
    fclose(stream);
    close(fd);
    std::exit(0);
}
