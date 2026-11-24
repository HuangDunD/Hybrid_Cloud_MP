// Integration executable: run against fresh storage/remote servers. Uses the
// production ComputeServer, DTX, BLink lookup, Lazy permissions and WAL flusher.
#include "common/fatal_checks.h"
#include "compute_server/worker/handler.h"
#include "dtx/dtx.h"
#include "mvcc/row.h"
#include <iostream>
#include <set>

namespace {
uint64_t Number(const DataItemPtr& item) {
    CHECK(item->valid && item->value_size >= 8);
    uint64_t result;
    memcpy(&result, item->value, 8);
    return result;
}
void Number(DataItem& item, uint64_t value) {
    CHECK(item.value_size >= 8);
    memcpy(item.value, &value, 8);
}
struct Fixture {
    IndexCache index;
    PageCache pages;
    table_id_t primary;
    MetaManager* meta;
    ComputeServer* server;
    std::thread flusher;
    explicit Fixture(const std::string& bench) {
        primary = bench == "tpcc" ? 2 : 0;
        Handler handler;
        handler.ConfigureComputeNodeRunSQL();
        WORKLOAD_MODE = bench == "ycsb" ? 2 : bench == "smallbank" ? 0 : 1;
        meta = new MetaManager(bench, &index, &pages, 0, SYSTEM_MODE);
        auto* node =
            new ComputeNode(0, meta->remote_server_nodes[0].ip, meta->remote_server_nodes[0].port, meta);
        server =
            new ComputeServer(node, {meta->remote_compute_nodes[0].ip}, {meta->remote_compute_nodes[0].port});
        CHECK(server->IsLazyWorkload() && server->IsLogEnabled());
        flusher = std::thread([this] {
            while (server->log_flush_running.load()) {
                server->WaitLogFlushTrigger(server->GetLogFlushIntervalMs());
                server->LogFlush();
            }
            server->LogFlush();
        });
        server->WaitReady();
        server->WaitForClusterStart();
    }
    std::unique_ptr<DTX> Begin() {
        auto tx =
            std::make_unique<DTX>(meta, 0, 0, 0, &index, &pages, server, server->get_storage_channel(),
                                  server->get_storage_channel(), server->get_pagetable_channel(), nullptr);
        tx->TxBegin(1);
        return tx;  // Runtime allocates txn identity; snapshot is independent.
    }
    DataItemPtr Read(DTX& tx, itemkey_t key, table_id_t table = -1) {
        if (table == -1) table = primary;
        tx.ClearReadOnlySet();
        auto item = std::make_shared<DataItem>(table);
        tx.AddToReadOnlySet(item, key);
        const bool ok = tx.TxExe();
        CHECK(ok);
        return item;
    }
    DataItemPtr Write(DTX& tx, itemkey_t key, uint64_t number) {
        auto item = std::make_shared<DataItem>(primary);
        tx.AddToReadWriteSet(item, key, [number](DataItem& row) { Number(row, number); });
        const bool ok = tx.TxExe();
        CHECK(ok);
        return item;
    }
    void Commit(DTX& tx) {
        const bool ok = tx.TxCommit();
        CHECK(ok);
    }
    undo::RowImage Current(itemkey_t key, table_id_t table = -1) {
        if (table == -1) table = primary;
        const auto rid = index.Search(table, key);
        const auto hdr = server->get_file_hdr_cached(table);
        auto* data = server->FetchSPage(table, rid.page_no_);
        auto row =
            mvcc::CopyRow(data, *hdr, {uint32_t(table), uint32_t(rid.page_no_), uint32_t(rid.slot_no_), key});
        server->ReleaseSPage(table, rid.page_no_);
        return row;
    }
    // Evict through real LRU/permission release, not by dropping a private copy.
    void EvictMain(itemkey_t key) {
        const auto target = index.Search(primary, key).page_no_;
        auto* pool = server->get_node()->getBufferPoolByIndex(primary);
        std::set<page_id_t> visited;
        for (const auto& entry : index.getRidsMap().at(primary)) {
            const auto page = entry.second.page_no_;
            if (page == target || !visited.insert(page).second) continue;
            server->FetchSPage(primary, page);
            server->ReleaseSPage(primary, page);
            if (!pool->is_in_bufferPool(target)) break;
        }
        CHECK(!pool->is_in_bufferPool(target)) << "fixture needs more pages than its small main buffer";
    }
    void Stop() {
        server->log_flush_running.store(false);
        flusher.join();
        const int rc = socket_finish_client(meta->remote_server_nodes[0].ip, meta->remote_server_meta_port);
        CHECK(rc == 0);
        const bool storage_stopped = server->ShutdownStorageServer();
        CHECK(storage_stopped);
        server->ShutdownRemoteServer();
    }
};
void Exercise(Fixture& f, const std::string& bench) {
    const auto key = f.index.getRidsMap().at(f.primary).begin()->first;
    const auto initial = f.Current(key);
    CHECK(initial.version.initial && initial.previous.empty());
    const auto pages_before = f.server->Mvcc().Undo().AllocatedPages();
    auto ro = f.Begin();
    f.Read(*ro, key);
    f.Commit(*ro);
    CHECK(f.server->Mvcc().Undo().AllocatedPages() == pages_before);
    auto init = f.Begin();
    f.Write(*init, key, 100);
    f.Commit(*init);
    auto old100 = f.Begin();
    const auto fixed100 = old100->SnapshotScn();
    CHECK(Number(f.Read(*old100, key)) == 100);
    auto writer = f.Begin();
    f.Write(*writer, key, 90);
    f.Commit(*writer);
    auto old90 = f.Begin();
    CHECK(old90->SnapshotScn() == f.server->Mvcc().Snapshots().SafeScn());
    CHECK(Number(f.Read(*old90, key)) == 90);
    writer = f.Begin();
    f.Write(*writer, key, 85);
    f.Write(*writer, key, 80);
    auto conflicting = f.Begin();
    auto conflict_item = std::make_shared<DataItem>(f.primary);
    conflicting->AddToReadWriteSet(conflict_item, key);
    const bool locked_out = conflicting->TxExe();
    CHECK(!locked_out && !conflicting->mvcc_write_conflict);  // active lock conflict, not stale snapshot

    CHECK(Number(f.Read(*writer, key)) == 80);
    CHECK(Number(f.Read(*old90, key)) == 90 && Number(f.Read(*old100, key)) == 100);
    f.Commit(*writer);
    auto now = f.Begin();
    CHECK(Number(f.Read(*now, key)) == 80);
    f.Commit(*now);
    CHECK(old100->SnapshotScn() == fixed100 && Number(f.Read(*old100, key)) == 100);
    // Old snapshot acquires the physical lock, then fails the SI write check.
    auto stale = std::make_shared<DataItem>(f.primary);
    old90->AddToReadWriteSet(stale, key, [](DataItem& r) { Number(r, 70); });
    const bool allowed = old90->TxExe(false);
    CHECK(!allowed && old90->mvcc_write_conflict);
    const bool retry_same_attempt = old90->TxExe();
    CHECK(!retry_same_attempt && old90->tx_status == TXStatus::TX_ABORT);
    CHECK(Number(f.Read(*old100, key)) == 100);
    std::cout << "PASS real DTX: fixed 100/90/80 snapshots, uncommitted self-read, equality, SI conflict\n";

    const auto before_lock = f.Current(key);
    auto lock = f.Begin();
    auto locked = std::make_shared<DataItem>(f.primary);
    lock->AddToReadWriteSet(locked, key);
    bool ok = lock->TxExe();
    CHECK(ok);
    const auto locked_row = f.Current(key);
    CHECK(undo::EncodeRow(locked_row) == undo::EncodeRow(before_lock));
    CHECK(Number(f.Read(*old100, key)) == 100);
    f.Commit(*lock);
    CHECK(undo::EncodeRow(f.Current(key)) == undo::EncodeRow(before_lock));
    // Legacy private-copy path: self-read sees private data; abort installs nothing.
    auto priv = f.Begin();
    auto copy = std::make_shared<DataItem>(f.primary);
    priv->AddToReadWriteSet(copy, key);
    ok = priv->TxExe();
    CHECK(ok);
    Number(*copy, 75);
    CHECK(Number(f.Read(*priv, key)) == 75);
    priv->TxAbortWorkLoad();
    CHECK(undo::EncodeRow(f.Current(key)) == undo::EncodeRow(before_lock));
    priv = f.Begin();
    copy = std::make_shared<DataItem>(f.primary);
    priv->AddToReadWriteSet(copy, key);
    ok = priv->TxExe();
    CHECK(ok);
    Number(*copy, 76);
    f.Write(*priv, key, 77);  // callback consumes the earlier pending private value
    CHECK(Number(copy) == 77);
    f.Commit(*priv);
    now = f.Begin();
    CHECK(Number(f.Read(*now, key)) == 77);
    f.Commit(*now);
    priv = f.Begin();
    copy = std::make_shared<DataItem>(f.primary);
    priv->AddToReadWriteSet(copy, key);
    ok = priv->TxExe();
    CHECK(ok);
    Number(*copy, 78);
    f.Commit(*priv);
    now = f.Begin();
    CHECK(Number(f.Read(*now, key)) == 78);
    f.Commit(*now);
    std::cout << "PASS real DTX: lock-only creator/history unchanged, private self-read/abort/writeback\n";

    const auto before_noop = f.Current(key);
    priv = f.Begin();
    copy = std::make_shared<DataItem>(f.primary);
    priv->AddToReadWriteSet(copy, key);
    ok = priv->TxExe();
    CHECK(ok);
    Number(*copy, 70);
    f.Write(*priv, key, 78);  // supersede private change with current value
    f.Commit(*priv);
    CHECK(undo::EncodeRow(f.Current(key)) == undo::EncodeRow(before_noop));

    const auto before_abort = f.Current(key);
    auto next_key = f.index.getRidsMap().at(f.primary).begin();
    ++next_key;
    const auto key2 = next_key->first;
    const auto before_abort2 = f.Current(key2);
    auto aborting = f.Begin();
    for (int i = 0; i < 40; ++i) f.Write(*aborting, i % 2 ? key2 : key, 1000 + i);
    CHECK(Number(f.Read(*aborting, key)) == 1038 && Number(f.Read(*aborting, key2)) == 1039);
    f.server->Mvcc().Undo().ClearCache();
    f.EvictMain(key);
    CHECK(Number(f.Read(*old100, key)) == 100);
    f.server->Mvcc().Undo().ClearCache();
    f.EvictMain(key);
    aborting->TxAbortWorkLoad();
    CHECK(undo::EncodeRow(f.Current(key)) == undo::EncodeRow(before_abort));
    CHECK(undo::EncodeRow(f.Current(key2)) == undo::EncodeRow(before_abort2));
    CHECK(f.server->UndoEvictions() > 0);
    std::cout << "PASS real DTX: 40 overwrites, small undo LRU, main eviction/refetch, reverse abort\n";
    f.Commit(*old100);

    const table_id_t tomb_table = bench == "tpcc" ? 4 : 0;
    const auto tomb_key = f.index.getRidsMap().at(tomb_table).begin()->first;
    const auto old_image = f.Current(tomb_key, tomb_table);
    auto before_delete = f.Begin();
    auto deleter = f.Begin();
    auto dead = std::make_shared<DataItem>(tomb_table);
    deleter->AddToReadWriteSet(dead, tomb_key);
    ok = deleter->TxExe();
    CHECK(ok);
    dead->valid = 0;  // exactly the current TPC-C Delivery private-copy deletion
    CHECK(!f.Read(*deleter, tomb_key, tomb_table)->valid);
    CHECK(f.Read(*before_delete, tomb_key, tomb_table)->valid);
    f.Commit(*deleter);
    now = f.Begin();
    CHECK(!f.Read(*now, tomb_key, tomb_table)->valid);
    f.Commit(*now);
    auto retained = f.Read(*before_delete, tomb_key, tomb_table);
    CHECK(retained->valid &&
          std::string(reinterpret_cast<char*>(retained->value), retained->value_size) == old_image.value);
    CHECK(f.Current(tomb_key, tomb_table).version.deleted);
    f.Commit(*before_delete);
    std::cout << "PASS real DTX: visible tombstone stops, old snapshot retains RID/index/history (table "
              << tomb_table << ")\n";
}
}  // namespace
int main(int argc, char** argv) {
    EnableFatalChecks();
    std::cout << std::unitbuf;
    CHECK(argc == 2);
    Fixture fixture(argv[1]);
    Exercise(fixture, argv[1]);
    fixture.Stop();
    std::cout << "PASS lazy_mvcc_test " << argv[1] << std::endl;
    // ComputeServer has process-lifetime detached RPC workers (same as benchmark main).
    std::exit(0);
}
