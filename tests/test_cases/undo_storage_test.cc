#include "common/fatal_checks.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <sys/resource.h>
#include <sys/wait.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include "bufferpool.h"
#include "undo/config.h"
#include "undo/manager.h"
#include <limits>
#include "undo/redo.h"
#include "mvcc/row.h"
#include "storage/storage_rpc.h"
#include "storage/log_manager.h"
#include "workload/tpcc/tpcc_db.h"
#include "workload/smallbank/smallbank_db.h"
#include "workload/ycsb/ycsb_db.h"
namespace undo {
// Advance an unused test allocator without materializing millions of old pages.
struct ManagerAddressLimitTest {
    static void LeaveOnePage(Manager& manager) {
        CHECK(manager.AllocatedPages() == 0);
        manager.next_page_ = manager.addressable_pages_ - 1;
    }
};
}  // namespace undo
using namespace std::chrono_literals;
namespace {
const std::string epoch(16, 'u');
const mvcc::TxnId txn{epoch, 0, 1};
const undo::PageKey key{undo::PageKind::Undo, epoch, 0, 1, 1};
const undo::RowId rid{0, 1, 0, 42};
std::string Encode(const LogRecord& record) {
    std::string bytes(record.log_tot_len_, '\0');
    record.serialize(bytes.data());
    return bytes;
}
undo::RowImage Old(size_t size = 664, char fill = 'a') {
    return {rid, {{}, true, false}, {}, std::string(size, fill)};
}
void Death(const std::function<void()>& fn) {
    const pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        rlimit lim{0, 0};
        setrlimit(RLIMIT_CORE, &lim);
        fn();
        _exit(0);
    }
    int status = 0;
    const auto waited = waitpid(pid, &status, 0);
    CHECK(waited == pid && WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
}
void CheckFormats() {
    static_assert(sizeof(DataItem) == 72 + sizeof(mvcc::RowFields));
    for (size_t size :
         {sizeof(tpcc_warehouse_val_t), sizeof(tpcc_district_val_t), sizeof(tpcc_customer_val_t),
          sizeof(tpcc_customer_index_val_t), sizeof(tpcc_history_val_t), sizeof(tpcc_new_order_val_t),
          sizeof(tpcc_order_val_t), sizeof(tpcc_order_index_val_t), sizeof(tpcc_order_line_val_t),
          sizeof(tpcc_item_val_t), sizeof(tpcc_stock_val_t), sizeof(smallbank_savings_val_t),
          sizeof(smallbank_checking_val_t), sizeof(ycsb_user_table_val)})
        CHECK(size <= undo::kMaxValueBytes);
    CHECK(undo::BufferSize(JsonConfig::load("{}", "test")) == 10000);
    CHECK(undo::BufferSize(JsonConfig::load("{\"undo_buffer_pool_size_pages\":1}", "test")) == 1);
    undo::Record full{1, {}, Old(undo::kMaxValueBytes)};
    auto page = undo::Initialize(key, txn, 1);
    CHECK(undo::Fits(page, full));
    undo::Append(page, 0, full, 1, 2);
    CHECK(!undo::Fits(page, full));
    const undo::Ref ref{key, 0, 1, 2};
    CHECK(undo::Read(page, ref, rid).old.value.size() == undo::kMaxValueBytes);
    undo::Seal(page, 2, 3);
    CHECK(undo::Read(page, ref, rid).old.value == full.old.value);
    undo::Redo original(undo::Operation::Append, key, txn, 1, 0, full);
    original.lsn_ = 2;
    auto bytes = Encode(original);
    undo::Redo copy;
    copy.Decode(bytes.data(), bytes.size());
    copy.Apply(page, true);
    Death([&] {
        auto bad = ref;
        bad.generation = 2;
        undo::Read(page, bad, rid);
    });
    Death([&] {
        auto bad = ref;
        bad.page.generation = 2;
        undo::Read(page, bad, rid);
    });
    Death([&] {
        auto bad = ref;
        bad.page.kind = undo::PageKind(2);
        undo::Read(page, bad, rid);
    });
    Death([&] {
        auto bad = rid;
        ++bad.key;
        undo::Read(page, ref, bad);
    });
    Death([&] {
        auto bad = ref;
        bad.slot = 1;
        undo::Read(page, bad, rid);
    });
    Death([&] {
        auto bad = ref;
        bad.lsn = 1;
        undo::Read(page, bad, rid);
    });
    Death([&] { undo::EncodeRow(Old(undo::kMaxValueBytes + 1)); });
    Death([&] { undo::Append(page, 1, full, 3, 4); });
    Death([&] {
        auto wrong = original;
        wrong.record.old.value[0] = 'z';
        wrong.Apply(page, true);
    });
    Death([&] {
        auto bad = bytes;
        bad.back() ^= 1;
        copy.Decode(bad.data(), bad.size());
    });
    Death([&] { copy.Decode(bytes.data(), bytes.size() - 1); });
    Death([&] {
        auto bad = page;
        bad[undo::kPageHeaderBytes + 4] = 0;
        const auto checksum = undo::Checksum({bad.data(), PAGE_SIZE - 8});
        for (size_t i = 0; i < 8; ++i) bad[PAGE_SIZE - 8 + i] = char(checksum >> (8 * i));
        undo::Inspect(bad);
    });
    Death([&] {
        auto bad = page;
        bad[undo::kPageHeaderBytes] = char(0xff);
        bad[undo::kPageHeaderBytes + 1] = char(0xff);
        const auto checksum = undo::Checksum({bad.data(), PAGE_SIZE - 8});
        for (size_t i = 0; i < 8; ++i) bad[PAGE_SIZE - 8 + i] = char(checksum >> (8 * i));
        undo::Inspect(bad);
    });
    Death([&] { undo::BufferSize(JsonConfig::load("{\"undo_buffer_pool_size_pages\":0}", "test")); });
    Death([&] {
        auto invalid = Old();
        invalid.id.page = 0;
        undo::EncodeRow(invalid);
    });
    std::cout << "PASS format: full 3750-byte value, all workload values, identity/bounds/completion; "
                 "death checks\n";
}
struct Scratch {
    std::filesystem::path previous = std::filesystem::current_path();
    std::string path;
    Scratch() {
        char name[] = "/tmp/undo-p2-test-XXXXXX";
        auto p = mkdtemp(name);
        CHECK(p);
        path = p;
        std::filesystem::current_path(path);
    }
    ~Scratch() {
        std::filesystem::current_path(previous);
        std::filesystem::remove_all(path);
    }
};
void CheckMainLayout() {
    Scratch scratch;
    DiskManager disk;
    StorageBufferPoolManager buffer(32, &disk);
    RmManager manager(&disk, &buffer);
    for (const size_t size :
         {sizeof(ycsb_user_table_val), sizeof(smallbank_savings_val_t), sizeof(tpcc_customer_val_t)}) {
        const std::string table = "layout_" + std::to_string(size);
        manager.create_file(table, sizeof(DataItem) + size);
        auto file = manager.open_file(table);
        const auto header = file->get_file_hdr();
        CHECK(header.format_ == 0x524d5632 && header.num_records_per_page_ > 0);
        CHECK(sizeof(RmPageHdr) + header.bitmap_size_ +
                  header.num_records_per_page_ * (header.record_size_ + sizeof(itemkey_t)) <=
              PAGE_SIZE);
        DataItem item(0, size);
        memset(item.value, 'L', size);
        std::string bytes(item.GetSerializeSize(), '\0');
        item.Serialize(bytes.data());
        CHECK(*reinterpret_cast<uint8_t**>(bytes.data() + offsetof(DataItem, value)) == nullptr);
        Rid last{};
        for (int n = 0; n <= header.num_records_per_page_; ++n)
            last = file->insert_record(100 + n, bytes.data(), nullptr);
        CHECK(last.page_no_ == 2 && last.slot_no_ == 0);
        manager.close_file(file.get());
        const int fd = disk.open_file(table);
        undo::PageBytes page{};
        disk.read_page(fd, 1, page.data(), PAGE_SIZE);
        const undo::RowId id{0, 1, uint32_t(header.num_records_per_page_ - 1),
                             uint64_t(100 + header.num_records_per_page_ - 1)};
        auto row = mvcc::CopyRow(page.data(), header, id);
        CHECK(row.version.initial && row.previous.empty());
        CHECK(row.value == std::string(size, 'L'));
        auto* raw = mvcc::RowAt(page.data(), header, id);
        raw->lock = EXCLUSIVE_LOCKED;
        raw->timeStamp = 99;
        CHECK(undo::EncodeRow(mvcc::CopyRow(page.data(), header, id)) == undo::EncodeRow(row));
        row.version = {txn, false, true};
        row.previous = {key, 0, 1, 2};
        mvcc::InstallRow(*raw, row);
        CHECK(raw->lock == EXCLUSIVE_LOCKED && raw->timeStamp == 99);
        CHECK(undo::EncodeRow(mvcc::CopyRow(page.data(), header, id)) == undo::EncodeRow(row));
        DataItem copied;
        mvcc::CopyToItem(copied, row);
        CHECK(!copied.valid && copied.mvcc.creator_local == txn.local);
        Death([&] {
            auto bad = id;
            ++bad.key;
            mvcc::CopyRow(page.data(), header, bad);
        });
        Death([&] {
            auto bad = header;
            bad.format_ = 0;
            mvcc::CopyRow(page.data(), bad, id);
        });
        Death([&] {
            raw->mvcc.format = 0;
            mvcc::CopyRow(page.data(), header, id);
        });
        disk.close_file(fd);
    }
    std::cout << "PASS MVC2 layout: real loader page boundary, max workload row, copy/serialize, creator "
                 "distinct from owner, 9 death checks\n";
}
class Server {
public:
    brpc::Server server;
    explicit Server(google::protobuf::Service& service) {
        int rc = server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE);
        CHECK(rc == 0);
        rc = server.Start("127.0.0.1:0", nullptr);
        CHECK(rc == 0);
    }
    ~Server() {
        server.Stop(0);
        server.Join();
    }
    std::string Address() { return "127.0.0.1:" + std::to_string(server.listen_address().port); }
};
void CheckReferenceReplay(bool main_first = false, bool legacy = false) {
    Scratch scratch;
    if (legacy) {
        for (int writer : {0, 1}) {
            const int fd = open(LogReplay::LogFilePath(writer).c_str(), O_CREAT | O_EXCL | O_RDWR, 0644);
            CHECK(fd >= 0);
            close(fd);
        }
    }
    DiskManager disk;
    const std::string table = "undo_ref_test";
    disk.create_file(table);
    const int fd = disk.open_file(table);
    const auto initial = Old(4);
    RmFileHdr file{};
    file.record_size_ = sizeof(DataItem) + initial.value.size();
    file.num_pages_ = 2;
    file.num_records_per_page_ = 1;
    file.bitmap_size_ = 1;
    undo::PageBytes page0{}, page1{};
    memcpy(page0.data() + OFFSET_FILE_HDR, &file, sizeof(file));
    RmPageHdr hdr{};
    hdr.num_records_ = 1;
    memcpy(page1.data(), &hdr, sizeof(hdr));
    const size_t offset = sizeof(RmPageHdr) + 1;
    Bitmap::set(page1.data() + sizeof(RmPageHdr), 0);
    memcpy(page1.data() + offset, &rid.key, sizeof(rid.key));
    DataItem item(0, initial.value.size());
    memcpy(item.value, initial.value.data(), initial.value.size());
    item.Serialize(page1.data() + offset + sizeof(rid.key));
    CHECK(undo::EncodeRow(mvcc::CopyRow(page1.data(), file, rid)) == undo::EncodeRow(initial));
    disk.write_page(fd, 0, page0.data(), PAGE_SIZE);
    disk.write_page(fd, 1, page1.data(), PAGE_SIZE);
    undo::Redo init(undo::Operation::Initialize, key, txn, 0);
    init.lsn_ = 1;
    undo::Record record{1, {}, initial};
    undo::Redo append(undo::Operation::Append, key, txn, 1, 0, record);
    append.lsn_ = 2;
    undo::Ref ref{key, 0, 1, 2};
    auto current = initial;
    current.previous = ref;
    current.version = {txn, false, false};
    current.value = "new!";
    auto next = page1;
    mvcc::InstallRow(*mvcc::RowAt(next.data(), file, rid), current);
    RmRecord rm(rid.key, file.record_size_, next.data() + offset + sizeof(rid.key));
    UpdateLogRecord update(0, 0, txn.local, rm, Rid{1, 0}, table);
    update.history = UpdateHistory::Install;
    update.lsn_ = 3;
    update.prev_lsn_ = 0;
    if (main_first) {
        {
            LogManager durable(&disk, nullptr);
            durable.write_batch_log_to_disk(Encode(update) + Encode(init) + Encode(append));
        }
        LogReplay replay(&disk, nullptr, "ycsb"); // invalid same-stream order must fail
        return;
    }
    undo::PageBytes applied{};
    {
        LogReplay replay(&disk, nullptr, "ycsb");
        LogManager durable(&disk, nullptr);
        LogManager reads(&disk, &replay);
        storage_service::StoragePoolImpl storage(&reads, &disk, nullptr, nullptr, 0, nullptr);
        Server server(storage);
        const auto main_bytes = Encode(update) + Encode(update), undo_bytes = Encode(init) + Encode(append);
        durable.write_batch_log_to_disk(undo_bytes);
        durable.write_batch_log_to_disk(main_bytes);
        CHECK(durable.get_fdatasync_count() == 2);
        replay.RegisterStream(0, 0, !legacy);
        const size_t header_bytes = legacy ? 0 : wal::kFrameHeaderSize;
        const auto undo_end = header_bytes + undo_bytes.size();
        const auto main_end = undo_end + header_bytes + main_bytes.size();
        // WAL is durable, but replay has not been allowed to consume it yet.
        CHECK(replay.PendingRecords() == 0);
        disk.read_page(fd, 1, applied.data(), PAGE_SIZE);
        CHECK(applied == page1);
        auto reader = std::async(std::launch::async, [&] {
            brpc::Channel channel;
            brpc::ChannelOptions options;
            options.timeout_ms = 10000;
            const int rc = channel.Init(server.Address().c_str(), &options);
            CHECK(rc == 0);
            storage_service::StorageService_Stub stub(&channel);
            storage_service::GetUndoPageRequest req;
            req.set_key(undo::EncodeKey(key));
            req.set_require_lsn(ref.lsn);
            storage_service::GetUndoPageResponse out;
            brpc::Controller ctl;
            stub.GetUndoPage(&ctl, &req, &out, nullptr);
            CHECK(!ctl.Failed());
            CHECK(out.data().size() == PAGE_SIZE);
            undo::PageBytes result;
            memcpy(result.data(), out.data().data(), PAGE_SIZE);
            return undo::Read(result, ref, rid).old.value;
        });
        const bool waiting = replay.UndoStore().WaitForReadWaits(1, 5s);
        CHECK(waiting);
        CHECK(reader.wait_for(0s) == std::future_status::timeout);
        replay.PublishDurable(0, undo_end, undo_bytes.size());
        CHECK(reader.get() == initial.value);
        replay.Drain();
        disk.read_page(fd, 1, applied.data(), PAGE_SIZE);
        CHECK(applied == page1); // replaying undo alone must not install the main row
        replay.PublishDurable(0, main_end, main_bytes.size());
        replay.Drain();
        disk.read_page(fd, 1, applied.data(), PAGE_SIZE);
        CHECK(undo::EncodeRow(mvcc::CopyRow(applied.data(), file, rid)) == undo::EncodeRow(current));
        auto orphan = append;
        orphan.slot = 1;
        orphan.prev_lsn_ = 2;
        orphan.lsn_ = 4;
        orphan.record.sequence = 2;
        orphan.record.previous_txn = ref;
        const auto encoded = Encode(orphan);
        durable.write_batch_log_to_disk(encoded);
        replay.PublishDurable(0, main_end + header_bytes + encoded.size(), encoded.size());
        replay.Drain();
        undo::PageBytes after{};
        disk.read_page(fd, 1, after.data(), PAGE_SIZE);
        CHECK(after == applied);

        // A different home with the same local tx number clears a lock or
        // restores the old row. Its UPDATE retains writer 0's existing head.
        UpdateLogRecord retained(0, 1, txn.local, rm, Rid{1, 0}, table);
        retained.history = UpdateHistory::Retain;
        retained.prev_lsn_ = update.lsn_;
        retained.lsn_ = 5;
        reads.write_batch_log_to_disk(Encode(retained));
        replay.Drain();
        disk.read_page(fd, 1, applied.data(), PAGE_SIZE);
        CHECK(reinterpret_cast<const RmPageHdr*>(applied.data())->LLSN_ == retained.lsn_);
        CHECK(undo::EncodeRow(mvcc::CopyRow(applied.data(), file, rid)) == undo::EncodeRow(current));
    }
    disk.write_page(fd, 1, page1.data(), PAGE_SIZE);
    std::filesystem::remove(undo::SpaceFile(key));
    for (int pass = 0; pass < 2; ++pass) {
        LogReplay replay(&disk, nullptr, "ycsb");
        undo::PageBytes actual{};
        disk.read_page(fd, 1, actual.data(), PAGE_SIZE);
        CHECK(actual == applied);
        CHECK(undo::Read(replay.UndoStore().ReadPage(key, ref.lsn), ref, rid).old.value == initial.value);
        replay.Drain();
    }
    disk.close_file(fd);
    std::cout << "PASS MVC2 main redo: ordered WAL, storage RPC replay wait, orphan unchanged, rebuild "
                 "and duplicate replay\n";
}
void CheckPinnedFrameWait(bool drop) {
    BufferPool pool(1, 2);
    undo::PageBytes bytes{};
    frame_id_t pinned = INVALID_FRAME_ID;
    CHECK(pool.checkIfDirectlyPutInBuffer(1, pinned));
    pool.insert_or_replace(0, 1, pinned, false, INVALID_PAGE_ID, bytes.data());
    std::promise<void> started;
    auto pending = std::async(std::launch::async, [&] {
        frame_id_t frame = INVALID_FRAME_ID;
        started.set_value();
        const auto victim = pool.replace_page(2, frame, [](page_id_t) { return true; });
        CHECK(frame == pinned);
        return victim.first;
    });
    started.get_future().wait();
    CHECK(pending.wait_for(10ms) == std::future_status::timeout);
    if (drop) pool.release_page(0, 1);
    else pool.unpin_page(1);
    CHECK(pending.wait_for(5s) == std::future_status::ready);
    CHECK(pending.get() == (drop ? INVALID_PAGE_ID : 1));
}

void CheckLastUndoPage() {
    struct Pages final : undo::PageAccess {
        BufferPool pool{1, 1};
        Page* FetchUndoPage(const undo::PageKey& key, bool, bool create) override {
            const auto number = undo::PageNumber(key);
            if (!create) return pool.fetch_page(number);
            undo::PageBytes zero{};
            frame_id_t frame = INVALID_FRAME_ID;
            CHECK(pool.checkIfDirectlyPutInBuffer(number, frame));
            return pool.insert_or_replace(UNDO_TABLE_ID, number, frame, false, INVALID_PAGE_ID, zero.data());
        }
        void ReleaseUndoPage(const undo::PageKey& key, bool) override { pool.unpin_page(undo::PageNumber(key)); }
        void ClearUndoCache() override {}
    } pages;
    uint64_t lsn = 0;
    const auto last = std::numeric_limits<page_id_t>::max();
    const auto writer = undo::PageIdentity(epoch, last).writer;
    undo::Manager manager(epoch, writer, {[&](std::unique_ptr<LogRecord>) { return ++lsn; }}, pages);
    undo::ManagerAddressLimitTest::LeaveOnePage(manager);
    const mvcc::TxnId other{epoch, 1, 2};
    manager.Begin({txn, 0});
    manager.Begin({other, 0});
    const undo::Manager::WriteSlot first{0, 1, 0}, second{0, 1, 1};
    CHECK(manager.AdmitNewWrite(txn, first));
    CHECK(manager.AdmitNewWrite(txn, first));
    CHECK(!manager.AdmitNewWrite(txn, second));
    CHECK(!manager.AdmitNewWrite(other));
    manager.Finish(txn);
    CHECK(manager.AdmitNewWrite(other));
    const auto old = Old(undo::kMaxValueBytes);
    const auto ref = manager.Append(other, old);
    CHECK(!ref.empty() && undo::PageNumber(ref.page) == last);
    CHECK(manager.Append(other, old).empty());
    CHECK(manager.Tail(other) == ref);
    manager.Finish(other);
    CHECK(manager.ReadRollback(ref, other, 1).old.value == old.value);
}

void CheckOutOfOrderReplay() {
    Scratch scratch;
    DiskManager disk;
    undo::Store store(disk);
    auto init = std::make_shared<undo::Redo>(undo::Operation::Initialize, key, txn, 0);
    init->lsn_ = 10;
    undo::Record rec{1, {}, Old()};
    auto append = std::make_shared<undo::Redo>(undo::Operation::Append, key, txn, 10, 0, rec);
    append->lsn_ = 20;
    auto seal = std::make_shared<undo::Redo>(undo::Operation::Seal, key, txn, 20);
    seal->lsn_ = 30;
    CHECK(store.Submit(seal) == 0);
    CHECK(store.Submit(append) == 0);
    CHECK(store.Submit(append) == 0);
    CHECK(store.Submit(init) == 4);
    CHECK(store.Submit(append) == 1 && store.Submit(init) == 1 && store.Submit(seal) == 1);
    const undo::Ref ref{key, 0, 1, 20};
    CHECK(undo::Read(store.ReadPage(key, 30), ref, rid).old.value == rec.old.value);
    store.Sync();
    std::cout << "PASS explicit reverse replay events, queued duplicates and immutable record checks\n";
}
}  // namespace
int main() {
    EnableFatalChecks();
    std::cout << std::unitbuf;
    CheckFormats();
    CheckMainLayout();
    Death([] { CheckReferenceReplay(true); });
    std::cout << "PASS main-before-undo WAL violates the replay invariant\n";
    CheckPinnedFrameWait(true);
    CheckPinnedFrameWait(false);
    CheckLastUndoPage();
    CheckOutOfOrderReplay();
    CheckReferenceReplay();
    CheckReferenceReplay(false, true);
    std::cout << "PASS undo P2 storage foundation (not business-row MVCC)\n";
}
