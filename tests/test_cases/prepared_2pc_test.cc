// Local format/state/replay checks only; no cluster or crash recovery test.
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
#include "storage/prepared_log_record.h"
#include "storage/logreplay.h"
#include "storage/log_manager.h"
#include <sys/wait.h>
#include "compute_node/prepare_commit_config.h"

static twopc_service::PrepareRequest Request(uint32_t version = prepared_2pc::kProtocolVersion) {
    twopc_service::PrepareRequest r;
    r.set_protocol_version(version);
    r.set_transaction_id(42);
    r.mutable_identity()->set_coordinator_node(0);
    r.mutable_identity()->set_coordinator_incarnation(std::string(16, 'a'));
    r.mutable_identity()->set_local_tx_id(42);
    r.set_start_ts(100);
    r.set_commit_ts(200);
    r.add_participants(0);
    r.add_participants(1);
    r.set_participant_node(1);
    auto* w = r.add_writes();
    w->mutable_item_id()->set_table_id(0);
    w->mutable_item_id()->set_page_no(1);
    w->mutable_item_id()->set_slot_id(0);
    w->set_table_name("prepared_test_table");
    w->set_key(7);
    w->set_operation(twopc_service::PreparedWrite::UPDATE);
    w->set_value(std::string("new\0", 4));
    return r;
}

static std::string Encode(const LogRecord& record) {
    std::string bytes(record.log_tot_len_, '\0');
    record.serialize(&bytes[0]);
    return bytes;
}

static void CheckConfigAndProtocolSelection() {
    assert(ReadCornusPrepareCommit(JsonConfig::load("{}", "test")));
    assert(ReadCornusPrepareCommit(JsonConfig::load(R"({"cornus_prepare_commit":1})", "test")));
    assert(!ReadCornusPrepareCommit(JsonConfig::load(R"({"cornus_prepare_commit":0})", "test")));
    for (const char* json : {R"({"cornus_prepare_commit":2})",
                             R"({"cornus_prepare_commit":-1})",
                             R"({"cornus_prepare_commit":"0"})",
                             R"({"cornus_prepare_commit":true})"}) {
        bool rejected = false;
        try { ReadCornusPrepareCommit(JsonConfig::load(json, "test")); }
        catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected);
    }
    twopc_service::PrepareRequest legacy;
    legacy.set_transaction_id(42);
    twopc_service::PrepareRequest wire;
    assert(wire.ParseFromString(legacy.SerializeAsString()));
    assert(prepared_2pc::IsLegacyPrepare(wire));
    assert(!prepared_2pc::ValidRequest(wire));
    assert(!prepared_2pc::IsLegacyPrepare(Request()));
    auto malformed = Request();
    malformed.set_protocol_version(0);
    assert(!prepared_2pc::IsLegacyPrepare(malformed));
    assert(!prepared_2pc::ValidRequest(malformed));
    // Single remote writers and the old ID-only protocol retain LEGACY Commit.
    // Multi-writer baseline v1 now uses the same durable write-set envelope as v2.
    auto baseline = Request(prepared_2pc::kParticipantReplyVersion);
    assert(wire.ParseFromString(baseline.SerializeAsString()));
    assert(prepared_2pc::ValidRequest(wire) && !prepared_2pc::IsLegacyPrepare(wire));
    assert(wire.writes(0).value() == baseline.writes(0).value());
    twopc_service::CommitRequest commit, commit_wire;
    commit.set_transaction_id(42);
    assert(commit_wire.ParseFromString(commit.SerializeAsString()));
    assert(commit_wire.kind() == twopc_service::CommitRequest::LEGACY);
    std::cout << "PASS: switch default/0/1 validation and legacy/prepared wire distinction\n";
}

static void CheckFormatAndContext(uint32_t version) {
    auto r = Request(version);
    PreparedLogRecord original(r);
    original.lsn_ = 11;
    auto bytes = Encode(original);
    PreparedLogRecord decoded;
    assert(decoded.Decode(bytes.data(), bytes.size()));
    assert(decoded.payload() == r.SerializeAsString());
    assert(decoded.lsn_ == 11 && decoded.log_node_id_ == 1);
    for (size_t n = 0; n < bytes.size(); ++n) assert(!decoded.Decode(bytes.data(), n));
    auto changed = bytes;
    changed[LOG_HEADER_SIZE + 5] ^= 1;
    assert(!decoded.Decode(changed.data(), changed.size()));
    changed = bytes;
    changed[OFFSET_LSN] ^= 1;
    assert(!decoded.Decode(changed.data(), changed.size()));
    auto invalid = r;
    invalid.add_participants(1);
    assert(!prepared_2pc::ValidRequest(invalid));
    invalid = r;
    *invalid.add_writes() = r.writes(0);
    assert(!prepared_2pc::ValidRequest(invalid));
    invalid = r;
    invalid.mutable_writes(0)->set_operation(twopc_service::PreparedWrite::UNSUPPORTED);
    assert(!prepared_2pc::ValidRequest(invalid));
    invalid = r;
    invalid.mutable_writes(0)->set_value(std::string(LOG_REPLAY_BUFFER_SIZE, 'x'));
    assert(!PreparedLogRecord::Fits(invalid));
    // Largest admitted logical record fits in one replay buffer.
    while (!PreparedLogRecord::Fits(invalid)) invalid.mutable_writes(0)->mutable_value()->pop_back();
    PreparedLogRecord largest(invalid);
    assert(largest.log_tot_len_ == LOG_REPLAY_BUFFER_SIZE);
    auto large_bytes = Encode(largest);
    assert(decoded.Decode(large_bytes.data(), large_bytes.size()));

    prepared_2pc::Registry registry;
    bool inserted = false;
    auto c = registry.Begin(r, &inserted);
    assert(c && inserted && !registry.IsPrepared(c));
    auto duplicate = registry.Begin(r, &inserted);
    assert(duplicate == c && !inserted);
    auto conflicting = r;
    conflicting.mutable_writes(0)->set_value("different");
    assert(!registry.Begin(conflicting, &inserted));
    registry.MarkPrepared(c, 11);
    assert(registry.IsPrepared(c));
    twopc_service::CommitRequest commit;
    commit.set_kind(twopc_service::CommitRequest::PREPARED);
    *commit.mutable_identity() = r.identity();
    commit.set_transaction_id(r.transaction_id());
    commit.set_start_ts(r.start_ts());
    commit.set_commit_ts(r.commit_ts() + 1);
    assert(!registry.TakeForCommit(commit));
    commit.set_commit_ts(r.commit_ts());
    assert(registry.TakeForCommit(commit) == c);
    assert(!registry.TakeForCommit(commit));
    registry.Erase(c);
    assert(!registry.TakeForCommit(commit));
    auto restarted = r;
    restarted.mutable_identity()->set_coordinator_incarnation(std::string(16, 'b'));
    assert(registry.Begin(restarted, &inserted) && inserted);
    assert(!registry.TakeForCommit(commit));
    std::cout << "PASS: v" << version << " prepared format, capacity, identity and context lifecycle\n";
}

static void CheckOnlineReplay(uint32_t version) {
    auto previous_dir = std::filesystem::current_path();
    char scratch[] = "/tmp/prepared-2pc-test-XXXXXX";
    assert(mkdtemp(scratch));
    std::filesystem::current_path(scratch);
    DiskManager disk;
    const auto request = Request(version);
    const auto table = request.writes(0).table_name();
    disk.create_file(table);
    const int table_fd = disk.open_file(table);
    RmFileHdr file_hdr{};
    file_hdr.record_size_ = sizeof(DataItem) + 4;
    file_hdr.num_pages_ = 2;
    file_hdr.num_records_per_page_ = 1;
    file_hdr.bitmap_size_ = 1;
    std::string page0(PAGE_SIZE, '\0');
    memcpy(&page0[OFFSET_FILE_HDR], &file_hdr, sizeof(file_hdr));
    disk.write_page(table_fd, 0, page0.data(), PAGE_SIZE);
    std::string page1(PAGE_SIZE, '\0');
    RmPageHdr page_hdr{};
    page_hdr.LLSN_ = 10;
    page_hdr.num_records_ = 1;
    memcpy(&page1[0], &page_hdr, sizeof(page_hdr));
    Bitmap::set(&page1[sizeof(RmPageHdr)], 0);
    const int tuple_offset = sizeof(RmPageHdr) + 1;
    const itemkey_t key = 7;
    memcpy(&page1[tuple_offset], &key, sizeof(key));
    DataItem item;
    item.table_id = 0;
    item.value_size = 4;
    item.lock = EXCLUSIVE_LOCKED;
    item.timeStamp = request.start_ts();
    memcpy(&page1[tuple_offset + sizeof(key)], &item, sizeof(item));
    memcpy(&page1[tuple_offset + sizeof(key) + sizeof(item)], "old!", 4);
    disk.write_page(table_fd, 1, page1.data(), PAGE_SIZE);
    {
        LogReplay replay(&disk, nullptr, "ycsb");
        LogManager logs(&disk, &replay);
        auto append = [&](const LogRecord& record) {
            logs.write_batch_log_to_disk(Encode(record));
            assert(replay.WaitUntilReplayed(std::chrono::seconds(5)));
        };
        PreparedLogRecord prepare(request);
        prepare.lsn_ = 11;
        append(prepare);
        std::string actual(PAGE_SIZE, '\0');
        disk.read_page(table_fd, 1, &actual[0], PAGE_SIZE);
        assert(actual == page1); // includes bitmap, lock bytes and page LSN

        // Subsequent UPDATE and LOCK still follow the original page LSN chain.
        std::string new_item(sizeof(DataItem) + 4, '\0');
        item.lock = UNLOCKED;
        item.commitTimeStamp = request.commit_ts();
        memcpy(&new_item[0], &item, sizeof(item));
        memcpy(&new_item[sizeof(item)], request.writes(0).value().data(), 4);
        RmRecord value(key, new_item.size(), &new_item[0]);
        UpdateLogRecord update(0, 1, 42, value, Rid{1, 0}, table);
        update.lsn_ = 12;
        update.prev_lsn_ = 10; // PREPARED did not advance the page to 11
        append(update);
        disk.read_page(table_fd, 1, &actual[0], PAGE_SIZE);
        assert(memcmp(actual.data() + tuple_offset + sizeof(key) + sizeof(item), "new\0", 4) == 0);
        memcpy(&page_hdr, actual.data(), sizeof(page_hdr));
        assert(page_hdr.LLSN_ == 12 && page_hdr.pre_LLSN_ == 10);
        LockLogRecord lock(0, 1, 43, 0, table, Rid{1, 0}, EXCLUSIVE_LOCKED, 300, 0, 1);
        lock.lsn_ = 13;
        lock.prev_lsn_ = 12;
        append(lock);
        BatchEndLogRecord batch(44, 1, 42);
        batch.lsn_ = 14;
        append(batch);
        disk.read_page(table_fd, 1, &actual[0], PAGE_SIZE);
        memcpy(&page_hdr, actual.data(), sizeof(page_hdr));
        assert(page_hdr.LLSN_ == 13 && page_hdr.pre_LLSN_ == 12);
        // The original vote bytes remain in the WAL after replay progress moves.
        auto vote = Encode(prepare);
        std::string retained(vote.size(), '\0');
        const int log_fd = open(LogReplay::LogFilePath(1).c_str(), O_RDONLY);
        assert(log_fd >= 0);
        assert(pread(log_fd, &retained[0], retained.size(), 0) == retained.size());
        assert(retained == vote);
        close(log_fd);
    }
    disk.close_file(table_fd);
    std::filesystem::current_path(previous_dir);
    std::filesystem::remove_all(scratch);
    std::cout << "PASS: v" << version << " online PREPARED replay leaves page unchanged and advances to UPDATE/LOCK/BATCHEND\n";
}

static void CheckMultiStreamReplay() {
    const auto old_dir = std::filesystem::current_path();
    char scratch[] = "/tmp/multi-stream-replay-XXXXXX";
    assert(mkdtemp(scratch));
    std::filesystem::current_path(scratch);
    DiskManager disk;
    RmFileHdr fh{};
    fh.record_size_ = sizeof(DataItem) + sizeof(int);
    fh.num_pages_ = 2; fh.num_records_per_page_ = 1; fh.bitmap_size_ = 1;
    std::string header(PAGE_SIZE, '\0');
    memcpy(&header[OFFSET_FILE_HDR], &fh, sizeof(fh));
    std::string initial(PAGE_SIZE, '\0');
    RmPageHdr ph{}; ph.num_records_ = 1;
    memcpy(&initial[0], &ph, sizeof(ph));
    Bitmap::set(&initial[sizeof(RmPageHdr)], 0);
    const int tuple = sizeof(RmPageHdr) + 1;
    const itemkey_t key = 7;
    memcpy(&initial[tuple], &key, sizeof(key));
    DataItem item; item.value_size = sizeof(int); item.lock = UNLOCKED;
    memcpy(&initial[tuple + sizeof(key)], &item, sizeof(item));
    int fds[2];
    for (int i = 0; i < 2; ++i) {
        const std::string name = "replay_table_" + std::to_string(i);
        disk.create_file(name); fds[i] = disk.open_file(name);
        disk.write_page(fds[i], 0, header.data(), PAGE_SIZE);
        disk.write_page(fds[i], 1, initial.data(), PAGE_SIZE);
    }
    auto update = [&](int node, int table, LLSN lsn, LLSN prev, int value) {
        std::string bytes(sizeof(DataItem) + sizeof(value), '\0');
        item.table_id = table;
        memcpy(&bytes[0], &item, sizeof(item));
        memcpy(&bytes[sizeof(item)], &value, sizeof(value));
        RmRecord image(key, bytes.size(), &bytes[0]);
        UpdateLogRecord r(0, node, lsn, image, Rid{1, 0}, "replay_table_" + std::to_string(table));
        r.lsn_ = lsn; r.prev_lsn_ = prev;
        return Encode(r);
    };
    auto page = [&](int table) {
        std::string bytes(PAGE_SIZE, '\0');
        disk.read_page(fds[table], 1, &bytes[0], PAGE_SIZE);
        return bytes;
    };
    auto check = [&](int table, LLSN lsn, int value) {
        auto bytes = page(table); RmPageHdr actual{}; int v;
        memcpy(&actual, bytes.data(), sizeof(actual));
        memcpy(&v, bytes.data() + tuple + sizeof(key) + sizeof(item), sizeof(v));
        assert(actual.LLSN_ == lsn && v == value);
    };
    std::string final_pages[2];
    {
        LogReplay replay(&disk, nullptr, "ycsb");
        LogManager logs(&disk, &replay);
        logs.write_batch_log_to_disk(update(1, 0, 20, 10, 20));
        assert(!replay.WaitUntilReplayed(std::chrono::milliseconds(30)));
        assert(replay.PendingRecords() == 1); check(0, 0, 0);
        logs.write_batch_log_to_disk(update(2, 1, 20, 0, 120));
        // Same page number in a different table must not share its dependency.
        for (int i = 0; i < 100 && replay.RecordsReplayed() == 0; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        check(1, 20, 120); check(0, 0, 0);
        logs.write_batch_log_to_disk(update(0, 0, 10, 0, 10));
        assert(replay.WaitUntilReplayed(std::chrono::seconds(5))); check(0, 20, 20);
        logs.write_batch_log_to_disk(update(1, 0, 50, 40, 50));
        logs.write_batch_log_to_disk(update(0, 0, 40, 30, 40));
        assert(!replay.WaitUntilReplayed(std::chrono::milliseconds(30)));
        logs.write_batch_log_to_disk(update(2, 0, 30, 20, 30));
        assert(replay.WaitUntilReplayed(std::chrono::seconds(5))); check(0, 50, 50);
        // INSERT/DELETE/LOCK participate in the same cross-file page chain.
        std::string insert_bytes(sizeof(DataItem) + sizeof(int), '\0');
        item.table_id = 0; item.lock = UNLOCKED; const int inserted_value = 80;
        memcpy(&insert_bytes[0], &item, sizeof(item));
        memcpy(&insert_bytes[sizeof(item)], &inserted_value, sizeof(inserted_value));
        RmRecord inserted(key, insert_bytes.size(), &insert_bytes[0]);
        InsertLogRecord ins(0, 1, 80, inserted, 1, 0, "replay_table_0");
        ins.lsn_ = 80; ins.prev_lsn_ = 70;
        DeleteLogRecord del(0, 0, 70, 0, "replay_table_0", 1, 0);
        del.lsn_ = 70; del.prev_lsn_ = 60;
        LockLogRecord lock(0, 2, 60, 0, "replay_table_0", Rid{1, 0}, EXCLUSIVE_LOCKED, 60, 2, 2);
        lock.lsn_ = 60; lock.prev_lsn_ = 50;
        logs.write_batch_log_to_disk(Encode(ins));
        logs.write_batch_log_to_disk(Encode(del));
        logs.write_batch_log_to_disk(Encode(lock));
        assert(replay.WaitUntilReplayed(std::chrono::seconds(5))); check(0, 80, 80);
        auto inserted_page = page(0); RmPageHdr inserted_header{};
        memcpy(&inserted_header, inserted_page.data(), sizeof(inserted_header));
        assert(inserted_header.num_records_ == 1 && Bitmap::is_set(inserted_page.data() + sizeof(RmPageHdr), 0));
        // Cross the replay read-buffer boundary using actual serialized records.
        std::string batch;
        for (int i = 0; i < 2000; ++i) {
            BatchEndLogRecord r(i, 0, i); r.lsn_ = 100 + i; batch += Encode(r);
        }
        logs.write_batch_log_to_disk(batch);
        assert(replay.WaitUntilReplayed(std::chrono::seconds(5)));
        assert(replay.RecordsRead() == 2009 && replay.RecordsReplayed() == 2009);
        assert(replay.DurableBytes() == replay.ReadBytes());
        assert(logs.get_logwrite_count() == logs.get_fdatasync_count());
        replay.Drain();
        final_pages[0] = page(0); final_pages[1] = page(1);
    }
    // Rebuild data from its original image using all three files, then repeat
    // restart without resetting pages to verify idempotence byte-for-byte.
    for (int i = 0; i < 2; ++i) disk.write_page(fds[i], 1, initial.data(), PAGE_SIZE);
    for (int repeat = 0; repeat < 2; ++repeat) {
        LogReplay replay(&disk, nullptr, "ycsb");
        assert(replay.WaitUntilReplayed(std::chrono::seconds(5)));
        assert(page(0) == final_pages[0] && page(1) == final_pages[1]);
        assert(replay.RecordsRead() == 2009 && replay.PendingRecords() == 0);
    }
    // Partial final record is discarded on restart, complete records retained.
    const std::string path = LogReplay::LogFilePath(2);
    const auto size = std::filesystem::file_size(path);
    int fd = open(path.c_str(), O_WRONLY | O_APPEND);
    assert(write(fd, "partial", 7) == 7); close(fd);
    { LogReplay replay(&disk, nullptr, "ycsb"); assert(replay.WaitUntilReplayed(std::chrono::seconds(5))); }
    assert(std::filesystem::file_size(path) == size);
    // A complete record with a missing predecessor must fail recovery, not be
    // skipped or reported as caught up. Run the real replayer in a child.
    const auto broken = update(3, 0, 90, 89, 90);
    int broken_fd = open(LogReplay::LogFilePath(3).c_str(), O_CREAT | O_WRONLY, 0644);
    assert(write(broken_fd, broken.data(), broken.size()) == broken.size()); close(broken_fd);
    const pid_t child = fork(); assert(child >= 0);
    if (child == 0) { LogReplay replay(&disk, nullptr, "ycsb"); _exit(0); }
    int status; assert(waitpid(child, &status, 0) == child);
    assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
    std::filesystem::remove(LogReplay::LogFilePath(3));
    for (int i = 0; i < 2; ++i) disk.close_file(fds[i]);
    std::filesystem::current_path(old_dir);
    std::filesystem::remove_all(scratch);
    std::cout << "PASS: independent WAL streams, page-chain reorder, table isolation, buffer boundary, full replay, restart and partial tail\n";
}

int main() {
    CheckConfigAndProtocolSelection();
    CheckMultiStreamReplay();
    for (auto version : {prepared_2pc::kParticipantReplyVersion, prepared_2pc::kProtocolVersion}) {
        CheckFormatAndContext(version);
        CheckOnlineReplay(version);
    }
}
