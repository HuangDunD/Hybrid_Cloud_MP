// Local format/state/replay checks only; no cluster or crash recovery test.
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
#include "storage/prepared_log_record.h"
#include "storage/logreplay.h"
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
        int log_fd = open(replay.GetLogFilePath().c_str(), O_RDWR);
        assert(log_fd >= 0);
        size_t end = sizeof(batch_id_t) + sizeof(size_t);
        auto append = [&](const LogRecord& record) {
            auto bytes = Encode(record);
            assert(pwrite(log_fd, bytes.data(), bytes.size(), end) == bytes.size());
            end += bytes.size();
            replay.add_max_replay_off_(bytes.size());
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            size_t progress = 0;
            do {
                assert(pread(log_fd, &progress, sizeof(progress), sizeof(batch_id_t)) == sizeof(progress));
                if (progress == end) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } while (std::chrono::steady_clock::now() < deadline);
            assert(progress == end);
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
        batch_id_t batch_id = 0;
        assert(pread(log_fd, &batch_id, sizeof(batch_id), 0) == sizeof(batch_id));
        assert(batch_id == 44);
        disk.read_page(table_fd, 1, &actual[0], PAGE_SIZE);
        memcpy(&page_hdr, actual.data(), sizeof(page_hdr));
        assert(page_hdr.LLSN_ == 13 && page_hdr.pre_LLSN_ == 12);
        // The original vote bytes remain in the WAL after replay progress moves.
        auto vote = Encode(prepare);
        std::string retained(vote.size(), '\0');
        assert(pread(log_fd, &retained[0], retained.size(), sizeof(batch_id_t) + sizeof(size_t)) == retained.size());
        assert(retained == vote);
        close(log_fd);
    }
    disk.close_file(table_fd);
    std::filesystem::current_path(previous_dir);
    std::filesystem::remove_all(scratch);
    std::cout << "PASS: v" << version << " online PREPARED replay leaves page unchanged and advances to UPDATE/LOCK/BATCHEND\n";
}

int main() {
    CheckConfigAndProtocolSelection();
    for (auto version : {prepared_2pc::kParticipantReplyVersion, prepared_2pc::kProtocolVersion}) {
        CheckFormatAndContext(version);
        CheckOnlineReplay(version);
    }
}
