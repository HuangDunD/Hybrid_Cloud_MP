#include <array>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include "storage/storage_rpc.h"
#include "core/recovery/observation.h"

namespace obs = recovery_observation;
void Check(bool ok, const char* text) { if (!ok) throw std::runtime_error(text); }
std::string Serialize(const LogRecord& log) {
    std::string bytes(log.log_tot_len_, '\0'); log.serialize(bytes.data()); return bytes;
}
std::string Value(itemkey_t key, bool locked = false) {
    DataItem item(0); item.value_size = sizeof(uint64_t); item.lock = locked ? EXCLUSIVE_LOCKED : 0;
    std::string bytes(sizeof(DataItem) + sizeof(uint64_t), '\0');
    std::memcpy(bytes.data(), &item, sizeof(item));
    uint64_t value = key * 7;
    std::memcpy(bytes.data() + sizeof(item), &value, sizeof(value));
    return bytes;
}
int main(int argc, char** argv) {
    try {
        if (argc != 2 || !std::filesystem::create_directory(argv[1])) {
            std::cerr << "provide a NEW isolated runtime directory\n"; return 2;
        }
        std::filesystem::current_path(argv[1]);
        DiskManager disk;
        StorageBufferPoolManager buffer(32, &disk);
        RmManager rm(&disk, &buffer);
        SmManager sm(&rm, &buffer);
        disk.create_file("fixture");
        int heap = disk.open_file("fixture");
        RmFileHdr file{};
        file.record_size_ = sizeof(DataItem) + sizeof(uint64_t);
        file.num_pages_ = 2; file.num_records_per_page_ = 8; file.bitmap_size_ = 1;
        file.first_free_page_no_ = 1;
        std::array<char, PAGE_SIZE> head{}, data{};
        std::memcpy(head.data() + sizeof(RmPageHdr), &file, sizeof(file));
        auto* page_hdr = reinterpret_cast<RmPageHdr*>(data.data());
        page_hdr->num_records_ = 1;
        Bitmap::set(data.data() + sizeof(RmPageHdr), 0);
        itemkey_t old_key = 7;
        char* first = data.data() + sizeof(RmPageHdr) + 1;
        std::memcpy(first, &old_key, sizeof(old_key));
        auto old_value = Value(old_key, true);
        std::memcpy(first + sizeof(old_key), old_value.data(), old_value.size());
        disk.write_page(heap, 0, head.data(), PAGE_SIZE);
        disk.write_page(heap, 1, data.data(), PAGE_SIZE);
        disk.set_fd2pageno(heap, 2);
        disk.create_file("fixture_bl");
        S_BLinkIndexHandle initial(&disk, &buffer, std::string("fixture"));
        initial.insert_entry(&old_key, Rid{1, 0});
        initial.write_file_hdr_to_page();
        buffer.flush_all_pages();
        buffer.clear_all_pages();
        int initial_pages = disk.get_fd2pageno(initial.getFD());
        LogReplay replay(&disk, &sm, "test");
        LogManager logs(&disk, &replay);
        storage_service::StoragePoolImpl service(&logs, &disk, &rm, nullptr, 0, &sm);
        itemkey_t committed_key = 42, aborted_key = 99;
        auto committed_value = Value(committed_key), aborted_value = Value(aborted_key);
        RmRecord committed(committed_key, committed_value.size(), committed_value.data());
        RmRecord aborted(aborted_key, aborted_value.size(), aborted_value.data());
        InsertLogRecord insert(1, 0, 1, committed, 1, 1, "fixture"); insert.lsn_ = 1; insert.prev_lsn_ = 0;
        BLinkInsertLogRecord index_insert(1, 0, 1, 10000, "fixture_bl", committed_key, Rid{1, 1});
        DeleteLogRecord erase(1, 0, 1, 0, "fixture", 1, 0); erase.lsn_ = 2; erase.prev_lsn_ = 1;
        BLinkDeleteLogRecord index_delete(1, 0, 1, 10000, "fixture_bl", old_key, Rid{1, 0});
        BatchEndLogRecord commit(1, 0, 1);
        InsertLogRecord uncommitted(2, 0, 2, aborted, 1, 2, "fixture"); uncommitted.lsn_ = 3; uncommitted.prev_lsn_ = 2;
        BLinkInsertLogRecord uncommitted_index(2, 0, 2, 10000, "fixture_bl", aborted_key, Rid{1, 2});
        replay.PauseReplay();
        logs.write_batch_log_to_disk(Serialize(insert) + Serialize(index_insert) + Serialize(erase) + Serialize(index_delete) + Serialize(commit) + Serialize(uncommitted) + Serialize(uncommitted_index));
        replay.ObserveRecoveryBacklog("fixture_A_failure_before_replay");
        Check(!replay.WaitReplayCaughtUp(2), "pause did not prevent physical application");
        replay.ResumeReplay();
        Check(replay.WaitReplayCaughtUp(2000), "valid WAL did not catch up");
        replay.ObserveRecoveryBacklog("fixture_after_replay");
        Check(disk.get_fd2pageno(initial.getFD()) == initial_pages, "fixture split unexpectedly");
        auto* index = sm.GetOrCreateBLinkHandle("fixture_bl");
        Rid rid{};
        Check(index->search(&committed_key, rid) && rid == Rid{1, 1}, "committed index insert absent");
        Check(!index->search(&old_key, rid), "committed index delete resurrected");
        Check(index->search(&aborted_key, rid), "fixture uncommitted index not materialized");
        storage_service::AnalyzeRecoveryPagesRequest request;
        request.set_failed_node_id(0);
        auto* heap_page = request.add_pages(); heap_page->set_table_id(0); heap_page->set_table_name("fixture"); heap_page->set_page_no(1); heap_page->set_gplm_lsn(3);
        auto* index_page = request.add_pages(); index_page->set_table_id(10000); index_page->set_table_name("fixture_bl"); index_page->set_page_no(BL_INIT_ROOT_PAGE_ID); index_page->set_gplm_lsn(0);
        storage_service::AnalyzeRecoveryPagesResponse response;
        brpc::Controller controller;
        service.AnalyzeRecoveryPages(&controller, &request, &response, nullptr);
        Check(!controller.Failed() && response.results_size() == 2, "recovery RPC failed");
        Check(response.results(0).status() == 0 && response.results(1).status() == 0, "background catchup incorrectly labelled targeted redo");
        Check(!index->search(&aborted_key, rid), "uncommitted BLink entry survived Undo");
        disk.read_page(heap, 1, data.data(), PAGE_SIZE);
        const char* bitmap = data.data() + sizeof(RmPageHdr);
        Check(!Bitmap::is_set(bitmap, 0) && Bitmap::is_set(bitmap, 1) && !Bitmap::is_set(bitmap, 2), "actual heap bitmap incorrect after Undo");
        size_t visible = 0;
        for (int slot = 0; slot < 8; ++slot) {
            if (!Bitmap::is_set(bitmap, slot)) continue;
            ++visible;
            const char* tuple = bitmap + 1 + slot * (sizeof(itemkey_t) + file.record_size_);
            itemkey_t key; std::memcpy(&key, tuple, sizeof(key));
            auto* item = reinterpret_cast<const DataItem*>(tuple + sizeof(key));
            uint64_t value; std::memcpy(&value, tuple + sizeof(key) + sizeof(DataItem), sizeof(value));
            Check(key == committed_key && item->valid && item->lock == 0 && value == key * 7, "heap key/value/valid incorrect");
            Check(index->search(&key, rid) && rid == Rid{1, slot}, "heap to actual BLink mismatch");
        }
        Check(visible == 1 && reinterpret_cast<RmPageHdr*>(data.data())->num_records_ == visible, "heap count does not match bitmap");
        auto* actual_leaf = index->fetch_node(BL_INIT_ROOT_PAGE_ID, BPOperation::SEARCH_OPERA);
        Check(actual_leaf->is_leaf() && actual_leaf->get_size() == 1 && *actual_leaf->get_key(0) == committed_key && *actual_leaf->get_rid(0) == Rid{1, 1}, "actual BLink key/RID set differs from heap");
        index->release_node(BL_INIT_ROOT_PAGE_ID, BPOperation::SEARCH_OPERA);
        delete actual_leaf;
        auto targeted = replay.RedoForPages({{"fixture", 1, 3, 0}});
        Check(targeted[0].scan_complete && targeted[0].no_matching_redo && !targeted[0].success, "already materialized heap incorrectly counted as redo work");
        obs::Emit("fixture_ready", 0, 1, 1, 1, 1, "actual_blink_heap_checked_after_undo");
        obs::Emit("fixture_ready", 10000, BL_INIT_ROOT_PAGE_ID, 1, 1, 1, "actual_blink_heap_checked_after_undo");
        auto failed_request = request;
        failed_request.mutable_pages(0)->set_table_name("");
        storage_service::AnalyzeRecoveryPagesResponse failed_response;
        brpc::Controller failed_controller;
        service.AnalyzeRecoveryPages(&failed_controller, &failed_request, &failed_response, nullptr);
        Check(failed_response.results(0).status() == -1, "invalid page was released");
        uint64_t before = obs::WallUs();
        replay.PauseReplay();
        BatchEndLogRecord later(3, 0, 3);
        logs.write_batch_log_to_disk(Serialize(later));
        auto segment = logfmt::SegFileName(1);
        int fd = ::open(segment.c_str(), O_RDWR);
        Check(fd >= 0, "fixture WAL open failed");
        char byte = 0; off_t offset = logfmt::SEG_HEADER_SIZE + logfmt::BLOCK_SIZE;
        Check(::pread(fd, &byte, 1, offset) == 1, "fixture WAL read failed"); byte ^= 1;
        Check(::pwrite(fd, &byte, 1, offset) == 1, "fixture WAL corruption failed"); ::close(fd);
        replay.ResumeReplay();
        Check(!replay.WaitReplayCaughtUp(20), "CRC-damaged block incorrectly marked caught up");
        std::cout << "{\"kind\":\"REAL_STORAGE_WAL_REPLAY_AND_UNDO\",\"victim\":\"A\",\"seed\":20260915,\"pending_heap_logs\":3,\"pending_index_logs\":3,\"splits\":0,\"visible_keys\":" << visible
                  << ",\"crc_rejection_us\":" << obs::WallUs() - before << ",\"correctness\":\"PASS\",\"process_crash\":false,\"priority_redo\":false}\n";
        obs::Recorder::Get().Flush();
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
    return 0;
}
