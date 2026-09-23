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
        // bounded DELETE 属已提交事务（回放边界测试，无 undo 载荷）：
        // 其 BatchEnd 必须在恢复前到达，Undo 才会跳过它；否则会被正确
        // 判为"缺前镜像"并使恢复保持隔离（fail-closed）
        BatchEndLogRecord bounded_commit(7, 0, 700);
        InsertLogRecord uncommitted(2, 0, 2, aborted, 1, 2, "fixture"); uncommitted.lsn_ = 3; uncommitted.prev_lsn_ = 2;
        BLinkInsertLogRecord uncommitted_index(2, 0, 2, 10000, "fixture_bl", aborted_key, Rid{1, 2});
        replay.PauseReplay();
        {
            const std::string name = "delete_name_fixture_long_table";
            const std::string suffix_name = name + "_fsm";
            disk.create_file(name);
            disk.create_file(suffix_name);
            int target_fd = disk.open_file(name);
            int unrelated_fd = disk.open_file(suffix_name);
            for (int fd : {target_fd, unrelated_fd}) {
                disk.write_page(fd, 0, head.data(), PAGE_SIZE);
                disk.write_page(fd, 1, data.data(), PAGE_SIZE);
            }
            DeleteLogRecord bounded(7, 0, 700, 0, name, 1, 0);
            bounded.lsn_ = 1;
            bounded.prev_lsn_ = 0;
            delete[] bounded.table_name_;
            bounded.table_name_ = new char[suffix_name.size() + 1];
            std::memcpy(bounded.table_name_, suffix_name.c_str(), suffix_name.size() + 1);
            replay.apply_sigle_log(&bounded, 0, /*sync_to_disk=*/true);
            std::array<char, PAGE_SIZE> target{}, unrelated{};
            disk.read_page(target_fd, 1, target.data(), PAGE_SIZE);
            disk.read_page(unrelated_fd, 1, unrelated.data(), PAGE_SIZE);
            Check(!Bitmap::is_set(target.data() + sizeof(RmPageHdr), 0), "DELETE ignored bounded table name");
            Check(unrelated == data, "DELETE wrote to suffix-named unrelated file");
            std::cout << "DELETE_LENGTH_BOUNDED_REPLAY_PASS\n";
        }
        logs.write_batch_log_to_disk(Serialize(insert) + Serialize(index_insert) + Serialize(erase) + Serialize(index_delete) + Serialize(commit) + Serialize(uncommitted) + Serialize(uncommitted_index) + Serialize(bounded_commit));
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
        // L11 语义（c3252d1）：heap 页经背景追平后 disk_lsn>=gplm_lsn，应为
        // no-modify（status=0）；BLink 派生页无条件走回放树整页恢复（status=1，
        // 计算页空间副本可能随死节点缓冲丢失，须从 WAL 权威重建）——两者
        // 均非失败。原断言"两页均 status==0"是 L11 之前的语义。
        Check(response.results(0).status() == 0, "heap background catchup incorrectly labelled targeted redo");
        Check(response.results(1).status() == 1, "blink replay-tree recovery not labelled targeted redo");
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
        {
            disk.create_file("page_space_bl");
            S_BLinkIndexHandle replay_tree(&disk, &buffer, std::string("page_space"));
            buffer.flush_all_pages();
            Check(std::filesystem::copy_file("page_space_bl", "page_space_bl_compute"), "initial page-space copy failed");
            storage_service::StoragePoolImpl isolated(&logs, &disk, &rm, nullptr, 0, nullptr);
            isolated.RegisterComputeIndex("page_space_bl", "page_space_bl_compute");
            for (itemkey_t key = 1; key <= 600; ++key)
                Check(replay_tree.insert_entry(&key, Rid{1, static_cast<int>(key)}) >= 0, "replay tree insert failed");
            buffer.flush_all_pages();
            const int replay_pages = disk.get_fd2pageno(replay_tree.getFD());
            Check(replay_pages > 3, "default-fanout split not exercised");
            std::vector<std::array<char, PAGE_SIZE>> before_pages(replay_pages);
            for (int p = 0; p < replay_pages; ++p)
                disk.read_page(replay_tree.getFD(), p, before_pages[p].data(), PAGE_SIZE);
            storage_service::CreatePageRequest allocate;
            allocate.set_table_id(10000); allocate.set_table_name("./page_space_bl");
            storage_service::CreatePageResponse allocated;
            brpc::Controller allocate_controller;
            isolated.CreatePage(&allocate_controller, &allocate, &allocated, nullptr);
            Check(!allocate_controller.Failed() && allocated.success() && allocated.page_no() == 3,
                  "compute allocation crossed into replay page space");
            storage_service::WritePageRequest write;
            write.mutable_page_id()->set_table_name("page_space_bl");
            write.mutable_page_id()->set_page_no(1);
            write.set_data(before_pages.back().data(), PAGE_SIZE);
            storage_service::WritePageResponse written;
            brpc::Controller write_controller;
            isolated.WritePage(&write_controller, &write, &written, nullptr);
            Check(!write_controller.Failed(), "compute page write failed");
            storage_service::GetPageRequest read;
            auto* wanted = read.add_page_id(); wanted->set_table_name("./page_space_bl"); wanted->set_page_no(1);
            storage_service::GetPageResponse fetched;
            brpc::Controller read_controller;
            isolated.GetPage(&read_controller, &read, &fetched, nullptr);
            Check(!read_controller.Failed() && fetched.data() == write.data() && fetched.allocated_pages_size() == 1 &&
                  fetched.allocated_pages(0) == 4, "physical read or allocation watermark crossed page spaces");
            for (int p = 0; p < replay_pages; ++p) {
                std::array<char, PAGE_SIZE> after{};
                disk.read_page(replay_tree.getFD(), p, after.data(), PAGE_SIZE);
                Check(after == before_pages[p], "compute eviction overwrote logical replay tree");
            }
            for (itemkey_t key = 1; key <= 600; ++key)
                Check(replay_tree.search(&key, rid) && rid == Rid{1, static_cast<int>(key)}, "replay key changed after physical write");
            write.mutable_page_id()->set_page_no(4);
            brpc::Controller bad_write_controller;
            isolated.WritePage(&bad_write_controller, &write, &written, nullptr);
            Check(bad_write_controller.Failed(), "unallocated physical page write accepted");
            std::cout << "INDEPENDENT_BLINK_PAGE_SPACE_PASS keys=600 default_fanout=253\n";
            auto* undo_tree = sm.GetOrCreateBLinkHandle("page_space_bl");
            const itemkey_t reincarnated = 100;
            const Rid new_rid{2, 0};
            undo_tree->remove_entry(&reincarnated);
            Check(undo_tree->insert_entry(&reincarnated, new_rid) >= 0, "same-key reinsertion failed");
            BLinkInsertLogRecord old_insert(9, 0, 9, 10000, "page_space_bl", reincarnated, Rid{1, 100});
            const auto old_insert_bytes = Serialize(old_insert);
            Check(!replay.ApplyUndoWalRecord(old_insert_bytes.data(), old_insert_bytes.size()), "old INSERT Undo touched new RID");
            BLinkDeleteLogRecord old_delete(9, 0, 9, 10000, "page_space_bl", reincarnated, Rid{1, 100});
            const auto old_delete_bytes = Serialize(old_delete);
            Check(!replay.ApplyUndoWalRecord(old_delete_bytes.data(), old_delete_bytes.size()), "old DELETE Undo replaced new RID");
            Check(undo_tree->search(&reincarnated, rid) && rid == new_rid, "new RID lost after old Undo");
            std::cout << "SAME_KEY_DIFFERENT_RID_UNDO_PASS\n";
        }
        {
            std::array<char, PAGE_SIZE> before_page{}, after_page{};
            disk.read_page(heap, 1, before_page.data(), PAGE_SIZE);
            const auto have = reinterpret_cast<const RmPageHdr*>(before_page.data())->LLSN_;
            InsertLogRecord missing(10, 0, 10, aborted, 1, 2, "fixture");
            missing.lsn_ = have + 2;
            missing.prev_lsn_ = have + 1;
            bool rejected = false;
            try { replay.apply_sigle_log(&missing, 0, /*sync_to_disk=*/true); }
            catch (const std::runtime_error&) { rejected = true; }
            disk.read_page(heap, 1, after_page.data(), PAGE_SIZE);
            Check(rejected && before_page == after_page, "missing WAL predecessor silently accepted");
            std::cout << "REPLAY_PREDECESSOR_GAP_REJECTED_PASS\n";
        }
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
