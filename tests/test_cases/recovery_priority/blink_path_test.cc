#include <array>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <sys/resource.h>
#include "compute_server/server.h"
#include "core/recovery/observation.h"

namespace obs = recovery_observation;
namespace {
constexpr int kIndex = 10000, kHeap = 0;
struct Entry {
    std::unique_ptr<Page> page = std::make_unique<Page>();
    bool ready = true;
    bool failed = false;
};
std::unordered_map<uint64_t, Entry> pages;
std::mutex mutex;
std::atomic<int> blocked{0};
std::atomic<uint64_t> accesses{0};
uint64_t PageKey(int table, int page) { return uint64_t(table) * 1000000 + page; }
Entry& Get(int table, int page) { return pages.at(PageKey(table, page)); }
void Check(bool valid, const char* what) { if (!valid) throw std::runtime_error(what); }
Page* Fetch(int table, int page) {
    obs::FetchScope trace(table, page);
    bool reported = false;
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto& e = Get(table, page);
            if (e.failed) throw std::runtime_error("fixture recovery failed");
            if (e.ready) {
                trace.Complete(true);
                ++accesses;
                return e.page.get();
            }
        }
        trace.Block("fixture_ir_wait");
        if (!reported) { ++blocked; reported = true; }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}
void SetReady(int table, int page, bool ready) {
    std::lock_guard<std::mutex> lock(mutex);
    if (Get(table, page).ready == ready) return;
    Get(table, page).ready = ready;
    obs::Emit(ready ? "fixture_ready" : "fixture_not_ready", table, page, 1, 1, 1, "static_readonly_verified_image");
}
void BeginRecovery(uint64_t epoch) {
    obs::Recorder::Get().SetEpoch(epoch);
    obs::Emit("recovery_begin", -1, -1, 0, 0, 0, "fixture_not_process_crash");
    for (const auto& entry : pages) {
        if (entry.second.ready) obs::Emit("fixture_baseline_ready", entry.first / 1000000, entry.first % 1000000);
    }
}
void WaitBlocked(int count) {
    auto until = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (blocked.load() < count && std::chrono::steady_clock::now() < until) std::this_thread::yield();
    Check(blocked.load() >= count, "query did not reach controlled wait");
}
void VerifyHeap(itemkey_t key, const Rid& rid) {
    auto* page = Fetch(kHeap, rid.page_no_);
    auto* hdr = reinterpret_cast<RmPageHdr*>(page->get_data());
    const char* bitmap = page->get_data() + sizeof(RmPageHdr);
    Check(Bitmap::is_set(bitmap, rid.slot_no_) && hdr->num_records_ == 4, "heap bitmap mismatch");
    const char* tuple = bitmap + 1 + rid.slot_no_ * (sizeof(itemkey_t) + sizeof(DataItem) + sizeof(uint64_t));
    itemkey_t actual; std::memcpy(&actual, tuple, sizeof(actual));
    const auto* item = reinterpret_cast<const DataItem*>(tuple + sizeof(itemkey_t));
    uint64_t value; std::memcpy(&value, tuple + sizeof(itemkey_t) + sizeof(DataItem), sizeof(value));
    Check(actual == key && item->valid == 1 && item->lock == 0 && value == key * 7, "actual heap key/value/valid mismatch");
}
void Query(BLinkIndexHandle& index, itemkey_t key, bool expected = true) {
    obs::RequestScope request;
    Rid rid{};
    bool found = index.search(&key, rid);
    Check(found == expected, "BLink hit/miss mismatch");
    if (found) VerifyHeap(key, rid);
    request.Finish(1);
}
}

Page* ComputeServer::rpc_lazy_fetch_s_page(table_id_t table, page_id_t page, bool) {
    Page* value = Fetch(table, page);
    std::lock_guard<std::mutex> lock(mutex);
    if (value->id_.page_no == INVALID_PAGE_ID) value->id_ = PageId(table, page);
    return value;
}
Page* ComputeServer::rpc_lazy_fetch_x_page(table_id_t table, page_id_t page, bool record) { return rpc_lazy_fetch_s_page(table, page, record); }
Page* ComputeServer::rpc_fetch_s_page(table_id_t table, page_id_t page) { return rpc_lazy_fetch_s_page(table, page, false); }
Page* ComputeServer::rpc_fetch_x_page(table_id_t table, page_id_t page) { return rpc_lazy_fetch_s_page(table, page, false); }
void ComputeServer::rpc_lazy_release_s_page(table_id_t, page_id_t) {}
void ComputeServer::rpc_lazy_release_x_page(table_id_t, page_id_t) {}
void ComputeServer::rpc_release_s_page(table_id_t, page_id_t) {}
void ComputeServer::rpc_release_x_page(table_id_t, page_id_t) {}

int main(int argc, char** argv) {
    try {
        SYSTEM_MODE = 1;
        const int queries = argc > 1 ? std::stoi(argv[1]) : 200;
        for (int p : {BL_HEAD_PAGE_ID, 2, 3, 4}) pages.emplace(PageKey(kIndex, p), Entry{});
        for (int p = 1; p <= 8; ++p) pages.emplace(PageKey(kHeap, p), Entry{});
        BLFileHdr header(2, 3, 4);
        header.serialize(Get(kIndex, BL_HEAD_PAGE_ID).page->get_data());
        BLinkNodeHandle root(Get(kIndex, 2).page.get());
        root.set_is_root(true); root.set_is_leaf(false); root.set_size(0); root.init_internal_node();
        root.set_rid(0, Rid{3, -1});
        itemkey_t separator = 16; Rid right{4, -1}; root.insert_pair(1, &separator, &right);
        for (int p : {3, 4}) {
            BLinkNodeHandle leaf(Get(kIndex, p).page.get());
            leaf.set_is_leaf(true); leaf.set_is_root(false); leaf.set_size(0);
            leaf.set_right_sibling(p == 3 ? 4 : INVALID_PAGE_ID);
            leaf.set_has_high_key(p == 3);
            if (p == 3) leaf.set_high_key(16);
            for (itemkey_t key = (p == 3 ? 0 : 16); key < (p == 3 ? 16 : 32); ++key) {
                Rid rid{int(key / 4 + 1), int(key % 4)};
                leaf.insert(&key, rid);
                char* raw = Get(kHeap, rid.page_no_).page->get_data();
                auto* heap_hdr = reinterpret_cast<RmPageHdr*>(raw);
                heap_hdr->num_records_ = 4;
                char* bitmap = raw + sizeof(RmPageHdr); Bitmap::set(bitmap, rid.slot_no_);
                char* tuple = bitmap + 1 + rid.slot_no_ * (sizeof(itemkey_t) + sizeof(DataItem) + sizeof(uint64_t));
                std::memcpy(tuple, &key, sizeof(key));
                DataItem item(kHeap); item.value_size = sizeof(uint64_t);
                std::memcpy(tuple + sizeof(key), &item, sizeof(item));
                uint64_t value = key * 7;
                std::memcpy(tuple + sizeof(key) + sizeof(item), &value, sizeof(value));
            }
        }
        // The RPC adapter is a fixture, while search/leaf lookup and page formats are production code.
        ComputeServer transport(ComputeServer::InProcessTag{}, nullptr, nullptr, nullptr, nullptr, nullptr);
        BLinkIndexHandle index(&transport, kIndex);
        obs::Emit("profile_contract", -1, -1, 1, 0, 0, "test_only_static_topology");
        obs::Emit("run_begin", -1, -1, 20260915, 0, 0, "blink_path_fixture");
        BeginRecovery(0);
        std::mt19937 rng(20260915);
        uint64_t begin = obs::WallUs();
        for (int i = 0; i < queries; ++i) Query(index, rng() % 32);
        uint64_t query_us = obs::WallUs() - begin;
        Query(index, 99, false);
        itemkey_t removed = 5;
        BLinkNodeHandle leaf(Get(kIndex, 3).page.get());
        leaf.remove(&removed);
        char* removed_heap = Get(kHeap, 2).page->get_data();
        Bitmap::reset(removed_heap + sizeof(RmPageHdr), 1);
        --reinterpret_cast<RmPageHdr*>(removed_heap)->num_records_;
        Query(index, removed, false);
        Rid restored{2, 1}; leaf.insert(&removed, restored);
        Bitmap::set(removed_heap + sizeof(RmPageHdr), 1);
        ++reinterpret_cast<RmPageHdr*>(removed_heap)->num_records_;
        Query(index, removed);
        for (int scenario = 0; scenario < 3; ++scenario) {
            BeginRecovery(scenario + 1);
            blocked = 0;
            SetReady(kIndex, 3, scenario == 1);
            SetReady(kHeap, 1, scenario == 0);
            std::atomic<bool> finished{false};
            std::thread reader([&] { Query(index, 1); finished = true; });
            WaitBlocked(1);
            Check(!finished, "request escaped before readiness");
            if (scenario != 1) SetReady(kIndex, 3, true);
            if (scenario == 2) { WaitBlocked(2); Check(!finished, "index alone completed the query"); }
            SetReady(kHeap, 1, true);
            reader.join(); Check(finished, "query did not finish");
        }
        BeginRecovery(4);
        blocked = 0;
        SetReady(kIndex, 3, false);
        std::thread b([&] { Query(index, 1); });
        std::thread c([&] { Query(index, 2); });
        WaitBlocked(2); SetReady(kIndex, 3, true); b.join(); c.join();
        { std::lock_guard<std::mutex> lock(mutex); Get(kIndex, 3).failed = true; }
        bool rejected = false;
        try { Query(index, 1); } catch (const std::runtime_error&) { rejected = true; }
        Check(rejected, "failed recovery was served");
        { std::lock_guard<std::mutex> lock(mutex); Get(kIndex, 3).failed = false; }
        for (itemkey_t key = 0; key < 32; ++key) Query(index, key);
        rusage usage{};
        getrusage(RUSAGE_SELF, &usage);
        std::cout << "{\"kind\":\"REAL_BLINK_HEAP_WITH_FIXTURE_PAGE_TRANSPORT\",\"queries\":" << queries
                  << ",\"query_loop_us\":" << query_us << ",\"max_rss_kib\":" << usage.ru_maxrss << ",\"page_accesses\":" << accesses
                  << ",\"correctness\":\"PASS\",\"process_crash\":false,\"physical_redo\":false}\n";
        obs::Recorder::Get().Flush();
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
    return 0;
}
