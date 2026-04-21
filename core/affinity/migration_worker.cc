#include "migration_worker.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "affinity_config.h"
#include "affinity_metrics.h"
#include "assignment_table.h"
#include "migration_planner.h"

#include "cache/index_cache.h"
#include "common.h"
#include "compute_server/server.h"
#include "record/record.h"
#include "util/bitmap.h"

namespace affinity {

namespace {

constexpr uint32_t kStableAssignmentEpochs = 2;
constexpr size_t kMaxPlansPerSourcePagePerSweep = 2;

std::atomic<bool> g_mig_stop{false};

constexpr size_t kPoolRefillPages = 8;

// Use a synthetic tx_id distinct from worker-allocated ids by setting the
// high bit. The log sub-system treats tx_id as opaque for InsertLog/DeleteLog
// (correlated only with replay).
constexpr uint64_t kMigrationTxIdMarker = 1ull << 63;
inline uint64_t mig_tx_id(uint32_t epoch) {
    return kMigrationTxIdMarker | static_cast<uint64_t>(epoch);
}

struct DestinationPagePoolKey {
    table_id_t table_id;
    int dst_node;

    bool operator==(const DestinationPagePoolKey& other) const {
        return table_id == other.table_id && dst_node == other.dst_node;
    }
};

struct DestinationPagePoolKeyHash {
    size_t operator()(const DestinationPagePoolKey& key) const {
        return (static_cast<size_t>(key.table_id) << 8) ^
               static_cast<size_t>(key.dst_node);
    }
};

std::mutex g_dst_page_pool_mu;
std::unordered_map<DestinationPagePoolKey, std::deque<page_id_t>,
                   DestinationPagePoolKeyHash>
    g_dst_page_pool;

struct CandidateStability {
    int dst_node = -1;
    uint32_t last_epoch = 0;
    uint32_t consecutive_epochs = 0;
};

std::unordered_map<uint64_t, CandidateStability> g_candidate_stability;

void RemoveCachedPageUnlocked(std::deque<page_id_t>& pages, page_id_t page_id) {
    pages.erase(std::remove(pages.begin(), pages.end(), page_id), pages.end());
}

page_id_t AcquireDestinationPage(ComputeServer* cs, table_id_t table_id,
                                 int dst_node, uint32_t empty_page_free_space) {
    const DestinationPagePoolKey key{table_id, dst_node};
    std::lock_guard<std::mutex> lk(g_dst_page_pool_mu);
    auto& pages = g_dst_page_pool[key];
    if (pages.empty()) {
        for (size_t i = 0; i < kPoolRefillPages; ++i) {
            const page_id_t new_page = cs->rpc_create_page_on_node(table_id, dst_node);
            if (new_page == INVALID_PAGE_ID) {
                break;
            }
            cs->update_page_space(table_id, new_page, empty_page_free_space);
            pages.push_back(new_page);
        }
    }
    if (pages.empty()) {
        return INVALID_PAGE_ID;
    }
    return pages.front();
}

void MarkDestinationPageState(table_id_t table_id, int dst_node, page_id_t page_id,
                              bool has_free_slot) {
    const DestinationPagePoolKey key{table_id, dst_node};
    std::lock_guard<std::mutex> lk(g_dst_page_pool_mu);
    auto it = g_dst_page_pool.find(key);
    if (it == g_dst_page_pool.end()) {
        return;
    }
    auto& pages = it->second;
    RemoveCachedPageUnlocked(pages, page_id);
    if (has_free_slot) {
        // Prefer filling a partially used page before touching colder pages.
        pages.push_front(page_id);
    }
    if (pages.empty()) {
        g_dst_page_pool.erase(it);
    }
}

void ClearDestinationPagePool() {
    std::lock_guard<std::mutex> lk(g_dst_page_pool_mu);
    g_dst_page_pool.clear();
}

// Sweep the current AssignmentTable snapshot for tuples whose target node
// differs from `self_node` and arithmetically still belong to us (so we can
// move them). Enqueues at most `cap` new plans per sweep so the batch stays
// bounded. Tuples whose key is not in the assignment table are ignored
// (cold tuples — keep arithmetic placement).
void PlannerSweep(ComputeServer* cs, int self_node, size_t cap) {
    auto snap = GetAssignmentTable().Current();
    if (!snap) return;
    auto& q = MigrationQueue::Instance();
    size_t added = 0;
    std::unordered_map<uint64_t, size_t> per_page_budget;
    for (const auto& kv : snap->map) {
        if (added >= cap) break;
        const uint64_t tid = kv.first;
        const int dst = kv.second;
        auto& stability = g_candidate_stability[tid];
        if (dst == self_node) {
            stability = CandidateStability{};
            continue;
        }

        const uint32_t table_id = unpack_table_id(tid);
        const uint64_t item_key = unpack_item_key(tid);

        // Locate the tuple via BLink. Skip if the local index doesn't know it.
        Rid src_rid = cs->get_rid_from_blink(static_cast<table_id_t>(table_id),
                                             static_cast<itemkey_t>(item_key));
        if (src_rid == INDEX_NOT_FOUND) continue;

        const int owner = cs->get_node_id_by_page_id(
            static_cast<table_id_t>(table_id), src_rid.page_no_);
        if (owner != self_node) continue;

        if (stability.dst_node == dst &&
            stability.last_epoch + 1 == static_cast<uint32_t>(snap->version)) {
            ++stability.consecutive_epochs;
        } else {
            stability.dst_node = dst;
            stability.consecutive_epochs = 1;
        }
        stability.last_epoch = static_cast<uint32_t>(snap->version);
        if (stability.consecutive_epochs < kStableAssignmentEpochs) continue;

        const uint64_t page_key =
            (static_cast<uint64_t>(table_id) << 32) |
            static_cast<uint64_t>(src_rid.page_no_);
        size_t& page_plans = per_page_budget[page_key];
        if (page_plans >= kMaxPlansPerSourcePagePerSweep) continue;

        MigrationPlan plan{};
        plan.tuple_id = tid;
        plan.src_node = self_node;
        plan.dst_node = dst;
        plan.epoch    = static_cast<uint32_t>(snap->version);
        if (q.InCooldown(plan.tuple_id, plan.epoch)) continue;
        if (q.Enqueue(plan)) {
            stats.migrations_planned.fetch_add(1, std::memory_order_relaxed);
            ++page_plans;
            ++added;
        }
    }
}

}  // namespace

bool MigrateOne(ComputeServer* cs, uint64_t tuple_id, int dst_node) {
    const table_id_t table_id =
        static_cast<table_id_t>(unpack_table_id(tuple_id));
    const itemkey_t  item_key =
        static_cast<itemkey_t>(unpack_item_key(tuple_id));
    const int self_node = cs->GetNodeID();

    // 1. Resolve src Rid via BLink.
    Rid src_rid = cs->get_rid_from_blink(table_id, item_key);
    if (src_rid == INDEX_NOT_FOUND) return false;

    // 2. Verify we still own the src page (race: another planner might have
    //    moved it already, or BLink was repointed by an app insert/update).
    if (cs->get_node_id_by_page_id(table_id, src_rid.page_no_) != self_node) {
        return false;
    }

    // 3. Read file_hdr to learn slot layout (record_size_, bitmap_size_,
    //    num_records_per_page_).
    auto file_hdr = cs->get_file_hdr_cached(table_id);
    if (!file_hdr) return false;
    const int record_size  = file_hdr->record_size_;
    const int bitmap_size  = file_hdr->bitmap_size_;
    const int slots_per_pg = file_hdr->num_records_per_page_;
    const size_t slot_bytes =
        static_cast<size_t>(record_size) + sizeof(itemkey_t);
    const uint32_t empty_page_free_space =
        static_cast<uint32_t>(slots_per_pg) * static_cast<uint32_t>(slot_bytes);

    auto slot_addr = [&](Page* p, int slot_no) {
        return p->get_data() + sizeof(RmPageHdr) + bitmap_size +
               static_cast<size_t>(slot_no) * slot_bytes;
    };
    auto bitmap_addr = [&](Page* p) {
        return p->get_data() + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    };

    // 4. Reuse a small pool of destination pages per (table, dst_node). This
    //    fills existing target pages before allocating more, instead of
    //    burning one page per migrated tuple.
    while (true) {
        const page_id_t dst_page = AcquireDestinationPage(
            cs, table_id, dst_node, empty_page_free_space);
        if (dst_page == INVALID_PAGE_ID) return false;

        // Lock pages in deterministic page-no order to avoid deadlock.
        Page* p_low = nullptr;
        Page* p_high = nullptr;
        page_id_t low_pn  = std::min<page_id_t>(src_rid.page_no_, dst_page);
        page_id_t high_pn = std::max<page_id_t>(src_rid.page_no_, dst_page);
        p_low  = cs->FetchXPage(table_id, low_pn);
        if (!p_low) return false;
        if (low_pn != high_pn) {
            p_high = cs->FetchXPage(table_id, high_pn);
            if (!p_high) {
                cs->ReleaseXPage(table_id, low_pn);
                return false;
            }
        } else {
            p_high = p_low;
        }
        Page* p_src = (src_rid.page_no_ == low_pn) ? p_low : p_high;
        Page* p_dst = (dst_page         == low_pn) ? p_low : p_high;

        bool src_locked = true;
        bool dst_locked = (low_pn != high_pn);
        auto release_src = [&]() {
            if (!src_locked) return;
            cs->ReleaseXPage(table_id, src_rid.page_no_);
            src_locked = false;
        };
        auto release_dst = [&]() {
            if (!dst_locked) return;
            cs->ReleaseXPage(table_id, dst_page);
            dst_locked = false;
        };
        auto release_pages = [&]() {
            release_dst();
            release_src();
        };

        char* src_bm = bitmap_addr(p_src);
        char* dst_bm = bitmap_addr(p_dst);

        if (!Bitmap::is_set(src_bm, src_rid.slot_no_)) {
            release_pages();
            return false;
        }

        int dst_slot = Bitmap::first_bit(false, dst_bm, slots_per_pg);
        if (dst_slot >= slots_per_pg) {
            release_pages();
            MarkDestinationPageState(table_id, dst_node, dst_page, false);
            continue;
        }

        char* src_slot = slot_addr(p_src, src_rid.slot_no_);
        char* dst_slot_p = slot_addr(p_dst, dst_slot);
        std::memcpy(dst_slot_p, src_slot, slot_bytes);

        itemkey_t* dst_key_ptr = reinterpret_cast<itemkey_t*>(dst_slot_p);
        DataItem* dst_item =
            reinterpret_cast<DataItem*>(dst_slot_p + sizeof(itemkey_t));
        dst_item->value = reinterpret_cast<uint8_t*>(
            dst_slot_p + sizeof(itemkey_t) + sizeof(DataItem));

        Bitmap::set(dst_bm, dst_slot);
        RmPageHdr* dst_hdr =
            reinterpret_cast<RmPageHdr*>(p_dst->get_data() + OFFSET_PAGE_HDR);
        dst_hdr->num_records_++;
        p_dst->set_dirty(true);

        Rid dst_rid{dst_page, dst_slot};
        const uint64_t mtxid = mig_tx_id(static_cast<uint32_t>(dst_node) +
                                         (static_cast<uint32_t>(self_node) << 16));

        cs->AddInsertLog(mtxid, dst_item, dst_key_ptr,
                         reinterpret_cast<const void*>(dst_item->value),
                         dst_rid, dst_hdr);

        // Only the page copy / destination-page metadata update needs both
        // X locks. Once the new tuple version is durable in dst, keep only the
        // source-page lock while we repoint the index and retire the old slot.
        release_dst();

        cs->delete_from_blink(table_id, item_key);
        cs->insert_into_blink(table_id, item_key, dst_rid);

        Bitmap::reset(src_bm, src_rid.slot_no_);
        RmPageHdr* src_hdr =
            reinterpret_cast<RmPageHdr*>(p_src->get_data() + OFFSET_PAGE_HDR);
        if (src_hdr->num_records_ > 0) src_hdr->num_records_--;
        p_src->set_dirty(true);
        itemkey_t key_for_delete = item_key;
        cs->AddDeleteLog(mtxid, table_id, &key_for_delete,
                         src_rid.page_no_, src_rid.slot_no_, src_hdr);

        const int src_free = slots_per_pg - src_hdr->num_records_;
        const int dst_free = slots_per_pg - dst_hdr->num_records_;
        const bool dst_has_free_slot = dst_hdr->num_records_ < slots_per_pg;

        release_src();

        cs->update_page_space(table_id, src_rid.page_no_,
                              static_cast<uint32_t>(src_free));
        cs->update_page_space(table_id, dst_page,
                              static_cast<uint32_t>(dst_free));
        MarkDestinationPageState(table_id, dst_node, dst_page, dst_has_free_slot);
        return true;
    }
}

void MigrationLoop(ComputeServer* cs) {
    if (!enable_affinity) return;
    if (ComputeNodeCount < 2) return;

    const int self_node = cs->GetNodeID();
    auto& q = MigrationQueue::Instance();
    std::vector<MigrationPlan> batch;
    batch.reserve(static_cast<size_t>(affinity_migration_batch));

    while (!g_mig_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(affinity_migration_tick_ms));

        // Refill the queue from the latest assignment.
        const size_t plan_cap =
            static_cast<size_t>(affinity_migration_batch) * 4;
        PlannerSweep(cs, self_node, plan_cap);

        // Drain a batch.
        batch.clear();
        q.Drain(batch, static_cast<size_t>(affinity_migration_batch));
        if (batch.empty()) continue;

        for (const auto& p : batch) {
            const bool ok = MigrateOne(cs, p.tuple_id, p.dst_node);
            if (ok) {
                stats.migrations_done.fetch_add(1, std::memory_order_relaxed);
            } else {
                stats.migrations_failed.fetch_add(1, std::memory_order_relaxed);
            }
            q.MarkDone(p.tuple_id, p.epoch, ok);
        }
    }
}

void RequestMigrationStop() {
    g_mig_stop.store(true, std::memory_order_relaxed);
    ClearDestinationPagePool();
}

}  // namespace affinity
