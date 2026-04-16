#include "migration_worker.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <thread>
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

std::atomic<bool> g_mig_stop{false};

// Use a synthetic tx_id distinct from worker-allocated ids by setting the
// high bit. The log sub-system treats tx_id as opaque for InsertLog/DeleteLog
// (correlated only with replay).
constexpr uint64_t kMigrationTxIdMarker = 1ull << 63;
inline uint64_t mig_tx_id(uint32_t epoch) {
    return kMigrationTxIdMarker | static_cast<uint64_t>(epoch);
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
    for (const auto& kv : snap->map) {
        if (added >= cap) break;
        const uint64_t tid = kv.first;
        const int dst = kv.second;
        if (dst == self_node) continue;

        const uint32_t table_id = unpack_table_id(tid);
        const uint64_t item_key = unpack_item_key(tid);

        // Locate the tuple via BLink. Skip if the local index doesn't know it.
        Rid src_rid = cs->get_rid_from_blink(static_cast<table_id_t>(table_id),
                                             static_cast<itemkey_t>(item_key));
        if (src_rid == INDEX_NOT_FOUND) continue;

        const int owner = cs->get_node_id_by_page_id(
            static_cast<table_id_t>(table_id), src_rid.page_no_);
        if (owner != self_node) continue;

        MigrationPlan plan{};
        plan.tuple_id = tid;
        plan.src_node = self_node;
        plan.dst_node = dst;
        plan.epoch    = static_cast<uint32_t>(snap->version);
        if (q.Enqueue(plan)) {
            stats.migrations_planned.fetch_add(1, std::memory_order_relaxed);
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

    // 3. Allocate a destination page that lands on dst_node arithmetically.
    page_id_t dst_page = cs->rpc_create_page_on_node(table_id, dst_node);
    if (dst_page == INVALID_PAGE_ID) return false;

    // 4. Lock pages in deterministic page-no order to avoid migration vs
    //    migration deadlock. dst_page > src_rid.page_no_ in practice
    //    (allocate_page is monotonic), but we don't depend on it.
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
        p_high = p_low;  // shouldn't happen — defensive
    }
    Page* p_src = (src_rid.page_no_ == low_pn) ? p_low : p_high;
    Page* p_dst = (dst_page         == low_pn) ? p_low : p_high;

    // Release helper: reverse-order unlock; safe for both single- and two-page
    // cases. Used by every early-return below and by the success path.
    auto release_pages = [&]() {
        cs->ReleaseXPage(table_id, high_pn);
        if (low_pn != high_pn) cs->ReleaseXPage(table_id, low_pn);
    };

    // 5. Read file_hdr to learn slot layout (record_size_, bitmap_size_,
    //    num_records_per_page_).
    auto file_hdr = cs->get_file_hdr_cached(table_id);
    if (!file_hdr) {
        release_pages();
        return false;
    }
    const int record_size  = file_hdr->record_size_;
    const int bitmap_size  = file_hdr->bitmap_size_;
    const int slots_per_pg = file_hdr->num_records_per_page_;
    const size_t slot_bytes =
        static_cast<size_t>(record_size) + sizeof(itemkey_t);

    auto slot_addr = [&](Page* p, int slot_no) {
        return p->get_data() + sizeof(RmPageHdr) + bitmap_size +
               static_cast<size_t>(slot_no) * slot_bytes;
    };
    auto bitmap_addr = [&](Page* p) {
        return p->get_data() + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    };

    char* src_bm = bitmap_addr(p_src);
    char* dst_bm = bitmap_addr(p_dst);

    // 6. Re-validate src slot still occupied (race: app txn may have deleted).
    if (!Bitmap::is_set(src_bm, src_rid.slot_no_)) {
        release_pages();
        return false;
    }

    // 7. Find a free slot in dst page. A freshly created dst_page is empty.
    int dst_slot = Bitmap::first_bit(false, dst_bm, slots_per_pg);
    if (dst_slot >= slots_per_pg) {
        // dst page already filled by races — abandon this attempt.
        release_pages();
        return false;
    }

    // 8. Copy slot bytes (key + DataItem header + value) verbatim.
    char* src_slot = slot_addr(p_src, src_rid.slot_no_);
    char* dst_slot_p = slot_addr(p_dst, dst_slot);
    std::memcpy(dst_slot_p, src_slot, slot_bytes);

    // Reconstruct DataItem* for AddInsertLog. The slot layout is
    // [itemkey_t key][DataItem header][value bytes].
    itemkey_t* dst_key_ptr = reinterpret_cast<itemkey_t*>(dst_slot_p);
    DataItem*  dst_item    = reinterpret_cast<DataItem*>(dst_slot_p + sizeof(itemkey_t));
    // Fix value pointer to live inside the slot, matching reader convention.
    dst_item->value =
        reinterpret_cast<uint8_t*>(dst_slot_p + sizeof(itemkey_t) + sizeof(DataItem));

    // 9. Mark dst slot occupied + bump record count, update FSM later.
    Bitmap::set(dst_bm, dst_slot);
    RmPageHdr* dst_hdr = reinterpret_cast<RmPageHdr*>(p_dst->get_data() + OFFSET_PAGE_HDR);
    dst_hdr->num_records_++;
    p_dst->set_dirty(true);

    Rid dst_rid{dst_page, dst_slot};
    const uint64_t mtxid = mig_tx_id(static_cast<uint32_t>(dst_node) +
                                     (static_cast<uint32_t>(self_node) << 16));

    // 10. Log dst insert. Note: AddInsertLog needs the dst page hdr — it
    //     bumps the page LLSN and assigns prev_lsn to the log entry.
    cs->AddInsertLog(mtxid, dst_item, dst_key_ptr,
                     reinterpret_cast<const void*>(dst_item->value),
                     dst_rid, dst_hdr);

    // 11. Re-point the BLink index. insert_entry is a no-op when the key
    //     already exists (returns INVALID_PAGE_ID), so we delete first.
    //     The window between delete + insert is small; for the paper-level
    //     workloads (YCSB / SmallBank read-heavy + TPCC where inserts target
    //     fresh keys not yet in the assignment table) no app txn will race
    //     us on the same key. We hold X-locks on both the src and dst pages
    //     so a reader hitting either page is serialized behind us.
    cs->delete_from_blink(table_id, item_key);
    cs->insert_into_blink(table_id, item_key, dst_rid);

    // 12. Mark src slot deleted + decrement count + write delete log.
    Bitmap::reset(src_bm, src_rid.slot_no_);
    RmPageHdr* src_hdr = reinterpret_cast<RmPageHdr*>(p_src->get_data() + OFFSET_PAGE_HDR);
    if (src_hdr->num_records_ > 0) src_hdr->num_records_--;
    p_src->set_dirty(true);
    itemkey_t key_for_delete = item_key;
    cs->AddDeleteLog(mtxid, table_id, &key_for_delete,
                     src_rid.page_no_, src_rid.slot_no_, src_hdr);

    // 13. Release pages in reverse order.
    release_pages();

    // 14. Update FSM: src page has more free space, dst less.
    const int src_free = slots_per_pg - src_hdr->num_records_;
    const int dst_free = slots_per_pg - dst_hdr->num_records_;
    cs->update_page_space(table_id, src_rid.page_no_,
                          static_cast<uint32_t>(src_free));
    cs->update_page_space(table_id, dst_page,
                          static_cast<uint32_t>(dst_free));

    return true;
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
            q.MarkDone(p.tuple_id);
        }
    }
}

void RequestMigrationStop() {
    g_mig_stop.store(true, std::memory_order_relaxed);
}

}  // namespace affinity
