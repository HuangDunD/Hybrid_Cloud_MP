#include "migration_worker.h"

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iterator>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "affinity_config.h"
#include "affinity_metrics.h"
#include "assignment_table.h"
#include "migration_batch.h"
#include "migration_policy.h"
#include "migration_planner.h"

#include "cache/index_cache.h"
#include "common.h"
#include "compute_server/server.h"
#include "record/record.h"
#include "util/bitmap.h"

namespace affinity {

namespace {

std::atomic<bool> g_mig_stop{false};

// Held by whichever MigrationLoop thread is currently running PlannerSweep.
// Multiple workers tick on the same cadence, but only one of them refills the
// queue per tick — the rest go straight to Drain. PlannerSweep is not safe
// against concurrent callers (it touches g_planner_cursor and
// g_candidate_stability without their own locks); serializing it via try_lock
// keeps planning single-writer while letting drain be N-way parallel.
std::mutex g_planner_mu;

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
    uint32_t same_target_observations = 0;
};

std::unordered_map<uint64_t, CandidateStability> g_candidate_stability;
size_t g_planner_cursor = 0;

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
            if (new_page < 0 || new_page >= ComputeNodeBufferPageSize) {
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
    if (snap->map.empty() || cap == 0) return;
    size_t added = 0;
    std::unordered_map<MigrationGroupKey, size_t, MigrationGroupKeyHash>
        per_source_dest_budget;

    auto it = snap->map.begin();
    const size_t map_size = snap->map.size();
    const size_t start_offset = g_planner_cursor % map_size;
    std::advance(it, static_cast<long>(start_offset));

    auto maybe_enqueue = [&](uint64_t tid,
                             const AssignmentTable::Entry& entry) {
        const int dst = entry.node_id;
        auto& stability = g_candidate_stability[tid];
        if (dst == self_node) {
            stability = CandidateStability{};
            return;
        }
        if (!IsFreshMigrationAssignment(entry.last_seen_version,
                                        snap->version)) {
            return;
        }

        const uint32_t table_id = unpack_table_id(tid);
        const uint64_t item_key = unpack_item_key(tid);

        // Locate the tuple via BLink. Skip if the local index doesn't know it.
        Rid src_rid = cs->get_rid_from_blink(static_cast<table_id_t>(table_id),
                                             static_cast<itemkey_t>(item_key));
        if (src_rid == INDEX_NOT_FOUND) return;

        const int owner = cs->get_node_id_by_page_id(
            static_cast<table_id_t>(table_id), src_rid.page_no_);
        if (owner != self_node) return;

        stability.same_target_observations =
            UpdateSameTargetObservations(stability.dst_node,
                                         stability.same_target_observations,
                                         dst);
        stability.dst_node = dst;
        stability.last_epoch = static_cast<uint32_t>(snap->version);
        if (!MigrationTargetObservedOftenEnough(
                stability.same_target_observations)) {
            return;
        }

        const MigrationGroupKey group_key{
            static_cast<table_id_t>(table_id), src_rid.page_no_, dst};
        size_t& group_plans = per_source_dest_budget[group_key];
        if (group_plans >= MaxPlansPerSourcePageDestPerSweep()) return;

        MigrationPlan plan{};
        plan.tuple_id = tid;
        plan.src_node = self_node;
        plan.dst_node = dst;
        plan.epoch    = static_cast<uint32_t>(snap->version);
        if (q.InCooldown(plan.tuple_id, plan.epoch)) return;
        if (q.Enqueue(plan)) {
            stats.migrations_planned.fetch_add(1, std::memory_order_relaxed);
            ++group_plans;
            ++added;
        }
    };

    size_t scanned = 0;
    std::vector<std::pair<uint64_t, AssignmentTable::Entry>> window;
    while (scanned < map_size && added < cap) {
        const size_t scan_window = MigrationPlannerPriorityWindow(
            cap - added, map_size - scanned);
        if (scan_window == 0) break;

        window.clear();
        window.reserve(scan_window);
        for (size_t i = 0; i < scan_window; ++i) {
            window.emplace_back(it->first, it->second);
            ++it;
            if (it == snap->map.end()) {
                it = snap->map.begin();
            }
            ++scanned;
        }

        std::sort(window.begin(), window.end(),
                  [](const auto& lhs, const auto& rhs) {
                      if (lhs.second.migration_priority !=
                          rhs.second.migration_priority) {
                          return lhs.second.migration_priority >
                                 rhs.second.migration_priority;
                      }
                      if (lhs.second.last_seen_version !=
                          rhs.second.last_seen_version) {
                          return lhs.second.last_seen_version >
                                 rhs.second.last_seen_version;
                      }
                      return lhs.first < rhs.first;
                  });

        for (const auto& candidate : window) {
            if (added >= cap) break;
            maybe_enqueue(candidate.first, candidate.second);
        }
    }
    g_planner_cursor = (start_offset + scanned) % map_size;
}

struct MigrationTableLayout {
    int record_size = 0;
    int bitmap_size = 0;
    int slots_per_pg = 0;
    size_t slot_bytes = 0;
    uint32_t empty_page_free_space = 0;
};

bool LoadMigrationTableLayout(ComputeServer* cs, table_id_t table_id,
                              MigrationTableLayout* out) {
    auto file_hdr = cs->get_file_hdr_cached(table_id);
    if (!file_hdr) return false;
    out->record_size = file_hdr->record_size_;
    out->bitmap_size = file_hdr->bitmap_size_;
    out->slots_per_pg = file_hdr->num_records_per_page_;
    out->slot_bytes =
        static_cast<size_t>(out->record_size) + sizeof(itemkey_t);
    out->empty_page_free_space =
        static_cast<uint32_t>(out->slots_per_pg) *
        static_cast<uint32_t>(out->slot_bytes);
    return out->slots_per_pg > 0 && out->slot_bytes > 0;
}

char* SlotAddr(Page* p, const MigrationTableLayout& layout, int slot_no) {
    return p->get_data() + sizeof(RmPageHdr) + OFFSET_PAGE_HDR +
           layout.bitmap_size +
           static_cast<size_t>(slot_no) * layout.slot_bytes;
}

char* BitmapAddr(Page* p) {
    return p->get_data() + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
}

bool ResolveMigrationPlan(ComputeServer* cs, const MigrationPlan& plan,
                          size_t original_index,
                          ResolvedMigrationPlan* resolved) {
    const int self_node = cs->GetNodeID();
    if (plan.src_node != self_node) return false;
    if (plan.dst_node < 0 || plan.dst_node >= ComputeNodeCount ||
        plan.dst_node == self_node) {
        return false;
    }

    const table_id_t table_id =
        static_cast<table_id_t>(unpack_table_id(plan.tuple_id));
    const itemkey_t item_key =
        static_cast<itemkey_t>(unpack_item_key(plan.tuple_id));

    Rid src_rid = cs->get_rid_from_blink(table_id, item_key);
    if (src_rid == INDEX_NOT_FOUND) return false;
    if (cs->get_node_id_by_page_id(table_id, src_rid.page_no_) != self_node) {
        return false;
    }

    resolved->plan = plan;
    resolved->table_id = table_id;
    resolved->item_key = item_key;
    resolved->src_rid = src_rid;
    resolved->original_index = original_index;
    return true;
}

size_t MigrateResolvedGroup(ComputeServer* cs,
                            const std::vector<ResolvedMigrationPlan>& resolved,
                            const MigrationGroup& group,
                            std::vector<bool>& migrated) {
    if (group.member_indices.empty()) return 0;

    MigrationTableLayout layout;
    if (!LoadMigrationTableLayout(cs, group.table_id, &layout)) return 0;

    const int self_node = cs->GetNodeID();
    std::vector<size_t> pending = group.member_indices;
    size_t moved = 0;

    while (!pending.empty()) {
        const page_id_t dst_page = AcquireDestinationPage(
            cs, group.table_id, group.dst_node, layout.empty_page_free_space);
        if (dst_page == INVALID_PAGE_ID) break;
        if (cs->get_node_id_by_page_id(group.table_id, dst_page) !=
            group.dst_node) {
            MarkDestinationPageState(group.table_id, group.dst_node, dst_page,
                                     false);
            break;
        }

        const page_id_t low_pn =
            std::min<page_id_t>(group.src_page, dst_page);
        const page_id_t high_pn =
            std::max<page_id_t>(group.src_page, dst_page);
        Page* p_low = cs->FetchXPage(group.table_id, low_pn);
        if (!p_low) break;
        Page* p_high = p_low;
        bool high_locked = false;
        if (low_pn != high_pn) {
            p_high = cs->FetchXPage(group.table_id, high_pn);
            if (!p_high) {
                cs->ReleaseXPage(group.table_id, low_pn);
                break;
            }
            high_locked = true;
        }

        auto release_pages = [&]() {
            if (high_locked) {
                cs->ReleaseXPage(group.table_id, high_pn);
            }
            cs->ReleaseXPage(group.table_id, low_pn);
        };

        Page* p_src = (group.src_page == low_pn) ? p_low : p_high;
        Page* p_dst = (dst_page == low_pn) ? p_low : p_high;
        char* src_bm = BitmapAddr(p_src);
        char* dst_bm = BitmapAddr(p_dst);
        RmPageHdr* src_hdr =
            reinterpret_cast<RmPageHdr*>(p_src->get_data() + OFFSET_PAGE_HDR);
        RmPageHdr* dst_hdr =
            reinterpret_cast<RmPageHdr*>(p_dst->get_data() + OFFSET_PAGE_HDR);

        std::vector<size_t> next_pending;
        bool dst_full = false;

        for (size_t pos = 0; pos < pending.size(); ++pos) {
            const int dst_slot =
                Bitmap::first_bit(false, dst_bm, layout.slots_per_pg);
            if (dst_slot >= layout.slots_per_pg) {
                dst_full = true;
                next_pending.insert(next_pending.end(), pending.begin() + pos,
                                    pending.end());
                break;
            }

            const ResolvedMigrationPlan& item = resolved[pending[pos]];
            if (item.original_index >= migrated.size()) continue;
            if (item.table_id != group.table_id ||
                item.plan.dst_node != group.dst_node ||
                item.src_rid.page_no_ != group.src_page ||
                item.src_rid.slot_no_ < 0 ||
                item.src_rid.slot_no_ >= layout.slots_per_pg) {
                continue;
            }
            if (!Bitmap::is_set(src_bm, item.src_rid.slot_no_)) {
                continue;
            }

            char* src_slot = SlotAddr(p_src, layout, item.src_rid.slot_no_);
            const itemkey_t src_key =
                *reinterpret_cast<const itemkey_t*>(src_slot);
            if (src_key != item.item_key ||
                cs->get_rid_from_blink(group.table_id, item.item_key) !=
                    item.src_rid) {
                continue;
            }
            const DataItem* src_item_probe = reinterpret_cast<const DataItem*>(
                src_slot + sizeof(itemkey_t));
            if (src_item_probe->lock != UNLOCKED) {
                continue;
            }

            char* dst_slot_p = SlotAddr(p_dst, layout, dst_slot);
            std::memcpy(dst_slot_p, src_slot, layout.slot_bytes);

            itemkey_t* dst_key_ptr =
                reinterpret_cast<itemkey_t*>(dst_slot_p);
            DataItem* dst_item = reinterpret_cast<DataItem*>(
                dst_slot_p + sizeof(itemkey_t));
            dst_item->value = reinterpret_cast<uint8_t*>(
                dst_slot_p + sizeof(itemkey_t) + sizeof(DataItem));

            Bitmap::set(dst_bm, dst_slot);
            ++dst_hdr->num_records_;
            p_dst->set_dirty(true);

            Rid dst_rid{dst_page, dst_slot};
            if (!cs->update_blink_entry(group.table_id, item.item_key,
                                        item.src_rid, dst_rid)) {
                Bitmap::reset(dst_bm, dst_slot);
                if (dst_hdr->num_records_ > 0) --dst_hdr->num_records_;
                p_dst->set_dirty(true);
                continue;
            }
            stats.blink_updates.fetch_add(1, std::memory_order_release);

            const uint64_t mtxid =
                mig_tx_id(static_cast<uint32_t>(group.dst_node) +
                          (static_cast<uint32_t>(self_node) << 16));

            cs->AddInsertLog(mtxid, dst_item, dst_key_ptr,
                             reinterpret_cast<const void*>(dst_item->value),
                             dst_rid, dst_hdr);

            Bitmap::reset(src_bm, item.src_rid.slot_no_);
            if (src_hdr->num_records_ > 0) --src_hdr->num_records_;
            p_src->set_dirty(true);
            itemkey_t key_for_delete = item.item_key;
            cs->AddDeleteLog(mtxid, group.table_id, &key_for_delete,
                             item.src_rid.page_no_, item.src_rid.slot_no_,
                             src_hdr);

            migrated[item.original_index] = true;
            ++moved;
        }

        const int src_free = layout.slots_per_pg - src_hdr->num_records_;
        const int dst_free = layout.slots_per_pg - dst_hdr->num_records_;
        const bool dst_has_free_slot =
            dst_hdr->num_records_ < layout.slots_per_pg;

        release_pages();

        cs->update_page_space(
            group.table_id, group.src_page,
            static_cast<uint32_t>(src_free) *
                static_cast<uint32_t>(layout.slot_bytes));
        cs->update_page_space(
            group.table_id, dst_page,
            static_cast<uint32_t>(dst_free) *
                static_cast<uint32_t>(layout.slot_bytes));
        MarkDestinationPageState(group.table_id, group.dst_node, dst_page,
                                 dst_has_free_slot);

        if (!dst_full) break;
        pending.swap(next_pending);
    }

    return moved;
}

std::vector<bool> MigrateBatchInternal(ComputeServer* cs,
                                       const std::vector<MigrationPlan>& batch) {
    std::vector<bool> migrated(batch.size(), false);
    std::vector<ResolvedMigrationPlan> resolved;
    resolved.reserve(batch.size());

    for (size_t i = 0; i < batch.size(); ++i) {
        ResolvedMigrationPlan item{};
        if (ResolveMigrationPlan(cs, batch[i], i, &item)) {
            resolved.push_back(item);
        }
    }
    if (resolved.empty()) return migrated;

    const auto groups = BuildMigrationGroups(resolved);
    for (const auto& group : groups) {
        MigrateResolvedGroup(cs, resolved, group, migrated);
    }
    return migrated;
}

}  // namespace

bool MigrateOne(ComputeServer* cs, uint64_t tuple_id, int dst_node) {
    MigrationPlan plan{};
    plan.tuple_id = tuple_id;
    plan.src_node = cs->GetNodeID();
    plan.dst_node = dst_node;
    plan.epoch = 0;
    std::vector<MigrationPlan> batch{plan};
    const auto migrated = MigrateBatchInternal(cs, batch);
    return !migrated.empty() && migrated[0];
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

        // Refill the queue from the latest assignment, but only top up to a
        // bounded depth. With N drainer workers per node, each takes up to
        // `batch` plans per tick — target depth therefore scales with N so
        // every worker has something to pull. With N=1 this collapses back to
        // the old `2 * batch` ceiling (no behavioural regression).
        if (g_planner_mu.try_lock()) {
            std::lock_guard<std::mutex> planner_lk(g_planner_mu, std::adopt_lock);
            const int n_workers = std::max(1, affinity_migration_workers);
            const size_t target_depth =
                static_cast<size_t>(affinity_migration_batch) *
                static_cast<size_t>(n_workers + 1);
            const size_t cur_depth = q.PendingCount();
            const size_t plan_cap =
                (cur_depth >= target_depth) ? 0 : (target_depth - cur_depth);
            PlannerSweep(cs, self_node, plan_cap);
        }

        // Drain a batch. Multiple workers may race here; MigrationQueue::Drain
        // is mutex-protected, so a tuple lands in exactly one worker's batch.
        batch.clear();
        q.Drain(batch, static_cast<size_t>(affinity_migration_batch));
        if (batch.empty()) continue;

        const auto migrated = MigrateBatchInternal(cs, batch);
        for (size_t i = 0; i < batch.size(); ++i) {
            const auto& p = batch[i];
            const bool ok = i < migrated.size() && migrated[i];
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
