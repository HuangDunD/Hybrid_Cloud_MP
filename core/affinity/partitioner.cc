#include "partitioner.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include "affinity_config.h"
#include "affinity_metrics.h"
#include "affinity_service.pb.h"
#include "assignment_table.h"
#include "compute_server/server.h"
#include "edge_shuffler.h"
#include "partition_acceptance.h"
#include "../../parmetis_sidecar/uds_protocol.h"

namespace affinity {

// ---------------- PartitionCoordinator ----------------

PartitionCoordinator& PartitionCoordinator::Instance() {
    static PartitionCoordinator g;
    return g;
}

std::shared_ptr<EpochCoord> PartitionCoordinator::Get(uint32_t epoch) {
    std::lock_guard<std::mutex> lk(map_mtx_);
    auto it = epochs_.find(epoch);
    if (it != epochs_.end()) return it->second;
    auto c = std::make_shared<EpochCoord>();
    c->epoch = epoch;
    c->owned_tuples_by_rank.resize(ComputeNodeCount);
    epochs_[epoch] = c;
    return c;
}

void PartitionCoordinator::Drop(uint32_t epoch) {
    std::lock_guard<std::mutex> lk(map_mtx_);
    epochs_.erase(epoch);
}

void PartitionCoordinator::OnInventory(uint32_t epoch, int from_rank,
                                       const uint64_t* owned, size_t n) {
    auto c = Get(epoch);
    std::lock_guard<std::mutex> lk(c->mtx);
    if (from_rank >= 0 && from_rank < static_cast<int>(c->owned_tuples_by_rank.size())) {
        c->owned_tuples_by_rank[from_rank].assign(owned, owned + n);
    }
    c->inventory_seen.insert(from_rank);
    if (static_cast<int>(c->inventory_seen.size()) >= ComputeNodeCount) {
        c->inventory_ready = true;
        c->cv.notify_all();
    }
}

void PartitionCoordinator::OnAssignmentSlice(uint32_t epoch, int from_rank,
                                             const uint64_t* tuples,
                                             const int32_t* nodes, size_t n,
                                             bool final) {
    auto c = Get(epoch);
    std::lock_guard<std::mutex> lk(c->mtx);
    for (size_t i = 0; i < n; ++i) {
        c->pending_assignment[tuples[i]] = nodes[i];
    }
    if (final) c->slice_seen.insert(from_rank);
    if (static_cast<int>(c->slice_seen.size()) >= ComputeNodeCount) {
        c->assignment_ready = true;
        c->cv.notify_all();
    }
}

bool PartitionCoordinator::WaitInventory(uint32_t epoch, int n_ranks, int timeout_ms) {
    auto c = Get(epoch);
    std::unique_lock<std::mutex> lk(c->mtx);
    return c->cv.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                          [&] { return c->inventory_ready || stopping_.load(); }) &&
           !stopping_.load();
}

bool PartitionCoordinator::WaitAssignment(uint32_t epoch, int n_ranks, int timeout_ms) {
    auto c = Get(epoch);
    std::unique_lock<std::mutex> lk(c->mtx);
    return c->cv.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                          [&] { return c->assignment_ready || stopping_.load(); }) &&
           !stopping_.load();
}

void PartitionCoordinator::Stop() {
    stopping_.store(true);
    std::lock_guard<std::mutex> lk(map_mtx_);
    for (auto& kv : epochs_) {
        std::lock_guard<std::mutex> lk2(kv.second->mtx);
        kv.second->cv.notify_all();
    }
}

// ---------------- Partitioner UDS client ----------------

namespace {

int resolve_physical_owner(ComputeServer* cs, uint64_t tuple_id, int fallback_node) {
    const table_id_t table_id =
        static_cast<table_id_t>(unpack_table_id(tuple_id));
    const itemkey_t item_key =
        static_cast<itemkey_t>(unpack_item_key(tuple_id));
    Rid rid = cs->get_rid_from_blink(table_id, item_key);
    if (rid == INDEX_NOT_FOUND) return fallback_node;
    return cs->get_node_id_by_page_id(table_id, rid.page_no_);
}

std::string resolve_uds_path(const std::string& base_path, int rank, int n_ranks) {
    if (n_ranks <= 1 || rank < 0) return base_path;
    return base_path + "." + std::to_string(rank);
}

int connect_uds(const std::string& path) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (path.size() >= sizeof(addr.sun_path)) {
        ::close(fd);
        return -1;
    }
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

bool write_full(int fd, const void* buf, size_t n) {
    const char* p = static_cast<const char*>(buf);
    while (n > 0) {
        ssize_t w = ::write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += w;
        n -= w;
    }
    return true;
}

bool read_full(int fd, void* buf, size_t n) {
    char* p = static_cast<char*>(buf);
    while (n > 0) {
        ssize_t r = ::read(fd, p, n);
        if (r == 0) return false;  // EOF — sidecar died
        if (r < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += r;
        n -= r;
    }
    return true;
}

// Send one partition request: header + variable-length arrays in the order
// declared in uds_protocol.h.
bool send_request(int fd, const affinity_uds::ReqHeader& hdr,
                  const std::vector<affinity_uds::idx_t>& vtxdist,
                  const std::vector<affinity_uds::idx_t>& xadj,
                  const std::vector<affinity_uds::idx_t>& adjncy,
                  const std::vector<affinity_uds::idx_t>& vwgt,
                  const std::vector<affinity_uds::idx_t>& vsize,
                  const std::vector<affinity_uds::idx_t>& adjwgt,
                  const std::vector<affinity_uds::idx_t>& prev_part) {
    if (!write_full(fd, &hdr, sizeof(hdr))) return false;
    if (!write_full(fd, vtxdist.data(), vtxdist.size() * sizeof(affinity_uds::idx_t))) return false;
    if (!write_full(fd, xadj.data(),    xadj.size()    * sizeof(affinity_uds::idx_t))) return false;
    if (!write_full(fd, adjncy.data(),  adjncy.size()  * sizeof(affinity_uds::idx_t))) return false;
    if (hdr.has_vwgt && !write_full(fd, vwgt.data(),    vwgt.size()    * sizeof(affinity_uds::idx_t))) return false;
    if (hdr.has_vsize && !write_full(fd, vsize.data(),   vsize.size()   * sizeof(affinity_uds::idx_t))) return false;
    if (hdr.has_adjwgt && !write_full(fd, adjwgt.data(),  adjwgt.size()  * sizeof(affinity_uds::idx_t))) return false;
    if (hdr.has_prev_part && !write_full(fd, prev_part.data(), prev_part.size() * sizeof(affinity_uds::idx_t))) return false;
    return true;
}

bool recv_response(int fd, affinity_uds::RespHeader& hdr,
                   std::vector<affinity_uds::idx_t>& part) {
    if (!read_full(fd, &hdr, sizeof(hdr))) return false;
    if (hdr.magic != affinity_uds::kRespMagic) return false;
    part.resize(hdr.nvtx_local);
    if (hdr.nvtx_local > 0 &&
        !read_full(fd, part.data(), hdr.nvtx_local * sizeof(affinity_uds::idx_t))) {
        return false;
    }
    return true;
}

// Push our local (tuple_id -> node_id) slice to all peers and to ourselves.
// Fires all peer RPCs concurrently via brpc::Join — for N ranks this is
// N-1x better than the previous serial loop on the partition critical path.
void BroadcastAssignmentSlice(ComputeServer* cs, uint32_t epoch,
                              const std::vector<uint64_t>& tuples,
                              const std::vector<int>& nodes) {
    const int self_rank = cs->GetNodeID();
    const int n_ranks = ComputeNodeCount;

    // Local short-circuit
    PartitionCoordinator::Instance().OnAssignmentSlice(
        epoch, self_rank, tuples.data(),
        reinterpret_cast<const int32_t*>(nodes.data()), tuples.size(), true);

    std::vector<brpc::CallId> cids;
    std::vector<std::unique_ptr<brpc::Controller>> cntls;
    std::vector<std::unique_ptr<affinity_proto::AssignmentSliceRequest>> reqs;
    std::vector<std::unique_ptr<affinity_proto::AssignmentSliceResponse>> resps;
    cids.reserve(n_ranks);
    cntls.reserve(n_ranks);
    reqs.reserve(n_ranks);
    resps.reserve(n_ranks);

    for (int r = 0; r < n_ranks; ++r) {
        if (r == self_rank) continue;
        auto* chan = cs->GetComputeChannel(r);
        if (!chan) continue;
        auto cntl = std::unique_ptr<brpc::Controller>(new brpc::Controller);
        auto req  = std::unique_ptr<affinity_proto::AssignmentSliceRequest>(
            new affinity_proto::AssignmentSliceRequest);
        auto resp = std::unique_ptr<affinity_proto::AssignmentSliceResponse>(
            new affinity_proto::AssignmentSliceResponse);
        req->set_epoch(epoch);
        req->set_from_rank(self_rank);
        req->set_tuple_ids(reinterpret_cast<const char*>(tuples.data()),
                           tuples.size() * sizeof(uint64_t));
        req->set_node_ids(reinterpret_cast<const char*>(nodes.data()),
                          nodes.size() * sizeof(int32_t));
        req->set_final(true);

        cids.push_back(cntl->call_id());
        affinity_proto::AffinityService_Stub stub(chan);
        stub.PushAssignmentSlice(cntl.get(), req.get(), resp.get(),
                                 brpc::DoNothing());
        cntls.push_back(std::move(cntl));
        reqs.push_back(std::move(req));
        resps.push_back(std::move(resp));
    }
    for (auto cid : cids) brpc::Join(cid);
    // Failures manifest as partition_skipped on the requesting side (the
    // assignment table simply won't advance this epoch). We don't inspect
    // cntl->Failed() here because the side effect — peer didn't see our slice —
    // is already handled by the WaitAssignment timeout.
}

void BroadcastVertexInventory(ComputeServer* cs, uint32_t epoch,
                              const std::vector<uint64_t>& owned) {
    const int self_rank = cs->GetNodeID();
    const int n_ranks = ComputeNodeCount;

    PartitionCoordinator::Instance().OnInventory(
        epoch, self_rank, owned.data(), owned.size());

    std::vector<brpc::CallId> cids;
    std::vector<std::unique_ptr<brpc::Controller>> cntls;
    std::vector<std::unique_ptr<affinity_proto::VertexInventoryRequest>> reqs;
    std::vector<std::unique_ptr<affinity_proto::VertexInventoryResponse>> resps;
    cids.reserve(n_ranks);
    cntls.reserve(n_ranks);
    reqs.reserve(n_ranks);
    resps.reserve(n_ranks);

    for (int r = 0; r < n_ranks; ++r) {
        if (r == self_rank) continue;
        auto* chan = cs->GetComputeChannel(r);
        if (!chan) continue;
        auto cntl = std::unique_ptr<brpc::Controller>(new brpc::Controller);
        auto req  = std::unique_ptr<affinity_proto::VertexInventoryRequest>(
            new affinity_proto::VertexInventoryRequest);
        auto resp = std::unique_ptr<affinity_proto::VertexInventoryResponse>(
            new affinity_proto::VertexInventoryResponse);
        req->set_epoch(epoch);
        req->set_from_rank(self_rank);
        req->set_owned_tuples(reinterpret_cast<const char*>(owned.data()),
                              owned.size() * sizeof(uint64_t));

        cids.push_back(cntl->call_id());
        affinity_proto::AffinityService_Stub stub(chan);
        stub.PushVertexInventory(cntl.get(), req.get(), resp.get(),
                                 brpc::DoNothing());
        cntls.push_back(std::move(cntl));
        reqs.push_back(std::move(req));
        resps.push_back(std::move(resp));
    }
    for (auto cid : cids) brpc::Join(cid);
}

// One epoch end-to-end. Returns true if AssignmentTable was advanced.
bool DoOnePartition(ComputeServer* cs, uint32_t epoch, int uds_fd,
                    bool* uds_error) {
    if (uds_error) *uds_error = false;
    auto& coord = PartitionCoordinator::Instance();
    const int self_rank = cs->GetNodeID();
    const int n_ranks = ComputeNodeCount;
    const auto t0 = std::chrono::steady_clock::now();

    // 1. Take EdgeShuffler accumulator for this epoch.
    auto acc = EdgeShuffler::Instance().WaitAndTake(
        epoch, affinity_shuffle_barrier_ms);
    if (!acc) {
        stats.partition_skipped.fetch_add(1, std::memory_order_relaxed);
        coord.Drop(epoch);
        return false;
    }

    // 2. Build sorted owned-tuple list (this rank's vertices).
    std::vector<uint64_t> owned;
    owned.reserve(acc->owned_edges.size());
    for (const auto& kv : acc->owned_edges) owned.push_back(kv.first);
    std::sort(owned.begin(), owned.end());

    // 3. Allgather inventories — broadcast our owned list, wait for peers.
    BroadcastVertexInventory(cs, epoch, owned);
    if (!coord.WaitInventory(epoch, n_ranks, affinity_uds_recv_timeout_ms)) {
        stats.partition_skipped.fetch_add(1, std::memory_order_relaxed);
        coord.Drop(epoch);
        return false;
    }

    auto coord_state = coord.Get(epoch);
    auto& by_rank = coord_state->owned_tuples_by_rank;

    // 4. Build vtxdist[] and global tuple_id -> vid map.
    std::vector<affinity_uds::idx_t> vtxdist(n_ranks + 1, 0);
    for (int r = 0; r < n_ranks; ++r) {
        vtxdist[r + 1] = vtxdist[r] +
                         static_cast<affinity_uds::idx_t>(by_rank[r].size());
    }
    std::unordered_map<uint64_t, affinity_uds::idx_t> tuple_to_vid;
    tuple_to_vid.reserve(static_cast<size_t>(vtxdist[n_ranks]));
    for (int r = 0; r < n_ranks; ++r) {
        affinity_uds::idx_t base = vtxdist[r];
        for (size_t i = 0; i < by_rank[r].size(); ++i) {
            tuple_to_vid[by_rank[r][i]] =
                base + static_cast<affinity_uds::idx_t>(i);
        }
    }

    // 5. Build local CSR for owned vertices using global vids.
    const size_t nvtx_local = owned.size();
    stats.last_partition_owned_vertices.store(static_cast<uint64_t>(nvtx_local),
                                              std::memory_order_relaxed);
    std::vector<affinity_uds::idx_t> xadj;
    xadj.reserve(nvtx_local + 1);
    xadj.push_back(0);
    std::vector<affinity_uds::idx_t> adjncy;
    std::vector<affinity_uds::idx_t> adjwgt;
    std::vector<affinity_uds::idx_t> vwgt;   // load weight: total local accesses
    std::vector<affinity_uds::idx_t> vsize;  // migration cost proxy: same as vwgt for now
    vwgt.reserve(nvtx_local);
    vsize.reserve(nvtx_local);

    // prev_part[i] = last epoch's AssignmentTable decision for owned[i], with
    // the physical page owner as fallback for tuples the table has never
    // decided on (first epoch, or cold tuples dropped by a previous snapshot
    // replace). ParMETIS_V3_AdaptiveRepart is *incremental*: it treats
    // prev_part as "this is where things live now, nudge them" and uses `itr`
    // to trade off cut quality against movement. Feeding it the raw physical
    // page owner makes the migration lag visible every cycle — migration
    // hasn't finished, so prev_part disagrees with what we told the table
    // last round, and AdaptiveRepart re-solves from scratch, producing a new
    // (possibly unrelated) assignment. Result: AssignmentTable thrashes.
    // Using the prior assignment here means each epoch only refines the
    // previous decision, which is exactly the AdaptiveRepart contract.
    auto asn_snap = GetAssignmentTable().Current();
    const auto& asn_map = asn_snap->map;
    std::vector<affinity_uds::idx_t> prev_part;
    prev_part.reserve(nvtx_local);

    for (uint64_t u : owned) {
        // node_access for u (sum across nodes) = how often u was touched.
        affinity_uds::idx_t w = 1;
        auto na_it = acc->owned_node_access.find(u);
        if (na_it != acc->owned_node_access.end()) {
            uint64_t total = 0;
            for (const auto& kv : na_it->second) total += kv.second;
            // ParMETIS idx_t is int32 — clamp to avoid overflow on hot keys.
            w = static_cast<affinity_uds::idx_t>(
                std::min<uint64_t>(total, static_cast<uint64_t>(INT32_MAX)));
            if (w < 1) w = 1;
        }
        vwgt.push_back(w);
        // vsize is migration cost, not access frequency. Keeping it constant
        // lets ParMETIS move hot tuples when the edge cut benefit is large.
        vsize.push_back(1);

        int prev_node;
        auto asn_it = asn_map.find(u);
        if (asn_it != asn_map.end()) {
            prev_node = asn_it->second.node_id;
        } else {
            prev_node = resolve_physical_owner(cs, u, self_rank);
        }
        prev_part.push_back(static_cast<affinity_uds::idx_t>(prev_node));

        const auto& nbrs = acc->owned_edges.at(u);
        for (const auto& kv : nbrs) {
            const uint64_t v = kv.first;
            auto vid_it = tuple_to_vid.find(v);
            if (vid_it == tuple_to_vid.end()) {
                // v wasn't reported by any rank's inventory — should not happen
                // given the symmetric shuffle, but skip rather than crash.
                continue;
            }
            adjncy.push_back(vid_it->second);
            adjwgt.push_back(static_cast<affinity_uds::idx_t>(
                std::min<uint32_t>(kv.second, static_cast<uint32_t>(INT32_MAX))));
        }
        xadj.push_back(static_cast<affinity_uds::idx_t>(adjncy.size()));
    }

    // 6. Send request to local sidecar.
    affinity_uds::ReqHeader hdr{};
    hdr.magic         = affinity_uds::kReqMagic;
    hdr.epoch         = epoch;
    hdr.nparts        = n_ranks;
    hdr.ncon          = 1;
    hdr.nvtx_local    = static_cast<int32_t>(nvtx_local);
    hdr.vtxdist_len   = n_ranks + 1;
    hdr.xadj_len      = static_cast<int32_t>(xadj.size());
    hdr.adjncy_len    = static_cast<int32_t>(adjncy.size());
    hdr.has_vwgt      = 1;
    hdr.has_vsize     = 1;
    hdr.has_adjwgt    = 1;
    hdr.has_prev_part = 1;
    hdr.ubvec         = static_cast<float>(affinity_ubvec);
    hdr.itr           = static_cast<float>(affinity_repart_itr);

    if (!send_request(uds_fd, hdr, vtxdist, xadj, adjncy, vwgt, vsize, adjwgt, prev_part)) {
        if (uds_error) *uds_error = true;
        stats.partition_skipped.fetch_add(1, std::memory_order_relaxed);
        coord.Drop(epoch);
        return false;
    }

    // 7. Receive part_local[] from sidecar.
    affinity_uds::RespHeader rhdr{};
    std::vector<affinity_uds::idx_t> part_local;
    if (!recv_response(uds_fd, rhdr, part_local)) {
        if (uds_error) *uds_error = true;
        stats.partition_skipped.fetch_add(1, std::memory_order_relaxed);
        coord.Drop(epoch);
        return false;
    }
    if (rhdr.status != 0 ||
        static_cast<size_t>(rhdr.nvtx_local) != nvtx_local) {
        stats.partition_skipped.fetch_add(1, std::memory_order_relaxed);
        coord.Drop(epoch);
        return false;
    }
    stats.last_edgecut.store(static_cast<uint64_t>(rhdr.edgecut),
                             std::memory_order_relaxed);

    // 8. Convert local part_local[i] into (tuple_id -> node_id) for our owned
    //    vertices and broadcast to all peers.
    std::vector<int> nodes;
    nodes.reserve(nvtx_local);
    uint64_t changed_vertices = 0;
    for (size_t i = 0; i < nvtx_local; ++i) {
        if (part_local[i] != prev_part[i]) {
            ++changed_vertices;
        }
        nodes.push_back(static_cast<int>(part_local[i]));
    }
    stats.last_partition_changed_vertices.store(changed_vertices,
                                                std::memory_order_relaxed);
    BroadcastAssignmentSlice(cs, epoch, owned, nodes);

    // 9. Wait for all peers' slices to arrive, then atomic-swap into the table.
    if (!coord.WaitAssignment(epoch, n_ranks, affinity_uds_recv_timeout_ms)) {
        stats.partition_skipped.fetch_add(1, std::memory_order_relaxed);
        coord.Drop(epoch);
        return false;
    }
    auto cs2 = coord.Get(epoch);
    auto snap = std::make_shared<AssignmentTable::Snapshot>();
    {
        std::lock_guard<std::mutex> lk(cs2->mtx);
        uint64_t existing_vertices = 0;
        uint64_t changed_existing_vertices = 0;
        for (const auto& kv : cs2->pending_assignment) {
            auto it = asn_map.find(kv.first);
            if (it == asn_map.end()) continue;
            ++existing_vertices;
            if (it->second.node_id != kv.second) {
                ++changed_existing_vertices;
            }
        }

        PartitionAcceptanceInput acceptance{};
        acceptance.current_assignment_size = asn_map.size();
        acceptance.changed_vertices = changed_existing_vertices;
        acceptance.owned_vertices = existing_vertices;
        acceptance.max_changed_vertices_ratio =
            affinity_max_changed_vertices_ratio;
        const auto acceptance_decision =
            DecidePartitionAcceptance(acceptance);
        if (acceptance_decision.clip_changed_vertices) {
            stats.partition_rejected.fetch_add(1, std::memory_order_relaxed);
        }

        // Partial acceptance must pick the same changed tuples on every
        // compute node. Sort by tuple_id before applying the movement budget;
        // unordered_map iteration order would let replicas diverge.
        std::vector<std::pair<uint64_t, int>> pending_sorted;
        pending_sorted.reserve(cs2->pending_assignment.size());
        for (const auto& kv : cs2->pending_assignment) {
            pending_sorted.emplace_back(kv.first, kv.second);
        }
        std::sort(pending_sorted.begin(), pending_sorted.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return lhs.first < rhs.first;
                  });

        snap->map.reserve(pending_sorted.size());
        uint64_t accepted_changed_existing_vertices = 0;
        for (const auto& kv : pending_sorted) {
            auto it = asn_map.find(kv.first);
            const bool changed_existing =
                it != asn_map.end() && it->second.node_id != kv.second;
            if (changed_existing &&
                !ShouldAcceptChangedVertex(
                    acceptance_decision,
                    accepted_changed_existing_vertices)) {
                continue;
            }
            if (changed_existing) {
                ++accepted_changed_existing_vertices;
            }
            snap->map.emplace(kv.first,
                              AssignmentTable::Entry{kv.second, epoch});
        }
        cs2->pending_assignment.clear();
        snap->version = epoch;
    }
    GetAssignmentTable().Merge(
        snap, static_cast<uint64_t>(affinity_assignment_ttl_epochs));
    // last_assignment_size now reports the CUMULATIVE table size (post-merge)
    // rather than this epoch's delta — the delta is already exposed via
    // last_partition_owned_vertices / last_partition_changed_vertices. This
    // is what timeseries plots "is the assignment growing toward its
    // steady-state footprint?" versus the old reading which just echoed the
    // aggregator's recent-activity window and looked like thrashing.
    stats.last_assignment_size.store(
        static_cast<uint64_t>(GetAssignmentTable().Size()),
        std::memory_order_relaxed);

    coord.Drop(epoch);

    const auto t1 = std::chrono::steady_clock::now();
    stats.partition_runs.fetch_add(1, std::memory_order_relaxed);
    stats.partition_total_ms.fetch_add(
        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count(),
        std::memory_order_relaxed);
    return true;
}

}  // namespace

static std::atomic<bool> g_part_stop{false};
void RequestPartitionerStop() {
    g_part_stop.store(true);
    PartitionCoordinator::Instance().Stop();
}

void PartitionerLoop(ComputeServer* cs) {
    if (!enable_affinity) return;
    if (ComputeNodeCount < 2) {
        std::fprintf(stderr,
                     "[affinity] ComputeNodeCount=%d, partitioner is a no-op.\n",
                     ComputeNodeCount);
        return;
    }

    int uds_fd = -1;
    const std::string uds_path = resolve_uds_path(
        affinity_sidecar_uds_path, cs ? cs->GetNodeID() : -1, ComputeNodeCount);
    uint32_t epoch = 1;

    while (!g_part_stop.load(std::memory_order_relaxed)) {
        int remaining_ms = affinity_partition_cycle_ms;
        while (remaining_ms > 0 && !g_part_stop.load(std::memory_order_relaxed)) {
            const int step_ms = std::min(remaining_ms, 100);
            std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
            remaining_ms -= step_ms;
        }
        if (g_part_stop.load(std::memory_order_relaxed)) break;

        if (uds_fd < 0) {
            uds_fd = connect_uds(uds_path);
            if (uds_fd < 0) {
                std::fprintf(stderr,
                             "[affinity] sidecar UDS connect to %s failed: %s\n",
                             uds_path.c_str(),
                             std::strerror(errno));
                continue;
            }
            std::fprintf(stderr, "[affinity] connected to sidecar at %s\n",
                         uds_path.c_str());
        }

        bool uds_error = false;
        const bool ok = DoOnePartition(cs, epoch, uds_fd, &uds_error);
        if (!ok && uds_error) {
            // Only reconnect after actual UDS IO failure. Peer/barrier/assignment
            // timeouts happen before or after a valid sidecar request and should
            // not tear down MPI ranks that are still in the collective loop.
            ::close(uds_fd);
            uds_fd = -1;
        }
        ++epoch;
    }

    if (uds_fd >= 0) ::close(uds_fd);
}

}  // namespace affinity
