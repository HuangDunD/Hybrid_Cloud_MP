#include "affinity_service.h"

#include <brpc/controller.h>

#include "edge_shuffler.h"
#include "partitioner.h"

namespace affinity {

void AffinityServiceImpl::ShuffleEdges(
    google::protobuf::RpcController* /*cntl*/,
    const affinity_proto::ShuffleEdgesRequest* req,
    affinity_proto::ShuffleEdgesResponse* resp,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    const size_t n = req->us_size();
    // The proto's repeated packed fields decode into RepeatedField<>; we hand
    // the underlying contiguous array to EdgeShuffler to avoid an extra copy.
    const uint64_t* us = n > 0 ? req->us().data() : nullptr;
    const uint64_t* vs = n > 0 ? req->vs().data() : nullptr;
    const uint32_t* ws = n > 0 ? req->ws().data() : nullptr;

    const size_t na = req->node_access_tuples_size();
    const uint64_t* na_t = na > 0 ? req->node_access_tuples().data() : nullptr;
    const int32_t*  na_n = na > 0 ? req->node_access_nodes().data() : nullptr;
    const uint32_t* na_c = na > 0 ? req->node_access_counts().data() : nullptr;

    EdgeShuffler::Instance().OnShuffleEdges(
        req->epoch(), req->from_rank(), us, vs, ws, n,
        na_t, na_n, na_c, na, req->final());

    resp->set_status(0);
}

void AffinityServiceImpl::Barrier(
    google::protobuf::RpcController* /*cntl*/,
    const affinity_proto::BarrierRequest* req,
    affinity_proto::BarrierResponse* resp,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    EdgeShuffler::Instance().OnBarrier(req->epoch(), req->from_rank());
    resp->set_status(0);
}

void AffinityServiceImpl::PushAssignmentSlice(
    google::protobuf::RpcController* /*cntl*/,
    const affinity_proto::AssignmentSliceRequest* req,
    affinity_proto::AssignmentSliceResponse* resp,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // tuple_ids/node_ids/migration_priorities are raw arrays packed as bytes
    // — see proto comments. Cast string storage back to arrays.
    const std::string& t_blob = req->tuple_ids();
    const std::string& n_blob = req->node_ids();
    const std::string& p_blob = req->migration_priorities();
    const size_t n = t_blob.size() / sizeof(uint64_t);
    const uint64_t* tuples =
        n > 0 ? reinterpret_cast<const uint64_t*>(t_blob.data()) : nullptr;
    const int32_t* nodes =
        n > 0 ? reinterpret_cast<const int32_t*>(n_blob.data()) : nullptr;
    const uint32_t* priorities =
        (!p_blob.empty() && n > 0)
            ? reinterpret_cast<const uint32_t*>(p_blob.data())
            : nullptr;
    // Sanity: tuples and nodes must be same count; otherwise drop silently.
    if ((tuples && nodes && n_blob.size() / sizeof(int32_t) != n) ||
        (priorities && p_blob.size() / sizeof(uint32_t) != n)) {
        resp->set_status(-1);
        return;
    }
    PartitionCoordinator::Instance().OnAssignmentSlice(
        req->epoch(), req->from_rank(), tuples, nodes, priorities, n,
        req->final());
    resp->set_status(0);
}

void AffinityServiceImpl::PushVertexInventory(
    google::protobuf::RpcController* /*cntl*/,
    const affinity_proto::VertexInventoryRequest* req,
    affinity_proto::VertexInventoryResponse* resp,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string& blob = req->owned_tuples();
    const size_t n = blob.size() / sizeof(uint64_t);
    const uint64_t* owned =
        n > 0 ? reinterpret_cast<const uint64_t*>(blob.data()) : nullptr;
    PartitionCoordinator::Instance().OnInventory(
        req->epoch(), req->from_rank(), owned, n);
    resp->set_status(0);
}

}  // namespace affinity
