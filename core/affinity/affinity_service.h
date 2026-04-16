// brpc service impl for affinity edge shuffling, barrier, and assignment broadcast.
// One instance per compute_server, registered via brpc::Server::AddService alongside
// the existing storage / compute / 2PC services. Phase 4 wires this into
// compute_server bootstrap.
#pragma once

#include "affinity_service.pb.h"

namespace affinity {

class AffinityServiceImpl : public affinity_proto::AffinityService {
public:
    AffinityServiceImpl() = default;

    void ShuffleEdges(google::protobuf::RpcController* cntl,
                      const affinity_proto::ShuffleEdgesRequest* req,
                      affinity_proto::ShuffleEdgesResponse* resp,
                      google::protobuf::Closure* done) override;

    void Barrier(google::protobuf::RpcController* cntl,
                 const affinity_proto::BarrierRequest* req,
                 affinity_proto::BarrierResponse* resp,
                 google::protobuf::Closure* done) override;

    void PushAssignmentSlice(google::protobuf::RpcController* cntl,
                             const affinity_proto::AssignmentSliceRequest* req,
                             affinity_proto::AssignmentSliceResponse* resp,
                             google::protobuf::Closure* done) override;

    void PushVertexInventory(google::protobuf::RpcController* cntl,
                             const affinity_proto::VertexInventoryRequest* req,
                             affinity_proto::VertexInventoryResponse* resp,
                             google::protobuf::Closure* done) override;
};

}  // namespace affinity
