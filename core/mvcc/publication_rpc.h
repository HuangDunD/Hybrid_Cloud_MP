#pragma once
#include <optional>
#include <brpc/channel.h>
#include "mvcc/protocol.h"
#include "mvcc/publication.pb.h"

namespace mvcc {

class PublicationHost {
public:
    virtual ~PublicationHost() = default;
    virtual std::optional<Status> LookupStatus(const TxnId&) const = 0;
};

class PublicationService final : public mvcc_wire::PublicationService {
public:
    PublicationService(SnapshotNode& node, PublicationHost& host) : node_(node), host_(host) {}
    void Attach(google::protobuf::RpcController*, const mvcc_wire::ContextRequest*,
                mvcc_wire::ContextReply*, google::protobuf::Closure*) override;
    void Finish(google::protobuf::RpcController*, const mvcc_wire::ContextRequest*,
                mvcc_wire::ContextReply*, google::protobuf::Closure*) override;
    void ReadCreatorAt(google::protobuf::RpcController*, const mvcc_wire::CreatorAtRequest*,
                       mvcc_wire::StatusReply*, google::protobuf::Closure*) override;
    void QueryStatus(google::protobuf::RpcController*, const mvcc_wire::StatusRequest*,
                     mvcc_wire::StatusReply*, google::protobuf::Closure*) override;
private:
    SnapshotNode& node_;
    PublicationHost& host_;
};
bool SendAttach(brpc::Channel& channel, const Context& context);
void SendFinish(brpc::Channel& channel, const Context& context);
std::optional<Status> SendCreatorAt(brpc::Channel&, const TxnId&, Timestamp snapshot);
std::optional<Status> SendStatusQuery(brpc::Channel& channel, const TxnId& id);
} // namespace mvcc
