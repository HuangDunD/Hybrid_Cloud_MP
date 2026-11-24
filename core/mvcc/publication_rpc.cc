#include "mvcc/publication_rpc.h"
#include <brpc/controller.h>
#include <brpc/closure_guard.h>
#include <butil/logging.h>

namespace mvcc {
namespace {
constexpr uint32_t kProtocolVersion = 5;
void Encode(const Context& c, mvcc_wire::Context* wire) {
    wire->set_epoch(c.id.epoch);
    wire->set_home(c.id.home);
    wire->set_local_id(c.id.local);
    wire->set_snapshot_ts(c.snapshot);
}
Context Decode(const mvcc_wire::Context& wire) {
    CHECK(wire.IsInitialized());
    return {{wire.epoch(), wire.home(), wire.local_id()}, wire.snapshot_ts()};
}
mvcc_wire::ContextRequest Request(const Context& c) {
    mvcc_wire::ContextRequest r;
    r.set_protocol_version(kProtocolVersion);
    Encode(c, r.mutable_context());
    return r;
}
Context Decode(const mvcc_wire::ContextRequest& r) {
    CHECK(r.IsInitialized() && r.protocol_version() == kProtocolVersion);
    return Decode(r.context());
}
mvcc_wire::StatusReply Encode(const std::optional<Status>& status) {
    mvcc_wire::StatusReply reply;
    reply.set_found(status.has_value());
    reply.set_state(status ? static_cast<uint32_t>(status->state) : 0);
    reply.set_commit_ts(status ? status->commit_ts : 0);
    return reply;
}
std::optional<Status> Decode(const mvcc_wire::StatusReply& reply) {
    CHECK(reply.IsInitialized());
    if (!reply.found()) {
        CHECK(reply.state() == 0 && reply.commit_ts() == 0);
        return std::nullopt;
    }
    const auto raw = reply.state();
    CHECK(raw <= static_cast<uint32_t>(State::ReadOnlyFinished)) << "invalid transaction state on the wire";
    Status status{static_cast<State>(raw), reply.commit_ts()};
    CHECK((status.state == State::Committed) == (status.commit_ts != 0));
    return status;
}
}  // namespace

void PublicationService::Attach(google::protobuf::RpcController*, const mvcc_wire::ContextRequest* request,
                               mvcc_wire::ContextReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    const bool active = node_.Attach(Decode(*request));
    response->set_active(active);
}
void PublicationService::Finish(google::protobuf::RpcController*, const mvcc_wire::ContextRequest* request,
                               mvcc_wire::ContextReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    node_.Finish(Decode(*request));
    response->set_active(false);
}
void PublicationService::ReadCreatorAt(google::protobuf::RpcController*, const mvcc_wire::CreatorAtRequest* request,
                                      mvcc_wire::StatusReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    CHECK(request->IsInitialized() && request->creator().protocol_version() == kProtocolVersion);
    const auto& id = request->creator();
    *response = Encode(node_.ReadCreatorAt({id.epoch(), id.home(), id.local_id()}, request->snapshot()));
}
void PublicationService::QueryStatus(google::protobuf::RpcController*, const mvcc_wire::StatusRequest* request,
                                    mvcc_wire::StatusReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    CHECK(request->IsInitialized() && request->protocol_version() == kProtocolVersion);
    CHECK(request->epoch().size() == 16 && request->local_id() != 0);
    *response = Encode(host_.LookupStatus({request->epoch(), request->home(), request->local_id()}));
}
bool SendAttach(brpc::Channel& channel, const Context& context) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    auto request = Request(context);
    mvcc_wire::ContextReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.Attach(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized()) << controller.ErrorText();
    return response.active();
}
void SendFinish(brpc::Channel& channel, const Context& context) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    auto request = Request(context);
    mvcc_wire::ContextReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.Finish(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized() && !response.active()) << controller.ErrorText();
}
namespace {
mvcc_wire::StatusRequest StatusRequest(const TxnId& id) {
    mvcc_wire::StatusRequest request;
    request.set_protocol_version(kProtocolVersion);
    request.set_epoch(id.epoch);
    request.set_home(id.home);
    request.set_local_id(id.local);
    return request;
}
}
std::optional<Status> SendCreatorAt(brpc::Channel& channel, const TxnId& id, Timestamp snapshot) {
    mvcc_wire::CreatorAtRequest request;
    *request.mutable_creator() = StatusRequest(id);
    request.set_snapshot(snapshot);
    mvcc_wire::StatusReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    mvcc_wire::PublicationService_Stub(&channel).ReadCreatorAt(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized()) << "MVCC creator RPC failed: " << controller.ErrorText();
    return Decode(response);
}
std::optional<Status> SendStatusQuery(brpc::Channel& channel, const TxnId& id) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    const auto request = StatusRequest(id);
    mvcc_wire::StatusReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.QueryStatus(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized()) << "MVCC status RPC failed: " << controller.ErrorText();
    return Decode(response);
}
} // namespace mvcc
