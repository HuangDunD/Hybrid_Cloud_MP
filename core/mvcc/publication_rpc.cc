#include "mvcc/publication_rpc.h"
#include <algorithm>
#include <brpc/controller.h>
#include <brpc/closure_guard.h>
#include <butil/logging.h>

namespace mvcc {
namespace {
constexpr uint32_t kProtocolVersion = 2;
constexpr size_t kMaxEnvelopesPerRpc = 64;
void Encode(const Context& c, mvcc_wire::Context* wire) {
    wire->set_epoch(c.id.epoch);
    wire->set_home(c.id.home);
    wire->set_local_id(c.id.local);
    wire->set_snapshot_scn(c.snapshot);
}
Context Decode(const mvcc_wire::Context& wire) {
    CHECK(wire.IsInitialized());
    return {{wire.epoch(), wire.home(), wire.local_id()}, wire.snapshot_scn()};
}
mvcc_wire::Publication Encode(const Message& m) {
    mvcc_wire::Publication wire;
    wire.set_protocol_version(kProtocolVersion);
    wire.set_node(m.node);
    switch (m.phase) {
        case Phase::Pending: wire.set_phase(mvcc_wire::Publication::PENDING); break;
        case Phase::Stage: wire.set_phase(mvcc_wire::Publication::STAGE); break;
        case Phase::Advance: wire.set_phase(mvcc_wire::Publication::ADVANCE); break;
    }
    Encode(m.resolution.context, wire.mutable_context());
    wire.set_slot(m.resolution.slot);
    wire.set_committed(m.resolution.committed);
    return wire;
}
Message Decode(const mvcc_wire::Publication& wire) {
    CHECK(wire.IsInitialized() && wire.protocol_version() == kProtocolVersion);
    Phase phase;
    switch (wire.phase()) {
        case mvcc_wire::Publication::PENDING: phase = Phase::Pending; break;
        case mvcc_wire::Publication::STAGE: phase = Phase::Stage; break;
        case mvcc_wire::Publication::ADVANCE: phase = Phase::Advance; break;
        default: LOG(FATAL) << "unknown publication phase"; return {};
    }
    return {wire.node(), phase,
            {Decode(wire.context()), wire.slot(), wire.committed()}};
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
    reply.set_commit_scn(status ? status->commit_scn : 0);
    return reply;
}
uint32_t Encode(CommitKind kind) {
    switch (kind) {
        case CommitKind::SingleWriter: return 0;
        case CommitKind::TwoPhase: return 1;
        case CommitKind::Cornus: return 2;
    }
    LOG(FATAL) << "invalid commit kind on the wire";
    return 0;
}
CommitKind DecodeKind(uint32_t kind) {
    CHECK(kind <= 2) << "invalid commit kind on the wire";
    return static_cast<CommitKind>(kind);
}
void EncodeEvidence(const std::map<NodeId, uint64_t>& evidence,
                    google::protobuf::RepeatedPtrField<mvcc_wire::NodeEvidence>* wire) {
    for (const auto& entry : evidence) {
        auto* item = wire->Add();
        item->set_node(entry.first);
        item->set_lsn(entry.second);
    }
}
std::map<NodeId, uint64_t> DecodeEvidence(
        const google::protobuf::RepeatedPtrField<mvcc_wire::NodeEvidence>& wire) {
    std::map<NodeId, uint64_t> evidence;
    for (const auto& item : wire) {
        CHECK(item.IsInitialized() && item.lsn() > 0);
        CHECK(evidence.emplace(item.node(), item.lsn()).second) << "duplicate evidence node";
    }
    return evidence;
}
std::optional<Status> Decode(const mvcc_wire::StatusReply& reply) {
    CHECK(reply.IsInitialized());
    if (!reply.found()) {
        CHECK(reply.state() == 0 && reply.commit_scn() == 0);
        return std::nullopt;
    }
    const auto raw = reply.state();
    CHECK(raw <= static_cast<uint32_t>(State::ReadOnlyFinished)) << "invalid transaction state on the wire";
    Status status{static_cast<State>(raw), reply.commit_scn()};
    CHECK((status.state == State::Committed) == (status.commit_scn != 0));
    return status;
}
}  // namespace

void PublicationService::PublishBatch(google::protobuf::RpcController*,
                                      const mvcc_wire::PublicationBatch* request,
                                      mvcc_wire::PublicationBatch* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    CHECK(request->entries_size() > 0 && request->entries_size() <= kMaxEnvelopesPerRpc);
    for (const auto& entry : request->entries()) {
        node_.Receive(Decode(entry));
        *response->add_entries() = entry;
    }
}
void PublicationService::Publish(google::protobuf::RpcController*, const mvcc_wire::Publication* request,
                                mvcc_wire::Publication* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    const Ack ack = node_.Receive(Decode(*request));
    *response = Encode(ack); // emitted only after local installation/frontier update
}
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
void PublicationService::ReserveLazy(google::protobuf::RpcController*, const mvcc_wire::LazyReservation* request,
                                     mvcc_wire::ReservedSlot* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    CHECK(request->IsInitialized() && request->protocol_version() == kProtocolVersion);
    response->set_slot(host_.ReserveLazy(Decode(request->context()), request->target_lsn()));
}
void PublicationService::Commit(google::protobuf::RpcController*, const mvcc_wire::CommitRequest* request,
                               mvcc_wire::CommitReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    CHECK(request->IsInitialized() && request->protocol_version() == kProtocolVersion);
    const auto slot = host_.SubmitCommit(Decode(request->context()), request->durable_lsn(),
                                         request->installed());
    CHECK(slot > request->context().snapshot_scn());
    response->set_slot(slot);
    response->set_committed(true);
}
void PublicationService::SubmitTwoPhase(google::protobuf::RpcController*,
                                        const mvcc_wire::TwoPhaseRequest* request,
                                        mvcc_wire::TwoPhaseReply* response,
                                        google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    CHECK(request->IsInitialized() && request->protocol_version() == kProtocolVersion);
    std::set<NodeId> writers(request->writers().begin(), request->writers().end());
    std::set<NodeId> installed(request->installed().begin(), request->installed().end());
    std::optional<WalPosition> decision;
    if (request->has_decision()) {
        CHECK(request->decision().IsInitialized());
        decision = WalPosition{request->context().epoch(), request->decision().node(),
                               request->decision().lsn()};
    }
    const auto context = Decode(request->context());
    const auto slot = host_.SubmitTwoPhase(context, DecodeKind(request->kind()), writers,
        DecodeEvidence(request->wal()), DecodeEvidence(request->prepare()), decision,
        request->prepared_quorum(), installed);
    CHECK(slot > context.snapshot);
    response->set_slot(slot);
    response->set_committed(true);
}
void PublicationService::ReserveTwoPhase(google::protobuf::RpcController*,
                                         const mvcc_wire::TwoPhaseRequest* request,
                                         mvcc_wire::TwoPhaseReply* response,
                                         google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    CHECK(request->IsInitialized() && request->protocol_version() == kProtocolVersion);
    std::set<NodeId> writers(request->writers().begin(), request->writers().end());
    std::optional<WalPosition> decision;
    if (request->has_decision()) {
        CHECK(request->decision().IsInitialized());
        decision = WalPosition{request->context().epoch(), request->decision().node(),
                               request->decision().lsn()};
    }
    const auto context = Decode(request->context());
    const auto slot = host_.ReserveTwoPhase(context, DecodeKind(request->kind()), writers,
        DecodeEvidence(request->wal()), DecodeEvidence(request->prepare()), decision,
        request->prepared_quorum());
    CHECK(slot > context.snapshot);
    response->set_slot(slot);
    response->set_committed(false); // the decision is made; publication is not complete yet
}
void PublicationService::CompleteTwoPhase(google::protobuf::RpcController*,
                                          const mvcc_wire::TwoPhaseCompleteRequest* request,
                                          mvcc_wire::TwoPhaseReply* response,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    CHECK(request->IsInitialized() && request->protocol_version() == kProtocolVersion);
    const auto context = Decode(request->context());
    std::set<NodeId> installed(request->installed().begin(), request->installed().end());
    const auto slot = host_.CompleteTwoPhase(context, installed);
    CHECK(slot > context.snapshot);
    response->set_slot(slot);
    response->set_committed(true);
}
void PublicationService::QueryStatus(google::protobuf::RpcController*, const mvcc_wire::StatusRequest* request,
                                    mvcc_wire::StatusReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    CHECK(request->IsInitialized() && request->protocol_version() == kProtocolVersion);
    CHECK(request->epoch().size() == 16 && request->local_id() != 0);
    *response = Encode(host_.LookupPublication({request->epoch(), request->home(), request->local_id()}));
}
Ack SendPublication(brpc::Channel& channel, const Message& message) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    auto request = Encode(message);
    mvcc_wire::Publication response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.Publish(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed()) << "MVCC publication RPC failed; decision cannot be aborted: " << controller.ErrorText();
    auto ack = Decode(response);
    CHECK(ack.node == message.node && ack.phase == message.phase && ack.resolution == message.resolution);
    return ack;
}
void SendPublications(brpc::Channel& channel, const std::vector<Message>& messages) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    for (size_t first = 0; first < messages.size();) {
        const size_t count = std::min(kMaxEnvelopesPerRpc, messages.size() - first);
        mvcc_wire::PublicationBatch request, response;
        for (size_t i = 0; i < count; ++i)
            *request.add_entries() = Encode(messages[first + i]);
        brpc::Controller controller;
        controller.set_timeout_ms(0x7fffffff);
        stub.PublishBatch(&controller, &request, &response, nullptr);
        CHECK(!controller.Failed()) << "MVCC publication RPC failed: " << controller.ErrorText();
        CHECK_EQ(response.entries_size(), count);
        for (size_t i = 0; i < count; ++i) {
            const auto ack = Decode(response.entries(i));
            const auto& sent = messages[first + i];
            CHECK(ack.node == sent.node && ack.phase == sent.phase && ack.resolution == sent.resolution);
        }
        first += count;
    }
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
Scn SendReserveLazy(brpc::Channel& channel, const Context& context, uint64_t target_lsn) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    mvcc_wire::LazyReservation request;
    request.set_protocol_version(kProtocolVersion);
    Encode(context, request.mutable_context());
    request.set_target_lsn(target_lsn);
    mvcc_wire::ReservedSlot response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.ReserveLazy(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized()) << controller.ErrorText();
    CHECK(response.slot() > context.snapshot);
    return response.slot();
}
Scn SendCommit(brpc::Channel& channel, const Context& context, uint64_t durable_lsn, bool installed) {
    CHECK(durable_lsn > 0 && installed);
    mvcc_wire::PublicationService_Stub stub(&channel);
    mvcc_wire::CommitRequest request;
    request.set_protocol_version(kProtocolVersion);
    Encode(context, request.mutable_context());
    request.set_durable_lsn(durable_lsn);
    request.set_installed(installed);
    mvcc_wire::CommitReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.Commit(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized()) << "MVCC Commit RPC failed: " << controller.ErrorText();
    CHECK(response.committed() && response.slot() > context.snapshot)
        << "coordinator returned an unusable commit position";
    return response.slot();
}
namespace {
void FillTwoPhase(const Context& context, CommitKind kind, const std::set<NodeId>& writers,
                  const std::map<NodeId, uint64_t>& wal, const std::map<NodeId, uint64_t>& prepare,
                  std::optional<WalPosition> decision, bool prepared_quorum,
                  mvcc_wire::TwoPhaseRequest* request) {
    CHECK(!writers.empty());
    request->set_protocol_version(kProtocolVersion);
    Encode(context, request->mutable_context());
    request->set_kind(Encode(kind));
    for (NodeId writer : writers) request->add_writers(writer);
    EncodeEvidence(wal, request->mutable_wal());
    EncodeEvidence(prepare, request->mutable_prepare());
    if (decision) {
        CHECK(decision->epoch == context.id.epoch && decision->lsn > 0);
        request->mutable_decision()->set_node(decision->writer);
        request->mutable_decision()->set_lsn(decision->lsn);
    }
    request->set_prepared_quorum(prepared_quorum);
}
}  // namespace
Scn SendTwoPhaseCommit(brpc::Channel& channel, const Context& context, CommitKind kind,
                       const std::set<NodeId>& writers, const std::map<NodeId, uint64_t>& wal,
                       const std::map<NodeId, uint64_t>& prepare,
                       std::optional<WalPosition> decision, bool prepared_quorum,
                       const std::set<NodeId>& installed) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    mvcc_wire::TwoPhaseRequest request;
    FillTwoPhase(context, kind, writers, wal, prepare, decision, prepared_quorum, &request);
    for (NodeId writer : installed) request.add_installed(writer);
    mvcc_wire::TwoPhaseReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.SubmitTwoPhase(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized())
        << "MVCC 2PC commit RPC failed: " << controller.ErrorText();
    CHECK(response.committed() && response.slot() > context.snapshot)
        << "coordinator returned an unusable 2PC commit position";
    return response.slot();
}
Scn SendReserveTwoPhase(brpc::Channel& channel, const Context& context, CommitKind kind,
                        const std::set<NodeId>& writers, const std::map<NodeId, uint64_t>& wal,
                        const std::map<NodeId, uint64_t>& prepare,
                        std::optional<WalPosition> decision, bool prepared_quorum) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    mvcc_wire::TwoPhaseRequest request;
    FillTwoPhase(context, kind, writers, wal, prepare, decision, prepared_quorum, &request);
    mvcc_wire::TwoPhaseReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.ReserveTwoPhase(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized())
        << "MVCC 2PC reserve RPC failed: " << controller.ErrorText();
    CHECK(!response.committed() && response.slot() > context.snapshot)
        << "coordinator did not reserve a 2PC position";
    return response.slot();
}
Scn SendCompleteTwoPhase(brpc::Channel& channel, const Context& context,
                         const std::set<NodeId>& installed) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    mvcc_wire::TwoPhaseCompleteRequest request;
    request.set_protocol_version(kProtocolVersion);
    Encode(context, request.mutable_context());
    for (NodeId writer : installed) request.add_installed(writer);
    mvcc_wire::TwoPhaseReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.CompleteTwoPhase(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized())
        << "MVCC 2PC complete RPC failed: " << controller.ErrorText();
    CHECK(response.committed() && response.slot() > context.snapshot)
        << "coordinator did not publish the 2PC commit";
    return response.slot();
}
std::optional<Status> SendStatusQuery(brpc::Channel& channel, const TxnId& id) {
    mvcc_wire::PublicationService_Stub stub(&channel);
    mvcc_wire::StatusRequest request;
    request.set_protocol_version(kProtocolVersion);
    request.set_epoch(id.epoch);
    request.set_home(id.home);
    request.set_local_id(id.local);
    mvcc_wire::StatusReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(0x7fffffff);
    stub.QueryStatus(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed() && response.IsInitialized()) << "MVCC status RPC failed: " << controller.ErrorText();
    return Decode(response);
}
} // namespace mvcc
