#pragma once
#include <optional>
#include <brpc/channel.h>
#include "mvcc/protocol.h"
#include "mvcc/publication.pb.h"

namespace mvcc {

// Implemented by LazyRuntime. The coordinator service is the only caller of
// SubmitCommit/SubmitTwoPhase; QueryStatus is answered by the creator's home.
class PublicationHost {
public:
    virtual ~PublicationHost() = default;
    virtual Scn ClockCommit(const Context&, uint64_t target_lsn) = 0;
    virtual Scn ReportWal(const WalPosition&) = 0;
    virtual Scn ReadCertified(const std::string& epoch) const = 0;
    virtual Scn ReserveLazy(const Context&, uint64_t target_lsn) = 0;
    virtual Scn SubmitCommit(const Context&, uint64_t durable_lsn, bool installed) = 0;
    // Reserved for a transaction whose evidence was collected by a remote home.
    // The coordinator reserves the position, installs the terminal state after
    // every proof arrives and returns only after both ACK rounds completed.
    virtual Scn SubmitTwoPhase(const Context&, CommitKind kind, const std::set<NodeId>& writers,
                               const std::map<NodeId, uint64_t>& wal,
                               const std::map<NodeId, uint64_t>& prepare,
                               std::optional<WalPosition> decision, bool prepared_quorum,
                               const std::set<NodeId>& installed) = 0;
    virtual Scn ReserveTwoPhase(const Context&, CommitKind kind, const std::set<NodeId>& writers,
                                const std::map<NodeId, uint64_t>& wal,
                                const std::map<NodeId, uint64_t>& prepare,
                                std::optional<WalPosition> decision, bool prepared_quorum) = 0;
    virtual Scn CompleteTwoPhase(const Context&, const std::set<NodeId>& installed) = 0;
    virtual std::optional<Status> LookupPublication(const TxnId&) const = 0;
};

class PublicationService final : public mvcc_wire::PublicationService {
public:
    PublicationService(SnapshotNode& node, PublicationHost& host) : node_(node), host_(host) {}
    void PublishBatch(google::protobuf::RpcController*, const mvcc_wire::PublicationBatch*,
                      mvcc_wire::PublicationBatch*, google::protobuf::Closure*) override;
    void Publish(google::protobuf::RpcController*, const mvcc_wire::Publication*,
                 mvcc_wire::Publication*, google::protobuf::Closure*) override;
    void Attach(google::protobuf::RpcController*, const mvcc_wire::ContextRequest*,
                mvcc_wire::ContextReply*, google::protobuf::Closure*) override;
    void Finish(google::protobuf::RpcController*, const mvcc_wire::ContextRequest*,
                mvcc_wire::ContextReply*, google::protobuf::Closure*) override;
    void ClockCommit(google::protobuf::RpcController*, const mvcc_wire::LazyReservation*,
                     mvcc_wire::ReservedSlot*, google::protobuf::Closure*) override;
    void ReportWal(google::protobuf::RpcController*, const mvcc_wire::DurableWriter*,
                   mvcc_wire::ReservedSlot*, google::protobuf::Closure*) override;
    void ReadCertified(google::protobuf::RpcController*, const mvcc_wire::ClockProgress*,
                       mvcc_wire::ReservedSlot*, google::protobuf::Closure*) override;
    void ReserveLazy(google::protobuf::RpcController*, const mvcc_wire::LazyReservation*,
                     mvcc_wire::ReservedSlot*, google::protobuf::Closure*) override;
    void Commit(google::protobuf::RpcController*, const mvcc_wire::CommitRequest*,
                mvcc_wire::CommitReply*, google::protobuf::Closure*) override;
    void SubmitTwoPhase(google::protobuf::RpcController*, const mvcc_wire::TwoPhaseRequest*,
                        mvcc_wire::TwoPhaseReply*, google::protobuf::Closure*) override;
    void ReserveTwoPhase(google::protobuf::RpcController*, const mvcc_wire::TwoPhaseRequest*,
                         mvcc_wire::TwoPhaseReply*, google::protobuf::Closure*) override;
    void CompleteTwoPhase(google::protobuf::RpcController*,
                          const mvcc_wire::TwoPhaseCompleteRequest*,
                          mvcc_wire::TwoPhaseReply*, google::protobuf::Closure*) override;
    void QueryStatus(google::protobuf::RpcController*, const mvcc_wire::StatusRequest*,
                     mvcc_wire::StatusReply*, google::protobuf::Closure*) override;
private:
    SnapshotNode& node_;
    PublicationHost& host_;
};
struct ClockCall {
    brpc::Controller controller;
    mvcc_wire::ReservedSlot reply;
    brpc::CallId call_id;
    Scn local_slot = 0;
};
void StartClockRpc(brpc::Channel& channel, const Context& context, uint64_t target, ClockCall& call);
Scn ReadCertifiedRpc(brpc::Channel& channel, const std::string& epoch);
Scn ReportWalRpc(brpc::Channel& channel, const WalPosition& durable);
Ack SendPublication(brpc::Channel& channel, const Message& message);
void SendPublications(brpc::Channel& channel, const std::vector<Message>& messages);
bool SendAttach(brpc::Channel& channel, const Context& context);
void SendFinish(brpc::Channel& channel, const Context& context);
// The call returns only after the coordinator has collected both ACK rounds;
// a timeout or missing ACK is a commit failure, never an implicit success.
Scn SendReserveLazy(brpc::Channel& channel, const Context& context, uint64_t target_lsn);
Scn SendCommit(brpc::Channel& channel, const Context& context, uint64_t durable_lsn, bool installed);
Scn SendTwoPhaseCommit(brpc::Channel& channel, const Context& context, CommitKind kind,
                       const std::set<NodeId>& writers, const std::map<NodeId, uint64_t>& wal,
                       const std::map<NodeId, uint64_t>& prepare,
                       std::optional<WalPosition> decision, bool prepared_quorum,
                       const std::set<NodeId>& installed);
Scn SendReserveTwoPhase(brpc::Channel& channel, const Context& context, CommitKind kind,
                        const std::set<NodeId>& writers, const std::map<NodeId, uint64_t>& wal,
                        const std::map<NodeId, uint64_t>& prepare,
                        std::optional<WalPosition> decision, bool prepared_quorum);
Scn SendCompleteTwoPhase(brpc::Channel& channel, const Context& context,
                         const std::set<NodeId>& installed);
std::optional<Status> SendStatusQuery(brpc::Channel& channel, const TxnId& id);
} // namespace mvcc
