#pragma once
#include <atomic>
#include <functional>
#include <set>
#include <thread>
#include "mvcc/protocol.h"
#include "mvcc/publication_rpc.h"
#include "undo/manager.h"
namespace mvcc {
// One runtime per compute node. Snapshots advance only through the coordinator
// of the fixed member set. Lazy, 2PC and mix transactions share it: the home
// fixes one Context, every participant imports that same Context, and only the
// home coordinator reserves a commit position.
class LazyRuntime final : public PublicationHost {
public:
    LazyRuntime(std::string epoch, uint32_t node, std::set<NodeId> members, NodeId coordinator,
                undo::Wal wal, undo::PageAccess& pages, brpc::Channel* channels,
                uint64_t undo_capacity = 0);
    ~LazyRuntime() override;
    Snapshot Begin();
    undo::RowImage VisibleRow(const Snapshot&, undo::RowImage candidate);
    bool WriteAllowed(const Snapshot&, const undo::RowImage& current);
    // Pre-decision write check used by 2PC participants that already hold the
    // tuple lock: only SnapshotConflict is a normal rejected outcome.
    WriteCheck CheckWriteRow(const Snapshot&, const undo::RowImage& current);
    Scn Complete(const Snapshot&, bool committed, bool wrote, uint64_t durable_lsn);
    Scn PrepareLazy(const Context&, uint64_t target_lsn);
    Scn ReserveLazy(const Context&, uint64_t target_lsn) override;
    Scn SubmitCommit(const Context&, uint64_t durable_lsn, bool installed) override;
    // 2PC / Cornus / single-remote-writer entry point. Evidence is collected by
    // the home; the fixed coordinator freezes it, decides and publishes.
    Scn SubmitTwoPhase(const Context&, CommitKind kind, const std::set<NodeId>& writers,
                       const std::map<NodeId, uint64_t>& wal,
                       const std::map<NodeId, uint64_t>& prepare,
                       std::optional<WalPosition> decision, bool prepared_quorum,
                       const std::set<NodeId>& installed) override;
    Scn ReserveTwoPhase(const Context&, CommitKind kind, const std::set<NodeId>& writers,
                        const std::map<NodeId, uint64_t>& wal,
                        const std::map<NodeId, uint64_t>& prepare,
                        std::optional<WalPosition> decision, bool prepared_quorum) override;
    Scn CompleteTwoPhase(const Context&, const std::set<NodeId>& installed) override;
    void End(const Snapshot&, State outcome);
    // Home side: pin and later drain one participant execution lifetime.
    Context Export(const Snapshot&, NodeId participant);
    void RemoteFinished(const Context&, NodeId participant);
    // Participant side: import the home's fixed Context and its undo segment.
    Snapshot Participant(const Context&);
    // Participant side: seal its undo subsegment and end the imported snapshot.
    // A home node must only seal the undo part; it ends through End().
    void SealParticipantUndo(const TxnId&);
    void FinishParticipant(const Context&);
    std::optional<Status> LookupPublication(const TxnId&) const override;
    std::optional<Status> LookupStatus(const TxnId&) const;
    SnapshotNode& Snapshots() { return snapshots_; }
    undo::Manager& Undo() { return undo_; }
    const std::string& Epoch() const { return epoch_; }
    Scn SafeScn() const { return snapshots_.SafeScn(); }
    Scn CoordinatorHead() const { return coordinator_.HeadPosition(); }
    auto PendingBeforeProof() const { return coordinator_.PendingBeforeProof(); }
    Scn CoordinatorNext() const { return coordinator_.NextPosition(); }
    uint64_t VisibleReads() const { return visible_reads_.load(); }
    uint64_t VisibleNs() const { return visible_ns_.load(); }
    uint64_t HistorySteps() const { return history_steps_.load(); }
    uint64_t SiConflicts() const { return si_conflicts_.load(); }
    bool IsCoordinator() const { return node_ == coordinator_node_; }
    NodeId Coordinator() const { return coordinator_node_; }
    // Test seam: block delivery of an explicit publication message until the
    // caller releases it. Production constructors never install a gate.
    void SetDeliveryGate(std::function<void(const Message&)> gate);
private:
    std::optional<Status> Resolve(const Version& version) const;
    void PublisherLoop();
    std::string epoch_;
    uint32_t node_;
    std::set<NodeId> members_;
    NodeId coordinator_node_;
    brpc::Channel* channels_ = nullptr;
    std::atomic<uint64_t> next_txn_{0};
    std::atomic<uint64_t> visible_reads_{0}, visible_ns_{0}, history_steps_{0}, si_conflicts_{0};
    SnapshotNode snapshots_;
    CommitCoordinator coordinator_;
    undo::Manager undo_;
    std::atomic<bool> stopping_{false};
    std::thread publisher_;
    std::mutex gate_mutex_;
    std::function<void(const Message&)> delivery_gate_;
};
}  // namespace mvcc
