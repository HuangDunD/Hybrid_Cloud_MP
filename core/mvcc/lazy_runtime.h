#pragma once
#include <atomic>
#include <functional>
#include <set>
#include "mvcc/protocol.h"
#include "mvcc/publication_rpc.h"
#include "undo/manager.h"
namespace mvcc {
// Each transaction begins on one home, where its immutable S and final decision live.
class LazyRuntime final : public PublicationHost {
public:
    LazyRuntime(std::string epoch, NodeId node, std::set<NodeId> members, undo::Wal wal,
                undo::PageAccess& pages, brpc::Channel* channels, RemoteTso remote_tso,
                uint64_t undo_capacity = 0, const Recovery& recovery = {});
    Snapshot Begin();
    uint64_t PendingTitWaiters() const { return snapshots_.PendingTitWaiters(); }
    Timestamp AllocateCommitTs(const Context& context) { return snapshots_.AllocateCommitTs(context); }
    // The home certifies only after the installed versions and stamped decision are durable.
    void CertifyRemoteCommit(const Context& context, Timestamp commit_ts) {
        snapshots_.CertifyRemoteCommit(context, commit_ts);
    }
    // Resolve visibility on the home at a fixed read view. May block only on
    // a creator whose commit overlaps this snapshot boundary.
    std::optional<Status> ReadCreatorAt(const TxnId&, Timestamp reader_snapshot) const;
    undo::RowImage VisibleRow(const Snapshot&, undo::RowImage candidate);
    bool WriteAllowed(const Snapshot&, const undo::RowImage& current);
    // Pre-decision write check used by 2PC participants that already hold the
    // tuple lock: only SnapshotConflict is a normal rejected outcome.
    WriteCheck CheckWriteRow(const Snapshot&, const undo::RowImage& current);
    void End(const Snapshot&, State outcome);
    // Home side: pin and later drain one participant execution lifetime.
    Context Export(const Snapshot&, NodeId participant);
    void RemoteFinished(const Context&, NodeId participant);
    // Participant side: import the home's fixed Context.
    Snapshot Participant(const Context&);
    // Participant side: seal its undo subsegment and end the imported snapshot.
    // A home node must only seal the undo part; it ends through End().
    void SealParticipantUndo(const TxnId&);
    void FinishParticipant(const Context&);
    std::optional<Status> LookupStatus(const TxnId&) const override;
    SnapshotNode& Snapshots() { return snapshots_; }
    undo::Manager& Undo() { return undo_; }
    const std::string& Epoch() const { return epoch_; }
    uint64_t VisibleReads() const { return visible_reads_.load(); }
    uint64_t VisibleNs() const { return visible_ns_.load(); }
    uint64_t HistorySteps() const { return history_steps_.load(); }
    uint64_t SiConflicts() const { return si_conflicts_.load(); }
    void ResetBenchmarkStats() {
        visible_reads_ = 0;
        visible_ns_ = 0;
        history_steps_ = 0;
        si_conflicts_ = 0;
        undo_.ResetBenchmarkStats();
    }
private:
    std::optional<Status> Resolve(const Version& version, std::optional<Timestamp> read_view = {}) const;
    std::string epoch_;
    uint32_t node_;
    std::set<NodeId> members_;
    brpc::Channel* channels_ = nullptr;
    std::atomic<uint64_t> next_txn_{0};
    std::atomic<uint64_t> visible_reads_{0}, visible_ns_{0}, history_steps_{0}, si_conflicts_{0};
    SnapshotNode snapshots_;
    undo::Manager undo_;
};
}  // namespace mvcc
