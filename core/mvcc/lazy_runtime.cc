#include "mvcc/lazy_runtime.h"
#include "dtx/benchmark_stats.h"
#include <utility>
namespace mvcc {
LazyRuntime::LazyRuntime(std::string epoch, uint32_t node, std::set<NodeId> members, NodeId coordinator,
                         undo::Wal wal, undo::PageAccess& pages, brpc::Channel* channels,
                         uint64_t undo_capacity)
    : epoch_(std::move(epoch)),
      node_(node),
      members_(std::move(members)),
      coordinator_node_(coordinator),
      channels_(channels),
      snapshots_(epoch_, node, members_),
      coordinator_(epoch_, members_),
      undo_(epoch_, node, std::move(wal), pages, undo_capacity) {
    CHECK(members_.count(node_) == 1 && members_.count(coordinator_node_) == 1);
    CHECK(channels_ != nullptr);
    if (IsCoordinator()) {
        for (const auto member : members_)
            publishers_.emplace_back([this, member] { PublisherLoop(member); });
    }
}
LazyRuntime::~LazyRuntime() {
    if (publishers_.empty()) return;
    stopping_.store(true);
    coordinator_.StopMessageWait();
    for (auto& worker : publishers_) worker.join();
}
Snapshot LazyRuntime::Begin() {
    const auto id = ++next_txn_;
    CHECK(id > 0);
    auto snapshot = snapshots_.Begin(id);
    undo_.Begin(snapshot.context());
    return snapshot;
}
void LazyRuntime::SetDeliveryGate(std::function<void(const Message&)> gate) {
    std::lock_guard<std::mutex> guard(gate_mutex_);
    delivery_gate_ = std::move(gate);
}
void LazyRuntime::PublisherLoop(NodeId member) {
    while (!stopping_.load(std::memory_order_acquire)) {
        const auto messages = coordinator_.WaitAndTakeMessages(member);
        if (stopping_.load(std::memory_order_acquire)) break;
        // Exactly one sender per member. The receiver installs every envelope in
        // FIFO order; an unrelated slow peer cannot stall this member's stream.
        if (member == node_) {
            for (const auto& message : messages) snapshots_.Receive(message);
        } else {
            SendPublications(channels_[member], messages);
        }
        std::function<void(const Message&)> gate;
        {
            std::lock_guard<std::mutex> guard(gate_mutex_);
            gate = delivery_gate_;
        }
        if (gate) for (const auto& message : messages) gate(message);
        coordinator_.Acknowledge(messages);
    }
}
Context LazyRuntime::Export(const Snapshot& snapshot, NodeId participant) {
    return snapshots_.Export(snapshot, participant);
}
void LazyRuntime::RemoteFinished(const Context& context, NodeId participant) {
    snapshots_.RemoteFinished(context, participant);
}
Snapshot LazyRuntime::Participant(const Context& context) {
    CHECK(context.id.epoch == epoch_ && context.id.local != 0 &&
          members_.count(context.id.home) == 1);
    if (context.id.home == node_) return snapshots_.Local(context);
    CHECK(snapshots_.Attach(context))
        << "participant RPC arrived after its execution lifetime was finished";
    undo_.Begin(context);
    return snapshots_.Imported(context);
}
void LazyRuntime::SealParticipantUndo(const TxnId& id) {
    CHECK(id.epoch == epoch_ && id.local != 0);
    undo_.Finish(id);
}
void LazyRuntime::FinishParticipant(const Context& context) {
    CHECK(context.id.home != node_);
    undo_.Finish(context.id);
    snapshots_.Finish(context);
}
namespace {
Plan LazyPlan(const Context& context, uint64_t target_lsn) {
    Plan plan;
    plan.context = context;
    plan.kind = CommitKind::SingleWriter;
    plan.writers = {context.id.home};
    plan.wal = {{context.id.home, target_lsn}};
    return plan;
}
}
Scn LazyRuntime::PrepareLazy(const Context& context, uint64_t target_lsn) {
    CHECK(context.id.home == node_ && target_lsn > 0);
    return IsCoordinator() ? ReserveLazy(context, target_lsn)
                           : SendReserveLazy(channels_[coordinator_node_], context, target_lsn);
}
Scn LazyRuntime::ReserveLazy(const Context& context, uint64_t target_lsn) {
    CHECK(IsCoordinator());
    return coordinator_.Reserve(LazyPlan(context, target_lsn), true);
}
Scn LazyRuntime::SubmitCommit(const Context& context, uint64_t durable_lsn, bool installed) {
    CHECK(IsCoordinator() && durable_lsn > 0 && installed);
    const auto slot = coordinator_.Reserve(LazyPlan(context, durable_lsn));
    coordinator_.DecideCommit(context.id);
    coordinator_.Installed(context.id, context.id.home);
    coordinator_.WalDurable(context.id, {epoch_, context.id.home, durable_lsn});
    CHECK(coordinator_.WaitComplete(context.id)) << "Lazy commit did not resolve COMMITTED";
    CHECK(slot > context.snapshot);
    return slot;
}
Scn LazyRuntime::ReserveTwoPhase(const Context& context, CommitKind kind,
                                 const std::set<NodeId>& writers,
                                 const std::map<NodeId, uint64_t>& wal,
                                 const std::map<NodeId, uint64_t>& prepare,
                                 std::optional<WalPosition> decision, bool prepared_quorum) {
    CHECK(IsCoordinator()) << "2PC commit must be submitted to the fixed coordinator";
    CHECK(context.id.epoch == epoch_ && context.id.local != 0 && members_.count(context.id.home) == 1);
    Plan plan;
    plan.context = context;
    plan.kind = kind;
    plan.writers = writers;
    plan.wal = wal;
    plan.prepare = prepare;
    plan.decision = decision;
    const auto slot = coordinator_.Reserve(plan);
    for (const auto& entry : prepare)
        coordinator_.Prepared(context.id, {epoch_, entry.first, entry.second});
    for (const auto& entry : wal)
        coordinator_.WalDurable(context.id, {epoch_, entry.first, entry.second});
    if (prepared_quorum) coordinator_.PreparedQuorum(context.id);
    coordinator_.DecideCommit(context.id);
    if (decision) coordinator_.DecisionDurable(context.id, *decision);
    CHECK(slot > context.snapshot);
    return slot;
}
Scn LazyRuntime::CompleteTwoPhase(const Context& context, const std::set<NodeId>& installed) {
    CHECK(IsCoordinator()) << "2PC commit must be completed on the fixed coordinator";
    for (NodeId writer : installed) coordinator_.Installed(context.id, writer);
    CHECK(coordinator_.WaitComplete(context.id)) << "2PC commit did not resolve COMMITTED";
    const auto status = coordinator_.Lookup(context.id);
    CHECK(status.state == State::Committed && status.commit_scn > context.snapshot);
    return status.commit_scn;
}
Scn LazyRuntime::SubmitTwoPhase(const Context& context, CommitKind kind,
                                const std::set<NodeId>& writers,
                                const std::map<NodeId, uint64_t>& wal,
                                const std::map<NodeId, uint64_t>& prepare,
                                std::optional<WalPosition> decision, bool prepared_quorum,
                                const std::set<NodeId>& installed) {
    const auto slot = ReserveTwoPhase(context, kind, writers, wal, prepare, decision, prepared_quorum);
    CHECK(CompleteTwoPhase(context, installed) == slot);
    return slot;
}
void LazyRuntime::End(const Snapshot& snapshot, State outcome) {
    snapshots_.End(snapshot, outcome);
}
std::optional<Status> LazyRuntime::LookupStatus(const TxnId& id) const {
    CHECK(id.epoch == epoch_ && id.local != 0 && members_.count(id.home) == 1);
    if (id.home == node_) return snapshots_.Lookup(id);
    // Terminal states installed by Stage are authoritative locally. Only an
    // unknown/active creator needs the home node's live registration.
    if (auto local = snapshots_.Lookup(id)) return local;
    return SendStatusQuery(channels_[id.home], id);
}
std::optional<Status> LazyRuntime::LookupPublication(const TxnId& id) const { return LookupStatus(id); }
std::optional<Status> LazyRuntime::Resolve(const Version& version) const {
    if (version.initial) return std::nullopt;
    CHECK(version.creator.epoch == epoch_ && members_.count(version.creator.home) == 1)
        << "version creator epoch/member mismatch";
    auto status = LookupStatus(version.creator);
    CHECK(status.has_value()) << "unresolved MVCC creator after authoritative lookup";
    return status;
}
undo::RowImage LazyRuntime::VisibleRow(const Snapshot& snapshot, undo::RowImage row) {
    const auto began = benchmark_stats::NowNs();
    const auto id = row.id;
    std::set<std::pair<undo::PageKey, uint32_t>> visited;
    auto finish = [&](undo::RowImage result) {
        ++visible_reads_;
        visible_ns_ += benchmark_stats::NowNs() - began;
        return result;
    };
    for (;;) {
        const auto visibility = Visible(snapshot, row.version, Resolve(row.version));
        if (visibility == Visibility::Value || visibility == Visibility::NotFound)
            return finish(std::move(row));
        CHECK(visibility == Visibility::FollowHistory && !row.previous.empty()) << "broken retained history";
        auto inserted = visited.emplace(row.previous.page, row.previous.slot);
        CHECK(inserted.second) << "cyclic row history";
        ++history_steps_;
        row = undo_.Read(row.previous, id).old;
    }
}
bool LazyRuntime::WriteAllowed(const Snapshot& snapshot, const undo::RowImage& current) {
    const auto result = CheckWriteRow(snapshot, current);
    if (result == WriteCheck::SnapshotConflict) return false;
    // The caller has already acquired the tuple lock and resolved its previous
    // holder. Active foreign creators cannot remain after that handoff.
    CHECK(result == WriteCheck::Allowed) << "tuple permission and version state disagree";
    return true;
}
WriteCheck LazyRuntime::CheckWriteRow(const Snapshot& snapshot, const undo::RowImage& current) {
    const auto check = CheckWrite(snapshot, current.version, Resolve(current.version));
    if (check == WriteCheck::SnapshotConflict) ++si_conflicts_;
    return check;
}
Scn LazyRuntime::Complete(const Snapshot& snapshot, bool committed, bool wrote, uint64_t durable) {
    if (!committed || !wrote) {
        snapshots_.End(snapshot, committed ? State::ReadOnlyFinished : State::Aborted);
        return 0;
    }
    CHECK(durable > 0);
    const auto& context = snapshot.context();
    const auto publication_begin = benchmark_stats::NowNs();
    Scn scn = IsCoordinator() ? SubmitCommit(context, durable, true)
                              : SendCommit(channels_[coordinator_node_], context, durable, true);
    benchmark_stats::scn_wait.Add(benchmark_stats::NowNs() - publication_begin);
    CHECK(scn > context.snapshot);
    snapshots_.End(snapshot, State::Committed);
    return scn;
}
}  // namespace mvcc
