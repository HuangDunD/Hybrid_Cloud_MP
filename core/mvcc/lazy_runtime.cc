#include "mvcc/lazy_runtime.h"
#include "dtx/benchmark_stats.h"
#include <utility>
namespace mvcc {
LazyRuntime::LazyRuntime(std::string epoch, NodeId node, std::set<NodeId> members, undo::Wal wal,
                         undo::PageAccess& pages, brpc::Channel* channels, RemoteTso remote_tso,
                         uint64_t undo_capacity, const Recovery& recovery)
    : epoch_(std::move(epoch)), node_(node), members_(std::move(members)), channels_(channels),
      next_txn_(recovery.last_local),
      snapshots_(epoch_, node, members_, std::move(remote_tso), recovery),
      undo_(epoch_, node, std::move(wal), pages, undo_capacity, recovery.undo_pages) {
    CHECK(channels_);
}
Snapshot LazyRuntime::Begin() {
    const auto id = ++next_txn_;
    CHECK(id > 0);
    return snapshots_.Begin(id);
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
void LazyRuntime::End(const Snapshot& snapshot, State outcome) {
    snapshots_.End(snapshot, outcome);
}
namespace {
bool TerminalCreator(const std::optional<Status>& value) {
    return value && (value->state == State::Committed || value->state == State::Aborted);
}
}
std::optional<Status> LazyRuntime::LookupStatus(const TxnId& id) const {
    const auto local = snapshots_.Lookup(id);
    if (id.home == node_) return local;
    if (TerminalCreator(local)) return local;
    benchmark_stats::ScopedTimer timer(benchmark_stats::tit_status_rpc);
    auto status = SendStatusQuery(channels_[id.home], id);
    if (TerminalCreator(status)) snapshots_.CacheRemoteTerminal(id, *status);
    return status;
}
std::optional<Status> LazyRuntime::ReadCreatorAt(const TxnId& id, Timestamp snapshot) const {
    if (id.home == node_) return snapshots_.ReadCreatorAt(id, snapshot);
    const auto local = snapshots_.Lookup(id);
    if (TerminalCreator(local)) return local;
    benchmark_stats::ScopedTimer timer(benchmark_stats::tit_status_rpc);
    auto status = SendCreatorAt(channels_[id.home], id, snapshot);
    if (TerminalCreator(status)) snapshots_.CacheRemoteTerminal(id, *status);
    return status;
}
std::optional<Status> LazyRuntime::Resolve(const Version& version, std::optional<Timestamp> read_view) const {
    if (version.initial) return std::nullopt;
    auto status = read_view
        ? ReadCreatorAt(version.creator, *read_view) : LookupStatus(version.creator);
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
        const auto status = row.version.creator == snapshot.context().id ? std::nullopt
            : Resolve(row.version, snapshot.context().snapshot);
        const auto visibility = Visible(snapshot, row.version, status);
        if (visibility == Visibility::Value || visibility == Visibility::NotFound)
            return finish(std::move(row));
        CHECK(visibility == Visibility::FollowHistory && !row.previous.empty()) << "broken retained history";
        auto inserted = visited.emplace(row.previous.page, row.previous.slot);
        CHECK(inserted.second) << "cyclic row history";
        ++history_steps_;
        benchmark_stats::ScopedTimer timer(benchmark_stats::undo_history_step);
        row = undo_.Read(row.previous, id).RestorePrevious(row, row.previous);
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
    const auto status = current.version.creator == snapshot.context().id ? std::nullopt
        : Resolve(current.version);
    const auto check = CheckWrite(snapshot, current.version, status);
    if (check == WriteCheck::SnapshotConflict) ++si_conflicts_;
    return check;
}
}  // namespace mvcc
