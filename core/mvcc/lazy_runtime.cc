#include "mvcc/lazy_runtime.h"
#include "dtx/benchmark_stats.h"
#include <utility>
namespace mvcc {
LazyRuntime::LazyRuntime(std::string epoch, uint32_t node, std::set<NodeId> members, NodeId coordinator,
                         undo::Wal wal, undo::PageAccess& pages, brpc::Channel* channels,
                         uint64_t undo_capacity, const Recovery& recovery, RemoteTso remote_tso)
    : epoch_(std::move(epoch)),
      node_(node),
      members_(std::move(members)),
      coordinator_node_(coordinator),
      channels_(channels),
      next_txn_(recovery.last_local),
      snapshots_(epoch_, node, members_, [this] { return RefreshCertified(); }, recovery, std::move(remote_tso)),
      coordinator_(epoch_, members_, recovery.safe),
      undo_(epoch_, node, std::move(wal), pages, undo_capacity, recovery.undo_pages) {
    CHECK(members_.count(node_) == 1 && members_.count(coordinator_node_) == 1);
    CHECK(channels_ != nullptr);
    if (UsesRemoteTso()) return;
    if (IsCoordinator()) {
        for (const auto member : members_)
            publishers_.emplace_back([this, member] { PublisherLoop(member); });
    } else {
        wal_reporter_ = std::thread([this] { WalReporterLoop(); });
    }
}
LazyRuntime::~LazyRuntime() {
    {
        std::lock_guard<std::mutex> lock(wal_mutex_);
        stopping_.store(true);
    }
    wal_changed_.notify_one();
    if (wal_reporter_.joinable()) wal_reporter_.join();
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
Scn LazyRuntime::ReadCertified(const std::string& epoch) const {
    CHECK(!UsesRemoteTso() && IsCoordinator() && epoch == epoch_);
    return coordinator_.HeadPosition() - 1;
}
Scn LazyRuntime::RefreshCertified() {
    ++clock_refresh_count_;
    if (IsCoordinator()) return ReadCertified(epoch_);
    benchmark_stats::ScopedTimer timer(benchmark_stats::scn_refresh_rpc);
    return ReadCertifiedRpc(channels_[coordinator_node_], epoch_);
}
Scn LazyRuntime::ReportWal(const WalPosition& durable) {
    CHECK(!UsesRemoteTso() && IsCoordinator());
    coordinator_.ObserveWriterWal(durable);
    return ReadCertified(epoch_);
}
void LazyRuntime::FlushedWriterWal(uint64_t durable_lsn) {
    if (UsesRemoteTso()) return;
    if (IsCoordinator()) {
        coordinator_.ObserveWriterWal({epoch_, node_, durable_lsn});
        return;
    }
    {
        std::lock_guard<std::mutex> lock(wal_mutex_);
        durable_to_report_ = std::max(durable_to_report_, durable_lsn);
    }
    wal_changed_.notify_one();
}
void LazyRuntime::WalReporterLoop() {
    uint64_t sent = 0;
    std::unique_lock<std::mutex> lock(wal_mutex_);
    for (;;) {
        wal_changed_.wait(lock, [&] { return stopping_.load() || durable_to_report_ > sent; });
        if (durable_to_report_ == sent && stopping_.load()) return;
        sent = durable_to_report_;
        lock.unlock();
        const auto start = benchmark_stats::NowNs();
        const auto prefix = ReportWalRpc(channels_[coordinator_node_], {epoch_, node_, sent});
        benchmark_stats::scn_writer_report_rpc.Add(benchmark_stats::NowNs() - start);
        snapshots_.ConfirmThrough(prefix);
        lock.lock();
    }
}
void LazyRuntime::CertifyRemoteCommit(const Context& context, Scn slot) {
    snapshots_.CertifyRemoteCommit(context, slot);
}
Scn LazyRuntime::AllocateClock(const Context& context) {
    if (UsesRemoteTso()) return snapshots_.AllocateRemoteCommit(context);
    if (IsCoordinator()) return coordinator_.ReserveClock(context);
    CHECK(context.id.home == node_);
    return AllocateClockRpc(channels_[coordinator_node_], context);
}
Scn LazyRuntime::ClockCommit(const Context& context, Scn slot, uint64_t target_lsn) {
    CHECK(!UsesRemoteTso() && IsCoordinator());
    coordinator_.BindClockWal(context, slot, target_lsn);
    CHECK(coordinator_.WaitComplete(context.id));
    return slot;
}
void LazyRuntime::StartClock(const Context& context, Scn slot, uint64_t lsn, ClockCall& call) {
    CHECK(!UsesRemoteTso() && context.id.home == node_ && lsn > 0);
    if (IsCoordinator()) {
        coordinator_.BindClockWal(context, slot, lsn);
        call.local_slot = slot;
    } else StartClockRpc(channels_[coordinator_node_], context, slot, lsn, call);
}
Scn LazyRuntime::FinishClock(const Context& context, ClockCall& call) {
    CHECK(!UsesRemoteTso());
    const auto start = benchmark_stats::NowNs();
    Scn slot = call.local_slot;
    if (IsCoordinator()) {
        CHECK(coordinator_.WaitComplete(context.id));
    } else {
        brpc::Join(call.call_id);
        CHECK(!call.controller.Failed() && call.reply.IsInitialized()) << call.controller.ErrorText();
        slot = call.reply.slot();
    }
    CHECK(slot > context.snapshot);
    snapshots_.ConfirmThrough(slot);
    const auto elapsed = benchmark_stats::NowNs() - start;
    benchmark_stats::scn_wait.Add(elapsed);
    benchmark_stats::lazy_scn_confirm.Add(elapsed);
    return slot;
}
Scn LazyRuntime::PrepareLazy(const Context& context, uint64_t target_lsn) {
    CHECK(!UsesRemoteTso() && context.id.home == node_ && target_lsn > 0);
    return IsCoordinator() ? ReserveLazy(context, target_lsn)
                           : SendReserveLazy(channels_[coordinator_node_], context, target_lsn);
}
Scn LazyRuntime::ReserveLazy(const Context& context, uint64_t target_lsn) {
    CHECK(!UsesRemoteTso() && IsCoordinator());
    return coordinator_.Reserve(LazyPlan(context, target_lsn), true);
}
Scn LazyRuntime::SubmitCommit(const Context& context, uint64_t durable_lsn, bool installed) {
    CHECK(!UsesRemoteTso() && IsCoordinator() && durable_lsn > 0 && installed);
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
    CHECK(!UsesRemoteTso() && IsCoordinator()) << "2PC commit must be submitted to the fixed coordinator";
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
    CHECK(!UsesRemoteTso() && IsCoordinator()) << "2PC commit must be completed on the fixed coordinator";
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
namespace {
bool TerminalCreator(const std::optional<Status>& value) {
    return value && (value->state == State::Committed || value->state == State::Aborted);
}
}
std::optional<Status> LazyRuntime::LookupStatus(const TxnId& id) const {
    const auto local = snapshots_.Lookup(id);
    if (id.home == node_) return local;
    if (!UsesRemoteTso()) return local ? local : SendStatusQuery(channels_[id.home], id);
    if (TerminalCreator(local)) return local;
    benchmark_stats::ScopedTimer timer(benchmark_stats::tit_status_rpc);
    auto status = SendStatusQuery(channels_[id.home], id);
    if (TerminalCreator(status)) snapshots_.CacheRemoteTerminal(id, *status);
    return status;
}
std::optional<Status> LazyRuntime::LookupPublication(const TxnId& id) const { return LookupStatus(id); }
std::optional<Status> LazyRuntime::ReadCreatorAt(const TxnId& id, Scn snapshot) const {
    CHECK(UsesRemoteTso());
    if (id.home == node_) return snapshots_.ReadCreatorAt(id, snapshot);
    const auto local = snapshots_.Lookup(id);
    if (TerminalCreator(local)) return local;
    benchmark_stats::ScopedTimer timer(benchmark_stats::tit_status_rpc);
    auto status = SendCreatorAt(channels_[id.home], id, snapshot);
    if (TerminalCreator(status)) snapshots_.CacheRemoteTerminal(id, *status);
    return status;
}
std::optional<Status> LazyRuntime::Resolve(const Version& version, std::optional<Scn> read_view) const {
    if (version.initial) return std::nullopt;
    auto status = UsesRemoteTso() && read_view
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
Scn LazyRuntime::Complete(const Snapshot& snapshot, bool committed, bool wrote, uint64_t durable) {
    if (!committed || !wrote) {
        snapshots_.End(snapshot, committed ? State::ReadOnlyFinished : State::Aborted);
        return 0;
    }
    CHECK(!UsesRemoteTso() && durable > 0);
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
