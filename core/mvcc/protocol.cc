#include "mvcc/protocol.h"
#include <algorithm>
#include <limits>
#include <butil/logging.h>

namespace mvcc {
namespace {
void ValidEpoch(const std::string& epoch) {
    CHECK(epoch.size() == 16 && epoch != std::string(16, '\0')) << "invalid MVCC epoch";
}
bool Covered(const std::map<NodeId, uint64_t>& targets,
             const std::map<NodeId, uint64_t>& observed) {
    for (const auto& target : targets) {
        auto it = observed.find(target.first);
        if (it == observed.end() || it->second < target.second) return false;
    }
    return true;
}
void CheckStatus(const Status& status) {
    switch (status.state) {
        case State::Active: case State::Preparing: case State::Installing:
        case State::Aborting: case State::Committed: case State::Aborted:
        case State::ReadOnlyFinished: break;
        default: LOG(FATAL) << "invalid transaction state";
    }
    CHECK((status.state == State::Committed) == (status.commit_scn != 0))
        << "only COMMITTED has a commit SCN";
    CHECK(status.state != State::ReadOnlyFinished) << "read-only transaction cannot create a version";
}
void CheckVersion(const Snapshot& reader, const Version& v) {
    if (v.initial) {
        CHECK(v.creator == TxnId{}) << "initial version must not name a transaction";
    } else {
        CHECK(v.creator.epoch == reader.context().id.epoch && v.creator.local != 0)
            << "version creator has invalid identity/epoch";
    }
}
}
SnapshotNode::SnapshotNode(std::string epoch, NodeId self, std::set<NodeId> members,
                           std::function<Scn()> read_certified)
    : epoch_(std::move(epoch)), self_(self), members_(std::move(members)),
      read_certified_(std::move(read_certified)) {
    ValidEpoch(epoch_);
    CHECK(members_.count(self_) == 1);
}
void SnapshotNode::Validate(const Context& context) const {
    CHECK(context.id.epoch == epoch_ && context.id.local != 0 && members_.count(context.id.home))
        << "invalid transaction identity";
}
void SnapshotNode::ConfirmLocked(Scn prefix) {
    // Every member installed each Clock before the coordinator could certify it.
    // Legacy two-phase slots have already advanced here before joining this prefix.
    while (safe_ < prefix) {
        const auto it = resolutions_.find(safe_ + 1);
        CHECK(it != resolutions_.end() && it->second.second == Phase::Clock);
        const auto& r = it->second.first;
        CHECK(r.committed && pending_creators_.erase(r.context.id) == 1);
        CHECK(outcomes_.emplace(r.context.id, Status{State::Committed, r.slot}).second);
        safe_ = staged_ = r.slot;
    }
}
void SnapshotNode::ConfirmThrough(Scn prefix) {
    std::lock_guard<std::mutex> guard(mutex_);
    ConfirmLocked(prefix);
}
void SnapshotNode::Refresh(std::unique_lock<std::mutex>& lock) {
    CHECK(read_certified_);
    lock.unlock();
    const auto prefix = read_certified_();
    lock.lock();
    ConfirmLocked(prefix);
}
Snapshot SnapshotNode::Begin(uint64_t local_id) {
    std::unique_lock<std::mutex> guard(mutex_);
    if (announced_clock_ > safe_) Refresh(guard);
    Context c{{epoch_, self_, local_id}, safe_};
    Validate(c);
    auto inserted = registrations_.emplace(c.id, Registration{c, false, {}});
    CHECK(inserted.second) << "transaction ID reused";
    return Snapshot(c);
}
Context SnapshotNode::Export(const Snapshot& snapshot, NodeId participant) {
    std::lock_guard<std::mutex> guard(mutex_);
    const auto& c = snapshot.context();
    Validate(c);
    CHECK(c.id.home == self_ && participant != self_ && members_.count(participant));
    auto it = registrations_.find(c.id);
    CHECK(it != registrations_.end() && it->second.context == c && !it->second.ended);
    auto inserted = it->second.exports.emplace(participant, false);
    CHECK(!inserted.first->second) << "cannot reopen drained remote execution";
    return c;
}
bool SnapshotNode::Attach(const Context& c) {
    std::unique_lock<std::mutex> guard(mutex_);
    Validate(c);
    CHECK(c.id.home != self_);
    // A peer may already have advanced this slot. An early announcement
    // alone is never evidence for a read, even when the caller imports it.
    if (c.snapshot > staged_ && announced_clock_ >= c.snapshot) Refresh(guard);
    while (c.snapshot > staged_) {
        const auto announced = resolutions_.find(c.snapshot);
        CHECK(announced != resolutions_.end() &&
              announced->second.second == Phase::Pending) << "context is ahead of publishable prefix";
        if (!pending_imports_) pending_imports_ = std::make_shared<bthread::CountdownEvent>();
        auto ready = pending_imports_;
        guard.unlock();
        CHECK_EQ(ready->wait(), 0);
        guard.lock();
        // Another member can be several certified Lazy positions ahead.
    }
    auto inserted = registrations_.emplace(c.id, Registration{c, false, {}});
    CHECK(inserted.first->second.context == c) << "snapshot changed on remote participant";
    return !inserted.first->second.ended;
}
Snapshot SnapshotNode::Imported(const Context& c) const {
    std::lock_guard<std::mutex> guard(mutex_);
    Validate(c);
    auto it = registrations_.find(c.id);
    CHECK(c.id.home != self_ && it != registrations_.end() &&
          it->second.context == c && !it->second.ended);
    return Snapshot(c);
}
Snapshot SnapshotNode::Local(const Context& c) const {
    std::lock_guard<std::mutex> guard(mutex_);
    Validate(c);
    CHECK(c.id.home == self_);
    auto it = registrations_.find(c.id);
    CHECK(it != registrations_.end() && it->second.context == c && !it->second.ended)
        << "local participant lacks the home's fixed snapshot";
    return Snapshot(c);
}
void SnapshotNode::Finish(const Context& c) {
    std::lock_guard<std::mutex> guard(mutex_);
    Validate(c);
    CHECK(c.id.home != self_);
    auto it = registrations_.find(c.id);
    CHECK(it != registrations_.end() && it->second.context == c);
    it->second.ended = true;
}
void SnapshotNode::RemoteFinished(const Context& c, NodeId participant) {
    std::lock_guard<std::mutex> guard(mutex_);
    Validate(c);
    CHECK(c.id.home == self_);
    auto it = registrations_.find(c.id);
    CHECK(it != registrations_.end() && it->second.context == c);
    auto peer = it->second.exports.find(participant);
    CHECK(peer != it->second.exports.end());
    peer->second = true;
}
void SnapshotNode::End(const Snapshot& snapshot, State outcome) {
    std::lock_guard<std::mutex> guard(mutex_);
    const auto& c = snapshot.context();
    Validate(c);
    CHECK(c.id.home == self_);
    auto it = registrations_.find(c.id);
    CHECK(it != registrations_.end() && it->second.context == c);
    for (const auto& peer : it->second.exports) CHECK(peer.second) << "remote snapshot still in use";
    CHECK(outcome == State::ReadOnlyFinished || outcome == State::Committed || outcome == State::Aborted);
    CHECK(!pending_creators_.count(c.id)) << "End before the durable Advance";
    auto known = outcomes_.find(c.id);
    if (known != outcomes_.end()) {
        CHECK(known->second.state == outcome) << "conflicting transaction end";
    } else {
        CHECK(outcome != State::Committed) << "End before terminal publication";
        outcomes_.emplace(c.id, Status{outcome, 0});
    }
    it->second.ended = true; // terminal outcome, if any, is retained separately
}
Ack SnapshotNode::Receive(const Message& m) {
    std::lock_guard<std::mutex> guard(mutex_);
    CHECK(m.node == self_);
    const auto& r = m.resolution;
    Validate(r.context);
    CHECK(r.slot > r.context.snapshot && r.slot != 0);
    ConfirmLocked(m.certified);
    if (m.phase != Phase::Advance) {
        CHECK(m.phase == Phase::Stage || m.phase == Phase::Clock || m.phase == Phase::Pending);
        CHECK(m.phase == Phase::Stage || r.committed);
        if (m.phase == Phase::Clock) announced_clock_ = std::max(announced_clock_, r.slot);
        auto old = resolutions_.find(r.slot);
        if (old != resolutions_.end()) {
            CHECK(old->second.first == r && old->second.second == m.phase) << "conflicting publication";
        } else {
            CHECK(r.slot > staged_);
            if (m.phase == Phase::Stage)
                CHECK(r.slot == staged_ + 1 && safe_ == staged_) << "terminal Stage skipped a position";
            auto context = registrations_.find(r.context.id);
            if (context != registrations_.end()) CHECK(context->second.context == r.context);
            CHECK(outcomes_.count(r.context.id) == 0) << "transaction assigned two commit positions";
            resolutions_.emplace(r.slot, std::make_pair(r, m.phase));
            if (m.phase == Phase::Stage) {
                CHECK(!pending_creators_.count(r.context.id));
                outcomes_.emplace(r.context.id, Status{r.committed ? State::Committed : State::Aborted,
                                                      r.committed ? r.slot : 0});
                staged_ = r.slot;
            } else {
                CHECK(pending_creators_.insert(r.context.id).second) << "duplicate pending creator";
            }
        }
    } else {
        CHECK(m.phase == Phase::Advance);
        auto it = resolutions_.find(r.slot);
        CHECK(it != resolutions_.end() && it->second.first == r) << "Advance without matching first round";
        CHECK(it->second.second != Phase::Clock);
        CHECK(r.slot <= safe_ || r.slot == safe_ + 1) << "noncontiguous snapshot frontier";
        if (r.slot > safe_) {
            safe_ = r.slot;
            if (it->second.second == Phase::Pending) {
                CHECK(pending_creators_.erase(r.context.id) == 1);
                CHECK(outcomes_.emplace(r.context.id, Status{State::Committed, r.slot}).second);
                staged_ = r.slot;
                if (pending_imports_) {
                    pending_imports_->signal();
                    pending_imports_.reset();
                }
            }
        }
    }
    return m;
}
std::optional<Status> SnapshotNode::Lookup(const TxnId& id) const {
    std::lock_guard<std::mutex> guard(mutex_);
    Validate({id, 0});
    auto outcome = outcomes_.find(id);
    if (outcome != outcomes_.end()) return outcome->second;
    auto active = registrations_.find(id);
    if (active != registrations_.end() && !active->second.ended) return Status{State::Active, 0};
    return std::nullopt; // unknown requires authoritative resolution, never history fallback
}
Scn SnapshotNode::SafeScn() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return safe_;
}
Scn SnapshotNode::RetentionFloor() const {
    std::lock_guard<std::mutex> guard(mutex_);
    Scn floor = safe_;
    for (const auto& entry : registrations_)
        if (entry.first.home == self_ && !entry.second.ended)
            floor = std::min(floor, entry.second.context.snapshot);
    return floor;
}
CommitCoordinator::CommitCoordinator(std::string epoch, std::set<NodeId> members)
    : epoch_(std::move(epoch)), members_(std::move(members)) {
    ValidEpoch(epoch_);
    CHECK(!members_.empty());
    for (NodeId member : members_) outboxes_.try_emplace(member);
}
void CommitCoordinator::Validate(const Plan& p) const {
    CHECK(p.context.id.epoch == epoch_ && p.context.id.local && members_.count(p.context.id.home));
    CHECK(!p.writers.empty());
    for (NodeId writer : p.writers) CHECK(members_.count(writer) == 1);
    for (const auto& w : p.wal) CHECK(p.writers.count(w.first) && w.second > 0);
    for (const auto& w : p.prepare) CHECK(p.writers.count(w.first) && w.second > 0);
    for (const auto& w : p.prepare) CHECK(p.wal.count(w.first) && w.second <= p.wal.at(w.first));
    if (p.kind == CommitKind::SingleWriter) {
        CHECK(p.writers.size() == 1 && p.wal.size() == 1 && p.wal.count(*p.writers.begin()));
        CHECK(p.prepare.empty() && !p.decision);
    } else {
        CHECK(p.kind == CommitKind::TwoPhase || p.kind == CommitKind::Cornus);
        CHECK(p.writers.size() >= 2);
        // Which nodes report installed evidence is fixed for every kind.
        CHECK(p.wal.size() == p.prepare.size());
        CHECK(p.wal.empty() || p.wal.size() == p.writers.size());
        if (p.kind == CommitKind::TwoPhase) {
            // Baseline 2PC freezes every writer's prepare prefix and its home
            // decision record before senders may install.
            CHECK(p.wal.size() == p.writers.size() && p.prepare.size() == p.writers.size());
            CHECK(p.decision.has_value());
        } else {
            CHECK(!p.decision) << "Cornus uses the durable quorum, not a home decision record";
        }
        if (p.decision)
            CHECK(p.decision->epoch == epoch_ && p.decision->writer == p.context.id.home &&
                  p.decision->lsn > 0);
    }
}
CommitCoordinator::Entry& CommitCoordinator::Find(const TxnId& id) {
    auto it = entries_.find(id);
    CHECK(it != entries_.end()) << "unknown MVCC transaction";
    return it->second;
}
const CommitCoordinator::Entry& CommitCoordinator::Find(const TxnId& id) const {
    auto it = entries_.find(id);
    CHECK(it != entries_.end()) << "unknown MVCC transaction";
    return it->second;
}
Scn CommitCoordinator::Reserve(const Plan& p, bool announce_pending) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    return ReserveLocked(p, announce_pending, false);
}
Scn CommitCoordinator::ReserveClock(const Plan& p) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    return ReserveLocked(p, true, true);
}
void CommitCoordinator::ObserveWriterWal(const WalPosition& d) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    CHECK(d.epoch == epoch_ && members_.count(d.writer) && d.lsn > 0);
    auto& prefix = writer_wal_[d.writer];
    if (d.lsn <= prefix) return;
    prefix = d.lsn;
    Pump();
}
Scn CommitCoordinator::ReserveLocked(const Plan& p, bool announce_pending, bool clock) {
    Validate(p);
    if (announce_pending) CHECK(p.kind == CommitKind::SingleWriter && p.writers.count(p.context.id.home));
    auto old = entries_.find(p.context.id);
    if (old != entries_.end()) {
        CHECK(old->second.plan == p && old->second.clock == clock) << "conflicting repeated Reserve";
        return old->second.slot;
    }
    CHECK(next_ != std::numeric_limits<Scn>::max() && p.context.snapshot < next_);
    auto [it, inserted] = entries_.try_emplace(p.context.id);
    CHECK(inserted);
    auto& e = it->second;
    e.plan = p;
    e.slot = next_++;
    e.state = p.kind == CommitKind::SingleWriter ? State::Active : State::Preparing;
    slots_.emplace(e.slot, p.context.id);
    if (announce_pending) {
        // The writer has finished installing and sealed its log. It can no
        // longer choose abort, but the target LSN is not yet durable evidence.
        e.announce_pending = true;
        e.clock = clock;
        e.state = State::Installing;
        e.installed.insert(p.context.id.home);
        e.resolution = Resolution{e.plan.context, e.slot, true};
        ++announced_before_proof_;
        Broadcast(clock ? Phase::Clock : Phase::Pending, *e.resolution);
    }
    return e.slot;
}
void CommitCoordinator::DecideCommit(const TxnId& id) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    auto& e = Find(id);
    CHECK(e.state == State::Active || e.state == State::Preparing ||
          e.state == State::Installing || e.state == State::Committed) << "Commit after Abort";
    if (e.state == State::Active || e.state == State::Preparing) e.state = State::Installing;
    Pump();
}
void CommitCoordinator::Evidence(const WalPosition& d,
        const std::map<NodeId, uint64_t>& targets, std::map<NodeId, uint64_t>& observed) {
    CHECK(d.epoch == epoch_ && targets.count(d.writer) && d.lsn > 0) << "wrong WAL stream/participant";
    observed[d.writer] = std::max(observed[d.writer], d.lsn); // WAL prefixes, NOT SCN publication
}
void CommitCoordinator::PreparedQuorum(const TxnId& id) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    auto& e = Find(id);
    CHECK(e.plan.kind == CommitKind::Cornus) << "only Cornus consumes a storage-collected YES set";
    // A duplicate observation after publication is legal and idempotent.
    CHECK(e.state == State::Preparing || e.state == State::Installing ||
          e.state == State::Committed) << "quorum after the transaction resolved non-committed";
    e.prepared_quorum = true;
    Pump();
}
void CommitCoordinator::Prepared(const TxnId& id, const WalPosition& d) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    auto& e = Find(id);
    Evidence(d, e.plan.prepare, e.prepared);
    Pump();
}
void CommitCoordinator::WalDurable(const TxnId& id, const WalPosition& d) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    auto& e = Find(id);
    Evidence(d, e.plan.wal, e.wal);
    Pump();
}
void CommitCoordinator::DecisionDurable(const TxnId& id, const WalPosition& d) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    auto& e = Find(id);
    CHECK(e.plan.decision && d.epoch == epoch_ && d.writer == e.plan.decision->writer && d.lsn > 0);
    // A committed decision is irreversible even if its notifications overtake YES.
    CHECK(e.state == State::Installing || e.state == State::Committed) << "decision proof requires DecideCommit";
    e.decision = std::max(e.decision, d.lsn);
    Pump();
}
void CommitCoordinator::Installed(const TxnId& id, NodeId writer) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    auto& e = Find(id);
    CHECK(e.plan.writers.count(writer) == 1) << "installed evidence from a non-writer";
    // Lazy may install while Active. Multi-writer installs follow the decision;
    // their notification may nevertheless arrive before other durable evidence.
    CHECK(e.state == State::Installing || e.state == State::Committed ||
          (e.plan.kind == CommitKind::SingleWriter &&
           (e.state == State::Active || e.state == State::Aborting || e.state == State::Aborted)));
    e.installed.insert(writer);
    Pump();
}
void CommitCoordinator::Abort(const TxnId& id, const std::map<NodeId, uint64_t>& targets) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    auto& e = Find(id);
    CHECK(targets.size() == e.plan.writers.size());
    for (const auto& w : targets) CHECK(e.plan.writers.count(w.first) && w.second > 0);
    if (e.state == State::Aborting || e.state == State::Aborted) {
        CHECK(e.rollback_targets == targets) << "conflicting Abort targets";
    } else {
        CHECK(e.state == State::Active || e.state == State::Preparing) << "cannot Abort a commit decision";
        e.rollback_targets = targets;
        e.state = State::Aborting;
    }
    Pump();
}
void CommitCoordinator::RolledBack(const TxnId& id, const WalPosition& d) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    auto& e = Find(id);
    CHECK(e.state == State::Aborting || e.state == State::Aborted);
    Evidence(d, e.rollback_targets, e.rolled_back);
    Pump();
}
bool CommitCoordinator::Ready(const Entry& e) const {
    if (e.state == State::Aborting) return Covered(e.rollback_targets, e.rolled_back);
    if (e.state != State::Installing) return false;
    if (!Covered(e.plan.wal, e.clock ? writer_wal_ : e.wal) || !Covered(e.plan.prepare, e.prepared)) return false;
    // Cornus proves the durable YES set either by every writer's prepare prefix
    // (v1) or by the storage-collected quorum flag (v2); neither is optional.
    if (e.plan.kind == CommitKind::Cornus && e.plan.prepare.empty() && !e.prepared_quorum)
        return false;
    if (e.installed != e.plan.writers) return false;
    return !e.plan.decision || e.decision >= e.plan.decision->lsn;
}
void CommitCoordinator::Broadcast(Phase phase, const Resolution& r) {
    for (auto& [member, queue] : outboxes_) {
        queue.messages.push_back({member, phase, r, head_ - 1});
        if (queue.messages.size() == 1) queue.ready.notify_one();
    }
}
void CommitCoordinator::Pump() {
    // Certify a contiguous prefix. Pending, WAL and installation evidence are
    // still checked per writer. Previous Advance receipts need not arrive first.
    for (; next_advance_ < next_; ++next_advance_) {
        auto& e = Find(slots_.at(next_advance_));
        if (e.clock) {
            if (head_ != next_advance_ || e.stage_acks != members_ || !Ready(e)) return;
            e.state = State::Committed;
            e.complete = true;
            ++head_;
            e.completed.notify_all();
            continue;
        }
        if (!e.announce_pending) {
            // Stage changes transaction status before Advance. Keep the original
            // barrier for 2PC, Cornus, rollback and the non-overlapped path.
            if (next_advance_ != head_) return;
            if (!e.resolution && Ready(e)) {
                e.resolution = Resolution{e.plan.context, e.slot, e.state == State::Installing};
                e.state = e.resolution->committed ? State::Committed : State::Aborted;
                Broadcast(Phase::Stage, *e.resolution);
            }
        }
        if (e.stage_acks != members_ || (e.announce_pending && !Ready(e))) return;
        if (e.announce_pending) e.state = State::Committed;
        e.advance_sent = true;
        Broadcast(Phase::Advance, *e.resolution);
    }
}
void CommitCoordinator::Acknowledge(const Ack& ack) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    AcknowledgeLocked(ack);
}
void CommitCoordinator::Acknowledge(const std::vector<Ack>& acks) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    for (const auto& ack : acks) AcknowledgeLocked(ack);
}
void CommitCoordinator::AcknowledgeLocked(const Ack& ack) {
    CHECK(members_.count(ack.node));
    auto& e = Find(ack.resolution.context.id);
    CHECK(e.resolution && *e.resolution == ack.resolution) << "ACK content differs from publication";
    const auto first_round = e.clock ? Phase::Clock : (e.announce_pending ? Phase::Pending : Phase::Stage);
    if (ack.phase == first_round) {
        const bool first = e.stage_acks.insert(ack.node).second;
        if (first && e.stage_acks.size() == members_.size()) {
            if (e.announce_pending && !Ready(e)) ++acknowledged_before_proof_;
            Pump();
        }
    } else {
        CHECK(ack.phase == Phase::Advance && e.advance_sent) << "Advance ACK before all Stage ACKs";
        if (!e.advance_acks.insert(ack.node).second ||
            e.advance_acks.size() != members_.size() || e.slot != head_) return;
        while (head_ < next_advance_) {
            auto& front = Find(slots_.at(head_));
            if (front.advance_acks != members_) break;
            front.complete = true;
            ++head_;
            front.completed.notify_all();
        }
        Pump();
    }
}
std::vector<Message> CommitCoordinator::TakeMessages() {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    std::vector<Message> messages;
    // All members receive the same broadcast sequence in this transport.
    const size_t count = outboxes_.begin()->second.messages.size();
    for (const auto& entry : outboxes_) CHECK_EQ(entry.second.messages.size(), count);
    messages.reserve(count * members_.size());
    for (size_t i = 0; i < count; ++i)
        for (auto& entry : outboxes_) messages.push_back(std::move(entry.second.messages[i]));
    for (auto& entry : outboxes_) entry.second.messages.clear();
    return messages;
}
std::vector<Message> CommitCoordinator::WaitAndTakeMessages(NodeId member) {
    std::unique_lock<bthread::Mutex> lock(mutex_);
    auto& queue = outboxes_.at(member);
    while (queue.messages.empty() && !message_stop_) queue.ready.wait(lock);
    std::vector<Message> messages;
    messages.swap(queue.messages);
    return messages;
}
void CommitCoordinator::StopMessageWait() {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    message_stop_ = true;
    for (auto& entry : outboxes_) entry.second.ready.notify_all();
}
bool CommitCoordinator::WaitComplete(const TxnId& id) {
    std::unique_lock<bthread::Mutex> lock(mutex_);
    auto& e = Find(id);
    while (!e.complete) e.completed.wait(lock);
    return e.state == State::Committed;
}
Scn CommitCoordinator::HeadPosition() const {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    return head_;
}
std::pair<uint64_t, uint64_t> CommitCoordinator::PendingBeforeProof() const {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    return {announced_before_proof_, acknowledged_before_proof_};
}
Scn CommitCoordinator::NextPosition() const {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    return next_;
}
bool CommitCoordinator::Succeeded(const TxnId& id) const {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    const auto& e = Find(id);
    return e.complete && e.state == State::Committed;
}
bool CommitCoordinator::CanReleaseWrites(const TxnId& id) const {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    return Find(id).complete;
}
Status CommitCoordinator::Lookup(const TxnId& id) const {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    const auto& e = Find(id);
    return {e.state, e.state == State::Committed ? e.slot : 0};
}
Visibility Visible(const Snapshot& reader, const Version& v, std::optional<Status> s) {
    CheckVersion(reader, v);
    if (v.initial || v.creator == reader.context().id)
        return v.deleted ? Visibility::NotFound : Visibility::Value;
    if (!s) return Visibility::NeedStatus;
    CheckStatus(*s);
    if (s->state != State::Committed || s->commit_scn > reader.context().snapshot)
        return Visibility::FollowHistory;
    return v.deleted ? Visibility::NotFound : Visibility::Value;
}
WriteCheck CheckWrite(const Snapshot& writer, const Version& v, std::optional<Status> s) {
    CheckVersion(writer, v);
    if (v.initial || v.creator == writer.context().id) return WriteCheck::Allowed;
    if (!s) return WriteCheck::NeedStatus;
    CheckStatus(*s);
    if (s->state == State::Aborted) return WriteCheck::FollowHistory;
    if (s->state != State::Committed) return WriteCheck::WaitForWriter;
    return s->commit_scn > writer.context().snapshot ? WriteCheck::SnapshotConflict : WriteCheck::Allowed;
}
} // namespace mvcc
