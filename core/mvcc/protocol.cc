#include "mvcc/protocol.h"
#include "dtx/benchmark_stats.h"
#include <algorithm>
#include <butil/logging.h>

namespace mvcc {
namespace {
void ValidEpoch(const std::string& epoch) {
    CHECK(epoch.size() == 16 && epoch != std::string(16, '\0')) << "invalid MVCC epoch";
}
void CheckStatus(const Status& status) {
    switch (status.state) {
        case State::Active: case State::Committing: case State::Committed: case State::Aborted:
        case State::ReadOnlyFinished: break;
        default: LOG(FATAL) << "invalid transaction state";
    }
    CHECK((status.state == State::Committed) == (status.commit_ts != 0))
        << "only COMMITTED has a commit timestamp";
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
                           RemoteTso remote_tso, const Recovery& recovery)
    : remote_tso_(std::move(remote_tso)), epoch_(std::move(epoch)), recovered_commits_(recovery.committed),
      recovery_floor_(recovery.max_commit_ts), self_(self), members_(std::move(members)) {
    CHECK(remote_tso_.snapshot && remote_tso_.commit);
    ValidEpoch(epoch_);
    CHECK(members_.count(self_) == 1);
    Timestamp legacy_count = 0;
    std::map<Timestamp, Timestamp> stamped_ranges; // disjoint across all writer streams
    for (const auto& [home, ranges] : recovered_commits_) {
        CHECK(members_.count(home));
        uint64_t previous = 0;
        for (auto [first, last, first_commit_ts] : ranges) {
            CHECK(previous < first && first <= last && last < (UINT64_MAX >> 8));
            if (first_commit_ts) {
                CHECK(first_commit_ts <= recovery.max_commit_ts && last - first <= recovery.max_commit_ts - first_commit_ts);
                CHECK(stamped_ranges.emplace(first_commit_ts, first_commit_ts + (last - first)).second);
            } else {
                CHECK(last - first + 1 <= recovery.max_commit_ts - legacy_count);
                legacy_count += last - first + 1;
            }
            previous = last;
        }
    }
    Timestamp previous_ts = legacy_count;
    for (const auto& [first, last] : stamped_ranges) {
        CHECK(first > previous_ts) << "overlapping commit timestamps in recovery image";
        previous_ts = last;
    }
    CHECK(previous_ts == recovery.max_commit_ts);
    legacy_floor_ = legacy_count;
}
void SnapshotNode::Validate(const Context& context) const {
    CHECK(context.id.epoch == epoch_ && context.id.local != 0 && members_.count(context.id.home))
        << "invalid transaction identity";
}
Snapshot SnapshotNode::Begin(uint64_t local_id) {
    Context c{{epoch_, self_, local_id}, remote_tso_.snapshot()};
    CHECK(c.snapshot >= recovery_floor_);
    std::lock_guard<std::mutex> guard(mutex_);
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
    std::lock_guard<std::mutex> guard(mutex_);
    Validate(c);
    CHECK(c.id.home != self_ && c.snapshot >= recovery_floor_);
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
    CHECK(!committing_.count(c.id)) << "End before durable TIT certification";
    auto known = outcomes_.find(c.id);
    if (known != outcomes_.end()) {
        CHECK(known->second.state == outcome) << "conflicting transaction end";
    } else {
        CHECK(outcome != State::Committed) << "End before commit certification";
        outcomes_.emplace(c.id, Status{outcome, 0});
    }
    it->second.ended = true; // terminal outcome, if any, is retained separately
}
std::shared_ptr<bthread::CountdownEvent> SnapshotNode::PendingCommit::WaitHandle() const {
    if (!changed) changed = std::make_shared<bthread::CountdownEvent>();
    return changed;
}
void SnapshotNode::PendingCommit::Notify() {
    if (changed) {
        changed->signal();
        changed.reset();
    }
}
namespace {
// The pending entry can be erased while its waiters sleep. Each waiter retains
// its own reference, and checks current state under the home mutex on wake.
void WaitForChange(std::shared_ptr<bthread::CountdownEvent> event, std::unique_lock<std::mutex>& lock) {
    lock.unlock();
    CHECK_EQ(event->wait(), 0);
    lock.lock();
}
}
Timestamp SnapshotNode::AllocateCommitTs(const Context& c) {
    CHECK(c.id.home == self_);
    Validate(c);
    std::unique_lock<std::mutex> lock(mutex_);
    auto registration = registrations_.find(c.id);
    CHECK(registration != registrations_.end() && registration->second.context == c);
    for (;;) {
        auto known = outcomes_.find(c.id);
        if (known != outcomes_.end()) {
            CHECK(known->second.state == State::Committed);
            return known->second.commit_ts;
        }
        CHECK(!registration->second.ended);
        // A reader must see this intent before the shared TSO can expose C.
        auto [entry, inserted] = committing_.try_emplace(c.id);
        if (inserted) break;
        if (entry->second.commit_ts) return entry->second.commit_ts;
        WaitForChange(entry->second.WaitHandle(), lock);
    }
    lock.unlock();
    const auto commit_ts = remote_tso_.commit(c.snapshot);
    CHECK(commit_ts > c.snapshot);
    lock.lock();
    auto& pending = committing_.at(c.id);
    CHECK(pending.commit_ts == 0);
    pending.commit_ts = commit_ts;
    pending.Notify(); // readers with older snapshots can now follow undo
    return commit_ts;
}
void SnapshotNode::CertifyRemoteCommit(const Context& c, Timestamp commit_ts) {
    benchmark_stats::ScopedTimer timer(benchmark_stats::tit_local_commit);
    CHECK(c.id.home == self_ && commit_ts > c.snapshot);
    Validate(c);
    std::lock_guard<std::mutex> lock(mutex_);
    auto registration = registrations_.find(c.id);
    CHECK(registration != registrations_.end() && registration->second.context == c);
    auto known = outcomes_.find(c.id);
    if (known != outcomes_.end()) {
        CHECK(known->second.state == State::Committed && known->second.commit_ts == commit_ts);
        return;
    }
    auto pending = committing_.find(c.id);
    CHECK(pending != committing_.end() && pending->second.commit_ts == commit_ts && !registration->second.ended);
    outcomes_.emplace(c.id, Status{State::Committed, commit_ts});
    pending->second.Notify();
    committing_.erase(pending);
}
std::optional<Status> SnapshotNode::ReadCreatorAt(const TxnId& id, Timestamp snapshot) const {
    CHECK(id.home == self_ && snapshot >= recovery_floor_);
    Validate({id, snapshot});
    std::unique_lock<std::mutex> lock(mutex_);
    std::optional<benchmark_stats::ScopedTimer> waiting;
    for (;;) {
        auto entry = committing_.find(id);
        if (entry == committing_.end()) return LookupLocked(id);
        // Before C is bound even the writer doesn't know whether C <= S.
        // Once bound, readers older than C can bypass this pending creator.
        if (entry->second.commit_ts > snapshot) return Status{State::Committing, 0};
        if (!waiting) waiting.emplace(benchmark_stats::tit_pending_wait);
        ++tit_waiters_;
        WaitForChange(entry->second.WaitHandle(), lock);
        --tit_waiters_;
    }
}
uint64_t SnapshotNode::PendingTitWaiters() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tit_waiters_;
}
void SnapshotNode::CacheRemoteTerminal(const TxnId& id, const Status& status) const {
    CHECK(id.home != self_);
    Validate({id, 0});
    CheckStatus(status);
    CHECK(status.state == State::Committed || status.state == State::Aborted);
    std::lock_guard<std::mutex> guard(mutex_);
    const auto& known = outcomes_.emplace(id, status).first->second;
    CHECK(known.state == status.state && known.commit_ts == status.commit_ts);
}
std::optional<Status> SnapshotNode::Lookup(const TxnId& id) const {
    std::lock_guard<std::mutex> guard(mutex_);
    return LookupLocked(id);
}
std::optional<Status> SnapshotNode::LookupLocked(const TxnId& id) const {
    Validate({id, 0});
    auto outcome = outcomes_.find(id);
    if (outcome != outcomes_.end()) return outcome->second;
    if (committing_.count(id)) return Status{State::Committing, 0};
    auto active = registrations_.find(id);
    if (active != registrations_.end() && !active->second.ended) return Status{State::Active, 0};
    const auto home = recovered_commits_.find(id.home);
    if (home != recovered_commits_.end()) {
        const auto& ranges = home->second;
        auto it = std::lower_bound(ranges.begin(), ranges.end(), id.local,
            [](const auto& range, uint64_t local) { return range.last < local; });
        if (it != ranges.end() && it->first <= id.local)
            return Status{State::Committed, it->first_commit_ts ? it->first_commit_ts + (id.local - it->first) : legacy_floor_};
    }
    return std::nullopt; // unknown requires authoritative resolution, never history fallback
}
Visibility Visible(const Snapshot& reader, const Version& v, std::optional<Status> s) {
    CheckVersion(reader, v);
    if (v.initial || v.creator == reader.context().id)
        return v.deleted ? Visibility::NotFound : Visibility::Value;
    if (!s) return Visibility::NeedStatus;
    CheckStatus(*s);
    if (s->state != State::Committed || s->commit_ts > reader.context().snapshot)
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
    return s->commit_ts > writer.context().snapshot ? WriteCheck::SnapshotConflict : WriteCheck::Allowed;
}
} // namespace mvcc
