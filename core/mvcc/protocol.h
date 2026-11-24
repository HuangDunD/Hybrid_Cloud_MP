#pragma once

#include <bthread/countdown_event.h>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace mvcc {
using Timestamp = uint64_t;
using NodeId = uint32_t;

struct TxnId {
    std::string epoch;
    NodeId home = 0;
    uint64_t local = 0;
    auto Key() const { return std::tie(epoch, home, local); }
    bool operator==(const TxnId& x) const { return Key() == x.Key(); }
    bool operator<(const TxnId& x) const { return Key() < x.Key(); }
};
// Pre-upgrade WAL without timestamps shares one recovery floor. Later WAL
// preserves each exact commit timestamp, including gaps in the TSO sequence.
struct Recovery {
    Timestamp max_commit_ts = 0;
    uint64_t last_local = 0;
    uint32_t undo_pages = 0;
    struct Range {
        uint64_t first, last;
        Timestamp first_commit_ts = 0;
    };
    using IdRanges = std::map<NodeId, std::vector<Range>>;
    IdRanges committed;
};
struct Context {
    TxnId id;
    Timestamp snapshot = 0;
    bool operator==(const Context& x) const { return id == x.id && snapshot == x.snapshot; }
};
enum class State { Active, Committing, Committed, Aborted, ReadOnlyFinished };
struct Status {
    State state = State::Active;
    Timestamp commit_ts = 0; // nonzero only for a durable commit decision
};
// A snapshot handle has no mutator. Only SnapshotNode can issue it.
class Snapshot {
public:
    const Context& context() const { return context_; }
private:
    explicit Snapshot(Context c) : context_(std::move(c)) {}
    Context context_;
    friend class SnapshotNode;
};

// Both operations belong to one cluster-wide timestamp order. Snapshot reads the last
// timestamp issued. Commit allocates a fresh timestamp above the view.
struct RemoteTso {
    std::function<Timestamp()> snapshot;
    std::function<Timestamp(Timestamp)> commit;
};

class SnapshotNode {
public:
    SnapshotNode(std::string epoch, NodeId self, std::set<NodeId> members,
                 RemoteTso remote_tso, const Recovery& recovery = {});
    Snapshot Begin(uint64_t local_id);
    uint64_t PendingTitWaiters() const;
    Timestamp AllocateCommitTs(const Context& context);
    void CertifyRemoteCommit(const Context& context, Timestamp commit_ts);
    std::optional<Status> ReadCreatorAt(const TxnId& creator, Timestamp snapshot) const;
    void CacheRemoteTerminal(const TxnId& creator, const Status& status) const;
    // Export pins home until RemoteFinished; one execution lifetime per participant.
    Context Export(const Snapshot& snapshot, NodeId participant);
    bool Attach(const Context& context); // false for a delayed duplicate after Finish
    Snapshot Imported(const Context& context) const;
    // Home-side participant view: the transaction that began here also owns
    // rows on this node, so it reuses its own registration instead of Attach.
    Snapshot Local(const Context& context) const;
    void Finish(const Context& context);
    void RemoteFinished(const Context& context, NodeId participant);
    void End(const Snapshot& snapshot, State outcome = State::ReadOnlyFinished);
    std::optional<Status> Lookup(const TxnId& id) const;
private:
    struct Registration {
        Context context;
        bool ended = false;
        std::map<NodeId, bool> exports; // true = remote execution drained
    };
    struct PendingCommit {
        Timestamp commit_ts = 0; // the home installs this entry before asking the TSO
        mutable std::shared_ptr<bthread::CountdownEvent> changed;
        std::shared_ptr<bthread::CountdownEvent> WaitHandle() const;
        void Notify();
    };
    std::map<TxnId, PendingCommit> committing_;
    mutable uint64_t tit_waiters_ = 0;
    const RemoteTso remote_tso_;
    std::optional<Status> LookupLocked(const TxnId& id) const;
    void Validate(const Context& context) const;
    const std::string epoch_;
    const Recovery::IdRanges recovered_commits_;
    const Timestamp recovery_floor_;
    Timestamp legacy_floor_ = 0;
    const NodeId self_;
    const std::set<NodeId> members_;
    mutable std::mutex mutex_;
    std::map<TxnId, Registration> registrations_;
    mutable std::map<TxnId, Status> outcomes_;
};

struct Version {
    TxnId creator; // independent of any current tuple-lock owner
    bool initial = false;
    bool deleted = false;
};
enum class Visibility { Value, NotFound, FollowHistory, NeedStatus };
enum class WriteCheck { Allowed, SnapshotConflict, WaitForWriter, FollowHistory, NeedStatus };
// status is resolved from the candidate's creator, never from the tuple lock.
Visibility Visible(const Snapshot& reader, const Version& version, std::optional<Status> status);
// Caller holds the row write permission and scans past aborted versions. Own
// versions are allowed only after the first-overwrite check has already passed.
WriteCheck CheckWrite(const Snapshot& writer, const Version& latest, std::optional<Status> status);
} // namespace mvcc
