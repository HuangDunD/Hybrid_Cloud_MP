#pragma once

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

/*
    这个文件主要负责事务可见性相关的工作：
    可见性是参考 Oracle RAC 的 SCN 来做的，即一个事务
    如果有写的话，那就提交的时候广播新的 SCN
*/

namespace mvcc {
using Scn = uint64_t;
using NodeId = uint32_t;

struct TxnId {
    std::string epoch;
    NodeId home = 0;
    uint64_t local = 0;
    auto Key() const { return std::tie(epoch, home, local); }
    bool operator==(const TxnId& x) const { return Key() == x.Key(); }
    bool operator<(const TxnId& x) const { return Key() < x.Key(); }
};
struct Context {
    TxnId id;
    Scn snapshot = 0;
    bool operator==(const Context& x) const { return id == x.id && snapshot == x.snapshot; }
};
struct WalPosition {
    std::string epoch;
    NodeId writer = 0;
    uint64_t lsn = 0;
    bool operator==(const WalPosition& x) const {
        return epoch == x.epoch && writer == x.writer && lsn == x.lsn;
    }
};
enum class State { Active, Preparing, Installing, Aborting, Committed, Aborted, ReadOnlyFinished };
struct Status {
    State state = State::Active;
    Scn commit_scn = 0; // nonzero only for globally resolved COMMITTED
};
enum class CommitKind { SingleWriter, TwoPhase, Cornus };
struct Plan {
    Context context;
    CommitKind kind = CommitKind::SingleWriter;
    // Every node that installs a main-row version. A read-only participant is
    // not a writer and never appears here.
    std::set<NodeId> writers;
    // Required WAL prefix in each writer's stream. Cornus may leave this empty:
    // its evidence is the storage-observed durable YES set, reported separately.
    std::map<NodeId, uint64_t> wal;
    std::map<NodeId, uint64_t> prepare; // empty on the single-writer shortcut
    std::optional<WalPosition> decision;
    bool operator==(const Plan& x) const {
        return context == x.context && kind == x.kind && writers == x.writers &&
            wal == x.wal && prepare == x.prepare && decision == x.decision;
    }
};
struct Resolution {
    Context context;
    Scn slot = 0;
    bool committed = false;
    bool operator==(const Resolution& x) const {
        return context == x.context && slot == x.slot && committed == x.committed;
    }
};
enum class Phase { Stage, Advance };
struct Message {
    NodeId node = 0;
    Phase phase = Phase::Stage;
    Resolution resolution;
};
using Ack = Message; // ACK echoes the full immutable envelope, never a bare max SCN.

// A snapshot handle has no mutator. Only SnapshotNode can issue it.
class Snapshot {
public:
    const Context& context() const { return context_; }
private:
    explicit Snapshot(Context c) : context_(std::move(c)) {}
    Context context_;
    friend class SnapshotNode;
};

class SnapshotNode {
public:
    SnapshotNode(std::string epoch, NodeId self, std::set<NodeId> members);
    Snapshot Begin(uint64_t local_id);
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
    Ack Receive(const Message& message);
    std::optional<Status> Lookup(const TxnId& id) const;
    Scn SafeScn() const;
    Scn RetentionFloor() const;
private:
    struct Registration {
        Context context;
        bool ended = false;
        std::map<NodeId, bool> exports; // true = remote execution drained
    };
    void Validate(const Context& context) const;
    const std::string epoch_;
    const NodeId self_;
    const std::set<NodeId> members_;
    mutable std::mutex mutex_;
    Scn staged_ = 0;
    Scn safe_ = 0;
    std::map<TxnId, Registration> registrations_;
    std::map<TxnId, Status> outcomes_;
    std::map<Scn, Resolution> resolutions_; // retained, including duplicate validation
};

// Transport-independent state machine. Methods take short mutexes; callers drain
// the outbox and perform RPC outside them. No I/O, callbacks or page waits here.
class CommitCoordinator {
public:
    CommitCoordinator(std::string epoch, std::set<NodeId> members);
    Scn Reserve(const Plan& plan);
    void DecideCommit(const TxnId& id);
    // A durable YES set observed by storage (Cornus protocol version 2) cannot
    // name individual prepare LSNs here; this flag is the complete quorum proof.
    void PreparedQuorum(const TxnId& id);
    void Prepared(const TxnId& id, const WalPosition& durable);
    void WalDurable(const TxnId& id, const WalPosition& durable);
    void DecisionDurable(const TxnId& id, const WalPosition& durable);
    void Installed(const TxnId& id, NodeId writer);
    void Abort(const TxnId& id, const std::map<NodeId, uint64_t>& rollback_targets);
    void RolledBack(const TxnId& id, const WalPosition& durable);
    void Acknowledge(const Ack& ack);
    // Publisher owns the outbox. Accept every individual ACK under one lock
    // and return only messages released by the usual ordered state machine.
    std::vector<Message> AcknowledgeAndTakeMessages(const std::vector<Ack>& acks);
    std::vector<Message> TakeMessages();
    std::vector<Message> WaitAndTakeMessages();
    void StopMessageWait();
    bool WaitComplete(const TxnId& id);
    Scn HeadPosition() const;
    Scn NextPosition() const;
    bool Succeeded(const TxnId& id) const;
    bool CanReleaseWrites(const TxnId& id) const;
    Status Lookup(const TxnId& id) const;
private:
    struct Entry {
        Plan plan;
        Scn slot = 0;
        State state = State::Active;
        std::map<NodeId, uint64_t> wal, prepared, rollback_targets, rolled_back;
        uint64_t decision = 0;
        bool prepared_quorum = false;
        std::set<NodeId> installed, stage_acks, advance_acks;
        std::optional<Resolution> resolution;
        bool advance_sent = false;
        bool complete = false;
        bthread::ConditionVariable completed;
    };
    Entry& Find(const TxnId& id);
    const Entry& Find(const TxnId& id) const;
    void Validate(const Plan& plan) const;
    void Evidence(const WalPosition& durable,
                  const std::map<NodeId, uint64_t>& targets,
                  std::map<NodeId, uint64_t>& observed);
    bool Ready(const Entry& entry) const;
    void AcknowledgeLocked(const Ack& ack);
    void Pump();
    void Broadcast(Phase phase, const Resolution& resolution);
    const std::string epoch_;
    const std::set<NodeId> members_;
    mutable bthread::Mutex mutex_;
    Scn next_ = 1;
    Scn head_ = 1;
    std::map<TxnId, Entry> entries_;
    std::map<Scn, TxnId> slots_;
    std::vector<Message> outbox_;
    bthread::ConditionVariable message_cv_;
    bool message_stop_ = false;
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
