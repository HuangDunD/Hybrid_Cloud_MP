#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <atomic>
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>
#include "compute_node/twoPC.pb.h"
#include "common.h"
#include "dtx/mvcc_2pc.h"
#include "undo/format.h"

namespace prepared_2pc {
constexpr uint32_t kParticipantReplyVersion = 1; // baseline: durable writes, participant replies + coordinator decision log
constexpr uint32_t kProtocolVersion = 2; // storage aggregates durable votes
using Requests = std::map<node_id_t, twopc_service::PrepareRequest>;

// Version zero is the original transaction-ID-only Prepare. Never downgrade
// a malformed durable request carrying metadata/write bytes into that protocol.
inline bool IsLegacyPrepare(const twopc_service::PrepareRequest& r) {
    return r.protocol_version() == 0 && !r.has_identity() && r.start_ts() == 0 &&
        r.commit_ts() == 0 && r.participants_size() == 0 && r.participant_node() == 0 &&
        r.writes_size() == 0 && !r.has_mvcc();
}

inline bool ValidIdentity(const twopc_service::TxnIdentity& id) {
    return id.coordinator_node() >= 0 && id.coordinator_incarnation().size() == 16;
}

inline bool ValidRequest(const twopc_service::PrepareRequest& r) {
    if ((r.protocol_version() != kProtocolVersion && r.protocol_version() != kParticipantReplyVersion) || !r.has_identity() ||
        !ValidIdentity(r.identity()) || r.identity().local_tx_id() != r.transaction_id() ||
        r.start_ts() == 0 || r.commit_ts() == 0 || r.participants_size() < 2 ||
        r.writes_size() == 0) return false;
    if (r.has_mvcc() &&
        (!mvcc_2pc::ValidContext(r.mvcc()) || r.mvcc().local_id() != r.transaction_id() ||
         r.mvcc().home() != r.identity().coordinator_node())) return false;
    int previous = -1;
    bool includes_self = false;
    for (int node : r.participants()) {
        if (node <= previous) return false;
        previous = node;
        includes_self |= node == r.participant_node();
    }
    if (!includes_self) return false;
    std::set<std::tuple<int, int, int>> tuples;
    for (const auto& w : r.writes()) {
        const auto& id = w.item_id();
        if (!w.has_item_id() || id.table_id() < 0 || id.page_no() <= 0 || id.slot_id() < 0 ||
            w.table_name().empty() || w.operation() != twopc_service::PreparedWrite::UPDATE ||
            !tuples.emplace(id.table_id(), id.page_no(), id.slot_id()).second) return false;
        if (!w.undo().empty() && !undo::ValidRefBytes(w.undo())) return false;
    }
    return true;
}

// A structured key avoids relying on protobuf serialization as identity equality.
using IdentityKey = std::tuple<int, std::string, uint64_t>;
inline IdentityKey Key(const twopc_service::TxnIdentity& id) {
    return {id.coordinator_node(), id.coordinator_incarnation(), id.local_tx_id()};
}
inline IdentityKey Key(const twopc_service::MvccContext& id) {
    return {id.home(), id.epoch(), id.local_id()};
}

inline twopc_service::AllPreparedRequest Metadata(const twopc_service::PrepareRequest& r) {
    twopc_service::AllPreparedRequest m;
    m.set_protocol_version(r.protocol_version());
    *m.mutable_identity() = r.identity();
    m.set_start_ts(r.start_ts());
    m.set_commit_ts(r.commit_ts());
    for (int node : r.participants()) m.add_participants(node);
    return m;
}

struct Context {
    enum State { PREPARING, PREPARED, APPLYING, APPLIED, DISCARDED };
    twopc_service::PrepareRequest request; // undo refs are attached before PREPARED
    const std::string payload;             // received bytes, used for duplicate comparison
    const bool wait_for_prepare;
    LLSN prepare_lsn = 0;
    LLSN applied_lsn = 0;
    uint64_t commit_scn = 0; // protected by this context's mutex
    bthread::Mutex mutex;
    bthread::ConditionVariable ready;
    State state = PREPARING;
    explicit Context(const twopc_service::PrepareRequest& r)
        : request(r), payload(r.SerializeAsString()),
          wait_for_prepare(r.has_mvcc() || r.protocol_version() == kProtocolVersion) {}
};

// The map lock never covers page access, I/O or per-transaction waiting.
class Registry {
public:
    std::shared_ptr<Context> Begin(const twopc_service::PrepareRequest& r, bool* inserted) {
        auto context = std::make_shared<Context>(r);
        std::lock_guard<std::mutex> guard(mutex_);
        auto result = contexts_.emplace(Key(r.identity()), context);
        *inserted = result.second;
        if (!result.second && result.first->second->payload != context->payload) return nullptr;
        return result.first->second;
    }
    bool IsPrepared(const std::shared_ptr<Context>& c) {
        std::lock_guard<bthread::Mutex> guard(c->mutex);
        return c->state == Context::PREPARED;
    }
    void MarkPrepared(const std::shared_ptr<Context>& c, LLSN lsn) {
        std::lock_guard<bthread::Mutex> guard(c->mutex);
        CHECK(c->state == Context::PREPARING);
        CHECK(lsn > 0);
        c->prepare_lsn = lsn;
        c->state = Context::PREPARED;
        c->ready.notify_all();
    }
    // Returns the context in APPLYING, or an already APPLIED context for a
    // legitimate duplicate Commit. A different commit_scn is a contradiction.
    std::shared_ptr<Context> TakeForCommit(const twopc_service::CommitRequest& r) {
        std::shared_ptr<Context> c;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            auto it = contexts_.find(Key(r.identity()));
            if (it == contexts_.end()) return nullptr;
            c = it->second;
        }
        std::unique_lock<bthread::Mutex> guard(c->mutex);
        // A storage notification may precede the participant's flush reply.
        // The Prepare owner populates the shared request until MarkPrepared.
        while (c->state == Context::PREPARING && c->wait_for_prepare) c->ready.wait(guard);
        if (c->state == Context::PREPARING || c->state == Context::DISCARDED) return nullptr;
        if (c->request.transaction_id() != r.transaction_id() ||
            c->request.start_ts() != r.start_ts() || c->request.commit_ts() != r.commit_ts() ||
            c->request.has_mvcc() != r.has_mvcc() ||
            (r.has_mvcc() && !mvcc_2pc::SameContext(c->request.mvcc(), r.mvcc()))) return nullptr;
        while (r.has_mvcc() && c->state == Context::APPLYING) {
            if (r.commit_scn() != c->commit_scn) return nullptr;
            c->ready.wait(guard);
        }
        if (c->state == Context::APPLIED) {
            if (r.has_mvcc() && r.commit_scn() != c->commit_scn) return nullptr;
            return c;
        }
        if (c->state != Context::PREPARED) return nullptr;
        if (r.has_mvcc()) {
            if (r.commit_scn() <= c->request.mvcc().snapshot_scn()) return nullptr;
            c->commit_scn = r.commit_scn();
        }
        c->state = Context::APPLYING;
        return c;
    }
    void MarkApplied(const std::shared_ptr<Context>& c, uint64_t durable_lsn) {
        std::lock_guard<bthread::Mutex> guard(c->mutex);
        CHECK(c->state == Context::APPLYING);
        CHECK(durable_lsn > 0);
        CHECK(durable_lsn >= c->prepare_lsn);
        c->applied_lsn = durable_lsn;
        c->state = Context::APPLIED;
        c->ready.notify_all();
    }
    uint64_t PreparePrefix(const std::shared_ptr<Context>& c) {
        std::unique_lock<bthread::Mutex> guard(c->mutex);
        while (c->state == Context::PREPARING) c->ready.wait(guard);
        return c->state == Context::DISCARDED ? 0 : c->prepare_lsn;
    }
    bool CanRelease(const twopc_service::ReleaseRequest& r) {
        std::lock_guard<std::mutex> guard(mutex_);
        const auto it = contexts_.find(Key(r.identity()));
        if (it == contexts_.end()) return false;
        const auto& c = *it->second;
        std::lock_guard<bthread::Mutex> entry_guard(it->second->mutex);
        return c.state == Context::APPLIED && c.commit_scn == r.commit_scn() &&
               c.request.start_ts() == r.start_ts() &&
               mvcc_2pc::SameContext(c.request.mvcc(), r.mvcc());
    }

    size_t Size() {
        std::lock_guard<std::mutex> guard(mutex_);
        return contexts_.size();
    }
    void Erase(const std::shared_ptr<Context>& c) {
        std::lock_guard<std::mutex> guard(mutex_);
        {
            std::lock_guard<bthread::Mutex> entry_guard(c->mutex);
            c->state = Context::DISCARDED;
            c->ready.notify_all();
        }
        contexts_.erase(Key(c->request.identity()));
    }
private:
    mutable std::mutex mutex_;
    std::map<IdentityKey, std::shared_ptr<Context>> contexts_;
};

// Single-writer shortcut: no durable Prepare is required, but a duplicate
// Commit must still be answered with the original applied prefix.
struct Single {
    twopc_service::CommitRequest request;
    const std::string payload;
    uint64_t durable_lsn = 0;
    bthread::Mutex mutex;
    bthread::ConditionVariable ready;
    bool applied = false;
    bool rejected = false;
    explicit Single(const twopc_service::CommitRequest& r) : request(r), payload(r.SerializeAsString()) {}
};
class SingleWrites {
public:
    std::shared_ptr<Single> Begin(const twopc_service::CommitRequest& r, bool* inserted) {
        auto single = std::make_shared<Single>(r);
        std::lock_guard<std::mutex> guard(mutex_);
        auto result = singles_.emplace(Key(r.mvcc()), single);
        *inserted = result.second;
        if (!result.second && result.first->second->payload != single->payload) return nullptr;
        return result.first->second;
    }
    void Applied(const std::shared_ptr<Single>& s, uint64_t durable_lsn) {
        std::lock_guard<bthread::Mutex> guard(s->mutex);
        CHECK(!s->applied && !s->rejected && durable_lsn > 0);
        s->durable_lsn = durable_lsn;
        s->applied = true;
        s->ready.notify_all();
    }
    void Reject(const std::shared_ptr<Single>& s) {
        std::lock_guard<bthread::Mutex> guard(s->mutex);
        CHECK(!s->applied && !s->rejected);
        s->rejected = true;
        s->ready.notify_all();
    }
    uint64_t WaitApplied(const std::shared_ptr<Single>& s) {
        std::unique_lock<bthread::Mutex> guard(s->mutex);
        while (!s->applied && !s->rejected) s->ready.wait(guard);
        return s->durable_lsn;
    }
    bool AppliedPrefix(const std::shared_ptr<Single>& s, uint64_t* durable_lsn) {
        std::lock_guard<bthread::Mutex> guard(s->mutex);
        if (!s->applied) return false;
        *durable_lsn = s->durable_lsn;
        return true;
    }
    bool CanRelease(const twopc_service::ReleaseRequest& r) {
        std::lock_guard<std::mutex> guard(mutex_);
        const auto it = singles_.find(Key(r.mvcc()));
        if (it == singles_.end()) return false;
        const auto& s = *it->second;
        std::lock_guard<bthread::Mutex> entry_guard(it->second->mutex);
        // The single-writer install precedes allocation of its global SCN.
        return s.applied && s.request.start_ts() == r.start_ts() &&
               mvcc_2pc::SameContext(s.request.mvcc(), r.mvcc());
    }
    size_t Size() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return singles_.size();
    }
private:
    mutable std::mutex mutex_;
    std::map<IdentityKey, std::shared_ptr<Single>> singles_;
};
class Releases {
public:
    enum class Kind { Finish, Single, Prepared };
    struct Entry {
        bthread::Mutex mutex;
        bthread::ConditionVariable ready;
        Kind kind = Kind::Finish;
        std::optional<std::string> payload;
        bool done = false;
    };

    bool ExpectWriter(const twopc_service::MvccContext& context, bool prepared) {
        auto entry = Get(Key(context));
        std::lock_guard<bthread::Mutex> guard(entry->mutex);
        const auto kind = prepared ? Kind::Prepared : Kind::Single;
        if (entry->payload || (entry->kind != Kind::Finish && entry->kind != kind)) return false;
        entry->kind = kind;
        return true;
    }
    std::shared_ptr<Entry> Begin(const twopc_service::ReleaseRequest& request, bool& owner) {
        auto entry = Get(Key(request.mvcc()));
        auto payload = request.SerializeAsString();
        std::unique_lock<bthread::Mutex> guard(entry->mutex);
        owner = false;
        const auto kind = request.finish_only() ? Kind::Finish :
                          (request.prepared() ? Kind::Prepared : Kind::Single);
        if (kind != entry->kind) return nullptr;
        if (!entry->payload) {
            entry->payload = std::move(payload);
            owner = true;
            return entry;
        }
        if (*entry->payload != payload) return nullptr;
        while (!entry->done) entry->ready.wait(guard);
        return entry;
    }

    void Complete(const std::shared_ptr<Entry>& entry) {
        std::lock_guard<bthread::Mutex> guard(entry->mutex);
        CHECK(!entry->done);
        entry->done = true;
        if (entry->kind == Kind::Finish) ++finished_;
        entry->ready.notify_all();
    }
    size_t Finished() const { return finished_.load(); }
private:
    std::mutex mutex_;
    std::map<IdentityKey, std::shared_ptr<Entry>> entries_;
    std::shared_ptr<Entry> Get(const IdentityKey& key) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto [it, inserted] = entries_.try_emplace(key);
        if (inserted) it->second = std::make_shared<Entry>();
        return it->second;
    }
    std::atomic<size_t> finished_{0};
};
} // namespace prepared_2pc
