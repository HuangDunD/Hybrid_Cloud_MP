#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <tuple>
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>
#include "compute_node/twoPC.pb.h"
#include "common.h"

namespace prepared_2pc {
constexpr uint32_t kParticipantReplyVersion = 1; // baseline: durable writes, participant replies + coordinator decision log
constexpr uint32_t kProtocolVersion = 2; // storage aggregates durable votes
using Requests = std::map<node_id_t, twopc_service::PrepareRequest>;

// Version zero is the original transaction-ID-only Prepare. Never downgrade
// a malformed durable request carrying metadata/write bytes into that protocol.
inline bool IsLegacyPrepare(const twopc_service::PrepareRequest& r) {
    return r.protocol_version() == 0 && !r.has_identity() && r.start_ts() == 0 &&
        r.commit_ts() == 0 && r.participants_size() == 0 && r.participant_node() == 0 &&
        r.writes_size() == 0;
}

inline bool ValidIdentity(const twopc_service::TxnIdentity& id) {
    return id.coordinator_node() >= 0 && id.coordinator_incarnation().size() == 16;
}

inline bool ValidRequest(const twopc_service::PrepareRequest& r) {
    if ((r.protocol_version() != kProtocolVersion && r.protocol_version() != kParticipantReplyVersion) || !r.has_identity() ||
        !ValidIdentity(r.identity()) || r.identity().local_tx_id() != r.transaction_id() ||
        r.start_ts() == 0 || r.commit_ts() == 0 || r.participants_size() < 2 ||
        r.writes_size() == 0) return false;
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
    }
    return true;
}

// A structured key avoids relying on protobuf serialization as identity equality.
using IdentityKey = std::tuple<int, std::string, uint64_t>;
inline IdentityKey Key(const twopc_service::TxnIdentity& id) {
    return {id.coordinator_node(), id.coordinator_incarnation(), id.local_tx_id()};
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
    enum State { PREPARING, PREPARED, APPLYING };
    const twopc_service::PrepareRequest request;
    const std::string payload;
    LLSN prepare_lsn = 0;
    bthread::Mutex mutex;
    bthread::ConditionVariable ready;
    State state = PREPARING; // protected by this context's mutex
    explicit Context(const twopc_service::PrepareRequest& r)
        : request(r), payload(r.SerializeAsString()) {}
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
        c->prepare_lsn = lsn;
        c->state = Context::PREPARED;
        c->ready.notify_all();
    }
    std::shared_ptr<Context> TakeForCommit(const twopc_service::CommitRequest& r) {
        std::shared_ptr<Context> c;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            auto it = contexts_.find(Key(r.identity()));
            if (it == contexts_.end()) return nullptr;
            c = it->second;
        }
        if (c->request.transaction_id() != r.transaction_id() ||
            c->request.start_ts() != r.start_ts() || c->request.commit_ts() != r.commit_ts()) return nullptr;
        std::unique_lock<bthread::Mutex> guard(c->mutex);
        // Storage's notification may overtake LogWrite's response and the local
        // MarkPrepared. Wait for that transition without any page or map lock.
        while (c->state == Context::PREPARING && c->request.protocol_version() == kProtocolVersion)
            c->ready.wait(guard);
        if (c->state != Context::PREPARED) return nullptr;
        c->state = Context::APPLYING;
        return c;
    }
    void Erase(const std::shared_ptr<Context>& c) {
        std::lock_guard<std::mutex> guard(mutex_);
        contexts_.erase(Key(c->request.identity()));
    }
private:
    std::mutex mutex_;
    std::map<IdentityKey, std::shared_ptr<Context>> contexts_;
};
} // namespace prepared_2pc
