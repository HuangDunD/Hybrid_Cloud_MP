#pragma once

#include <brpc/channel.h>
#include "compute_node/prepared_txn.h"

namespace page_cost { class OnlineCommitCost; }

namespace prepared_2pc {

// In-flight RPC work that must finish before the cluster drains.
class PendingRpcs {
public:
    void Add();
    void Done();
    void Drain();
private:
    bthread::Mutex mutex;
    bthread::ConditionVariable cv;
    size_t pending = 0;
};

// Coordinator-side transient waiters. Prepare RPCs own their messages through
// completion; their responses are drained at shutdown, not before Commit starts.
class PrepareNotifications {
public:
    ~PrepareNotifications() { Drain(); }
    bool Prepare(const Requests& requests, brpc::Channel* channels);
    void Commit(const Requests& requests, brpc::Channel* channels,
                page_cost::OnlineCommitCost* cost_model = nullptr);
    bool Accept(const twopc_service::AllPreparedRequest& notification);
    void Drain();

private:
    struct Waiter {
        explicit Waiter(std::string m) : metadata(std::move(m)) {}
        ~Waiter();
        std::atomic<uint64_t> last_reply_ns{0};
        uint64_t notified_ns = 0;
        const std::string metadata;
        bthread::Mutex mutex;
        bthread::ConditionVariable cv;
        bool durable = false;
    };
    struct Call;
    static void RPCDone(Call* call);
    std::mutex mutex_;
    std::map<IdentityKey, std::shared_ptr<Waiter>> waiters_;
    std::shared_ptr<PendingRpcs> transport_ = std::make_shared<PendingRpcs>();
};
} // namespace prepared_2pc
