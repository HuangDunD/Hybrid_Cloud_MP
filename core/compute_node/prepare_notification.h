#pragma once

#include <brpc/channel.h>
#include "compute_node/prepared_txn.h"

namespace prepared_2pc {

// Coordinator-side transient waiters. Prepare RPCs own their messages through
// completion; their responses are drained at shutdown, not before Commit starts.
class PrepareNotifications {
public:
    ~PrepareNotifications() { Drain(); }
    bool Prepare(const Requests& requests, brpc::Channel* channels);
    void Commit(const Requests& requests, brpc::Channel* channels);
    bool Accept(const twopc_service::AllPreparedRequest& notification);
    void Drain();

private:
    struct Waiter {
        explicit Waiter(std::string m) : metadata(std::move(m)) {}
        const std::string metadata;
        bthread::Mutex mutex;
        bthread::ConditionVariable cv;
        bool durable = false;
    };
    struct Transport {
        bthread::Mutex mutex;
        bthread::ConditionVariable cv;
        size_t pending = 0;
        void Add();
        void Done();
        void Drain();
    };
    struct Call;
    static void RPCDone(Call* call);
    std::mutex mutex_;
    std::map<IdentityKey, std::shared_ptr<Waiter>> waiters_;
    std::shared_ptr<Transport> transport_ = std::make_shared<Transport>();
};
} // namespace prepared_2pc
