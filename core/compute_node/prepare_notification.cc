#include "compute_node/prepare_notification.h"
#include <brpc/controller.h>
#include <butil/logging.h>
#include "storage/prepared_log_record.h"
#include "compute_node/online_commit_rpc.h"

namespace prepared_2pc {
struct PrepareNotifications::Call {
    twopc_service::PrepareRequest request;
    twopc_service::PrepareResponse response;
    brpc::Controller controller;
    std::shared_ptr<Transport> transport;
};

void PrepareNotifications::Transport::Add() {
    std::lock_guard<bthread::Mutex> guard(mutex);
    ++pending;
}
void PrepareNotifications::Transport::Done() {
    std::lock_guard<bthread::Mutex> guard(mutex);
    CHECK(pending > 0);
    if (--pending == 0) cv.notify_all();
}
void PrepareNotifications::Transport::Drain() {
    std::unique_lock<bthread::Mutex> guard(mutex);
    while (pending != 0) cv.wait(guard);
}
void PrepareNotifications::Drain() { transport_->Drain(); }

void PrepareNotifications::RPCDone(Call* call) {
    // A failed RPC is never a vote or an instruction to abort durable YESes.
    CHECK(!call->controller.Failed() && call->response.ok())
        << "Prepare RPC failed; recovery is not implemented: " << call->controller.ErrorText();
    auto transport = call->transport;
    delete call;
    transport->Done();
}

bool PrepareNotifications::Prepare(const Requests& requests, brpc::Channel* channels) {
    CHECK(requests.size() > 1);
    const auto& first = requests.begin()->second;
    const auto expected = Metadata(first).SerializeAsString();
    CHECK(first.participants_size() == requests.size());
    for (int node : first.participants()) CHECK(requests.count(node) == 1);
    for (const auto& entry : requests) {
        CHECK(entry.first == entry.second.participant_node());
        CHECK(entry.second.protocol_version() == kProtocolVersion && ValidRequest(entry.second));
        CHECK(PreparedLogRecord::Fits(entry.second));
        CHECK(Metadata(entry.second).SerializeAsString() == expected);
    }
    auto waiter = std::make_shared<Waiter>(expected);
    const auto key = Key(first.identity());
    {
        std::lock_guard<std::mutex> guard(mutex_);
        // Register before sending: a fast notification must never be lost.
        CHECK(waiters_.emplace(key, waiter).second);
    }
    for (const auto& entry : requests) {
        auto* call = new Call;
        call->request = entry.second;
        call->transport = transport_;
        call->controller.set_max_retry(0);
        transport_->Add();
        twopc_service::TwoPCService_Stub stub(&channels[entry.first]);
        stub.Prepare(&call->controller, &call->request, &call->response,
                     brpc::NewCallback(RPCDone, call));
    }
    {
        std::unique_lock<bthread::Mutex> guard(waiter->mutex);
        while (!waiter->durable) waiter->cv.wait(guard);
    }
    {
        std::lock_guard<std::mutex> guard(mutex_);
        waiters_.erase(key);
    }
    return true; // Only AllPrepared can satisfy this wait; no Prepare Join here.
}

bool PrepareNotifications::Accept(const twopc_service::AllPreparedRequest& message) {
    if (message.protocol_version() != kProtocolVersion || !message.has_identity() ||
        !ValidIdentity(message.identity())) return false;
    std::shared_ptr<Waiter> waiter;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = waiters_.find(Key(message.identity()));
        // Late/duplicate notifications do not recreate or apply transactions.
        if (it == waiters_.end()) return true;
        waiter = it->second;
    }
    if (message.SerializeAsString() != waiter->metadata) return false;
    std::lock_guard<bthread::Mutex> guard(waiter->mutex);
    waiter->durable = true;
    waiter->cv.notify_all();
    return true;
}
namespace { void CommitDone() {} }
void PrepareNotifications::Commit(const Requests& requests, brpc::Channel* channels,
                                  page_cost::OnlineCommitCost* cost_model) {
    CHECK(requests.size() > 1);
    struct Call {
        twopc_service::CommitRequest request;
        twopc_service::CommitResponse response;
        brpc::Controller controller;
        brpc::CallId id;
        int peer;
    };
    std::vector<std::unique_ptr<Call>> calls;
    for (const auto& entry : requests) {
        const auto& prepared = entry.second;
        auto call = std::make_unique<Call>();
        call->peer = entry.first;
        auto& request = call->request;
        request.set_kind(twopc_service::CommitRequest::PREPARED);
        *request.mutable_identity() = prepared.identity();
        request.set_transaction_id(prepared.transaction_id());
        request.set_start_ts(prepared.start_ts());
        request.set_commit_ts(prepared.commit_ts());
        call->id = call->controller.call_id();
        twopc_service::TwoPCService_Stub stub(&channels[entry.first]);
        stub.Commit(&call->controller, &request, &call->response,
                    brpc::NewCallback(CommitDone));
        calls.push_back(std::move(call));
    }
    bool all_completed = true;
    for (const auto& call : calls) {
        brpc::Join(call->id);
        if (call->controller.Failed() || !call->response.ok()) {
            LOG(ERROR) << "Prepared Commit did not complete: " << call->controller.ErrorText();
            all_completed = false;
        } else if (cost_model) {
            page_cost::ObserveControlReply(*cost_model, call->peer, call->controller.latency_us(),
                                           call->response.cost());
        }
    }
    CHECK(all_completed) << "Prepared Commit failure; recovery is not implemented";
}

} // namespace prepared_2pc
