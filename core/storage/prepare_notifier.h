#pragma once

#include <deque>
#include <optional>
#include <ostream>
#include <brpc/channel.h>
#include "compute_node/prepared_txn.h"

// Normal-path aggregation only. All writers must use this same storage service.
// It does not implement LogOnce, recovery scanning or a durable decision table.
class DurablePrepareNotifier {
public:
    ~DurablePrepareNotifier() { Drain(); }
    void Configure(const std::vector<std::string>& ips, const std::vector<int>& ports, bool use_rdma);
    // Caller must have completed the WAL durability/replication barrier first.
    void ObserveDurableBatch(const std::string& bytes);
    void Drain();
    size_t PendingTransactions() const;
    void DumpTiming(std::ostream& out) const;

private:
    struct VoteSet {
        twopc_service::AllPreparedRequest metadata;
        std::map<int, std::string> votes; // participant -> exact owned payload
        bool notified = false;
        uint64_t first_vote_ns = 0;
    };
    struct State {
        bthread::Mutex mutex;
        bthread::ConditionVariable drained;
        std::map<prepared_2pc::IdentityKey, VoteSet> transactions;
        // 已完成身份 -> 当时发送的完整元信息。只用于抑制“同一笔事务的重复投票”，
        // 元信息不一致即说明 local_tx_id 被复用，直接失败而不是静默丢弃。
        std::map<prepared_2pc::IdentityKey, std::string> completed;
        std::deque<prepared_2pc::IdentityKey> completion_order;
        size_t pending_rpcs = 0;
        uint64_t completed_count = 0, vote_tail_ns = 0;
    };
    struct Call;
    static void NotifyDone(Call* call);
    std::optional<twopc_service::AllPreparedRequest> AddVote(const twopc_service::PrepareRequest& request);
    void Send(const twopc_service::AllPreparedRequest& metadata);
    std::shared_ptr<State> state_ = std::make_shared<State>();
    std::vector<std::shared_ptr<brpc::Channel>> channels_;
};
