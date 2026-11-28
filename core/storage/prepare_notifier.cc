#include "storage/prepare_notifier.h"
#include <brpc/controller.h>
#include <butil/logging.h>
#include "config.h"
#include "dtx/benchmark_stats.h"
#include "storage/prepared_log_record.h"

struct DurablePrepareNotifier::Call {
    twopc_service::AllPreparedRequest request;
    twopc_service::AllPreparedResponse response;
    brpc::Controller controller;
    std::shared_ptr<brpc::Channel> channel;
    std::shared_ptr<State> state;
};

void DurablePrepareNotifier::Configure(const std::vector<std::string>& ips,
                                      const std::vector<int>& ports, bool use_rdma) {
    CHECK(channels_.empty() && ips.size() == ports.size());
    brpc::ChannelOptions options;
    SET_BRPC_RDMA_OPTION(options, use_rdma);
    options.timeout_ms = 0x7fffffff;
    options.max_retry = 0;
    for (size_t i = 0; i < ips.size(); ++i) {
        auto channel = std::make_shared<brpc::Channel>();
        const std::string address = ips[i] + ":" + std::to_string(ports[i]);
        CHECK(channel->Init(address.c_str(), &options) == 0);
        channels_.push_back(std::move(channel));
    }
}

std::optional<twopc_service::AllPreparedRequest> DurablePrepareNotifier::AddVote(
        const twopc_service::PrepareRequest& request, BatchTiming& timing) {
    const auto encode_start = benchmark_stats::NowNs();
    const auto key = prepared_2pc::Key(request.identity());
    const auto metadata = prepared_2pc::Metadata(request);
    const auto metadata_bytes = metadata.SerializeAsString();
    const auto payload = request.SerializeAsString();
    const auto wait_start = benchmark_stats::NowNs();
    std::lock_guard<bthread::Mutex> guard(state_->mutex);
    const auto acquired = benchmark_stats::NowNs();
    // End the hold sample before unlocking, including on duplicate/partial votes.
    struct HoldTimer {
        uint64_t start;
        Sample& sample;
        ~HoldTimer() { sample.ns += benchmark_stats::NowNs() - start; }
    } hold{acquired, timing[VoteHold]};
    ++timing[Encode].count;
    timing[Encode].ns += wait_start - encode_start;
    ++timing[VoteWait].count;
    timing[VoteWait].ns += acquired - wait_start;
    ++timing[VoteHold].count;
    auto done = state_->completed.find(key);
    if (done != state_->completed.end()) {
        // 同一笔事务的重复投票（重放/重复日志）幂等返回；元信息不同则说明
        // local_tx_id 在同一 incarnation 内被复用，身份不再是事务唯一标识。
        CHECK(done->second == metadata_bytes)
            << "local_tx_id reused inside one coordinator incarnation";
        return std::nullopt;
    }
    auto result = state_->transactions.emplace(key, VoteSet{});
    auto& tx = result.first->second;
    if (result.second) {
        tx.metadata = metadata;
        tx.first_vote_ns = benchmark_stats::NowNs();
    }
    CHECK(tx.metadata.SerializeAsString() == metadata_bytes)
        << "Conflicting participant set/timestamps for durable Prepare";
    auto vote = tx.votes.emplace(request.participant_node(), payload);
    CHECK(vote.second || vote.first->second == payload) << "Conflicting duplicate durable YES";
    if (tx.notified || tx.votes.size() != tx.metadata.participants_size()) return std::nullopt;
    for (int node : tx.metadata.participants()) CHECK(tx.votes.count(node) == 1);
    tx.notified = true;
    ++state_->completed_count;
    state_->vote_tail_ns += benchmark_stats::NowNs() - tx.first_vote_ns;
    return tx.metadata;
}

void DurablePrepareNotifier::ObserveDurableBatch(const std::string& bytes) {
    const auto begin = benchmark_stats::NowNs();
    BatchTiming timing{};
    size_t offset = 0;
    while (offset < bytes.size()) {
        CHECK(bytes.size() - offset >= LOG_HEADER_SIZE);
        uint32_t length;
        LogType type;
        memcpy(&length, bytes.data() + offset + OFFSET_LOG_TOT_LEN, sizeof(length));
        memcpy(&type, bytes.data() + offset + OFFSET_LOG_TYPE, sizeof(type));
        CHECK(length >= LOG_HEADER_SIZE && length <= bytes.size() - offset);
        if (type == LogType::PREPARED) {
            const auto decode_start = benchmark_stats::NowNs();
            PreparedLogRecord record;
            CHECK(record.Decode(bytes.data() + offset, length));
            const auto parse_start = benchmark_stats::NowNs();
            twopc_service::PrepareRequest request;
            CHECK(request.ParseFromString(record.payload()));
            timing[Parse].ns += benchmark_stats::NowNs() - parse_start;
            ++timing[Parse].count;
            timing[Decode].ns += parse_start - decode_start;
            ++timing[Decode].count;
            if (prepared_2pc::StorageNotifies(request.protocol_version())) {
                CHECK(request.identity().coordinator_node() < channels_.size())
                    << "Storage Prepare notifications are not configured for this coordinator";
                auto notification = AddVote(request, timing);
                if (notification) {
                    const auto send_start = benchmark_stats::NowNs();
                    Send(*notification);
                    timing[Dispatch].ns += benchmark_stats::NowNs() - send_start;
                    ++timing[Dispatch].count;
                }
            }
        }
        offset += length;
    }
    timing[Batch] = {1, benchmark_stats::NowNs() - begin};
    // Publish once per batch; avoid shared counter traffic inside the vote lock.
    for (size_t i = 0; i < timing.size(); ++i) {
        if (timing[i].count == 0) continue;
        timing_[i].count.fetch_add(timing[i].count, std::memory_order_relaxed);
        timing_[i].ns.fetch_add(timing[i].ns, std::memory_order_relaxed);
    }
}

void DurablePrepareNotifier::Send(const twopc_service::AllPreparedRequest& metadata) {
    auto* call = new Call;
    call->request = metadata;
    call->state = state_;
    call->channel = channels_.at(metadata.identity().coordinator_node());
    {
        std::lock_guard<bthread::Mutex> guard(state_->mutex);
        ++state_->pending_rpcs;
    }
    twopc_service::TwoPCService_Stub stub(call->channel.get());
    // No disk/vote-map lock covers this asynchronous RPC, and LogWrite does not
    // wait for the coordinator callback. All message bytes live through completion.
    stub.AllPrepared(&call->controller, &call->request, &call->response,
                     brpc::NewCallback(NotifyDone, call));
}

void DurablePrepareNotifier::NotifyDone(Call* call) {
    CHECK(!call->controller.Failed() && call->response.ok())
        << "AllPrepared notification failed; recovery is not implemented: " << call->controller.ErrorText();
    auto state = call->state;
    const auto key = prepared_2pc::Key(call->request.identity());
    const auto metadata_bytes = call->request.SerializeAsString();
    delete call;
    std::lock_guard<bthread::Mutex> guard(state->mutex);
    state->transactions.erase(key);
    state->completed.emplace(key, metadata_bytes);
    state->completion_order.push_back(key);
    constexpr size_t kRecentCompletions = 4096;
    while (state->completion_order.size() > kRecentCompletions) {
        state->completed.erase(state->completion_order.front());
        state->completion_order.pop_front();
    }
    CHECK(state->pending_rpcs > 0);
    if (--state->pending_rpcs == 0) state->drained.notify_all();
}

void DurablePrepareNotifier::Drain() {
    std::unique_lock<bthread::Mutex> guard(state_->mutex);
    while (state_->pending_rpcs != 0) state_->drained.wait(guard);
}
size_t DurablePrepareNotifier::PendingTransactions() const {
    std::lock_guard<bthread::Mutex> guard(state_->mutex);
    return state_->transactions.size();
}

void DurablePrepareNotifier::ResetBenchmarkStats() {
    std::lock_guard<bthread::Mutex> guard(state_->mutex);
    state_->completed_count = 0;
    state_->vote_tail_ns = 0;
    for (auto& phase : timing_) {
        phase.count.store(0, std::memory_order_relaxed);
        phase.ns.store(0, std::memory_order_relaxed);
    }
}

void DurablePrepareNotifier::DumpTiming(std::ostream& out) const {
    std::lock_guard<bthread::Mutex> guard(state_->mutex);
    static constexpr const char* names[] = {"batch", "decode", "parse", "encode", "vote_lock_wait",
                                             "vote_lock_hold", "dispatch"};
    static_assert(std::size(names) == PhaseCount);
    for (size_t i = 0; i < timing_.size(); ++i) {
        out << "storage_prepare_" << names[i] << "_count=" << timing_[i].count.load() << '\n'
            << "storage_prepare_" << names[i] << "_ns=" << timing_[i].ns.load() << '\n';
    }
    out << "storage_prepare_complete_count=" << state_->completed_count << '\n'
        << "storage_prepare_vote_tail_ns=" << state_->vote_tail_ns << '\n';
}
