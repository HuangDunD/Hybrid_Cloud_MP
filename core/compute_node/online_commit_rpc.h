#pragma once
#include "online_commit_cost.h"
#include "twoPC.pb.h"

namespace page_cost {
// Declare after ClosureGuard so telemetry is populated BEFORE the response is sent.
// Includes all handler work, including logging, page/lock work and this snapshot.
class CommitCostReply {
    OnlineCommitCost* model_;
    twopc_service::CommitCostTelemetry* response_;
    int64_t start_;
public:
    CommitCostReply(OnlineCommitCost* model, twopc_service::CommitCostTelemetry* response)
        : model_(model), response_(response), start_(model ? MonotonicNs() : 0) {}
    ~CommitCostReply() {
        if (!model_) return;
        const auto log = model_->Local();
        response_->set_version(1);
        response_->set_log_valid(log.valid);
        response_->set_log_mean_ms(log.mean_ms);
        for (double value : log.phase_ms) response_->add_log_phase_ms(value);
        response_->set_log_samples(log.samples);
        response_->set_log_age_ns(log.age_ns);
        response_->set_handler_ns(MonotonicNs() - start_);
    }
};
inline void ObserveControlReply(OnlineCommitCost& model, int node, int64_t latency_us,
                                const twopc_service::CommitCostTelemetry& reply,
                                int64_t now = MonotonicNs()) {
    if (reply.version() != 1 || reply.handler_ns() == 0 || latency_us < 0) return;
    LogCosts f;
    if (reply.log_phase_ms_size() == 3 && reply.log_age_ns() <= uint64_t(INT64_MAX)) {
        f.valid = reply.log_valid(); f.mean_ms = reply.log_mean_ms();
        f.samples = reply.log_samples(); f.age_ns = reply.log_age_ns();
        for (size_t i = 0; i < 3; ++i) f.phase_ms[i] = reply.log_phase_ms(i);
    }
    model.ObserveRPC(node, latency_us / 1000.0, reply.handler_ns() / 1e6, f, now);
}
} // namespace page_cost
