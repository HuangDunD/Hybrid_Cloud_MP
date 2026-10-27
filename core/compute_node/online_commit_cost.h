#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <map>
#include <mutex>
#include <ostream>
#include <set>
#include <string>

namespace page_cost {
inline int64_t MonotonicNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}
enum class LogPhase : int { Commit = 0, Prepare = 1, Backup = 2 };
struct OnlineConfig {
    bool enabled = false; // Old config files retain their fixed-parameter behavior.
    double update_interval_ms = 1000;
    double alpha = 0.2;
    uint64_t min_samples = 100;
    double ttl_ms = 10000;
    double freeze_after_ms = 0; // 0: continuously adapt; otherwise freeze each valid series.
};
struct LogCosts {
    bool valid = false;
    double mean_ms = 0;
    std::array<double, 3> phase_ms{};
    uint64_t samples = 0;
    int64_t age_ns = 0;
};
struct CommitEstimate { bool ready = false; double delta_ms = 0; };

// Local monotonic durations only. Peer ages are durations, never clock timestamps.
// Updates are window means followed by EWMA, rather than per-request EWMA.
class OnlineCommitCost {
    struct Window {
        double mean = 0, sum = 0;
        uint64_t pending = 0, samples = 0, total = 0, updates = 0;
        int64_t begin = 0, first = 0, last = 0, published = 0;
        bool started = false, valid = false, frozen = false;
        bool Fresh(const OnlineConfig& c, int64_t now) const {
            return valid && (frozen || (now >= published && (now - published) / 1e6 <= c.ttl_ms));
        }
        void Publish(const OnlineConfig& c, int64_t now) {
            if (frozen || !started || now < last) return;
            if ((now - last) / 1e6 > c.ttl_ms) {
                pending = 0; sum = 0; valid = false; samples = 0;
                started = false;
                return;
            }
            if ((now - begin) / 1e6 >= c.update_interval_ms && pending >= c.min_samples) {
                const double value = sum / pending;
                mean = Fresh(c, now) ? (1 - c.alpha) * mean + c.alpha * value : value;
                samples += pending;
                pending = 0; sum = 0; begin = now; published = now;
                valid = true; ++updates;
            }
            if (c.freeze_after_ms > 0 && Fresh(c, now) &&
                (now - first) / 1e6 >= c.freeze_after_ms) frozen = true;
        }
        void Add(double value, const OnlineConfig& c, int64_t now) {
            ++total;
            if (frozen) return;
            // Concurrent callbacks may acquire this mutex in a different order.
            if (started && now < last) now = last;
            if (!started || (now - last) / 1e6 > c.ttl_ms) {
                begin = first = now; sum = 0; pending = samples = 0; valid = false; started = true;
            }
            if ((now - begin) / 1e6 > c.ttl_ms) {
                begin = now; pending = 0; sum = 0;
            }
            last = now; sum += value; ++pending;
            Publish(c, now);
        }
    };
    struct Peer {
        Window rpc;
        LogCosts log;
        int64_t received = 0, first = 0;
        bool frozen = false, started = false;
    };
    OnlineConfig config_;
    mutable std::mutex mutex_;
    Window log_;
    std::array<Window, 3> phases_{};
    Window rpc_; // Reporting only; decisions use the destination's own estimate.
    std::map<int, Peer> peers_; // Destinations are validated against the configured nodes by caller.
    uint64_t invalid_rpc_ = 0, unavailable_ = 0;

    LogCosts LocalLocked(int64_t now) {
        log_.Publish(config_, now);
        LogCosts result;
        if (!log_.Fresh(config_, now)) return result;
        result.valid = true; result.mean_ms = log_.mean; result.samples = log_.samples;
        result.age_ns = log_.frozen ? 0 : now - log_.published;
        for (size_t i = 0; i < phases_.size(); ++i) {
            phases_[i].Publish(config_, now);
            result.phase_ms[i] = phases_[i].Fresh(config_, now) ? phases_[i].mean : log_.mean;
        }
        return result;
    }
    bool ValidLog(const LogCosts& log) const {
        if (!log.valid || log.samples < config_.min_samples || log.age_ns < 0 ||
            log.age_ns / 1e6 > config_.ttl_ms || !std::isfinite(log.mean_ms) || log.mean_ms < 0) return false;
        for (double ms : log.phase_ms) if (!std::isfinite(ms) || ms < 0) return false;
        return true;
    }
public:
    void Configure(const OnlineConfig& c) { config_ = c; } // Before workers start.
    void ObserveLog(LogPhase phase, double ms, int64_t now = MonotonicNs()) {
        const auto index = static_cast<size_t>(phase);
        if (!config_.enabled || index >= phases_.size() || !std::isfinite(ms) || ms < 0) return;
        std::lock_guard<std::mutex> guard(mutex_);
        log_.Add(ms, config_, now); phases_[index].Add(ms, config_, now);
    }
    LogCosts Local(int64_t now = MonotonicNs()) {
        std::lock_guard<std::mutex> guard(mutex_);
        return LocalLocked(now);
    }
    // Called only for successful legacy control RPCs. brpc's completed latency
    // avoids counting delayed Join() calls as RPC time. No GetDataItem timing here.
    void ObserveRPC(int node, double total_ms, double handler_ms, const LogCosts& log,
                    int64_t now = MonotonicNs()) {
        if (!config_.enabled || node < 0) return;
        std::lock_guard<std::mutex> guard(mutex_);
        if (!std::isfinite(total_ms) || !std::isfinite(handler_ms) || handler_ms <= 0 ||
            total_ms < handler_ms) { ++invalid_rpc_; return; }
        auto& p = peers_[node];
        p.rpc.Add(total_ms - handler_ms, config_, now);
        rpc_.Add(total_ms - handler_ms, config_, now);
        if (ValidLog(log) && !p.frozen && (!p.started || now >= p.received)) {
            if (!p.started) { p.first = now; p.started = true; }
            p.log = log; p.received = now;
            if (config_.freeze_after_ms > 0 && (now - p.first) / 1e6 >= config_.freeze_after_ms)
                p.frozen = true;
        }
    }
    CommitEstimate Estimate(const std::set<int>& writers, int local, int64_t now = MonotonicNs()) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto missing = [&]() { ++unavailable_; return CommitEstimate{}; };
        if (writers.size() <= 1) return missing();
        const auto own = LocalLocked(now);
        if (!own.valid) return missing();
        double prepare = 0, commit = 0;
        for (int node : writers) {
            auto it = peers_.find(node);
            if (it == peers_.end()) return missing();
            auto& p = it->second;
            p.rpc.Publish(config_, now);
            if (!p.rpc.Fresh(config_, now)) return missing();
            const auto f = node == local ? own : p.log;
            if (!f.valid || (node != local && !p.frozen &&
                (now < p.received || double(now - p.received + f.age_ns) / 1e6 > config_.ttl_ms)))
                return missing();
            prepare = std::max(prepare, p.rpc.mean + f.phase_ms[1]);
            commit = std::max(commit, p.rpc.mean + f.phase_ms[0]);
        }
        // Parallel participants contribute the slowest phase, not a sum.
        // With equal r/f this reduces to 2*r+2*f. Subtract Lazy's one local Commit.
        return {true, std::max(0.0, prepare + own.phase_ms[2] + commit - own.phase_ms[0])};
    }
    void Dump(std::ostream& out, int64_t now = MonotonicNs()) {
        std::lock_guard<std::mutex> guard(mutex_);
        const auto f = LocalLocked(now);
        rpc_.Publish(config_, now);
        out << "hybrid_online_enabled=" << config_.enabled << '\n'
            << "hybrid_online_update_interval_ms=" << config_.update_interval_ms << '\n'
            << "hybrid_online_alpha=" << config_.alpha << '\n'
            << "hybrid_online_min_samples=" << config_.min_samples << '\n'
            << "hybrid_online_ttl_ms=" << config_.ttl_ms << '\n'
            << "hybrid_online_freeze_after_ms=" << config_.freeze_after_ms << '\n'
            << "hybrid_online_f_ready=" << f.valid << '\n'
            << "hybrid_online_f_ms=" << f.mean_ms << '\n'
            << "hybrid_online_r_ready=" << rpc_.Fresh(config_, now) << '\n'
            << "hybrid_online_r_ms=" << (rpc_.Fresh(config_, now) ? rpc_.mean : 0) << '\n'
            << "hybrid_online_log_samples=" << log_.total << '\n'
            << "hybrid_online_rpc_samples=" << rpc_.total << '\n'
            << "hybrid_online_log_updates=" << log_.updates << '\n'
            << "hybrid_online_rpc_updates=" << rpc_.updates << '\n'
            << "hybrid_online_invalid_rpc=" << invalid_rpc_ << '\n'
            << "hybrid_online_unavailable=" << unavailable_ << '\n'
            << "hybrid_online_f_frozen=" << log_.frozen << '\n';
        const char* names[] = {"commit", "prepare", "backup"};
        for (size_t i = 0; i < phases_.size(); ++i) {
            out << "hybrid_online_" << names[i] << "_samples=" << phases_[i].total << '\n'
                << "hybrid_online_" << names[i] << "_f_ms=" << f.phase_ms[i] << '\n';
        }
        for (auto& entry : peers_) {
            auto& p = entry.second;
            p.rpc.Publish(config_, now);
            const bool fresh = p.log.valid && (p.frozen ||
                (now >= p.received && double(now - p.received + p.log.age_ns) / 1e6 <= config_.ttl_ms));
            const auto prefix = "hybrid_online_peer_" + std::to_string(entry.first);
            out << prefix << "_r_ready=" << p.rpc.Fresh(config_, now) << '\n'
                << prefix << "_r_ms=" << (p.rpc.Fresh(config_, now) ? p.rpc.mean : 0) << '\n'
                << prefix << "_f_ready=" << fresh << '\n'
                << prefix << "_f_ms=" << (fresh ? p.log.mean_ms : 0) << '\n';
        }
    }
};
} // namespace page_cost
