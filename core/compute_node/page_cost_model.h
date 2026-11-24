#pragma once

#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <map>
#include <ostream>
#include <set>
#include <stdexcept>
#include <vector>
#include "util/json_config.h"
#include "online_commit_cost.h"
#include "page_cost_stats.h"

namespace page_cost {

struct Config {
    bool enabled = false; // Existing configurations retain the hot-key selector.
    bool selector_explicit = false; // Missing switch retains the historical ratio rule.
    bool include_2pc_execution_cost = true; // Preserve older configs; explicit 0 selects the simple model.
    bool include_remote_fetch = true;
    double rpc_rtt_ms = 0.1;
    double log_wait_ms = 5.0;
    double initial_page_ms = 1.0;
    double alpha = 0.2;
    size_t max_entries = 65536; // Legacy config only; arrays now follow table page capacities.
    double history_ttl_ms = 10000;
    OnlineConfig online;

    static Config Read(const JsonConfig& root) {
        Config c;
        const auto j = root.get("hybrid_cost_model");
        if (!j.exists()) return c;
        if (!j.is_dict()) throw std::invalid_argument("hybrid_cost_model must be an object");
        auto flag = [&](const char* key, bool& dst) {
            const auto v = j.get(key);
            if (!v.exists()) return;
            if (!v.is_int64() || (v.get_int64() != 0 && v.get_int64() != 1))
                throw std::invalid_argument(std::string("hybrid_cost_model.") + key + " must be 0 or 1");
            dst = v.get_int64() != 0;
        };
        auto number = [&](const char* key, double& dst) {
            const auto v = j.get(key);
            if (!v.exists()) return;
            if (v.is_int64()) dst = static_cast<double>(v.get_int64());
            else if (v.is_double()) dst = v.get_double();
            else throw std::invalid_argument(std::string("hybrid_cost_model.") + key + " must be numeric");
            if (!std::isfinite(dst) || dst <= 0)
                throw std::invalid_argument(std::string("hybrid_cost_model.") + key + " must be finite and positive");
        };
        c.selector_explicit = j.get("enabled").exists();
        flag("enabled", c.enabled);
        flag("include_2pc_execution_cost", c.include_2pc_execution_cost);
        flag("include_remote_fetch", c.include_remote_fetch);
        number("rpc_rtt_ms", c.rpc_rtt_ms);
        number("log_wait_ms", c.log_wait_ms);
        number("initial_page_ms", c.initial_page_ms);
        number("ewma_alpha", c.alpha);
        number("history_ttl_ms", c.history_ttl_ms);
        flag("online_sampling", c.online.enabled);
        number("online_update_interval_ms", c.online.update_interval_ms);
        number("online_ewma_alpha", c.online.alpha);
        number("online_history_ttl_ms", c.online.ttl_ms);
        const auto minimum = j.get("online_min_samples");
        if (minimum.exists()) {
            if (!minimum.is_int64() || minimum.get_int64() < 1 || minimum.get_int64() > 10000000)
                throw std::invalid_argument("hybrid_cost_model.online_min_samples must be in [1, 10000000]");
            c.online.min_samples = minimum.get_int64();
        }
        const auto freeze = j.get("online_freeze_after_ms");
        if (freeze.exists() && !(freeze.is_int64() && freeze.get_int64() == 0) &&
            !(freeze.is_double() && freeze.get_double() == 0))
            number("online_freeze_after_ms", c.online.freeze_after_ms);
        if (c.online.alpha > 1 || c.online.ttl_ms < c.online.update_interval_ms)
            throw std::invalid_argument("online alpha must be <= 1 and TTL >= update interval");
        const auto max = j.get("max_entries");
        if (max.exists()) {
            if (!max.is_int64() || max.get_int64() < 16 || max.get_int64() > 10000000)
                throw std::invalid_argument("hybrid_cost_model.max_entries must be in [16, 10000000]");
            c.max_entries = static_cast<size_t>(max.get_int64());
        }
        if (c.alpha > 1) throw std::invalid_argument("hybrid_cost_model.ewma_alpha must be <= 1");
        return c;
    }

    bool HotKeyDecision(int hot_keys, int total_keys, double historical_threshold) const {
        // Explicit enabled=0 is the requested any-hot-key baseline. Model mode
        // retains its existing fallback; missing config keeps old experiments compatible.
        if (selector_explicit && !enabled) return hot_keys > 0;
        return total_keys <= 0 || static_cast<double>(hot_keys) / total_keys >= historical_threshold;
    }
};

struct Key {
    int table;
    int page;
    Kind kind;
    bool operator==(const Key& b) const { return table == b.table && page == b.page && kind == b.kind; }
};
struct Estimate { double ms; bool page_history; };
struct Access { int table; int page; bool write; int home; };
struct Decision {
    bool eligible = false; bool twopc = false;
    double lazy_ms = 0; double twopc_ms = 0; double commit_delta_ms = 0;
};

// Legacy transaction selector. All page observations live in PageCostStats.
class PageCostModel {
    using Average = PageCostStats::Average;
    PageCostStats& stats_;
    Config config_;
    mutable std::mutex totals_mutex_;
    uint64_t page_samples_ = 0, page_hits_ = 0, page_misses_ = 0;
    uint64_t decisions_ = 0, twopc_ = 0, fallback_txns_ = 0;
    double lazy_ms_ = 0, remote_ms_ = 0, decision_ms_ = 0;
    double commit_delta_ms_ = 0;

    bool Fresh(const Average& a, int64_t now) const {
        return a.samples && now >= a.updated && double(now - a.updated) / 1e6 <= config_.history_ttl_ms;
    }
public:
    explicit PageCostModel(PageCostStats& stats) : stats_(stats) {}
    OnlineCommitCost online;
    static int64_t NowNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    // Startup only, before workers or RPC handlers run.
    void Configure(Config c) { config_ = c; stats_.Configure(c.alpha, c.history_ttl_ms); online.Configure(c.online); }
    // Reporting totals only; retain the estimator's observations.
    void ResetBenchmarkStats() {
        std::lock_guard<std::mutex> guard(totals_mutex_);
        page_samples_ = 0; page_hits_ = 0; page_misses_ = 0;
        decisions_ = 0; twopc_ = 0; fallback_txns_ = 0;
        lazy_ms_ = 0; remote_ms_ = 0; decision_ms_ = 0; commit_delta_ms_ = 0;
    }
    const Config& config() const { return config_; }
    double CommitDifferenceMs() const { return 2 * config_.rpc_rtt_ms + 2 * config_.log_wait_ms; }
    bool Choose2PC(double lazy_ms, double remote_ms) const {
        return lazy_ms > remote_ms + CommitDifferenceMs();
    }

    // Called once before the first business-page fetch. Snapshot returns
    // {usable, upgrade}; the caller must still acquire real locks afterwards.
    template<class Snapshot>
    Decision Decide(const std::vector<Access>& accesses, int local, Snapshot snapshot,
                    int64_t now = NowNs()) {
        std::set<int> writers;
        std::map<std::pair<int, int>, Access> pages;
        for (const auto& a : accesses) {
            if (a.write) writers.insert(a.home);
            auto [it, inserted] = pages.emplace(std::make_pair(a.table, a.page), a);
            if (!inserted) it->second.write = it->second.write || a.write;
        }
        Decision d;
        if (writers.size() <= 1) return d;
        d.commit_delta_ms = CommitDifferenceMs();
        if (config_.online.enabled) {
            const auto commit = online.Estimate(writers, local, now);
            if (!commit.ready) return d; // Keep the existing hot-key decision during cold/stale periods.
            d.commit_delta_ms = commit.delta_ms;
        }
        d.eligible = true;
        // The simple model ignores ALL 2PC execution estimates. Sampling can
        // continue, but neither remote RPCs nor home-local pages enter this side.
        if (config_.include_2pc_execution_cost) {
            // Remote RPCs are per item, while permission acquisition is per page.
            for (const auto& a : accesses)
                if (a.home != local) d.twopc_ms += PredictRemote(a.write, now);
        }
        for (const auto& entry : pages) {
            const auto& a = entry.second;
            const auto state = snapshot(a);
            if (state.usable) continue;
            const auto kind = state.upgrade ? Kind::Upgrade : (a.write ? Kind::Write : Kind::Read);
            const double ms = Predict({a.table, a.page, kind}, now).ms;
            d.lazy_ms += ms;
            // Only the extended model cancels the shared home-local page cost.
            if (config_.include_2pc_execution_cost && a.home == local) d.twopc_ms += ms;
        }
        d.twopc = d.lazy_ms > d.twopc_ms + d.commit_delta_ms;
        return d;
    }

    void Observe(Key key, double ms, int64_t now = NowNs()) {
        if (!std::isfinite(ms) || ms < 0) return;
        stats_.ObserveAcquisition(key.table, key.page, key.kind, ms, now);
        std::lock_guard<std::mutex> guard(totals_mutex_);
        ++page_samples_;
    }

    Estimate Predict(Key key, int64_t now = NowNs()) {
        const auto a = stats_.Acquisition(key.table, key.page, key.kind);
        Estimate result{config_.initial_page_ms, false};
        if (Fresh(a, now)) result = {a.ms, true};
        std::lock_guard<std::mutex> guard(totals_mutex_);
        if (result.page_history) ++page_hits_;
        else {
            ++page_misses_;
            const auto fallback = stats_.Fallback(key.kind);
            if (Fresh(fallback, now)) result.ms = fallback.ms;
        }
        return result;
    }

    // End-to-end GetDataItem time, including participant-side page acquisition.
    // Do not add another page-transfer estimate on top of this RPC estimate.
    void ObserveRemote(bool write, double ms, int64_t now = NowNs()) {
        if (!std::isfinite(ms) || ms < 0) return;
        stats_.ObserveRemoteFallback(write, ms, now);
    }
    double PredictRemote(bool write, int64_t now = NowNs()) const {
        if (!config_.include_remote_fetch) return 0;
        const auto a = stats_.RemoteFallback(write);
        return Fresh(a, now) ? a.ms : config_.rpc_rtt_ms;
    }
    void RecordDecision(bool twopc, double lazy_ms, double remote_ms, double elapsed_ms,
                        double commit_delta_ms = -1) {
        std::lock_guard<std::mutex> guard(totals_mutex_);
        ++decisions_;
        twopc_ += twopc;
        lazy_ms_ += lazy_ms;
        remote_ms_ += remote_ms;
        decision_ms_ += elapsed_ms;
        commit_delta_ms_ += commit_delta_ms < 0 ? CommitDifferenceMs() : commit_delta_ms;
    }
    void RecordFallback() {
        std::lock_guard<std::mutex> guard(totals_mutex_);
        ++fallback_txns_;
    }
    size_t EntryCount() { return stats_.AcquisitionEntries(); }
    void Dump(std::ostream& out) {
        online.Dump(out);
        std::lock_guard<std::mutex> guard(totals_mutex_);
        out << "hybrid_cost_enabled=" << config_.enabled << '\n'
            << "hybrid_cost_include_2pc_execution_cost=" << config_.include_2pc_execution_cost << '\n'
            << "hybrid_cost_rpc_rtt_ms=" << config_.rpc_rtt_ms << '\n'
            << "hybrid_cost_log_wait_ms=" << config_.log_wait_ms << '\n'
            << "hybrid_cost_include_remote_fetch=" << config_.include_remote_fetch << '\n'
            << "hybrid_cost_commit_delta_ms=" << CommitDifferenceMs() << '\n'
            << "hybrid_cost_decisions=" << decisions_ << '\n'
            << "hybrid_cost_2pc=" << twopc_ << '\n'
            << "hybrid_cost_lazy=" << decisions_ - twopc_ << '\n'
            << "hybrid_cost_fallback=" << fallback_txns_ << '\n'
            << "hybrid_cost_page_samples=" << page_samples_ << '\n'
            << "hybrid_cost_page_history_hits=" << page_hits_ << '\n'
            << "hybrid_cost_page_history_misses=" << page_misses_ << '\n'
            << "hybrid_cost_predicted_lazy_total_ms=" << lazy_ms_ << '\n'
            << "hybrid_cost_predicted_2pc_access_total_ms=" << remote_ms_ << '\n'
            << "hybrid_cost_decision_total_ms=" << decision_ms_ << '\n';
        out << "hybrid_cost_predicted_commit_delta_total_ms=" << commit_delta_ms_ << '\n';
    }
};
} // namespace page_cost
