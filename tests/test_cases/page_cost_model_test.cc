#include "compute_node/page_cost_model.h"
#include "LPLM/local_LR_page_lock.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <limits>
#include <sstream>
#include <thread>

using namespace page_cost;
static void Near(double a, double b) { assert(std::abs(a - b) < 1e-8); }
static Config Parse(const std::string& text) { return Config::Read(JsonConfig::load(text, "test")); }

int main() {
    assert(!Parse("{}").enabled);
    assert(Parse(R"({"hybrid_cost_model":{"enabled":1,"rpc_rtt_ms":1,"ewma_alpha":0.5}})").enabled);
    for (const char* text : {
        R"({"hybrid_cost_model":{"enabled":2}})",
        R"({"hybrid_cost_model":{"rpc_rtt_ms":0}})",
        R"({"hybrid_cost_model":{"log_wait_ms":-1}})",
        R"({"hybrid_cost_model":{"ewma_alpha":2}})",
        R"({"hybrid_cost_model":{"max_entries":1}})",
        R"({"hybrid_cost_model":{"include_remote_fetch":true}})"}) {
        bool rejected = false;
        try { Parse(text); } catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected);
    }
    assert(Parse(R"({"hybrid_cost_model":{"enabled":1}})").include_2pc_execution_cost);
    assert(!Parse(R"({"hybrid_cost_model":{"include_2pc_execution_cost":0}})").include_2pc_execution_cost);
    bool invalid_execution_flag = false;
    try { Parse(R"({"hybrid_cost_model":{"include_2pc_execution_cost":2}})"); }
    catch (const std::invalid_argument&) { invalid_execution_flag = true; }
    assert(invalid_execution_flag);

    // Exercise the exact initial/fallback predicate called by DTX. A single hot
    // key out of ten must select 2PC in the explicit baseline, below the old 20% threshold.
    const auto baseline = Parse(R"({"hybrid_cost_model":{"enabled":0}})");
    assert(!baseline.enabled && baseline.selector_explicit);
    assert(baseline.HotKeyDecision(1, 10, 0.2));
    assert(baseline.HotKeyDecision(1, 100, 1.0));
    assert(!baseline.HotKeyDecision(0, 10, 0.0));
    assert(!baseline.HotKeyDecision(0, 0, 0.2));
    const auto modeled = Parse(R"({"hybrid_cost_model":{"enabled":1}})");
    assert(modeled.enabled && modeled.selector_explicit);
    assert(!modeled.HotKeyDecision(1, 10, 0.2)); // existing model fallback remains unchanged
    assert(modeled.HotKeyDecision(2, 10, 0.2));
    assert(modeled.HotKeyDecision(0, 0, 0.2));
    for (const char* old_config : {"{}", R"({"hybrid_cost_model":{}})"}) {
        const auto legacy = Parse(old_config);
        assert(!legacy.selector_explicit);
        assert(!legacy.HotKeyDecision(1, 10, 0.2));
        assert(legacy.HotKeyDecision(2, 10, 0.2));
    }

    Config c;
    c.alpha = 0.5;
    c.history_ttl_ms = 1;
    Model history;
    history.Configure(c);
    const Key a{1, 9, Kind::Read};
    Near(history.Predict(a, 100).ms, 1);
    history.Observe(a, 4, 100);
    history.Observe(a, 8, 200);
    Near(history.Predict(a, 300).ms, 6);
    assert(history.Predict(a, 300).page_history);
    assert(!history.Predict({2, 9, Kind::Read}, 300).page_history);
    Near(history.Predict({2, 9, Kind::Read}, 300).ms, 6); // same-type fallback
    Near(history.Predict({1, 9, Kind::Write}, 300).ms, 1);
    history.Observe({2, 9, Kind::Read}, 20, 400);
    Near(history.Predict(a, 500).ms, 6); // tables never alias
    history.Observe(a, std::numeric_limits<double>::quiet_NaN(), 500);
    Near(history.Predict(a, 500).ms, 6);
    Near(history.Predict(a, 2000000).ms, 1); // expired page AND class fallback
    history.Observe(a, 30, 2000001);
    Near(history.Predict(a, 2000002).ms, 30); // stale average reset

    // Exercise real local permission transitions, including a resident S page
    // requiring an upgrade and a resident X page blocked by a local holder.
    LRLocalPageLock lock(9);
    assert(!lock.SnapshotForCost(false).usable);
    assert(lock.LockShared());
    assert(!lock.SnapshotForCost(false).usable); // granting
    lock.LockRemoteOK(0);
    assert(lock.SnapshotForCost(false).usable);
    assert(!lock.SnapshotForCost(true).usable && lock.SnapshotForCost(true).upgrade);
    lock.tryUnlockShared();
    lock.UnlockShared();
    lock.UnlockMtx();
    assert(lock.LockExclusive());
    lock.LockRemoteOK(0);
    assert(!lock.SnapshotForCost(false).usable);
    lock.tryUnlockExclusive();
    lock.UnlockExclusive();
    lock.UnlockMtx();
    assert(lock.SnapshotForCost(true).usable);
    assert(lock.TryBeginEvict());
    assert(!lock.SnapshotForCost(true).usable);
    lock.EndEvict();

    Model selector;
    c.history_ttl_ms = 10000;
    c.rpc_rtt_ms = 1;
    c.log_wait_ms = 2;
    selector.Configure(c); // commit difference = 6 ms
    selector.Observe({1, 1, Kind::Write}, 20, 100);
    selector.Observe({1, 2, Kind::Write}, 30, 100);
    selector.Observe({1, 3, Kind::Upgrade}, 15, 100);
    auto missing = [](const Access&) { return LRLocalPageLock::CostSnapshot{false, false}; };
    auto ready = [](const Access&) { return LRLocalPageLock::CostSnapshot{true, false}; };
    // Same page accessed twice: one permission estimate but two remote RPCs.
    std::vector<Access> accesses{{1, 1, false, 1}, {1, 1, true, 1}, {1, 2, true, 2}};
    auto d = selector.Decide(accesses, 0, missing, 200);
    assert(d.eligible && d.twopc);
    Near(d.lazy_ms, 50);
    Near(d.twopc_ms, 3);
    d = selector.Decide(accesses, 0, ready, 200);
    assert(d.eligible && !d.twopc);
    Near(d.lazy_ms, 0);
    // Home-local costs are common to both branches, even when very expensive.
    d = selector.Decide({{1, 2, true, 0}, {1, 1, true, 1}}, 0, missing, 200);
    Near(d.lazy_ms, 50);
    Near(d.twopc_ms, 31);
    assert(d.twopc);
    selector.ObserveRemote(true, 25, 100);
    d = selector.Decide({{1, 2, true, 0}, {1, 1, true, 1}}, 0, missing, 200);
    Near(d.twopc_ms, 55);
    assert(!d.twopc);
    auto upgrade = [](const Access& a) { return LRLocalPageLock::CostSnapshot{a.page != 3, a.page == 3}; };
    d = selector.Decide({{1, 3, true, 1}, {1, 2, true, 2}}, 0, upgrade, 200);
    Near(d.lazy_ms, 15);
    for (auto items : {std::vector<Access>{{1, 1, false, 1}, {1, 2, false, 2}},
                       std::vector<Access>{{1, 1, true, 0}},
                       std::vector<Access>{{1, 1, true, 1}, {1, 2, true, 1}}})
        assert(!selector.Decide(items, 0, missing, 200).eligible);
    assert(!selector.Choose2PC(9, 3)); // equality retains Lazy
    assert(selector.Choose2PC(9.1, 3));
    c.include_remote_fetch = false;
    selector.Configure(c);
    Near(selector.PredictRemote(true, 200), 0);

    // Simple model must remove BOTH execution terms, even with expensive remote
    // history and an unavailable home-local page. Only Lazy cost vs commit delta remains.
    c.include_remote_fetch = true;
    c.include_2pc_execution_cost = false;
    selector.Configure(c);
    d = selector.Decide({{1, 2, true, 0}, {1, 1, true, 1}}, 0, missing, 200);
    Near(d.lazy_ms, 50);
    Near(d.twopc_ms, 0);
    assert(d.eligible && d.twopc); // extended model above selected Lazy
    d = selector.Decide(accesses, 0, ready, 200);
    Near(d.twopc_ms, 0);
    assert(!d.twopc); // no page acquisition cost, so retain Lazy
    Model boundary;
    c.initial_page_ms = 3;
    boundary.Configure(c);
    d = boundary.Decide({{1, 1, true, 0}, {1, 2, true, 1}}, 0, missing, 200);
    Near(d.lazy_ms, 6);
    assert(!d.twopc); // strict > at the simple-model commit threshold

    // Bounded histories under concurrent sampling and prediction.
    Model concurrent;
    c.max_entries = 64;
    concurrent.Configure(c);
    std::vector<std::thread> threads;
    for (int n = 0; n < 8; ++n) threads.emplace_back([&, n] {
        for (int p = 0; p < 1000; ++p) {
            concurrent.Observe({n, p, Kind::Write}, 2);
            assert(concurrent.Predict({n, p, Kind::Write}).ms >= 0);
        }
    });
    for (auto& t : threads) t.join();
    assert(concurrent.EntryCount() <= 64);
    std::ostringstream out;
    concurrent.Dump(out);
    assert(out.str().find("hybrid_cost_page_samples=8000\n") != std::string::npos);
    std::cout << "page_cost_model_test passed (config, history, permissions, selector, concurrency)\n";
}
