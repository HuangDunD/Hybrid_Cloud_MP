#include "compute_node/page_cost_model.h"
#include "compute_node/online_commit_rpc.h"
#include "storage/log_record.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <limits>
#include <sstream>
#include <thread>
#include <vector>
#include <brpc/server.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>

using namespace page_cost;
constexpr int64_t MS = 1000000;
static void Near(double a, double b) { assert(std::abs(a - b) < 1e-7); }
static LogCosts F(double f) { return {true, f, {f, f, f}, 2, 0}; }
static OnlineConfig Settings() {
    OnlineConfig c; c.enabled = true; c.min_samples = 2;
    c.update_interval_ms = 10; c.ttl_ms = 100; c.alpha = .5; return c;
}
static void Seed(OnlineCommitCost& model, int64_t base = MS) {
    for (auto t : {base, base + 10 * MS}) {
        model.ObserveLog(LogPhase::Commit, 2, t);
        for (int node : {0, 1, 2}) model.ObserveRPC(node, 10.1, 10, F(2), t);
    }
}
static void Configuration() {
    for (const char* text : {
        R"({"online_sampling":2})", R"({"online_min_samples":0})",
        R"({"online_min_samples":1.5})", R"({"online_update_interval_ms":0})",
        R"({"online_ewma_alpha":2})", R"({"online_history_ttl_ms":100})",
        R"({"online_freeze_after_ms":-1})", R"({"online_freeze_after_ms":true})"}) {
        bool rejected = false;
        try { Config::Read(JsonConfig::load(std::string("{\"hybrid_cost_model\":") + text + "}", "test")); }
        catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected);
    }
    const auto c = Config::Read(JsonConfig::load(
        R"({"hybrid_cost_model":{"enabled":1,"online_sampling":1,"online_freeze_after_ms":0.0}})", "test"));
    assert(c.online.enabled && c.online.freeze_after_ms == 0);
    assert(!Config::Read(JsonConfig::load("{}", "test")).online.enabled);
}
static void WindowsAndEstimates() {
    OnlineCommitCost model; model.Configure(Settings());
    assert(!model.Estimate({1, 2}, 0, MS).ready);
    model.ObserveLog(LogPhase::Commit, 100, MS);
    assert(!model.Local(2 * MS).valid); // time AND sample threshold are required

    OnlineCommitCost costs; costs.Configure(Settings()); Seed(costs);
    auto f = costs.Local(11 * MS);
    assert(f.valid); Near(f.mean_ms, 2);
    auto estimate = costs.Estimate({1, 2}, 0, 11 * MS);
    assert(estimate.ready); Near(estimate.delta_ms, 4.2); // 2r + 2f, no storage RPC double count
    assert(!costs.Estimate({1, 3}, 0, 11 * MS).ready); // never use local r for an unknown peer
    Near(costs.Estimate({0, 1}, 0, 11 * MS).delta_ms, 4.2); // self RPC is real in legacy path
    costs.ObserveRPC(2, 10.1, 10, F(7), 12 * MS);
    Near(costs.Estimate({1, 2}, 0, 12 * MS).delta_ms, 14.2); // maximum, not participant sum
    // Two batches with different costs: sample-weighted window mean, then EWMA.
    costs.ObserveLog(LogPhase::Commit, 4, 15 * MS);
    costs.ObserveLog(LogPhase::Commit, 8, 21 * MS);
    Near(costs.Local(21 * MS).mean_ms, 4); // (old 2 + new window 6) / 2
    costs.ObserveLog(LogPhase::Backup, 3, 22 * MS);
    costs.ObserveLog(LogPhase::Backup, 3, 32 * MS);
    Near(costs.Local(32 * MS).phase_ms[2], 3);
    Near(costs.Estimate({1, 2}, 0, 32 * MS).delta_ms, 13.2); // 7.1 + backup 3 + 7.1 - local commit 4
    assert(!costs.Estimate({1, 2}, 0, 200 * MS).ready);
    assert(!costs.Local(200 * MS).valid);
    Seed(costs, 201 * MS);
    Near(costs.Estimate({1, 2}, 0, 211 * MS).delta_ms, 4.2); // stale values reset
    // A late old response must not appear fresh merely because it just arrived.
    auto old = F(9); old.age_ns = 99 * MS;
    costs.ObserveRPC(2, 10.1, 10, old, 212 * MS);
    assert(!costs.Estimate({1, 2}, 0, 214 * MS).ready);

    OnlineCommitCost sparse; auto slow = Settings(); slow.min_samples = 3; sparse.Configure(slow);
    sparse.ObserveLog(LogPhase::Commit, 100, MS);
    sparse.ObserveLog(LogPhase::Commit, 100, 90 * MS);
    sparse.ObserveLog(LogPhase::Commit, 1, 150 * MS);
    assert(!sparse.Local(151 * MS).valid); // do not accumulate ancient sparse samples
}
static void FreezeAndSelection() {
    auto c = Settings(); c.freeze_after_ms = 20;
    OnlineCommitCost frozen; frozen.Configure(c); Seed(frozen);
    for (int n : {0, 1, 2}) frozen.ObserveRPC(n, 10.1, 10, F(2), 21 * MS);
    Near(frozen.Local(21 * MS).mean_ms, 2);
    frozen.ObserveLog(LogPhase::Commit, 100, 1000 * MS);
    for (int n : {0, 1, 2}) frozen.ObserveRPC(n, 100, 10, F(100), 1000 * MS);
    Near(frozen.Estimate({1, 2}, 0, 1000 * MS).delta_ms, 4.2);

    Config pc; pc.enabled = true; pc.online = Settings(); pc.initial_page_ms = 3;
    pc.rpc_rtt_ms = .1; pc.log_wait_ms = 100; // must not leak the fixed threshold into online decisions
    Model model; model.Configure(pc);
    struct State { bool usable; bool upgrade; };
    auto missing = [](const Access&) { return State{false, false}; };
    std::vector<Access> accesses{{1, 1, true, 1}, {1, 2, true, 2}};
    assert(!model.Decide(accesses, 0, missing, MS).eligible);
    Seed(model.online);
    auto decision = model.Decide(accesses, 0, missing, 11 * MS);
    assert(decision.eligible && decision.twopc); Near(decision.commit_delta_ms, 4.2);
    pc.include_2pc_execution_cost = false;
    model.Configure(pc);
    model.ObserveRemote(true, 1000, 11 * MS);
    decision = model.Decide(accesses, 0, missing, 11 * MS);
    assert(decision.eligible && decision.twopc);
    Near(decision.twopc_ms, 0); Near(decision.commit_delta_ms, 4.2);
    // Disabling execution estimates must NOT freeze online log/commit costs.
    model.online.ObserveRPC(2, 10.1, 10, F(4), 12 * MS);
    decision = model.Decide(accesses, 0, missing, 12 * MS);
    assert(decision.eligible && !decision.twopc);
    assert(!model.Decide({{1, 1, true, 1}}, 0, missing, 12 * MS).eligible);
    assert(!model.Decide(accesses, 0, missing, 200 * MS).eligible);
}
static void WireAndConcurrency() {
    OnlineCommitCost model; model.Configure(Settings()); Seed(model);
    twopc_service::CommitCostTelemetry wire;
    wire.set_version(1); wire.set_handler_ns(10 * MS); wire.set_log_valid(true);
    wire.set_log_mean_ms(2); wire.set_log_samples(2);
    for (int i = 0; i < 3; ++i) wire.add_log_phase_ms(2);
    ObserveControlReply(model, 1, 10100, wire, 12 * MS);
    auto invalid = wire; invalid.set_handler_ns(20 * MS);
    ObserveControlReply(model, 1, 10100, invalid, 13 * MS); // reject, do not turn into zero r
    ObserveControlReply(model, 1, 10100, {}, 13 * MS); // older binary's empty telemetry
    std::ostringstream out; model.Dump(out, 13 * MS);
    assert(out.str().find("hybrid_online_invalid_rpc=1\n") != std::string::npos);

    BatchEndLogRecord record(1, 0, 1);
    std::string before(record.log_tot_len_, '\0'), after = before;
    record.serialize(before.data()); record.cost_log_phase_ = 2; record.cost_enqueued_ns_ = 123;
    record.serialize(after.data()); assert(before == after); // no WAL format/LSN changes

    OnlineCommitCost concurrent; concurrent.Configure(Settings());
    std::vector<std::thread> threads;
    for (int n = 0; n < 8; ++n) threads.emplace_back([&, n] {
        for (int i = 0; i < 1000; ++i) {
            concurrent.ObserveLog(LogPhase::Commit, 2, (i + 1) * MS);
            concurrent.ObserveRPC(n, 10.1, 10, F(2), (i + 1) * MS);
            concurrent.Local((i + 1) * MS);
        }
    });
    for (auto& t : threads) t.join();
    std::ostringstream counts; concurrent.Dump(counts, 1000 * MS);
    assert(counts.str().find("hybrid_online_log_samples=8000\n") != std::string::npos);
    assert(counts.str().find("hybrid_online_rpc_samples=8000\n") != std::string::npos);
}
// Local real RPC verifies response lifetime and excludes handler work/late Join
// from the transport estimate. It does not simulate a full database transaction.
class TimedParticipant : public twopc_service::TwoPCService {
public:
    OnlineCommitCost model;
    void Prepare(google::protobuf::RpcController*, const twopc_service::PrepareRequest*,
                 twopc_service::PrepareResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        CommitCostReply timer(&model, response->mutable_cost());
        bthread_usleep(2000);
        response->set_ok(true);
    }
};
static void NoOp() {}
static void LocalRPC() {
    auto c = Settings(); c.min_samples = 1; c.update_interval_ms = .001; c.alpha = 1;
    c.ttl_ms = 10000;
    TimedParticipant node; node.model.Configure(c);
    node.model.ObserveLog(LogPhase::Commit, 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    node.model.ObserveLog(LogPhase::Commit, 2);
    brpc::Server server;
    assert(server.AddService(&node, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
    assert(server.Start("127.0.0.1:0", nullptr) == 0);
    brpc::Channel channel;
    brpc::ChannelOptions options; options.max_retry = 0; options.timeout_ms = 5000;
    assert(channel.Init(("127.0.0.1:" + std::to_string(server.listen_address().port)).c_str(), &options) == 0);
    twopc_service::TwoPCService_Stub stub(&channel);
    OnlineCommitCost coordinator; coordinator.Configure(c);
    double sum = 0;
    for (int i = 0; i < 2; ++i) {
        brpc::Controller cntl;
        twopc_service::PrepareRequest request;
        twopc_service::PrepareResponse response;
        const auto call_id = cntl.call_id();
        stub.Prepare(&cntl, &request, &response, brpc::NewCallback(NoOp));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        brpc::Join(call_id);
        assert(!cntl.Failed() && response.ok() && response.cost().version() == 1);
        assert(response.cost().handler_ns() >= MS && response.cost().log_valid());
        const double residual = cntl.latency_us() / 1000.0 - response.cost().handler_ns() / 1e6;
        assert(residual >= 0);
        sum += residual;
        ObserveControlReply(coordinator, 1, cntl.latency_us(), response.cost());
    }
    std::ostringstream out; out.precision(17); coordinator.Dump(out);
    const std::string text = out.str(), key = "hybrid_online_peer_1_r_ms=";
    const auto pos = text.find(key); assert(pos != std::string::npos);
    Near(std::stod(text.substr(pos + key.size())), sum / 2);
    assert(text.find("hybrid_online_rpc_samples=2\n") != std::string::npos);
    server.Stop(0); server.Join();
}
int main() {
    Configuration(); WindowsAndEstimates(); FreezeAndSelection(); WireAndConcurrency(); LocalRPC();
    std::cout << "online_commit_cost_test passed (windows, EWMA, peers, expiry, freeze, selection, wire, concurrency, local RPC)\n";
}
