// Full publication path over real loopback brpc: two LazyRuntime instances,
// coordinator on node 0, a real Commit RPC from node 1, authoritative creator
// status RPCs, and an explicit held-ACK gate. No pages or WAL are needed here.
#include "common/fatal_checks.h"
#include "mvcc/lazy_runtime.h"
#include "mvcc/publication_rpc.h"
#include "undo/manager.h"
#include <atomic>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <condition_variable>
#include <chrono>
#include <future>
#include <set>
#include <thread>
#include <iostream>
#include <vector>
#include <mutex>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

using namespace mvcc;
namespace {
int FreePort() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0); CHECK(fd >= 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    CHECK(bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    socklen_t size = sizeof(address);
    CHECK(getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) == 0);
    const int port = ntohs(address.sin_port);
    close(fd);
    return port;
}
struct NullPages final : undo::PageAccess {
    Page* FetchUndoPage(const undo::PageKey&, bool, bool) override { CHECK(false); return nullptr; }
    void ReleaseUndoPage(const undo::PageKey&, bool) override { CHECK(false); }
    void ClearUndoCache() override {}
};
undo::Wal NullWal() {
    return {[](std::unique_ptr<LogRecord>) -> uint64_t { CHECK(false); return 0; }};
}
}
int main() {
    EnableFatalChecks();
    const std::string epoch(16, 'c');
    const int port0 = FreePort(), port1 = FreePort();
    brpc::Channel channels0[2], channels1[2];
    CHECK(channels0[0].Init(("127.0.0.1:" + std::to_string(port0)).c_str(), nullptr) == 0);
    CHECK(channels0[1].Init(("127.0.0.1:" + std::to_string(port1)).c_str(), nullptr) == 0);
    CHECK(channels1[0].Init(("127.0.0.1:" + std::to_string(port0)).c_str(), nullptr) == 0);
    CHECK(channels1[1].Init(("127.0.0.1:" + std::to_string(port1)).c_str(), nullptr) == 0);
    NullPages pages0, pages1;
    LazyRuntime runtime0(epoch, 0, {0, 1}, 0, NullWal(), pages0, channels0);
    LazyRuntime runtime1(epoch, 1, {0, 1}, 0, NullWal(), pages1, channels1);
    PublicationService service0(runtime0.Snapshots(), runtime0);
    PublicationService service1(runtime1.Snapshots(), runtime1);
    brpc::Server server0, server1;
    CHECK(server0.AddService(&service0, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
    CHECK(server1.AddService(&service1, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
    brpc::ServerOptions options; options.num_threads = 4;
    CHECK(server0.Start(("127.0.0.1:" + std::to_string(port0)).c_str(), &options) == 0);
    CHECK(server1.Start(("127.0.0.1:" + std::to_string(port1)).c_str(), &options) == 0);

    auto writer = runtime0.Begin();
    auto old = runtime1.Begin();
    CHECK(writer.context().snapshot == 0 && old.context().snapshot == 0);
    auto active = runtime1.LookupStatus(writer.context().id);
    CHECK(active && active->state == State::Active) << "remote home must report ACTIVE";

    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool gate_entered = false, gate_release = false;
    runtime0.SetDeliveryGate([&](const Message& message) {
        if (message.node != 1 || message.phase != Phase::Stage) return;
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_entered = true;
        gate_cv.notify_all();
        gate_cv.wait(lock, [&] { return gate_release; });
    });
    std::atomic<bool> done{false};
    Scn committed = 0;
    std::thread commit([&] {
        committed = runtime0.Complete(writer, true, true, 101);
        done.store(true);
    });
    {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait(lock, [&] { return gate_entered; });
    }
    CHECK(!done.load()) << "commit reported success before the Stage ACK";
    CHECK(runtime0.SafeScn() == 0 && runtime1.SafeScn() == 0);
    auto during = runtime1.Begin();
    CHECK(during.context().snapshot == 0) << "Stage alone must not advance the Begin boundary";
    const auto staged = runtime1.LookupStatus(writer.context().id);
    CHECK(staged && staged->state == State::Committed && staged->commit_scn == 1);
    CHECK(Visible(during, {writer.context().id, false, false}, staged) == Visibility::FollowHistory);
    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        gate_release = true;
        gate_cv.notify_all();
    }
    commit.join();
    CHECK(done.load() && committed == 1);
    CHECK(runtime0.SafeScn() == 1 && runtime1.SafeScn() == 1);
    auto fresh = runtime1.Begin();
    CHECK(fresh.context().snapshot == 1);
    CHECK(Visible(fresh, {writer.context().id, false, false},
                  runtime1.LookupStatus(writer.context().id)) == Visibility::Value);
    CHECK(Visible(old, {writer.context().id, false, false},
                  runtime1.LookupStatus(writer.context().id)) == Visibility::FollowHistory);
    runtime1.Snapshots().End(during); runtime1.Snapshots().End(fresh); runtime1.Snapshots().End(old); runtime0.Snapshots().End(writer, State::Committed);

    // A transaction begun on the non-coordinator node commits through the real
    // Commit RPC; every member must advance before the call returns.
    auto remote = runtime1.Begin();
    const auto remote_scn = runtime1.Complete(remote, true, true, 202);
    CHECK(remote_scn == 2 && runtime0.SafeScn() == 2 && runtime1.SafeScn() == 2);
    auto fresh2 = runtime0.Begin();
    CHECK(fresh2.context().snapshot == 2);
    CHECK(Visible(fresh2, {remote.context().id, false, false},
                  runtime0.LookupStatus(remote.context().id)) == Visibility::Value);
    runtime0.Snapshots().End(fresh2);

    auto aborted = runtime1.Begin();
    CHECK(runtime0.LookupStatus(aborted.context().id)->state == State::Active);
    runtime1.Complete(aborted, false, true, 0);
    const auto aborted_status = runtime0.LookupStatus(aborted.context().id);
    CHECK(aborted_status && aborted_status->state == State::Aborted && aborted_status->commit_scn == 0);
    auto after_abort = runtime0.Begin();
    CHECK(Visible(after_abort, {aborted.context().id, false, false}, aborted_status) ==
          Visibility::FollowHistory);
    runtime0.Snapshots().End(after_abort);

    // Park more remote Commit handlers than server workers behind one Stage ACK.
    // All requests must reserve slots while the publisher is paused, then Advance.
    const auto first_slot = runtime0.CoordinatorNext();
    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        gate_release = false;
        gate_entered = false;
    }
    constexpr size_t concurrent = 12;
    std::vector<std::future<Scn>> waiting;
    for (size_t i = 0; i < concurrent; ++i) {
        auto snapshot = runtime1.Begin();
        waiting.push_back(std::async(std::launch::async, [&, snapshot, i] {
            return runtime1.Complete(snapshot, true, true, 300 + i);
        }));
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (runtime0.CoordinatorNext() < first_slot + concurrent) {
        CHECK(std::chrono::steady_clock::now() < deadline) << "blocked RPCs starved the coordinator";
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        std::unique_lock<std::mutex> lock(gate_mutex);
        CHECK(gate_cv.wait_until(lock, deadline, [&] { return gate_entered; }));
        gate_release = true;
        gate_cv.notify_all();
    }
    std::set<Scn> published;
    for (auto& pending : waiting) {
        CHECK(pending.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
        published.insert(pending.get());
    }
    CHECK(published.size() == concurrent && *published.begin() == first_slot);
    CHECK(*published.rbegin() == first_slot + concurrent - 1 && runtime1.SafeScn() == *published.rbegin());

    server0.Stop(0); server1.Stop(0); server0.Join(); server1.Join();
    std::cout << "PASS: real coordinator Commit RPC, held Stage ACK, ordered SCN, remote ACTIVE/COMMITTED/ABORTED\n";
    std::cout << "mvcc_cluster_test: all checks passed\n";
}
