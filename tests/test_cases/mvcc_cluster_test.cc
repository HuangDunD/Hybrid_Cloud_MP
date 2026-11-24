#include "common/fatal_checks.h"
#include "mvcc/lazy_runtime.h"
#include "mvcc/publication_rpc.h"
#include "undo/manager.h"
#include <atomic>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <chrono>
#include <future>
#include <thread>
#include <iostream>
#include <vector>

using namespace mvcc;
namespace {
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
    brpc::Channel channels0[2], channels1[2];
    std::atomic<Timestamp> clock{0};
    RemoteTso tso{[&] { return clock.load(); }, [&](Timestamp) { return ++clock; }};
    NullPages pages0, pages1;
    LazyRuntime runtime0(epoch, 0, {0, 1}, NullWal(), pages0, channels0, tso);
    LazyRuntime runtime1(epoch, 1, {0, 1}, NullWal(), pages1, channels1, tso);
    PublicationService service0(runtime0.Snapshots(), runtime0);
    PublicationService service1(runtime1.Snapshots(), runtime1);
    brpc::Server server0, server1;
    CHECK(server0.AddService(&service0, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
    CHECK(server1.AddService(&service1, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
    brpc::ServerOptions options; options.num_threads = 4;
    CHECK_EQ(server0.Start("127.0.0.1:0", &options), 0);
    CHECK_EQ(server1.Start("127.0.0.1:0", &options), 0);
    for (auto* channels : {channels0, channels1}) {
        CHECK_EQ(channels[0].Init(butil::endpoint2str(server0.listen_address()).c_str(), nullptr), 0);
        CHECK_EQ(channels[1].Init(butil::endpoint2str(server1.listen_address()).c_str(), nullptr), 0);
    }

    auto writer = runtime0.Begin();
    auto old = runtime1.Begin();
    CHECK(writer.context().snapshot == 0 && old.context().snapshot == 0);
    auto active = runtime1.LookupStatus(writer.context().id);
    CHECK(active && active->state == State::Active) << "remote home must report ACTIVE";

    const Timestamp low = runtime0.AllocateCommitTs(writer.context());
    auto recent = runtime1.Begin();
    CHECK_EQ(recent.context().snapshot, low);
    const auto latest = recent.context().snapshot;
    const auto id = writer.context().id;
    // More remote readers than brpc worker threads can wait on the same TIT entry.
    constexpr size_t readers = 12;
    std::vector<std::future<std::optional<Status>>> waiting;
    for (size_t i = 0; i < readers; ++i) {
        waiting.push_back(std::async(std::launch::async, [&] { return runtime1.ReadCreatorAt(id, latest); }));
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (runtime0.PendingTitWaiters() < readers) {
        CHECK(std::chrono::steady_clock::now() < deadline);
        std::this_thread::yield();
    }

    auto independent = runtime1.Begin();
    const auto high = runtime1.AllocateCommitTs(independent.context());
    CHECK_GT(high, low);
    runtime1.CertifyRemoteCommit(independent.context(), high);
    runtime1.End(independent, State::Committed);
    auto after = runtime0.Begin();
    CHECK_EQ(runtime0.ReadCreatorAt(independent.context().id, after.context().snapshot)->commit_ts, high);
    CHECK_EQ(runtime0.PendingTitWaiters(), readers);

    runtime0.CertifyRemoteCommit(writer.context(), low);
    runtime0.End(writer, State::Committed);
    for (auto& reader : waiting) {
        CHECK(reader.wait_until(deadline) == std::future_status::ready);
        const auto status = reader.get();
        CHECK(status && status->state == State::Committed && status->commit_ts == low);
    }
    CHECK(Visible(old, {id, false, false}, runtime1.ReadCreatorAt(id, old.context().snapshot)) ==
          Visibility::FollowHistory);
    auto aborted = runtime1.Begin();
    runtime1.End(aborted, State::Aborted);
    CHECK(runtime0.LookupStatus(aborted.context().id)->state == State::Aborted);
    runtime0.End(after, State::ReadOnlyFinished);
    runtime1.End(old, State::ReadOnlyFinished);
    runtime1.End(recent, State::ReadOnlyFinished);
    server0.Stop(0);
    server1.Stop(0);
    server0.Join();
    server1.Join();
    std::cout << "PASS: waiting readers, independent writer and terminal status over loopback RPC\n";
}
