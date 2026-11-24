#include "common/fatal_checks.h"
#include "mvcc/protocol.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <thread>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#include <butil/logging.h>

using namespace mvcc;
namespace {
const std::string epoch(16, 'm');
struct TestClock {
    std::atomic<Timestamp> timestamp{0};
    RemoteTso Client() {
        return {[this] { return timestamp.load(); }, [this](Timestamp snapshot) {
            const auto commit_ts = ++timestamp;
            CHECK_GT(commit_ts, snapshot);
            return commit_ts;
        }};
    }
};

struct Cluster {
    TestClock clock;
    SnapshotNode a{epoch, 0, {0, 1}, clock.Client()};
    SnapshotNode b{epoch, 1, {0, 1}, clock.Client()};
};
void RemoteTsoDelayedReply() {
    using namespace std::chrono_literals;
    std::atomic<Timestamp> timestamp{0};
    std::promise<void> allocated, deliver;
    auto allocated_at_tso = allocated.get_future();
    auto reply = deliver.get_future();
    RemoteTso tso{[&] { return timestamp.load(); }, [&](Timestamp floor) {
        const auto c = ++timestamp;
        CHECK(c > floor);
        allocated.set_value();
        reply.wait();
        return c;
    }};
    SnapshotNode home(epoch, 0, {0, 1}, tso);
    SnapshotNode peer(epoch, 1, {0, 1}, tso);
    const auto writer = home.Begin(1);
    const Version changed{writer.context().id, false, false};
    const auto old = peer.Begin(1);
    CHECK_EQ(old.context().snapshot, 0);
    auto commit = std::async(std::launch::async, [&] { return home.AllocateCommitTs(writer.context()); });
    CHECK(allocated_at_tso.wait_for(5s) == std::future_status::ready);
    const auto recent = peer.Begin(2);
    CHECK_EQ(recent.context().snapshot, 1);

    auto read = [&](const Snapshot& view) {
        return Visible(view, changed, home.ReadCreatorAt(changed.creator, view.context().snapshot));
    };
    auto read_old = std::async(std::launch::async, [&] { return read(old); });
    auto read_recent = std::async(std::launch::async, [&] { return read(recent); });
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (home.PendingTitWaiters() != 2) {
        CHECK(std::chrono::steady_clock::now() < deadline) << "both readers must wait for the unbound C";
        std::this_thread::yield();
    }

    deliver.set_value();
    CHECK(commit.wait_for(5s) == std::future_status::ready && commit.get() == 1);
    CHECK(read_old.wait_for(5s) == std::future_status::ready && read_old.get() == Visibility::FollowHistory);
    CHECK(home.Lookup(changed.creator)->state == State::Committing);
    CHECK_EQ(home.PendingTitWaiters(), 1);
    CHECK(read_recent.wait_for(0s) != std::future_status::ready);
    home.CertifyRemoteCommit(writer.context(), 1);
    CHECK(read_recent.wait_for(5s) == std::future_status::ready && read_recent.get() == Visibility::Value);
    CHECK(read(old) == Visibility::FollowHistory && read(recent) == Visibility::Value);
    home.End(writer, State::Committed);
    peer.End(old);
    peer.End(recent);
}
void VisibilityAndConflict() {
    Cluster x;
    auto writer = x.a.Begin(1);
    auto old = x.b.Begin(1);
    Version initial{{}, true, false}, own{writer.context().id, false, false};
    CHECK(Visible(old, initial, std::nullopt) == Visibility::Value);
    initial.deleted = true;
    CHECK(Visible(old, initial, std::nullopt) == Visibility::NotFound);
    CHECK(Visible(writer, own, std::nullopt) == Visibility::Value);
    own.deleted = true;
    CHECK(Visible(writer, own, std::nullopt) == Visibility::NotFound);
    CHECK(CheckWrite(writer, own, std::nullopt) == WriteCheck::Allowed);
    own.deleted = false;
    CHECK(Visible(old, own, std::nullopt) == Visibility::NeedStatus);
    CHECK(CheckWrite(old, own, std::nullopt) == WriteCheck::NeedStatus);
    for (auto state : {State::Active, State::Committing, State::Aborted}) {
        CHECK(Visible(old, own, Status{state, 0}) == Visibility::FollowHistory);
        CHECK(CheckWrite(old, own, Status{state, 0}) ==
              (state == State::Aborted ? WriteCheck::FollowHistory : WriteCheck::WaitForWriter));
    }
    const auto slot = x.a.AllocateCommitTs(writer.context());
    x.a.CertifyRemoteCommit(writer.context(), slot);
    auto equal = x.b.Begin(2);
    CHECK(equal.context().snapshot == slot);
    CHECK(Visible(equal, own, x.a.Lookup(own.creator)) == Visibility::Value);
    CHECK(CheckWrite(equal, own, x.a.Lookup(own.creator)) == WriteCheck::Allowed);
    CHECK(Visible(old, own, x.a.Lookup(own.creator)) == Visibility::FollowHistory);
    CHECK(CheckWrite(old, own, x.a.Lookup(own.creator)) == WriteCheck::SnapshotConflict);
    own.deleted = true;
    CHECK(Visible(equal, own, x.a.Lookup(own.creator)) == Visibility::NotFound);
    CHECK(CheckWrite(old, own, x.a.Lookup(own.creator)) == WriteCheck::SnapshotConflict);
    // The business candidate is unchanged by acquiring/releasing another tuple lock.
    struct Row { Version version; TxnId lock_owner; } row{own, writer.context().id};
    row.lock_owner = old.context().id;
    CHECK(Visible(equal, row.version, x.a.Lookup(row.version.creator)) == Visibility::NotFound);
    row.lock_owner = {};
    CHECK(Visible(equal, row.version, x.a.Lookup(row.version.creator)) == Visibility::NotFound);
    std::cout << "PASS: equality, self writes, initialization/tombstones, creator != lock owner, SI conflicts\n";
}
void Dies(const std::function<void()>& f) {
    const pid_t pid = fork(); CHECK(pid >= 0);
    if (pid == 0) {
        struct rlimit limit{0, 0};
        const int limited = setrlimit(RLIMIT_CORE, &limit); CHECK(limited == 0);
        f(); _exit(0);
    }
    int status = 0;
    const auto waited = waitpid(pid, &status, 0); CHECK(waited == pid);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT) << "invariant was not rejected";
}
void IllegalTransitions() {
    Dies([] { Cluster x; const auto s = x.a.Begin(1); x.a.Export(s, 1); x.a.End(s); });
    Dies([] { Cluster x; const auto s = x.a.Begin(1); x.a.End(s); x.a.Begin(1); });
    Dies([] { Cluster x; const auto s = x.a.Begin(1); x.a.CertifyRemoteCommit(s.context(), 1); });
    Dies([] { Cluster x; const auto s = x.a.Begin(1); x.a.AllocateCommitTs(s.context()); x.a.End(s); });
    Dies([] { Cluster x; const auto s = x.a.Begin(1); const auto c = x.a.AllocateCommitTs(s.context());
              x.a.CertifyRemoteCommit(s.context(), c + 1); });
    Dies([] { Cluster x; const auto s = x.a.Begin(1); const auto t = x.b.Begin(1);
              Visible(s, {t.context().id, false, false}, Status{State::Committed, 0}); });
}
static void RecoveredSnapshot() {
    Recovery image;
    image.max_commit_ts = 4;
    image.committed = {{0, {{2, 3}, {6, 6}}}, {1, {{2, 2}}}};
    TestClock clock;
    clock.timestamp = image.max_commit_ts;
    SnapshotNode reader(epoch, 1, {0, 1}, clock.Client(), image);
    const auto next = reader.Begin(7);
    CHECK(next.context().snapshot == 4);
    CHECK(reader.Lookup({epoch, 0, 3})->commit_ts == 4);
    CHECK(reader.Lookup({epoch, 1, 2})->state == State::Committed);
    CHECK(!reader.Lookup({epoch, 0, 4}) && !reader.Lookup({epoch, 1, 3}));
    CHECK_EQ(reader.AllocateCommitTs(next.context()), 5);
    Dies([&] { reader.Lookup({std::string(16, 'x'), 0, 2}); });
    Dies([&] { reader.Attach({{epoch, 0, 9}, 3}); });

    // The missing slot 6 was reserved by a writer that lost its end record.
    // WAL may arrive in different timestamp order, including within one home.
    image.max_commit_ts = 9;
    image.committed[0].push_back({7, 8, 8});
    image.committed[1].push_back({3, 3, 7});
    image.committed[1].push_back({4, 4, 5});
    clock.timestamp = image.max_commit_ts;
    SnapshotNode mixed(epoch, 0, {0, 1}, clock.Client(), image);
    CHECK(mixed.Lookup({epoch, 0, 2})->commit_ts == 4);
    CHECK(mixed.Lookup({epoch, 0, 8})->commit_ts == 9);
    CHECK(mixed.Lookup({epoch, 1, 3})->commit_ts == 7);
    CHECK(mixed.Lookup({epoch, 1, 4})->commit_ts == 5);
    CHECK(mixed.Begin(20).context().snapshot == 9);
    image.committed[1].push_back({5, 5, 9});
    Dies([&] { SnapshotNode corrupt(epoch, 1, {0, 1}, clock.Client(), image); });
}

}

int main() {
    EnableFatalChecks();
    IllegalTransitions();
    RecoveredSnapshot();
    VisibilityAndConflict();
    RemoteTsoDelayedReply();
    std::cout << "mvcc_protocol_test: all checks passed\n";
}
