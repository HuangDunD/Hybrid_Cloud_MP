#include "common/fatal_checks.h"
#include "mvcc/protocol.h"
#include <algorithm>
#include <array>
#include <condition_variable>
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
struct Cluster {
    CommitCoordinator coordinator{epoch, {0, 1, 2}};
    SnapshotNode a{epoch, 0, {0, 1, 2}}, b{epoch, 1, {0, 1, 2}}, c{epoch, 2, {0, 1, 2}};
    SnapshotNode& Node(NodeId n) { CHECK(n < 3); return n == 0 ? a : (n == 1 ? b : c); }
    void Deliver(const Message& message) {
        const auto ack = Node(message.node).Receive(message);
        coordinator.Acknowledge(ack);
    }
    void Drain() {
        while (true) {
            auto messages = coordinator.TakeMessages();
            if (messages.empty()) return;
            for (const auto& message : messages) Deliver(message);
        }
    }
};
Plan Single(const Snapshot& s, NodeId writer = 0) {
    return {s.context(), CommitKind::SingleWriter, {writer}, {{writer, 50}}, {}, std::nullopt};
}
Plan Multi(const Snapshot& s, CommitKind kind = CommitKind::TwoPhase) {
    Plan p{s.context(), kind, {0, 1}, {{0, 50}, {1, 70}}, {{0, 40}, {1, 60}}, std::nullopt};
    if (kind == CommitKind::TwoPhase) p.decision = WalPosition{epoch, s.context().id.home, 80};
    return p;
}
WalPosition Wal(NodeId writer, uint64_t lsn) { return {epoch, writer, lsn}; }
void Ready(Cluster& x, const Snapshot& s, NodeId writer = 0) {
    x.coordinator.DecideCommit(s.context().id);
    x.coordinator.Installed(s.context().id, writer);
    x.coordinator.WalDurable(s.context().id, Wal(writer, 50));
}
void CheckInvisible(const Snapshot& s, const TxnId& creator, std::optional<Status> status) {
    const auto result = Visible(s, {creator, false, false}, status);
    CHECK(result == Visibility::FollowHistory || result == Visibility::NeedStatus);
}
void ClockAndDurablePrefix() {
    std::array<int, 4> order{0, 1, 2, 3};
    do {
        CommitCoordinator coordinator(epoch, {0, 1, 2});
        auto certified = [&] { return coordinator.HeadPosition() - 1; };
        SnapshotNode a(epoch, 0, {0, 1, 2}, certified), b(epoch, 1, {0, 1, 2}, certified),
                     c(epoch, 2, {0, 1, 2}, certified);
        const auto low = a.Begin(1), high = b.Begin(1);
        const auto id = low.context().id;
        const Version version{id, false, false};
        coordinator.ObserveWriterWal(Wal(1, 100));  // proof before the reservation, another stream
        coordinator.ObserveWriterWal(Wal(0, 49));
        CHECK(coordinator.ReserveClock(Single(low)) == 1);
        CHECK(coordinator.ReserveClock(Single(high, 1)) == 2);
        auto messages = coordinator.TakeMessages();
        CHECK(messages.size() == 6);
        SnapshotNode* nodes[] = {&a, &b, &c};
        for (const auto& m : messages) {
            CHECK(m.phase == Phase::Clock);
            nodes[m.node]->Receive(m);
            if (m.resolution.slot == 2) coordinator.Acknowledge(m);
        }
        auto during = c.Begin(1);
        for (int op : order) {
            CHECK(!coordinator.Succeeded(id) && !coordinator.Succeeded(high.context().id));
            CHECK(c.Begin(10 + op).context().snapshot == 0);
            CheckInvisible(during, id, c.Lookup(id));
            if (op == 3) coordinator.ObserveWriterWal(Wal(0, 50));
            else {
                coordinator.Acknowledge(messages[op]);
                coordinator.Acknowledge(messages[op]);
            }
        }
        CHECK(coordinator.Succeeded(id) && coordinator.Succeeded(high.context().id));
        CHECK(coordinator.TakeMessages().empty()); // no post-WAL Advance round
        auto after = b.Begin(2);
        CHECK(after.context().snapshot == 2);
        const auto imported = b.Export(after, 2);
        CHECK(c.Attach(imported));
        CHECK(Visible(c.Imported(imported), version, c.Lookup(id)) == Visibility::Value);
        CHECK(Visible(during, version, c.Lookup(id)) == Visibility::FollowHistory);
        CHECK(CheckWrite(during, version, c.Lookup(id)) == WriteCheck::SnapshotConflict);
        coordinator.ObserveWriterWal(Wal(0, 40));
        CHECK(coordinator.ReserveClock(Single(low)) == 1 && certified() == 2);
    } while (std::next_permutation(order.begin(), order.end()));
}
void PendingBeforeWal() {
    // Permute durability and the three first-round ACKs. No combination of
    // duplicate ACKs or a smaller durable prefix can replace the missing proof.
    std::array<int, 4> order{0, 1, 2, 3};
    do {
        Cluster x;
        auto writer = x.a.Begin(1), later = x.b.Begin(1), old = x.c.Begin(1);
        const auto& id = writer.context().id;
        const auto plan = Single(writer);
        CHECK(x.coordinator.Reserve(plan, true) == 1);
        CHECK(x.coordinator.Reserve(plan, true) == 1);
        CHECK(x.coordinator.Reserve(Single(later, 1), true) == 2);
        Ready(x, later, 1);
        auto pending = x.coordinator.TakeMessages();
        CHECK(pending.size() == 6);
        for (size_t i = 3; i < pending.size(); ++i) {
            CHECK(pending[i].phase == Phase::Pending && pending[i].resolution.slot == 2);
            x.Deliver(pending[i]); // later slot fully announced before any ACK for slot one
        }
        CHECK(x.coordinator.TakeMessages().empty() && !x.coordinator.CanReleaseWrites(later.context().id));
        CHECK(x.b.Lookup(later.context().id)->state == State::Active);
        x.coordinator.WalDurable(id, Wal(0, 49));
        for (int op : order) {
            CHECK(x.coordinator.TakeMessages().empty());
            CHECK(x.coordinator.Lookup(id).state == State::Installing);
            if (op == 3) x.coordinator.WalDurable(id, Wal(0, 50));
            else { x.Deliver(pending[op]); x.Deliver(pending[op]); }
            CHECK(x.c.SafeScn() == 0 && !x.coordinator.CanReleaseWrites(id));
            CHECK(x.a.Lookup(id)->state == State::Active);
            CheckInvisible(old, id, x.c.Lookup(id));
        }
        auto advance = x.coordinator.TakeMessages();
        CHECK(advance.size() == 6 && advance[0].phase == Phase::Advance);
        CHECK(advance[3].phase == Phase::Advance && advance[3].resolution.slot == 2);
        x.Deliver(advance[0]);
        auto fresh = x.a.Begin(2);
        const auto exported = x.a.Export(fresh, 1);
        CHECK(exported.snapshot == 1);
        // Delay B's proof past the import. It cannot expose pending as committed.
        auto imported = std::async(std::launch::async, [&] {
            CHECK(x.b.Attach(exported));
            return Visible(x.b.Imported(exported), {id, false, false}, x.b.Lookup(id));
        });
        CHECK(imported.wait_for(std::chrono::milliseconds(2)) == std::future_status::timeout);
        x.Deliver(advance[1]);
        CHECK(imported.get() == Visibility::Value);
        CHECK(!x.coordinator.Succeeded(id));
        // A can observe 2 while C is still at 0. An imported snapshot of 2
        // must remain blocked even after C receives the Advance for slot 1.
        x.Deliver(advance[3]);
        x.Deliver(advance[4]);
        auto second = x.a.Begin(3);
        auto on_c = x.a.Export(second, 2);
        CHECK(on_c.snapshot == 2 && x.c.SafeScn() == 0);
        auto catch_up = std::async(std::launch::async, [&] { return x.c.Attach(on_c); });
        CHECK(catch_up.wait_for(std::chrono::milliseconds(2)) == std::future_status::timeout);
        const auto held = x.c.Receive(advance[2]);
        CHECK(catch_up.wait_for(std::chrono::milliseconds(2)) == std::future_status::timeout);
        x.Deliver(advance[5]);
        CHECK(catch_up.get() && x.c.Imported(on_c).context().snapshot == 2);
        CHECK(!x.coordinator.CanReleaseWrites(later.context().id));
        x.coordinator.Acknowledge(held);
        CHECK(x.coordinator.Succeeded(id) && x.coordinator.CanReleaseWrites(id));
        x.Drain();
        x.Deliver(pending[1]); x.Deliver(advance[1]);
        CHECK(x.b.SafeScn() == 2 && x.coordinator.Succeeded(later.context().id));
        CHECK(Visible(old, {id, false, false}, x.c.Lookup(id)) == Visibility::FollowHistory);
    } while (std::next_permutation(order.begin(), order.end()));
}
void HoleAndAckOrdering() {
    Cluster x;
    auto low = x.a.Begin(1);
    auto high = x.b.Begin(1); // same local ID on a different home is a distinct ID
    auto old = x.c.Begin(1);
    auto l = x.coordinator.Reserve(Single(low));
    auto h = x.coordinator.Reserve(Single(high, 1));
    CHECK(l == 1 && h == 2);
    auto again = x.coordinator.Reserve(Single(high, 1));
    CHECK(again == h);
    Ready(x, high, 1);
    auto messages = x.coordinator.TakeMessages();
    CHECK(messages.empty() && x.b.SafeScn() == 0 && !x.coordinator.Succeeded(high.context().id));
    auto during_hole = x.c.Begin(2);
    x.coordinator.DecideCommit(low.context().id);
    x.coordinator.Installed(low.context().id, 0);
    x.coordinator.WalDurable(low.context().id, Wal(0, 49));
    messages = x.coordinator.TakeMessages();
    CHECK(messages.empty());
    x.coordinator.WalDurable(low.context().id, Wal(0, 50));
    messages = x.coordinator.TakeMessages();
    CHECK(messages.size() == 3 && messages.front().resolution.slot == 1);
    x.Deliver(messages[2]); x.Deliver(messages[2]); x.Deliver(messages[0]);
    auto none = x.coordinator.TakeMessages();
    CHECK(none.empty() && x.a.SafeScn() == 0);
    CHECK(!x.coordinator.CanReleaseWrites(low.context().id));
    x.Deliver(messages[1]);
    auto advance = x.coordinator.TakeMessages();
    CHECK(advance.size() == 3);
    x.Deliver(advance[0]);
    auto before_success = x.a.Begin(2);
    CHECK(before_success.context().snapshot == 1);
    CHECK(x.b.SafeScn() == 0 && !x.coordinator.Succeeded(low.context().id));
    // Cross-node execution uses the imported snapshot, even while B's Begin boundary is old.
    auto context = x.a.Export(before_success, 1);
    const bool attached = x.b.Attach(context);
    CHECK(attached);
    auto imported = x.b.Imported(context);
    CHECK(Visible(imported, {low.context().id, false, false}, x.b.Lookup(low.context().id)) == Visibility::Value);
    x.Deliver(advance[1]); x.Deliver(advance[1]);
    CHECK(!x.coordinator.Succeeded(low.context().id));
    x.Deliver(advance[2]);
    CHECK(x.coordinator.Succeeded(low.context().id) && x.coordinator.CanReleaseWrites(low.context().id));
    auto new_on_b = x.b.Begin(2);
    CHECK(new_on_b.context().snapshot == 1);
    CHECK(Visible(new_on_b, {low.context().id, false, false}, x.b.Lookup(low.context().id)) == Visibility::Value);
    x.Drain(); // the high position may only publish after the complete low round
    CHECK(x.coordinator.Succeeded(high.context().id));
    x.Deliver(messages[0]); x.Deliver(advance[2]); // stale duplicates cannot regress safe
    CHECK(x.a.SafeScn() == 2 && x.c.SafeScn() == 2);
    for (const auto* reader : {&old, &during_hole}) {
        CHECK(reader->context().snapshot == 0);
        CheckInvisible(*reader, low.context().id, x.c.Lookup(low.context().id));
        CheckInvisible(*reader, high.context().id, x.c.Lookup(high.context().id));
    }
    x.b.Finish(context); x.b.Finish(context);
    x.a.RemoteFinished(context, 1); x.a.RemoteFinished(context, 1);
    const bool reopened = x.b.Attach(context);
    CHECK(!reopened);
    x.a.End(before_success);
    x.a.End(low, State::Committed); x.b.End(high, State::Committed);
    x.c.End(old); x.c.End(during_hole); x.b.End(new_on_b);
    CHECK(x.a.RetentionFloor() == 2 && x.b.RetentionFloor() == 2 && x.c.RetentionFloor() == 2);
    std::cout << "PASS: low/high hole, two ACK rounds, fixed snapshots, cross-node Begin/context\n";
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
    for (auto state : {State::Active, State::Preparing, State::Installing, State::Aborting, State::Aborted}) {
        CHECK(Visible(old, own, Status{state, 0}) == Visibility::FollowHistory);
        CHECK(CheckWrite(old, own, Status{state, 0}) ==
              (state == State::Aborted ? WriteCheck::FollowHistory : WriteCheck::WaitForWriter));
    }
    const auto slot = x.coordinator.Reserve(Single(writer));
    Ready(x, writer); x.Drain();
    auto equal = x.b.Begin(2);
    CHECK(equal.context().snapshot == slot);
    CHECK(Visible(equal, own, x.b.Lookup(own.creator)) == Visibility::Value);
    CHECK(CheckWrite(equal, own, x.b.Lookup(own.creator)) == WriteCheck::Allowed);
    CHECK(Visible(old, own, x.b.Lookup(own.creator)) == Visibility::FollowHistory);
    CHECK(CheckWrite(old, own, x.b.Lookup(own.creator)) == WriteCheck::SnapshotConflict);
    own.deleted = true;
    CHECK(Visible(equal, own, x.b.Lookup(own.creator)) == Visibility::NotFound);
    CHECK(CheckWrite(old, own, x.b.Lookup(own.creator)) == WriteCheck::SnapshotConflict);
    // The business candidate is unchanged by acquiring/releasing another tuple lock.
    struct Row { Version version; TxnId lock_owner; } row{own, writer.context().id};
    row.lock_owner = old.context().id;
    CHECK(Visible(equal, row.version, x.b.Lookup(row.version.creator)) == Visibility::NotFound);
    row.lock_owner = {};
    CHECK(Visible(equal, row.version, x.b.Lookup(row.version.creator)) == Visibility::NotFound);
    std::cout << "PASS: equality, self writes, initialization/tombstones, creator != lock owner, SI conflicts\n";
}
void EvidencePermutations() {
    std::array<int, 7> order{0, 1, 2, 3, 4, 5, 6};
    int count = 0;
    do {
        Cluster x;
        auto s = x.a.Begin(1);
        const auto slot = x.coordinator.Reserve(Multi(s));
        CHECK(slot == 1);
        const auto id = s.context().id;
        x.coordinator.DecideCommit(id);
        // Every proper prefix omits at least one required proof.
        for (size_t i = 0; i < order.size(); ++i) {
            switch (order[i]) {
                case 0: x.coordinator.Prepared(id, Wal(0, 40)); break;
                case 1: x.coordinator.Prepared(id, Wal(1, 60)); break;
                case 2: x.coordinator.WalDurable(id, Wal(0, 50)); break;
                case 3: x.coordinator.WalDurable(id, Wal(1, 70)); break;
                case 4: x.coordinator.Installed(id, 0); break;
                case 5: x.coordinator.Installed(id, 1); break;
                case 6: x.coordinator.DecisionDurable(id, Wal(0, 80)); break;
            }
            if (i + 1 < order.size()) {
                auto pending = x.coordinator.TakeMessages();
                CHECK(pending.empty() && x.c.SafeScn() == 0);
            }
            CHECK(!x.coordinator.Succeeded(id) && !x.coordinator.CanReleaseWrites(id));
        }
        x.Drain();
        CHECK(x.coordinator.Succeeded(id));
        auto reader = x.c.Begin(1);
        CHECK(reader.context().snapshot == 1);
        CHECK(Visible(reader, {id, false, false}, x.c.Lookup(id)) == Visibility::Value);
        ++count;
    } while (std::next_permutation(order.begin(), order.end()));
    CHECK(count == 5040);
    std::cout << "PASS: all 5040 Prepare/WAL/decision/install notification permutations\n";
}
void AbortAndCornus() {
    Cluster x;
    auto aborted = x.a.Begin(1), high = x.a.Begin(2);
    const auto low_slot = x.coordinator.Reserve(Multi(aborted));
    const auto high_slot = x.coordinator.Reserve(Single(high));
    CHECK(low_slot == 1 && high_slot == 2);
    Ready(x, high);
    x.coordinator.Abort(aborted.context().id, {{0, 81}, {1, 91}});
    x.coordinator.RolledBack(aborted.context().id, Wal(1, 91));
    auto pending = x.coordinator.TakeMessages();
    CHECK(pending.empty());
    x.coordinator.RolledBack(aborted.context().id, Wal(0, 80));
    pending = x.coordinator.TakeMessages(); CHECK(pending.empty());
    x.coordinator.RolledBack(aborted.context().id, Wal(0, 81));
    x.Drain();
    CHECK(!x.coordinator.Succeeded(aborted.context().id) && x.coordinator.CanReleaseWrites(aborted.context().id));
    CHECK(x.coordinator.Succeeded(high.context().id));
    CHECK(x.b.Lookup(aborted.context().id)->state == State::Aborted);
    x.coordinator.Abort(aborted.context().id, {{0, 81}, {1, 91}});
    x.coordinator.Prepared(aborted.context().id, Wal(1, 60)); // delayed YES does not resurrect
    x.coordinator.RolledBack(aborted.context().id, Wal(0, 81));
    auto ro = x.b.Begin(1);
    const auto before = ro.context().snapshot;
    x.b.End(ro); x.b.End(ro);
    CHECK(x.b.Lookup(ro.context().id)->state == State::ReadOnlyFinished);
    auto cornus = x.c.Begin(1);
    const auto cornus_slot = x.coordinator.Reserve(Multi(cornus, CommitKind::Cornus));
    CHECK(cornus_slot == before + 1); // read-only end did not allocate a position
    const auto id = cornus.context().id;
    x.coordinator.DecideCommit(id);
    x.coordinator.Installed(id, 0); x.coordinator.Installed(id, 1);
    x.coordinator.WalDurable(id, Wal(0, 50)); x.coordinator.WalDurable(id, Wal(1, 70));
    x.coordinator.Prepared(id, Wal(0, 40));
    pending = x.coordinator.TakeMessages(); CHECK(pending.empty());
    x.coordinator.Prepared(id, Wal(1, 60)); x.Drain();
    CHECK(x.coordinator.Succeeded(id));
    std::cout << "PASS: Abort fills position only after rollback, read-only end, Cornus all-YES gate\n";
}
class Barrier {
public:
    explicit Barrier(int count) : left_(count) {}
    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        --left_;
        if (!left_) cv_.notify_all();
        else cv_.wait(lock, [&] { return !left_; });
    }
private:
    std::mutex mutex_; std::condition_variable cv_; int left_;
};
void BeginAdvanceRaceAndRetention() {
    for (int round = 0; round < 64; ++round) {
        Cluster x;
        auto long_reader = x.b.Begin(1), writer = x.a.Begin(1);
        auto context = x.b.Export(long_reader, 2);
        bool attached = x.c.Attach(context); CHECK(attached);
        const auto slot = x.coordinator.Reserve(Single(writer)); CHECK(slot == 1);
        Ready(x, writer);
        auto stage = x.coordinator.TakeMessages();
        for (const auto& m : stage) x.Deliver(m);
        auto advance = x.coordinator.TakeMessages();
        Barrier start(3);
        std::optional<Snapshot> racing;
        Scn sampled = 99;
        std::thread begin([&] { start.Wait(); racing = x.b.Begin(2); sampled = x.b.RetentionFloor(); });
        std::thread publish([&] { start.Wait(); x.Deliver(advance[1]); });
        start.Wait(); begin.join(); publish.join();
        CHECK(racing && racing->context().snapshot <= 1 && sampled == 0);
        const auto result = Visible(*racing, {writer.context().id, false, false}, x.b.Lookup(writer.context().id));
        CHECK(result == (racing->context().snapshot == 0 ? Visibility::FollowHistory : Visibility::Value));
        x.Deliver(advance[0]); x.Deliver(advance[2]);
        CHECK(Visible(*racing, {writer.context().id, false, false}, x.b.Lookup(writer.context().id)) == result);
        CHECK(x.b.RetentionFloor() == 0); // pinned old snapshot survives publication
        x.c.Finish(context); x.b.RemoteFinished(context, 2); x.b.End(long_reader);
        x.b.End(*racing);
        CHECK(x.b.RetentionFloor() == 1);
    }
    std::cout << "PASS: 64 barrier-controlled Begin/Advance races and remote snapshot retention\n";
}
void ConcurrentReservations() {
    Cluster x;
    auto old = x.c.Begin(1);
    std::vector<Snapshot> writers;
    for (int i = 0; i < 8; ++i) writers.push_back(x.Node(i % 2).Begin(i + 1));
    Barrier start(9);
    std::array<Scn, 8> slots{};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) threads.emplace_back([&, i] {
        start.Wait();
        const auto plan = Single(writers[i], i % 2);
        slots[i] = x.coordinator.Reserve(plan);
        const auto repeated = x.coordinator.Reserve(plan);
        CHECK(repeated == slots[i]);
    });
    start.Wait();
    for (auto& thread : threads) thread.join();
    CHECK((std::set<Scn>(slots.begin(), slots.end()) == std::set<Scn>{1, 2, 3, 4, 5, 6, 7, 8}));
    for (Scn slot = 8; slot != 0; --slot) {
        const auto index = std::find(slots.begin(), slots.end(), slot) - slots.begin();
        Ready(x, writers[index], index % 2);
        if (slot != 1) {
            auto messages = x.coordinator.TakeMessages(); CHECK(messages.empty());
        }
    }
    x.Drain();
    auto fresh = x.c.Begin(2);
    CHECK(fresh.context().snapshot == 8 && old.context().snapshot == 0);
    for (const auto& writer : writers) {
        const auto id = writer.context().id;
        CHECK(x.coordinator.Succeeded(id));
        CHECK(Visible(fresh, {id, false, false}, x.c.Lookup(id)) == Visibility::Value);
        CHECK(Visible(old, {id, false, false}, x.c.Lookup(id)) == Visibility::FollowHistory);
    }
    std::cout << "PASS: 8 concurrent reservations, idempotent allocation, reverse readiness\n";
}
void WriterSetAndQuorum() {
    Cluster x;
    auto s = x.a.Begin(1);
    Plan p{s.context(), CommitKind::Cornus, {0, 1}, {}, {}, std::nullopt};
    const auto slot = x.coordinator.Reserve(p);
    CHECK(slot == 1);
    x.coordinator.DecideCommit(s.context().id);
    x.coordinator.Installed(s.context().id, 0);
    x.coordinator.Installed(s.context().id, 1);
    CHECK(x.coordinator.TakeMessages().empty())
        << "Cornus without per-writer prefixes needs the storage-collected YES set";
    x.coordinator.PreparedQuorum(s.context().id);
    x.coordinator.PreparedQuorum(s.context().id); // idempotent observation
    auto messages = x.coordinator.TakeMessages();
    CHECK(messages.size() == 3 && messages.front().resolution.slot == slot);
    for (const auto& message : messages) x.Deliver(message);
    x.Drain();
    CHECK(x.coordinator.Succeeded(s.context().id));
    CHECK(x.coordinator.CanReleaseWrites(s.context().id));
    auto reader = x.c.Begin(1);
    CHECK(reader.context().snapshot == slot);
    CHECK(Visible(reader, {s.context().id, false, false}, x.c.Lookup(s.context().id)) ==
          Visibility::Value);
    std::cout << "PASS: writer set and Cornus durable-quorum evidence gate\n";
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
    Dies([] { Cluster x; auto s = x.a.Begin(1); auto p = Single(s); x.coordinator.Reserve(p);
              p.wal[0]++; x.coordinator.Reserve(p); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.coordinator.Reserve(Single(s));
              x.coordinator.DecideCommit(s.context().id); x.coordinator.Abort(s.context().id, {{0, 51}}); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.coordinator.Reserve(Single(s));
              x.coordinator.Abort(s.context().id, {{0, 51}}); x.coordinator.DecideCommit(s.context().id); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.coordinator.Reserve(Single(s)); Ready(x, s);
              auto messages = x.coordinator.TakeMessages(); messages[0].resolution.slot = 2; x.Deliver(messages[0]); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.coordinator.Reserve(Single(s)); Ready(x, s);
              auto messages = x.coordinator.TakeMessages(); x.Deliver(messages[0]);
              messages[0].resolution.committed = false; x.Deliver(messages[0]); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.coordinator.Reserve(Single(s)); Ready(x, s);
              auto messages = x.coordinator.TakeMessages(); messages[0].phase = Phase::Advance;
              x.coordinator.Acknowledge(messages[0]); });
    Dies([] { Cluster x; auto w = x.a.Begin(1); x.coordinator.Reserve(Single(w)); Ready(x, w); x.Drain();
              auto s = x.a.Begin(2); auto c = x.a.Export(s, 1); x.b.Attach(c);
              c.snapshot = 0; x.b.Attach(c); });
    Dies([] { Cluster x; auto s = x.a.Begin(1);
              x.coordinator.Reserve({s.context(), CommitKind::TwoPhase, {0, 1},
                                     {{0, 10}, {1, 20}}, {{0, 10}, {1, 20}}, std::nullopt}); });
    Dies([] { Cluster x; auto s = x.a.Begin(1);
              x.coordinator.Reserve({s.context(), CommitKind::SingleWriter, {0},
                                     {{1, 10}}, {}, std::nullopt}); });
    Dies([] { Cluster x; auto s = x.a.Begin(1);
              x.coordinator.Reserve({s.context(), CommitKind::SingleWriter, {0},
                                     {{0, 10}}, {}, std::nullopt});
              x.coordinator.DecideCommit(s.context().id);
              x.coordinator.Installed(s.context().id, 1); });
    Dies([] { Cluster x; auto s = x.a.Begin(1);
              Plan p{s.context(), CommitKind::Cornus, {0, 1}, {{0, 10}, {1, 20}},
                     {{0, 10}}, std::nullopt};
              x.coordinator.Reserve(p); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.a.Export(s, 1); x.a.End(s); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.a.End(s); x.a.Begin(1); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.coordinator.Reserve(Single(s));
              x.coordinator.WalDurable(s.context().id, Wal(2, 999)); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); auto p = Single(s); p.context.id.epoch[0] = 'x';
              x.coordinator.Reserve(p); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); auto t = x.b.Begin(1);
              Visible(s, {t.context().id, false, false}, Status{State::Committed, 0}); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.coordinator.Reserve(Single(s)); Ready(x, s);
              auto messages = x.coordinator.TakeMessages();
              for (const auto& message : messages) x.Deliver(message);
              x.Drain(); messages[0].resolution.slot = 2; x.a.Receive(messages[0]); });
    Dies([] { Cluster x; auto s = x.a.Begin(1); x.coordinator.Reserve(Single(s)); Ready(x, s);
              auto messages = x.coordinator.TakeMessages(); messages[0].resolution.committed = false;
              x.coordinator.Acknowledge(messages[0]); });
    std::cout << "PASS: 14 invariant death checks (including NDEBUG build)\n";
}
}
static void RecoveredSnapshot() {
    Recovery image;
    image.safe = 4;
    image.committed = {{0, {{2, 3}, {6, 6}}}, {1, {{2, 2}}}};
    SnapshotNode reader(epoch, 1, {0, 1}, {}, image);
    const auto next = reader.Begin(7);
    CHECK(next.context().snapshot == 4);
    CHECK(reader.Lookup({epoch, 0, 3})->commit_scn == 4);
    CHECK(reader.Lookup({epoch, 1, 2})->state == State::Committed);
    CHECK(!reader.Lookup({epoch, 0, 4}) && !reader.Lookup({epoch, 1, 3}));
    CommitCoordinator coordinator(epoch, {0, 1}, image.safe);
    CHECK(coordinator.ReserveClock(Single(next, 1)) == 5);
    Dies([&] { reader.Lookup({std::string(16, 'x'), 0, 2}); });
    Dies([&] { reader.Attach({{epoch, 0, 9}, 3}); });
}

int main() {
    EnableFatalChecks();
    // Fork before starting any threads or RPC runtime.
    IllegalTransitions();
    RecoveredSnapshot();
    ClockAndDurablePrefix();
    PendingBeforeWal(); HoleAndAckOrdering(); VisibilityAndConflict(); EvidencePermutations(); WriterSetAndQuorum();
    AbortAndCornus(); BeginAdvanceRaceAndRetention(); ConcurrentReservations();
    std::cout << "mvcc_protocol_test: all checks passed\n";
}
