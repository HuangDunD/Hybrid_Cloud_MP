#include "common/fatal_checks.h"
#include "mvcc/publication_rpc.h"
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/logging.h>
#include <iostream>

using namespace mvcc;
int main() {
    EnableFatalChecks();
    const std::string epoch(16, 'r');
    const std::set<NodeId> members{0, 1, 2};
    SnapshotNode a(epoch, 0, members), b(epoch, 1, members), c(epoch, 2, members);
    CommitCoordinator coordinator(epoch, members);
    struct Host : PublicationHost {
        explicit Host(SnapshotNode& n) : node(n) {}
        SnapshotNode& node;
        Scn AllocateClock(const Context&) override { CHECK(false); return 0; }
        Scn ClockCommit(const Context&, Scn, uint64_t) override { CHECK(false); return 0; }
        Scn ReportWal(const WalPosition&) override { CHECK(false); return 0; }
        Scn ReadCertified(const std::string&) const override { CHECK(false); return 0; }
        Scn ReserveLazy(const Context&, uint64_t) override { CHECK(false); return 0; }
        Scn SubmitCommit(const Context&, uint64_t, bool) override { CHECK(false); return 0; }
        Scn SubmitTwoPhase(const Context&, CommitKind, const std::set<NodeId>&,
                           const std::map<NodeId, uint64_t>&, const std::map<NodeId, uint64_t>&,
                           std::optional<WalPosition>, bool, const std::set<NodeId>&) override {
            CHECK(false); return 0;
        }
        Scn ReserveTwoPhase(const Context&, CommitKind, const std::set<NodeId>&,
                            const std::map<NodeId, uint64_t>&, const std::map<NodeId, uint64_t>&,
                            std::optional<WalPosition>, bool) override { CHECK(false); return 0; }
        Scn CompleteTwoPhase(const Context&, const std::set<NodeId>&) override { CHECK(false); return 0; }
        std::optional<Status> LookupPublication(const TxnId& id) const override { return node.Lookup(id); }
    } host_a{a}, host_b{b}, host_c{c};
    PublicationService service_a(a, host_a), service_b(b, host_b), service_c(c, host_c);
    brpc::Server server_a, server_b, server_c;
    int result = server_a.AddService(&service_a, brpc::SERVER_DOESNT_OWN_SERVICE); CHECK(result == 0);
    result = server_b.AddService(&service_b, brpc::SERVER_DOESNT_OWN_SERVICE); CHECK(result == 0);
    result = server_c.AddService(&service_c, brpc::SERVER_DOESNT_OWN_SERVICE); CHECK(result == 0);
    brpc::ServerOptions options; options.num_threads = 4;
    result = server_a.Start("127.0.0.1:0", &options); CHECK(result == 0);
    result = server_b.Start("127.0.0.1:0", &options); CHECK(result == 0);
    result = server_c.Start("127.0.0.1:0", &options); CHECK(result == 0);
    brpc::Channel channels[3];
    result = channels[0].Init(butil::endpoint2str(server_a.listen_address()).c_str(), nullptr); CHECK(result == 0);
    result = channels[1].Init(butil::endpoint2str(server_b.listen_address()).c_str(), nullptr); CHECK(result == 0);
    result = channels[2].Init(butil::endpoint2str(server_c.listen_address()).c_str(), nullptr); CHECK(result == 0);
    auto writer = a.Begin(1), old = b.Begin(1);
    const auto id = writer.context().id;
    const auto slot = coordinator.Reserve({writer.context(), CommitKind::SingleWriter, {0}, {{0, 100}}, {}, std::nullopt});
    CHECK(slot == 1);
    coordinator.DecideCommit(id); coordinator.Installed(id, 0);
    coordinator.WalDurable(id, {epoch, 0, 99});
    auto messages = coordinator.TakeMessages(); CHECK(messages.empty());
    coordinator.WalDurable(id, {epoch, 0, 100});
    messages = coordinator.TakeMessages(); CHECK(messages.size() == members.size());
    const auto& staged = messages;
    a.Receive(staged[0]);
    SendPublications(channels[1], {staged[1], staged[1]});
    SendPublications(channels[2], {staged[2]});
    CHECK(staged.size() == members.size());
    coordinator.Acknowledge(staged[2]);
    auto ack = staged[1];
    coordinator.Acknowledge(ack); coordinator.Acknowledge(ack);
    auto none = coordinator.TakeMessages(); CHECK(none.empty());
    CHECK(b.SafeScn() == 0 && !coordinator.Succeeded(id));
    coordinator.Acknowledge(staged[0]);
    messages = coordinator.TakeMessages(); CHECK(messages.size() == members.size());
    ack = SendPublication(channels[0], messages[0]); coordinator.Acknowledge(ack);
    coordinator.Acknowledge(SendPublication(channels[2], messages[2]));
    auto fresh_a = a.Begin(2);
    auto context = a.Export(fresh_a, 1);
    const bool attached = SendAttach(channels[1], context); CHECK(attached);
    const bool duplicate = SendAttach(channels[1], context); CHECK(duplicate);
    auto imported = b.Imported(context);
    CHECK(imported.context().snapshot == 1 && b.SafeScn() == 0);
    CHECK(Visible(imported, {id, false, false}, b.Lookup(id)) == Visibility::Value);
    CHECK(!coordinator.Succeeded(id));
    ack = SendPublication(channels[1], messages[1]); // hold ACK after receiver has advanced
    CHECK(b.SafeScn() == 1 && !coordinator.Succeeded(id) && !coordinator.CanReleaseWrites(id));
    coordinator.Acknowledge(ack);
    CHECK(coordinator.Succeeded(id) && coordinator.CanReleaseWrites(id));
    auto fresh_b = b.Begin(2);
    CHECK(fresh_b.context().snapshot == 1);
    CHECK(Visible(fresh_b, {id, false, false}, b.Lookup(id)) == Visibility::Value);
    CHECK(Visible(old, {id, false, false}, b.Lookup(id)) == Visibility::FollowHistory);
    SendFinish(channels[1], context); a.RemoteFinished(context, 1);
    SendFinish(channels[1], context); a.RemoteFinished(context, 1);
    const bool reopened = SendAttach(channels[1], context); CHECK(!reopened);
    a.End(fresh_a); a.End(writer, State::Committed); b.End(old); b.End(fresh_b);
    CHECK(a.RetentionFloor() == 1 && b.RetentionFloor() == 1);
    server_a.Stop(0); server_b.Stop(0); server_c.Stop(0);
    server_a.Join(); server_b.Join(); server_c.Join();
    std::cout << "PASS: real loopback brpc Stage/Advance and context transfer, ephemeral ports, held ACK\n";
    std::cout << "mvcc_publication_rpc_test: all checks passed (WAL/install events controlled, no business pages)\n";
}
