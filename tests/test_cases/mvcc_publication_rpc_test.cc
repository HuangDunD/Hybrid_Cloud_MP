#include "common/fatal_checks.h"
#include "mvcc/publication_rpc.h"
#include <atomic>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/logging.h>
#include <iostream>

using namespace mvcc;
int main() {
    EnableFatalChecks();
    const std::string epoch(16, 'r');
    std::atomic<Timestamp> now{0};
    RemoteTso tso{[&] { return now.load(); }, [&](Timestamp) { return ++now; }};
    SnapshotNode home(epoch, 0, {0, 1}, tso), peer(epoch, 1, {0, 1}, tso);
    struct Host final : PublicationHost {
        explicit Host(SnapshotNode& node) : node(node) {}
        std::optional<Status> LookupStatus(const TxnId& id) const override { return node.Lookup(id); }
        SnapshotNode& node;
    } home_host(home), peer_host(peer);

    PublicationService home_service(home, home_host), peer_service(peer, peer_host);
    brpc::Server home_server, peer_server;
    CHECK_EQ(home_server.AddService(&home_service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    CHECK_EQ(peer_server.AddService(&peer_service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    brpc::ServerOptions options;
    options.num_threads = 4;
    CHECK_EQ(home_server.Start("127.0.0.1:0", &options), 0);
    CHECK_EQ(peer_server.Start("127.0.0.1:0", &options), 0);
    brpc::Channel to_home, to_peer;
    CHECK_EQ(to_home.Init(butil::endpoint2str(home_server.listen_address()).c_str(), nullptr), 0);
    CHECK_EQ(to_peer.Init(butil::endpoint2str(peer_server.listen_address()).c_str(), nullptr), 0);

    auto writer = home.Begin(1), old = peer.Begin(1);
    const auto id = writer.context().id;
    CHECK(SendStatusQuery(to_home, id)->state == State::Active);
    const auto c = home.AllocateCommitTs(writer.context());
    CHECK(SendStatusQuery(to_home, id)->state == State::Committing);
    const Version version{id, false, false};
    CHECK(Visible(old, version, SendCreatorAt(to_home, id, old.context().snapshot)) == Visibility::FollowHistory);
    home.CertifyRemoteCommit(writer.context(), c);
    home.End(writer, State::Committed);
    CHECK_EQ(SendStatusQuery(to_home, id)->commit_ts, c);

    auto fresh = home.Begin(2);
    const auto context = home.Export(fresh, 1);
    CHECK(SendAttach(to_peer, context));
    CHECK(SendAttach(to_peer, context));
    auto imported = peer.Imported(context);
    CHECK_EQ(imported.context().snapshot, c);
    CHECK(Visible(imported, version, SendCreatorAt(to_home, id, c)) == Visibility::Value);
    CHECK(Visible(old, version, SendCreatorAt(to_home, id, old.context().snapshot)) == Visibility::FollowHistory);
    SendFinish(to_peer, context);
    home.RemoteFinished(context, 1);
    SendFinish(to_peer, context);
    home.RemoteFinished(context, 1);
    CHECK(!SendAttach(to_peer, context));
    home.End(fresh);
    peer.End(old);
    home_server.Stop(0);
    peer_server.Stop(0);
    home_server.Join();
    peer_server.Join();
    std::cout << "PASS: creator status, snapshot reads and idempotent participant attach/finish over brpc\n";
}
