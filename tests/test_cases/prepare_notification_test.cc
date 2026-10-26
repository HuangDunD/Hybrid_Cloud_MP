// Local protocol integration: real brpc, storage LogWrite/fdatasync, notifier,
// coordinator client and participant context; page updates are simulated here.
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <thread>
#include <brpc/server.h>
#include <brpc/controller.h>
#include "compute_node/prepare_notification.h"
#include "storage/prepared_log_record.h"
#include "storage/storage_rpc.h"

class Gate {
public:
    explicit Gate(bool open = true) : open_(open) {}
    void Wait() {
        std::unique_lock<bthread::Mutex> guard(mutex_);
        while (!open_) cv_.wait(guard);
    }
    void Open() {
        std::lock_guard<bthread::Mutex> guard(mutex_);
        open_ = true;
        cv_.notify_all();
    }
    void Close() { std::lock_guard<bthread::Mutex> guard(mutex_); open_ = false; }
private:
    bthread::Mutex mutex_;
    bthread::ConditionVariable cv_;
    bool open_;
};

template <typename Predicate> void Until(Predicate p) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!p() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    CHECK(p()) << "Local protocol test timed out";
}

static std::string Encode(const LogRecord& record) {
    std::string result(record.log_tot_len_, '\0');
    record.serialize(&result[0]);
    return result;
}

static prepared_2pc::Requests Requests(uint64_t txn, std::initializer_list<int> writers) {
    prepared_2pc::Requests requests;
    for (int node : writers) {
        auto& r = requests[node];
        r.set_protocol_version(prepared_2pc::kProtocolVersion);
        r.set_transaction_id(txn);
        r.mutable_identity()->set_coordinator_node(0);
        r.mutable_identity()->set_coordinator_incarnation(std::string(16, 'n'));
        r.mutable_identity()->set_local_tx_id(txn);
        r.set_start_ts(txn * 100);
        r.set_commit_ts(txn * 100 + 1);
        for (int writer : writers) r.add_participants(writer);
        r.set_participant_node(node);
        auto* w = r.add_writes();
        w->mutable_item_id()->set_table_id(0);
        w->mutable_item_id()->set_page_no(node + 1);
        w->mutable_item_id()->set_slot_id(0);
        w->set_table_name("notification_test");
        w->set_key(node);
        w->set_operation(twopc_service::PreparedWrite::UPDATE);
        w->set_value("after image");
    }
    return requests;
}

static void Append(brpc::Channel* channel, const std::string& bytes) {
    storage_service::StorageService_Stub stub(channel);
    storage_service::LogWriteRequest request;
    storage_service::LogWriteResponse response;
    brpc::Controller controller;
    request.set_log(bytes);
    stub.LogWrite(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed()) << controller.ErrorText();
}

class Node : public twopc_service::TwoPCService {
public:
    prepared_2pc::PrepareNotifications* coordinator;
    brpc::Channel* storage;
    LogManager* log_manager;
    prepared_2pc::Registry contexts;
    Gate mark_prepared, prepare_reply, commit_reply;
    std::atomic<int> entered_commit{0}, applied{0}, prepare_replies{0}, notifications{0};

    void AllPrepared(google::protobuf::RpcController*, const twopc_service::AllPreparedRequest* request,
                     twopc_service::AllPreparedResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        CHECK(log_manager->get_fdatasync_count() > 0);
        ++notifications;
        response->set_ok(coordinator->Accept(*request));
    }
    void Prepare(google::protobuf::RpcController*, const twopc_service::PrepareRequest* request,
                 twopc_service::PrepareResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        CHECK(prepared_2pc::ValidRequest(*request));
        bool inserted = false;
        auto context = contexts.Begin(*request, &inserted);
        CHECK(context && inserted);
        PreparedLogRecord record(*request);
        record.lsn_ = request->transaction_id();
        Append(storage, Encode(record));
        // Deliberately let the storage notification/Commit overtake MarkPrepared.
        mark_prepared.Wait();
        contexts.MarkPrepared(context, record.lsn_);
        prepare_reply.Wait();
        response->set_ok(true);
        ++prepare_replies;
    }
    void Commit(google::protobuf::RpcController*, const twopc_service::CommitRequest* request,
                twopc_service::CommitResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        CHECK(request->kind() == twopc_service::CommitRequest::PREPARED);
        ++entered_commit;
        auto context = contexts.TakeForCommit(*request);
        CHECK(context);
        ++applied; // actual business-page operations are covered separately
        contexts.Erase(context);
        commit_reply.Wait();
        response->set_ok(true);
    }
};

static int Start(brpc::Server& server, google::protobuf::Service* service) {
    CHECK(server.AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
    CHECK(server.Start("127.0.0.1:0", nullptr) == 0);
    return server.listen_address().port;
}

int main() {
    const auto old_dir = std::filesystem::current_path();
    char scratch[] = "/tmp/prepare-notification-test-XXXXXX";
    CHECK(mkdtemp(scratch) != nullptr);
    std::filesystem::current_path(scratch);
    {
        DiskManager disk;
        LogReplay replay(&disk, nullptr, "ycsb");
        LogManager logs(&disk, &replay);
        storage_service::StoragePoolImpl storage(&logs, &disk, nullptr, nullptr, 0, nullptr);
        brpc::Server storage_server;
        const int storage_port = Start(storage_server, &storage);
        brpc::Channel storage_channel;
        brpc::ChannelOptions options;
        options.max_retry = 0;
        options.timeout_ms = 10000;
        CHECK(storage_channel.Init(("127.0.0.1:" + std::to_string(storage_port)).c_str(), &options) == 0);
        prepared_2pc::PrepareNotifications coordinator;
        Node nodes[3];
        brpc::Server servers[3];
        brpc::Channel channels[3];
        std::vector<std::string> ips(3, "127.0.0.1");
        std::vector<int> ports;
        for (int i = 0; i < 3; ++i) {
            nodes[i].coordinator = &coordinator;
            nodes[i].storage = &storage_channel;
            nodes[i].log_manager = &logs;
            ports.push_back(Start(servers[i], &nodes[i]));
            CHECK(channels[i].Init(("127.0.0.1:" + std::to_string(ports.back())).c_str(), &options) == 0);
        }
        storage.ConfigurePrepareNotifications(ips, ports, false);
        nodes[1].mark_prepared.Close();
        nodes[1].prepare_reply.Close();
        nodes[2].prepare_reply.Close();
        nodes[2].commit_reply.Close();
        auto requests = Requests(1, {1, 2});
        auto transaction = std::async(std::launch::async, [&] {
            CHECK(coordinator.Prepare(requests, channels));
            coordinator.Commit(requests, channels);
        });
        Until([&] { return nodes[1].entered_commit == 1 && nodes[2].entered_commit == 1; });
        CHECK(nodes[1].applied == 0); // PREPARING was not rejected or applied early
        CHECK(nodes[1].prepare_replies == 0 && nodes[2].prepare_replies == 0);
        nodes[1].mark_prepared.Open();
        Until([&] { return nodes[1].applied == 1 && nodes[2].applied == 1; });
        CHECK(transaction.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
        nodes[2].commit_reply.Open();
        CHECK(transaction.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        transaction.get();
        // Commit finished while BOTH original Prepare responses remain blocked.
        CHECK(nodes[1].prepare_replies == 0 && nodes[2].prepare_replies == 0);
        auto drain = std::async(std::launch::async, [&] { coordinator.Drain(); });
        CHECK(drain.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
        nodes[1].prepare_reply.Open();
        nodes[2].prepare_reply.Open();
        CHECK(drain.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        drain.get();
        std::cout << "PASS: storage notification bypasses Prepare replies; early Commit waits; all Commit replies required; shutdown drains callbacks\n";

        // A coordinator which is itself a writer is also part of the durable set.
        requests = Requests(2, {0, 1, 2});
        CHECK(coordinator.Prepare(requests, channels));
        coordinator.Commit(requests, channels);
        coordinator.Drain();
        CHECK(nodes[0].applied == 1 && nodes[0].notifications == 2);
        std::cout << "PASS: coordinator-as-writer included\n";

        // Real WAL durability plus direct aggregator inspection. Repeated P1
        // records cannot replace P2, and normal completion frees the vote map.
        DurablePrepareNotifier notifier;
        notifier.Configure(ips, ports, false);
        requests = Requests(3, {1, 2});
        auto persist = [&](const twopc_service::PrepareRequest& r) {
            PreparedLogRecord record(r);
            auto bytes = Encode(record);
            logs.write_batch_log_to_disk(bytes);
            notifier.ObserveDurableBatch(bytes);
        };
        const int before = nodes[0].notifications;
        persist(requests.at(1));
        persist(requests.at(1));
        CHECK(notifier.PendingTransactions() == 1 && nodes[0].notifications == before);
        persist(requests.at(2));
        notifier.Drain();
        CHECK(notifier.PendingTransactions() == 0 && nodes[0].notifications == before + 1);
        persist(requests.at(1));
        persist(requests.at(2));
        notifier.Drain();
        CHECK(notifier.PendingTransactions() == 0 && nodes[0].notifications == before + 1);
        auto old = Requests(4, {1, 2});
        for (auto& entry : old) {
            entry.second.set_protocol_version(prepared_2pc::kParticipantReplyVersion);
            persist(entry.second);
        }
        CHECK(notifier.PendingTransactions() == 0 && nodes[0].notifications == before + 1);
        std::cout << "PASS: unique durable writers, duplicate/late votes, v1 compatibility and vote-map cleanup\n";
        auto first_start = Requests(5, {1, 2});
        auto second_start = first_start;
        for (auto& entry : second_start)
            entry.second.mutable_identity()->set_coordinator_incarnation(std::string(16, 'r'));
        const int before_restart = nodes[0].notifications;
        persist(first_start.at(1));
        persist(second_start.at(2));
        CHECK(notifier.PendingTransactions() == 2 && nodes[0].notifications == before_restart);
        persist(first_start.at(2));
        persist(second_start.at(1));
        notifier.Drain();
        CHECK(notifier.PendingTransactions() == 0 && nodes[0].notifications == before_restart + 2);
        std::cout << "PASS: equal local transaction IDs from different incarnations never combine votes\n";


        storage_server.Stop(0);
        storage_server.Join();
        for (auto& server : servers) server.Stop(0);
        for (auto& server : servers) server.Join();
    }
    std::filesystem::current_path(old_dir);
    std::filesystem::remove_all(scratch);
}
