#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include "compute_server/server.h"

namespace obs = recovery_observation;
namespace {
void Check(bool ok, const char* text) { if (!ok) throw std::runtime_error(text); }

class StorageFixture : public storage_service::StorageService {
public:
    std::atomic<int> calls{0}, fault{0};
    std::string Image(int page) {
        std::string result(PAGE_SIZE, '\0');
        RmPageHdr hdr{}; hdr.LLSN_ = fault == 3 ? 1 : 9;
        std::memcpy(result.data(), &hdr, sizeof(hdr));
        uint64_t value = 20260915 + page;
        std::memcpy(result.data() + 64, &value, sizeof(value));
        return result;
    }
    template<class Request, class Response>
    void Reply(google::protobuf::RpcController* controller, const Request* request, Response* response) {
        ++calls;
        if (fault == 1) { controller->SetFailed("injected storage failure"); return; }
        response->set_data(fault == 2 ? "short" : Image(request->page_id(0).page_no()));
    }
    void GetPage(google::protobuf::RpcController* c, const storage_service::GetPageRequest* q,
                 storage_service::GetPageResponse* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Reply(c, q, r);
    }
    void GetPageWithLsn(google::protobuf::RpcController* c, const storage_service::GetPageWithLsnRequest* q,
                        storage_service::GetPageWithLsnResponse* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Reply(c, q, r);
    }
};
class CountedGPLM : public page_table_service::PageTableServiceImpl {
public:
    using PageTableServiceImpl::PageTableServiceImpl;
    std::atomic<int> calls{0};
    std::atomic<bool> missing_replica{false};
    void LRPSLock(google::protobuf::RpcController* c, const page_table_service::PSLockRequest* q,
                  page_table_service::PSLockResponse* r, google::protobuf::Closure* done) override {
        ++calls;
        if (missing_replica) {
            brpc::ClosureGuard guard(done);
            r->set_need_storage_fetch(false); r->set_wait_lock_release(false);
            r->set_newest_node(2); r->set_lsn(9); return;
        }
        PageTableServiceImpl::LRPSLock(c, q, r, done);
    }
    void LRPXLock(google::protobuf::RpcController* c, const page_table_service::PXLockRequest* q,
                  page_table_service::PXLockResponse* r, google::protobuf::Closure* done) override {
        ++calls;
        PageTableServiceImpl::LRPXLock(c, q, r, done);
    }
};
struct OwnedServer {
    brpc::Server server;
    explicit OwnedServer(google::protobuf::Service* service) {
        Check(server.AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0, "add service failed");
        brpc::ServerOptions options;
        Check(server.Start("127.0.0.1:0", &options) == 0, "start loopback service failed");
    }
    ~OwnedServer() { server.Stop(0); server.Join(); }
    std::string Endpoint() const { return butil::endpoint2str(server.listen_address()).c_str(); }
};
void WaitCalls(const std::atomic<int>& count, int target) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (count < target && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
    Check(count >= target, "RPC did not reach wait gate");
}
}

int main() {
    try {
        SYSTEM_MODE = 1; WORKLOAD_MODE = 0; ComputeNodeCount = 3;
        StorageFixture storage;
        OwnedServer storage_server(&storage);
        std::vector<GlobalLockTable*> globals(10001, nullptr);
        std::vector<GlobalValidTable*> validity(10001, nullptr);
        GlobalLockTable heap_globals(32), index_globals(32);
        GlobalValidTable heap_validity(32), index_validity(32);
        globals[0] = &heap_globals; globals[10000] = &index_globals;
        validity[0] = &heap_validity; validity[10000] = &index_validity;
        CountedGPLM gplm(&globals, &validity);
        OwnedServer page_server(&gplm);
        std::vector<int> partitions(10001, 8);
        MetaManager meta(MetaManager::InProcessTag{}, 1, partitions);
        BufferPool heap_pool(32, 32), index_pool(32, 32);
        LRLocalPageLockTable heap_locks(32), index_locks(32);
        std::vector<BufferPool*> pools(10001, nullptr);
        std::vector<LRLocalPageLockTable*> locks(10001, nullptr);
        pools[0] = &heap_pool; pools[10000] = &index_pool;
        locks[0] = &heap_locks; locks[10000] = &index_locks;
        ComputeNode node(ComputeNode::InProcessTag{}, 1, &meta, pools, locks, storage_server.Endpoint());
        brpc::Channel channels[3];
        brpc::ChannelOptions channel_options;
        channel_options.timeout_ms = 500; channel_options.max_retry = 0;
        for (auto& channel : channels) Check(channel.Init(page_server.Endpoint().c_str(), &channel_options) == 0, "channel init failed");
        ComputeServer client(ComputeServer::InProcessTag{}, &node, channels, &gplm, &globals, &validity);
        client.table_name_meta.resize(10001);
        client.table_name_meta[0] = "fixture"; client.table_name_meta[10000] = "fixture_bl";
        obs::Recorder::Get().SetEpoch(1);
        obs::Emit("run_begin", -1, -1, 20260915, 0, 0, "real_lazy_loopback_fixture");
        int checks = 0;
        auto read = [&](int table, int page) {
            obs::RequestScope request;
            auto* value = client.rpc_lazy_fetch_s_page(table, page, false);
            uint64_t actual; std::memcpy(&actual, value->get_data() + 64, sizeof(actual));
            Check(actual == uint64_t(20260915 + page), "real fetch returned wrong bytes");
            client.rpc_lazy_release_s_page(table, page);
            request.Finish(1);
        };
        read(0, 1);
        int storage_calls = storage.calls, remote_calls = gplm.calls;
        read(0, 1);
        Check(storage.calls == storage_calls && gplm.calls == remote_calls, "local hit unexpectedly fetched remotely"); ++checks;
        read(0, 9);
        Check(gplm.calls == remote_calls, "local manager path incorrectly counted as remote RPC"); ++checks;
        read(10000, 1); ++checks;
        {
            obs::RequestScope request(2000);
            auto* page = client.rpc_lazy_fetch_x_page(0, 2, false);
            Check(page != nullptr, "exclusive fetch failed");
            auto waiting = std::async(std::launch::async, [&] { read(0, 2); });
            Check(waiting.wait_for(std::chrono::milliseconds(10)) == std::future_status::timeout, "reader passed local X lock");
            client.rpc_lazy_release_x_page(0, 2);
            waiting.get(); request.Finish(1); ++checks;
        }
        auto* ir = heap_globals.LR_GetLock(3);
        ir->SetIRLock();
        remote_calls = gplm.calls;
        auto waiting = std::async(std::launch::async, [&] { read(0, 3); });
        WaitCalls(gplm.calls, remote_calls + 6);
        Check(waiting.wait_for(std::chrono::milliseconds(1)) == std::future_status::timeout, "IR wait escaped");
        gplm.ReleaseIRLockForPage(0, 3);
        waiting.get(); ++checks;
        std::atomic<bool> cancelled{false};
        heap_globals.LR_GetLock(4)->SetIRLock();
        remote_calls = gplm.calls;
        auto cancel_wait = std::async(std::launch::async, [&] {
            obs::RequestScope request(3000, true, obs::WallUs() - 100, 0, &cancelled);
            try { client.rpc_lazy_fetch_s_page(0, 4, false); return false; }
            catch (const recovery::RequestCancelled&) { request.Finish(3); return true; }
        });
        WaitCalls(gplm.calls, remote_calls + 3); cancelled = true;
        Check(cancel_wait.get() && heap_globals.LR_GetLock(4)->IsIRLocked(), "cancel released IR"); ++checks;
        for (int fault : {1, 2, 3}) {
            storage.fault = fault;
            bool rejected = false;
            obs::RequestScope request(4000 + fault);
            try { client.rpc_fetch_page_from_storage_with_lsn(0, 5, 9, false); }
            catch (const recovery::PageUnavailable&) { rejected = true; }
            Check(rejected, "failed, short or stale storage page accepted"); request.Finish(2); ++checks;
        }
        storage.fault = 1;
        bool quarantined = false;
        try { read(0, 5); } catch (const recovery::PageUnavailable&) { quarantined = true; }
        Check(quarantined && heap_locks.GetLock(5)->IsFetchQuarantined() && !heap_pool.is_in_bufferPool(5), "failed fetch installed zero page"); ++checks;
        storage.fault = 0;
        quarantined = false;
        try { read(0, 5); } catch (const recovery::PageUnavailable&) { quarantined = true; }
        Check(quarantined, "ordinary retry cleared local quarantine"); ++checks;
        gplm.missing_replica = true;
        storage_calls = storage.calls;
        bool rejected = false;
        try { read(0, 6); } catch (const recovery::PageUnavailable&) { rejected = true; }
        Check(rejected && storage.calls == storage_calls && !heap_pool.is_in_bufferPool(6), "missing replica silently fell back to storage"); ++checks;
        gplm.missing_replica = false;
        auto* report_ir = heap_globals.LR_GetLock(7); report_ir->SetIRLock();
        page_table_service::ReportPageStatusRequest report;
        report.mutable_page_id()->set_table_id(0); report.mutable_page_id()->set_page_no(7);
        report.set_reporter_node_id(2); report.set_has_valid_copy(true); report.set_lock_mode(0);
        page_table_service::ReportPageStatusResponse report_result;
        gplm.ReportPageStatus_Localcall(&report, &report_result);
        Check(!report_result.ir_released() && report_ir->IsIRLocked(), "granting report released IR"); ++checks;
        obs::Emit("run_end", -1, -1, checks, 0, 0, "real_lazy_loopback_fixture");
        std::cout << "{\"kind\":\"REAL_LAZY_LPLM_GPLM_RPC_WITH_STORAGE_FIXTURE\",\"checks\":" << checks
                  << ",\"gplm_rpc_calls\":" << gplm.calls << ",\"storage_calls\":" << storage.calls
                  << ",\"correctness\":\"PASS\",\"cluster_crash\":false,\"safe_ready_protocol\":false}\n";
        obs::Recorder::Get().Flush();
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
    return 0;
}
