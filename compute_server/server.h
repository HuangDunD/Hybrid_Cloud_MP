#pragma once
#include "oltp/catalog.h"
#include "undo/manager.h"
#include "util/wal_batch_trace.h"
#include "mvcc/lazy_runtime.h"
#include "mvcc/remote_tso.h"
#include "mvcc/publication_rpc.h"
#include <functional>
#include "dtx/benchmark_stats.h"
#include <brpc/channel.h>
#include <butil/logging.h> 
#include <brpc/server.h>
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>
#include <gflags/gflags.h>
#include <mutex>
#include <condition_variable>
#include <map>
#include <set>
#include <random>
#include <chrono>
#include <cstdlib>
#include <pthread.h>
#include <sys/prctl.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "common.h"
#include "config.h"
#include "base/data_item.h"
#include "compute_node.h"
#include "compute_node/compute_node.pb.h"
#include "compute_node/twoPC.pb.h"
#include "compute_node/prepared_txn.h"
#include "compute_node/prepare_notification.h"
#include "compute_node/prepare_commit_config.h"
#include "compute_node/page_cost_model.h"
#include "compute_node/online_commit_rpc.h"
#include "compute_node/calvin.pb.h"
#include "fiber/thread.h"
#include "LPLM/local_page_lock.h"
#include "record/record.h"
#include "remote_page_table/remote_page_table.pb.h"
#include "remote_page_table/remote_partition_table.pb.h"
#include "storage/storage_rpc.h"
#include "storage/txn_log.h"
#include "remote_page_table/remote_page_table_rpc.h"
#include "remote_page_table/remote_partition_table_rpc.h"
#include "remote_page_table/timestamp_rpc.h"
#include "GPLM/global_page_lock.h"
#include "GPLM/global_valid_table.h"
#include "GPLM/compute_server_interface.h"

// sql
#include "sql_executor/record_printer.h"
#include "sql_executor/sql_common.h"

#include "index/bp_tree/latch_crabbing/bp_tree.h"
#include "index/bp_tree/blink/blink.h"
#include "core/fsm/fsm_tree.h"

#include "util/bitmap.h"
#include "util/json_config.h"
#include "error_library.h"
#include "dtx/lock_wait.h"

extern double ReadOperationRatio; // for workload generator
extern std::atomic<int64_t> global_wait_log_flush_time_ns;
extern std::atomic<int64_t> global_log_flush_count;
extern std::atomic<int64_t> global_log_flush_total_time_ns;
extern std::atomic<int64_t> global_log_flush_to_lock_done_time_ns;
extern std::atomic<int64_t> global_log_flush_to_max_lsn_time_ns;
extern std::atomic<int64_t> global_log_flush_to_serialize_done_time_ns;
extern std::atomic<int64_t> global_log_flush_storage_rpc_time_ns;
extern std::atomic<int64_t> global_log_flush_update_persist_lsn_time_ns;
extern std::atomic<int64_t> global_log_flush_total_batch_size;
extern std::atomic<int64_t> global_log_bytes;
extern std::atomic<int64_t> global_log_flush_max_batch_size;
extern std::atomic<int64_t> global_wait_log_flush_count;
extern std::atomic<int64_t> global_wait_log_flush_push_page_time_ns;
extern std::atomic<int64_t> global_wait_log_flush_evict_page_time_ns;

extern std::atomic<int64_t> twopc_remote_fetch_time_ns;
extern std::atomic<int64_t> twopc_remote_fetch_count;

extern std::atomic<int64_t> global_wait_commit_log_time_ns;
extern std::atomic<int64_t> global_wait_prepare_log_time_ns;
extern std::atomic<int64_t> global_wait_backup_log_time_ns;
extern std::atomic<int64_t> global_update_log_count;
extern std::atomic<int64_t> global_fetch_storage_page_time_ns;
extern std::atomic<int64_t> ownership_transfer_count;
extern std::atomic<int64_t> ownership_transfer_time_total;
extern std::atomic<int64_t> global_notify_push_page_time_ns;
extern std::atomic<int64_t> global_notify_push_page_count;

extern double ConsecutiveAccessRatio;  // for workload generator
extern double HotPageRatio;  // for workload generator
extern double HotPageRange;  // for workload generator

class ComputeServer;

namespace compute_node_service{
class ComputeNodeServiceImpl : public ComputeNodeService {
    public:
    ComputeNodeServiceImpl(ComputeServer* s): server(s) {};

    virtual ~ComputeNodeServiceImpl(){};

    virtual void Pending(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::PendingRequest* request,
                       ::compute_node_service::PendingResponse* response,
                       ::google::protobuf::Closure* done);
        
    virtual void PushPage(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::PushPageRequest* request,
                       ::compute_node_service::PushPageResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void NotifyPushPage(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NotifyPushPageRequest* request,
                       ::compute_node_service::NotifyPushPageResponse* response,
                       ::google::protobuf::Closure* done);
                       
    virtual void GetPage(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::GetPageRequest* request,
                       ::compute_node_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void NotifyCreateTable(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NotifyCreateTableRequest* request,
                       ::compute_node_service::NotifyCreateTableResponse* response,
                       ::google::protobuf::Closure* done);
                       
    virtual void NotifyDropTable(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NotifyDropTableRequest* request,
                       ::compute_node_service::NotifyDropTableResponse* response,
                       ::google::protobuf::Closure* done);
    virtual void quitDropTable(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::quitDropTableRequest* request,
                       ::compute_node_service::quitDropTableResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void ClearTable(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::ClearTableRequest* request,
                       ::compute_node_service::ClearTableResponse* response,
                       ::google::protobuf::Closure* done);
                       
    virtual void LockSuccess(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::LockSuccessRequest* request,
                       ::compute_node_service::LockSuccessResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void TransferDTX(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::TransferDTXRequest* request,
                       ::compute_node_service::TransferDTXResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void TransferHotLocate(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::TransferHotLocateRequest* request,
                       ::compute_node_service::TransferHotLocateResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void RegisterTupleWait(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::RegisterTupleWaitRequest* request,
                       ::compute_node_service::RegisterTupleWaitResponse* response,
                       ::google::protobuf::Closure* done);

    void QueryTxnStatus(::google::protobuf::RpcController*,
                        const compute_node_service::QueryTxnStatusRequest*,
                        compute_node_service::QueryTxnStatusResponse*,
                        ::google::protobuf::Closure*) override;
    void NotifyTxnFinished(::google::protobuf::RpcController*,
                           const compute_node_service::NotifyTxnFinishedRequest*,
                           compute_node_service::NotifyTxnFinishedResponse*,
                           ::google::protobuf::Closure*) override;

    // 分布式死锁检测的单跳：告知询问方「某事务当前在等谁」
    virtual void QueryWaiterHolder(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::QueryWaiterHolderRequest* request,
                       ::compute_node_service::QueryWaiterHolderResponse* response,
                       ::google::protobuf::Closure* done);

    // 锁过户：同步确认接手者还在等待 -> 接受（装队列 + 唤醒）或回 accepted=false
    virtual void HandoffQueue(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::HandoffQueueRequest* request,
                       ::compute_node_service::HandoffQueueResponse* response,
                       ::google::protobuf::Closure* done);

    // 过户落地后同步队尾等待者的「我在等谁」（死锁检测的链要跟着锁一起走）
    virtual void UpdateWaiterHolder(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::UpdateWaiterHolderRequest* request,
                       ::compute_node_service::UpdateWaiterHolderResponse* response,
                       ::google::protobuf::Closure* done);

    private:
    ComputeServer* server;
};
};

namespace twopc_service{
class TwoPCServiceImpl : public TwoPCService {
    public:
    TwoPCServiceImpl(ComputeServer* s): server(s) {
        clock_gettime(CLOCK_REALTIME, &next_commit_time);
    };

    virtual ~TwoPCServiceImpl(){};

    void AllPrepared(::google::protobuf::RpcController* controller,
                     const ::twopc_service::AllPreparedRequest* request,
                     ::twopc_service::AllPreparedResponse* response,
                     ::google::protobuf::Closure* done) override;
    virtual void GetDataItem(::google::protobuf::RpcController* controller,
                       const ::twopc_service::GetDataItemRequest* request,
                       ::twopc_service::GetDataItemResponse* response,
                       ::google::protobuf::Closure* done);
    virtual void WriteDataItem(::google::protobuf::RpcController* controller,
                        const ::twopc_service::WriteDataItemRequest* request,
                        ::twopc_service::WriteDataItemResponse* response,
                        ::google::protobuf::Closure* done);
    virtual void Prepare(::google::protobuf::RpcController* controller,
                        const ::twopc_service::PrepareRequest* request,
                        ::twopc_service::PrepareResponse* response,
                        ::google::protobuf::Closure* done);
    virtual void Commit(::google::protobuf::RpcController* controller,
                        const ::twopc_service::CommitRequest* request,
                        ::twopc_service::CommitResponse* response,
                        ::google::protobuf::Closure* done);
    virtual void Release(::google::protobuf::RpcController* controller,
                        const ::twopc_service::ReleaseRequest* request,
                        ::twopc_service::ReleaseResponse* response,
                        ::google::protobuf::Closure* done);
    virtual void Abort(::google::protobuf::RpcController* controller,
                        const ::twopc_service::AbortRequest* request,
                        ::twopc_service::AbortResponse* response,
                        ::google::protobuf::Closure* done);
    private:
    ComputeServer* server;

    struct timespec next_commit_time;
    TxnLog txn_log;
};
};

struct dtx_entry {
  dtx_entry(uint64_t seed, int type, tx_id_t tid, bool is_par):seed(seed),type(type),tid(tid),is_partitioned(is_par) {}
  uint64_t seed;
  int type;
  tx_id_t tid;
  bool is_partitioned;
};

const int DEFAULT_LOG_FLUSH_INTERVAL_MS = 3;
const size_t DEFAULT_LOG_FLUSH_BATCH_TRIGGER = 16;

// Class ComputeNode 可以建立与pagetable的连接，但不能直接与其他计算节点通信
// 因为compute_node_rpc.h引用了compute_node.h，compute_node.h引用了compute_node_rpc.h，会导致循环引用
// 所以建立一个ComputeServer类，ComputeServer类可以与其他计算节点通信
class ComputeServer : public ComputeServerInterface, public undo::PageAccess {
public:
    std::unique_ptr<mvcc::LazyRuntime> mvcc_runtime_;
    std::mutex ready_mutex_;
    std::condition_variable ready_changed_;
    bool ready_ = false;
    void WaitReady() {
        std::unique_lock<std::mutex> lock(ready_mutex_);
        ready_changed_.wait(lock, [this] { return ready_; });
    }

    Page* FetchUndoPage(const undo::PageKey&, bool exclusive, bool create = false) override;
    void ReleaseUndoPage(const undo::PageKey&, bool exclusive) override;
    void ClearUndoCache() override;
    uint64_t UndoEvictions() const { return undo_evictions_.load(); }
    std::atomic<uint64_t> undo_evictions_{0};
    std::atomic<uint64_t> main_data_evictions_{0};
    // Undo-specific page traffic, separate from main-table counters whose
    // table_id<10000 conditions intentionally remain unchanged.
    std::atomic<uint64_t> mvcc_lock_conflicts_{0};
    std::atomic<uint64_t> undo_permission_transfers_{0};
    std::atomic<uint64_t> undo_remote_fetches_{0};
    std::atomic<uint64_t> undo_wal_waits_{0};
    std::atomic<uint64_t> undo_wal_wait_ns_{0};
    std::atomic<uint64_t> undo_fetch_count_{0};
    std::atomic<uint64_t> undo_fetch_ns_{0};

    // lazy + 非 SQL 模式
    // SQL 模式有很多特殊之处，比如表结构可变，比如启动的时候不知道有哪些表
    // 所以这里区分下，SQL 模式单独来处理
    bool IsLazyWorkload() const { return SYSTEM_MODE == 1 && WORKLOAD_MODE != 4; }
    // Benchmark dispatch selects its own execution strategy. SQL shares the
    // Lazy runtime but discovers rows through its dynamic catalog and executors.
    bool IsMvccWorkload() const {
        return WORKLOAD_MODE != 4 && (SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 4);
    }
    void WaitForClusterStart(std::function<void()> reset_stats = {});
    bool UsesMvcc() const { return IsMvccWorkload() || (WORKLOAD_MODE == 4 && SYSTEM_MODE == 1); }
    mvcc::LazyRuntime& Mvcc() { CHECK(mvcc_runtime_); return *mvcc_runtime_; }
    page_cost::PageCostModel page_cost_model_;
    void PreloadBenchmarkPages();
    void ResetBenchmarkStats();
    bool CostModelActive() const {
        return SYSTEM_MODE == 4 && (WORKLOAD_MODE == 0 || WORKLOAD_MODE == 2 || oltp::Enabled(WORKLOAD_MODE))
               && page_cost_model_.config().enabled;
    }
    bool OnlineCostActive() const {
        return CostModelActive() && page_cost_model_.config().online.enabled;
    }

    virtual brpc::Channel* GetComputeChannel(int node_id) override {
        return &nodes_channel[node_id];
    }

    virtual void PushPageToOther(table_id_t table_id, page_id_t page_id, node_id_t dest_node_id, bool need_to_wait_log , bool from_global , int push_type) override;
    virtual int Pending(page_id_t page_id, bool xpending, table_id_t table_id , node_id_t dest_node_id) override;
    void NotifyLockSuccess(const compute_node_service::LockSuccessRequest& request) override;

    virtual int GetNodeID() override {
        return node_->getNodeID();
    }

    ComputeServer(ComputeNode* node, std::vector<std::string> compute_ips, std::vector<int> compute_ports): page_cost_model_(node->page_cost_stats_), node_(node){
        bool preload_pages = false;
        {
            std::string config_filepath = "../../config/compute_node_config.json";
            auto json_config = JsonConfig::load_file(config_filepath);
            auto local_compute_node = json_config.get("local_compute_node");
            preload_pages = local_compute_node.get("benchmark_preload_pages").get_bool(false);
            two_pc_variant_configured_ = TwoPCVariantConfigured(local_compute_node);
            two_pc_variant_ = ReadTwoPCVariant(local_compute_node, SYSTEM_MODE == 4);
            page_cost_model_.Configure(page_cost::Config::Read(json_config));
            auto log_flush_interval_node = json_config.get("log_flush_interval_ms");
            auto log_flush_batch_node = json_config.get("log_flush_batch_trigger");
            auto generate_log_node = local_compute_node.get("generate_log");
            if (generate_log_node.exists() && generate_log_node.is_int64()) {
                generate_log_ = (generate_log_node.get_int64() != 0);
            }
            if (log_flush_interval_node.exists() && log_flush_interval_node.is_int64()) {
                int value = static_cast<int>(log_flush_interval_node.get_int64());
                if (value > 0) {
                    log_flush_interval_ms_ = value;
                }
            }
            if (log_flush_batch_node.exists() && log_flush_batch_node.is_int64()) {
                int64_t value = log_flush_batch_node.get_int64();
                if (value > 0) {
                    log_flush_batch_trigger_ = static_cast<size_t>(value);
                }
            }
        }
        if (CostModelActive()) {
            // The selector's commit delta was calibrated against baseline 2PC
            // and has not been re-fitted for the Cornus commit paths. Keep the
            // old behaviour for an unpinned MIX default instead of breaking
            // existing cost-model configurations; an explicit Cornus variant
            // remains an error.
            if (UsesCornusCommit() && !two_pc_variant_configured_) {
                LOG(WARNING) << "hybrid_cost_model requires baseline 2PC; MIX defaults to "
                             << two_pc_variant_ << " but falls back to 2pc for this run";
                two_pc_variant_ = "2pc";
            }
            CHECK(!UsesCornusCommit())
                << "hybrid_cost_model requires the baseline 2pc variant; "
                << "set local_compute_node.two_pc_variant to 2pc";
            CHECK(generate_log_) << "hybrid_cost_model requires generate_log=1";
            CHECK(PARALLEL_PAGE_FETCH == 0) << "hybrid_cost_model currently requires parallel_page_fetch=0";
            if (OnlineCostActive()) {
                LOG(INFO) << "Online commit costs enabled: interval="
                          << page_cost_model_.config().online.update_interval_ms << "ms, min_samples="
                          << page_cost_model_.config().online.min_samples
                          << "; cold/stale samples use hot-key selector";
            } else {
                LOG(INFO) << "Hybrid page cost model enabled: fixed r=" << page_cost_model_.config().rpc_rtt_ms
                          << "ms f=" << page_cost_model_.config().log_wait_ms << "ms, delta="
                          << page_cost_model_.CommitDifferenceMs() << "ms";
            }
        }
        if (WORKLOAD_MODE != 4){
            // 如果不是 SQL 模式，那表名都是硬编码到系统里的
            InitTableNameMeta();
        }
        // 构造与其他计算节点通信的channel
        nodes_channel = new brpc::Channel[ComputeNodeCount];
        brpc::ChannelOptions options;
        SET_BRPC_RDMA_OPTION(options, use_rdma);
        options.timeout_ms = 0x7FFFFFFF;
        options.connect_timeout_ms = 1000; // 1s
        options.max_retry = 10;
        for(int i = 0; i < ComputeNodeCount; i++){
            std::string remote_node = compute_ips[i] + ":" + std::to_string(compute_ports[i]);
            if(nodes_channel[i].Init(remote_node.c_str(), &options) != 0) {
                LOG(ERROR) << "Fail to init channel";
                exit(1);
            }
        }

        if (UsesMvcc()) {
            if (IsMvccWorkload()) {
                storage_service::OpendbResponse response;
                node_->OpenMvccDatabase("", response);
            }
            const auto& image = node_->sql_recovery;
            const std::string epoch = image.epoch();
            mvcc::Recovery recovery;
            recovery.max_commit_ts = image.max_commit_ts();
            recovery.last_local = image.local_id();
            recovery.undo_pages = image.undo_pages();
            current_llsn_ = persist_lsn = image.writer_lsn();
            for (const auto& writer : image.writers()) {
                auto& ranges = recovery.committed[writer.home()];
                for (const auto& run : writer.committed())
                    ranges.push_back({run.first(), run.last(), run.first_commit_ts()});
            }
            CHECK(generate_log_) << "MVCC requires its undo and main-row redo in the WAL";
            CHECK(node_->getNodeID() >= 0 && node_->getNodeID() < ComputeNodeCount);
            std::set<mvcc::NodeId> members;
            for (int i = 0; i < ComputeNodeCount; ++i) members.insert(mvcc::NodeId(i));
            CHECK(members.size() == size_t(ComputeNodeCount));
            mvcc::RemoteTso tso;
            tso.snapshot = [this, floor = recovery.max_commit_ts] {
                benchmark_stats::ScopedTimer timer(benchmark_stats::tso_snapshot_rpc);
                return mvcc::GetSnapshotTs(node_->page_table_channel, floor);
            };
            tso.commit = [this](mvcc::Timestamp snapshot) {
                benchmark_stats::ScopedTimer timer(benchmark_stats::tso_commit_rpc);
                return mvcc::GetCommitTs(node_->page_table_channel, snapshot);
            };
            mvcc_runtime_ = std::make_unique<mvcc::LazyRuntime>(epoch, node_->getNodeID(),
                std::move(members), MakeUndoWal(), *this, nodes_channel, std::move(tso),
                node_->undo_capacity_pages_, recovery);
        }
        LR_GlobalPageLock::SetComputeServer(this);

        std::thread t([this,compute_ips,compute_ports,preload_pages] {
            // Init compute node server
            brpc::Server server;
            auto disk_manager = std::make_shared<DiskManager>();
            auto log_manager = std::make_shared<LogManager>(disk_manager.get(), nullptr, "Raft_Log" + std::to_string(node_->getNodeID()));
            compute_node_service::ComputeNodeServiceImpl compute_node_service_impl(this);
            twopc_service::TwoPCServiceImpl twoPC_service_impl(this);
            storage_service::StoragePoolImpl storage_service_impl(log_manager.get(), disk_manager.get(), nullptr, nodes_channel, 0, nullptr);
            // 和存储层的通信
            if (server.AddService(&storage_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add compute_node_service";
                return;
            }
            // 自己也给自己整一个服务，让别人可以感知到
            if (server.AddService(&compute_node_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add compute_node_service";
                return;
            }
            if (server.AddService(&twoPC_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add twoPC_service";
                return;
            }

            std::unique_ptr<mvcc::PublicationService> publication_service;
            if (UsesMvcc()) {
                publication_service=std::make_unique<mvcc::PublicationService>(Mvcc().Snapshots(), Mvcc());
                int rc=server.AddService(publication_service.get(),brpc::SERVER_DOESNT_OWN_SERVICE);CHECK(rc==0);
            }
            std::cout << "Init...\n";

            // add meta server service in each compute node
            // 初始化全局的bufferpool和page_lock_table
            // Main/BLink/FSM retain their ranges; undo has one explicit namespace.
            global_page_lock_table_list_ = new std::vector<GlobalLockTable*>(PAGE_TABLE_SLOT_COUNT , nullptr);
            global_valid_table_list_ = new std::vector<GlobalValidTable*>(PAGE_TABLE_SLOT_COUNT , nullptr);
            int table_cnt;
            if (WORKLOAD_MODE == 0){
                table_cnt = 2;
            }else if (WORKLOAD_MODE == 1){
                table_cnt = 11;
            }else if (oltp::Enabled(WORKLOAD_MODE)) {
                table_cnt = node_->getMetaManager()->GetTableNum();
            }else if (WORKLOAD_MODE == 2){
                table_cnt = 1;
            }else if (WORKLOAD_MODE == 4){
                // SQL 模式由  table_exist() 函数来进行初始化
                table_cnt = 0;
            }
            bl_indexes.resize(10000);
            // B+ 树存在 10000 到 20000 之间
            for (int i = 0 ; i < table_cnt ; i++){
                bl_indexes[i] = new BLinkIndexHandle(this , i + 10000);
            }

            fsm_trees.resize(10000);
            for (int i = 0 ; i < table_cnt ; i++){
                fsm_trees[i] = new SecFSM(this , i + 20000);
            }
            

            for (int i = 0 ; i < table_cnt ; i++){
                // 与 ComputeNode 中本地 lock table 的容量保持一致：
                //   主表 / B+ 树 / FSM 都用 max(n+10, 2048)
                // 避免 page_id 越过 capacity_。
                int n_main  = node_->meta_manager_->page_num_per_table[i];
                int n_blink = node_->meta_manager_->page_num_per_table[i + 10000];
                int n_fsm   = node_->meta_manager_->page_num_per_table[i + 20000];
                size_t cap_main  = std::max((size_t)std::max(0, n_main)  + 10, (size_t)2048);
                size_t cap_blink = std::max((size_t)std::max(0, n_blink) + 10, (size_t)2048);
                size_t cap_fsm   = std::max((size_t)std::max(0, n_fsm)   + 10, (size_t)2048);

                (*global_page_lock_table_list_)[i] = new GlobalLockTable(cap_main);
                (*global_valid_table_list_)[i] = new GlobalValidTable(cap_main);

                // BLink
                (*global_page_lock_table_list_)[i + 10000] = new GlobalLockTable(cap_blink);
                (*global_valid_table_list_)[i + 10000] = new GlobalValidTable(cap_blink);

                // FSM
                (*global_page_lock_table_list_)[i + 20000] = new GlobalLockTable(cap_fsm);
                (*global_valid_table_list_)[i + 20000] = new GlobalValidTable(cap_fsm);
            }

            if (UsesMvcc()) {
                const auto capacity = node_->undo_lock_table_initial_capacity_;
                (*global_page_lock_table_list_)[UNDO_TABLE_ID] = new GlobalLockTable(capacity);
                (*global_valid_table_list_)[UNDO_TABLE_ID] = new GlobalValidTable(capacity);
                global_page_lock_table_list_->at(UNDO_TABLE_ID)->Reset();
                global_page_lock_table_list_->at(UNDO_TABLE_ID)->BuildRPCConnection(compute_ips, compute_ports);
            }

            page_table_service_impl_ = new page_table_service::PageTableServiceImpl(global_page_lock_table_list_, global_valid_table_list_);
            if (server.AddService(page_table_service_impl_, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add page_table_service";
                return;
            }
            
            // 初始化Meta Server
            for (size_t i = 0 ; i < table_cnt ; i++){
                global_page_lock_table_list_->at(i)->Reset();
                global_valid_table_list_->at(i)->Reset();
                global_page_lock_table_list_->at(i)->BuildRPCConnection(compute_ips , compute_ports);

                // blink
                global_page_lock_table_list_->at(i + 10000)->Reset();
                global_valid_table_list_->at(i + 10000)->Reset();
                global_page_lock_table_list_->at(i + 10000)->BuildRPCConnection(compute_ips , compute_ports);

                // fsm
                global_page_lock_table_list_->at(i + 20000)->Reset();
                global_valid_table_list_->at(i + 20000)->Reset();
                global_page_lock_table_list_->at(i + 20000)->BuildRPCConnection(compute_ips , compute_ports);
            }


            butil::EndPoint point;
            point = butil::EndPoint(butil::IP_ANY, compute_ports[node_->getNodeID()]);
            brpc::ServerOptions server_options;
            server_options.num_threads = 8;
            SET_BRPC_RDMA_OPTION(server_options, use_rdma);

            // SQL 模式下，初始化一下每个已存在的 table
            for (int i = 0 ; i < node_->table_names.size() ; i++){
                bool res = table_exist(node_->table_names[i]);
                assert(res);
            }
            // Each node loads only its own partition. Other nodes cannot start
            // transactions until all nodes reach the existing startup barrier.
            if (preload_pages && WORKLOAD_MODE != 4 &&
                (SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 4))
                PreloadBenchmarkPages();
            std::cout << "Initlize Over , Server Start\n";

            if (server.Start(point,&server_options) != 0) {
                LOG(ERROR) << "Fail to start Server";
                exit(1);
            }

            
            // std::cout << "Fininsh start server\n";
            {
                std::lock_guard<std::mutex> lock(ready_mutex_);
                ready_ = true;
            }
            ready_changed_.notify_all();
            server.RunUntilAskedToQuit();
            exit(1);
        });
        t.detach();
    }

    LLSN AddUpdateLog(uint64_t tx_id , DataItem* item,
                                   itemkey_t *key,
                                   Rid rid,
                                   const void* value,
                                   RmPageHdr* pagehdr,
                                    tx_id_t commit_ts = 0, UpdateHistory history = UpdateHistory::Retain,
                                    const std::shared_ptr<LogFlushTiming>& timing = nullptr);
    LLSN AddLockLog(uint64_t tx_id, table_id_t table_id,
                    const Rid& rid, lock_t lock_type, RmPageHdr* pagehdr,
                    tx_id_t time_stamp, node_id_t txn_home_node, node_id_t lock_wait_record_node);
    LLSN AddInsertLog(uint64_t tx_id , DataItem* item,
                     itemkey_t* key,
                     const void* value,
                     const Rid& rid,
                     RmPageHdr* pagehdr);
    LLSN AddDeleteLog(uint64_t tx_id , table_id_t table_id,
            itemkey_t* key,
            int page_no,
            int slot_no,RmPageHdr* pagehdr);

    const std::string getTableNameByTableID(table_id_t table_id) ;

    
    page_id_t search_free_page(table_id_t table_id , uint32_t min_space_needed){
        return fsm_trees[table_id]->find_free_page(min_space_needed);
    }

    void update_page_space(table_id_t table_id , uint32_t page_id , uint32_t free_space){
        //assert(false);
        return fsm_trees[table_id]->update_page_space(page_id , free_space);
    }

    LLSN UpdatePageLLSN(RmPageHdr* pagehdr) {
        log_mtx.lock();
        LLSN lsn = update_page_llsn(pagehdr);
        return lsn;
    }

    Rid get_rid_from_blink(table_id_t table_id , itemkey_t key){
        Rid result;
        bool exist = bl_indexes[table_id]->search(&key , result);
        if (exist){
            return result;
        }
        return INDEX_NOT_FOUND;
    }

    RmFileHdr::ptr get_file_hdr(table_id_t table_id){
        // 元组大小信息存储在页面 0 中
        Page *page_0 = nullptr;
        std::string remote_data;
        bool is_remote = false;

        if (SYSTEM_MODE == 0){
            // eager
            page_0 = rpc_fetch_s_page(table_id , 0);
        }else if (SYSTEM_MODE == 1){
            page_0 = rpc_lazy_fetch_s_page(table_id , 0);
        }else if (SYSTEM_MODE == 2){
            // 2pc
            node_id_t node_id = get_node_id_by_page_id(table_id, 0);
            if (node_id == node_->getNodeID()) {
                page_0 = local_fetch_s_page(table_id , 0);
            } else {
                is_remote = true;
                twopc_service::GetDataItemRequest request;
                twopc_service::GetDataItemResponse response;
                twopc_service::ItemID* item_id = new twopc_service::ItemID();
                item_id->set_table_id(table_id);
                item_id->set_page_no(0);
                item_id->set_slot_id(0);
                item_id->set_lock_data(false);
                request.set_allocated_item_id(item_id);
                
                twopc_service::TwoPCService_Stub stub(&nodes_channel[node_id]);
                brpc::Controller cntl;
                stub.GetDataItem(&cntl, &request, &response, NULL);
                
                if(cntl.Failed()){
                    LOG(ERROR) << "Fail to get page 0 from remote compute node " << node_id;
                    assert(false);
                }
                remote_data = response.data();
            }
        }else if (SYSTEM_MODE == 3){
            page_0 = single_fetch_s_page(table_id , 0);
        }else if (SYSTEM_MODE == 4){
            page_0 = rpc_lazy_fetch_s_page(table_id , 0);
        }else {
            assert(false);
        }
        
        RmFileHdr* hdr;
        if (is_remote) {
            hdr = reinterpret_cast<RmFileHdr*>(const_cast<char*>(remote_data.c_str()) + sizeof(RmPageHdr));
        } else {
            hdr = reinterpret_cast<RmFileHdr*>(page_0->get_data() + sizeof(RmPageHdr));
        }
        hdr->CheckFormat();
        auto ret = std::make_shared<RmFileHdr>(*hdr);

        if (!is_remote) {
            if (SYSTEM_MODE == 0){
                rpc_release_s_page(table_id , 0);
            }else if (SYSTEM_MODE == 1){
                rpc_lazy_release_s_page(table_id , 0);
            }else if (SYSTEM_MODE == 2){
                // local_release_s_page(table_id , 0);
                if (!is_remote){
                    local_release_s_page(table_id , 0);
                }
            }else if (SYSTEM_MODE == 3){
                single_release_s_page(table_id , 0);
            }
        }
        return ret;
    }

    RmFileHdr::ptr get_file_hdr_cached(table_id_t table_id){
        std::lock_guard<bthread::Mutex> lk(file_hdr_cache_mutex_);
        if (file_hdr_cache_.find(table_id) != file_hdr_cache_.end()){
            return file_hdr_cache_[table_id];
        }
        RmFileHdr::ptr hdr = get_file_hdr(table_id);
        file_hdr_cache_[table_id] = hdr;
        return hdr;
    }

    char *FetchSPage(table_id_t table_id , page_id_t page_id , int type = -1, benchmark_stats::Stage phase = benchmark_stats::StageCount){
        Page *page = nullptr;
        assert(table_id >= 0 && table_id < 30000);
        assert(page_id >= 0);
        if(SYSTEM_MODE == 0) {
            // Eager
            page = rpc_fetch_s_page(table_id, page_id);
        } else if(SYSTEM_MODE == 1 || SYSTEM_MODE == 4){
            // Lazy
            page = rpc_lazy_fetch_s_page(table_id,page_id,phase);
        } else if(SYSTEM_MODE == 2){
            // 2PC
            page = local_fetch_s_page(table_id,page_id);
        } else if(SYSTEM_MODE == 3){
            page = single_fetch_s_page(table_id,page_id);
        } else {
            assert(false);
        }
        return page->get_data();
    }

    Page *FetchXPage(table_id_t table_id , page_id_t page_id , int type = -1, benchmark_stats::Stage phase = benchmark_stats::StageCount){
        assert(table_id >= 0 && table_id < 30000);
        assert(page_id >= 0);
        Page *page = nullptr;
        if(SYSTEM_MODE == 0) {
            page = rpc_fetch_x_page(table_id,page_id);
        } else if(SYSTEM_MODE == 1 || SYSTEM_MODE == 4){
            page = rpc_lazy_fetch_x_page(table_id,page_id,phase);
        } else if(SYSTEM_MODE == 2){
            page = local_fetch_x_page(table_id,page_id);
        } else if(SYSTEM_MODE == 3){
            // TODO
            assert(false);
            page = single_fetch_x_page(table_id,page_id);
        } else {
            assert(false);
        }
        return page;
    }

    void ReleaseSPage(table_id_t table_id , page_id_t page_id , int type = -1){
        assert(table_id >= 0 && table_id < 30000);
        if (SYSTEM_MODE == 0){
            rpc_release_s_page(table_id , page_id);
        }else if (SYSTEM_MODE == 1 || SYSTEM_MODE == 4){
            rpc_lazy_release_s_page(table_id , page_id);
        }else if (SYSTEM_MODE == 2){
            local_release_s_page(table_id , page_id);
        }else if (SYSTEM_MODE == 3){
            // TODO
            assert(false);
        }else {
            assert(false);
        }
    }
    
    void ReleaseXPage(table_id_t table_id , page_id_t page_id , int type = -1){
        if (SYSTEM_MODE == 0){
            rpc_release_x_page(table_id , page_id);
        }else if (SYSTEM_MODE == 1 || SYSTEM_MODE == 4){
            rpc_lazy_release_x_page(table_id , page_id);
        }else if (SYSTEM_MODE == 2){
            local_release_x_page(table_id , page_id);
        }else if (SYSTEM_MODE == 3){
            // TODO
            assert(false);
        }else {
            assert(false);
        }
    }

    // SQL
    // ----------------------------------------------------------------------
    std::string getTableNameFromTableIDSQL(table_id_t table_id){
        for (auto it = get_node()->db_meta.m_tabs.begin() ; it != get_node()->db_meta.m_tabs.end() ; it++){
            if (it->second.table_id == table_id){
                return it->first;
            }
        }
        return "";
    }
    // 创建一张新表
    void create_table(const std::string &tab_name , std::vector<ColDef> cols , const std::string pri_key){
        if (get_node()->db_meta.is_table(tab_name)){
            throw LJ::TableAlreadyExistsError(tab_name);
        }

        // 验证主键名字确实在表里
        if (pri_key != "") {
            bool found = false;
            for (auto &col_def : cols){
                if (col_def.name == pri_key){
                    if (col_def.type != ColType::TYPE_INT){
                        throw std::logic_error("指定主键只能是单个列，且类型需为 TYPE_INT");
                    }
                    // 将这列的类型转化为 ITEMKEY
                    col_def.type = ColType::TYPE_ITEMKEY;
                    found = true;
                    break;
                }
            }
            if (!found){
                throw std::logic_error("主键不是本表的列!");
            }
        }

        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        storage_service::CreateTableRequest request;
        storage_service::CreateTableResponse response;
        brpc::Controller cntl;

        request.set_tab_name(tab_name);
        for (int i = 0 ; i < cols.size() ; i++){
            request.add_cols_name(cols[i].name);
            request.add_cols_len(cols[i].len);
            request.add_cols_type(cols[i].type);
        }
        // 证明只有一个主键列，先不允许这样吧
        if (cols.size() == 1 && !pri_key.empty()){
            throw std::logic_error("不允许只有一个主键列");
        }

        storage_stub.CreateTable(&cntl , &request , &response , NULL);
        if (cntl.Failed()){
            assert(false);
        }

        int error_code = response.error_code();
        if (error_code == 0){
            const bool created = table_exist(tab_name);
            int table_id = response.table_id();
            CHECK(created && table_id == get_node()->db_meta.get_table(tab_name).table_id);
            std::atomic<bool> has_rpc_error(false);
            std::vector<brpc::CallId> cids;
            for (int i = 0 ; i < ComputeNodeCount ; i++){
                if (i == getNodeID()){
                    continue;
                }
                brpc::Controller* cntl = new brpc::Controller();
                compute_node_service::ComputeNodeService_Stub compute_node_stub(&nodes_channel[i]);
                compute_node_service::NotifyCreateTableRequest notify_request;
                compute_node_service::NotifyCreateTableResponse* notify_response = new compute_node_service::NotifyCreateTableResponse();
                notify_request.set_table_id(table_id);
                notify_request.set_tab_name(tab_name);
                cids.push_back(cntl->call_id());
                compute_node_stub.NotifyCreateTable(cntl , &notify_request , notify_response ,
                    brpc::NewCallback(ComputeServer::NotifyCreateTableRPCDone, notify_response, cntl, &has_rpc_error));
            }
            for (auto cid : cids){
                brpc::Join(cid);
            }
            if (has_rpc_error.load()){
                assert(false);
            }
        }else {
            std::cout << "Error Code = " << error_code << "\n";
            LJ::throw_error_by_code(error_code);
        }
    }

    void dropTable(const std::string &tab_name){
        if (!table_exist(tab_name)) throw LJ::TableNotFoundError(tab_name);
        const auto table_id = get_node()->db_meta.get_table(tab_name).table_id;
        // 通知存储层 DropTable
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        storage_service::DropTableRequest request;
        storage_service::DropTableResponse response;
        brpc::Controller cntl;
        request.set_tab_name(tab_name);
        storage_stub.DropTable(&cntl , &request , &response , NULL);
        if (cntl.Failed()){
            assert(false);
        }

        int error_code = response.error_code();
        if (error_code == 0){
            for (int j = 0 ; j < ComputeNodeCount ; j++){
                if (j == getNodeID()){
                    continue;
                }else {
                    brpc::Controller cntl;
                    compute_node_service::ComputeNodeService_Stub stub(&nodes_channel[j]);
                    compute_node_service::ClearTableRequest req;
                    compute_node_service::ClearTableResponse resp;
                    req.set_table_id(table_id);
                    stub.ClearTable(&cntl, &req, &resp, NULL);
                    CHECK(!cntl.Failed()) << cntl.ErrorText();
                }
            }

            clearTable(table_id);
        } else {
            LJ::throw_error_by_code(error_code);
        }
    }


    // 清理掉 table 的缓冲区，FSM，锁表等信息
    void clearTable(table_id_t table_id){
        {
            std::lock_guard<std::mutex> lk(tab_meta_mtx);
            std::string tab_name = getTableNameByTableID(table_id);
            assert(tab_name != "");
            get_node()->db_meta.m_tabs.erase(tab_name);

        }
        {
            std::lock_guard<bthread::Mutex> lk(file_hdr_cache_mutex_);
            file_hdr_cache_.erase(table_id);
        }
        assert(table_id < 10000);

        if (SYSTEM_MODE == 1){
            if (get_node()->lazy_local_page_lock_tables[table_id]){
                delete get_node()->lazy_local_page_lock_tables[table_id];
                get_node()->lazy_local_page_lock_tables[table_id] = nullptr;
            }
            if (get_node()->lazy_local_page_lock_tables[table_id + 10000]){
                delete get_node()->lazy_local_page_lock_tables[table_id + 10000];
                get_node()->lazy_local_page_lock_tables[table_id + 10000] = nullptr;
            }
            if (get_node()->lazy_local_page_lock_tables[table_id + 20000]){
                delete get_node()->lazy_local_page_lock_tables[table_id + 20000];
                get_node()->lazy_local_page_lock_tables[table_id + 20000] = nullptr;
            }
        }else {
            assert(false);
        }

        get_node()->page_cost_stats_.DropTable(table_id);
        get_node()->getBufferPool()->DropTable(table_id);
        get_node()->getBufferPool()->DropTable(table_id + 10000);
        get_node()->getBufferPool()->DropTable(table_id + 20000);

        if ((*global_page_lock_table_list_)[table_id]){
            delete (*global_page_lock_table_list_)[table_id];
            (*global_page_lock_table_list_)[table_id] = nullptr;
        }
        if ((*global_valid_table_list_)[table_id]){
            delete (*global_valid_table_list_)[table_id];
            (*global_valid_table_list_)[table_id] = nullptr;
        }
        if ((*global_page_lock_table_list_)[table_id + 10000]){
            delete (*global_page_lock_table_list_)[table_id + 10000];
            (*global_page_lock_table_list_)[table_id + 10000] = nullptr;
        }
        if ((*global_valid_table_list_)[table_id + 10000]){
            delete (*global_valid_table_list_)[table_id + 10000];
            (*global_valid_table_list_)[table_id + 10000] = nullptr;
        }
        if ((*global_page_lock_table_list_)[table_id + 20000]){
            delete (*global_page_lock_table_list_)[table_id + 20000];
            (*global_page_lock_table_list_)[table_id + 20000] = nullptr;
        }
        if ((*global_valid_table_list_)[table_id + 20000]){
            delete (*global_valid_table_list_)[table_id + 20000];
            (*global_valid_table_list_)[table_id + 20000] = nullptr;
        }

        if (bl_indexes.size() > static_cast<size_t>(table_id) && bl_indexes[table_id]){
            delete bl_indexes[table_id];
            bl_indexes[table_id] = nullptr;
        }
        if (fsm_trees.size() > static_cast<size_t>(table_id) && fsm_trees[table_id]){
            delete fsm_trees[table_id];
            fsm_trees[table_id] = nullptr;
        }
    }

    std::string show_tables(){
        std::vector<std::string> captions = {"Tables"};
        RecordPrinter printer(captions.size());
        Context context;
        context.m_data_send = new char[BUFFER_LENGTH];
        context.m_offset = new int(0);
        context.m_ellipsis = false;
        
        // Head
        printer.print_separator(&context);
        printer.print_record(captions, &context);
        printer.print_separator(&context);

        brpc::Controller cntl;
        storage_service::StorageService_Stub stub(get_storage_channel());
        storage_service::ShowTableRequest req;
        storage_service::ShowTableResponse resp;
        stub.ShowTable(&cntl , &req , &resp , NULL);
        if (cntl.Failed()){
            assert(false);
        }

        // Body
        for (int i = 0 ; i < resp.tab_name_size() ; i++){
            std::vector<std::string> table_info = {resp.tab_name(i)};
            printer.print_record(table_info , &context);
        }

        // Foot
        printer.print_separator(&context);
        RecordPrinter::print_record_count(resp.tab_name_size() , &context);

        std::string ret;
        if (context.m_data_send != nullptr && context.m_offset != nullptr && *context.m_offset > 0) {
            ret.assign(context.m_data_send, *context.m_offset);
        }
        delete[] context.m_data_send;
        delete context.m_offset;

        return ret;
    }

    // 判断 table 是否存在
    bool table_exist(const std::string table_name){
        // db_meta 只是一个缓存层，就算删除表信息没有及时同步到 node，去存储里面拿也照样拿不到页面
        // 后续可以设置一个通知，某个节点把表给删了，通知其它节点下
        if (node_->db_meta.is_table(table_name)){
            return true;
        }

        // 如果 db_meta 没有，那就去存储层求证下，确实没有这个表
        storage_service::StorageService_Stub storage_stub(&node_->storage_channel);
        storage_service::TableExistRequest request;
        storage_service::TableExistResponse response;
        brpc::Controller cntl;

        request.set_table_name(table_name);
        storage_stub.TableExist(&cntl , &request , &response , NULL);
        bool exist = response.ans();

        if (exist){
            TabMeta tab_meta;
            tab_meta.name = table_name;

            int cur_offset = 0;
            for (int i = 0 ; i < response.col_names_size() ; i++){
                std::string col = response.col_names(i);
                ColMeta c;
                c.tab_name = table_name;
                c.name = col;
                if (response.col_types(i) == "TYPE_INT"){
                    c.type = ColType::TYPE_INT;
                }else if (response.col_types(i) == "TYPE_FLOAT"){
                    c.type = ColType::TYPE_FLOAT;
                }else if (response.col_types(i) == "TYPE_STRING"){
                    c.type = ColType::TYPE_STRING;
                }else if (response.col_types(i) == "TYPE_ITEMKEY"){
                    c.type = ColType::TYPE_ITEMKEY;
                }else{
                    assert(false);
                }

                c.len = response.col_lens(i);
                
                // 如果不是主键，那就统计一下 offset
                if (c.type != ColType::TYPE_ITEMKEY){
                    c.offset = cur_offset;
                    cur_offset += c.len;
                }

                tab_meta.cols.emplace_back(c);
            }

            assert(response.primary_size() == 1 || response.primary_size() == 0);
            std::string pkey = "";
            if (response.primary_size() == 1){
                pkey = response.primary(0);
            }

            tab_meta.primary_key = pkey;
            tab_meta.table_id = response.table_id();

            node_->db_meta.set_table_meta(table_name , tab_meta);

            // 除此之外，还需要设置 meta_manager，缓冲池，以及锁表，有效性表的信息
            assert(tab_meta.table_id < 10000);

            if (SYSTEM_MODE == 1){
                node_->page_cost_stats_.InitTable(tab_meta.table_id, 2048);
                node_->lazy_local_page_lock_tables[tab_meta.table_id] = new LRLocalPageLockTable(
                    2048, &node_->page_cost_stats_, tab_meta.table_id);
                node_->lazy_local_page_lock_tables[tab_meta.table_id + 10000] = new LRLocalPageLockTable(2048);
                node_->lazy_local_page_lock_tables[tab_meta.table_id + 20000] = new LRLocalPageLockTable(2048);
            }else {
                assert(false);
            }

            (*global_page_lock_table_list_)[tab_meta.table_id] = new GlobalLockTable(2048);
            (*global_valid_table_list_)[tab_meta.table_id] = new GlobalValidTable(2048);

            // BLink
            if (tab_meta.primary_key != "") {
                (*global_page_lock_table_list_)[tab_meta.table_id + 10000] = new GlobalLockTable(2048);
                (*global_valid_table_list_)[tab_meta.table_id + 10000] = new GlobalValidTable(2048);
            }

            // FSM
            (*global_page_lock_table_list_)[tab_meta.table_id + 20000] = new GlobalLockTable(2048);
            (*global_valid_table_list_)[tab_meta.table_id + 20000] = new GlobalValidTable(2048);

            std::vector<std::string> compute_ips;
            std::vector<int> compute_ports;
            for(auto& node : node_->meta_manager_->remote_compute_nodes){
                compute_ips.push_back(node.ip);
                compute_ports.push_back(node.port);
            }

            {
                global_page_lock_table_list_->at(tab_meta.table_id)->Reset();
                global_valid_table_list_->at(tab_meta.table_id)->Reset();
                global_page_lock_table_list_->at(tab_meta.table_id)->BuildRPCConnection(compute_ips , compute_ports);

                // blink
                if (tab_meta.primary_key != "") {
                    global_page_lock_table_list_->at(tab_meta.table_id + 10000)->Reset();
                    global_valid_table_list_->at(tab_meta.table_id + 10000)->Reset();
                    global_page_lock_table_list_->at(tab_meta.table_id + 10000)->BuildRPCConnection(compute_ips , compute_ports);
                }

                // fsm
                global_page_lock_table_list_->at(tab_meta.table_id + 20000)->Reset();
                global_valid_table_list_->at(tab_meta.table_id + 20000)->Reset();
                global_page_lock_table_list_->at(tab_meta.table_id + 20000)->BuildRPCConnection(compute_ips , compute_ports);
            }

            if (tab_meta.primary_key != "") {
                bl_indexes[tab_meta.table_id] = new BLinkIndexHandle(this , tab_meta.table_id + 10000);
            }
            fsm_trees[tab_meta.table_id] = new SecFSM(this , tab_meta.table_id + 20000);

            // std::cout << "Init table , blink and fsm , table_id = " << tab_meta.table_id << "\n"; 
        }

        return exist;
    }

    table_id_t get_table_id(const std::string tab_name){
        if (table_exist(tab_name)){
            assert(node_->db_meta.is_table(tab_name));
            return node_->db_meta.get_table(tab_name).get_table_id();
        }
        return INVALID_TABLE_ID;
    }

    std::string desc_table(const std::string tab_name){
        bool exist = table_exist(tab_name);
        if (!exist){
            throw LJ::TableNotFoundError(tab_name);
        }

        TabMeta tab = get_node()->db_meta.get_table(tab_name);

        std::vector<std::string> captions = {"Field", "Type"};
        RecordPrinter printer(captions.size());
        Context context;
        context.m_data_send = new char[BUFFER_LENGTH];
        context.m_offset = new int(0);
        context.m_ellipsis = false;

        printer.print_separator(&context);
        printer.print_record(captions, &context);
        printer.print_separator(&context);

        for (auto &col : tab.cols) {
            std::vector<std::string> field_info = {
                col.name,
                coltype2str(col.type),
            };
            printer.print_record(field_info, &context);
        }

        // Print footer
        printer.print_separator(&context);

        std::string ret;
        // 立刻打印
        if (context.m_data_send != nullptr && context.m_offset != nullptr && *context.m_offset > 0) {
            ret.assign(context.m_data_send, *context.m_offset);
        }
        delete[] context.m_data_send;
        delete context.m_offset;

        return ret;
    }
    // SQL END
    // --------------------------------------------------

    // blink
    void insert_into_blink(table_id_t table_id , itemkey_t key , Rid value){
        bl_indexes[table_id]->insert_entry(&key , value);
    }
    Rid delete_from_blink(table_id_t table_id , itemkey_t key){
        return bl_indexes[table_id]->delete_entry(&key);
    }

    ~ComputeServer() { pending_releases_.Drain(); }
    static void InvalidRPCDone(partition_table_service::InvalidResponse* response, brpc::Controller* cntl);

    static void LazyReleaseRPCDone(page_table_service::PAnyUnLockResponse* response, brpc::Controller* cntl);

    void PSUnlockRPCDone(page_table_service::PSUnlockResponse* response, brpc::Controller* cntl, page_id_t page_id);

    void PXUnlockRPCDone(page_table_service::PXUnlockResponse* response, brpc::Controller* cntl, page_id_t page_id);

    static void PSlockRPCDone(page_table_service::PSLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish);

    static void PXlockRPCDone(page_table_service::PXLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish);

    static void PushPageRPCDone(compute_node_service::PushPageResponse* response, brpc::Controller* cntl);
    // 新增：携带页元数据的回调，便于归还 pending 计数
    static void PushPageRPCDone(compute_node_service::PushPageResponse* response,
                                brpc::Controller* cntl,
                                int table_id,
                                int page_id,
                                ComputeServer* server);

    static void NotifyCreateTableRPCDone(compute_node_service::NotifyCreateTableResponse* response,
                                         brpc::Controller* cntl,
                                         std::atomic<bool>* has_error);

    static void NotifyDropTableRPCDone(compute_node_service::NotifyDropTableResponse* response,
                                       brpc::Controller* cntl,
                                       std::atomic<bool>* has_error);

    // ****************** for eager release *********************
    Page* rpc_fetch_s_page(table_id_t table_id, page_id_t page_id);

    Page* rpc_fetch_x_page(table_id_t table_id, page_id_t page_id);

    void rpc_release_s_page(table_id_t table_id, page_id_t page_id);
    
    void rpc_release_x_page(table_id_t table_id, page_id_t page_id);

    // ****************** eager release end *********************

    // ****************** for lazy release *********************
    Page* rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id, benchmark_stats::Stage phase = benchmark_stats::StageCount);
    Page* rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id, benchmark_stats::Stage phase = benchmark_stats::StageCount, bool create_undo = false);
    void rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id);
    void rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id);
    // ****************** lazy release end ********************

    void rpc_flush_page_to_storage(table_id_t table_id , page_id_t page_id){
        CHECK(table_id != UNDO_TABLE_ID) << "undo pages are persisted only by WAL replay";
        Page *old_page = node_->fetch_page(table_id , page_id);
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl_wp;
        storage_service::WritePageRequest req;
        storage_service::WritePageResponse resp;
        auto* pid = req.mutable_page_id();
        pid->set_table_name(getTableNameByTableID(table_id));
        pid->set_page_no(page_id);
        req.set_data(old_page->get_data(), PAGE_SIZE);
        storage_stub.WritePage(&cntl_wp, &req, &resp, NULL);
        if (cntl_wp.Failed()) {
            LOG(ERROR) << "WritePage RPC failed for table_id=" << table_id
                        << " page_id=" << page_id
                        << " err=" << cntl_wp.ErrorText();
            assert(false);
        }
    }

    // TryBeginEvict has reserved the local permission. The existing GPLM
    // eviction handshake decides whether the frame may actually be discarded.
    bool ReleaseLazyPageForEviction(table_id_t table_id, page_id_t page_id) {
        auto* local = node_->getLazyPageLockTable(table_id)->GetLock(page_id);
        const int mode = local->getUnlockType();  // holds the local mutex
        if (mode == 0) {
            local->UnlockMtx();
            return false;
        }
        auto* page = node_->getBufferPool(table_id)->fetch_page({table_id, page_id});
        if (PageUsesWal(table_id)) {
            const auto start = std::chrono::high_resolution_clock::now();
            wait_log_flush(page);
            global_wait_log_flush_evict_page_time_ns +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::now() - start).count();
        } else {
            rpc_flush_page_to_storage(table_id, page_id);
        }
        page_table_service::BufferReleaseUnlockRequest request;
        request.mutable_page_id()->set_table_id(table_id);
        request.mutable_page_id()->set_page_no(page_id);
        request.set_node_id(node_->node_id);
        request.set_lsn(PageUsesWal(table_id) ? PageLsn(page) : 0);
        local->UnlockMtx();

        page_table_service::BufferReleaseUnlockResponse response;
        const auto owner = get_node_id_by_page_id(table_id, page_id);
        if (owner == node_->node_id) {
            page_table_service_impl_->BufferReleaseUnlock_LocalCall(&request, &response);
        } else {
            brpc::Controller controller;
            page_table_service::PageTableService_Stub stub(nodes_channel + owner);
            stub.BufferReleaseUnlock(&controller, &request, &response, nullptr);
            CHECK(!controller.Failed()) << controller.ErrorText();
        }
        return response.agree();
    }

    void CancelLazyEviction(table_id_t table_id, page_id_t page_id) {
        auto* local = node_->getLazyPageLockTable(table_id)->GetLock(page_id);
        const int mode = local->getUnlockType();
        // A rejected eviction must return a still-owned idle frame to LRU.
        // Pending may already have discarded it and released the permission.
        if (mode != 0) node_->getBufferPool(table_id)->unpin_page({table_id, page_id});
        local->UnlockMtx();
        local->EndEvict();
    }

    void FinishLazyEviction(table_id_t table_id, page_id_t page_id) {
        auto* local = node_->getLazyPageLockTable(table_id)->GetLock(page_id);
        const int mode = local->UnlockAny();
        if (mode) local->UnlockRemoteOK();
        local->EndEvict();
        node_->evict_page_cnt++;
        if (table_id == UNDO_TABLE_ID) ++undo_evictions_;
        if (table_id < 10000 && page_id > 0) ++main_data_evictions_;
    }

    // Shared frames may evict a different table or page type. The victim,
    // not the incoming request, determines which permission protocol to use.
    int BufferPageProtocol(PageId id) const {
        if (SYSTEM_MODE == 0) return 0;
        if (SYSTEM_MODE == 2 && id.table_id < 10000) return 2;
        return 1;
    }

    Page* put_page_into_buffer(table_id_t table_id, page_id_t page_id, const void* data, int type) {
        auto* pool = node_->getBufferPool(table_id);
        const PageId incoming{table_id, page_id};
        if (type == 0 && pool->checkIfDirectlyUpdate(incoming, data))
            return pool->fetch_page(incoming);
        frame_id_t frame = INVALID_FRAME_ID;
        if (pool->checkIfDirectlyPutInBuffer(incoming, frame))
            return pool->insert_or_replace(incoming, frame, false, {}, data);

        auto begin_evict = [this](PageId victim) {
            const int protocol = BufferPageProtocol(victim);
            if (protocol == 0)
                return node_->eager_local_page_lock_tables[victim.table_id]->GetLock(victim.page_no)->tryBeginEvict();
            if (protocol == 2)
                return node_->local_page_lock_tables[victim.table_id]->GetLock(victim.page_no)->TryBeginEvict();
            return node_->getLazyPageLockTable(victim.table_id)->GetLock(victim.page_no)->TryBeginEvict();
        };
        while (true) {
            auto [victim, current] = pool->replace_page(incoming, frame, begin_evict);
            if (victim.page_no == INVALID_PAGE_ID)
                return pool->insert_or_replace(incoming, frame, false, {}, data);
            const int protocol = BufferPageProtocol(victim);
            auto end_evict = [&] {
                if (protocol == 0)
                    node_->eager_local_page_lock_tables[victim.table_id]->GetLock(victim.page_no)->EndEvict();
                else if (protocol == 2)
                    node_->local_page_lock_tables[victim.table_id]->GetLock(victim.page_no)->EndEvict();
                else node_->getLazyPageLockTable(victim.table_id)->GetLock(victim.page_no)->EndEvict();
            };
            if (!(victim == current)) {
                // replace_page only pins a frame if the tag still matches.
                end_evict();
                continue;
            }
            if (protocol == 0) {
                rpc_flush_page_to_storage(victim.table_id, victim.page_no);
            } else if (protocol == 1 && !ReleaseLazyPageForEviction(victim.table_id, victim.page_no)) {
                CancelLazyEviction(victim.table_id, victim.page_no);
                continue;
            }
            Page* page = pool->insert_or_replace(incoming, frame, true, victim, data);
            if (protocol == 1) {
                FinishLazyEviction(victim.table_id, victim.page_no);
            } else {
                ++node_->evict_page_cnt;
                if (victim.table_id < 10000 && victim.page_no > 0) ++main_data_evictions_;
                end_evict();
            }
            return page;
        }
    }

    page_id_t rpc_create_page(table_id_t table_id){
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl;
        storage_service::CreatePageRequest req;
        storage_service::CreatePageResponse resp;

        req.set_table_name(getTableNameByTableID(table_id));        
        req.set_table_id(table_id);
        
        storage_stub.CreatePage(&cntl , &req , &resp , NULL);
        if (cntl.Failed()) {
            LOG(ERROR) << "Create Page Error";
            assert(false);
        }

        page_id_t new_page = resp.page_no();

        if (table_id < 10000){
            // 创建了页面之后，需要通知其它节点页面数量变多了，方法是向 page0 写入一个信息
            Page *x_page = FetchXPage(table_id , 0);
            x_page->set_dirty(true);
            RmFileHdr *file_hdr = reinterpret_cast<RmFileHdr*>(x_page->get_data() + sizeof(RmPageHdr));
            // 有可能我和别人一起创建了新页面，我创建了 5，别人创建了 6，然后别人更新了 6，那我就不用管了
            if (file_hdr->num_pages_ < new_page + 1){
                file_hdr->num_pages_ = new_page + 1;
            }
            ReleaseXPage(table_id , 0);
        }

        assert(resp.success());
        return resp.page_no();
    }

    void rpc_delete_node(table_id_t table_id , page_id_t page_id){
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl;
        storage_service::DeletePageRequest req;
        storage_service::DeletePageResponse resp;

        req.set_page_no(page_id);
        req.set_table_id(table_id);
        req.set_table_name(getTableNameByTableID(table_id));

        storage_stub.DeletePage(&cntl , &req , &resp , NULL);
        if (cntl.Failed()) {
            LOG(ERROR) << "Create Page Error";
            assert(false);
        }

        assert(resp.successs());
    }

    void rpc_lazy_release_all_page();

    void rpc_lazy_release_all_page_async();
    void rpc_lazy_release_all_page_async_new();
    // ****************** lazy release end *********************

    // ****************** for 2PC *********************

    Page* local_fetch_s_page(table_id_t table_id, page_id_t page_id);

    Page* local_fetch_x_page(table_id_t table_id, page_id_t page_id);

    void local_release_s_page(table_id_t table_id, page_id_t page_id);

    void local_release_x_page(table_id_t table_id, page_id_t page_id);

    void Get_2pc_Local_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data , itemkey_t key , tx_id_t tx_id, const mvcc::Context* context, tx_id_t start_ts = 0, uint64_t* holder_ts = nullptr, node_id_t* txn_home_node = nullptr, bool* not_found = nullptr, bool* mvcc_conflict = nullptr);
    void Get_2pc_Remote_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data , tx_id_t tx_id, const mvcc::Context* context, tx_id_t start_ts = 0, uint64_t* holder_ts = nullptr, node_id_t* txn_home_node = nullptr, bool* not_found = nullptr, bool* mvcc_conflict = nullptr, bool* undo_full = nullptr);

    // Participant-side MVCC row access shared by the RPC handlers and the
    // home-local fetch shortcut. Reads return the version visible at the fixed
    // Context; writes return the locked current row for the home SI check.
    undo::RowImage Read2pcVisibleRow(table_id_t table, const Rid& rid, const mvcc::Snapshot& snapshot);
    char* Fetch2pcReadRow(table_id_t table_id, const Rid& rid,
                          const mvcc::Context& context, bool* not_found);
    char* Fetch2pcWriteRow(table_id_t table_id, const Rid& rid,
                           const mvcc::Context& context, tx_id_t tx_id, tx_id_t start_ts,
                           node_id_t requester, uint64_t* holder_ts, node_id_t* holder_home,
                           bool* conflict, itemkey_t* out_key = nullptr, bool* full = nullptr);
    // RPC handlers split out of twopc_server.cc so both the coordinator-local
    // shortcut and the remote wire path run the identical participant code.
    void HandleMvccGetDataItem(const twopc_service::GetDataItemRequest* request,
                               twopc_service::GetDataItemResponse* response,
                               table_id_t table_id, page_id_t page_id, int slot_id, bool lock);
    void HandleMvccCommit(const twopc_service::CommitRequest* request,
                          twopc_service::CommitResponse* response, bool prepared);
    void HandleMvccRelease(const twopc_service::ReleaseRequest* request,
                           twopc_service::ReleaseResponse* response);
    // Participant-side install of one prepared/single write; keeps the row lock.
    LLSN Install2pcMvccWrite(const twopc_service::PreparedWrite& write, const mvcc::Context& context,
                             tx_id_t start_ts, node_id_t coordinator, node_id_t participant,
                             const std::string& value, bool deleted, const undo::Ref& undo, const undo::RowImage& before,
                             const std::shared_ptr<LogFlushTiming>& timing = nullptr);

    // Home-side evidence collection and post-publication drain for P5.
    void CommitRemoteSingleMvcc_2pc(node_id_t writer, const twopc_service::CommitRequest& request);
    // The install owner must publish Applied only after a WAL barrier covers the returned LSN.
    LLSN InstallSingleMvccWrites(const twopc_service::CommitRequest& request, const mvcc::Context& context,
                                 std::shared_ptr<prepared_2pc::Single>& pending,
                                 const std::shared_ptr<LogFlushTiming>& timing = nullptr);
    void ReleaseMvcc_2pc(const mvcc::Context& context, uint64_t start_ts, uint64_t commit_ts,
                         const std::map<node_id_t, std::vector<twopc_service::ItemID>>& locked,
                         const std::set<node_id_t>& nodes, const std::set<mvcc::NodeId>& writers,
                         bool deferred_unlock, std::function<void()> finished,
                         const prepared_2pc::Requests* prepared = nullptr);
    void AbortMvcc_2pc(const mvcc::Context& context, uint64_t start_ts,
                       const std::map<node_id_t, std::vector<std::pair<table_id_t, Rid>>>& items,
                       const std::set<node_id_t>& exported);
    // Test seam: runs after a participant installed its writes and before the
    // RPC returns. Production never installs a gate.
    void SetMvccGate(std::function<void(const mvcc::TxnId&, mvcc::NodeId, const char*)>);
    void RunMvccGate(const mvcc::TxnId&, mvcc::NodeId, const char*);

    bool Prepare_2pc(const prepared_2pc::Requests& requests,
                     std::map<node_id_t, LLSN>* prepare_lsn = nullptr,
                     benchmark_stats::PrepareSample* sample = nullptr);
    bool PrepareLegacy_2pc(const std::unordered_set<node_id_t>& nodes, uint64_t txn_id);
    void CommitPrepared_2pc(const prepared_2pc::Requests& requests,
                            std::map<node_id_t, LLSN>* durable);
    static const std::string& ProcessIncarnation();
    const std::string coordinator_incarnation_ = ProcessIncarnation();
    prepared_2pc::Registry prepared_txns_;
    prepared_2pc::SingleWrites single_writes_;
    prepared_2pc::PrepareNotifications prepare_notifications_;
    std::atomic<bool> mvcc_gate_enabled_{false};
    std::mutex mvcc_gate_mutex_;
    std::function<void(const mvcc::TxnId&, mvcc::NodeId, const char*)> mvcc_gate_;
    prepared_2pc::PendingRpcs pending_releases_;
    prepared_2pc::Releases mvcc_releases_;
    size_t MvccFinishedCount() const { return mvcc_releases_.Finished(); }

    int Commit_2pc(std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map, uint64_t txn_id, uint64_t commit_ts, bool sync = true);

    void Abort_2pc(std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map, uint64_t txn_id, bool sync = true);


    static void AbortRPCDone(twopc_service::AbortResponse* response, brpc::Controller* cntl);

    static void CommitRPCDone(twopc_service::CommitResponse* response, brpc::Controller* cntl,
                              int* add_latency, ComputeServer* server, int peer);

    // ****************** 2PC end *********************

    // ****************** for single *********************
    Page* single_fetch_s_page(table_id_t table_id, page_id_t page_id);
    Page* single_fetch_x_page(table_id_t table_id, page_id_t page_id);
    void single_release_s_page(table_id_t table_id, page_id_t page_id);
    void single_release_x_page(table_id_t table_id, page_id_t page_id);
    // ****************** for single end *********************



    std::vector<std::string> table_name_meta;
    void InitTableNameMeta();
    std::string rpc_fetch_page_from_storage(table_id_t table_id, page_id_t page_id);
    std::string rpc_fetch_page_from_storage_with_lsn(table_id_t table_id , page_id_t page_id , LLSN page_lsn);

    inline uint64_t get_partitioned_size(table_id_t table_id){
        return node_->meta_manager_->GetPartitionSizePerTable(table_id);
    }
    
    inline bool is_partitioned_page(table_id_t table_id , page_id_t page_id , node_id_t node_id){
        // Page 0 is the file-header page, not part of any data partition.
        if (page_id == 0) return node_id == 0;
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        CHECK(partition_size > 0) << "data table has no partition size";
        int belong_par = ((page_id - 1) / partition_size) % ComputeNodeCount;
        return (node_id == belong_par);
    }

    // 获取到 page_id 所在的分区对应的节点
    inline node_id_t get_node_id_by_page_id(table_id_t table_id , page_id_t page_id){
        if (table_id == UNDO_TABLE_ID) {
            const auto key = undo::PageIdentity(Mvcc().Epoch(), page_id);
            CHECK(key.writer < uint32_t(ComputeNodeCount));
            return key.writer;
        }
        // Page 0 is the file-header page; a negative page index must not become
        // a negative node id (small TPC-C tables have partition_size == 1).
        if (page_id == 0) return 0;
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        CHECK(partition_size > 0) << "data table has no partition size";
        int node_id = ((page_id - 1) / partition_size) % ComputeNodeCount;
        CHECK(node_id >= 0 && node_id < ComputeNodeCount);
        return node_id;
    }

    /*
        页面转移走，或者页面被淘汰之前，需要等待这个页面的日志落盘
    */
    LLSN PageLsn(Page* page) {
        if (page->get_page_id().table_id == UNDO_TABLE_ID) {
            undo::PageBytes bytes;
            memcpy(bytes.data(), page->get_data(), PAGE_SIZE);
            const auto header = undo::Inspect(bytes);
            CHECK(header.key == undo::PageIdentity(Mvcc().Epoch(), page->get_page_id().page_no));
            if (page->is_dirty()) CHECK(header.key.writer == uint32_t(node_->getNodeID()));
            return header.lsn;
        }
        return reinterpret_cast<RmPageHdr*>(page->get_data())->LLSN_;
    }

    void wait_log_flush(Page *page){
        if (!generate_log_) {
            if (page->is_dirty()) {
                page->set_dirty(false);
            }
            return;
        }
        if (!page->is_dirty()){
            return ;
        }

        
        global_wait_log_flush_count++;
        auto start = std::chrono::high_resolution_clock::now();

        const LLSN required_lsn = PageLsn(page);
        const bool undo_page = page->get_page_id().table_id == UNDO_TABLE_ID;
        if (undo_page) ++undo_wal_waits_;
        RequestLogFlush(required_lsn);
        {
            std::unique_lock<bthread::Mutex> lock(persist_lsn_mtx);
            while(required_lsn > persist_lsn){
                persist_lsn_cond.wait(lock);
            }
        }
        // LOG(INFO) << "Page Wait Log Flush Over , table_id = " << page->get_page_id().table_id << " page_id = " << page->get_page_id().page_no << " wait lsn = " << hdr->LLSN_;
        page->set_dirty(false);

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        if (undo_page) undo_wal_wait_ns_ += duration;
        global_wait_log_flush_time_ns += duration;
        global_wait_log_flush_push_page_time_ns += duration;
        benchmark_stats::log_waits[benchmark_stats::PageLog].Add(duration);
    }

    // Attribute the wait to the batch containing its target record, even if already durable.
    uint64_t wait_log_flush(LLSN require_lsn, int log_type, const std::shared_ptr<LogFlushTiming>& timing,
                            bool already_requested = false) {
        if (!generate_log_) {
            return 0;
        }
        // log_type: CommitLog, PrepareLog, DecisionLog or AbortLog
        global_wait_log_flush_count++;
        auto start = std::chrono::high_resolution_clock::now();
        if (!already_requested) RequestLogFlush(require_lsn);
        {
            std::unique_lock<bthread::Mutex> lock(persist_lsn_mtx);
            while(require_lsn > persist_lsn){
                persist_lsn_cond.wait(lock);
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        global_wait_log_flush_time_ns += duration;
        
        benchmark_stats::log_waits[log_type].Add(duration);
        if (log_type == 0) {
            global_wait_commit_log_time_ns += duration;
        } else if (log_type == 1) {
            global_wait_prepare_log_time_ns += duration;
        } else if (log_type == 2) {
            global_wait_backup_log_time_ns += duration;
        }

        const auto returned_ns = benchmark_stats::NowNs();
        benchmark_stats::log_details[log_type].Add(
            timing->rpc_start_ns - timing->enqueued_ns, timing->rpc_end_ns - timing->rpc_start_ns,
            returned_ns - timing->rpc_end_ns, timing->storage_ns, timing->write_ns, timing->sync_ns);
        return returned_ns - timing->enqueued_ns;
    }

    // 生成下一个 LLSN（原子操作）
    LLSN generate_next_llsn(){
        return ++current_llsn_;
    }

    // 推进 LLSN 至已知的最大值（用于页面读取后的同步）
    void advance_llsn(LLSN new_llsn){
        if (new_llsn > current_llsn_) {
            current_llsn_ = new_llsn;
        }
    }

    // 封装提交时的 LLSN 推进流程
    LLSN update_page_llsn(RmPageHdr* page_hdr){
        LLSN old_page_llsn = page_hdr->LLSN_;
        LLSN new_llsn = generate_next_llsn();
        if (new_llsn <= old_page_llsn) {
            new_llsn = old_page_llsn + 1;
            advance_llsn(new_llsn);
        }
        page_hdr->pre_LLSN_ = old_page_llsn;
        page_hdr->LLSN_ = new_llsn;
        
        return new_llsn;
    }

    // 生成一个随机的数据页ID
    Page_request_info GernerateRandomPageID(std::mt19937& gen, std::uniform_real_distribution<>& dis);
    page_id_t last_generated_page_id = 0;

    // 从远程取数据页
    std::string UpdatePageFromRemoteCompute(table_id_t table_id, page_id_t page_id, node_id_t node_id );

    // 获取与其他计算节点通信的channel
    inline brpc::Channel* get_pagetable_channel(){ return &node_->page_table_channel; }
    inline brpc::Channel* get_storage_channel(){ return &node_->storage_channel; }
    inline brpc::Channel* get_compute_channel(){ return nodes_channel; }

    inline ComputeNode* get_node(){ return node_; }

    // Minimal home-node TIT and deferred record cleanup for Lazy / Mix writers.
    TxnStatus QueryTxnStatus(node_id_t home, uint64_t start_ts);
    // Returns with page X held, including on failure. May release/re-fetch only
    // when a foreign record lock requires a home-node status query.
    bool ResolveFinishedWriter(table_id_t table, const Rid& rid, tx_id_t tx_id,
                               uint64_t start_ts, node_id_t home, Page*& page);
    void TryCleanCommittedRecord(table_id_t table, const Rid& rid, itemkey_t key,
                                 tx_id_t tx_id, uint64_t start_ts, uint64_t commit_ts,
                                 std::optional<node_id_t> participant_txn_home = std::nullopt);

    // 元组锁等待管理器（WAIT_DETECT 模式）：事务注册/等待/唤醒的节点级入口
    LockWaitManager* lock_wait_mgr(){ return &lock_wait_mgr_; }

    // Legacy participant requests do not always carry start_ts. Read the owner
    // under page X first. Empty queues need no RPC/refetch; contested queues
    // release the untouched page before reserving and refetch before writing.
    template <typename Fetch, typename Release>
    Page* FetchXPageForUnlock(table_id_t table_id, const Rid& rid,
                              PendingHandoff* handoff, Fetch fetch, Release release) {
      const TupleWaitKey key{table_id, rid.page_no_, rid.slot_no_};
      const auto hdr = get_file_hdr_cached(table_id);
      auto tuple_at = [&](Page* page) -> DataItem* {
        char* slots = page->get_data() + sizeof(RmPageHdr) + OFFSET_PAGE_HDR + hdr->bitmap_size_;
        return reinterpret_cast<DataItem*>(slots + rid.slot_no_ *
                   (hdr->record_size_ + sizeof(itemkey_t)) + sizeof(itemkey_t));
      };
      return lock_wait_mgr_.FetchForUnlock(
          key, (node_id_t)node_->getNodeID(), nodes_channel, handoff, fetch, release,
          [&](Page* page) -> uint64_t {
            auto* item = tuple_at(page);
            CHECK(item->lock == EXCLUSIVE_LOCKED);
            return item->timeStamp;
          });
    }

    // In-page phase: pure memory update. All network work is outside page X.
    ALWAYS_INLINE
    bool UnlockTupleWithHandoff2PC(table_id_t table_id, const Rid& rid, DataItem* item,
                                  uint64_t holder_ts, PendingHandoff* handoff) {
      const TupleWaitKey key{table_id, rid.page_no_, rid.slot_no_};
      CHECK(handoff->prepared && handoff->key == key && handoff->source_holder_ts == holder_ts);
      CHECK(item->lock == EXCLUSIVE_LOCKED && item->timeStamp == holder_ts);
      if (handoff->new_holder_ts == 0) return false;
      item->timeStamp = handoff->new_holder_ts;
      item->txn_home_node = (uint8_t)handoff->new_txn_home_node;
      item->lock_wait_record_node = (uint8_t)handoff->new_txn_home_node;
      return true;
    }

    ALWAYS_INLINE
    void DeliverHandoff2PC(const PendingHandoff& ph) {
      lock_wait_mgr_.DeliverHandoff(ph, (node_id_t)node_->getNodeID(), nodes_channel);
    }

    std::mutex update_m;
    double tx_update_time = 0;

    node_id_t getNodeID() const {
        return node_->getNodeID();
    }

    int get_alive_fiber_cnt(){
        return alive_fiber_cnt.load();
    }
    void set_alive_fiber_cnt(int value){
        alive_fiber_cnt.store(value);
    }
    void decrease_alive_fiber_cnt(int desc_cnt){
        alive_fiber_cnt.fetch_sub(desc_cnt);
    }

    void EnterSqlTransaction() {
        std::lock_guard<std::mutex> guard(tab_meta_mtx);
        if (sql_ddl_active_) throw std::runtime_error("DDL in progress");
        ++sql_active_transactions_;
    }
    void LeaveSqlTransaction() {
        std::lock_guard<std::mutex> guard(tab_meta_mtx);
        CHECK(sql_active_transactions_ > 0);
        --sql_active_transactions_;
    }
    bool TrySqlDdl() {
        std::lock_guard<std::mutex> guard(tab_meta_mtx);
        if (sql_active_transactions_ || sql_ddl_active_) return false;
        sql_ddl_active_ = true;
        LogFlush(); // include deferred row cleanup in the pre-DDL WAL prefix
        return true;
    }
    void ReleaseSqlDdl() {
        std::lock_guard<std::mutex> guard(tab_meta_mtx);
        CHECK(sql_ddl_active_);
        sql_ddl_active_ = false;
    }
    // Use the existing schema-change RPCs in member order. A busy member
    // rejects DDL immediately, including when its reader has only done BEGIN.
    void WithSqlDdl(const std::string& name, const std::function<void()>& operation) {
        int held = 0;
        auto release = [&] {
            while (held > 0) {
                if (--held == getNodeID()) { ReleaseSqlDdl(); continue; }
                brpc::Controller cntl;
                compute_node_service::ComputeNodeService_Stub stub(&nodes_channel[held]);
                compute_node_service::quitDropTableRequest req;
                compute_node_service::quitDropTableResponse resp;
                req.set_tab_name(name);
                stub.quitDropTable(&cntl, &req, &resp, nullptr);
                CHECK(!cntl.Failed()) << cntl.ErrorText();
            }
        };
        try {
            for (; held < ComputeNodeCount; ++held) {
                bool ok;
                if (held == getNodeID()) ok = TrySqlDdl();
                else {
                    brpc::Controller cntl;
                    compute_node_service::ComputeNodeService_Stub stub(&nodes_channel[held]);
                    compute_node_service::NotifyDropTableRequest req;
                    compute_node_service::NotifyDropTableResponse resp;
                    req.set_tab_name(name);
                    stub.NotifyDropTable(&cntl, &req, &resp, nullptr);
                    CHECK(!cntl.Failed()) << cntl.ErrorText();
                    ok = resp.ok();
                }
                if (!ok) throw std::logic_error("DDL busy: active SQL transaction or DDL");
            }
            operation();
        } catch (...) {
            release();
            throw;
        }
        release();
    }

    struct PageTransferLogWait {
        LLSN lsn;
        uint64_t start_ns;
        std::atomic<uint64_t> elapsed_ns{0};
        PageTransferLogWait(LLSN target, uint64_t start) : lsn(target), start_ns(start) {}
    };

    // Only live page sends whose LSN is not durable are tracked. Complete their
    // timers at watermark publication, not when a waiter/scheduler runs again.
    std::shared_ptr<PageTransferLogWait> TrackPageTransferLogWait(Page* page) {
        if (!IsLogEnabled() || !page->is_dirty()) return nullptr;
        const auto lsn = PageLsn(page);
        std::lock_guard<bthread::Mutex> lock(persist_lsn_mtx);
        if (lsn <= persist_lsn) return nullptr;
        auto wait = std::make_shared<PageTransferLogWait>(lsn, benchmark_stats::NowNs());
        page_transfer_log_waits_.push_back(wait);
        return wait;
    }

    /**
     * @brief 批量刷新日志到存储层
     */
    void LogFlush(){
        // Serialize complete flushes: a drain must also wait for a batch already
        // removed from log_records, and persisted LSN must not overtake that batch.
        std::lock_guard<bthread::Mutex> flush_guard(log_flush_mtx_);
        if (!generate_log_) {
            return;
        }
        auto start = std::chrono::high_resolution_clock::now();
        // 批量取出所有日志（在锁作用域内）
        std::vector<LogRecord*> batch_logs;
        uint64_t first_enqueued = 0;
        {
            std::lock_guard<bthread::Mutex> lk(log_mtx);
            
            // 快速检查：如果没有日志，直接返回
            if (log_records.empty()) {
                return;
            }
            
            // 使用 swap 快速转移所有权，减少锁持有时间
            batch_logs.swap(log_records);
            first_enqueued = first_batch_enqueue_ns_;
        }  // 锁在这里自动释放
        auto lock_done = std::chrono::high_resolution_clock::now();
        global_log_flush_to_lock_done_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(lock_done - start).count();
        
        global_log_flush_count++;
        global_log_flush_total_batch_size += batch_logs.size();
        if (static_cast<int64_t>(batch_logs.size()) > global_log_flush_max_batch_size) {
            global_log_flush_max_batch_size = batch_logs.size();
        }

        // 1. 将 batch_logs 序列化成字符串
        size_t total_size = 0;
        LLSN max_lsn = 0;  // 记录本批次中最大的 LSN
        
        for (auto* log : batch_logs) {
            total_size += log->log_tot_len_;

            if (log->lsn_ > max_lsn) {
                max_lsn = log->lsn_;
            }
        }
        auto max_lsn_done = std::chrono::high_resolution_clock::now();
        global_log_flush_to_max_lsn_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(max_lsn_done - lock_done).count();

        std::string serialized_logs;
        std::stringstream ss;
        if (total_size > 0) {
            serialized_logs.resize(total_size);
            // C++11 保证 string 内存连续，可以直接写入
            char* dest_ptr = &serialized_logs[0];
            
            // 第二遍遍历：直接序列化
            for (auto* log : batch_logs) {
                ss << "\nlog lsn = " << log->lsn_ << " log prev lsn = " << log->prev_lsn_ << "\n";
                log->serialize(dest_ptr);
                dest_ptr += log->log_tot_len_;
            }
        }
        // LOG(INFO) << "Log Flush : " << ss.str();

        auto serialize_done = std::chrono::high_resolution_clock::now();
        global_log_flush_to_serialize_done_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(serialize_done - max_lsn_done).count();
        
        global_log_bytes += serialized_logs.size();

        LogFlushTiming batch_timing;
        if (!serialized_logs.empty()) {
            storage_service::StorageService_Stub storage_stub(get_storage_channel());
            brpc::Controller cntl;
            cntl.set_max_retry(0); // Prototype WAL writes fail the run instead of being resent.
            storage_service::LogWriteRequest request;
            storage_service::LogWriteResponse response;
            
            request.set_log(std::move(serialized_logs));
            
            batch_timing.rpc_start_ns = benchmark_stats::NowNs();
            storage_stub.LogWrite(&cntl, &request, &response, NULL);
            batch_timing.rpc_end_ns = benchmark_stats::NowNs();
            batch_timing.storage_ns = response.storage_ns();
            batch_timing.write_ns = response.write_ns();
            batch_timing.sync_ns = response.sync_ns();
            
            // No protocol may acknowledge a failed batch as durable.
            if (cntl.Failed()) {
                LOG(ERROR) << "LogFlush failed: " << cntl.ErrorText();
                std::abort();
            }
        }
        auto storage_rpc_done = std::chrono::high_resolution_clock::now();
        global_log_flush_storage_rpc_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(storage_rpc_done - serialize_done).count();

        // Publish before persist_lsn: waiters read after taking persist_lsn_mtx.
        for (const auto* log : batch_logs) {
            if (log->flush_timing_) {
                const auto enqueued = log->flush_timing_->enqueued_ns;
                *log->flush_timing_ = batch_timing;
                log->flush_timing_->enqueued_ns = enqueued;
            }
        }
        if (FILE* trace = wal_trace::Output()) {
            std::fprintf(trace, "batch %d %llu %zu %zu %llu %llu %llu\n",
                int(batch_logs.front()->log_node_id_), (unsigned long long)batch_logs.front()->lsn_,
                total_size, batch_logs.size(), (unsigned long long)first_enqueued,
                (unsigned long long)batch_timing.rpc_start_ns, (unsigned long long)batch_timing.rpc_end_ns);
        }
        LLSN ready_lsn = 0;
        int64_t cost_durable_ns = 0;
        if (max_lsn > 0) {
            std::lock_guard<bthread::Mutex> lk_lsn(persist_lsn_mtx);
            if (max_lsn > persist_lsn) {
                persist_lsn = max_lsn;
                if (!page_transfer_log_waits_.empty()) {
                    const auto durable_ns = benchmark_stats::NowNs();
                    auto it = page_transfer_log_waits_.begin();
                    while (it != page_transfer_log_waits_.end()) {
                        const auto& wait = *it;
                        if (wait->lsn <= persist_lsn) {
                            wait->elapsed_ns.store(durable_ns - wait->start_ns, std::memory_order_release);
                            it = page_transfer_log_waits_.erase(it);
                        } else {
                            ++it;
                        }
                    }
                }
                if (OnlineCostActive()) cost_durable_ns = page_cost::MonotonicNs();
                persist_lsn_cond.notify_all();
            }
            ready_lsn = persist_lsn;
        }
        if (cost_durable_ns) {
            for (const auto* log : batch_logs) {
                if (log->cost_log_phase_ >= 0 && log->cost_enqueued_ns_ > 0 &&
                    cost_durable_ns >= log->cost_enqueued_ns_) {
                    page_cost_model_.online.ObserveLog(static_cast<page_cost::LogPhase>(log->cost_log_phase_),
                        (cost_durable_ns - log->cost_enqueued_ns_) / 1e6, cost_durable_ns);
                }
            }
        }
        if (ready_lsn > 0 && node_->push_page_with_scheduler){
            assert(node_->getPushPageScheduler());
            node_->getPushPageScheduler()->markPushReady(ready_lsn);
        }

        // LOG(INFO) << "LogFlush Over , now ready lsn = " << persist_lsn;
        auto update_persist_lsn_done = std::chrono::high_resolution_clock::now();
        global_log_flush_update_persist_lsn_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(update_persist_lsn_done - storage_rpc_done).count();


        
        // 4. 释放已持久化的日志内存
        for (auto* log : batch_logs) {
            delete log;
        }
        auto end = std::chrono::high_resolution_clock::now();
        global_log_flush_total_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    /**
     * @brief 添加日志到共享队列
     * 
     * 将生成的日志记录添加到节点级别的共享日志队列中，等待后台线程批量刷新。
     * 
     * @param log 日志记录指针（所有权转移给 log_records）
     * 
     * 线程安全：使用互斥锁保护
     */
    // Allocate and enqueue metadata atomically with respect to page logs/flush.
    // In particular, a higher flushed LSN cannot pass an unqueued Prepare.
    // Undo uses the actual writer's existing batch WAL and persistence barrier.
    undo::Wal MakeUndoWal() {
        CHECK(generate_log_) << "undo requires WAL";
        return {
            [this](std::unique_ptr<LogRecord> record) {
                CHECK(record->log_node_id_ == node_->getNodeID());
                return AddMetadataLog(record.release());
            }
        };
    }
    LLSN AddMetadataLog(LogRecord* log, int cost_phase = -1,
                        std::shared_ptr<LogFlushTiming> timing = nullptr) {
        if (!IsLogEnabled()) {
            delete log;
            return 0;
        }
        log_mtx.lock();
        if (cost_phase >= 0 && OnlineCostActive()) {
            log->cost_log_phase_ = cost_phase;
            log->cost_enqueued_ns_ = page_cost::MonotonicNs();
        }
        const LLSN lsn = generate_next_llsn();
        log->lsn_ = lsn;
        AddToLogNoBlock(log, timing); // transfers ownership and releases log_mtx
        return lsn;
    }

    LLSN AppendLogAndWait(LogRecord* log, int log_type, int cost_phase = -1,
                          const mvcc::Context* committing_writer = nullptr) {
        CHECK(bool(committing_writer) == (log->log_type_ == LogType::MVCC_COMMIT));
        const auto commit_ts = committing_writer ? static_cast<MvccCommitLogRecord*>(log)->commit_ts : 0;
        const bool stamped = committing_writer || log->log_type_ == LogType::REMOTE_SINGLE_COMMIT;
        auto timing = generate_log_ ? std::make_shared<LogFlushTiming>() : nullptr;
        const auto lsn = AddMetadataLog(log, cost_phase, timing);
        if (stamped) {
            CHECK(timing && lsn > 0);
            RequestLogFlush(lsn);
        }
        const auto elapsed_ns = wait_log_flush(lsn, log_type, timing, stamped);
        if (stamped) benchmark_stats::commit_wal_wait.Add(elapsed_ns);
        if (committing_writer) {
            // Readers may trust COMMITTED in the writer's TIT immediately.
            // This wait covers the stamped commit record and its earlier row/undo records.
            Mvcc().CertifyRemoteCommit(*committing_writer, commit_ts);
        }
        return lsn;
    }

    void AddToLogNoBlock(LogRecord* log, const std::shared_ptr<LogFlushTiming>& timing = nullptr) {
        if (!generate_log_) {
            delete log;
            log_mtx.unlock();
            return;
        }
        if (timing) {
            timing->enqueued_ns = benchmark_stats::NowNs();
            log->flush_timing_ = timing;
        }
        bool need_notify = false;
        if (log_records.empty() && wal_trace::Output()) first_batch_enqueue_ns_ = benchmark_stats::NowNs();
        log_records.emplace_back(log);
        need_notify = log_records.size() >= log_flush_batch_trigger_;
        log_mtx.unlock();
        if (need_notify) {
            NotifyLogFlush();
        }
    }

    int GetLogFlushIntervalMs() const {
        return log_flush_interval_ms_;
    }

    bool IsCornusPrepareCommitEnabled() const {
        return two_pc_variant_ == "2pc-cornus-opt";
    }

    bool UsesCornusCommit() const {
        return two_pc_variant_ != "2pc";
    }

    bool IsLogEnabled() const {
        return generate_log_;
    }

    // 事务启动的时候调用，主要是负责一些统计信息的变更以及初始化
    void OnTxnExecuted() {
        const uint64_t txn_cnt = ++executed_txn_cnt_;
        // Benchmarks use an independent reporter, including when all transactions stall.
        if (benchmark_stats::runtime.start_ns.load(std::memory_order_relaxed) != 0) return;
        const int64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch()).count();
        if (txn_cnt == 1) {          // 第一笔事务：只记基线（原子自增保证只有一个线程看到 1）
            tps_last_print_ns_.store(now_ns, std::memory_order_relaxed);
            tps_last_print_committed_.store(
                committed_txn_cnt_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            return;
        }
        // 无锁快筛：绝大多数调用在这里直接返回，不碰锁也不打印
        if (now_ns - tps_last_print_ns_.load(std::memory_order_relaxed) < kTpsPrintIntervalNs) return;
        std::lock_guard<std::mutex> lk(tps_print_mtx_);
        const int64_t last_ns = tps_last_print_ns_.load(std::memory_order_relaxed);
        if (now_ns - last_ns < kTpsPrintIntervalNs) return;   // 别的线程刚打过
        const uint64_t committed = committed_txn_cnt_.load(std::memory_order_relaxed);
        const uint64_t last_committed = tps_last_print_committed_.load(std::memory_order_relaxed);
        const double dt = (double)(now_ns - last_ns) / 1e9;   // 用真实间隔，打印被拖延也算得准
        tps_last_print_ns_.store(now_ns, std::memory_order_relaxed);
        tps_last_print_committed_.store(committed, std::memory_order_relaxed);
        const double window_tps = (dt > 0 ? (double)(committed - last_committed) / dt : 0.0);
        std::cout << "Executed Txn Cnt = " << txn_cnt
                  << "  TPS = " << window_tps
                  << "  Abort Cnt = " << aborted_txn_cnt_.load(std::memory_order_relaxed)
                  << "\n";
        // 同时写 glog：stdout 被 test_multi_node 的 SSH 线程吞在内存里，崩溃/卡住时看不到；
        // glog 每秒一条，便于后台监控真实进度而不是只看 combo 是否结束。
        LOG(INFO) << "[PROGRESS] node=" << get_node()->getNodeID()
                  << " executed=" << txn_cnt
                  << " committed=" << committed
                  << " aborted=" << aborted_txn_cnt_.load(std::memory_order_relaxed)
                  << " window_tps=" << window_tps
                  << " window_s=" << dt;
    }

    // 事务提交（DTX::TxCommit 收尾）与回滚（DTX::Abort 收尾）各调用一次。
    // 只加原子计数，供上面的实时 TPS/回滚数量使用。
    void OnTxnCommitted() { committed_txn_cnt_.fetch_add(1, std::memory_order_relaxed); }
    void OnTxnAborted()   { aborted_txn_cnt_.fetch_add(1, std::memory_order_relaxed); }
    
    /**
     * @brief 获取当前已持久化的最大 LSN
     * 
     * @return LLSN 已持久化的最大日志序列号
     */
    LLSN CurrentLsn() const {
        std::lock_guard<bthread::Mutex> guard(log_mtx);
        return current_llsn_;
    }
    LLSN GetPersistedLSN() const {
        std::lock_guard<bthread::Mutex> lk(persist_lsn_mtx);
        return persist_lsn;
    }
    
    
    /**
     * @brief 服务关闭：确保所有日志已刷新到存储层
     * 
     * 在服务器关闭前调用此方法，确保所有待持久化的日志都已写入存储层。
     */
    void Shutdown() {
        std::cout << "ComputeServer shutting down, flushing remaining logs...";
        log_flush_running.store(false);  // 通知后台线程停止
        NotifyLogFlush();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));  // 等待线程结束
        LogFlush();  // 最后一次刷新
        std::cout << "All logs flushed, persist_lsn=" << GetPersistedLSN();
    }

    bool ShutdownStorageServer() {
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl;
        storage_service::ShutdownStorageRequest request;
        storage_service::ShutdownStorageResponse response;
        storage_stub.ShutdownStorage(&cntl, &request, &response, NULL);
        return !cntl.Failed() && response.ok();
    }

    bool ShutdownRemoteServer() {
        partition_table_service::PartitionTableService_Stub partable_stub(get_pagetable_channel());
        brpc::Controller cntl;
        partition_table_service::SendFinishRequest request;
        partition_table_service::SendFinishResponse response;
        request.set_node_id(node_->getNodeID());
        partable_stub.SendFinish(&cntl, &request, &response, NULL);
        return !cntl.Failed();
    }


public:
    std::vector<BLinkIndexHandle*> bl_indexes;
    std::vector<SecFSM*> fsm_trees;
    
    // 后台日志刷新线程控制（需要外部访问，故放在 public）
    std::atomic<bool> log_flush_running{true};  // 控制后台线程是否继续运行

    // A durable request survives notifications and any in-flight RPC. It is
    // satisfied only when the successfully persisted watermark covers it.
    void RequestLogFlush(LLSN target_lsn) {
        if (!generate_log_) return;
        std::lock_guard<bthread::Mutex> lock(persist_lsn_mtx);
        if (target_lsn > persist_lsn) {
            requested_flush_lsn_ = std::max(requested_flush_lsn_, target_lsn);
            log_flush_trigger_cond.notify_one();
        }
    }

    // The timer is only for background work, never a gate on durability.
    void WaitLogFlushTrigger(long ms) {
        std::unique_lock<bthread::Mutex> lock(persist_lsn_mtx);
        if (requested_flush_lsn_ <= persist_lsn && !log_flush_batch_ready_ &&
            log_flush_running.load()) {
            log_flush_trigger_cond.wait_for(lock, ms * 1000);
        }
        log_flush_batch_ready_ = false;
    }

    // 唤醒日志刷新线程
    void NotifyLogFlush() {
        std::lock_guard<bthread::Mutex> lock(persist_lsn_mtx);
        log_flush_batch_ready_ = true;
        log_flush_trigger_cond.notify_one();
    }

private:
    bthread::ConditionVariable log_flush_trigger_cond;
    LLSN requested_flush_lsn_{0}; // protected by persist_lsn_mtx
    bool log_flush_batch_ready_{false};
    int log_flush_interval_ms_{DEFAULT_LOG_FLUSH_INTERVAL_MS};
    size_t log_flush_batch_trigger_{DEFAULT_LOG_FLUSH_BATCH_TRIGGER};
    bool generate_log_{true};
    std::string two_pc_variant_{"2pc-cornus-opt"};
    bool two_pc_variant_configured_{false};
    std::atomic<uint64_t> executed_txn_cnt_{0};
    std::atomic<uint64_t> committed_txn_cnt_{0};   // 已提交事务数（TPS(avg) 用）
    std::atomic<uint64_t> aborted_txn_cnt_{0};     // 已回滚事务数（Abort() 收尾时计数）
    // 进度打印间隔：默认 1s 一行（原来每笔一行太吵）
    static constexpr int64_t kTpsPrintIntervalNs = 1000LL * 1000 * 1000;
    std::mutex tps_print_mtx_;
    std::atomic<int64_t> tps_last_print_ns_{0};            // 上一次打印的时间
    std::atomic<uint64_t> tps_last_print_committed_{0};    // 上一次打印时的提交数（算窗口 TPS 用）

    ComputeNode* node_;
    std::vector<GlobalLockTable*>* global_page_lock_table_list_;
    std::vector<GlobalValidTable*>* global_valid_table_list_;


    brpc::Channel* nodes_channel; //与其他计算节点通信的channel
    page_table_service::PageTableServiceImpl* page_table_service_impl_; // 保存在类中，以便本地调用

    LockWaitManager lock_wait_mgr_; // 元组锁等待管理器（WAIT_DETECT 模式）

    // 当前已经完成的协程计数
    std::atomic<int> alive_fiber_cnt;

    // SQL
    unsigned sql_active_transactions_ = 0;
    std::mutex tab_meta_mtx;
    bool sql_ddl_active_ = false;

    // Cache for RmFileHdr
    bthread::Mutex file_hdr_cache_mutex_;
    std::map<table_id_t, RmFileHdr::ptr> file_hdr_cache_;

    // 日志管理：节点级别的共享日志系统
    // 所有事务的日志都写入此共享队列，由后台线程统一刷新到存储层
    uint64_t first_batch_enqueue_ns_ = 0; // protected by log_mtx
    std::vector<LogRecord*> log_records;           // 共享日志队列
    bthread::Mutex log_flush_mtx_;
    mutable bthread::Mutex log_mtx;                 // 保护 log_records 的互斥锁
    
    // 持久化 LSN 管理
    LLSN persist_lsn = 0;                       // 已持久化到存储层的最大 LSN
    mutable bthread::Mutex persist_lsn_mtx;         // 保护 persist_lsn 的互斥锁
    bthread::ConditionVariable persist_lsn_cond;   // persist_lsn 条件变量
    std::vector<std::shared_ptr<PageTransferLogWait>> page_transfer_log_waits_; // persist_lsn_mtx

    LLSN current_llsn_ = 0;                     // 本节点当前的最大 LLSN, 初始为0
};

int socket_finish_client(std::string ip, int port, bool drain_barrier = false,
                         std::function<void()> flush_logs = {}, std::function<void()> reset_stats = {});
