#pragma once
#include <brpc/channel.h>
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <mutex>
#include <condition_variable>
#include <map>
#include <optional>
#include <random>
#include <chrono>
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
#include "../core/recovery_interface/recovery_scheduler.h"
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
#include "scheduler/corotine_scheduler.h"
#include "GPLM/global_page_lock.h"
#include "GPLM/global_valid_table.h"
#include "recovery_page_catalog/recovery_page_catalog.h"

// sql
#include "sql_executor/record_printer.h"
#include "sql_executor/sql_common.h"

#include "index/bp_tree/blink/blink.h"
#include "core/fsm/fsm_tree.h"

#include "util/bitmap.h"
#include "util/bench_control.h"
#include "error_library.h"

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

    // 心跳与故障通知
    virtual void Heartbeat(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::HeartbeatRequest* request,
                       ::compute_node_service::HeartbeatResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void NotifyNodeFailure(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NodeFailureNotification* request,
                       ::compute_node_service::NodeFailureAck* response,
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
    virtual void Abort(::google::protobuf::RpcController* controller,
                        const ::twopc_service::AbortRequest* request,
                        ::twopc_service::AbortResponse* response,
                        ::google::protobuf::Closure* done);
    private:
    ComputeServer* server;

    struct timespec next_commit_time;
    std::mutex commit_log_mutex;
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

// 日志刷新配置常量
const size_t LOG_FLUSH_THRESHOLD = 10000;        // 日志数量阈值：达到300条触发刷新
const int LOG_FLUSH_INTERVAL_MS = 10;          // 时间间隔：30ms触发刷新

// Class ComputeNode 可以建立与pagetable的连接，但不能直接与其他计算节点通信
// 因为compute_node_rpc.h引用了compute_node.h，compute_node.h引用了compute_node_rpc.h，会导致循环引用
// 所以建立一个ComputeServer类，ComputeServer类可以与其他计算节点通信
class ComputeServer {
public:
    ComputeServer(ComputeNode* node, std::vector<std::string> compute_ips, std::vector<int> compute_ports): node_(node){
        if (WORKLOAD_MODE != 4){
            // 如果不是 SQL 模式，那表名都是硬编码到系统里的
            InitTableNameMeta();
        }
        if (SYSTEM_MODE == 13){
            // 如果是时间片轮转算法的话，热点页面集合用一个哈希来存储
            InitHotPages();
        }
        // 构造与其他计算节点通信的channel
        nodes_channel = new brpc::Channel[ComputeNodeCount];
        brpc::ChannelOptions options;
        options.use_rdma = use_rdma;
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

        std::thread t([this,compute_ips,compute_ports] {
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

            std::cout << "Init...\n";

            // add meta server service in each compute node
            // 初始化全局的bufferpool和page_lock_table
            // 初始化 30000 个，0 到 10000 存表，10000 到 20000 存B+树，20000 到 30000 存FSM
            global_page_lock_table_list_ = new std::vector<GlobalLockTable*>(30000 , nullptr);
            global_valid_table_list_ = new std::vector<GlobalValidTable*>(30000 , nullptr);
            int table_cnt;
            if (WORKLOAD_MODE == 0){
                table_cnt = 2;
            }else if (WORKLOAD_MODE == 1){
                table_cnt = 11;
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
                (*global_page_lock_table_list_)[i] = new GlobalLockTable();
                (*global_valid_table_list_)[i] = new GlobalValidTable();

                // BLink
                (*global_page_lock_table_list_)[i + 10000] = new GlobalLockTable();
                (*global_valid_table_list_)[i + 10000] = new GlobalValidTable();

                // FSM
                (*global_page_lock_table_list_)[i + 20000] = new GlobalLockTable();
                (*global_valid_table_list_)[i + 20000] = new GlobalValidTable();
            }

            page_table_service_impl_ = new page_table_service::PageTableServiceImpl(global_page_lock_table_list_, global_valid_table_list_);
            // R2c C1: 汇报路径接入统一恢复影响目录
            page_table_service_impl_->SetRecoveryCatalog(&recovery_catalog_);
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
            server_options.num_threads = 0;  // 使用默认值(>= 8+EPOLL_THREAD_NUM)，避免低于已有 bthread 并发度
            server_options.use_rdma = use_rdma;

            // SQL 模式下，初始化一下每个已存在的 table
            for (int i = 0 ; i < node_->table_names.size() ; i++){
                bool res = table_exist(node_->table_names[i]);
                assert(res);
            }
            std::cout << "Initlize Over , Server Start\n";

            if (server.Start(point,&server_options) != 0) {
                LOG(ERROR) << "Fail to start Server";
                exit(1);
            }

            if (bench_control::enabled() && WORKLOAD_MODE != 4) {
                {
                    std::lock_guard<std::mutex> lock(rpc_lifecycle_mutex_);
                    rpc_ready_ = true;
                    rpc_lifecycle_cv_.notify_all();
                }
                bench_control::publish("rpc-ready");
                std::unique_lock<std::mutex> lock(rpc_lifecycle_mutex_);
                rpc_lifecycle_cv_.wait(lock, [this] { return rpc_stop_; });
                lock.unlock();
                server.Stop(0);
                server.Join();
            } else {
                server.RunUntilAskedToQuit();
                exit(1);
            }
        });
        if (bench_control::enabled() && WORKLOAD_MODE != 4) rpc_thread_ = std::move(t);
        else t.detach();
    }

    void WaitRpcReady() {
        std::unique_lock<std::mutex> lock(rpc_lifecycle_mutex_);
        if (!rpc_lifecycle_cv_.wait_for(lock, std::chrono::seconds(40), [this] { return rpc_ready_; }))
            throw std::runtime_error("compute RPC initialization deadline");
    }

    void StopRpc() {
        {
            std::lock_guard<std::mutex> lock(rpc_lifecycle_mutex_);
            rpc_stop_ = true;
            rpc_lifecycle_cv_.notify_all();
        }
        if (rpc_thread_.joinable()) rpc_thread_.join();
    }

    std::thread rpc_thread_;
    std::mutex rpc_lifecycle_mutex_;
    std::condition_variable rpc_lifecycle_cv_;
    bool rpc_ready_ = false;
    bool rpc_stop_ = false;

    // fsm_retry_times：FSM 页面访问失败时的重试次数（默认 2 次）；
    // 重试用尽后 SecFSM 会降级为返回共享存储上最新申请的页面
    page_id_t search_free_page(table_id_t table_id , uint32_t min_space_needed , uint32_t fsm_retry_times = 2){
        return fsm_trees[table_id]->find_free_page(min_space_needed , fsm_retry_times);
    }

    // 更新 FSM 中页面的空间信息，返回更新前的空间估计值；
    // 返回 UINT32_MAX 表示 FSM 类别未变化或更新失败（调用方无需记日志）。
    // 旧值用于 FSMUPDATE 日志的 undo 恢复
    uint32_t update_page_space(table_id_t table_id , uint32_t page_id , uint32_t free_space){
        //assert(false);
        return fsm_trees[table_id]->update_page_space(page_id , free_space);
    }

    LLSN UpdatePageLLSN(RmPageHdr* pagehdr) {
        log_mtx.lock();
        LLSN lsn = update_page_llsn(pagehdr);
        return lsn;
    }

    // 为不带页面的逻辑日志（blink/FSMUPDATE）分配单调递增 LSN。
    // 注意：该函数持有 log_mtx，调用方必须紧接着调用
    // AddToLogNoBlock（其内部解锁），与 UpdatePageLLSN 的用法一致
    LLSN GenLogLSN(){
        log_mtx.lock();
        return generate_next_llsn();
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
            page_0 = rpc_lazy_fetch_s_page(table_id , 0 , true);
        }else if (SYSTEM_MODE == 2){
            // 2pc
            node_id_t node_id = get_recovery_node_id(table_id, 0);
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
                    LOG(ERROR) << "Fail to get page 0 from remote compute node " << node_id << ": " << cntl.ErrorText();
                    // 不崩溃 - 目标节点可能已故障
                }
                remote_data = response.data();
            }
        }else if (SYSTEM_MODE == 3){
            page_0 = single_fetch_s_page(table_id , 0);
        }else{
            assert(false);
        }
        
        RmFileHdr* hdr;
        if (is_remote) {
            hdr = reinterpret_cast<RmFileHdr*>(const_cast<char*>(remote_data.c_str()) + sizeof(RmPageHdr));
        } else {
            hdr = reinterpret_cast<RmFileHdr*>(page_0->get_data() + sizeof(RmPageHdr));
        }
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
        std::lock_guard<std::mutex> lk(file_hdr_cache_mutex_);
        if (file_hdr_cache_.find(table_id) != file_hdr_cache_.end()){
            return file_hdr_cache_[table_id];
        }
        RmFileHdr::ptr hdr = get_file_hdr(table_id);
        file_hdr_cache_[table_id] = hdr;
        return hdr;
    }

    char *FetchSPage(table_id_t table_id , page_id_t page_id){
        Page *page = nullptr;
        assert(table_id >= 0 && table_id < 30000);
        assert(page_id >= 0);
        if(SYSTEM_MODE == 0) {
            // Eager
            page = rpc_fetch_s_page(table_id, page_id);
        } 
        else if(SYSTEM_MODE == 1){
            // Lazy
            page = rpc_lazy_fetch_s_page(table_id,page_id , true);
        }
        else if(SYSTEM_MODE == 2){
            // 2PC
            page = local_fetch_s_page(table_id,page_id);
        }
        else if(SYSTEM_MODE == 3){
            page = single_fetch_s_page(table_id,page_id);
        } else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            page = rpc_ts_fetch_s_page(table_id , page_id);
        } else{
            assert(false);
        }
        return page->get_data();
    }

    Page *FetchXPage(table_id_t table_id , page_id_t page_id){
        assert(table_id >= 0 && table_id < 30000);
        assert(page_id >= 0);
        Page *page = nullptr;
        if(SYSTEM_MODE == 0) {
            page = rpc_fetch_x_page(table_id,page_id);
        } else if(SYSTEM_MODE == 1){
            page = rpc_lazy_fetch_x_page(table_id,page_id , true);
        } else if(SYSTEM_MODE == 2){
            page = local_fetch_x_page(table_id,page_id);
        } else if(SYSTEM_MODE == 3){
            page = single_fetch_x_page(table_id,page_id);
        } else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            page = rpc_ts_fetch_x_page(table_id , page_id);
        } else {
            assert(false);
        }
        return page;
    }

    void ReleaseSPage(table_id_t table_id , page_id_t page_id){
        assert(table_id >= 0 && table_id < 30000);
        if (SYSTEM_MODE == 0){
            rpc_release_s_page(table_id , page_id);
        }else if (SYSTEM_MODE == 1){
            rpc_lazy_release_s_page(table_id , page_id);
        }else if (SYSTEM_MODE == 2){
            local_release_s_page(table_id , page_id);
        }else if (SYSTEM_MODE == 3){
            single_release_s_page(table_id, page_id);
        }else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            rpc_ts_release_s_page(table_id , page_id);
        }else {
            assert(false);
        }
    }
    void ReleaseXPage(table_id_t table_id , page_id_t page_id){
        if (SYSTEM_MODE == 0){
            rpc_release_x_page(table_id , page_id);
        }else if (SYSTEM_MODE == 1){
            rpc_lazy_release_x_page(table_id , page_id);
        }else if (SYSTEM_MODE == 2){
            local_release_x_page(table_id , page_id);
        }else if (SYSTEM_MODE == 3){
            single_release_x_page(table_id, page_id);
        }else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            rpc_ts_release_x_page(table_id , page_id);
        }else {
            assert(false);
        }
    }

    // SQL
    // ----------------------------------------------------------------------
    std::string getTableNameFromTableID(table_id_t table_id){
        for (auto it = get_node()->db_meta.m_tabs.begin() ; it != get_node()->db_meta.m_tabs.end() ; it++){
            if (it->second.table_id == table_id){
                return it->first;
            }
        }
        return "";
    }
    // 创建一张新表
    void create_table(const std::string &tab_name , std::vector<ColDef> cols , const std::string pri_key){
        if (!tryCreateTable()){
            throw std::logic_error("Create Table Failed (Maybe Droping table now?), Please Try Later");
        }

        // assert(pri_key != "");

        try {
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
            if (cols.size() == 1){
                throw std::logic_error("不允许只有一个主键列");
            }

            storage_stub.CreateTable(&cntl , &request , &response , NULL);
            if (cntl.Failed()){
                assert(false);
            }

            int error_code = response.error_code();
            if (error_code == 0){
                assert(table_exist(tab_name));
                int table_id = response.table_id();
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
        }catch (...) {
            NotifyCreateTableSuccess();
            throw;
        }

        NotifyCreateTableSuccess();
    }

    void dropTable(const std::string &tab_name){
        // 先给表加上锁，等待访问这个表的事务做完，同时不让后续事务再访问这个表
        if (!tryDropTable(tab_name)){
            throw std::logic_error("Try Drop Table Failed , Please Try Later");
        }

        bool exist = table_exist(tab_name);
        if (!exist){
            NotifyDropTableOver();
            throw LJ::TableNotFoundError(tab_name);
        }

        TabMeta tab = get_node()->db_meta.get_table(tab_name);
        table_id_t table_id = tab.table_id;

        // 通知其它节点先尝试 dropTable，如果有节点失败的话，就放弃 dropTable
        for (int i = 0 ; i < ComputeNodeCount ; i++){
            if (i == getNodeID()){
                continue;
            }
            brpc::Controller* cntl = new brpc::Controller();
            compute_node_service::ComputeNodeService_Stub compute_node_stub(&nodes_channel[i]);
            compute_node_service::NotifyDropTableRequest notify_request;
            compute_node_service::NotifyDropTableResponse* notify_response = new compute_node_service::NotifyDropTableResponse();
            notify_request.set_tab_name(tab_name);
            compute_node_stub.NotifyDropTable(cntl , &notify_request , notify_response , NULL);
            if (!notify_response->ok()){
                // 只要有一个节点不允许 drop，那就放弃 drop
                for (int j = 0 ; j < i ; j++){
                    brpc::Controller cntl_quit;
                    compute_node_service::ComputeNodeService_Stub quit_stub(&nodes_channel[j]);
                    compute_node_service::quitDropTableRequest quit_req;
                    compute_node_service::quitDropTableResponse quit_resp;
                    quit_req.set_tab_name(tab_name);
                    quit_stub.quitDropTable(&cntl_quit , &quit_req , &quit_resp , NULL);
                }
                throw std::logic_error("Not Allow To Drop now , please Try Later");
            }
        }

        // 此时所有节点都已经设置好 dropTable 标记了，可以开始删除表了

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
                }
            }

            table_id_t tab_id = get_node()->db_meta.get_table(tab_name).table_id;
            clearTable(table_id);
            NotifyDropTableOver();

        }else {
            // 存储层不允许删除表，那就通知其它节点放弃删除表
            for (int i = 0 ; i < ComputeNodeCount ; i++){
                if (i == getNodeID()){
                    NotifyDropTableOver();
                }else {
                    brpc::Controller cntl_quit;
                    compute_node_service::ComputeNodeService_Stub quit_stub(&nodes_channel[i]);
                    compute_node_service::quitDropTableRequest quit_req;
                    compute_node_service::quitDropTableResponse quit_resp;
                    quit_req.set_tab_name(tab_name);
                    quit_stub.quitDropTable(&cntl_quit , &quit_req , &quit_resp , NULL);
                }
            }
            LJ::throw_error_by_code(error_code);
        }
    }


    // 清理掉 table 的缓冲区，FSM，锁表等信息
    void clearTable(table_id_t table_id){
        {
            std::lock_guard<std::mutex> lk(tab_meta_mtx);
            std::string tab_name = getTableNameFromTableID(table_id);
            assert(tab_name != "");
            get_node()->db_meta.m_tabs.erase(tab_name);
            table_use.erase(tab_name);
        }
        {
            std::lock_guard<std::mutex> lk(file_hdr_cache_mutex_);
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

        if (get_node()->local_buffer_pools[table_id]){
            delete get_node()->local_buffer_pools[table_id];
            get_node()->local_buffer_pools[table_id] = nullptr;
        }
        if (get_node()->local_buffer_pools[table_id + 10000]){
            delete get_node()->local_buffer_pools[table_id + 10000];
            get_node()->local_buffer_pools[table_id + 10000] = nullptr;
        }
        if (get_node()->local_buffer_pools[table_id + 20000]){
            delete get_node()->local_buffer_pools[table_id + 20000];
            get_node()->local_buffer_pools[table_id + 20000] = nullptr;
        }

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
                node_->lazy_local_page_lock_tables[tab_meta.table_id] = new LRLocalPageLockTable();
                node_->lazy_local_page_lock_tables[tab_meta.table_id + 10000] = new LRLocalPageLockTable();
                node_->lazy_local_page_lock_tables[tab_meta.table_id + 20000] = new LRLocalPageLockTable();
            }else {
                assert(false);
            }

            node_->local_buffer_pools[tab_meta.table_id] = new BufferPool(node_->pool_size_per_table , 10000);
            if (tab_meta.primary_key != ""){
                node_->local_buffer_pools[tab_meta.table_id + 10000] = new BufferPool(node_->pool_size_per_blink , 10000);
            }
            node_->local_buffer_pools[tab_meta.table_id + 20000] = new BufferPool(node_->pool_size_per_fsm , 5000);
            
            (*global_page_lock_table_list_)[tab_meta.table_id] = new GlobalLockTable();
            (*global_valid_table_list_)[tab_meta.table_id] = new GlobalValidTable();

            // BLink
            if (tab_meta.primary_key != "") {
                (*global_page_lock_table_list_)[tab_meta.table_id + 10000] = new GlobalLockTable();
                (*global_valid_table_list_)[tab_meta.table_id + 10000] = new GlobalValidTable();
            }

            // FSM
            (*global_page_lock_table_list_)[tab_meta.table_id + 20000] = new GlobalLockTable();
            (*global_valid_table_list_)[tab_meta.table_id + 20000] = new GlobalValidTable();

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
    bool insert_into_blink(table_id_t table_id , itemkey_t key , Rid value){
        return bl_indexes[table_id]->insert_entry(&key , value) != INVALID_PAGE_ID;
    }
    Rid delete_from_blink(table_id_t table_id , itemkey_t key){
        return bl_indexes[table_id]->delete_entry(&key);
    }

    void PushPageToOther(table_id_t table_id , page_id_t page_id , node_id_t dest_node_id);

    struct InProcessTag {};
    ComputeServer(InProcessTag, ComputeNode* node, brpc::Channel* channels,
                  page_table_service::PageTableServiceImpl* service,
                  std::vector<GlobalLockTable*>* locks, std::vector<GlobalValidTable*>* validity)
        : node_(node), global_page_lock_table_list_(locks), global_valid_table_list_(validity),
          nodes_channel(channels), page_table_service_impl_(service) {}

    ~ComputeServer(){}

    static void PushPageRPCDone(compute_node_service::PushPageResponse* response,
                                brpc::Controller* cntl,
                                int table_id,
                                int page_id,
                                ComputeServer* server);

    static void NotifyCreateTableRPCDone(compute_node_service::NotifyCreateTableResponse* response,
                                         brpc::Controller* cntl,
                                         std::atomic<bool>* has_error);

    // ****************** for eager release *********************
    Page* rpc_fetch_s_page(table_id_t table_id, page_id_t page_id);

    Page* rpc_fetch_x_page(table_id_t table_id, page_id_t page_id);

    void rpc_release_s_page(table_id_t table_id, page_id_t page_id);
    
    void rpc_release_x_page(table_id_t table_id, page_id_t page_id);

    // ****************** eager release end *********************

    // ****************** for lazy release *********************
    Page* rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id, bool need_to_record = false);
    Page* rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id, bool need_to_record = false);
    void rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id);
    void rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id);
    // 第 14 层（fault-0088）：LPLM stuck 自愈强制回收泄漏 latch 后，向
    // GPLM 补发远程释放（防 holder 泄漏锁死其他节点）。xlock=被回收的
    // 远程锁模式（true=X/false=S）。不触碰本地 LPLM 状态。
    // L16（smoke-017）：force_forfeit_holders=true 仅由 45s 授权墙钟超时
    // 路径置位——等待者从未持有锁份额（解锁恒 stale），请求 manager 没收
    // 卡死 holders 并推进队列；第 14 层调用点保持 false（本节点可能真实
    // 持有份额，正常解锁即可）。
    void ReleaseRemoteForForcedPage(table_id_t table_id, page_id_t page_id, bool xlock,
                                    bool force_forfeit_holders = false);
    // ****************** lazy release end ********************

    // ******************* for ts fetch ***********************
    Page *rpc_ts_fetch_s_page(table_id_t table_id , page_id_t page_id);
    Page *rpc_ts_fetch_x_page(table_id_t table_id , page_id_t page_id);
    void rpc_ts_release_s_page(table_id_t table_id , page_id_t page_id);
    void rpc_ts_release_x_page(table_id_t table_id , page_id_t page_id);
   

    // 切换当前节点所在的时间片
    void ts_switch_phase(uint64_t time_slice);
    void ts_switch_phase_hot_new(uint64_t time_slice);

    inline bool is_ts_par_page(table_id_t table_id , page_id_t page_id , int now_ts_cnt){
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        assert(partition_size > 0);        
        int page_belong_par = ((page_id - 1) / partition_size) % ComputeNodeCount;
        return page_belong_par == now_ts_cnt;
    }

    int get_ts_belong_par(table_id_t table_id , page_id_t page_id){
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        assert(partition_size > 0);        
        int page_belong_par = ((page_id - 1) / partition_size) % ComputeNodeCount;
        return page_belong_par;
    }

    void tryLockTs(table_id_t table_id , page_id_t page_id , bool is_write){
        bool is_hot = false;
        if (SYSTEM_MODE == 13){
            is_hot = is_hot_page(table_id , page_id);
        }
        
        if (is_hot){
            if (is_write){
                node_->switch_mtx_hot.lock();
            }
            while (!(is_ts_par_page(table_id , page_id , node_->ts_cnt_hot) && node_->getPhaseHotNoBlock() == TsPhase::RUNNING)){
                if (is_write) node_->switch_mtx_hot.unlock();
                int target = get_ts_belong_par(table_id , page_id);
                node_->getScheduler()->YieldToHotSlice(target);
                if (is_write) node_->switch_mtx_hot.lock();
            }
            if (is_write){
                node_->set_page_dirty_hot(table_id , page_id , true);
            } 
            node_->ts_inflight_fetch_hot++;
            if (is_write) node_->switch_mtx_hot.unlock();
        }else{
            if (is_write){
                node_->switch_mtx.lock();
            }
            bool cond1 = is_ts_par_page(table_id , page_id , node_->ts_cnt);
            bool cond2 = (node_->getPhaseNoBlock() == TsPhase::RUNNING);
            while (!(cond1 && cond2)){
                if (is_write){
                    node_->switch_mtx.unlock();
                }
                int target = get_ts_belong_par(table_id , page_id);
                // if (!cond2){
                //     node_->getScheduler()->YieldAllToSlice(node_->ts_cnt);
                // }
                node_->getScheduler()->YieldToSlice(target);

                if (is_write) node_->switch_mtx.lock();
                cond1 = is_ts_par_page(table_id , page_id , node_->ts_cnt);
                cond2 = (node_->getPhaseNoBlock() == TsPhase::RUNNING);
            }
            if (is_write){
                node_->set_page_dirty(table_id , page_id , true);
                node_->switch_mtx.unlock();
            }
            
            node_->ts_inflight_fetch.fetch_add(1);
        }
    }
    // ******************* ts fetch end ***********************

    void rpc_flush_page_to_storage(table_id_t table_id , page_id_t page_id){
        // 第 17 层续（early26 WAL 实证 lsn=173 prev=172 之后 176 prev 仍=172）：
        // 驱逐写回时本页可能有"已生成但未发送"的日志（AddToLogNoBlock 只入
        // 本地队列）。若不先等日志落盘：写回整页被 storage 版本守卫拒绝后
        // 副本即被逐出，重取时（L17 取页屏障只追平已落盘 WAL）读回旧版页，
        // 页头 LLSN 回退 → 本事务后续日志 prev 断链 → storage 回放抛错
        // abort。故驱逐写回前先等共享日志队列全部持久化（WAL ≥ 副本版本），
        // 之后无论写回被拒（增量由 replay 重建）还是跳过（等版本），链都
        // 连续。等待失败（storage 不可达等）抛错中止当前路径，绝不带未
        // 持久化增量丢弃副本。
        {
            uint64_t flush_ticket_17;
            {
                std::lock_guard<std::mutex> lock(log_mtx);
                flush_ticket_17 = log_enqueue_sequence_;
            }
            if (flush_ticket_17 > 0 && !WaitLogReceipt(flush_ticket_17, 30000)) {
                throw std::runtime_error("log flush deadline before page eviction write-back");
            }
        }
        Page *old_page = node_->fetch_page(table_id , page_id);
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl_wp;
        storage_service::WritePageRequest req;
        storage_service::WritePageResponse resp;
        auto* pid = req.mutable_page_id();
        pid->set_table_name(table_name_meta[table_id]);
        pid->set_page_no(page_id);
        req.set_data(old_page->get_data(), PAGE_SIZE);
        storage_stub.WritePage(&cntl_wp, &req, &resp, NULL);
        if (cntl_wp.Failed()) {
            LOG(ERROR) << "WritePage RPC failed for table_id=" << table_id
                        << " page_id=" << page_id
                        << " err=" << cntl_wp.ErrorText();
        }
    }

    Page* checkIfDirectlyPutInBuffer(table_id_t table_id , page_id_t page_id , const void *data){
        frame_id_t frame_id = INVALID_FRAME_ID;
        bool ans = node_->getBufferPoolByIndex(table_id)->checkIfDirectlyPutInBuffer(page_id , frame_id);
        if (ans){
            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(
                table_id,
                page_id ,
                frame_id ,
                false ,
                INVALID_PAGE_ID ,
                data
            );
            assert(page != nullptr);
            return page;
        }
        return nullptr;
    }

    // 已经在缓冲区内的，更新其数据
    // 只有 eager 和 ts 模式下会调用这个
    bool checkIfDirectlyUpdate(table_id_t table_id , page_id_t page_id , const void *data){
        assert(SYSTEM_MODE == 0 || SYSTEM_MODE == 12 || SYSTEM_MODE == 13);
        return node_->getBufferPoolByIndex(table_id)->checkIfDirectlyUpdate(page_id , data);
    }

    Page *put_page_into_buffer(table_id_t table_id , page_id_t page_id , const void *data , int type , bool need_to_record = false){
        if (type == 0){
            // eager
            return put_page_into_buffer_eager(table_id , page_id , data);
        }else if (type == 1){
            // lazy
            Page *page = put_page_into_buffe_lazy(table_id , page_id , data , need_to_record);
            // 即使节点拿到了这个页面的X 锁，也不一定修改这个页面，is_dirty 的作用即判断下，本节点是否修改过这个页面
            // 之所以需要知道是否修改过，是因为存在一个情况：假如节点 0啥也没干，persist_lsn = 0，节点干了一堆活，persist_lsn = 1000
            // 然后节点 1 把页面传给了节点 0，节点 0 拿到了 X 锁，结果还是啥也没干，persist_lsn 仍然等于 0
            // 此时节点 1 又申请了这个页面，节点 0 需要把页面传过去，传过去之前，需要等这个页面的日志刷下去，如果 is_dirty = fals，就不需要等待了
            page->set_dirty(false);
            return page;
        }else if (type == 2){
            // 2pc
            return put_page_into_buffer_2pc(table_id , page_id , data);
        }else if (type == 3){
            // single，TODO
        }else if (type == 12 || type == 13){
            // 时间片
            return put_page_into_buffer_ts(table_id , page_id , data);
        }
        assert(false);
    }

    // 将页面放进缓冲区中，如果缓冲区满，选择一个页面淘汰
    // lazy_release 的淘汰策略
    Page *put_page_into_buffe_lazy(table_id_t table_id , page_id_t page_id , const void *data , bool need_to_record) {
        bool is_from_lru = false;
        frame_id_t frame_id = -1;

        // IR Recovery: 页面可能因 lazy release 仍留在缓冲区（GPLM 已 reset 但 buffer 未清除）
        // 此时直接更新缓冲区中的数据即可
        Page* existing = node_->getBufferPoolByIndex(table_id)->try_fetch_page(page_id);
        if (existing != nullptr) {
            memcpy(existing->get_data(), data, PAGE_SIZE);
            return existing;
        }

        // 先试试看缓冲区是否有空闲位置
        Page *page = checkIfDirectlyPutInBuffer(table_id , page_id , data);
        if (page != nullptr){
            return page;
        }

        // 作为一个参数传入淘汰窗口中，目标是锁定一个页面，确保页面淘汰过程中别的线程无法访问本页面
        auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            return this->node_->lazy_local_page_lock_tables[table_id]->GetLock(victim_page_id)->TryBeginEvict();
        });

        // LOG(INFO) << "Put Page Into Buffer , table_id = " << table_id << " page_id = " << page_id; 
        
        int try_cnt = -1;
        // 循环直到找到一个可淘汰的页面
        while(true){
            /*
                淘汰的流程，总结一下：
                1. 先去 lru_list 里找到一个没在用的页面
                2. 执行 try_begin_evict，尝试去锁定这个页面，锁定成功后，不允许其它线程再去获取这个页面锁
                3. 向远程发送解锁请求，远程如果同意了，那就真正释放掉这个页面，否则回到第一步再选一个页面
            */
            try_cnt++;
            // 先找到一个淘汰的页面，这个函数并没有真正淘汰，只是选择了一个页面
            std::pair<page_id_t , page_id_t> res = node_->getBufferPoolByIndex(table_id)->replace_page(page_id , frame_id , try_cnt , try_begin_evict);
            page_id_t replaced_page_id = res.first;

            // IR Recovery: 并发线程已将该页面插入缓冲区，直接返回
            if (replaced_page_id == INVALID_PAGE_ID && res.second == INVALID_PAGE_ID) {
                Page* existing = node_->getBufferPoolByIndex(table_id)->try_fetch_page(page_id);
                if (existing != nullptr) {
                    memcpy(existing->get_data(), data, PAGE_SIZE);
                    return existing;
                }
                // 奇怪的边界情况：页面又被淘汰了，重试
                continue;
            }

            assert(frame_id >= 0);
            assert(replaced_page_id != INVALID_PAGE_ID);
            LRLocalPageLock *lr_local_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(replaced_page_id);
            if (res.second == INVALID_PAGE_ID){
                lr_local_lock->UnlockMtx();
                lr_local_lock->EndEvict();
                continue;
            }

            int unlock_remote = lr_local_lock->getUnlockType();
            // 如果 unlock_remote = 0，代表页面已经被远程释放了，例如 Pending 释放
            // 这种情况下页面是不可控的，直接跳过
            if (unlock_remote == 0){
                lr_local_lock->UnlockMtx();
                lr_local_lock->EndEvict();
                continue;
            }

            // 读锁和写锁都要放
            {
                /*
                    这里把页面写回到磁盘，至于为啥是先写回到磁盘，再去远程解锁呢，难道不怕远程不允许解锁吗？
                    有两个方面的考虑：
                    1. 有一个边界条件，如果先解锁远程，再写回到磁盘，远程解锁之后，假设此时没有节点持有页面所有权了，然后另外一个
                       节点又去申请了页面所有权，远程通知它去存储拿，但是其实页面还没刷到存储中，节点就读取到了错误的数据
                    2. 测试了一下，远程不同意的概率是很低的，几万分之一(缓冲区不是很小的时候)，因此就算先刷下去也没关系，即使远程解锁失败了，对性能的影响也不是很大 
                */
                // LOG(INFO) << "Flush To Disk Because It Might be replaced , table_id = " << table_id << " page_id = " << replaced_page_id;
                if (!need_to_record){
                    rpc_flush_page_to_storage(table_id , replaced_page_id);
                }else {
                    // std::cout << "Table ID = " << table_id << " Replace page = " << replaced_page_id << " Flush Log To Disk\n";
                    // 这里需要把日志给刷下去
                    // 只有数据表才需要检查 LSN (table_id < 10000)
                    if (table_id < 10000) {
                        Page *page = node_->getBufferPoolByIndex(table_id)->fetch_page(replaced_page_id);
                        wait_log_flush(page);
                    }
                }
            }

            auto *request = new page_table_service::BufferReleaseUnlockRequest();
            Page *page = node_->getBufferPoolByIndex(table_id)->fetch_page(replaced_page_id);
            if (table_id < 10000) {
                RmPageHdr *page_hdr = (RmPageHdr*)page->get_data();
                request->set_lsn(page_hdr->LLSN_);
            } else {
                request->set_lsn(0);
            }
            
            // LOG(INFO) << "BufferRelease Unlock , table_id = " << table_id << " page_id = " << replaced_page_id << " lsn = " << request->lsn();
            lr_local_lock->UnlockMtx();

            auto *response = new page_table_service::BufferReleaseUnlockResponse();
            auto *pid = new page_table_service::PageID();
            pid->set_page_no(replaced_page_id);
            pid->set_table_id(table_id);
            request->set_allocated_page_id(pid);
            request->set_node_id(node_->node_id);

            // 这里需要拿到页面的 LLSN，此时页面一定在缓冲区里，并且不会被淘汰，直接去拿就行
            

            node_id_t page_belong_node = get_recovery_node_id(table_id , replaced_page_id);
            if (page_belong_node == node_->node_id){
                this->page_table_service_impl_->BufferReleaseUnlock_LocalCall(request , response);
            }else {
                brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
                page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
                brpc::Controller cntl;
                pagetable_stub.BufferReleaseUnlock(&cntl , request , response , NULL);
                if (cntl.Failed()){
                    LOG(WARNING) << "BufferReleaseUnlock RPC failed for page " << replaced_page_id << ": " << cntl.ErrorText();
                    // 不崩溃 - 视为远程拒绝释放，尝试淘汰其他页面
                    lr_local_lock->EndEvict();
                    delete response;
                    delete request;
                    continue;
                }
            }
            
            if (!response->agree()){
                // 远程不允许释放，那我就换一个页面淘汰
                lr_local_lock->EndEvict();

                // LOG(INFO) << "Try To Evict A Page , But Remote Refuse , table_id = " << table_id << " page_id = " << replaced_page_id; 

                delete response;
                delete request;
                continue;
            }

            delete response;
            delete request;

            // LOG(INFO) << "Evicting a page success , table_id = " << table_id << " page_id = " << page_id << " replaced table_id = " << replaced_page_id;

            page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(
                table_id,
                page_id ,
                frame_id ,
                true ,
                replaced_page_id ,
                data
            );

            int lock_type1 = lr_local_lock->UnlockAny();
            if (lock_type1){
                lr_local_lock->UnlockRemoteOK();
            }
            node_->evict_page_cnt++;
            lr_local_lock->EndEvict();
            return page;
        }
    }

    // eager 模式下，把数据放到缓冲区里面，不需要通知远程
    Page *put_page_into_buffer_eager(table_id_t table_id , page_id_t page_id , const void *data){
        bool is_from_lru = false;
        frame_id_t frame_id = -1;
        if (checkIfDirectlyUpdate(table_id , page_id , data)){
            return node_->fetch_page(table_id , page_id);
        }
        Page *page = checkIfDirectlyPutInBuffer(table_id , page_id , data);
        if (page != nullptr){
            return page;
        }

        auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            return this->node_->eager_local_page_lock_tables[table_id]->GetLock(victim_page_id)->tryBeginEvict();
        });

        int try_cnt = -1;
        while(true){
            try_cnt++;
            auto [replaced_page_id , later_page_id] = node_->getBufferPoolByIndex(table_id)->replace_page(page_id , frame_id , try_cnt , try_begin_evict);
            assert(frame_id >= 0);
            assert(replaced_page_id != INVALID_PAGE_ID);
            ERLocalPageLock *local_lock = node_->eager_local_page_lock_tables[table_id]->GetLock(replaced_page_id);
            if (later_page_id == INVALID_PAGE_ID){
                local_lock->EndEvict();
                continue;
            }

            rpc_flush_page_to_storage(table_id , replaced_page_id);
            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(table_id , page_id , frame_id , true , replaced_page_id , data);
            node_->evict_page_cnt++;
            local_lock->EndEvict();
            return page;
        }
        return page;
    }

    Page *put_page_into_buffer_ts(table_id_t table_id , page_id_t page_id , const void *data){
        bool is_from_lru = false;
        frame_id_t frame_id = -1;
        if (checkIfDirectlyUpdate(table_id , page_id , data)){
            return node_->fetch_page(table_id , page_id);
        }
        
        Page *page = checkIfDirectlyPutInBuffer(table_id , page_id , data);
        if (page != nullptr){
            return page;
        }

        auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            return this->node_->local_page_lock_tables[table_id]->GetLock(victim_page_id)->TryBeginEvict();
        });

        int try_cnt = -1;
        while (true){
            try_cnt++;
            std::pair<page_id_t , page_id_t> res = node_->getBufferPoolByIndex(table_id)->replace_page(page_id , frame_id , try_cnt , try_begin_evict);
            page_id_t replaced_page_id = res.first;
            assert(frame_id >= 0);
            assert(replaced_page_id != INVALID_PAGE_ID);
            LocalPageLock *local_lock = node_->local_page_lock_tables[table_id]->GetLock(replaced_page_id);
            if (res.second == INVALID_PAGE_ID){
                local_lock->EndEvict();
                continue;
            }

            rpc_flush_page_to_storage(table_id , replaced_page_id);
            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(table_id , page_id , frame_id , true , replaced_page_id , data);
            // 页面已经被淘汰了，所有权自然不在我这里了
            local_lock->SetDirty(false);
            {
                if (SYSTEM_MODE == 12){
                    std::lock_guard<std::mutex> lk(node_->switch_mtx);
                    node_->set_page_dirty(table_id , page_id , false);
                } else {
                    std::lock_guard<std::mutex> lk(node_->switch_mtx_hot);
                    node_->set_page_dirty_hot(table_id , page_id , false);
                }
                
            }
            

            node_->evict_page_cnt++;
            local_lock->EndEvict();
            return page;
        }
        return nullptr;
    }

    Page *put_page_into_buffer_2pc(table_id_t table_id , page_id_t page_id , const void *data){
        bool is_from_lru = false;
        frame_id_t frame_id = INVALID_FRAME_ID;
        Page *page = checkIfDirectlyPutInBuffer(table_id , page_id , data);
        if (page != nullptr){
            return page;
        }

        auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            return this->node_->local_page_lock_tables[table_id]->GetLock(victim_page_id)->TryBeginEvict();
        });

        int try_cnt = -1;
        while(true){
            try_cnt++;
            std::pair<page_id_t , page_id_t> res = node_->getBufferPoolByIndex(table_id)->replace_page(page_id , frame_id , try_cnt , try_begin_evict);
            page_id_t replaced_page_id = res.first;
            assert(frame_id >= 0);
            assert(replaced_page_id != INVALID_PAGE_ID);

            LocalPageLock *local_lock = node_->local_page_lock_tables[table_id]->GetLock(replaced_page_id);
            if (res.second == INVALID_PAGE_ID){
                local_lock->EndEvict();
                continue;
            }

            rpc_flush_page_to_storage(table_id , replaced_page_id);
            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(table_id , page_id , frame_id , true , replaced_page_id , data);
            node_->evict_page_cnt++;
            local_lock->EndEvict();
            return page;
        }

        assert(false);
        return nullptr;
    }

    page_id_t rpc_create_page(table_id_t table_id){
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl;
        storage_service::CreatePageRequest req;
        storage_service::CreatePageResponse resp;

        // SQL 模式下，通过 db_meta 获取表名字
        if (WORKLOAD_MODE == 4){
            // B+ 树存在 10000 - 20000，FSM 存在 20000 到 30000
            int tab_id = 0;
            if (table_id < 10000){
                tab_id = table_id;
            }else if (table_id < 20000){
                tab_id = table_id - 10000;
            }else if (table_id < 30000){
                tab_id = table_id - 20000;
            }else {
                assert(false);
            }

            std::string tab_name = getTableNameFromTableID(tab_id);
            assert(tab_name != "");

            if (table_id >= 10000 && table_id < 20000){
                tab_name += "_bl";
            }else if (table_id >= 20000 && table_id < 30000){
                tab_name += "_fsm";
            }

            req.set_table_name(tab_name);
        }else{
            req.set_table_name(table_name_meta[table_id]);
        }
        
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
        req.set_table_name(table_name_meta[table_id]);

        storage_stub.DeletePage(&cntl , &req , &resp , NULL);
        if (cntl.Failed()) {
            LOG(ERROR) << "Create Page Error";
            assert(false);
        }

        assert(resp.successs());
    }

    // ****************** lazy release end *********************

    // ****************** for 2PC *********************
    Page* local_fetch_s_page(table_id_t table_id, page_id_t page_id);

    Page* local_fetch_x_page(table_id_t table_id, page_id_t page_id);

    void local_release_s_page(table_id_t table_id, page_id_t page_id);

    void local_release_x_page(table_id_t table_id, page_id_t page_id);

    void Get_2pc_Remote_data(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data);

    void Write_2pc_Remote_data(node_id_t node_id, table_id_t table_id, Rid rid, char* data); 

    void Write_2pc_Local_data(node_id_t node_id, table_id_t table_id, Rid rid, char* data);

    void Get_2pc_Local_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data , itemkey_t key);

    void Get_2pc_Remote_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data);

    bool Prepare_2pc(std::unordered_set<node_id_t> node_id, uint64_t txn_id);

    int Commit_2pc(std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map, uint64_t txn_id, bool sync = true);

    void Abort_2pc(std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map, uint64_t txn_id, bool sync = true);

    static void PrepareRPCDone(twopc_service::PrepareResponse* response, brpc::Controller* cntl);

    static void AbortRPCDone(twopc_service::AbortResponse* response, brpc::Controller* cntl);

    static void CommitRPCDone(twopc_service::CommitResponse* response, brpc::Controller* cntl, int* add_latency);

    // ****************** 2PC end *********************

    // ****************** for single *********************
    Page* single_fetch_s_page(table_id_t table_id, page_id_t page_id);
    Page* single_fetch_x_page(table_id_t table_id, page_id_t page_id);
    void single_release_s_page(table_id_t table_id, page_id_t page_id);
    void single_release_x_page(table_id_t table_id, page_id_t page_id);
    // ****************** for single end *********************



    std::vector<std::string> table_name_meta;
    void InitTableNameMeta();
    std::string rpc_fetch_page_from_storage(table_id_t table_id, page_id_t page_id , bool need_to_record);
    std::string rpc_fetch_page_from_storage_with_lsn(table_id_t table_id , page_id_t page_id , LLSN page_lsn , bool need_to_record);

    inline uint64_t get_partitioned_size(table_id_t table_id){
        return node_->meta_manager_->GetPartitionSizePerTable(table_id);
    }
    
    inline bool is_partitioned_page(table_id_t table_id , page_id_t page_id , node_id_t node_id){
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        int belong_par = ((page_id - 1) / partition_size) % ComputeNodeCount;
        return (node_id == belong_par);
    }

    inline uint64_t make_page_key(table_id_t table_id, page_id_t page_id) const {
        return (static_cast<uint64_t>(static_cast<uint32_t>(table_id)) << 32) | static_cast<uint32_t>(page_id);
    }

    void InitHotPages(){
        std::string config_filepath;
        uint32_t tot_account;
        uint32_t hot_account;

        if (WORKLOAD_MODE == 0){
            config_filepath = "../../config/smallbank_config.json";
            auto json_config = JsonConfig::load_file(config_filepath);
            auto conf = json_config.get("smallbank");
            tot_account = conf.get("num_accounts").get_uint64();
            hot_account = conf.get("num_hot_accounts").get_uint64();
        }else if (WORKLOAD_MODE == 2){
            config_filepath = "../../config/ycsb_config.json";
            auto json_config = JsonConfig::load_file(config_filepath);
            auto conf = json_config.get("ycsb");
            tot_account = conf.get("num_record").get_uint64();
            hot_account = conf.get("num_hot_record").get_uint64();
        }else {
            assert(false);
        }
        
        double hot_rate = (double)hot_account / (double)tot_account;

        hot_page_set.clear();
        auto table_size = node_->meta_manager_->GetTableNum();
        std::cout << "Init Hot Page , Table Num = " << table_size << "\n";

        for (int node_id = 0 ; node_id < ComputeNodeCount ; node_id++){
            for (int table_id = 0 ; table_id < table_size ; table_id++){
                int partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
                auto page_num_node_i = node_->meta_manager_->GetPageNumPerNode(node_id , table_id , ComputeNodeCount);
                uint64_t hot_len = static_cast<uint64_t>(page_num_node_i * hot_rate);
                assert(hot_len < page_num_node_i);

                for (int i = 0 ; i < hot_len ; i++){
                    page_id_t page_id = i;
                    page_id = (i / partition_size) * (ComputeNodeCount * partition_size) 
                        + (node_id * partition_size)
                        + i % partition_size
                        + 1;
                    // // LOG(INFO) << "Hot Page ID = " << page_id << " Partition Size = " 
                    //     << partition_size << " Page Num Node " << node_id << " = " << page_num_node_i
                    //     << " hot Len = " << hot_len
                    //     << " hot rate = " << hot_rate;
                    hot_page_set.insert(make_page_key(table_id , page_id));
                }
            }
        }
    }

    inline bool is_hot_page(table_id_t table_id, page_id_t page_id){
        return hot_page_set.find(make_page_key(table_id, page_id)) != hot_page_set.end();
    }

    // 获取到 page_id 所在的分区对应的节点
    inline node_id_t get_node_id_by_page_id(table_id_t table_id , page_id_t page_id){
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        assert(partition_size != 0);
        int node_id = ((page_id - 1) / partition_size) % ComputeNodeCount;
        assert(node_id < ComputeNodeCount);
        return node_id;
    }

    /*
        页面转移走，或者页面被淘汰之前，需要等待这个页面的日志落盘
    */
    void wait_log_flush(Page *page){
        if (!page->is_dirty()){
            // no_need_wait_cnt++;
            return ;
        }


        RmPageHdr *hdr = reinterpret_cast<RmPageHdr*>(page->get_data());
        // LOG(INFO) << "Transfer To Other , Need Wait Log Flush , table_id = " << page->get_page_id().table_id << " page_id = " << page->get_page_id().page_no << " wait lsn = " << hdr->LLSN_;
        std::unique_lock<std::mutex> lock(persist_lsn_mtx);
        // 添加超时避免永久阻塞 brpc worker 线程
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while(hdr->LLSN_ > persist_lsn){
            if (persist_lsn_cond.wait_until(lock, deadline) == std::cv_status::timeout) {
                LOG(WARNING) << "wait_log_flush timeout for page table=" << page->get_page_id().table_id
                            << " page=" << page->get_page_id().page_no << " lsn=" << hdr->LLSN_ << " persist_lsn=" << persist_lsn;
                return;
            }
        }

        page->set_dirty(false);
        // LOG(INFO) << "Wait Log Flush Over , Now Persist Lsn = " << persist_lsn << " table_id = " << page->get_page_id().table_id << " page_id = " << page->get_page_id().page_no << " page lsn = " << hdr->LLSN_;
    }

    void wait_log_flush(LLSN require_lsn){
        std::unique_lock<std::mutex> lock(persist_lsn_mtx);
        while(require_lsn > persist_lsn){
            persist_lsn_cond.wait(lock);
        }
    }

    // P3 修复：触发一次紧急日志刷新（唤醒后台刷新线程立即执行，而非等待 10ms/100ms 周期）
    void RequestUrgentLogFlush(){
        {
            std::lock_guard<std::mutex> lk(log_flush_cv_mtx_);
            log_flush_urgent_ = true;
        }
        log_flush_cv_.notify_all();
    }

    // 获取当前成功完成的日志刷新轮次
    uint64_t GetFlushRound(){
        std::lock_guard<std::mutex> lk(persist_lsn_mtx);
        return flush_round_;
    }

    /**
     * @brief P3 修复：等待日志持久化（带超时与主动重试）
     *
     * 相比 wait_log_flush 的两点增强：
     * 1. 等待前先触发 urgent 刷新，提交延迟不再受后台线程 10ms/100ms 周期制约；
     * 2. 每 500ms 超时唤醒并主动重试刷新，30s 硬超时后返回 false，
     *    避免存储层不可达时事务线程永久阻塞。
     *
     * @param require_lsn 本事务最大数据日志的 LSN
     * @param need_round  BatchEnd 日志入队后观察到的刷新轮次。
     *                    需等待 flush_round_ > need_round：仅等 require_lsn 不够——
     *                    数据日志可能先于 BatchEnd 被刷出，若此时崩溃，
     *                    存储层缺少 BatchEnd 会导致已提交事务被 UndoForFailedNode 误撤销。
     * @return true 已确认持久化；false 硬超时（日志仍留在队列中，由重试机制继续发送）
     */
    bool wait_log_flush_v2(LLSN require_lsn, uint64_t need_round){
        const auto hard_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        // 缩短等待：先请求一次紧急刷新
        RequestUrgentLogFlush();
        std::unique_lock<std::mutex> lock(persist_lsn_mtx);
        while (require_lsn > persist_lsn || flush_round_ <= need_round){
            if (persist_lsn_cond.wait_for(lock, std::chrono::milliseconds(500)) == std::cv_status::timeout){
                if (std::chrono::steady_clock::now() >= hard_deadline){
                    LOG(ERROR) << "wait_log_flush_v2 timeout: require_lsn=" << require_lsn
                               << " persist_lsn=" << persist_lsn
                               << " flush_round=" << flush_round_
                               << " need_round=" << need_round;
                    return false;
                }
                // 长时间未推进，主动再触发一次刷新重试（LogFlush 失败会重入队）
                lock.unlock();
                RequestUrgentLogFlush();
                lock.lock();
            }
        }
        return true;
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

    // 从远程取数据页
    std::string UpdatePageFromRemoteCompute(table_id_t table_id, page_id_t page_id, node_id_t node_id , bool need_to_record);

    // 获取与其他计算节点通信的channel
    inline brpc::Channel* get_pagetable_channel(){ return &node_->page_table_channel; }
    inline brpc::Channel* get_storage_channel(){ return &node_->storage_channel; }
    inline brpc::Channel* get_compute_channel(){ return nodes_channel; }

    inline ComputeNode* get_node(){ return node_; }

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

    bool addTableUse(const std::string &tab_name){
        tab_meta_mtx.lock();
        if (is_dropingTable){
            tab_meta_mtx.unlock();
            return false;
        }
        if (table_use.find(tab_name) == table_use.end()){
            table_use[tab_name] = 1;
        }else {
            table_use[tab_name]++;
        }
        tab_meta_mtx.unlock();
        return true;
    }
    void decreaseTableUse(const std::string &tab_name){
        std::lock_guard<std::mutex> lk(tab_meta_mtx);
        assert(table_use.find(tab_name) != table_use.end());
        assert(table_use[tab_name] > 0);
        table_use[tab_name]--;
    }
    bool tryDropTable(const std::string &tab_name){
        if (!node_->db_meta.is_table(tab_name)){
            throw std::logic_error("Table Not Found");
        }

        {
            std::lock_guard<std::mutex> lk(tab_meta_mtx);
            if (is_dropingTable){
                return false;
            }
            if (is_creatingTable){
                return false;
            }
            is_dropingTable = true;
        }

        while(true){
            tab_meta_mtx.lock();
            assert(!is_creatingTable);
            assert(is_dropingTable);

            // 等事务处理完这个表，我再删除
            if (table_use[tab_name] > 0){
                tab_meta_mtx.unlock();
                usleep(30);
                continue;
            }else {
                tab_meta_mtx.unlock();
                break;
            }
        }

        return true;
    }
    
    void NotifyDropTableOver(){
        std::lock_guard<std::mutex> lk(tab_meta_mtx);
        assert(is_dropingTable);
        is_dropingTable = false;
    }
    bool tryCreateTable(){
        std::lock_guard<std::mutex> lk(tab_meta_mtx);
        // 正在删除表，不允许建新表
        if (is_dropingTable){
            return false;
        }
        is_creatingTable = true;
        return true;
    }
    void NotifyCreateTableSuccess(){
        std::lock_guard<std::mutex> lk(tab_meta_mtx);
        assert(is_creatingTable);
        is_creatingTable = false;
    }


    /**
     * @brief 批量刷新日志到存储层
     * 
     * 此方法实现了后台日志刷新机制，将共享日志队列中的所有日志批量持久化到存储层。
     * 
     * 工作流程：
     * 1. 从共享队列中批量取出所有待持久化的日志
     * 2. 序列化日志并通过 RPC 发送到存储层
     * 3. 更新 persist_lsn（已持久化的最大 LSN）
     * 4. 释放已持久化日志占用的内存
     * 
     * 线程安全：使用互斥锁保护共享日志队列的访问
     * 性能优化：使用 swap 减少锁持有时间
     */
    void LogFlush(){
        // C2 修复：LogFlush 会被后台刷新线程、事务提交路径、恢复
        // Phase2/3（server.h:2261/2383）并发调用。若不加串行化，两个
        // 并发 flush 各自 swap 走一部分队列，RPC 到达存储层的顺序可能
        // 与 LSN 顺序交错，导致同一页面的日志在日志文件中乱序。
        // swap + RPC + 失败回队必须互斥，保证批间顺序与 LSN 顺序一致。
        std::lock_guard<std::mutex> flush_lk(log_flush_mtx_);

        // 批量取出所有日志（在锁作用域内）
        std::vector<LogRecord*> batch_logs;
        uint64_t batch_end_sequence = 0;
        {
            std::lock_guard<std::mutex> lk(log_mtx);

            // 快速检查：如果没有日志，直接返回
            if (log_records.empty()) {
                return;
            }

            // 使用 swap 快速转移所有权，减少锁持有时间
            batch_logs.swap(log_records);
            batch_end_sequence = log_enqueue_sequence_;
        }  // 锁在这里自动释放

        // 1. 将 batch_logs 序列化成字符串
        size_t total_size = 0;
        LLSN max_lsn = 0;  // 记录本批次中最大的 LSN

        for (auto* log : batch_logs) {
            total_size += log->log_tot_len_;

            if (log->lsn_ > max_lsn) {
                max_lsn = log->lsn_;
            }
        }

        std::string serialized_logs;
        // std::stringstream ss;
        if (total_size > 0) {
            serialized_logs.resize(total_size);
            // C++11 保证 string 内存连续，可以直接写入
            char* dest_ptr = &serialized_logs[0];

            // 第二遍遍历：直接序列化
            for (auto* log : batch_logs) {
                // ss << "\nlog lsn = " << log->lsn_ << " log prev lsn = " << log->prev_lsn_ << "\n";
                log->serialize(dest_ptr);
                dest_ptr += log->log_tot_len_;
            }
        }

        // 2. 调用存储层接口批量写入日志
        bool flush_ok = true;
        if (!serialized_logs.empty()) {
            storage_service::StorageService_Stub storage_stub(get_storage_channel());
            brpc::Controller cntl;
            storage_service::LogWriteRequest request;
            storage_service::LogWriteResponse response;

            request.set_log(std::move(serialized_logs));
            request.set_urgent(0);  // 后台刷新，非紧急

            storage_stub.LogWrite(&cntl, &request, &response, NULL);

            if (cntl.Failed()) {
                LOG(ERROR) << "Batch LogFlush failed: " << cntl.ErrorText();
                flush_ok = false;
            }
        }

        if (flush_ok) {
            // 3. 更新 persist_lsn（已持久化的最大 LSN）和 flush_round_（成功刷新轮次）
            std::lock_guard<std::mutex> lk_lsn(persist_lsn_mtx);
            if (max_lsn > persist_lsn) {
                persist_lsn = max_lsn;
            }
            log_acked_sequence_ = batch_end_sequence;
            flush_round_++;
            persist_lsn_cond.notify_all();

            // 4. 释放已持久化的日志内存
            for (auto* log : batch_logs) {
                delete log;
            }
        } else {
            // P3 修复：RPC 失败时日志绝不能丢弃（原实现直接 delete 会造成已提交事务日志
            // 永久丢失），也绝不能推进 persist_lsn（否则 wait_log_flush 会误判已完成）。
            // 将本批日志重新插回队列头部，由后台线程/urgent 机制重试发送。
            {
                std::lock_guard<std::mutex> lk(log_mtx);
                log_records.insert(log_records.begin(), batch_logs.begin(), batch_logs.end());
            }
            LOG(WARNING) << "LogFlush failed, " << batch_logs.size()
                         << " log records re-queued for retry";
        }
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
    uint64_t AddToLog(LogRecord *log){
        std::lock_guard<std::mutex> lock(log_mtx);
        log_records.emplace_back(log);
        return ++log_enqueue_sequence_;
    }

    void AddToLogNoBlock(LogRecord *log){
        log_records.emplace_back(log);
        ++log_enqueue_sequence_;
        log_mtx.unlock();
    }

    bool WaitLogReceipt(uint64_t ticket, int timeout_ms = 30000){
        assert(ticket > 0);
        RequestUrgentLogFlush();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        std::unique_lock<std::mutex> lock(persist_lsn_mtx);
        while (log_acked_sequence_ < ticket) {
            if (persist_lsn_cond.wait_until(lock, deadline) == std::cv_status::timeout) {
                return log_acked_sequence_ >= ticket;
            }
        }
        return true;
    }
    
    /**
     * @brief 获取当前已持久化的最大 LSN
     * 
     * @return LLSN 已持久化的最大日志序列号
     */
    LLSN GetPersistedLSN() const {
        std::lock_guard<std::mutex> lk(persist_lsn_mtx);
        return persist_lsn;
    }
    
    /**
     * @brief 检查是否需要刷新日志（基于数量阈值）
     * 
     * @return bool 如果日志数量达到阈值返回 true
     */
    bool ShouldFlushLog() const {
        std::lock_guard<std::mutex> lk(log_mtx);
        return log_records.size() >= LOG_FLUSH_THRESHOLD;
    }
    
    /**
     * @brief 服务关闭：确保所有日志已刷新到存储层
     * 
     * 在服务器关闭前调用此方法，确保所有待持久化的日志都已写入存储层。
     */
    JsonConfig DrainWorkloadLogs() {
        uint64_t ticket;
        { std::lock_guard<std::mutex> lock(log_mtx); ticket = log_enqueue_sequence_; }
        if (ticket && !WaitLogReceipt(ticket)) throw std::runtime_error("workload log drain deadline");
        std::lock_guard<std::mutex> flush_lock(log_flush_mtx_);
        std::lock_guard<std::mutex> queue_lock(log_mtx);
        std::lock_guard<std::mutex> ack_lock(persist_lsn_mtx);
        if (!log_records.empty() || log_enqueue_sequence_ != ticket || log_acked_sequence_ != ticket)
            throw std::runtime_error("log producer changed after workload drain");
        auto result = JsonConfig::empty_dict("drain");
        result.insert_uint64("enqueued_ticket", ticket);
        result.insert_uint64("acked_ticket", log_acked_sequence_);
        result.insert_uint64("persist_lsn", persist_lsn);
        result.insert_uint64("queued_records", 0);
        result.insert_bool("flush_in_flight", false);
        return result;
    }

    void Shutdown() {
        DrainWorkloadLogs();
        log_flush_running.store(false);
        RequestUrgentLogFlush();
    }


public:
    std::vector<BLinkIndexHandle*> bl_indexes;
    std::vector<SecFSM*> fsm_trees;
    
    // 后台日志刷新线程控制（需要外部访问，故放在 public）
    std::atomic<bool> log_flush_running{true};  // 控制后台线程是否继续运行

    // P3 修复：紧急刷新唤醒（提交路径触发，缩短持久化等待延迟）
    // 后台刷新线程（handler.cc）通过 cv 等待 urgent 标志，事务提交路径 RequestUrgentLogFlush 唤醒
    std::mutex log_flush_cv_mtx_;
    std::condition_variable log_flush_cv_;
    bool log_flush_urgent_ = false;             // 由 log_flush_cv_mtx_ 保护
    std::mutex log_flush_mtx_;                  // C2：串行化 LogFlush，保证日志批间顺序与 LSN 一致

    // 吞吐量监控：全局事务提交计数器（所有工作线程共享）
    std::atomic<uint64_t> global_committed_tx_total{0};

    // 恢复进行中查询（request_workload 据此在恢复窗口拒绝新事务 admission，
    // 已 admission 事务不受影响——其终局由 tainted/abort 机制处理）
    bool IsRecoveryInProgress() const {
        return recovery_in_progress_.load(std::memory_order_acquire);
    }

    // 本进程是否完成过至少一次故障恢复。IsRecoveryInProgress() 无法区分
    // 「从未发生恢复」与「恢复已完成」（两者均为 false），但恢复残留自愈
    // （ClearFetchQuarantineAfterRecovery）与存储兜底授权（AuthorizeStorage）
    // 只应发生在后者——从未恢复的进程里隔离/防御语义必须保持
    // （recovery_e0_test 的 ordinary-retry / missing-replica 契约）。
    bool HasCompletedRecovery() const {
        return recovery_completed_once_.load(std::memory_order_acquire);
    }

    // 第 15 层（R2 live fault，fault-0524）：恢复出口记录死亡节点，
    // 供后续首个事务的 workload key admission 顺带向 remote 发送 evict
    //（清死亡节点残留的 workload key 锁——victim 被 SIGKILL 后无人释放，
    // remote owner 表原本无死节点回收，导致后续该 key 全部确定性
    // KEY_CONFLICT）。恢复线程无 remote channel，由持有 channel 的工作
    // 线程在 AcquireWorkloadKeys 时消费。
    std::atomic<int32_t> pending_workload_evict_node_{-1};
    int32_t TakePendingWorkloadEvictNode() {
        return pending_workload_evict_node_.exchange(-1, std::memory_order_acq_rel);
    }
    void RestorePendingWorkloadEvictNode(int32_t node) {
        int32_t expected = -1;
        pending_workload_evict_node_.compare_exchange_strong(expected, node,
                                                             std::memory_order_acq_rel);
    }

    // 故障恢复纪元：每次故障恢复递增，事务可对比检测恢复是否发生
    std::atomic<uint64_t> recovery_epoch{0};

    // R2c C1: 统一恢复影响目录（页状态 × 恢复代 × 版本）。分类账本，
    // 强制隔离机构仍是 GPLM IR 锁；两机构在 Phase 1/4 与汇报路径同步更新
    recovery_catalog::RecoveryPageCatalog recovery_catalog_;

    // 细粒度恢复：标记是否正在进行恢复
    std::atomic<bool> recovery_in_progress_{false};

    // 一次性标志：本进程完成过至少一次故障恢复（恢复成功出口置位，
    // 从不清除）。与 recovery_in_progress_ 的区别见 HasCompletedRecovery。
    std::atomic<bool> recovery_completed_once_{false};

    // 判断一个页面是否受故障恢复影响
    // R2c C3：判定升级为「C1 目录 AFFECTED ∪ 旧判定（原管理者故障）」。
    // 目录覆盖 A 管理（接管）、A 持 X、文件头页（本节点视角）；旧判定
    // 兼容他节点接管页——本地目录对它们是 UNAFFECTED 默认，但远程访问
    // 会被接管者的 IR 锁挡住，本地提前 taint 等价且省一次远程往返。
    // 碰到受影响页的事务按 32.2.1 回滚-等待-重试：taint abort 返回
    // RECOVERY_AFFECTED，由驱动以新事务有界重试。
    bool IsPageAffectedByRecovery(table_id_t table_id, page_id_t page_id) {
        if (!recovery_in_progress_.load(std::memory_order_acquire)) return false;
        if (recovery_catalog_.IsIsolated((uint64_t)table_id, (uint64_t)page_id)) {
            return true;
        }
        node_id_t original = get_node_id_by_page_id(table_id, page_id);
        return IsNodeFailed(original);
    }

    // 故障节点集合
    std::mutex failed_nodes_mutex_;
    std::unordered_set<node_id_t> failed_nodes_;

    bool IsNodeFailed(node_id_t node_id) {
        std::lock_guard<std::mutex> lk(failed_nodes_mutex_);
        return failed_nodes_.count(node_id) > 0;
    }

    // 故障恢复哈希：将故障节点管理的页面平均分配给所有存活节点
    node_id_t get_recovery_node_id(table_id_t table_id, page_id_t page_id) {
        node_id_t original = get_node_id_by_page_id(table_id, page_id);
        if (!IsNodeFailed(original)) return original;
        // 构建存活节点列表（确定性顺序），将故障节点的页面平均分配
        std::vector<node_id_t> surviving;
        surviving.reserve(ComputeNodeCount);
        for (int i = 0; i < ComputeNodeCount; i++) {
            if (!IsNodeFailed(i)) {
                surviving.push_back(i);
            }
        }
        if (surviving.empty()) {
            LOG(FATAL) << "No surviving compute nodes!";
            return 0;
        }
        return surviving[page_id % surviving.size()];
    }

    void MarkNodeFailed(node_id_t failed_node_id) {
        {
            std::lock_guard<std::mutex> lk(failed_nodes_mutex_);
            if (failed_nodes_.count(failed_node_id) > 0) return;  // 已处理
            failed_nodes_.insert(failed_node_id);
        }

        // IR 重试（smoke-020 实证）：恢复主流程自身会取页（Phase 2 扫描
        // LogFlush 后的本地 LPLM/Phase 4 日志回放），恢复窗口内与其他
        // 取页/IR 并发时可撞上 GPLM S→X 升级互等，被 L16 45s 授权墙钟
        // 一并 abort。原实现一旦中断：recovery_in_progress_ 永远 true
        //（admission 永久拒绝新事务），且 LPLM/GPLM 泄漏残留依赖"恢复
        // 完成"类自愈（HasCompletedRecovery）清理——全部永不触发，节点
        // 永久瘫痪（实测 B/C 全停摆、victim 存储分片无人接管）。恢复
        // 主体必须重试到成功：Phase 1a CleanFailedNodeAndSetIRLock 与
        // 1b 接管 Reset+SetIRLock 幂等（重入无 victim 锁可清/重置同状
        // 态）；SetIRScanExpected barrier 去重并缓存早到通知；Scan 上
        // 报与 SetRecoveryAbort 幂等；Phase 4 存储侧按 LSN 分析幂等。
        for (int ir_attempt = 1; ; ir_attempt++) {
        try {

        // P0 修复（admission 屏障时序）：恢复标志在检测到故障的第一时间
        // 置位——必须在 Phase 1 GPLM 清理/接管开始之前，让 request_workload
        // 的 admission 检查尽早拒绝新事务，缩小"读到 false 后登记 active"
        // 与恢复开始的竞态窗口。原实现直到 Phase 2 扫描后才置位，Phase 1
        // 期间的新事务会带着旧 GPLM 视图执行。
        recovery_in_progress_.store(true, std::memory_order_release);

        // 第 9 层缺陷修复（live-early7）：本地 key2leaf 缓存记录的是故障前
        // 世界的页布局；恢复期间存储端 undo/replay 重排页空间后，同号页可
        // 能已不是叶子，旧条目会击穿 checkIfDirectlyGetPage 的 is_leaf 断
        // 言。缓存条目可能指向任何分区（跨分区 S 锁读），故在恢复起点对全
        // 部本地索引句柄整体作废。
        for (size_t t = 0; t < bl_indexes.size(); t++) {
            if (bl_indexes[t] != nullptr) bl_indexes[t]->ClearKey2LeafCache();
        }
        // 递增恢复纪元（保留用于兼容旧逻辑）
        recovery_epoch.fetch_add(1);
        // R2c C1: 统一恢复影响目录开启新代（fetch_add 后 load 即本故障的代数）。
        // 旧代条目/表扫描背书全部作废——旧代报告不能解隔离（契约测试
        // recovery_page_catalog_test 覆盖）
        uint64_t r2c_generation = recovery_epoch.load();
        recovery_catalog_.BeginGeneration(r2c_generation, failed_node_id);

        recovery_observation::Recorder::Get().SetEpoch(recovery_epoch.load() + 1);
        recovery_observation::Emit("failure_detected", -1, -1, failed_node_id);
        recovery_observation::Span recovery_span("compute_recovery");
        node_id_t my_id = node_->getNodeID();
        LOG(ERROR) << "[IR Recovery] Node " << my_id << " starting instance recovery for failed node " << failed_node_id;

        // 构建存活节点列表（与 get_recovery_node_id 保持一致）
        std::vector<node_id_t> surviving;
        surviving.reserve(ComputeNodeCount);
        for (int i = 0; i < ComputeNodeCount; i++) {
            if (!IsNodeFailed(i)) surviving.push_back(i);
        }

        // ==================== Phase 1: IR Lock + GPLM 清理/重分布 ====================
        int xlock_cleaned_pages = 0;
        int slock_cleaned_pages = 0;
        int redistributed_pages = 0;

        // 遍历所有表
        for (size_t t = 0; t < global_page_lock_table_list_->size(); t++) {
            GlobalLockTable* glt = global_page_lock_table_list_->at(t);
            GlobalValidTable* gvt = global_valid_table_list_->at(t);
            if (glt == nullptr || gvt == nullptr) continue;

            // Phase 1a: 清理本节点管理的 GPLM 中，故障节点持有的页面
            // X 锁页面上 IR 锁（最新数据可能丢失），S 锁页面仅清除 holder
            // R2c C1: 收集 X 页清单登记统一目录（AFFECTED/FAILED_NODE_X_HOLDER）；
            // S-only 页清理即完成、无残留状态，走表级 UNAFFECTED 默认，不登记
            std::vector<page_id_t> phase1a_x_pages;
            auto [x_cleaned, s_cleaned] = glt->CleanFailedNodeAndSetIRLock(failed_node_id, gvt, &phase1a_x_pages);
            for (page_id_t ip : phase1a_x_pages) {
                recovery_catalog_.MarkAffected(r2c_generation, t, ip,
                                                recovery_catalog::RAffectedReason::FAILED_NODE_X_HOLDER);
            }
            xlock_cleaned_pages += x_cleaned;
            slock_cleaned_pages += s_cleaned;

            // Phase 1b: 接管故障节点管理的页面（平均分配到所有存活节点）
            // R2c C1 修复（31.5 遗留审计项）：接管循环从 p=1 起步跳过 page 0，
            // 导致每表文件头页（RmFileHdr/空闲链表）从不被接管、从不隔离恢复。
            // 现纳入 p=0：page 0 视为受故障影响的公共页（与 Phase 3 无条件
            // SetRecoveryAbort 的既有语义对齐），由 surviving[0] 接管并上 IR。
            // 注：循环上界仍保持判定分区名义范围（partition_size × 节点数），
            // 与 Phase 3 的 ComputeNodeBufferPageSize 全范围不同——上界统一
            // 属行为变更，无实验依据，记录为审计结论不在本轮扩大。
            auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(t);
            if (partition_size == 0) continue;

            for (page_id_t p = 0; p < ComputeNodeBufferPageSize && p <= (page_id_t)(partition_size * ComputeNodeCount); p++) {
                // p==0 特判：文件头页恒视为受影响（避免 (p-1) 无符号下溢）
                node_id_t original_owner = (p == 0) ? failed_node_id
                                                    : ((p - 1) / partition_size) % ComputeNodeCount;
                if (original_owner != failed_node_id) continue;
                // 使用与 get_recovery_node_id 相同的算法确定新 owner
                node_id_t new_owner = surviving[p % surviving.size()];
                if (new_owner == my_id) {
                    // 本节点接管此页面：重置锁状态 + 上 IR 锁
                    LR_GlobalPageLock* gl = glt->LR_GetLock(p);
                    gl->mutexLock();
                    gl->Reset();
                    gl->SetIRLock();
                    gl->mutexUnlock();
                    gvt->GetValidInfo(p)->MarkOnluInStorage();
                    redistributed_pages++;
                    // R2c C1: 接管页登记统一目录（AFFECTED，待恢复验证）
                    recovery_catalog_.MarkAffected(
                        r2c_generation, t, p,
                        p == 0 ? recovery_catalog::RAffectedReason::FILE_HEADER
                               : recovery_catalog::RAffectedReason::FAILED_MANAGER);
                }
            }
            // R2c C1: 该表 Phase 1a+1b 扫描完成——表内未登记页获得
            // "已扫描无故障证据"背书（Classify 返回 UNAFFECTED）
            recovery_catalog_.MarkTableSwept(r2c_generation, t);
        }

        // 设置 IR 扫描期望值：所有存活节点（含自身的本地调用）。
        // P0 修复：按故障节点分桶 + survivor 名单（barrier 内部去重并缓存
        // 早到通知，见 remote_page_table_rpc.h）
        page_table_service_impl_->SetIRScanExpected(failed_node_id, surviving);

        LOG(INFO) << "[IR Recovery] Phase 1 complete: X-lock cleaned " << xlock_cleaned_pages
                  << " pages (IR locked), S-lock cleaned " << slock_cleaned_pages
                  << " pages (no IR lock), redistributed " << redistributed_pages << " pages to surviving nodes";

        // ==================== Phase 2: 先扫描 LPLM（在唤醒等待线程之前！）====================
        // 必须在 wakeup 之前扫描，因为 wakeup 会导致 LPLM 状态被重置
        RunIRRecoveryScan(failed_node_id, my_id);

        // ==================== Phase 3: 唤醒所有可能阻塞在 LPLM cv.wait 的线程 ====================
        // 恢复进行中标志已在 MarkNodeFailed 入口置位（admission 屏障，见上），
        // 此处仅唤醒等待线程；不再重复置位

        // 只唤醒那些等待故障节点相关页面的线程（原管理者是故障节点的页面）
        for (size_t t = 0; t < node_->lazy_local_page_lock_tables.size(); t++) {
            LRLocalPageLockTable* lplm = node_->lazy_local_page_lock_tables[t];
            if (lplm == nullptr) continue;
            auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(t);
            if (partition_size == 0) continue;
            for (page_id_t p = 0; p < ComputeNodeBufferPageSize; p++) {
                // 只对原管理者是故障节点的页面执行 SetRecoveryAbort
                node_id_t original_owner = ((p - 1) / partition_size) % ComputeNodeCount;
                if (p == 0 || original_owner == failed_node_id) {
                    LRLocalPageLock* lr = lplm->GetLock(p);
                    lr->SetRecoveryAbort();
                }
            }
        }
        LOG(INFO) << "[IR Recovery] Phase 3: woke up LPLM waiters for failed node " << failed_node_id << " pages";

        // ==================== Phase 4: 日志分析恢复（真正的第三阶段）====================
        // 等待 Phase 2 所有节点扫描完成，获取仍持有 IR 锁的页面列表
        // 然后向存储层发请求分析日志，确定哪些页面需要回放
        RunIRRecoveryPhase3(failed_node_id, my_id, surviving);

        // 成功走完主体：在 try 内直接 return（异常路径才进 catch）
        return;

        } catch (const std::exception& e) {
            // L16/防御等待/IR deadline 等中断恢复主体：保持
            // recovery_in_progress_=true（admission 继续拒绝新事务是正确
            // 的——尚未恢复完成），退避后重试。绝不带着未完成恢复放行。
            LOG(ERROR) << "[IR Recovery] attempt " << ir_attempt << " for failed node "
                       << failed_node_id << " aborted: " << e.what()
                       << (ir_attempt % 10 == 1 ? " — retrying" : " — retrying(suppressed log)");
            std::this_thread::sleep_for(std::chrono::seconds(2));
        } catch (...) {
            LOG(ERROR) << "[IR Recovery] attempt " << ir_attempt << " for failed node "
                       << failed_node_id << " aborted by unknown exception — retrying";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        }  // for：IR 重试直到成功
    }

    // Phase 2: 扫描本节点的 buffer pool，汇报持有的页面状态
    void RunIRRecoveryScan(node_id_t failed_node_id, node_id_t my_id) {
        LOG(INFO) << "[IR Recovery] Node " << my_id << " starting Phase 2 scan...";

        // P8 修复：在扫描前先刷新本节点日志到存储层
        // 确保当所有节点的 IRScanComplete 到达时，所有存活节点的日志已在存储层
        LogFlush();

        int reported_pages = 0;

        // 遍历所有表的 buffer pool 和 LPLM
        for (size_t t = 0; t < node_->lazy_local_page_lock_tables.size(); t++) {
            LRLocalPageLockTable* lplm = node_->lazy_local_page_lock_tables[t];
            BufferPool* bp = node_->local_buffer_pools[t];
            if (lplm == nullptr || bp == nullptr) continue;

            // 遍历 LPLM 中的每个页面，检查哪些页面原来归故障节点管理
            auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(t);
            if (partition_size == 0) continue;

            for (page_id_t p = 1; p < ComputeNodeBufferPageSize && p <= (page_id_t)(partition_size * ComputeNodeCount); p++) {
                // 检查这个页面是否原来由故障节点管理
                node_id_t original_owner = ((p - 1) / partition_size) % ComputeNodeCount;
                if (original_owner != failed_node_id) continue;

                // 检查本节点是否有此页面（在 LPLM 中是否有远程锁或正在申请锁）
                LRLocalPageLock* lr = lplm->GetLock(p);
                if (!lr->HasOwnerOrGranting()) continue;  // LPLM 中无远程锁且不在 granting，跳过

                // 本节点持有此页面的远程锁 → 向新的 GPLM 管理者汇报
                node_id_t new_manager = get_recovery_node_id(t, p);

                page_table_service::ReportPageStatusRequest req;
                page_table_service::ReportPageStatusResponse resp;
                page_table_service::PageID* page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(p);
                page_id_pb->set_table_id(t);
                req.set_allocated_page_id(page_id_pb);
                req.set_reporter_node_id(my_id);
                req.set_has_valid_copy(true);
                // P0 修复（有效副本判定）：上报本地副本页头 LSN。heap 页读
                // RmPageHdr::LLSN_；blink/FSM 派生表（+10000/+20000）页头无
                // LSN 字段，上报 0=无法证明新鲜度——接收方按非有效副本处理
                //（IR 保留，Phase 4 存储分析兜底），杜绝凭持锁声明有效副本。
                // 页不在本地 buffer（granting 中/已逐出）同样上报 0。
                uint64_t reporter_lsn = 0;
                const bool derived_table = (t >= 10000 && t < 30000);
                if (!derived_table && bp != nullptr) {
                    Page* local = bp->try_fetch_page(p);
                    if (local != nullptr) {
                        reporter_lsn = static_cast<uint64_t>(
                            reinterpret_cast<const RmPageHdr*>(local->get_data())->LLSN_);
                    }
                }
                req.set_reporter_lsn(reporter_lsn);
                // 获取本地锁模式
                int lock_mode = 0;  // NONE
                if (lr->IsUpgrading()) {
                    lock_mode = 1;  // SHARED (upgrading from S → X)
                } else if (lr->HasOwner()) {
                    // 判断是 S 还是 X
                    lock_mode = lr->getLock() == EXCLUSIVE_LOCKED ? 2 : 1;
                }
                req.set_lock_mode(lock_mode);

                if (new_manager == my_id) {
                    // 本地调用
                    page_table_service_impl_->ReportPageStatus_Localcall(&req, &resp);
                } else {
                    brpc::Controller cntl;
                    brpc::Channel* channel = nodes_channel + new_manager;
                    page_table_service::PageTableService_Stub stub(channel);
                    stub.ReportPageStatus(&cntl, &req, &resp, NULL);
                    if (cntl.Failed()) {
                        LOG(ERROR) << "[IR Recovery] Failed to report page status for table="
                                   << t << " page=" << p << " to node " << new_manager
                                   << ": " << cntl.ErrorText();
                    }
                }
                reported_pages++;
            }

            // 同样检查由正常节点管理但故障节点持锁的页面
            // 这里不需要汇报，因为这些页面的 GPLM 管理者是存活节点自己已经清理了
        }

        LOG(INFO) << "[IR Recovery] Node " << my_id << " reported " << reported_pages
                  << " pages. Sending scan complete...";

        // 通知所有存活节点（或其 GPLM manager）：本节点扫描完毕
        for (int i = 0; i < ComputeNodeCount; i++) {
            if (i == (int)failed_node_id || IsNodeFailed(i)) continue;

            page_table_service::IRScanCompleteRequest req;
            page_table_service::IRScanCompleteResponse resp;
            req.set_reporter_node_id(my_id);
            req.set_failed_node_id(failed_node_id);

            if (i == my_id) {
                page_table_service_impl_->IRScanComplete_Localcall(&req, &resp);
            } else {
                // P6 修复：添加重试逻辑，防止通知丢失导致 Barrier 永久阻塞
                bool success = false;
                for (int retry = 0; retry < 3 && !success; retry++) {
                    brpc::Controller cntl;
                    cntl.set_timeout_ms(5000);
                    brpc::Channel* channel = nodes_channel + i;
                    page_table_service::PageTableService_Stub stub(channel);
                    stub.IRScanComplete(&cntl, &req, &resp, NULL);
                    if (!cntl.Failed()) {
                        success = true;
                    } else {
                        LOG(WARNING) << "[IR Recovery] Failed to send scan complete to node " << i
                                     << " (attempt " << (retry + 1) << "): " << cntl.ErrorText();
                        if (retry < 2) usleep(500000);  // 500ms backoff
                    }
                }
                if (!success) {
                    LOG(ERROR) << "[IR Recovery] Failed to send IRScanComplete to node " << i
                               << " after 3 retries, barrier may be affected";
                }
            }
        }

        LOG(INFO) << "[IR Recovery] Node " << my_id << " Phase 2 complete.";
    }

    // Phase 3（日志分析阶段）: 向存储层发请求分析剩余 IR 锁页面的日志状态
    // P0 修复：surviving（本节点视角的存活列表）随请求传给存储层，
    // 用于 Undo 的存活节点前缀过滤（防止误撤销存活节点在途/已提交事务）
    void RunIRRecoveryPhase3(node_id_t failed_node_id, node_id_t my_id,
                             const std::vector<node_id_t>& surviving) {
        LOG(INFO) << "[IR Recovery] Node " << my_id << " waiting for Phase 2 barrier...";

        // 等待所有存活节点的 Phase 2 扫描完成
        // P0 修复：barrier 超时=恢复失败（保持隔离），不再强制完成——
        // 带着不完整的扫描信息继续 Phase 4 会提前放行/误发布
        recovery_observation::Span barrier_span("phase2_barrier");
        auto barrier = page_table_service_impl_->WaitPhase2AndGetRemainingIRPages(failed_node_id);
        if (!barrier.has_value()) {
            LOG(ERROR) << "[IR Recovery] Node " << my_id
                       << ": Phase 2 barrier FAILED for failed node " << failed_node_id
                       << " — recovery stays ISOLATED (recovery_in_progress remains set; "
                          "no Phase 4 publish, no IR release)";
            recovery_observation::Emit("recovery_error", -1, -1, 0, 0, 0, "phase2_barrier_timeout_isolated");
            // 不清 recovery_in_progress_：新事务继续被拒（admission 拒绝），
            // IR 页保持隔离。样本按 FAIL 收尾（驱动探测超时），符合
            // "缺终局或错误未解决即 FAIL" 的验收纪律
            return;
        }
        auto remaining_pages = std::move(*barrier);
        barrier_span.Stop(remaining_pages.size());

        if (remaining_pages.empty()) {
            // P0 修复：IR 集合为空不能跳过事务终结——故障节点的未提交事务
            //（无存活副本页但脏数据已物化到存储）仍需要 Undo。发一次空页
            // 列表的 AnalyzeRecoveryPages，存储侧会执行 Undo（按恢复代去重）
            LOG(INFO) << "[IR Recovery] Phase 3: no remaining IR-locked pages; "
                         "still invoking storage analysis for shared Undo";
            storage_service::AnalyzeRecoveryPagesRequest request;
            request.set_failed_node_id(failed_node_id);
            for (node_id_t n : surviving) request.add_alive_node_ids(n);
            storage_service::AnalyzeRecoveryPagesResponse response;
            storage_service::StorageService_Stub storage_stub(get_storage_channel());
            brpc::Controller cntl;
            cntl.set_timeout_ms(30000);
            storage_stub.AnalyzeRecoveryPages(&cntl, &request, &response, NULL);
            if (cntl.Failed()) {
                LOG(ERROR) << "[IR Recovery] Phase 3 (empty pages): AnalyzeRecoveryPages RPC failed: "
                           << cntl.ErrorText() << " — recovery stays ISOLATED";
                recovery_observation::Emit("recovery_error", -1, -1, 0, 0, 0, "rpc_failed_undo_isolated");
                return;
            }
            LOG(WARNING) << "[IR Recovery] Phase 3 (empty pages): shared undo ensured for failed node "
                         << failed_node_id;
            // Undo 已确保执行：这是恢复成功路径。先清理恢复残留
            // （废弃取页/零页缓存/valid 注册，见 ClearStaleAbandonedFetchStates），
            // 再清除 tainted 标志放行新事务——顺序不能颠倒，否则放行瞬间的
            // 新取页可能命中脏缓冲并重新注册 valid（fault-010/011 根因）。
            ClearStaleAbandonedFetchStates(failed_node_id);
            // 置位一次性恢复完成标志（先于放行：放行后的新取页立即
            // 看到 HasCompletedRecovery()=true，自愈/兜底授权同步生效）
            recovery_completed_once_.store(true, std::memory_order_release);
            recovery_in_progress_.store(false, std::memory_order_release);
            return;
        }

        LOG(INFO) << "[IR Recovery] Phase 3: Node " << my_id
                  << " analyzing " << remaining_pages.size() << " IR-locked pages via storage...";

        // R3 C-2（22.8）：可插拔策略接口接线。默认 OFF＝原路径零改动
        //（R2 行为保留，global 对照完整）。接口只决定合法任务的优先顺序
        //——对 remaining_pages 重排；Redo/Undo/验证/发布仍走公共
        // AnalyzeRecoveryPages 路径，策略不获得任何执行权限。接口开销
        // 单列计时（[R3-IFACE] 日志），不混入恢复时长。
        {
            const char* env_policy = getenv("HCM_RECOVERY_POLICY");
            std::string policy_name = env_policy != nullptr ? env_policy : "OFF";
            if (policy_name != "OFF") {
                auto t0 = std::chrono::steady_clock::now();
                std::string perr;
                auto policy =
                    recovery_iface::PolicyRegistry::Instance().Create(policy_name, &perr);
                if (policy == nullptr) {
                    LOG(ERROR) << "[R3-IFACE] policy '" << policy_name
                               << "' unavailable: " << perr
                               << " — public default order kept (fail-closed)";
                } else {
                    static std::atomic<uint64_t> r3_iface_epoch{1};
                    recovery_iface::RecoveryContext::Budget budget;
                    budget.total_cost_units = remaining_pages.size();
                    budget.remaining_cost_units = remaining_pages.size();
                    budget.executor_threads = 1;
                    budget.io_weight = 100;
                    recovery_iface::RecoveryContext ctx(1, r3_iface_epoch.fetch_add(1), 0, 0, 0,
                                                        remaining_pages.size(), budget);
                    if (!policy->Initialize(ctx, &perr)) {
                        LOG(ERROR) << "[R3-IFACE] policy '" << policy_name
                                   << "' init failed: " << perr
                                   << " — public default order kept (fail-closed)";
                    } else {
                        auto t1 = std::chrono::steady_clock::now();
                        // 物化 catalog：task_id 按 remaining_pages 原序编号
                        //（B0 公共默认序＝原序＝与 R2 行为一致的恒等重排）
                        recovery_iface::RecoveryTaskCatalog::Builder builder;
                        for (size_t i = 0; i < remaining_pages.size(); ++i) {
                            recovery_iface::TaskSpec s;
                            s.task_id = i + 1;
                            s.table_id = remaining_pages[i].table_id;
                            s.key_lo = remaining_pages[i].page_id;
                            s.key_hi = remaining_pages[i].page_id + 1;
                            s.wal_end_lsn = remaining_pages[i].gplm_lsn;
                            s.shared_cost_estimate = 1;
                            s.state = recovery_iface::TaskState::READY;
                            s.influence = recovery_iface::RPageState::AFFECTED;
                            builder.Add(std::move(s));
                        }
                        std::unique_ptr<recovery_iface::RecoveryTaskCatalog> catalog;
                        auto berr = builder.Build(&catalog);
                        auto t2 = std::chrono::steady_clock::now();
                        if (berr.has_value() || catalog == nullptr) {
                            LOG(ERROR) << "[R3-IFACE] catalog build failed: "
                                       << (berr ? *berr : std::string("null"))
                                       << " — public default order kept (fail-closed)";
                        } else {
                            recovery_iface::DemandSnapshot snap;
                            snap.epoch = ctx.epoch;
                            snap.version = 1;
                            policy->OnDemandSnapshot(snap);
                            auto proposal = policy->Propose(snap, catalog->TotalCost());
                            auto t3 = std::chrono::steady_clock::now();
                            recovery_iface::RecoveryScheduler sched(
                                ctx, std::shared_ptr<const recovery_iface::RecoveryTaskCatalog>(
                                         std::move(catalog)));
                            size_t accepted = sched.Submit(proposal, policy->name());
                            // 重排：公共执行队列顺序即处理顺序
                            std::vector<decltype(remaining_pages)::value_type> ordered;
                            ordered.reserve(remaining_pages.size());
                            recovery_iface::RecoveryScheduler::QueueItem item;
                            while (sched.PopReady(&item)) {
                                ordered.push_back(remaining_pages[item.task_id - 1]);
                            }
                            auto t4 = std::chrono::steady_clock::now();
                            auto us = [](std::chrono::steady_clock::time_point a,
                                         std::chrono::steady_clock::time_point b) {
                                return std::chrono::duration_cast<std::chrono::microseconds>(
                                           b - a)
                                    .count();
                            };
                            LOG(INFO) << "[R3-IFACE] policy=" << policy_name
                                      << " pages=" << remaining_pages.size()
                                      << " groups_accepted=" << accepted
                                      << " queue_depth=" << sched.QueueDepth()
                                      << " materialize_us=" << us(t1, t2)
                                      << " propose_us=" << us(t2, t3)
                                      << " schedule_us=" << us(t3, t4)
                                      << " total_iface_us=" << us(t0, t4)
                                      << " (interface overhead listed separately)";
                            if (ordered.size() == remaining_pages.size()) {
                                remaining_pages = std::move(ordered);
                            } else {
                                LOG(ERROR) << "[R3-IFACE] reorder incomplete (" << ordered.size()
                                           << "/" << remaining_pages.size()
                                           << ") — public default order kept (fail-closed)";
                            }
                        }
                    }
                }
            }
        }


        // 先确保本节点所有待刷的日志已经发送到存储层
        LogFlush();

        // 构造存储层分析请求 - 分批发送避免单个请求过大
        const int BATCH_SIZE = 500;
        int total_no_modify = 0;
        int total_replayed = 0;

        for (size_t batch_start = 0; batch_start < remaining_pages.size(); batch_start += BATCH_SIZE) {
            size_t batch_end = std::min(batch_start + (size_t)BATCH_SIZE, remaining_pages.size());

            storage_service::AnalyzeRecoveryPagesRequest request;
            request.set_failed_node_id(failed_node_id);
            // P0 修复：存活列表随请求传递，存储侧 Undo 据此跳过存活节点前缀事务
            for (node_id_t n : surviving) request.add_alive_node_ids(n);

            for (size_t i = batch_start; i < batch_end; i++) {
                auto& page_info = remaining_pages[i];
                auto* pb_page = request.add_pages();

                // 解析 table_name
                std::string tab_name;
                table_id_t table_id = page_info.table_id;
                if (WORKLOAD_MODE == 4) {  // SQL mode
                    int tab_id = 0;
                    if (table_id < 10000) {
                        tab_id = table_id;
                    } else if (table_id < 20000) {
                        tab_id = table_id - 10000;
                    } else if (table_id < 30000) {
                        tab_id = table_id - 20000;
                    }
                    tab_name = getTableNameFromTableID(tab_id);
                    if (table_id >= 10000 && table_id < 20000) {
                        tab_name += "_bl";
                    } else if (table_id >= 20000 && table_id < 30000) {
                        tab_name += "_fsm";
                    }
                } else {
                    if (table_id < (table_id_t)table_name_meta.size()) {
                        tab_name = table_name_meta[table_id];
                    }
                }

                pb_page->set_table_name(tab_name);
                pb_page->set_page_no(page_info.page_id);
                pb_page->set_gplm_lsn(page_info.gplm_lsn);
                pb_page->set_table_id(table_id);
            }

            // 发送到存储层
            storage_service::StorageService_Stub storage_stub(get_storage_channel());
            brpc::Controller cntl;
            cntl.set_timeout_ms(30000);  // 30s 超时，日志回放可能耗时
            storage_service::AnalyzeRecoveryPagesResponse response;

            storage_stub.AnalyzeRecoveryPages(&cntl, &request, &response, NULL);

            if (cntl.Failed()) {
                LOG(ERROR) << "[IR Recovery] Phase 3: AnalyzeRecoveryPages RPC failed: " << cntl.ErrorText();
                recovery_observation::Emit("recovery_error", -1, -1, batch_end - batch_start, 0, 0, "rpc_failed_ir_retained");
                return;
            }

            if (response.results_size() != request.pages_size()) {
                // P0 修复（消灭静默失败）：响应不完整时 IR 保持隔离——必须
                // 显式记录，原实现只 Emit（trace 未启用时完全不可见），
                // 表现为"恢复无日志地永久卡住"
                LOG(ERROR) << "[IR Recovery] Phase 3: incomplete response: results="
                           << response.results_size() << " request_pages=" << request.pages_size()
                           << " — IR locks retained (recovery stays isolated)";
                recovery_observation::Emit("recovery_error", -1, -1, 0, 0, 0, "incomplete_response_ir_retained");
                return;
            }
            for (int i = 0; i < response.results_size(); ++i) {
                const auto& result = response.results(i);
                if (result.table_id() != request.pages(i).table_id() ||
                    result.page_no() != request.pages(i).page_no() ||
                    (result.status() != 0 && result.status() != 1)) {
                    LOG(ERROR) << "[IR Recovery] Phase 3: invalid result at " << i
                               << ": table=" << result.table_id() << " page=" << result.page_no()
                               << " status=" << result.status()
                               << " (request table=" << request.pages(i).table_id()
                               << " page=" << request.pages(i).page_no()
                               << ") — IR locks retained (recovery stays isolated)";
                    recovery_observation::Emit("recovery_error", result.table_id(), result.page_no(), result.status(), 0, 0, "invalid_result_ir_retained");
                    return;
                }
            }
            recovery_observation::Span publish_span("ir_publish_batch");
            // 处理存储层返回的结果
            int released_direct = 0, released_replayed = 0, retained_isolated = 0;
            for (int i = 0; i < response.results_size(); i++) {
                const auto& result = response.results(i);
                table_id_t table_id = result.table_id();
                page_id_t page_no = result.page_no();

                if (result.status() == 0) {
                    // 页面无修改或已经是最新，直接释放 IR 锁
                    page_table_service_impl_->ReleaseIRLockForPage(table_id, page_no);
                    // R2c C1: 目录同步迁移 RECOVERED_READY（验证版本取存储侧结论）
                    recovery_catalog_.MarkRecovered(
                        recovery_catalog_.current_generation(), table_id, page_no,
                        (uint64_t)result.recovered_lsn());
                    total_no_modify++;
                    released_direct++;
                } else if (result.status() == 1) {
                    // 页面需要日志回放，存储层已经回放完毕并返回了最新数据
                    // 将回放后的页面数据写回存储层（存储层的 read_page_with_lsn 内部已处理）
                    // 释放 IR 锁，标记为 storage-only（数据已在存储层）
                    page_table_service_impl_->ReleaseIRLockForPage(table_id, page_no);
                    recovery_catalog_.MarkRecovered(
                        recovery_catalog_.current_generation(), table_id, page_no,
                        (uint64_t)result.recovered_lsn());
                    total_replayed++;
                    released_replayed++;
                } else {
                    // R2 证据修复：status=-1（分析失败/redo 未能确认）的页
                    // 保持 IR 锁是 fail-closed 的正确语义（数据状态无法确认，
                    // 拒绝服务该页），但原实现完全静默——客户端会在该页上
                    // 无限轮询且无人知道原因（r2-fault-small-003 的 page 2
                    // 挂死即此路径）。必须显式记录隔离页，供定位与验收。
                    retained_isolated++;
                    LOG(ERROR) << "[IR Recovery] Phase 3: page ANALYSIS FAILED, IR lock retained "
                               << "(table=" << table_id << " page=" << page_no
                               << " gplm_lsn=" << request.pages(i).gplm_lsn()
                               << " recovered_lsn=" << result.recovered_lsn() << ")";
                }
            }
            if (retained_isolated)
                LOG(WARNING) << "[IR Recovery] Phase 3 batch: released " << released_direct
                             << " direct + " << released_replayed << " replayed; RETAINED ISOLATED (IR locked, unservicable): "
                             << retained_isolated;
        }

        LOG(INFO) << "[IR Recovery] Phase 3 complete: " << total_no_modify
                  << " pages released directly, " << total_replayed
                  << " pages recovered via log replay. All IR locks cleared.";
        // R2 证据：INFO 默认不落盘，补一条 WARNING 级汇总保证恢复完成
        // 对外可见（日志/验收可观测）
        // R2c C1: 目录终局取证（残留 AFFECTED = status=-1 隔离页/漏处理页）
        {
            auto cnt = recovery_catalog_.GetCounts();
            LOG(WARNING) << "[IR Recovery] Phase 3 COMPLETE (node " << my_id
                         << "): released_direct=" << total_no_modify
                         << " released_replayed=" << total_replayed
                         << " | catalog gen=" << cnt.generation
                         << " affected=" << cnt.affected
                         << " recovered_ready=" << cnt.recovered_ready
                         << " swept_tables=" << cnt.swept_tables
                         << " stale=" << cnt.stale_entries;
            if (cnt.affected > 0) {
                for (const auto& k : recovery_catalog_.ListIsolated()) {
                    LOG(WARNING) << "[IR Recovery] catalog ISOLATED retained: table="
                                 << k.table_id << " page=" << k.page_id;
                }
            }
        }

        // P0 修复（恢复后残留状态清理）：恢复窗口内被中止的取页可能把
        // 故障节点原分区页面的 LPLM 留在"废弃在途"中间态（is_granting=true
        // 且无活跃使用），使后续取页永久自旋（faultdiag-003 page 22）；
        // 同时清理零页获取残留的 LPLM 持有态与页表 valid 注册（fault-010/011）。
        // 必须在放行新事务（recovery_in_progress_=false）之前完成——
        // 否则放行瞬间的新取页仍可能命中脏缓冲并重新注册 valid。
        ClearStaleAbandonedFetchStates(failed_node_id);

        // 置位一次性恢复完成标志（顺序同上：先于放行）
        recovery_completed_once_.store(true, std::memory_order_release);

        // 恢复完成，清除恢复进行中标志，此后新事务不再被标记为 tainted
        recovery_in_progress_.store(false, std::memory_order_release);
    }

    // 清理故障节点原分区页面的恢复残留状态（恢复完成、放行新请求之前
    // 调用；活跃事务持有的锁不受影响）：
    // ① ResetStaleAbandonedFetch：废弃在途取页的 granting 残留；
    // ② ResetIdleRemoteHeldState：恢复窗口内取到的"零页/旧页"副本因
    //   remote_mode=SHARED 被当作本地有效副本反复复用（fault-010 实测
    //   页 2 缓冲零页永不过期），清持有状态强制下次取页重走存储获取，
    //   put_page_into_buffe_lazy 用 replay 后的真实页覆盖旧缓冲；
    // ③ InvalidateValidCopiesForPage：零页获取时幸存者在权威页表
    //   （本节点接管的故障分区页）注册的 valid 副本残留——fault-011
    //   实测清 LPLM 后重取仍命中零页，因为 LRPSLock 的 GetValid 看到
    //   本节点 status=true → need_storage=false → try_fetch_page 又取
    //   回本地脏缓冲。标回 storage-only 后下次取页强制从存储取真实页。
    //   本地页表实例仅对本节点接管的页权威，必须限定
    //   get_recovery_node_id(t,p)==my_id（各幸存者合计全覆盖故障分区）。
    void ClearStaleAbandonedFetchStates(node_id_t failed_node_id) {
        int stale_reset = 0;
        int idle_remote_reset = 0;
        int deferred_invalidate = 0;
        int valid_residue_cleared = 0;
        for (size_t t = 0; t < node_->lazy_local_page_lock_tables.size(); t++) {
            LRLocalPageLockTable* lplm = node_->lazy_local_page_lock_tables[t];
            if (lplm == nullptr) continue;
            auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(t);
            if (partition_size == 0) continue;
            for (page_id_t p = 0; p < ComputeNodeBufferPageSize; p++) {
                node_id_t original_owner = ((p - 1) / partition_size) % ComputeNodeCount;
                if (p != 0 && original_owner != failed_node_id) continue;
                LRLocalPageLock* l = lplm->GetLock(p);
                bool local_reset_1 = false;
                if (l->ResetStaleAbandonedFetch()) { stale_reset++; local_reset_1 = true; }
                if (l->ResetIdleRemoteHeldState()) { idle_remote_reset++; local_reset_1 = true; }
                else if (l->DeferInvalidateIfHeld()) deferred_invalidate++;
                // D-1 修复（R2c 早锚点冷缓存连环 stuck）：本地撤销的持有/排队
                // 可能已通过 Phase 2 汇报或受理的 LRPXLock 登记进 manager
                //（新管理者）的 GPLM——只清本地会留下无人再释放的幽灵份额，
                // 后续 X 授权被挡 45s/轮直至 wall budget 耗尽（r2c-20260923-c5
                // page 31 实证：B 的 idle S 被 reset 后 C 侧 holders=[1,] 永生）。
                // 撤销幂等：manager 侧 UnlockAny stale 返回并撤销排队，无副作用。
                if (p != 0 && local_reset_1) {
                    node_id_t withdraw_mgr = get_recovery_node_id(t, p);
                    page_table_service::PAnyUnLockRequest wreq;
                    page_table_service::PAnyUnLockResponse wresp;
                    page_table_service::PageID* wpid = new page_table_service::PageID();
                    wpid->set_page_no(p);
                    wpid->set_table_id(t);
                    wreq.set_allocated_page_id(wpid);
                    wreq.set_node_id(node_->node_id);
                    wreq.set_lsn(0);
                    wreq.set_force_forfeit_holders(false);
                    if (withdraw_mgr == node_->node_id) {
                        page_table_service_impl_->LRPAnyUnLock_Localcall(&wreq, &wresp);
                    } else {
                        // R3 D-3 层2：withdraw 失败时重定向新 manager 重试
                        //（单发失败＝幽灵份额残留，L16 兜底实证不充分——
                        // r3-20260923-d3-fix-early-001 page 31 stall 300s/轮）。
                        bool wdone = false;
                        for (int i = 0; i < 6 && !wdone; ++i) {
                            if (i > 0) usleep(10 * 1000); // 给 redistribute 映射传播窗口
                            brpc::Controller wcntl;
                            brpc::Channel* wchannel = nodes_channel + withdraw_mgr;
                            page_table_service::PageTableService_Stub wstub(wchannel);
                            wcntl.set_timeout_ms(5000);
                            wstub.LRPAnyUnLock(&wcntl, &wreq, &wresp, NULL);
                            if (!wcntl.Failed()) { wdone = true; break; }
                            node_id_t retry_mgr = get_recovery_node_id(t, p);
                            if (retry_mgr == withdraw_mgr) continue;
                            withdraw_mgr = retry_mgr;
                            if (withdraw_mgr == node_->node_id) {
                                page_table_service_impl_->LRPAnyUnLock_Localcall(&wreq, &wresp);
                                wdone = true;
                            }
                        }
                        if (!wdone) {
                            LOG(WARNING) << "[IR Recovery] residue withdraw unlock RPC failed: table="
                                         << t << " page=" << p << " mgr=" << withdraw_mgr
                                         << " (no reachable manager; residue to Phase1a/2)";
                        }
                    }
                    LOG(WARNING) << "[IR Recovery] withdrew residue registration at manager: table="
                                 << t << " page=" << p << " mgr=" << withdraw_mgr;
                }
                if (p != 0 && get_recovery_node_id(t, p) == node_->node_id) {
                    if (page_table_service_impl_->InvalidateValidCopiesForPage(t, p)) valid_residue_cleared++;
                }
            }
        }
        if (stale_reset || idle_remote_reset || valid_residue_cleared || deferred_invalidate)
            LOG(WARNING) << "[IR Recovery] cleared recovery residue on failed node partition: "
                         << stale_reset << " stale abandoned fetch states, "
                         << idle_remote_reset << " idle remote-held states, "
                         << deferred_invalidate << " deferred invalidates (released with last local lock), "
                         << valid_residue_cleared << " valid-table residues (stale buffer copies invalidated; next fetch re-acquires from storage)";
        // 第 15 层（fault-0524）：登记死亡节点，首个新事务 admission 时向
        // remote evict 其残留 workload key 锁（victim 死时无人释放）
        pending_workload_evict_node_.store((int32_t)failed_node_id,
                                           std::memory_order_release);
    }

private:
    ComputeNode* node_;
    std::vector<GlobalLockTable*>* global_page_lock_table_list_;
    std::vector<GlobalValidTable*>* global_valid_table_list_;


    brpc::Channel* nodes_channel; //与其他计算节点通信的channel
    page_table_service::PageTableServiceImpl* page_table_service_impl_; // 保存在类中，以便本地调用

    // 时间片轮转的，表示当前多少个协程已经完成了或者没必要启动了
    std::atomic<int> alive_fiber_cnt;
    std::unordered_set<uint64_t> hot_page_set;

    // SQL
    std::unordered_map<std::string , int> table_use;
    std::mutex tab_meta_mtx;
    bool is_dropingTable = false;
    bool is_creatingTable = false;

    // Cache for RmFileHdr
    std::mutex file_hdr_cache_mutex_;
    std::map<table_id_t, RmFileHdr::ptr> file_hdr_cache_;

    // 日志管理：节点级别的共享日志系统
    // 所有事务的日志都写入此共享队列，由后台线程统一刷新到存储层
    std::vector<LogRecord*> log_records;           // 共享日志队列
    mutable std::mutex log_mtx;                 // 保护 log_records 的互斥锁
    uint64_t log_enqueue_sequence_ = 0;          // log_mtx
    uint64_t log_acked_sequence_ = 0;            // persist_lsn_mtx

    // 持久化 LSN 管理
    LLSN persist_lsn = 0;                       // 已持久化到存储层的最大 LSN
    mutable std::mutex persist_lsn_mtx;         // 保护 persist_lsn 的互斥锁
    std::condition_variable persist_lsn_cond;   // persist_lsn 条件变量
    uint64_t flush_round_ = 0;                  // 成功完成的 LogFlush 轮次（persist_lsn_mtx 保护）

    LLSN current_llsn_ = 0;                     // 本节点当前的最大 LLSN, 初始为0
};

int socket_start_client(std::string ip, int port);

int socket_finish_client(std::string ip, int port);
