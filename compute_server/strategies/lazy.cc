#include "compute_server/server.h"

#include "thread"
#include "atomic"
#include <iomanip>
#include <chrono>
#include "core/recovery/observation.h"

static std::atomic<int> cnt{0};

static std::mutex page_cnt_mtx;
static std::vector<int> page_cnt(10000 , 0);

namespace {
template<class T> struct ResponseGuard {
    T*& response;
    ~ResponseGuard() { delete response; response = nullptr; }
};
class FetchFailureGuard {
public:
    explicit FetchFailureGuard(LRLocalPageLock* lock) : lock_(lock), exceptions_(std::uncaught_exceptions()) {}
    ~FetchFailureGuard() {
        if (armed_ && quarantine_ && std::uncaught_exceptions() > exceptions_) lock_->QuarantineFetch();
    }
    void Arm() { armed_ = true; }
    // L16 授权等待超时（smoke-018）：fetch 止步于加锁阶段，无数据装入，
    // 且超时路径已完成远程 force release + ResetStaleAbandonedFetch 自愈
    // 清理，本地无中间状态可污染——隔离属过度防御（隔离的解封条件是
    // HasCompletedRecovery，健康负载永无恢复事件，页将被永久隔离）。
    // ordinary-retry 契约针对的是存储取页失败（坏页防御），此路径不涉
    // 及数据，disarm 不影响该契约。
    void Disarm() { quarantine_ = false; }
private:
    LRLocalPageLock* lock_;
    int exceptions_;
    bool armed_ = false;
    bool quarantine_ = true;
};
}

// BLink 的多节点索引同步走的也是 lazy ，不需要统计，这个 need_to_record 就是用来隔离 BLink 的
Page* ComputeServer::rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id, bool need_to_record) {
    recovery_observation::FetchScope observation(table_id, page_id);
    assert(page_id < ComputeNodeBufferPageSize);
    if (need_to_record){
        this->node_->fetch_allpage_cnt++;
        int k1 = cnt.fetch_add(1);
        if (k1 % 100 == 0){
            std::cout << "Lazy Fetch Page Cnt = " << k1 << "\n";
        }
    }
    
    // LOG(INFO) << "fetching S Page " << "table_id = " << table_id << " page_id = " << page_id;
    Page *page = nullptr;
    // 先在本地进行加锁，这一步同时确保对于单个页面，主节点只有一个页面会在竞争这个页面所有权
    auto* local_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id);
    // P0 修复（恢复后解隔离）：恢复窗口内的取页失败会隔离该页（防止复用
    // 不一致的中间状态）。恢复完成后必须允许重新取页——新一次取页重新走
    // 完整协议，不复用旧状态；否则该页在本节点永久不可用。
    // 判定用 HasCompletedRecovery()（本进程完成过恢复）而非
    // !IsRecoveryInProgress()：从未恢复的进程里隔离语义必须保持
    // （recovery_e0_test 的 ordinary-retry 契约）。
    if (HasCompletedRecovery() && local_lock->ClearFetchQuarantineAfterRecovery()) {
        LOG(WARNING) << "[IR Recovery] cleared fetch quarantine after recovery: table="
                     << table_id << " page=" << page_id;
    }
    FetchFailureGuard failure_guard(local_lock);
    int force_stolen = 0;
    bool lock_remote = local_lock->LockShared(&force_stolen);
    failure_guard.Arm();
    if (force_stolen != 0) {
        LOG(WARNING) << "[IR Recovery] stuck local latch force-released (S): table=" << table_id
                     << " page=" << page_id << " stolen_remote_mode=" << force_stolen;
        ReleaseRemoteForForcedPage(table_id, page_id, force_stolen == 2);
    }
    // 第 18 层补丁（early29 fault-0307 复发）：global_valid_table_list_ 是
    // 本地实例，仅对本节点接管的页权威（fault-011 结论）。故障窗口内若
    // 本节点非该页恢复管理者，本地有效表条目缺失/陈旧，第 18 层的
    // IsValid 校验无从判定（vinfo=nullptr 直通旧副本，实测 B 用旧副本
    // 把 C 已提交的 DELETE"复活"成 DUPLICATE_KEY）。此时本地快路径的
    // "无竞争"不可信：回滚本地 S 份额，转 PXL 由管理者权威仲裁 push 源。
    if (!lock_remote && recovery_epoch.load() > 0 &&
        get_recovery_node_id(table_id, page_id) != node_->node_id) {
        local_lock->AbortHeldLatchOnFetchFailure(false);
        lock_remote = true;
        LOG(WARNING) << "[IR Recovery] non-manager local fast path bypassed to PXL (S): table="
                     << table_id << " page=" << page_id;
    }
    // 第 14 层（fault-0088）：本地 latch 已设置后，取页流程任何抛错点
    //（防御等待超时/read-reject/IR deadline/存储兜底超时）都必须回滚本
    // 线程的本地 latch 份额再上抛——否则已授予态计数泄漏成为 LPLM 自愈
    // 死角，后续同页取页永久自旋（TxAbortWorkLoad 回滚死锁实测）。
    try {
    // 如果本地加锁成功，说明页面所有权在我身上，页面也一定在缓冲区里，直接去拿即可
    if (!lock_remote){
        // 第 12 层缺陷修复（live-early11）：本地加锁成功不代表本地副本可信。
        // Phase 1b 会把接管页/IR 释放页在 GPLM 有效表标记为 only-in-storage
        //（HasAnyValid()=-1，该状态仅出现在故障恢复流）。此时本地缓冲中的
        // 副本可能是故障前的旧世界共享副本——页角色已漂移，undo/常规导航据
        // 此命中非叶页并崩溃于 blink.cc:1034 is_leaf 断言（live-early11 C
        // 节点 Phase 2 撤销扫描实证：经由本地快路径而非 storage fallback，
        // 第 10 层守卫未覆盖）。修复：恢复窗口内延迟等待恢复完成；完成后从
        // 存储取权威副本（Phase 4 已修正计算页空间），禁止服务本地陈旧副本。
        // 若本地缓冲无此页（重分布页首取），原路径 fetch_page 会因缺席断言
        // 崩溃，本修复一并覆盖该形态。有界等待超时按防御等待契约抛错。
        bool only_in_storage_12 = false;
        GlobalValidInfo* vinfo_18 = nullptr;
        if (recovery_epoch.load() > 0 &&
            static_cast<size_t>(table_id) < global_valid_table_list_->size()) {
            GlobalValidTable* gvt_12 = (*global_valid_table_list_)[table_id];
            if (gvt_12 != nullptr) {
                GlobalValidInfo* vinfo_12 = gvt_12->GetValidInfo(page_id);
                if (vinfo_12 != nullptr && vinfo_12->HasAnyValid() == -1)
                    only_in_storage_12 = true;
                vinfo_18 = vinfo_12;
            }
        }
        // 第 18 层（early28 fault-0307）：本地加锁成功且页非 only-in-storage
        // 时，旧逻辑直接服务本地缓冲副本——但故障后 GPLM 有效表可能已把
        // 权威指向其他节点（如另一幸存者提交了该页修改，实测 C 的 DELETE
        // 400026 CONFIRMED_COMMITTED 后 B 用旧副本重插报 DUPLICATE_KEY，
        // 已提交的删除被旧副本"复活"）。本地快路径必须校验有效表：本节点
        // 无效即本地副本是旧世界残留，改走存储权威源（取页内含日志 flush
        // 等待 + replay 追平屏障，读到版本 ≥ 已提交 WAL），覆盖旧副本。
        const bool local_copy_stale_18 =
            !only_in_storage_12 && vinfo_18 != nullptr &&
            !vinfo_18->IsValid_NoBlock(node_->node_id);
        if (only_in_storage_12 || local_copy_stale_18) {
            const int wait_max_12 = [] {
                const char* env = ::getenv("HCM_IR_WAIT_MAX_MS");
                return env ? std::max(1000, atoi(env)) : 120000;
            }();
            int waited_12 = 0;
            while (!HasCompletedRecovery()) {
                if (++waited_12 >= wait_max_12)
                    throw std::runtime_error("only-in-storage local copy deferred past recovery deadline (S)");
                usleep(2000);
            }
            if (local_copy_stale_18) {
                LOG(WARNING) << "[IR Recovery] stale local copy bypassed, served from storage (S): table="
                             << table_id << " page=" << page_id;
            } else {
                LOG(WARNING) << "[IR Recovery] only-in-storage page served from storage (S): table="
                             << table_id << " page=" << page_id;
            }
            observation.AuthorizeStorage(true);
            std::string data_12 = rpc_fetch_page_from_storage(table_id, page_id, need_to_record);
            page = put_page_into_buffer(table_id, page_id, data_12.c_str(), 1, need_to_record);
        } else {
            if (need_to_record){
                node_->fetch_from_local_cnt++;
            }
            // 一定在缓冲池里
            page = node_->local_buffer_pools[table_id]->fetch_page(page_id);
        }
    } else {
        // 在远程加锁
        if (need_to_record){
            node_->lock_remote_cnt++;
        }

        // 故障容错重试循环：RPC 失败或故障恢复中断时重试
        bool need_full_retry = true;
        int rpc_retry_count = 0;
        // 第 16 层（early23 fault-0064/0066）：-1 重试经 already_queued 重发
        // Pending 理论自愈，但 GPLM 队列推进本身卡住（FIFO 队头阻塞 / Unlock
        // RPC 在途失败致 owner 残留 / GPLM-LPLM 状态分裂）时永不收敛（实测
        // A/C 两节点同页 S/X 互等 8 分钟无自愈）。慢路径墙钟总时限兜底：
        // 超时先幂等远程解锁（清 GPLM owner 残留并 TransferControl 唤醒队列），
        // 再清本地 granting 残留，随后抛错转确定性中止。
        const int64_t grant_total_ms_16 = [] {
            const char* env = ::getenv("HCM_FETCH_GRANT_TOTAL_MS");
            return static_cast<int64_t>(env ? std::max(10000, atoi(env)) : 300000);
        }();
        const auto grant_start_16 = std::chrono::steady_clock::now();
        int64_t grant_reported_16 = 0;
        while (need_full_retry) {
            need_full_retry = false;

            // IR Recovery: 重试时需要重新设置 LPLM 的 is_granting 状态
            // 因为 SetRecoveryAbort 可能已经清理了 is_granting 和 lock
            node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->ResetForRetry(false);

            page_table_service::PSLockRequest request;
            page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
            ResponseGuard<page_table_service::PSLockResponse> response_guard{response};
            page_table_service::PageID *page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            request.set_allocated_page_id(page_id_pb);
            request.set_node_id(node_->node_id);

            // IR Recovery: 使用故障恢复路由
            node_id_t page_belong_node = get_recovery_node_id(table_id , page_id);

            // IR 锁重试循环
            bool ir_retry = true;
            int ir_waited_ms = 0;
            // R2 修复：恢复中页面可能长期持 IR 锁（Phase4 分析失败页按
            // fail-closed 永久保留），无限 1ms 轮询会让事务挂死且客户端
            // 无从感知。有界等待（默认 120s），超时抛错 → workload 层归类
            // 为确定性 ABORT（防御等待超时=事务失败，不是节点失败）。
            const int ir_wait_max_ms = [] {
                const char* env = ::getenv("HCM_IR_WAIT_MAX_MS");
                return env ? std::max(1000, atoi(env)) : 120000;
            }();
            // 第 10 层缺陷修复（live-early9）：恢复窗口内的无 LSN storage
            // fallback 会取回多代滞后的旧版本页——lazy 模式页的最新版本
            // 在计算节点缓冲，存储端版本可能对应任意历史布局（页角色已
            // 漂移，undo 导航据此命中非叶页并崩溃于 blink.cc is_leaf 断
            // 言），且 put_page_into_buffer 会反向覆盖本地正确副本（同
            // fault-010 污染机理）。故障已检测且恢复未完成期间一律延迟
            // 重取：恢复完成后存储已 replay（权威）、GPLM 授权链路重建，
            // 重走完整加锁流程必然收敛；有界等待超时按防御等待契约抛错
            // （workload 层归类确定性 ABORT）。带 LSN 的 GPLM 授权取页
            // （need_storage_fetch 路径）不在拦截之列：LSN 匹配保证取回
            // 的是授权时刻的自洽版本。
            int recovery_fallback_waited_ms = 0;
            auto storage_fallback_or_defer = [&]() -> bool {
                if (HasCompletedRecovery()) return true;
                if (recovery_epoch.load() == 0) return true;  // 故障未检测：无恢复窗口语义
                if (++recovery_fallback_waited_ms >= ir_wait_max_ms)
                    throw std::runtime_error("storage fallback deferred past recovery deadline");
                usleep(2000);
                need_full_retry = true;
                return false;
            };
            while (ir_retry) {
                local_lock->CheckFetchAllowed();
                ir_retry = false;
                if(page_belong_node == node_->node_id) {
                    this->page_table_service_impl_->LRPSLock_Localcall(&request, response);
                } else {
                    brpc::Controller cntl;
                    brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
                    page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
                    pagetable_stub.LRPSLock(&cntl, &request, response, NULL);
                    if(cntl.Failed()){
                        rpc_retry_count++;
                        if (rpc_retry_count <= 3) {
                            LOG(WARNING) << "LRPSLock RPC failed for page " << page_id << " (retry " << rpc_retry_count << "): " << cntl.ErrorText();
                        }
                        delete response; response = nullptr;
                        // 指数退避: 10ms, 20ms, 40ms, ..., 上限 500ms
                        int backoff_us = std::min(10000 * (1 << std::min(rpc_retry_count - 1, 5)), 500000);
                        usleep(backoff_us);
                        need_full_retry = true;
                        break;
                    }
                }
                // 如果返回 IR 锁，等待后重试
                if (!need_full_retry && response->ir_locked()) {
                    observation.Block("ir_management_or_recovery");
                    if (++ir_waited_ms >= ir_wait_max_ms)
                        throw std::runtime_error("IR lock wait deadline exceeded (page isolated by recovery analysis)");
                    usleep(1000);  // 1ms backoff
                    response->Clear();
                    page_id_pb = new page_table_service::PageID();
                    page_id_pb->set_page_no(page_id);
                    page_id_pb->set_table_id(table_id);
                    request.set_allocated_page_id(page_id_pb);
                    ir_retry = true;
                }
            }
            if (need_full_retry) continue;
            observation.Unblock();

            bool need_storage = response->need_storage_fetch();
            observation.AuthorizeStorage(need_storage);
            if(!response->wait_lock_release()){
                if (need_to_record){
                    node_->fetch_three_cnt++;
                }
                node_id_t valid_node = response->newest_node();
                if (need_storage){
                    std::string data;
                    if (need_to_record){
                        LLSN lsn = response->lsn();
                        assert(lsn != (LLSN)-1);
                        data = rpc_fetch_page_from_storage_with_lsn(table_id , page_id , lsn , need_to_record);
                    }else {
                        // 第 19 层（smoke-014 C SIGABRT）：need_storage_fetch 的
                        // 第 10 层豁免仅对带 LSN 的 heap 表取页（need_to_record
                        // =true）成立。派生表（BLink/FSM）走无 LSN GetPage，lazy
                        // 模式下其最新版本在计算节点缓冲，storage 端副本可能对应
                        // 任意历史布局（页角色漂移）——恢复窗口内取回旧版索引页，
                        // insert_entry 据此命中非叶页崩溃于 blink.cc:991 is_leaf
                        // 断言。与 push-failure fallback 同款守卫：恢复窗口内延迟
                        // 重走完整加锁流程，有界超时抛错转确定性 ABORT
                        if (!storage_fallback_or_defer()) continue;
                        data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                    }
                    page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                } else if(valid_node != -1){    
                    if (need_to_record){
                        node_->fetch_from_remote_cnt++;
                    }
                    bool push_ok = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryGetPushData(table_id);
                    if (!push_ok) {
                        // 故障恢复中断了 push 等待，LPLM 状态未变（仍是 holder），从存储获取
                        LOG(WARNING) << "[storage-fallback] S TryGetPushData failed: table=" << table_id << " page=" << page_id << " valid_node=" << valid_node << " need_storage=" << need_storage;
                        // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                        page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                    } else {
                        page = node_->try_fetch_page(table_id , page_id);
                        if (!page) {
                            LOG(WARNING) << "[storage-fallback] S page not in buffer after push: table=" << table_id << " page=" << page_id << " valid_node=" << valid_node;
                            // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                            page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                        }
                    }
                } else if (valid_node == -1) {
                    // 本节点已持有最新页面（恢复重试场景），直接从本地缓冲区获取
                    page = node_->try_fetch_page(table_id , page_id);
                    if (!page) {
                        // 页面可能已被 Pending handler 释放但 GPLM 未同步（LRPAnyUnlock 失败），从存储获取
                        LOG(WARNING) << "[storage-fallback] S valid_node==-1 page not in buffer: table=" << table_id << " page=" << page_id;
                        // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                        page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                    }
                }
            } else{
                // 等待加锁成功
                double wait_push_time = 0.0;
                int lock_result = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(table_id , &wait_push_time);
                if (lock_result == -1) {
                    // 故障恢复中断：LPLM 状态保持 granting，重新尝试整个加锁流程
                    VLOG(1) << "[IR Recovery] TryRemoteLockSuccess aborted for table=" << table_id << " page=" << page_id << ", retrying";
                    delete response; response = nullptr;
                    // 第 16 层：墙钟计总时限（TryRemoteLockSuccess 每轮内部
                    // cv 等待 500ms，不能用迭代计数估时）；每 30s 打一条
                    // stderr 观测；超时自愈三步后转确定性中止
                    const int64_t grant_waited_16 = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - grant_start_16).count();
                    if (grant_waited_16 >= grant_total_ms_16) {
                        fprintf(stderr, "[HCM-L16] S grant stall timeout table=%u page=%u node=%d waited_ms=%ld, force remote release\n",
                                static_cast<unsigned>(table_id), static_cast<unsigned>(page_id),
                                static_cast<int>(node_->node_id), static_cast<long>(grant_waited_16));
                        fflush(stderr);
                        LOG(WARNING) << "[IR Recovery] remote grant stalled past deadline (S): table="
                                     << table_id << " page=" << page_id << " waited_ms=" << grant_waited_16
                                     << ", force-releasing remote lock and aborting fetch";
                        ReleaseRemoteForForcedPage(table_id, page_id, false, /*force_forfeit_holders=*/true);
                        // smoke-020/022 实证：超时点常处于"X 已被 GPLM 授予
                        //（is_released=false）+ is_pending 等待推送"的冻结态，
                        // ResetStaleAbandonedFetch 的 is_released 守卫拒绝清理，
                        // 残留冻结锁死后续取页（只能等 GrantingStallLimitMs
                        // 兜底）。改用 ForceResetFrozenGrant 无条件清理冻结，
                        // 并对清出的远程份额立即补发 force release（manager
                        // 侧没收/推进等待队列，下一请求立即可被授予）。
                        {
                            int frozen_stolen = 0;
                            if (node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)
                                    ->ForceResetFrozenGrant(&frozen_stolen) &&
                                frozen_stolen != 0) {
                                ReleaseRemoteForForcedPage(table_id, page_id, frozen_stolen == 2,
                                                           /*force_forfeit_holders=*/true);
                            }
                        }
                        // 自愈清理完成，本地无中间状态——不隔离（见 Disarm 注释）
                        failure_guard.Disarm();
                        throw std::runtime_error("remote grant stalled past deadline (S)");
                    }
                    if (grant_waited_16 - grant_reported_16 >= 30000) {
                        grant_reported_16 = grant_waited_16;
                        fprintf(stderr, "[HCM-L16] S grant stall waiting table=%u page=%u node=%d waited_ms=%ld\n",
                                static_cast<unsigned>(table_id), static_cast<unsigned>(page_id),
                                static_cast<int>(node_->node_id), static_cast<long>(grant_waited_16));
                        fflush(stderr);
                    }
                    usleep(2000);  // 2ms backoff
                    need_full_retry = true;
                    continue;
                }
                if (lock_result == -2) {
                    // GPLM 已授予锁但 push 源故障，从存储获取数据
                    LOG(WARNING) << "[storage-fallback] S push source failed (-2): table=" << table_id << " page=" << page_id;
                    // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                    page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                } else {

                // 需要检查一下是否需要向同一批次获得锁的节点发送PushPage
                std::list<node_id_t> push_list = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->getPushList();
                while (!push_list.empty()){
                    PushPageToOther(table_id , page_id , push_list.back());
                    push_list.pop_back();
                }
                if (need_to_record){
                    node_->fetch_from_remote_cnt++;
                    node_->fetch_four_cnt++;
                }
                page = node_->try_fetch_page(table_id , page_id);
                if (!page) {
                    VLOG(1) << "[IR Recovery] Page not in buffer after lock success for table=" << table_id << " page=" << page_id << ", fetching from storage";
                    // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                    page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                }

                update_m.lock();
                tx_update_time += wait_push_time;
                update_m.unlock();

                } // end lock_result != -2
            }
            //! lock remote ok and unlatch local
            node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id, false);
            delete response; response = nullptr;
        } // end of need_full_retry loop
    }
    } catch (...) {
        local_lock->AbortHeldLatchOnFetchFailure(false);
        throw;
    }
    assert(page);
    assert(page->get_page_id().page_no == page_id && page->get_page_id().table_id == table_id);
    observation.Complete(!lock_remote);
    // LOG(INFO) << "fetch S Over " << "table_id = " << table_id << " page_id = " << page_id;

    return page;
}

Page* ComputeServer::rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id, bool need_to_record) {
    recovery_observation::FetchScope observation(table_id, page_id);
    // LOG(INFO) << "Fetching X , table_id = " << table_id << " page_id = " << page_id << " ";
    assert(page_id < ComputeNodeBufferPageSize);
    if (need_to_record){
        int k1 = cnt.fetch_add(1);
        if (k1 % 100 == 0){
            std::cout << "Lazy Fetch Page Cnt = " << k1 << "\n";
        }
        this->node_->fetch_allpage_cnt++;
    }
    
    // LOG(INFO) << "fetching X Page " << "table_id = " << table_id << " page_id = " << page_id;

    Page *page = nullptr;
    // 先在本地进行加锁

    auto* local_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id);
    // P0 修复（恢复后解隔离）：与 S 取页同理，恢复完成后允许重新取页
    if (HasCompletedRecovery() && local_lock->ClearFetchQuarantineAfterRecovery()) {
        LOG(WARNING) << "[IR Recovery] cleared fetch quarantine after recovery (X): table="
                     << table_id << " page=" << page_id;
    }
    FetchFailureGuard failure_guard(local_lock);
    int force_stolen = 0;
    bool lock_remote = local_lock->LockExclusive(&force_stolen);
    failure_guard.Arm();
    if (force_stolen != 0) {
        LOG(WARNING) << "[IR Recovery] stuck local latch force-released (X): table=" << table_id
                     << " page=" << page_id << " stolen_remote_mode=" << force_stolen;
        ReleaseRemoteForForcedPage(table_id, page_id, force_stolen == 2);
    }
    // 第 18 层补丁（early29）：与 S 路径同款——本节点非该页恢复管理者时
    // 本地有效表非权威，第 18 层校验无从判定，回滚本地 X 份额转 PXL。
    if (!lock_remote && recovery_epoch.load() > 0 &&
        get_recovery_node_id(table_id, page_id) != node_->node_id) {
        local_lock->AbortHeldLatchOnFetchFailure(true);
        lock_remote = true;
        LOG(WARNING) << "[IR Recovery] non-manager local fast path bypassed to PXL (X): table="
                     << table_id << " page=" << page_id;
    }
    // 第 14 层（fault-0088）：与 S 路径同理，持 latch 后的抛错点统一回滚
    try {
    if (!lock_remote){
        // 第 12 层缺陷修复（live-early11）：与 S 路径同一守卫——本地加锁成功
        // 不代表本地副本可信。GPLM 有效表 only-in-storage（HasAnyValid()=-1，
        // 仅故障恢复流标记）的页，本地缓冲副本可能是故障前旧世界共享副本
        //（页角色漂移，live-early11 C 节点 Phase 2 撤销的 X 取页经本地快路径
        // 命中陈旧副本，崩溃于 blink.cc:1034）。恢复窗口内延迟等待恢复完成；
        // 完成后从存储取权威副本；重分布页本地首取（缓冲缺席原路径会断言
        // 崩溃）一并覆盖。有界等待超时按防御等待契约抛错。
        bool only_in_storage_12 = false;
        GlobalValidInfo* vinfo_18 = nullptr;
        if (recovery_epoch.load() > 0 &&
            static_cast<size_t>(table_id) < global_valid_table_list_->size()) {
            GlobalValidTable* gvt_12 = (*global_valid_table_list_)[table_id];
            if (gvt_12 != nullptr) {
                GlobalValidInfo* vinfo_12 = gvt_12->GetValidInfo(page_id);
                if (vinfo_12 != nullptr && vinfo_12->HasAnyValid() == -1)
                    only_in_storage_12 = true;
                vinfo_18 = vinfo_12;
            }
        }
        // 第 18 层（early28 fault-0307）：与 S 路径同一守卫——本地副本必须
        // 在 GPLM 有效表里有效才可信。故障后权威可能已指向其他节点（实测
        // C 的 DELETE CONFIRMED_COMMITTED 后 B 用旧副本重插报 DUPLICATE_KEY），
        // 本节点无效即旧世界残留，改走存储权威源（取页内含日志 flush 等待
        // + replay 追平屏障），覆盖旧副本。
        const bool local_copy_stale_18 =
            !only_in_storage_12 && vinfo_18 != nullptr &&
            !vinfo_18->IsValid_NoBlock(node_->node_id);
        if (only_in_storage_12 || local_copy_stale_18) {
            const int wait_max_12 = [] {
                const char* env = ::getenv("HCM_IR_WAIT_MAX_MS");
                return env ? std::max(1000, atoi(env)) : 120000;
            }();
            int waited_12 = 0;
            while (!HasCompletedRecovery()) {
                if (++waited_12 >= wait_max_12)
                    throw std::runtime_error("only-in-storage local copy deferred past recovery deadline (X)");
                usleep(2000);
            }
            if (local_copy_stale_18) {
                LOG(WARNING) << "[IR Recovery] stale local copy bypassed, served from storage (X): table="
                             << table_id << " page=" << page_id;
            } else {
                LOG(WARNING) << "[IR Recovery] only-in-storage page served from storage (X): table="
                             << table_id << " page=" << page_id;
            }
            observation.AuthorizeStorage(true);
            std::string data_12 = rpc_fetch_page_from_storage(table_id, page_id, need_to_record);
            page = put_page_into_buffer(table_id, page_id, data_12.c_str(), 1, need_to_record);
        } else {
            if (need_to_record){
                node_->fetch_from_local_cnt++;
            }
            page = node_->fetch_page(table_id , page_id);
        }
    }else if(lock_remote){
        if (need_to_record){
            node_->lock_remote_cnt++;
        }

        // 故障容错重试循环
        bool need_full_retry = true;
        int rpc_retry_count = 0;
        // 第 16 层（early23 fault-0064/0066）：与 S 路径同款慢路径墙钟总时限
        //（实测 C 节点 commit 取 X 页与 A 节点 S 取页同页互等 8 分钟）。
        const int64_t grant_total_ms_16 = [] {
            const char* env = ::getenv("HCM_FETCH_GRANT_TOTAL_MS");
            return static_cast<int64_t>(env ? std::max(10000, atoi(env)) : 300000);
        }();
        const auto grant_start_16 = std::chrono::steady_clock::now();
        int64_t grant_reported_16 = 0;
        while (need_full_retry) {
            need_full_retry = false;

            // IR Recovery: 重试时需要重新设置 LPLM 的 is_granting 状态
            // 因为 SetRecoveryAbort 可能已经清理了 is_granting 和 lock
            node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->ResetForRetry(true);

            page_table_service::PXLockRequest request;
            page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
            ResponseGuard<page_table_service::PXLockResponse> response_guard{response};
            page_table_service::PageID* page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            request.set_allocated_page_id(page_id_pb);
            request.set_node_id(node_->node_id);

            // IR Recovery: 使用故障恢复路由
            node_id_t page_belong_node = get_recovery_node_id(table_id , page_id);

            // IR 锁重试循环
            bool ir_retry = true;
            int ir_waited_ms = 0;
            // R2 修复：恢复中页面可能长期持 IR 锁（Phase4 分析失败页按
            // fail-closed 永久保留），无限 1ms 轮询会让事务挂死且客户端
            // 无从感知。有界等待（默认 120s），超时抛错 → workload 层归类
            // 为确定性 ABORT（防御等待超时=事务失败，不是节点失败）。
            const int ir_wait_max_ms = [] {
                const char* env = ::getenv("HCM_IR_WAIT_MAX_MS");
                return env ? std::max(1000, atoi(env)) : 120000;
            }();
            // 第 10 层缺陷修复（live-early9）：与 S 路径同一守卫——恢复窗口
            // 内无 LSN 的 storage fallback 可能取回多代滞后的旧版本索引页
            // （页角色漂移，undo 导航命中非叶页崩溃于 blink.cc:1034），且
            // 覆盖本地正确副本。延迟到恢复完成后重取，超时抛错归确定性
            // ABORT。
            int recovery_fallback_waited_ms = 0;
            auto storage_fallback_or_defer = [&]() -> bool {
                if (HasCompletedRecovery()) return true;
                if (recovery_epoch.load() == 0) return true;  // 故障未检测：无恢复窗口语义
                if (++recovery_fallback_waited_ms >= ir_wait_max_ms)
                    throw std::runtime_error("storage fallback deferred past recovery deadline (X)");
                usleep(2000);
                need_full_retry = true;
                return false;
            };
            while (ir_retry) {
                local_lock->CheckFetchAllowed();
                ir_retry = false;
                if( page_belong_node == node_->node_id) {
                    this->page_table_service_impl_->LRPXLock_Localcall(&request, response);
                } else{
                    brpc::Controller cntl;
                    brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
                    page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
                    pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
                    if(cntl.Failed()){
                        rpc_retry_count++;
                        if (rpc_retry_count <= 3) {
                            LOG(WARNING) << "LRPXLock RPC failed for page " << page_id << " (retry " << rpc_retry_count << "): " << cntl.ErrorText();
                        }
                        delete response; response = nullptr;
                        int backoff_us = std::min(10000 * (1 << std::min(rpc_retry_count - 1, 5)), 500000);
                        usleep(backoff_us);
                        need_full_retry = true;
                        break;
                    }
                }
                if (!need_full_retry && response->ir_locked()) {
                    observation.Block("ir_management_or_recovery");
                    // P0 修复：X 路径此前无界轮询（S 路径已有界）——Phase 4
                    // 分析失败页的 IR 锁按 fail-closed 永久保留，无界等待
                    // 使事务挂死且客户端无从感知。与 S 路径同契约：有界等待
                    // 超时抛错 → workload 层归类确定性 ABORT
                    if (++ir_waited_ms >= ir_wait_max_ms)
                        throw std::runtime_error("IR lock wait deadline exceeded (page isolated by recovery analysis)");
                    usleep(1000);
                    response->Clear();
                    page_id_pb = new page_table_service::PageID();
                    page_id_pb->set_page_no(page_id);
                    page_id_pb->set_table_id(table_id);
                    request.set_allocated_page_id(page_id_pb);
                    ir_retry = true;
                }
            }
            if (need_full_retry) continue;
            observation.Unblock();

            bool need_fetch_from_storage = response->need_storage_fetch();
            observation.AuthorizeStorage(need_fetch_from_storage);

            if(!response->wait_lock_release()){
                node_id_t valid_node = response->newest_node();
                if (need_to_record){
                    node_->fetch_three_cnt++;
                }
                if (need_fetch_from_storage){
                    LLSN lsn = response->lsn();
                    assert(lsn != (LLSN)-1);
                    std::string data;
                    if (need_to_record){
                        data = rpc_fetch_page_from_storage_with_lsn(table_id , page_id , lsn , need_to_record);
                    }else {
                        // 第 19 层（smoke-014 C SIGABRT）：与 S 路径同款——
                        // need_storage_fetch 的第 10 层豁免仅对带 LSN 的 heap
                        // 表取页成立，派生表（BLink/FSM）无 LSN GetPage 在恢复
                        // 窗口内可取回页角色漂移的旧版索引页（blink.cc:991
                        // is_leaf 断言崩溃实证）。恢复窗口内延迟重走完整加锁
                        // 流程，有界超时抛错转确定性 ABORT
                        if (!storage_fallback_or_defer()) continue;
                        data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                    }
                    page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                } else if(valid_node != -1){
                    if (need_to_record){
                        node_->fetch_from_remote_cnt++;
                    }
                    bool push_ok = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryGetPushData(table_id);
                    if (!push_ok) {
                        VLOG(1) << "[IR Recovery] TryGetPushData aborted for X table=" << table_id << " page=" << page_id << ", fetching from storage";
                        // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                        page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                    } else {
                        page = node_->try_fetch_page(table_id , page_id);
                        if (!page) {
                            VLOG(1) << "[IR Recovery] Page not in buffer after X push for table=" << table_id << " page=" << page_id << ", fetching from storage";
                            // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                            page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                        }
                    }
                }else if (valid_node == -1) {
                    // S 锁升级 X 锁的情况
                    if (need_to_record){
                        node_->fetch_from_local_cnt++;
                    }
                    page = node_->try_fetch_page(table_id , page_id);
                    if (!page) {
                        VLOG(1) << "[IR Recovery] Page not in buffer (X valid_node==-1) for table=" << table_id << " page=" << page_id << ", fetching from storage";
                        // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                        page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                    }
                }else {
                    assert(false);
                }
            } else{
                // 等待加锁成功
                double wait_push_time = 0.0;
                int lock_result = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(table_id , &wait_push_time);
                if (lock_result == -1) {
                    VLOG(1) << "[IR Recovery] TryRemoteLockSuccess aborted for X table=" << table_id << " page=" << page_id << ", retrying";
                    delete response; response = nullptr;
                    // 第 16 层：墙钟计总时限；每 30s 打一条 stderr 观测；
                    // 超时自愈三步后转确定性中止
                    const int64_t grant_waited_16 = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - grant_start_16).count();
                    if (grant_waited_16 >= grant_total_ms_16) {
                        fprintf(stderr, "[HCM-L16] X grant stall timeout table=%u page=%u node=%d waited_ms=%ld, force remote release\n",
                                static_cast<unsigned>(table_id), static_cast<unsigned>(page_id),
                                static_cast<int>(node_->node_id), static_cast<long>(grant_waited_16));
                        // smoke-015 诊断：超时自愈前 dump GPLM 状态（holder/队列）
                        if (table_id < global_page_lock_table_list_->size() &&
                            (*global_page_lock_table_list_)[table_id] != nullptr) {
                            fprintf(stderr, "[HCM-DIAG] X stall-timeout table=%u page=%u mgr=%d gplm{%s}\n",
                                    static_cast<unsigned>(table_id), static_cast<unsigned>(page_id),
                                    static_cast<int>(get_recovery_node_id(table_id, page_id)),
                                    (*global_page_lock_table_list_)[table_id]->LR_GetLock(page_id)->DumpState().c_str());
                        }
                        fflush(stderr);
                        LOG(WARNING) << "[IR Recovery] remote grant stalled past deadline (X): table="
                                     << table_id << " page=" << page_id << " waited_ms=" << grant_waited_16
                                     << ", force-releasing remote lock and aborting fetch";
                        ReleaseRemoteForForcedPage(table_id, page_id, true, /*force_forfeit_holders=*/true);
                        // 同 S 路径：无条件清冻结残留并补发 force release
                        //（ResetStaleAbandonedFetch 的 is_released 守卫在
                        // "X 已授予+等推送"冻结态下拒绝清理，smoke-020 实证）
                        {
                            int frozen_stolen = 0;
                            if (node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)
                                    ->ForceResetFrozenGrant(&frozen_stolen) &&
                                frozen_stolen != 0) {
                                ReleaseRemoteForForcedPage(table_id, page_id, frozen_stolen == 2,
                                                           /*force_forfeit_holders=*/true);
                            }
                        }
                        // 自愈清理完成，本地无中间状态——不隔离（见 Disarm 注释）
                        failure_guard.Disarm();
                        throw std::runtime_error("remote grant stalled past deadline (X)");
                    }
                    if (grant_waited_16 - grant_reported_16 >= 30000) {
                        grant_reported_16 = grant_waited_16;
                        fprintf(stderr, "[HCM-L16] X grant stall waiting table=%u page=%u node=%d waited_ms=%ld\n",
                                static_cast<unsigned>(table_id), static_cast<unsigned>(page_id),
                                static_cast<int>(node_->node_id), static_cast<long>(grant_waited_16));
                        // smoke-015 诊断：stall 时 dump 本节点 GPLM 副本状态与 manager 路由
                        if (table_id < global_page_lock_table_list_->size() &&
                            (*global_page_lock_table_list_)[table_id] != nullptr) {
                            fprintf(stderr, "[HCM-DIAG] X stall table=%u page=%u mgr=%d gplm{%s}\n",
                                    static_cast<unsigned>(table_id), static_cast<unsigned>(page_id),
                                    static_cast<int>(get_recovery_node_id(table_id, page_id)),
                                    (*global_page_lock_table_list_)[table_id]->LR_GetLock(page_id)->DumpState().c_str());
                        }
                        fflush(stderr);
                    }
                    usleep(2000);
                    need_full_retry = true;
                    continue;
                }
                if (lock_result == -2) {
                    VLOG(1) << "[IR Recovery] TryRemoteLockSuccess push aborted for X table=" << table_id << " page=" << page_id << ", fetching from storage";
                    // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                    page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                } else {

                if (need_to_record){
                    node_->fetch_from_remote_cnt++;
                    node_->fetch_four_cnt++;
                }

                page = node_->try_fetch_page(table_id , page_id);
                if (!page) {
                    VLOG(1) << "[IR Recovery] Page not in buffer after X lock success for table=" << table_id << " page=" << page_id << ", fetching from storage";
                    // R2: push-failure/missing-buffer fallback. After recovery has
                    // completed the storage is authoritative (replay applied, Phase 3
                    // done), so a fallback fetch is verified-safe; during recovery the
                    // RequireStorageSource guard still rejects unverified fallbacks.
                    if (HasCompletedRecovery()) observation.AuthorizeStorage(true);
                    if (!storage_fallback_or_defer()) continue;
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                    page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                }
                
                update_m.lock();
                tx_update_time += wait_push_time;
                update_m.unlock();

                } // end lock_result != -2
            }
            //! lock remote ok and unlatch local
            node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id, true);
            delete response; response = nullptr;
        } // end of need_full_retry loop
    }
    } catch (...) {
        local_lock->AbortHeldLatchOnFetchFailure(true);
        throw;
    }
    assert(page);
    assert(page->get_page_id().page_no == page_id && page->get_page_id().table_id == table_id);
    observation.Complete(!lock_remote);


    return page;
}

// 第 14 层（fault-0088）：LPLM stuck 自愈强制回收泄漏 latch 后，向 GPLM
// 补发远程释放。只发 RPC，不触碰本地 LPLM/缓冲状态（本地侧已在
// ForceReleaseStuckLock 内清理）。LSN 上报与正常释放路径同规则
//（blink/FSM 页置 0，数据页读页头 LLSN；页缺席时不设置）。
void ComputeServer::ReleaseRemoteForForcedPage(table_id_t table_id, page_id_t page_id, bool xlock,
                                               bool force_forfeit_holders) {
    page_table_service::PAnyUnLockRequest request;
    page_table_service::PAnyUnLockResponse* response = new page_table_service::PAnyUnLockResponse();
    page_table_service::PageID* page_id_pb = new page_table_service::PageID();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_id(table_id);
    request.set_allocated_page_id(page_id_pb);
    request.set_node_id(node_->node_id);
    // L16：等待者超时自愈——请求 manager（RPC 或 Localcall 版）在 stale
    // 分支没收全部卡死 holders 并推进等待队列
    request.set_force_forfeit_holders(force_forfeit_holders);
    {
        Page* p = node_->try_fetch_page(table_id, page_id);
        if (p != nullptr) {
            if (table_id >= 10000 && table_id < 30000) {
                request.set_lsn(0);
            } else {
                request.set_lsn(reinterpret_cast<RmPageHdr*>(p->get_data())->LLSN_);
            }
            node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
        }
    }
    node_id_t page_belong_node = get_recovery_node_id(table_id , page_id);
    if( page_belong_node == node_->node_id) {
        this->page_table_service_impl_->LRPAnyUnLock_Localcall(&request, response);
    } else {
        brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
        page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
        brpc::Controller cntl;
        // L16 超时自愈路径（smoke-025 实证）：对端 manager 可能正忙于自身
        // IR（GPLM mutex 被长期持有/线程池饿死），同步 RPC 无默认超时会
        // 无限阻塞——卡死超时后的 throw 路径，IR 重试（D3）永远不触发，
        // 节点双停摆 25 分钟。设 5s 超时：失败仅记录（manager 侧残留由
        // L16-Forfeit 的下次触发与 GPLM 侧自愈兜底），本地继续 throw。
        cntl.set_timeout_ms(5000);
        pagetable_stub.LRPAnyUnLock(&cntl, &request, response, NULL);
        if (cntl.Failed()) {
            LOG(WARNING) << "[IR Recovery] ForceRelease remote unlock RPC failed: table=" << table_id
                         << " page=" << page_id << " xlock=" << xlock << " err=" << cntl.ErrorText();
        }
    }
    delete response; response = nullptr;
}

void ComputeServer::rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id) {
    // LOG(INFO) << "Releasing S Page " << "table_id = " << table_id << " page_id = " << page_id;
    LRLocalPageLock *lr_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id);
    // 第 14 层：锁已被 stuck 自愈强制回收（本地+远程均已处理），幂等跳过
    if (lr_lock->ConsumeForceReleased()) {
        LOG(WARNING) << "[IR Recovery] lazy release S skipped (latch already force-recovered): table="
                     << table_id << " page=" << page_id;
        return;
    }
    auto [unlock_remote, need_unpin] = lr_lock->tryUnlockShared();
    if (unlock_remote == -1) {
        // tryUnlockShared 内部竞争窗口消费了 force_released 标志（mutex 已释放）
        LOG(WARNING) << "[IR Recovery] lazy release S skipped (latch already force-recovered): table="
                     << table_id << " page=" << page_id;
        return;
    }

    // 对于 S 锁来说，这里无论是否 immediate release，都需要去检查 DestNodeIDNoBlock 并推送
    // 比如我现在本地两个 s 锁，放掉一个的时候，判断还不能立刻释放，但是可以推送页面了
    // TODO：页面推送的逻辑似乎可以放在 Pending 里？Pending 只要发现是读锁，就推送，写锁延迟到 release 推送
    if (lr_lock->getDestNodeIDNoBlock() != INVALID_NODE_ID){
        PushPageToOther(table_id , page_id , lr_lock->getDestNodeIDNoBlock());
        // 用完记得重新设置为 -1，防止下一轮误判了
        lr_lock->setDestNodeIDNoBlock(INVALID_NODE_ID);
    }

    if (unlock_remote == 0) {
        // 在这里 unpin，如果在后面 unpin 有 bug，可能 lock 减为 0 的时候会被 Replacer 锁定
        if (need_unpin){
            node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
        }
        lr_lock->UnlockShared();
        lr_lock->UnlockMtx();
        return;
    }
    
    lr_lock->UnlockShared();
    // 如果需要推送数据，先把数据页给推出去
    
    // rpc release page 
    // page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
    page_table_service::PAnyUnLockRequest request;
    page_table_service::PAnyUnLockResponse* response = new page_table_service::PAnyUnLockResponse();
    page_table_service::PageID* page_id_pb = new page_table_service::PageID();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_id(table_id);
    request.set_allocated_page_id(page_id_pb);
    request.set_node_id(node_->node_id);

    // P4 修复：携带页面当前 LSN，GPLM 解锁时更新 lsn_id（Phase 4 Redo 的目标 LSN 依据）
    // 修复：blink（10000-20000）/ FSM（20000-30000）页面头不是 RmPageHdr，
    // 直接 reinterpret_cast 会把 BLNodeHdr/S_FSMPageHeader 的中间字段当成
    // LLSN 读出垃圾值污染 GPLM。这两类页面的恢复由逻辑日志
    // （BLINKINSERT/BLINKDELETE/FSMUPDATE）重放保证，上报 LSN 置 0
    {
        Page* p = node_->try_fetch_page(table_id, page_id);
        if (p != nullptr) {
            if (table_id >= 10000 && table_id < 30000) {
                request.set_lsn(0);
            } else {
                request.set_lsn(reinterpret_cast<RmPageHdr*>(p->get_data())->LLSN_);
            }
            node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
        }
    }

    node_id_t page_belong_node = get_recovery_node_id(table_id , page_id);
    if( page_belong_node == node_->node_id) {
        // 如果是本地节点, 则直接调用
        this->page_table_service_impl_->LRPAnyUnLock_Localcall(&request, response);
    }
    else{
        // 如果是远程节点, 则通过RPC调用
        brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
        page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
        brpc::Controller cntl;
        pagetable_stub.LRPAnyUnLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
                LOG(WARNING) << "RPC Error: " << cntl.ErrorText();
            LOG(WARNING) << "Fail to unlock page " << page_id << " in remote page table";
        }
    }

    node_->getBufferPoolByIndex(table_id)->releaseBufferPage(table_id , page_id);
    lr_lock->UnlockRemoteOK();

    // LOG(INFO) << "Immediate Release S page , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();
    delete response; response = nullptr;
}

void ComputeServer::rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id) {
    // LOG(INFO) << "Release X Page , table_id = " << table_id << " page_id = " << page_id << " ";
    LRLocalPageLock *lr_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id);
    // 第 14 层：锁已被 stuck 自愈强制回收（本地+远程均已处理），幂等跳过
    if (lr_lock->ConsumeForceReleased()) {
        LOG(WARNING) << "[IR Recovery] lazy release X skipped (latch already force-recovered): table="
                     << table_id << " page=" << page_id;
        return;
    }
    int unlock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->tryUnlockExclusive();
    if (unlock_remote == -1) {
        // tryUnlockExclusive 内部竞争窗口消费了 force_released 标志（mutex 已释放）
        LOG(WARNING) << "[IR Recovery] lazy release X skipped (latch already force-recovered): table="
                     << table_id << " page=" << page_id;
        return;
    }
    if (unlock_remote == 0){
        // 对于x 锁来说，由于同一时间单节点只能持有一个，因此放锁的时候，如果不需要等待，dest_node_id 一定是 -1
        // 协议不变式降级为观测+自愈（live-smoke-002/004 调试结论）：S 计数>1
        // 期间的等待者记录（dest）在 S→X 升级授予后可能残留且 is_pending=false，
        // 原 assert 会崩溃整个节点；改为按慢路径同款语义完成推送后继续本地
        // 释放——推送幂等（目标死亡由 NotifyPushPage 跳过），观测不静默。
        if (lr_lock->getDestNodeIDNoBlock() != INVALID_NODE_ID) {
            LOG(ERROR) << "[LPLM] X fast-path dest residue healed: table="
                       << table_id << " page=" << page_id
                       << " dest=" << lr_lock->getDestNodeIDNoBlock();
            PushPageToOther(table_id , page_id , lr_lock->getDestNodeIDNoBlock());
            lr_lock->setDestNodeIDNoBlock(INVALID_NODE_ID);
        }
        // LOG(INFO) << "Lazy Release X , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();
        assert(lr_lock->getLock() == EXCLUSIVE_LOCKED);
        // 对于写锁来说，一定是需要 unpin 的
        node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        return ;   
    }

    if (lr_lock->getDestNodeIDNoBlock() != INVALID_NODE_ID){
        // 释放 X 锁，需要把页面给刷下去
        // B+ 树不需要等待
        PushPageToOther(table_id , page_id , lr_lock->getDestNodeIDNoBlock());
        lr_lock->setDestNodeIDNoBlock(INVALID_NODE_ID);
    }

    lr_lock->UnlockExclusive();

    assert(unlock_remote == 2);
    // page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
    page_table_service::PAnyUnLockRequest unlock_request;
    page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
    page_table_service::PageID* page_id_pb = new page_table_service::PageID();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_id(table_id);
    unlock_request.set_allocated_page_id(page_id_pb);
    unlock_request.set_node_id(node_->node_id);

    // P4 修复：携带页面当前 LSN，GPLM 解锁时更新 lsn_id（Phase 4 Redo 的目标 LSN 依据）
    // 修复：blink（10000-20000）/ FSM（20000-30000）页面头不是 RmPageHdr，
    // 误读会污染 GPLM。这两类页面由逻辑日志重放恢复，上报 LSN 置 0
    {
        Page* p = node_->try_fetch_page(table_id, page_id);
        if (p != nullptr) {
            if (table_id >= 10000 && table_id < 30000) {
                unlock_request.set_lsn(0);
            } else {
                unlock_request.set_lsn(reinterpret_cast<RmPageHdr*>(p->get_data())->LLSN_);
            }
            node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
        }
    }

    node_id_t page_belong_node = get_recovery_node_id(table_id , page_id);
    if( page_belong_node == node_->node_id) {
        // 如果是本地节点, 则直接调用
        this->page_table_service_impl_->LRPAnyUnLock_Localcall(&unlock_request, unlock_response);
    }
    else{
        // 如果是远程节点, 则通过RPC调用
        brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
        page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
        brpc::Controller cntl;
        pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
        if(cntl.Failed()){
                LOG(WARNING) << "RPC Error: " << cntl.ErrorText();
            LOG(WARNING) << "Fail to unlock page " << page_id << " in remote page table";
        }
    }

    // 不需要写回到存储层，能到这里的，说明页面肯定会发给别人
    // 释放掉自己的缓冲区
    node_->getBufferPoolByIndex(table_id)->releaseBufferPage(table_id , page_id);
    lr_lock->UnlockRemoteOK();

    // LOG(INFO) << "Immediate Release X Page , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();

    // delete response; response = nullptr;
    delete unlock_response;

    return;
}
