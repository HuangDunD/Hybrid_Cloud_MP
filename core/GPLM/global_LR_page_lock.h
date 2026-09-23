#pragma once
#include "common.h"
#include "config.h"
#include "compute_node/compute_node.pb.h"
#include "GPLM/global_valid_table.h"

#include <iostream>
#include <list>
#include <algorithm> 
#include <mutex>
#include <atomic>
#include <cassert>
#include <brpc/channel.h>
#include <queue>
#include <set>
#include <bthread/butex.h>
#include <unistd.h>

struct LRRequest{
    node_id_t node_id;  // 请求的节点id
    table_id_t table_id;
    bool xlock;         // 请求的类型
};

class LR_GlobalPageLock{
private:
    page_id_t page_id;                      // 数据页id
    lock_t lock;                            // 读写锁, 记录当前数据页的ref
    std::list<node_id_t> hold_lock_nodes;   // 持有锁的节点
    brpc::Channel** compute_channels;       // 用于和计算节点通信的channel
    bool is_pending = false;                // 是否正在pending
    int src_node_id;    // 在 SetComputeNodePending 阶段推送数据的节点 ID
    LLSN lsn_id = 0;
    std::atomic<bool> ir_locked{false};      // Instance Recovery 锁
    std::set<node_id_t> stale_unlock_tombstones_; // R2c D-2：stale unlock 撤销者（拦截过时汇报重建）

private:
    std::list<LRRequest> request_queue;
    int s_request_num = 0;
    int x_request_num = 0;
    bthread::Mutex mutex;
    
    // 验证 SetComputeNodePending 阶段和真正 PushPage 选择推送页面的节点一致
    // node_id_t pending_src_node_id = -1;

public:
    bool getIsPendingNoBlock(){
        return is_pending;
    }

    LLSN getLsnIDNoBlock() const {
        return lsn_id;
    }
    void setLsnIDNoBlock(LLSN newest_lsn_id){
        lsn_id = newest_lsn_id;
    }

    void mutexLock(){
        mutex.lock();
    }
    void mutexUnlock(){
        mutex.unlock();
    }

    LR_GlobalPageLock(page_id_t pid, brpc::Channel** c) {
        page_id = pid;
        lock = 0;
        compute_channels = c;
        is_pending = false;
    }
    
    void Reset(){
        lock = 0;
        hold_lock_nodes.clear();
        stale_unlock_tombstones_.clear(); // R2c D-2：条目复位连带清 tombstone
        request_queue.clear();
        s_request_num = 0;
        x_request_num = 0;
        is_pending = false;
        src_node_id = INVALID_NODE_ID;
        ir_locked = false;
    }

    // ==================== Instance Recovery 锁 ====================
    void SetIRLock() { ir_locked = true; stale_unlock_tombstones_.clear(); }   // R2c D-2：新故障代开表，tombstone 重新计
    void ClearIRLock() { ir_locked = false; stale_unlock_tombstones_.clear(); } // R2c D-2：本代重建结束，tombstone 使命完成
    bool IsIRLocked() const { return ir_locked; }
    bool IsIRLockedNoBlock() const { return ir_locked; }

    // 清理故障节点在本页面的所有状态，需要持有 mutex
    // 返回值: 0=故障节点不是 holder, 1=故障节点持有 S 锁, 2=故障节点持有 X 锁
    int CleanFailedNodeNoBlock(node_id_t failed_node_id) {
        int result = 0;
        // 从 hold_lock_nodes 移除
        auto it = std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), failed_node_id);
        if (it != hold_lock_nodes.end()) {
            hold_lock_nodes.erase(it);
            if (lock == EXCLUSIVE_LOCKED) {
                lock = 0;
                result = 2;  // 持有 X 锁
            } else if (lock > 0) {
                --lock;
                result = 1;  // 持有 S 锁
            }
        }
        // 清空整个请求队列（存活节点的请求将通过 LPLM wakeup 重试）
        request_queue.clear();
        s_request_num = 0;
        x_request_num = 0;
        // 重置 src_node_id
        src_node_id = INVALID_NODE_ID;
        // 重置 pending 状态
        is_pending = false;
        return result;
    }

    // 在 IR 恢复期间，存活节点汇报其持有的锁状态
    // 在 mutex 保护下调用
    void RecoverAddHolder(node_id_t node_id, bool exclusive) {
        // R2c D-2：该节点的释放已先于本汇报到达权威（stale unlock
        // tombstone）——汇报快照过时，拒绝重建，否则权威残留幽灵份额
        //（matrix-early-002 page 32 实证：holders=[2] 挡 X 排队 45s/轮）。
        if (IsTombstonedNoBlock(node_id)) {
            LOG(WARNING) << "[RecoverAddHolder] node " << node_id
                         << " report rejected (stale-unlock tombstoned, page " << page_id
                         << ") — release arrived before report";
            return;
        }
        // 确保不重复添加
        if (std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) != hold_lock_nodes.end()) {
            return;
        }
        if (exclusive) {
            assert(lock == 0 && hold_lock_nodes.empty());
            lock = EXCLUSIVE_LOCKED;
        } else {
            assert(lock != EXCLUSIVE_LOCKED);
            lock++;
        }
        hold_lock_nodes.push_back(node_id);
    }

    std::list<node_id_t> get_hold_lock_nodes() {
        return hold_lock_nodes;
    }

    // smoke-015 诊断（L16 stall 死锁定位）：dump GPLM 权威锁状态。
    // 供 stall 观测点调用；自身加锁，调用方不得持有本页 mutex
    std::string DumpState() {
        std::lock_guard<bthread::Mutex> guard(mutex);
        std::string s = "lock=";
        s += std::to_string(lock);
        s += " pending=" + std::to_string(is_pending ? 1 : 0);
        s += " ir=" + std::to_string(ir_locked.load() ? 1 : 0);
        s += " holders=[";
        for (auto n : hold_lock_nodes) s += std::to_string(n) + ",";
        s += "] queue=[";
        for (auto& r : request_queue)
            s += std::to_string(r.node_id) + (r.xlock ? ":X," : ":S,");
        s += "]";
        return s;
    }

    bool is_request_queue_empty() const {
        return request_queue.empty();
    }

    void add_hold_lock_node(node_id_t node_id){
        // 如果hold_lock_nodes中已经有了这个node_id, 则不再添加, 否则添加
        // 无需加锁, 因为这个函数只会在持有mutex的函数调用
        if (std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) != hold_lock_nodes.end()) {
            VLOG(1) << "[IR Recovery] add_hold_lock_node: node " << node_id << " already in hold_lock_nodes for page " << page_id;
            return;
        }
        hold_lock_nodes.push_back(node_id);
    }

    // mutex 在调用这个函数之前已经被持有
    static void PendingRPCDone(compute_node_service::PendingResponse* response, brpc::Controller* cntl) {
        // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
        std::unique_ptr<compute_node_service::PendingResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            // RPC失败了. response里的值是未定义的，勿用。
            LOG(WARNING) << "PendingRPC failed: " << cntl->ErrorText();
        } else {
            // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        }
        // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
    }

    static void LockSuccessRPCDone(compute_node_service::LockSuccessResponse* response, brpc::Controller* cntl) {
        // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
        std::unique_ptr<compute_node_service::LockSuccessResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            // RPC失败了. response里的值是未定义的，勿用。
            LOG(WARNING) << "LockSuccessRPC failed: " << cntl->ErrorText();
        } else {
            // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        }
        // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
    }

    static void NotifyPushPageRPCDone(compute_node_service::NotifyPushPageResponse* response, brpc::Controller* cntl) {
        std::unique_ptr<compute_node_service::NotifyPushPageResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            LOG(WARNING) << "NotifyPushPageRPC failed: " << cntl->ErrorText();
        }
    }

    // XPending 代表当前持有锁的类型
    // 第一个无法满足加上锁的节点会调用这个函数，通知所有持有锁的节点Pending，让他们尽快让出锁
    void SetComputenodePending(node_id_t n, bool XPending, table_id_t table_id, GlobalValidInfo* valid_info) {
        // 构造request
        compute_node_service::PendingRequest request;
        compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);

        int trans_node_id = -1; // 从哪个节点发
        if(XPending) {
            assert(hold_lock_nodes.size() == 1);
            // 只有一个节点会持有X 锁，且下一轮拿到锁的一定不是本节点，所以 trans_node_id 大胆设置成队首
            trans_node_id = hold_lock_nodes.front(); 
            request.set_pending_type(compute_node_service::PendingType::XPending);
        }
        else {
            request.set_pending_type(compute_node_service::PendingType::SPending);
            assert(hold_lock_nodes.size() >= 1); 
            // 如果下一轮持有锁的节点 n，本轮也持有锁，那就不需要 PushPage，所以把 trans_node_id 设置为-1，这样就没人会 PushPage 了
            if(!valid_info->IsValid(n)){
                trans_node_id = valid_info->get_newest_nodeID();
            }
        }
        assert(src_node_id == INVALID_NODE_ID);
        // 把这一轮向谁 Push 了数据页给记录下来
        src_node_id = n;

        // Debug：记录下当前选择的节点
        // pending_src_node_id = trans_node_id;
        // 向所有的持有锁的计算节点发送释放锁请求
        std::vector<brpc::CallId> cids;
        for(auto node_id : hold_lock_nodes){
            /*
                这里的 node_id == n continue 是一个很精妙的设计，如果下一轮就有自己的话，不需要向自己发送 Pending 请求
                那么问题就来了：如果不给自己 Pending，那如果没人 Pending 了导致没人执行 LRPAnyUnlock 怎么办？
                走到这里两个路径(先不考虑 LRPAnyLock 的那个，那个我觉得有点问题，后边改改)
                ⚠️⚠️：先明确一个观点，能走到这个函数里的 n，一定是下一轮能拿到所有权的节点，具体自己去看 LockExclusive/Shared
                1. LockExclusive：
                    1.1 如果 hold_lock_nodes 只有本节点的话，那一定是共享锁，因为如果是排他的就不需要调用 LRPXLock 了
                    1.2 为了隔绝上面这种情况，所以在 LockExclusive 也做了处理，上面这种情况直接升级锁就行
                    1.3 所以走到这里的，hold_lock_nodes 一定至少包含一个其他主节点，所以不用怕没人 Pending
                2. LockShared：更简单了，走到这个函数内只有一个情况：当前持有者是排他锁，且请求队列为空
                    2.1 当前持有者是排他的，如果这个持有者是它自己的话，那一定不会向 RemoteServer 请求锁，所以一定不会触发这个 node_id == n
                总的来说：如果 node_id == n，那跳过即可， node_id == n 的节点会阻塞在 TryRemoteLockSuccess里，由另外一个主节点解锁的时候 NotifyLockSuccess
                而 node_id == n 的节点不会删除
            */
            if(node_id == n) continue; // 不需要向自己发送请求

            if (node_id == trans_node_id){
                request.set_dest_node_id(n);
            }else{
                request.set_dest_node_id(-1);
            }
            
            // LOG(INFO) << "Send Pending to node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id << " dest node : " << request.dest_node_id();
            brpc::Channel* channel = compute_channels[node_id];

            compute_node_service::ComputeNodeService_Stub computenode_stub(channel);
            brpc::Controller* cntl = new brpc::Controller();
            compute_node_service::PendingResponse* response = new compute_node_service::PendingResponse();
            cids.push_back(cntl->call_id());
            computenode_stub.Pending(cntl, &request, response, 
                brpc::NewCallback(PendingRPCDone, response, cntl));
        }
    
        // 在这里释放mutex
        mutex.unlock();
        if (NetworkLatency != 0)  usleep(NetworkLatency);
    }

    void UnlockMutex(){
        mutex.unlock();
    }

    // n 请求的节点
    // XLock：请求的节点是否是 X 锁
    void NotifyPushPage(table_id_t table_id , node_id_t dest_node_id , node_id_t src_node_id){
        compute_node_service::NotifyPushPageRequest request;
        compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);

        request.set_src_node_id(src_node_id);
        request.add_dest_node_ids(dest_node_id);
        assert(src_node_id != dest_node_id);

        brpc::Channel* channel = compute_channels[src_node_id];
        compute_node_service::ComputeNodeService_Stub computenode_stub(channel);

        brpc::Controller* cntl = new brpc::Controller();
        compute_node_service::NotifyPushPageResponse* response = new compute_node_service::NotifyPushPageResponse();
        computenode_stub.NotifyPushPage(cntl, &request, response, NULL);
        if (cntl->Failed()){
            LOG(WARNING) << "NotifyPushPage failed for table=" << table_id << " page=" << page_id
                       << " src=" << src_node_id << " dest=" << dest_node_id
                       << " error: " << cntl->ErrorText();
            // 不崩溃 - 源节点可能已故障，等待故障恢复流程处理
        }
        delete cntl;
        delete response;
    }

    bool LockShared(node_id_t node_id, table_id_t table_id, GlobalValidInfo* valid_info) {
        mutex.lock();
        // IR Recovery 安全检查：如果节点已经是 holder，说明是 recovery abort 后的重复请求
        if (std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) != hold_lock_nodes.end()) {
            VLOG(1) << "[IR Recovery] LockShared: node " << node_id << " already holds page " << page_id << ", treating as success";
            return true;
        }
        // 可以直接获得锁
        if(lock != EXCLUSIVE_LOCKED && request_queue.empty()){
            // 可以直接上锁
            // // // // LOG(INFO) << "LockShared Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << node_id;
            lock++;
            add_hold_lock_node(node_id);
            assert(lock == hold_lock_nodes.size());
            assert(1 == std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id));
            return true;
        }
        else if(request_queue.empty()){
            // 请求队列为空，且当前持有者是排他锁
            assert(lock == EXCLUSIVE_LOCKED);
            LRRequest r{node_id, table_id, 0};
            request_queue.push_back({r});
            is_pending = true;
            s_request_num++;
            assert(s_request_num==1);
            SetComputenodePending(node_id, true, table_id, valid_info); // 这里被X锁占用，所以需要释放X锁
            return false;
            // mutex.unlock(); // 在SetComputenodePending()中会释放mutex
        }
        else if(!request_queue.empty()){
            // 队列不空, 就一定有一个正在pending
            assert(is_pending == true);
            assert(lock != 0);
            // IR Recovery: 防止同一节点重复请求（recovery abort 重试可能导致重复入队）
            bool already_queued = std::any_of(request_queue.begin(), request_queue.end(),
                [node_id](const LRRequest& r) { return r.node_id == node_id; });
            if (!already_queued) {
                LRRequest r{node_id, table_id, 0};
                request_queue.push_back({r});
                s_request_num++;
            }
            mutex.unlock();
            return false;
        }
        return false;
    }

    bool LockExclusive(node_id_t node_id, table_id_t table_id, GlobalValidInfo* valid_info) {
        mutex.lock();
        // IR Recovery 安全检查：如果节点已经持有 X 锁，说明是 recovery abort 后的重复请求
        if (lock == EXCLUSIVE_LOCKED && !hold_lock_nodes.empty() && hold_lock_nodes.front() == node_id) {
            VLOG(1) << "[IR Recovery] LockExclusive: node " << node_id << " already holds X on page " << page_id << ", treating as success";
            return true;
        }
        if(lock != 0 && is_pending == false) {
            assert(request_queue.empty()); 
            assert(s_request_num == 0 && x_request_num == 0);
            // 如果当前数据页已经有了读锁, 且等待队列第一个就是写锁，那直接升级就行
            if(lock == 1 && hold_lock_nodes.front() == node_id){
                // // // // LOG(INFO) << "LOCK UPDATE SUCCESS: table_id: "<< table_id<< "page_id:" << page_id << " in node: " << node_id;
                lock = EXCLUSIVE_LOCKED;
                return true;
            }
            // 如果不可以升级, 则需要使用SetComputenodePending()释放已持有节点的锁
            LRRequest r{node_id, table_id, 1};
            request_queue.push_back({r});
            is_pending = true;
            bool xpending = false;
            x_request_num++;
            if(lock == EXCLUSIVE_LOCKED){
                assert(hold_lock_nodes.size() == 1);
                assert(node_id != hold_lock_nodes.front());
                xpending = true;
            }
            SetComputenodePending(node_id, xpending,table_id,valid_info);
            // mutex.unlock(); // 在SetComputenodePending()中会释放mutex
        }
        else if(lock == 0 && is_pending == false){
            // 可以直接上锁
            assert(request_queue.empty()); 
            assert(s_request_num == 0 && x_request_num == 0);
            lock = EXCLUSIVE_LOCKED;
            add_hold_lock_node(node_id);
            assert(hold_lock_nodes.size() == 1);
            assert(node_id == hold_lock_nodes.front());
            return true;
        }
        else{
            // 如果 Pending 的话，就直接加入到等待队列中
            assert(is_pending);
            assert(request_queue.size() > 0);
            // IR Recovery: 防止同一节点重复请求（recovery abort 重试可能导致重复入队）
            bool already_queued = std::any_of(request_queue.begin(), request_queue.end(),
                [node_id](const LRRequest& r) { return r.node_id == node_id; });
            if (!already_queued) {
                LRRequest r{node_id, table_id, 1};
                request_queue.push_back({r});
                x_request_num++;
            }
            mutex.unlock();
        }
        return false;
    }

    bool UnlockShared(node_id_t node_id) {
        mutex.lock();
        assert(lock > 0);
        assert(lock != EXCLUSIVE_LOCKED);
        assert(hold_lock_nodes.size() == lock);
        --lock;
        hold_lock_nodes.remove(node_id);
        mutex.unlock();
        return true;
    }

    bool UnlockExclusive(node_id_t node_id){
        mutex.lock();
        lock = 0;
        hold_lock_nodes.remove(node_id);
        assert(hold_lock_nodes.size() == 0);
        mutex.unlock();
        return true;
    }

    void SendComputenodeLockSuccess(table_id_t table_id , GlobalValidInfo *valid_info , bool push_or_pull){
        // 这里有mutex
        bool xlock = (lock == EXCLUSIVE_LOCKED);
        // 向所有的持有锁的计算节点发送加锁成功请求
        std::vector<brpc::CallId> cids;
        assert(!hold_lock_nodes.empty());

        if (NetworkLatency != 0)  usleep(NetworkLatency);

        // 此时 hold_lock_nodes 都是下一轮能够获取到锁的页面
        for(auto node_id : hold_lock_nodes){
            // 构造request
            compute_node_service::LockSuccessRequest request;
            compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            request.set_allocated_page_id(page_id_pb);
            request.set_xlock_succeess(xlock); 
            
            bool found = false;
            std::stringstream ss;
            if (NetworkLatency != 0)  usleep(NetworkLatency);
            if (node_id == src_node_id){
                for (auto node_id_ : hold_lock_nodes){
                    // 如果是第一轮已经 Push 的节点，跳过，不需要向他 Push 页面
                    if (node_id_ == src_node_id){
                        continue;
                    }
                    request.add_dest_node_ids(node_id_);
                    ss << node_id_ << " ";
                }
                // 有且只有一种情况：本节点之前加了 S 锁，升级成 X 锁
                if (valid_info->IsValid_NoBlock(src_node_id)){
                    request.set_is_newest(true);
                }else {
                    request.set_is_newest(false);
                }
                src_node_id = INVALID_NODE_ID;
            }else{
                request.set_is_newest(false);
            }

            // // LOG(INFO) << "Send LockSuccess , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_id << " IsValid : " << request.is_newest();

            
            // 发送请求
            brpc::Channel* channel = compute_channels[node_id];
            compute_node_service::ComputeNodeService_Stub computenode_stub(channel);
            brpc::Controller* cntl = new brpc::Controller();
            compute_node_service::LockSuccessResponse* response = new compute_node_service::LockSuccessResponse();
            cids.push_back(cntl->call_id());
            computenode_stub.LockSuccess(cntl, &request, response, 
                brpc::NewCallback(LockSuccessRPCDone, response, cntl));
        }

        return;
        // 等待所有的请求完成
        // for(auto cid : cids){
        //     brpc::Join(cid);
        // }
    }

    /*
        让渡的策略：
        1. 如果现在没人持有锁，那就把这个锁让给下一个请求的，如果下一个请求的是读锁，那就一次性把读锁全都给请求队列中的读锁
        2. 如果现在有人持有读锁，那让渡的策略是，除非只有一个读锁，且下一个请求的是这个独占读锁的相同节点的写锁请求，否则不给所有权
    */
    bool TransferControl(table_id_t table_id){
        // mutex is hold here
        assert(lock != EXCLUSIVE_LOCKED);
        if(request_queue.empty()){
            // 主动释放锁
            assert(!is_pending);
            assert(s_request_num==0 && x_request_num==0);
            // mutex.unlock();
            return false;
        }
        // judge if lock success
        if(lock == 0) {
            // 进入到这里，说明需要在下一轮授予锁了
            // 必须是在 Pending 阶段才能释放锁，
            assert(is_pending);
            assert(hold_lock_nodes.size()==0);
            
            // 验证：队首的一定是之前选择中转的那个节点，因为它是第一个进入的
            auto request = request_queue.front();
            request_queue.pop_front();
            assert(src_node_id != INVALID_NODE_ID);
            assert(request.node_id == src_node_id);
            if(request.xlock){                      
                lock = EXCLUSIVE_LOCKED;
                add_hold_lock_node(request.node_id);
                // // // LOG(INFO) << "Next Round X, table_id = " << table_id << " page_id = " << page_id << " Next Node : "  << request.node_id;
                x_request_num--;
                // is_pending 是在 LockShared/Exclusive 里面设置为 true 的，表示 pending 开始，别人来了无法直接获取锁
                // 在这里设置为 false，表示这轮授予锁结束了，该拿到锁的节点拿到锁了
                is_pending = false;
            }
            else{
                // 授予队列首部共享锁
                lock++;
                add_hold_lock_node(request.node_id);
                // // // // LOG(INFO) << "Transfer Shared Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << request.node_id << " lock: " << lock;
                s_request_num--;
                // 遍历队列找出其他S锁一次授予
                if(s_request_num > 0){
                    for (auto it = request_queue.begin(); it != request_queue.end();) {
                        if (it->xlock == false) {
                            lock++;
                            add_hold_lock_node(it->node_id);
                            // // // // LOG(INFO) << "Transfer Shared Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << it->node_id << " lock: " << lock;
                            s_request_num--;
                            it = request_queue.erase(it); // 并返回下一个元素的迭代器
                        } else {
                            ++it; // 继续遍历下一个元素
                        }
                    }
                }

                std::stringstream ss;
                std::list<node_id_t> cp = hold_lock_nodes;
                while (!cp.empty()){
                    ss << cp.front() << " ";
                    cp.pop_front();
                }
                // // // LOG(INFO) << "Next Round S, table_id = " << table_id << " page_id = " << page_id << " Next Nodes : " << ss.str();
                is_pending = false;
                assert(s_request_num==0);
            }
        }
        else{
            // 走到这里说明上一轮持锁的还没全部释放完，先等着吧
            assert(is_pending);
            assert(hold_lock_nodes.size()>0);
            auto request = request_queue.front();
            // 这种情况很特殊，举个例子：
            /*
                1. Node0 和 Node1 此时都持有 S 锁，然后 Node0 想要 X 锁，它向 RemoteServer 发送了 Lock 请求
                2. 由于 Node0 本轮已经持有锁，所以 RemoteServer 只会向 Node1 发送 Pending，Node1 释放锁就会走到这个地方
                3，此时直接授予所有权即可
            */
            if(lock == 1 && hold_lock_nodes.front() == request.node_id){
                lock = EXCLUSIVE_LOCKED;
                x_request_num--;
                is_pending = false;
                request_queue.pop_front();
                // // // LOG(INFO) << "Transfer Exclusive Update Success: table_id = "<< table_id<< " page_id = " << page_id << " in node: " << request.node_id << " lock: " << lock;
            }
            else{
                // // // LOG(INFO) << "TransferControl Failed , Still Running , table_id = " << table_id << " page_id = " << page_id << " lock = " << lock;
                // mutex.unlock();
                return false;
            } 
        }
        return true;
    }



    void TransferPending(table_id_t table_id, std::atomic<int>& immedia_transfer, GlobalValidInfo* valid_info) {
        // mutex is hold here, need unlock in this fun
        // judge if need pending
        if(is_pending || request_queue.empty()) {
            // 上一个pending没结束或者没有下一个pending
            mutex.unlock();
            return;
        }
        else{
            // // // LOG(INFO) << "TransferPending , table_id = " << table_id << " page_id = " << page_id << "\n";
            immedia_transfer++;
            // 判断下一个pending
            auto request = request_queue.front();
            if(request.xlock){
                assert(x_request_num > 0);
                // IR Recovery 安全检查：跳过重复请求（recovery abort 重试导致）
                if(lock == EXCLUSIVE_LOCKED && hold_lock_nodes.size() == 1 && request.node_id == hold_lock_nodes.front()){
                    VLOG(1) << "[IR Recovery] TransferPending: skipping stale X-request from node "
                                 << request.node_id << " (already holds X on page " << page_id << ")";
                    request_queue.pop_front();
                    x_request_num--;
                    immedia_transfer--;
                    TransferPending(table_id, immedia_transfer, valid_info);
                    return;
                }
                // 需要设置下一轮的 is_pending
                is_pending = true;
                bool xpending = false;
                if(lock == EXCLUSIVE_LOCKED){
                    assert(hold_lock_nodes.size() == 1);
                    xpending = true;
                }
                // 在这里unlock
                // 这个是为了找到下一轮的持锁
                SetComputenodePending(request.node_id, xpending, table_id, valid_info);
                return;
            }
            else{
                assert(s_request_num > 0);
                assert(lock == EXCLUSIVE_LOCKED);
                assert(hold_lock_nodes.size() == 1);
                // IR Recovery 安全检查：跳过重复请求
                if(request.node_id == hold_lock_nodes.front()){
                    VLOG(1) << "[IR Recovery] TransferPending: skipping stale S-request from node "
                                 << request.node_id << " (already holds X on page " << page_id << ")";
                    request_queue.pop_front();
                    s_request_num--;
                    immedia_transfer--;
                    TransferPending(table_id, immedia_transfer, valid_info);
                    return;
                }
                is_pending = true;
                bool xpending = true;
                // 在这里unlock
                SetComputenodePending(request.node_id, xpending, table_id, valid_info);
                return;
            }
        }
        return;
    }

    bool CheckIsHoldNoBlock(node_id_t node_id){
        return std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) != hold_lock_nodes.end();
    }

    // R2c D-2 修复（r2c-20260923-matrix-early-002 幽灵 S）：
    // 权威侧 tombstone——stale unlock（UnlockAny 到达时该节点不在 holders）
    // 记录撤销者，RecoverAddHolder 拒绝为已撤销节点重建份额。
    // 根因：Phase 2 汇报是"扫描时刻快照"，与汇报者的事务释放并发——
    // 释放先到（stale 无操作）＋汇报后到（重建）＝权威残留幽灵 S，
    // 本地已无记录永不释放，后续 X 授权被挡 45s/轮直至 wall budget
    //（5d820da 收窄 L16 没收范围后失去兜底，早锚点冷缓存必踩）。
    // 拒绝安全性：stale unlock 到达且无 holder ⇒ 权威从未持有或已移除
    // 该份额 ⇒ 之后到达的同节点汇报必然是快照过时（本地已清）。
    // 生命周期：随 IR 代清理（SetIRLock/ClearIRLock）与条目复位。
    bool IsTombstonedNoBlock(node_id_t node_id) const {
        return stale_unlock_tombstones_.count(node_id) > 0;
    }
    void TombstoneNoBlock(node_id_t node_id) {
        stale_unlock_tombstones_.insert(node_id);
    }
    void ClearTombstonesNoBlock() { stale_unlock_tombstones_.clear(); }

    bool UnlockAnyNoBlock(node_id_t node_id){
        bool need_validate = false;
        // IR Recovery 安全检查：节点可能已被 CleanFailedNodeNoBlock/Reset 移除
        auto it = std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id);
        if (it == hold_lock_nodes.end()) {
            LOG(WARNING) << "[UnlockAnyNoBlock] node " << node_id
                         << " not in hold_lock_nodes (page " << page_id << "), likely post-recovery stale unlock";
            TombstoneNoBlock(node_id); // R2c D-2：拦截同节点过时汇报重建
            return false;
        }
        if(lock == EXCLUSIVE_LOCKED){
            lock = 0;
            hold_lock_nodes.erase(it);
            need_validate = true;
        }
        else{
            --lock;
            hold_lock_nodes.erase(it);
        }
        return need_validate;
    }

    // 节点n前来解锁
    // 返回三态：0 = stale unlock（节点已不在 hold_lock_nodes，锁份额未变）；
    // 1 = S 解锁成功；2 = X 解锁成功（need_validate）。
    // stale 必须由调用方跳过 TransferControl：锁份额未变时无权触发所有权
    // 转移，且他节点仍持 X 时 TransferControl 的 assert(lock != EXCLUSIVE_LOCKED)
    // 必然违反（r2-20260923-live-smoke-006：第 16 层自愈的强制解锁触发 SIGABRT）。
    // D-1 修复（R2c 早锚点冷缓存连环 stuck）：撤销 node_id 在请求队列中的
    // 排队请求。调用方必须已持有 mutex。两个实证缺陷迫使撤销语义加入：
    // ① 恢复完成清理（ClearStaleAbandonedFetchStates）把本地 idle 持有
    //   reset 后不再发释放，manager 侧 holder 登记成为永生幽灵——后续 X
    //   授权被挡 45s/轮直至 wall budget 耗尽（r2c-20260923-c5 page 31）；
    // ② L16 超时自愈 force_forfeit 推进队列时会把锁授予已超时放弃的队首
    //   等待者（发起者自己），幽灵份额从被没收方转移到发起方。
    // 撤销规则：
    // - 非队首：直接移除并递减请求计数；
    // - 队首（即 src_node_id/下一轮授予目标）：src 让位给新队首，维持
    //   TransferControl 的"队首==src"约定；
    // - 队列清空且无人持锁：完全复位 pending 态（holders 仍卡死时保持
    //   pending，等待真实 holder 释放或 L16 没收推进）。
    // 返回是否移除了排队请求。
    bool CancelQueuedRequestNoLock(node_id_t node_id) {
        for (auto it = request_queue.begin(); it != request_queue.end(); ++it) {
            if (it->node_id != node_id) continue;
            const bool was_front = (it == request_queue.begin());
            if (it->xlock) x_request_num--; else s_request_num--;
            request_queue.erase(it);
            if (request_queue.empty()) {
                // GPLM 不变式（LockExclusive:396 等 assert 依赖）：is_pending ⟺
                // request_queue 非空。清空时必须无条件复位（含 holders 仍持锁的
                // 情形——持锁是正常态非 pending 态；真实 holder 释放时 UnlockAny
                // 会重置 is_pending=true 并 TransferControl 推进）。
                is_pending = false;
                src_node_id = INVALID_NODE_ID;
            } else if (was_front && src_node_id == node_id) {
                src_node_id = request_queue.front().node_id;
            }
            LOG(WARNING) << "[CancelQueuedRequest] node " << node_id
                         << " withdrawn from request queue (page " << page_id << ")";
            return true;
        }
        return false;
    }

    int UnlockAny(node_id_t node_id){
        mutex.lock();
        // IR Recovery 安全检查：节点可能已被 CleanFailedNodeNoBlock/Reset 移除
        auto it = std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id);
        if (it == hold_lock_nodes.end()) {
            // D-1 修复：发 unlock 的节点不在 holders——它是放弃者（本地清理/
            // L16 超时）。其排队请求必须一并撤销，否则幽灵排队者会在后续
            // TransferControl 中被授予无人释放的锁份额。
            CancelQueuedRequestNoLock(node_id);
            TombstoneNoBlock(node_id); // R2c D-2：拦截同节点过时汇报重建
            LOG(WARNING) << "[UnlockAny] node " << node_id
                         << " not in hold_lock_nodes (page " << page_id << "), likely post-recovery stale unlock";
            return 0;
        }
        if(lock == EXCLUSIVE_LOCKED){
            lock = 0;
            hold_lock_nodes.erase(it);
            return 2;
        }
        else{
            --lock;
            hold_lock_nodes.erase(it);
        }
        return 1;
    }

    int getLockNoBlock(){
        return lock;
    }

    // L16 死锁打破（smoke-017）：等待者 45s 超时 force release 时，持有方
    // 事务已卡死（互卡环实证：holders 卡 5 分钟不释放）。调用方必须已持有
    // mutex（UnlockAny 返回 stale 后的路径）——没收全部持有份额并复位 lock。
    // 返回被没收的 holders，供调用方清 valid 注册与打日志。
    std::list<node_id_t> ForfeitAllHoldersNoLock() {
        std::list<node_id_t> forfeited = hold_lock_nodes;
        hold_lock_nodes.clear();
        lock = 0;
        return forfeited;
    }

    // R2c 修正（r2c-20260923-d1-late-003 实证）：ForfeitAllHoldersNoLock 无条件
    // 没收全部 holders，会把幸存节点的合法活跃份额一并清掉——本地 LPLM 与
    // 页副本无失效通知，持有者继续以 S 读写，与新 X 授权并发，页锁互斥破坏
    //（dtx_exe.cc:789 EXCLUSIVE_LOCKED assert 崩溃链）。没收范围收窄为仅
    // 发起者（L16 超时放弃者 node_id）自己的残留份额：
    // - 失败节点份额由恢复流程（CleanFailedNodeNoBlock/Phase 1a）负责；
    // - 幸存者合法份额由其持有者的正常事务流程释放；
    // - 幸存者幽灵份额由治本修复（ClearStaleAbandonedFetchStates 的撤销
    //   RPC）从源头消除，不再依赖此处没收。
    // 调用方必须已持有 mutex。返回被没收的 holders（可能为空）。
    std::list<node_id_t> ForfeitHoldersOfNoLock(node_id_t owner) {
        std::list<node_id_t> forfeited;
        if (lock == EXCLUSIVE_LOCKED) {
            // X 锁：仅当 X 持有者恰为 owner 时没收
            if (!hold_lock_nodes.empty() && hold_lock_nodes.front() == owner) {
                forfeited.push_back(hold_lock_nodes.front());
                hold_lock_nodes.clear();
                lock = 0;
            }
            return forfeited;
        }
        // S 计数锁：移除 owner 的全部 S 份额
        for (auto it = hold_lock_nodes.begin(); it != hold_lock_nodes.end();) {
            if (*it == owner) {
                forfeited.push_back(*it);
                it = hold_lock_nodes.erase(it);
                --lock;
            } else {
                ++it;
            }
        }
        return forfeited;
    }

    void InvalidOK(){
        mutex.unlock();
    }

};
