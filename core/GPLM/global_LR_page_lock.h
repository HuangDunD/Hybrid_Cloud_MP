#pragma once
#include "common.h"
#include "config.h"
#include "compute_node/compute_node.pb.h"
#include "GPLM/global_valid_table.h"
#include "GPLM/compute_server_interface.h"

#include <iostream>
#include <list>
#include <algorithm> 
#include <mutex>
#include <cassert>
#include <brpc/channel.h>
#include <queue>
#include <bthread/butex.h>
#include <unistd.h>
#include <atomic>
#include <chrono>

extern std::atomic<int64_t> global_notify_push_page_time_ns;
extern std::atomic<int64_t> global_notify_push_page_count;
#ifdef ENABLE_LAZY_RTT_STATS
extern std::atomic<int64_t> lazy_2RTT_count;
extern std::atomic<int64_t> lazy_3RTT_count;
#define LAZY_2RTT_INC() lazy_2RTT_count.fetch_add(1, std::memory_order_relaxed)
#define LAZY_3RTT_INC() lazy_3RTT_count.fetch_add(1, std::memory_order_relaxed)
#else
#define LAZY_2RTT_INC() ((void)0)
#define LAZY_3RTT_INC() ((void)0)
#endif

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

    std::atomic<int> remote_push_tar_cnt{0};   // 表示推送页面的目标节点数量

private:
    std::list<LRRequest> request_queue;
    int s_request_num = 0;
    int x_request_num = 0;
    bthread::Mutex mutex;
    
    // 验证 SetComputeNodePending 阶段和真正 PushPage 选择推送页面的节点一致
    // node_id_t pending_src_node_id = -1;
    static ComputeServerInterface* compute_server_instance;

public:
    static void SetComputeServer(ComputeServerInterface* server) {
        compute_server_instance = server;
    }

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
        is_pending = false;
        src_node_id = INVALID_NODE_ID;
    }

    std::list<node_id_t> get_hold_lock_nodes() {
        return hold_lock_nodes;
    }

    bool is_request_queue_empty() const {
        return request_queue.empty();
    }

    void add_hold_lock_node(node_id_t node_id){
        // 如果hold_lock_nodes中已经有了这个node_id, 则不再添加, 否则添加
        // 无需加锁, 因为这个函数只会在持有mutex的函数调用
        assert(std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) == hold_lock_nodes.end());
        hold_lock_nodes.push_back(node_id);
    }

    // mutex 在调用这个函数之前已经被持有
    static void PendingRPCDone(compute_node_service::PendingResponse* response, brpc::Controller* cntl) {
        // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
        std::unique_ptr<compute_node_service::PendingResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            // RPC失败了. response里的值是未定义的，勿用。
            LOG(ERROR) << "PendingRPC failed: " << cntl->ErrorText();
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
            LOG(ERROR) << "PendingRPC failed" << cntl->ErrorText();
        } else {
            // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        }
        // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
    }

    static void NotifyPushPageRPCDone(LR_GlobalPageLock* self, compute_node_service::NotifyPushPageResponse* response, brpc::Controller* cntl) {
        std::unique_ptr<compute_node_service::NotifyPushPageResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            LOG(ERROR) << "NotifyPushPageRPC failed: " << cntl->ErrorText();
        }
        assert(self->remote_push_tar_cnt.load() > 0);
        self->remote_push_tar_cnt--;
    }

    // XPending 代表当前持有锁的类型
    // 第一个无法满足加上锁的节点会调用这个函数，通知所有持有锁的节点Pending，让他们尽快让出锁
    void SetComputenodePending(node_id_t n, bool XPending, table_id_t table_id, GlobalValidInfo* valid_info , bool from_global) {
        // 构造request
        compute_node_service::PendingRequest request;
        compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);

        node_id_t dest_node_id_local = INVALID_NODE_ID;
        bool has_local = false;

        int trans_node_id = -1; // 从哪个节点发
        if(XPending) {
            assert(hold_lock_nodes.size() == 1);
            // 只有一个节点会持有X 锁，且下一轮拿到锁的一定不是本节点，所以 trans_node_id 大胆设置成队首
            trans_node_id = hold_lock_nodes.front(); 
            request.set_pending_type(compute_node_service::PendingType::XPending);
        } else {
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

        // 向所有的持有锁的计算节点发送释放锁请求
        std::vector<brpc::CallId> cids;
        // 由于发送 Pending 可能会导致节点淘汰页面，所以这里需要等待 NotifyPushPage 完成
        // wait_push_page_rpc_done();
        for(auto node_id : hold_lock_nodes){
            /*
                这里的 node_id == n continue ：如果下一轮就有自己的话，不需要向自己发送 Pending 请求
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

            node_id_t set_dest_node;
            if (node_id == trans_node_id){
                set_dest_node = n;
            }else{
                set_dest_node = -1;
            }
            request.set_dest_node_id(set_dest_node);

            if (table_id < 10000){
                // LOG(INFO) << "Set ComputeNode Pending , table_id = " << table_id << " page_id = " << page_id << " pending node = " << node_id;
            }
            
            assert(compute_server_instance != nullptr);
            if (node_id == compute_server_instance->GetNodeID()) {
                // compute_server_instance->Pending(page_id, XPending, table_id , set_dest_node);
                if (trans_node_id == compute_server_instance->GetNodeID()){
                    LAZY_2RTT_INC();
                }
                assert(dest_node_id_local == INVALID_NODE_ID);
                dest_node_id_local = set_dest_node;
                has_local = true;
                continue;
            }
            
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

        // 对于那些在本地的，最后调用即可
        // 至于为啥不在上边，因为 Pending 会调用 LRPAnyUnlock，而 LRPAnyUnlock 是需要锁的，在上面会死锁，所以防掉锁之后，再调用
        if (has_local){
            compute_server_instance->Pending(page_id, XPending, table_id , dest_node_id_local);
        }else {
            if (from_global){
                LAZY_3RTT_INC();
            }else {
                LAZY_2RTT_INC();
            }
        }
    }

    void UnlockMutex(){
        mutex.unlock();
    }

    void wait_push_page_rpc_done(){
        while(remote_push_tar_cnt.load() > 0){
            assert(remote_push_tar_cnt.load() >= 0);
            usleep(50);
        }
    }

    // n 请求的节点
    // XLock：请求的节点是否是 X 锁
    void NotifyPushPage(table_id_t table_id , node_id_t dest_node_id , node_id_t src_node_id , bool from_global){
        auto notify_push_page_start = std::chrono::high_resolution_clock::now();
        compute_node_service::NotifyPushPageRequest request;
        compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);

        assert(compute_server_instance);
        if (src_node_id == compute_server_instance->GetNodeID()) {
            if (table_id < 10000){
                // LOG(INFO) << "Local Notify Push Page To Node : " << dest_node_id << " table_id = " << table_id << " page_id = " << page_id;
                LAZY_2RTT_INC();
            }
            if (from_global){
                compute_server_instance->PushPageToOther(table_id, page_id, dest_node_id, true , true , 0);
            }else {
                compute_server_instance->PushPageToOther(table_id, page_id, dest_node_id, true , true , 1);
            }
            if (table_id < 10000){
                // LOG(INFO) << "Local Notify Push Page Over , table_id = " << table_id << " page_id = " << page_id;
            }
            auto notify_push_page_end = std::chrono::high_resolution_clock::now();
            auto notify_push_page_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(notify_push_page_end - notify_push_page_start).count();
            global_notify_push_page_time_ns.fetch_add(notify_push_page_duration_ns);
            global_notify_push_page_count.fetch_add(1);
            return;
        }

        if (table_id < 10000){
            // LOG(INFO) << "Remote Notify Push Page To Node : " << dest_node_id << " table_id = " << table_id << " page_id = " << page_id;
            if (from_global){
                // 锁表在远程，通知推页面的目标也在远程，3 轮网络传输
                LAZY_3RTT_INC();
            }else {
                // 锁表在本地，但是要把页面推给远程的节点，2 轮网络传输
                LAZY_2RTT_INC();
            }
        }

        request.set_src_node_id(src_node_id);
        request.add_dest_node_ids(dest_node_id);
        assert(src_node_id != dest_node_id);

        brpc::Channel* channel = compute_channels[src_node_id];
        
        compute_node_service::ComputeNodeService_Stub computenode_stub(channel);

        brpc::Controller cntl;
        compute_node_service::NotifyPushPageResponse response;
        // computenode_stub.NotifyPushPage(cntl, &request, response, 
        //         brpc::NewCallback(NotifyPushPageRPCDone, this, response, cntl));
        computenode_stub.NotifyPushPage(&cntl, &request, &response, NULL);
        if (table_id < 10000){
            // LOG(INFO) << "Remote Notify Push Page Over , table_id = " << table_id << " page_id = " << page_id;
        }
        auto notify_push_page_end = std::chrono::high_resolution_clock::now();
        auto notify_push_page_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(notify_push_page_end - notify_push_page_start).count();
        global_notify_push_page_time_ns.fetch_add(notify_push_page_duration_ns);
        global_notify_push_page_count.fetch_add(1);
    }

    // from_global：是从远程的全局锁表来的，还是本地的全局锁表来的
    bool LockShared(node_id_t node_id, table_id_t table_id, GlobalValidInfo* valid_info , bool from_global) {
        mutex.lock();
        if(lock != EXCLUSIVE_LOCKED && request_queue.empty()){
            // 可以直接上锁
            lock++;
            add_hold_lock_node(node_id);
            assert(lock == hold_lock_nodes.size());
            assert(1 == std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id));
            return true;
        } else if(request_queue.empty()){
            // 请求队列为空，且当前持有者是排他锁
            assert(lock == EXCLUSIVE_LOCKED);
            LRRequest r{node_id, table_id, 0};
            request_queue.push_back({r});
            is_pending = true;
            s_request_num++;
            assert(s_request_num==1);
            SetComputenodePending(node_id, true, table_id, valid_info , from_global); // 这里被X锁占用，所以需要释放X锁
            return false;
            // mutex.unlock(); // 在SetComputenodePending()中会释放mutex
        } else if(!request_queue.empty()){
            // 队列不空, 就一定有一个正在pending
            assert(is_pending == true);
            assert(lock != 0);
            LRRequest r{node_id, table_id, 0};
            request_queue.push_back({r});
            s_request_num++;
            mutex.unlock();
            return false;
        }
        return false;
    }

    bool LockExclusive(node_id_t node_id, table_id_t table_id, GlobalValidInfo* valid_info , bool from_global) {
        mutex.lock();
        if(lock != 0 && is_pending == false) {
            assert(request_queue.empty()); 
            assert(s_request_num == 0 && x_request_num == 0);
            // 如果当前数据页已经有了读锁, 且等待队列第一个就是写锁，那直接升级就行
            if(lock == 1 && hold_lock_nodes.front() == node_id){
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
            SetComputenodePending(node_id, xpending,table_id,valid_info , from_global);
            // mutex.unlock(); // 在SetComputenodePending()中会释放mutex
        } else if(lock == 0 && is_pending == false){
            // 可以直接上锁
            assert(request_queue.empty()); 
            assert(s_request_num == 0 && x_request_num == 0);
            lock = EXCLUSIVE_LOCKED;
            add_hold_lock_node(node_id);
            assert(hold_lock_nodes.size() == 1);
            assert(node_id == hold_lock_nodes.front());
            return true;
        } else{
            // 如果 Pending 的话，就直接加入到等待队列中
            assert(is_pending);
            assert(request_queue.size() > 0);
            LRRequest r{node_id, table_id, 1};
            request_queue.push_back({r});
            x_request_num++;
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
            if (node_id == src_node_id){
                for (auto node_id_ : hold_lock_nodes){
                    // 如果是第一轮已经 Push 的节点，跳过，不需要向他 Push 页面
                    if (node_id_ == src_node_id){
                        continue;
                    }
                    request.add_dest_node_ids(node_id_);
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
                x_request_num--;
                // is_pending 是在 LockShared/Exclusive 里面设置为 true 的，表示 pending 开始，别人来了无法直接获取锁
                // 在这里设置为 false，表示这轮授予锁结束了，该拿到锁的节点拿到锁了
                is_pending = false;
            }
            else{
                // 授予队列首部共享锁
                lock++;
                add_hold_lock_node(request.node_id);
                s_request_num--;
                // 遍历队列找出其他S锁一次授予
                if(s_request_num > 0){
                    for (auto it = request_queue.begin(); it != request_queue.end();) {
                        if (it->xlock == false) {
                            lock++;
                            add_hold_lock_node(it->node_id);
                            s_request_num--;
                            it = request_queue.erase(it); // 并返回下一个元素的迭代器
                        } else {
                            ++it; // 继续遍历下一个元素
                        }
                    }
                }
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
            }
            else{
                return false;
            } 
        }
        return true;
    }



    void TransferPending(table_id_t table_id, std::atomic<int>& immedia_transfer, GlobalValidInfo* valid_info , bool from_global) {
        // mutex is hold here, need unlock in this fun
        // judge if need pending
        if(is_pending || request_queue.empty()) {
            // 上一个pending没结束或者没有下一个pending
            mutex.unlock();
            return;
        }
        else{
            immedia_transfer++;
            // 判断下一个pending
            auto request = request_queue.front();
            if(request.xlock){
                assert(x_request_num > 0);
                // 需要设置下一轮的 is_pending
                is_pending = true;
                bool xpending = false;
                if(lock == EXCLUSIVE_LOCKED){
                    assert(hold_lock_nodes.size() == 1);
                    assert(request.node_id != hold_lock_nodes.front());
                    xpending = true;
                }
                // 在这里unlock
                // 这个是为了找到下一轮的持锁
                SetComputenodePending(request.node_id, xpending, table_id, valid_info , from_global);
                return;
            }
            else{
                assert(s_request_num > 0);
                assert(lock == EXCLUSIVE_LOCKED);
                assert(hold_lock_nodes.size() == 1);
                assert(request.node_id != hold_lock_nodes.front());
                is_pending = true;
                bool xpending = true;
                // 在这里unlock
                SetComputenodePending(request.node_id, xpending, table_id, valid_info , from_global);
                return;
            }
        }
        return;
    }

    bool CheckIsHoldNoBlock(node_id_t node_id){
        return std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) != hold_lock_nodes.end();
    }

    bool UnlockAnyNoBlock(node_id_t node_id){
                bool need_validate = false;
        if(lock == EXCLUSIVE_LOCKED){
            assert(hold_lock_nodes.size() == 1);
            assert(hold_lock_nodes.front() == node_id);

            lock = 0;
            hold_lock_nodes.remove(node_id);
            // 在外部释放mutex
            need_validate = true;
        }
        else{
            assert(lock == hold_lock_nodes.size());
            assert(1 == std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id));

            --lock;
            hold_lock_nodes.remove(node_id);
            assert(lock == hold_lock_nodes.size());
            // 在Transfer Control中释放mutex
        }
        return need_validate;
    }

    // 节点n前来解锁
    bool UnlockAny(node_id_t node_id){
        mutex.lock();
        bool need_validate = false;
        if(lock == EXCLUSIVE_LOCKED){
            assert(hold_lock_nodes.size() == 1);
            assert(hold_lock_nodes.front() == node_id);

            lock = 0;
            hold_lock_nodes.remove(node_id);
            // 在外部释放mutex
            need_validate = true;
        }
        else{
            assert(lock == hold_lock_nodes.size());
            assert(1 == std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id));

            --lock;
            hold_lock_nodes.remove(node_id);
            assert(lock == hold_lock_nodes.size());
            // 在Transfer Control中释放mutex
        }
        return need_validate;
    }

    int getLockNoBlock(){
        return lock;
    }

    void InvalidOK(){
        mutex.unlock();
    }

};
