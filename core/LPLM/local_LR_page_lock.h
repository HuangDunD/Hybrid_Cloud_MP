#pragma once
#include "common.h"
#include "config.h"

#include <mutex>
#include <cassert>
#include <iostream>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <thread>
#include "list"

// 这里是想要使用LRLocalPageLock来实现Lazy Release的功能
class LRLocalPageLock{ 
private:
    page_id_t page_id;          // 数据页id
    lock_t lock;                // 读写锁, 记录当前数据页的ref
    LockMode remote_mode;       // 这个计算节点申请的远程节点的锁模式
    bool is_pending = false;    // 是否正在pending
    bool is_granting = false;   // 是否正在授权
    bool success_return = false; // 成功加锁返回

    bool need_wait;         // 是否需要把等待被人把页面推送过来
    bool update_success = false; // 是否更新成功

    node_id_t dest_node_id; // 准备推送的数据页，由于 Pending 的时候说不定正在用这个数据页，所以在这里记录下来，留到 lazy_release 的时候再推送
    std::list<node_id_t> push_list; // 推送给本轮持有锁的节点列表
    bool is_evicting;   // 是否正在驱逐页面
    bool is_released;   // 表示是否真正释放释放所有权了(而不是lazyRelease赖着的)
    std::atomic<bool> recovery_abort{false}; // 故障恢复时唤醒等待线程

private:
    std::mutex mutex;    // 用于保护读写锁的互斥锁
    std::condition_variable cv; // 条件变量，用于等待远程锁的成功通知

public:
    LRLocalPageLock(page_id_t pid) {
        page_id = pid;
        lock = 0;
        remote_mode = LockMode::NONE;
        is_evicting = false;
        is_released = true;

        dest_node_id = INVALID_NODE_ID;
    }

    // 故障恢复：唤醒所有等待此页面的线程，并清理可能导致 busy-wait 的中间状态
    void SetRecoveryAbort() {
        std::lock_guard<std::mutex> lk(mutex);
        // 清理 is_pending 防止 LockShared/LockExclusive 中 busy-wait 死锁
        // 场景：崩溃节点的 GPLM 发出 Pending 后节点崩溃，本地 is_pending=true 永远不会被清理
        if (is_pending) {
            is_pending = false;
            // 不修改 remote_mode！让重试逻辑通过 GPLM 来确认真实状态
            // 如果 is_granting=false，说明已持有锁，保留 remote_mode 让后续正常释放
        }
        // 清理 is_granting 状态，让等待线程能够重新发起加锁请求
        if (is_granting) {
            is_granting = false;
            // 重置本地锁计数（granting 期间 lock 已被设置）
            lock = 0;
            remote_mode = LockMode::NONE;
        }
        recovery_abort.store(true);
        cv.notify_all();
    }

    void setDestNodeID(node_id_t node_id){
        std::lock_guard<std::mutex> lk(mutex);
        dest_node_id = node_id;
    }
    void setDestNodeIDNoBlock(node_id_t node_id_){
        dest_node_id = node_id_;
    }
    int getDestNodeID(){
        std::lock_guard<std::mutex> lk(mutex);
        return dest_node_id;
    }
    int getDestNodeIDNoBlock() const {
        return dest_node_id;
    }
    void addToPushListNoBlock(node_id_t dest_node_id){
        push_list.emplace_back(dest_node_id);
    }
    std::list<node_id_t> getPushList() {
        std::lock_guard<std::mutex> lk(mutex);
        std::list<node_id_t> ret = push_list;
        push_list.clear();
        return ret;
    }
    
    bool LockShared() {
        // // LOG(INFO) << "LockShared: " << page_id;
        bool lock_remote = false;
        bool try_latch = true;
        while(try_latch){
            mutex.lock();
            if(is_granting || is_pending || is_evicting){
                // 当前节点已经有线程正在远程获取这个数据页的锁，其他线程无需再去远程获取锁
                // 其他节点正在远程申请这个数据页的锁, 为了防止饿死, 应阻塞而不授予锁
                mutex.unlock();
                std::this_thread::yield();
            } else if(remote_mode == LockMode::EXCLUSIVE){
                if(lock == EXCLUSIVE_LOCKED) {
                    mutex.unlock();
                    std::this_thread::yield();
                }
                else {
                    lock++;
                    // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                    lock_remote = false;
                    try_latch = false;
                    mutex.unlock();
                }
            } else if(remote_mode == LockMode::SHARED){
                if(lock == EXCLUSIVE_LOCKED) {
                    mutex.unlock();
                    LOG(ERROR) << "Locol Grant Exclusive Lock, however remote only grant shared lock";
                } else {
                    lock++;
                    mutex.unlock();
                    // 由于远程已经持有共享锁, 因此无需再去远程获取锁
                    lock_remote = false;
                    try_latch = false;
                }
            } else if(remote_mode == LockMode::NONE){
                lock++;
                is_granting = true;
                lock_remote = true;
                try_latch = false;
                mutex.unlock();
            } else{
                assert(false);
            }
        }
        return lock_remote;
    }

    bool LockExclusive() {
        // LOG(INFO) << "LockExclusive: " << page_id << std::endl;
        bool lock_remote = false;
        bool try_latch = true;
        while(try_latch){
            mutex.lock();
            if(is_granting || is_pending || is_evicting){
                // 当前节点已经有线程正在远程获取这个数据页的锁，其他线程无需再去远程获取锁
                // 其他节点正在远程申请这个数据页的锁, 为了防止饿死, 应阻塞而不授予锁
                mutex.unlock();
                std::this_thread::yield();
            }
            else if(remote_mode == LockMode::EXCLUSIVE){
                if(lock != 0) {
                    mutex.unlock();
                    std::this_thread::yield();
                }
                else {
                    lock = EXCLUSIVE_LOCKED;
                    mutex.unlock();
                    // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                    lock_remote = false;
                    try_latch = false;
                }
            }
            else if(remote_mode == LockMode::SHARED || remote_mode == LockMode::NONE){
                // 还在用呢
                if(lock != 0) {
                    mutex.unlock();
                    std::this_thread::yield();
                }
                else {
                    lock = EXCLUSIVE_LOCKED;
                    is_granting = true;
                    lock_remote = true;
                    try_latch = false;
                    mutex.unlock();
                }
            }
            else{
                assert(false);
            }
        }
        return lock_remote;
    }

    // 这个函数是在 PushPage 中调用的，也就是数据真正到达了本地，写入缓存区后，才调用这个函数，把 update_success 设置为 true
    void RemotePushPageSuccess(){
        std::unique_lock<std::mutex> l(mutex);
        if (!is_granting) {
            // IR Recovery: 可能是 recovery_abort 后的残留回调，忽略
            VLOG(1) << "[IR Recovery] RemotePushPageSuccess called but is_granting=false for page " << page_id << ", ignoring stale push";
            return;
        }
        update_success = true;
        cv.notify_one(); // 通知等待的线程远程页面推送成功
    }

    void RemoteNotifyLockSuccess(bool xlock, bool is_newest){
        mutex.lock();
        if (!is_granting) {
            // IR Recovery: 可能是 recovery_abort 后重试期间收到的旧 LockSuccess
            VLOG(1) << "[IR Recovery] RemoteNotifyLockSuccess called but is_granting=false for page " << page_id << ", ignoring stale notification";
            mutex.unlock();
            return;
        }
        if(xlock) assert(lock == EXCLUSIVE_LOCKED);
        else assert(lock > 0); 
        success_return = true;

        need_wait = !is_newest;
    
        cv.notify_one(); // 通知等待的线程远程锁成功
    }

    // 返回 true = 成功, false = 被故障恢复中断
    bool TryGetPushData(table_id_t table_id){
        // LOG(INFO) << "Try Get Push Data , table_id = " << table_id << " page_id = " << page_id;
        std::unique_lock<std::mutex> lock(mutex);
        if (!is_granting) {
            VLOG(1) << "[IR Recovery] TryGetPushData called but is_granting=false for page " << page_id;
            return false;
        }
        bool push_ok = cv.wait_for(lock, std::chrono::milliseconds(500), [this]{
            return update_success || recovery_abort.load();
        });
        if (!push_ok) {
            // 超时：推送源可能因 recovery 无法完成推送
            VLOG(1) << "[IR Recovery] TryGetPushData timed out for page " << page_id;
            return false;
        }
        if (recovery_abort.load()) {
            // 故障恢复中断，不重置 is_granting 和 lock（重试循环需要保持 LPLM 状态）
            recovery_abort.store(false);
            return false;
        }
        // LOG(INFO) << "Try Get Push Data Over , table_id = " << table_id << " page_id = " << page_id;
        update_success = false;
        return true;
    }

    // 返回值: -1 = 故障恢复中断(GPLM未授予锁,可重试), -2 = 故障恢复中断(GPLM已授予锁但push失败,需从存储获取), 0 = 不需要等待push, 1 = 需要等待push且已完成
    int TryRemoteLockSuccess(table_id_t table_id , double* wait_push_time = nullptr){
        std::unique_lock<std::mutex> lock(mutex);
        if (!is_granting) {
            // IR Recovery: is_granting 被外部已清理（如 recovery_abort 后旧 LockSuccess 到达又被忽略）
            VLOG(1) << "[IR Recovery] TryRemoteLockSuccess called but is_granting=false for page " << page_id;
            return -1;
        }
        // 等待远程锁成功通知 或 故障恢复中断
        // 使用超时等待防止 recovery 后因 Pending 链断裂导致永久阻塞：
        // 场景：recovery abort 后重试时 GPLM 返回 wait_lock_release=true，
        // 但 holder 节点的 LPLM 也处于 is_granting 状态且 Pending 无法正常完成
        bool wait_result = cv.wait_for(lock, std::chrono::milliseconds(500), 
            [this] { return success_return || recovery_abort.load(); });
        if (!wait_result) {
            // 超时：可能是 recovery 后的 Pending 链断裂，返回 -1 让调用方重试
            VLOG(1) << "[IR Recovery] TryRemoteLockSuccess timed out for page " << page_id << ", retrying";
            success_return = false;
            update_success = false;
            return -1;
        }
        if (recovery_abort.load()) {
            // 故障恢复中断：不重置 is_granting 和 lock，重试循环需要保持 LPLM 状态
            // 这样重试时可以继续使用当前 granting 状态发送新 RPC
            recovery_abort.store(false);
            success_return = false;
            update_success = false;
            return -1;
        }
        // update_node == -1：不需要获取最新数据页，否则表示需要从最新节点获取，update_node 的值就是最新数据所在的节点
        // push_or_pull = true：远程推送过来，=false：当前节点需要主动去拉取
        bool ret = need_wait;
        if(!need_wait){
            // IR Recovery: 恢复重试期间，旧的 push 回调可能残留设置了 update_success，
            // 而新的 GPLM 响应说不需要等待推送（need_wait=false），此时直接清理即可
            if (update_success) {
                VLOG(1) << "[IR Recovery] TryRemoteLockSuccess: stale update_success detected for page " << page_id << ", clearing";
            }
            update_success = false;
        } else{
            // 需要等待远程把数据给推送过来
            struct timespec start_time, end_time;
            clock_gettime(CLOCK_REALTIME, &start_time);
            bool push_result = cv.wait_for(lock, std::chrono::milliseconds(500), 
                [this] { return update_success || recovery_abort.load(); });
            if (!push_result) {
                // 超时：push 源节点可能因 recovery 导致无法完成推送，从存储获取
                VLOG(1) << "[IR Recovery] TryRemoteLockSuccess push timed out for page " << page_id << ", fetching from storage";
                success_return = false;
                update_success = false;
                need_wait = false;
                return -2;
            }
            if (recovery_abort.load()) {
                // 故障恢复中断：GPLM 已授予锁但 push 源节点故障，不重置 is_granting/lock
                // 返回 -2 表示需要从存储获取数据，不能重试（避免重复加锁）
                recovery_abort.store(false);
                success_return = false;
                update_success = false;
                need_wait = false;
                return -2;
            }

            clock_gettime(CLOCK_REALTIME, &end_time);
            auto wait = (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
            if(wait_push_time != nullptr){
                *wait_push_time = wait;
            }
            update_success = false;
            need_wait = false;
        }
        // 重置远程加锁成功标志位
        success_return = false;
        // LOG(INFO) << "TryRemote LockSuccess , table_id = " << table_id << " page_id = " << page_id;
        return ret ? 1 : 0;
    }

    bool TryBeginEvict(){
        // is_evicting：我正在选这孩子淘汰，你们这些线程别来沾边
        std::lock_guard<std::mutex> lk(mutex);
        if (is_evicting || is_released){
            return false;
        }
        // lock = 0 -> 说明不持有远程锁或者持有远程锁但是对页面操作完释放了本地的 latch
        // is_granting ==1 -> 一定是 lock > 0, 正在申请远程锁
        // is_pending == 1 -> 你还在用， 让你放的时候没放掉， 等你用完自己放
        // TODO：这里参数的选择可能有问题，后面优化下
        if (lock == 0 && !is_granting && !is_pending){
            is_evicting = true;
            return true;
        }
        return false;
    }

    void EndEvict(){
        std::lock_guard<std::mutex> lk(mutex);
        assert(is_evicting);
        is_evicting = false;
    }

    bool isEvicting() {
        std::lock_guard<std::mutex> lk(mutex);
        return is_evicting;
    }

    // 调用LockExclusive()或者LockShared()之后, 如果返回true, 则需要调用这个函数将granting状态转换为shared或者exclusive
    void LockRemoteOK(node_id_t node_id){
        // // LOG(INFO) << "LockRemoteOK: " << page_id << std::endl;
        mutex.lock();
        assert(is_granting == true);
        // 可以通过lock的值来判断远程的锁模式，因为LockMode::GRANTING和LockMode::UPGRADING的时候其他线程不能加锁
        if(lock == EXCLUSIVE_LOCKED){
            // // LOG(INFO) << "LockRemoteOK: " << page_id << " EXCLUSIVE_LOCKED in node " << node_id;
            remote_mode = LockMode::EXCLUSIVE;
        }
        else{
            // // LOG(INFO) << "LockRemoteOK: " << page_id << " SHARED in node " << node_id;
            remote_mode = LockMode::SHARED;
        }
        // assert(is_released);
        is_granting = false;
        is_released = false;
        mutex.unlock();
    }

    std::pair<int,bool> tryUnlockShared(){
        int unlock_remote = 0;
        bool need_unpin = false;
        mutex.lock();
        // SQL 验证
        assert(lock > 0);
        assert(lock != EXCLUSIVE_LOCKED);
        assert(!is_granting);
        // 如果释放了当前锁后，可以释放了
        if ((lock - 1) == 0 && is_pending){
            is_released = true;
            unlock_remote = (remote_mode == LockMode::SHARED) ? 1 : 2;
        }else if ((lock - 1) == 0){
            need_unpin = true;
        }
        return std::make_pair(unlock_remote , need_unpin);
    }

    int getLock() const {
        return lock;
    }

    // 返回<是否需要释放远程锁， 是否需要push页面>
    int UnlockShared() {
        --lock;
        if(lock == 0 && is_pending){
            is_pending = false; // 释放远程锁后，将is_pending置为false
            remote_mode = LockMode::NONE;
        }
    }

    int tryUnlockExclusive(){
        int unlock_remote = 0;
        mutex.lock();
        assert(remote_mode == LockMode::EXCLUSIVE);
        assert(lock == EXCLUSIVE_LOCKED);
        assert(!is_granting);
        if (is_pending){
            is_released = true;
            unlock_remote = 2;
        }
        return unlock_remote;
    }
    void UnlockExclusive(){
        lock = 0;
        if(is_pending){
            is_pending = false; // 释放远程锁后，将is_pending置为false
            remote_mode = LockMode::NONE;
        }
    }


    int UnlockAny(){
        // 这个函数在一个线程结束的时候调用，此时本地的锁已经释放，远程的锁也应该释放
        int unlock_remote; // 0表示不需要释放远程锁, 1表示需要释放S锁, 2表示需要释放X锁
        mutex.lock();
        assert(lock == 0);
        assert(!is_granting && !is_pending);
        if(remote_mode == LockMode::NONE){
            // 远程没有持有锁
            unlock_remote = 0;
            mutex.unlock();
        }
        else if(remote_mode == LockMode::SHARED){
            unlock_remote = 1;
            remote_mode = LockMode::NONE;
        }
        else if(remote_mode == LockMode::EXCLUSIVE){
            unlock_remote = 2;
            remote_mode = LockMode::NONE;
        }
        else{
            assert(false);
        }
        is_released = true;
        return unlock_remote;
    }

    // 调用UnlockExclusive()或者UnlockShared()之后, 如果返回true, 则需要调用这个函数释放本地的mutex
    void UnlockRemoteOK(){
        mutex.unlock();
    }

    int Pending(node_id_t n, bool xpending){
        int unlock_remote = 0;
        mutex.lock();
        if (is_pending) {
            // IR Recovery: 可能收到重复的 Pending（stale RPC 或 recovery 重发），忽略
            VLOG(1) << "[IR Recovery] Pending called but is_pending already true for page " << page_id << ", ignoring duplicate";
            mutex.unlock();
            return 0;
        }

        // 如果远程还持有锁
        if(!is_granting && remote_mode != LockMode::NONE) {
            assert(remote_mode == LockMode::SHARED || remote_mode == LockMode::EXCLUSIVE);
            // 如果没人在用了，那就立刻释放锁
            if(lock == 0){
                // 立刻在远程释放锁
                unlock_remote = (remote_mode == LockMode::SHARED) ? 1 : 2;
                is_released = true;
                remote_mode = LockMode::NONE;
                // 在函数外部unlock
            }
            else{   //如果有人在用，那就等待锁释放
                is_pending = true;
                // mutex.unlock();
            }
        }
        else if(!is_granting && remote_mode == LockMode::NONE){ 
            // IR Recovery: recovery 后 remote_mode 被清理为 NONE，但 stale Pending 仍然到达
            // 返回 0 表示不需要释放锁（本地已无锁）
            VLOG(1) << "[IR Recovery] Pending called but remote_mode==NONE && !is_granting for page " << page_id << ", ignoring stale Pending";
            // unlock_remote = 3; 
            // mutex.unlock();
        }
        else if(is_granting && remote_mode == LockMode::SHARED){  
            // 远程已经获取了S锁，正在申请X锁
            // 注意此时本地一定不在使用共享锁，因为如果在用的话不会向远程申请，而是等到它用完
            assert(lock == EXCLUSIVE_LOCKED);
            if(xpending){ 
                is_pending = true;
                // mutex.unlock();
            }
            else{
                // 要求释放S锁
                unlock_remote = 1;
                remote_mode = LockMode::NONE;

                // 在函数外部unlock
            }
        }
        else if(is_granting && remote_mode == LockMode::NONE){
            // 这里考虑两种情况，第一种是没有主动释放锁，0->X / 0->S, 本地还未来得及将remote_mode设置为SHARED或者EXCLUSIVE
            // 第二种是主动释放锁，接受了过时的pending，而又来了新的加锁请求
            // 无论xpengding是true还是false, 都一样
            is_pending = true;
            // mutex.unlock();
        }
        else{
            // is_granting == true, remote_mode == EXCLUSIVE
            // IR Recovery: 节点持有 X 锁且正在 granting（可能因 recovery 重试导致），设为 pending
            VLOG(1) << "[IR Recovery] Pending called with is_granting=true && remote_mode==EXCLUSIVE for page " << page_id << ", setting pending";
            is_pending = true;
        }
        return unlock_remote;
    }

    int getLockType(){
        assert(lock != 0);
        if (lock != EXCLUSIVE_LOCKED){
            return 1;
        }
        return 2;
    }

    void VarifyRemoteLock(bool status){
        mutex.lock();
        if(status == true){
            // x lock remote
            assert(remote_mode == LockMode::EXCLUSIVE);
        } else{
            // s lock remote
            assert(remote_mode == LockMode::SHARED || remote_mode == LockMode::EXCLUSIVE);
        }
        mutex.unlock();
    }

    void lockMtx(){
        mutex.lock();
    }
    void UnlockMtx(){
        mutex.unlock();
    }

    // Debug
    bool IsUpgrading() {
        std::lock_guard<std::mutex> l(mutex);
        return remote_mode == LockMode::SHARED && is_granting;
    }

    // Debug
    bool HasOwner() {
        std::lock_guard<std::mutex> l(mutex);
        return (remote_mode == LockMode::SHARED || remote_mode == LockMode::EXCLUSIVE);
    }

    // Phase 2 扫描用：包含 granting 状态的页面
    bool HasOwnerOrGranting() {
        std::lock_guard<std::mutex> l(mutex);
        return (remote_mode == LockMode::SHARED || 
                remote_mode == LockMode::EXCLUSIVE || 
                is_granting);
    }

    int getUnlockType(){
        mutex.lock();
        if (remote_mode == LockMode::NONE){
            return 0;
        }else if (remote_mode == LockMode::EXCLUSIVE){
            return 2;
        }else if (remote_mode == LockMode::SHARED){
            return 1;
        }else {
            assert(false);
        }
        return -1;
    }
};

// Lazy Release的锁表
class LRLocalPageLockTable{ 
public:  
    LRLocalPageLockTable(){
        for(int i=0; i<ComputeNodeBufferPageSize; i++){
            LRLocalPageLock* lock = new LRLocalPageLock(i);
            page_table[i] = lock;
        }
    }

    LRLocalPageLock* GetLock(page_id_t page_id) {
        return page_table[page_id];
    }
    
private:
    LRLocalPageLock* page_table[ComputeNodeBufferPageSize];
};
