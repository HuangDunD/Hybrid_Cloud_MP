#pragma once
#include "common.h"
#include "config.h"

#include <mutex>
#include <cassert>
#include <iostream>
#include <condition_variable>
#include <atomic>
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>
#include <butil/logging.h>
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

private:
    bthread::Mutex mutex;    // 用于保护读写锁的互斥锁
    bthread::ConditionVariable cv; // 条件变量，用于等待远程锁的成功通知

public:
    LRLocalPageLock(page_id_t pid) {
        page_id = pid;
        lock = 0;
        remote_mode = LockMode::NONE;
        is_evicting = false;
        is_released = true;

        dest_node_id = INVALID_NODE_ID;
    }

    void setDestNodeID(node_id_t node_id){
        std::lock_guard<bthread::Mutex> lk(mutex);
        dest_node_id = node_id;
    }
    void setDestNodeIDNoBlock(node_id_t node_id_){
        dest_node_id = node_id_;
    }
    int getDestNodeID(){
        std::lock_guard<bthread::Mutex> lk(mutex);
        return dest_node_id;
    }
    int getDestNodeIDNoBlock() const {
        return dest_node_id;
    }
    void addToPushListNoBlock(node_id_t dest_node_id){
        push_list.emplace_back(dest_node_id);
    }
    std::list<node_id_t> getPushList() {
        std::lock_guard<bthread::Mutex> lk(mutex);
        std::list<node_id_t> ret = push_list;
        push_list.clear();
        return ret;
    }
    
    bool LockShared() {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while(true){
            if(is_granting || is_pending || is_evicting){
                // 当前节点已经有线程正在远程获取这个数据页的锁，其他线程无需再去远程获取锁
                // 其他节点正在远程申请这个数据页的锁, 为了防止饿死, 应阻塞而不授予锁
                cv.wait(lock, [this] { return !is_granting && !is_pending && !is_evicting; });
                continue;
            } else if(remote_mode == LockMode::EXCLUSIVE){
                if(this->lock == EXCLUSIVE_LOCKED) {
                    cv.wait(lock, [this] {
                        return this->lock != EXCLUSIVE_LOCKED || is_granting ||
                               is_pending || is_evicting ||
                               remote_mode != LockMode::EXCLUSIVE;
                    });
                    continue;
                }
                else {
                    this->lock++;
                    // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                    return false;
                }
            } else if(remote_mode == LockMode::SHARED){
                if(this->lock == EXCLUSIVE_LOCKED) {
                    LOG(ERROR) << "Locol Grant Exclusive Lock, however remote only grant shared lock";
                    assert(false);
                } else {
                    this->lock++;
                    // 由于远程已经持有共享锁, 因此无需再去远程获取锁
                    return false;
                }
            } else if(remote_mode == LockMode::NONE){
                this->lock++;
                is_granting = true;
                return true;
            } else{
                assert(false);
            }
        }
    }

    bool LockExclusive() {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while(true){
            if(is_granting || is_pending || is_evicting){
                // 当前节点已经有线程正在远程获取这个数据页的锁，其他线程无需再去远程获取锁
                // 其他节点正在远程申请这个数据页的锁, 为了防止饿死, 应阻塞而不授予锁
                cv.wait(lock, [this] { return !is_granting && !is_pending && !is_evicting; });
                continue;
            }
            else if(remote_mode == LockMode::EXCLUSIVE){
                if(this->lock != 0) {
                    cv.wait(lock, [this] {
                        return this->lock == 0 || is_granting || is_pending ||
                               is_evicting || remote_mode != LockMode::EXCLUSIVE;
                    });
                    continue;
                }
                else {
                    this->lock = EXCLUSIVE_LOCKED;
                    // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                    return false;
                }
            }
            else if(remote_mode == LockMode::SHARED || remote_mode == LockMode::NONE){
                // 还在用呢
                if(this->lock != 0) {
                    cv.wait(lock, [this] {
                        return this->lock == 0 || is_granting || is_pending || is_evicting;
                    });
                    continue;
                }
                else {
                    this->lock = EXCLUSIVE_LOCKED;
                    is_granting = true;
                    return true;
                }
            }
            else{
                assert(false);
            }
        }
    }

    // 这个函数是在 PushPage 中调用的，也就是数据真正到达了本地，写入缓存区后，才调用这个函数，把 update_success 设置为 true
    void RemotePushPageSuccess(){
        std::unique_lock<bthread::Mutex> l(mutex);
        assert(is_granting == true);
        update_success = true;
        cv.notify_all(); // 通知等待的线程远程页面推送成功
    }

    void RemoteNotifyLockSuccess(bool xlock, bool is_newest){
        mutex.lock();
        assert(is_granting == true);
        if(xlock) assert(lock == EXCLUSIVE_LOCKED);
        else assert(lock > 0); 
        success_return = true;

        need_wait = !is_newest;
    
        cv.notify_all(); // 通知等待的线程远程锁成功
    }

    double TryGetPushData(table_id_t table_id){
        struct timespec start_time, end_time;
        clock_gettime(CLOCK_REALTIME, &start_time);
        
        std::unique_lock<bthread::Mutex> lock(mutex);
        assert(is_granting == true);

        cv.wait(lock , [this]{
            return update_success;
        });
        clock_gettime(CLOCK_REALTIME, &end_time);
        update_success = false;
        return (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000.0;
    }

    // 调用时机：fetch s/x page 的时候，无法立刻获得锁，我就来尝试看看能不能拿到锁
    bool TryRemoteLockSuccess(table_id_t table_id , double* wait_push_time = nullptr){
        std::unique_lock<bthread::Mutex> lock(mutex);
        assert(is_granting == true);
        // 等待远程锁成功通知
        cv.wait(lock, [this] { return success_return; });
        // LOG(INFO) << "Wait Lock Success , table_id = " << table_id << " page_id = " << page_id;
        // update_node == -1：不需要获取最新数据页，否则表示需要从最新节点获取，update_node 的值就是最新数据所在的节点
        // push_or_pull = true：远程推送过来，=false：当前节点需要主动去拉取
        bool ret = need_wait;
        if(!need_wait){
            assert(update_success == false); 
        } else{
            // 需要等待远程把数据给推送过来
            struct timespec start_time, end_time;
            clock_gettime(CLOCK_REALTIME, &start_time);
            cv.wait(lock, [this] { return update_success; });

            clock_gettime(CLOCK_REALTIME, &end_time);
            auto wait = (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
            if(wait_push_time != nullptr){
                *wait_push_time = wait;
            }
            update_success = false;
            need_wait = false;
        }
        // LOG(INFO) << "Wait Page Push Success , table_id = " << table_id << " page_id = " << page_id;
        // 重置远程加锁成功标志位
        success_return = false;
        return ret;
    }

    bool TryBeginEvict(){
        // is_evicting：我正在选这孩子淘汰，你们这些线程别来沾边
        std::lock_guard<bthread::Mutex> lk(mutex);
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
        std::lock_guard<bthread::Mutex> lk(mutex);
        assert(is_evicting);
        is_evicting = false;
        cv.notify_all();
    }

    bool isEvicting() {
        std::lock_guard<bthread::Mutex> lk(mutex);
        return is_evicting;
    }

    // 调用LockExclusive()或者LockShared()之后, 如果返回true, 则需要调用这个函数将granting状态转换为shared或者exclusive
    void LockRemoteOK(node_id_t node_id){
        mutex.lock();
        assert(is_granting == true);
        // 可以通过lock的值来判断远程的锁模式，因为LockMode::GRANTING和LockMode::UPGRADING的时候其他线程不能加锁
        if(lock == EXCLUSIVE_LOCKED){
            remote_mode = LockMode::EXCLUSIVE;
        }
        else{
            remote_mode = LockMode::SHARED;
        }
        // assert(is_released);
        is_granting = false;
        is_released = false;
        cv.notify_all();
        mutex.unlock();
    }

    // 申请远程 X 锁失败 (例如 GPLM 精检发现无效转移要求回滚): 撤销 LockExclusive 留下的
    // is_granting + lock=EXCLUSIVE_LOCKED 状态, 而不影响 remote_mode (它本来就 != EXCLUSIVE)。
    // 这样 GPLM/本地状态都回到了 LockExclusive 调用之前的样子。
    void LockExclusiveAbort(){
        mutex.lock();
        assert(is_granting == true);
        assert(lock == EXCLUSIVE_LOCKED);
        lock = 0;
        is_granting = false;
        // remote_mode 保持原状 (NONE 或 SHARED), 因为我们最终没有真正升级它
        cv.notify_all();
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
        cv.notify_all();
        return lock;
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
        cv.notify_all();
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
            cv.notify_all();
            mutex.unlock();
        }
        else if(remote_mode == LockMode::SHARED){
            unlock_remote = 1;
            remote_mode = LockMode::NONE;
            cv.notify_all();
        }
        else if(remote_mode == LockMode::EXCLUSIVE){
            unlock_remote = 2;
            remote_mode = LockMode::NONE;
            cv.notify_all();
        }
        else{
            assert(false);
        }
        is_released = true;
        return unlock_remote;
    }

    // 调用UnlockExclusive()或者UnlockShared()之后, 如果返回true, 则需要调用这个函数释放本地的mutex
    void UnlockRemoteOK(){
        cv.notify_all();
        mutex.unlock();
    }

    int Pending(node_id_t n, bool xpending){
        int unlock_remote = 0;
        mutex.lock();
        assert(!is_pending);

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
            }
        }
        else if(!is_granting && remote_mode == LockMode::NONE){ 
            // 我魔改之后，这种应该不会出现了，因为一定只有一个节点会发送 Pending
            assert(false);
            // unlock_remote = 3; 
        }
        else if(is_granting && remote_mode == LockMode::SHARED){  
            // 远程已经获取了S锁，正在申请X锁
            // 注意此时本地一定不在使用共享锁，因为如果在用的话不会向远程申请，而是等到它用完
            assert(lock == EXCLUSIVE_LOCKED);
            if(xpending){ 
                is_pending = true;
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
        }
        else{
            // is_granting == true, remote_mode == EXCLUSIVE
            assert(false);
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
        std::lock_guard<bthread::Mutex> l(mutex);
        return remote_mode == LockMode::SHARED && is_granting;
    }

    // Debug
    bool HasOwner() {
        std::lock_guard<bthread::Mutex> l(mutex);
        return (remote_mode == LockMode::SHARED || remote_mode == LockMode::EXCLUSIVE);
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
    // 默认容量保持 ComputeNodeBufferPageSize 不变（兼容 SQL 模式以及不传参的旧调用）；
    // 负载模式下由 ComputeNode 根据每张表/B+ 树/FSM 的实际页面数量传入对应容量。
    explicit LRLocalPageLockTable(size_t capacity = ComputeNodeBufferPageSize)
        : capacity_(NormalizePageLockTableCapacity(capacity)) {
        page_table = new LRLocalPageLock*[capacity_];
        for(size_t i = 0; i < capacity_; i++){
            page_table[i] = new LRLocalPageLock(i);
        }
    }

    ~LRLocalPageLockTable(){
        if (page_table != nullptr){
            for(size_t i = 0; i < capacity_; i++){
                delete page_table[i];
            }
            delete[] page_table;
            page_table = nullptr;
        }
    }

    LRLocalPageLock* GetLock(page_id_t page_id) {
        assert((size_t)page_id < capacity_);
        return page_table[page_id];
    }

    size_t capacity() const { return capacity_; }
    
private:
    size_t capacity_ = 0;
    LRLocalPageLock** page_table = nullptr;
};
