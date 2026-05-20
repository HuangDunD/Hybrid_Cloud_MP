#include "server.h"

#include "thread"
#include "atomic"
#include <iomanip>

static std::atomic<int> cnt{0};

static std::mutex page_cnt_mtx;
static std::vector<int> page_cnt(10000 , 0);

// BLink 的多节点索引同步走的也是 lazy ，不需要统计，这个 need_to_record 就是用来隔离 BLink 的
Page* ComputeServer::rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id, bool need_to_record) {
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
    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    // 如果本地加锁成功，说明页面所有权在我身上，页面也一定在缓冲区里，直接去拿即可
    if (!lock_remote){
        if (need_to_record){
            node_->fetch_from_local_cnt++;
        }
        // 一定在缓冲池里
        page = node_->local_buffer_pools[table_id]->fetch_page(page_id);
    } else {
        // 在远程加锁
        if (need_to_record){
            node_->lock_remote_cnt++;
        }

        // 故障容错重试循环：RPC 失败或故障恢复中断时重试
        bool need_full_retry = true;
        int rpc_retry_count = 0;
        while (need_full_retry) {
            need_full_retry = false;

            // IR Recovery: 重试时需要重新设置 LPLM 的 is_granting 状态
            // 因为 SetRecoveryAbort 可能已经清理了 is_granting 和 lock
            node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->ResetForRetry(false);

            page_table_service::PSLockRequest request;
            page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
            page_table_service::PageID *page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            request.set_allocated_page_id(page_id_pb);
            request.set_node_id(node_->node_id);

            // IR Recovery: 使用故障恢复路由
            node_id_t page_belong_node = get_recovery_node_id(table_id , page_id);

            // IR 锁重试循环
            bool ir_retry = true;
            while (ir_retry) {
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
                        delete response;
                        // 指数退避: 10ms, 20ms, 40ms, ..., 上限 500ms
                        int backoff_us = std::min(10000 * (1 << std::min(rpc_retry_count - 1, 5)), 500000);
                        usleep(backoff_us);
                        need_full_retry = true;
                        break;
                    }
                }
                // 如果返回 IR 锁，等待后重试
                if (!need_full_retry && response->ir_locked()) {
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

            bool need_storage = response->need_storage_fetch();
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
                        VLOG(1) << "[IR Recovery] TryGetPushData aborted for table=" << table_id << " page=" << page_id << ", fetching from storage";
                        std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                        page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                    } else {
                        page = node_->try_fetch_page(table_id , page_id);
                        if (!page) {
                            VLOG(1) << "[IR Recovery] Page not in buffer after push for table=" << table_id << " page=" << page_id << ", fetching from storage";
                            std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                            page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                        }
                    }
                } else if (valid_node == -1) {
                    // 本节点已持有最新页面（恢复重试场景），直接从本地缓冲区获取
                    page = node_->try_fetch_page(table_id , page_id);
                    if (!page) {
                        // 页面可能已被 Pending handler 释放但 GPLM 未同步（LRPAnyUnlock 失败），从存储获取
                        VLOG(1) << "[IR Recovery] Page not in buffer (valid_node==-1) for table=" << table_id << " page=" << page_id << ", fetching from storage";
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
                    delete response;
                    usleep(2000);  // 2ms backoff
                    need_full_retry = true;
                    continue;
                }
                if (lock_result == -2) {
                    // GPLM 已授予锁但 push 源故障，从存储获取数据
                    VLOG(1) << "[IR Recovery] TryRemoteLockSuccess push aborted for table=" << table_id << " page=" << page_id << ", fetching from storage";
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
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                    page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                }

                update_m.lock();
                tx_update_time += wait_push_time;
                update_m.unlock();

                } // end lock_result != -2
            }
            //! lock remote ok and unlatch local
            node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
            delete response;
        } // end of need_full_retry loop
    }
    assert(page);
    assert(page->get_page_id().page_no == page_id && page->get_page_id().table_id == table_id);
    // LOG(INFO) << "fetch S Over " << "table_id = " << table_id << " page_id = " << page_id;

    return page;
}

Page* ComputeServer::rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id, bool need_to_record) {
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

    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    
    if (!lock_remote){
        // LOG(INFO) << "fetch page from local buffer where table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();
        if (need_to_record){
            node_->fetch_from_local_cnt++;
        }
        page = node_->fetch_page(table_id , page_id);
    }else if(lock_remote){
        if (need_to_record){
            node_->lock_remote_cnt++;
        }

        // 故障容错重试循环
        bool need_full_retry = true;
        int rpc_retry_count = 0;
        while (need_full_retry) {
            need_full_retry = false;

            // IR Recovery: 重试时需要重新设置 LPLM 的 is_granting 状态
            // 因为 SetRecoveryAbort 可能已经清理了 is_granting 和 lock
            node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->ResetForRetry(true);

            page_table_service::PXLockRequest request;
            page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
            page_table_service::PageID* page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            request.set_allocated_page_id(page_id_pb);
            request.set_node_id(node_->node_id);

            // IR Recovery: 使用故障恢复路由
            node_id_t page_belong_node = get_recovery_node_id(table_id , page_id);

            // IR 锁重试循环
            bool ir_retry = true;
            while (ir_retry) {
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
                        delete response;
                        int backoff_us = std::min(10000 * (1 << std::min(rpc_retry_count - 1, 5)), 500000);
                        usleep(backoff_us);
                        need_full_retry = true;
                        break;
                    }
                }
                if (!need_full_retry && response->ir_locked()) {
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

            bool need_fetch_from_storage = response->need_storage_fetch();

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
                        std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                        page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                    } else {
                        page = node_->try_fetch_page(table_id , page_id);
                        if (!page) {
                            VLOG(1) << "[IR Recovery] Page not in buffer after X push for table=" << table_id << " page=" << page_id << ", fetching from storage";
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
                    delete response;
                    usleep(2000);
                    need_full_retry = true;
                    continue;
                }
                if (lock_result == -2) {
                    VLOG(1) << "[IR Recovery] TryRemoteLockSuccess push aborted for X table=" << table_id << " page=" << page_id << ", fetching from storage";
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
                    std::string data = rpc_fetch_page_from_storage(table_id , page_id , need_to_record);
                    page = put_page_into_buffer(table_id , page_id , data.c_str() , 1 , need_to_record);
                }
                
                update_m.lock();
                tx_update_time += wait_push_time;
                update_m.unlock();

                } // end lock_result != -2
            }
            //! lock remote ok and unlatch local
            node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
            delete response;
        } // end of need_full_retry loop
    }
    assert(page);
    assert(page->get_page_id().page_no == page_id && page->get_page_id().table_id == table_id);


    return page;
}

void ComputeServer::rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id) {
    // LOG(INFO) << "Releasing S Page " << "table_id = " << table_id << " page_id = " << page_id;
    LRLocalPageLock *lr_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id);
    auto [unlock_remote, need_unpin] = lr_lock->tryUnlockShared();

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
    delete response;
}

void ComputeServer::rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id) {
    // LOG(INFO) << "Release X Page , table_id = " << table_id << " page_id = " << page_id << " ";
    int unlock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->tryUnlockExclusive();
    LRLocalPageLock *lr_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id);
    if (unlock_remote == 0){
        // 对于x 锁来说，由于同一时间单节点只能持有一个，因此放锁的时候，如果不需要等待，dest_node_id 一定是 -1
        assert(lr_lock->getDestNodeIDNoBlock() == INVALID_PAGE_ID);
        // LOG(INFO) << "Lazy Release X , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();
        assert(lr_lock->getLock() == 0);
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

    // delete response;
    delete unlock_response;

    return;
}

// 这里用异步的方法实现释放所有数据页
void ComputeServer::rpc_lazy_release_all_page_async() {
    assert(false);
}

// 这里用异步的方法实现释放所有数据页
void ComputeServer::rpc_lazy_release_all_page_async_new() {
    assert(false);
}