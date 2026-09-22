// author:hcy
// date:2024.6.25

// 这个文件用于实现远程的页表，通过brpc实现，在无rdma环境下适用
#pragma once
#include "config.h"
#include "core/recovery/observation.h"
#include "GPLM/global_page_lock_table.h"
#include "GPLM/global_valid_table.h"
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <unistd.h>

#include "remote_page_table.pb.h"
#include <condition_variable>
#include <map>
#include <optional>
#include <set>

static int agree_cnt = 0;
static int reject_cnt = 0;

namespace page_table_service{
class PageTableServiceImpl : public PageTableService {
    public:
    PageTableServiceImpl(std::vector<GlobalLockTable*>* global_page_lock_table_list, std::vector<GlobalValidTable*>* global_valid_table_list):
        page_lock_table_list_(global_page_lock_table_list), page_valid_table_list_(global_valid_table_list){};


    virtual ~PageTableServiceImpl(){};

    virtual void PSLock(::google::protobuf::RpcController* controller,
            const ::page_table_service::PSLockRequest* request,
            ::page_table_service::PSLockResponse* response,
            ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        node_id_t node_id = request->node_id();
        table_id_t table_id = request->page_id().table_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockShared();

        bool need_from_storage = false;
        node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
        response->set_need_storage_fetch(need_from_storage);
        
        response->set_newest_node(newest_node_id);
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        response->set_allocated_page_id(page_id_pb);
        
        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void PSLock_Localcall(const ::page_table_service::PSLockRequest* request,
            ::page_table_service::PSLockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        node_id_t node_id = request->node_id();
        table_id_t table_id = request->page_id().table_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockShared();

        bool need_from_storage = false;
        node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
        // LOG(INFO) << "table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_id << " need_from_storage = " << need_from_storage << " newest_node = " << newest_node_id;
        response->set_need_storage_fetch(need_from_storage);
        response->set_newest_node(newest_node_id);
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        response->set_allocated_page_id(page_id_pb);
        
        return;
    }


    virtual void PSUnlock(::google::protobuf::RpcController* controller,
            const ::page_table_service::PSUnlockRequest* request,
            ::page_table_service::PSUnlockResponse* response,
            ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockShared();
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockMtx();

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void PSUnlock_Localcall(const ::page_table_service::PSUnlockRequest* request,
            ::page_table_service::PSUnlockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockShared();
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockMtx();

        return;
    }

    virtual void PXLock(::google::protobuf::RpcController* controller,
            const ::page_table_service::PXLockRequest* request,
            ::page_table_service::PXLockResponse* response,
            ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        node_id_t node_id = request->node_id();
        table_id_t table_id = request->page_id().table_id();

        bool need_from_storage = false;
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockExclusive();
        node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
        response->set_need_storage_fetch(need_from_storage);

        response->set_newest_node(newest_node_id);
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        response->set_allocated_page_id(page_id_pb);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void PXLock_Localcall(const ::page_table_service::PXLockRequest* request,
                       ::page_table_service::PXLockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        node_id_t node_id = request->node_id();
        table_id_t table_id = request->page_id().table_id();

        bool need_from_storage = false;
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockExclusive();
        node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
        response->set_need_storage_fetch(need_from_storage);

        response->set_newest_node(newest_node_id);
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        response->set_allocated_page_id(page_id_pb);

        return;
    }

    virtual void PXUnlock(::google::protobuf::RpcController* controller,
            const ::page_table_service::PXUnlockRequest* request,
            ::page_table_service::PXUnlockResponse* response,
            ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockExclusive();
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockMtx();
        // std::cout <<"table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " has the newest" << std::endl;
        
        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void PXUnlock_Localcall(const ::page_table_service::PXUnlockRequest* request,
            ::page_table_service::PXUnlockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockExclusive();
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockMtx();

        return;
    }

    // 以下是LAZY RELEASE模式的锁
    virtual void LRPXLock(::google::protobuf::RpcController* controller,
                    const ::page_table_service::PXLockRequest* request,
                    ::page_table_service::PXLockResponse* response,
                    ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        // IR 锁检查
        if (page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->IsIRLockedNoBlock()) {
            response->set_ir_locked(true);
            response->set_wait_lock_release(true);
            return;
        }

        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
        bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockExclusive(node_id,table_id, valid_info);

        response->set_wait_lock_release(!lock_success);
        response->set_lsn((LLSN)-1);
        if(lock_success){
            bool need_from_storage = false;
            node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
            response->set_need_storage_fetch(need_from_storage);
            response->set_newest_node(newest_node_id);

            LLSN now_lsn = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->getLsnIDNoBlock();
            response->set_lsn(now_lsn);

            if (!need_from_storage){
                if (newest_node_id != INVALID_NODE_ID){
                    // LOG(INFO) << "Notify node" << newest_node_id << " to Push , table_id = " << table_id << " page_id = " << page_id;
                    // 通知目前持有锁的节点，把数据推送给请求的节点
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , node_id , newest_node_id);
                }
            }
            page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();

            page_table_service::PageID *page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            response->set_allocated_page_id(page_id_pb);
        }

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void LRPXLock_Localcall(const ::page_table_service::PXLockRequest* request,
                       ::page_table_service::PXLockResponse* response){
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // IR 锁检查
            if (page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->IsIRLockedNoBlock()) {
                response->set_ir_locked(true);
                response->set_wait_lock_release(true);
                return;
            }

            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockExclusive(node_id,table_id, valid_info);

            response->set_wait_lock_release(!lock_success);
            response->set_lsn((LLSN)-1);
            if(lock_success){
                bool need_from_storage = false;
                node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
                response->set_need_storage_fetch(need_from_storage);
                response->set_newest_node(newest_node_id);

                LLSN now_lsn = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->getLsnIDNoBlock();
                response->set_lsn(now_lsn);

                if (!need_from_storage){
                    if (newest_node_id != INVALID_NODE_ID){
                        // 通知目前持有锁的节点，把数据推送给请求的节点
                        page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , node_id , newest_node_id);
                    }
                }
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();

                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);
            }
        }
                                                       
    virtual void LRPSLock(::google::protobuf::RpcController* controller,
                        const ::page_table_service::PSLockRequest* request,
                        ::page_table_service::PSLockResponse* response,
                        ::google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // IR 锁检查：故障恢复期间拒绝锁请求
            if (page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->IsIRLockedNoBlock()) {
                response->set_ir_locked(true);
                response->set_wait_lock_release(true);
                return;
            }

            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockShared(node_id,table_id, valid_info);
  
            response->set_wait_lock_release(!lock_success);
            response->set_lsn((LLSN)-1);
            if(lock_success){
                bool need_from_storage = false;
                node_id_t newest_node = valid_info->GetValid(node_id , need_from_storage);

                response->set_need_storage_fetch(need_from_storage);
                response->set_newest_node(newest_node);

                LLSN now_lsn = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->getLsnIDNoBlock();
                response->set_lsn(now_lsn);

                if (!need_from_storage){
                    if (newest_node != INVALID_NODE_ID) {
                        // 通知目前持有锁的节点，把数据推送给请求的节点
                        page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , node_id , newest_node);
                    }
                    // newest_node == -1 且 need_from_storage == false：请求节点已持有最新页面，无需推送
                }
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();

                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);

                //  LOG(INFO) << "LRPSLock LockSuccess Directly , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;
            }else{
                //  LOG(INFO) << "LRPSLock Wait Lock, node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id << " LockSuccess ? " << lock_success;
            }
            
            // 添加模拟延迟
            if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
            return;
        }

    // todo hcy:local call逻辑改成和 rpc 的 func 一样
    void LRPSLock_Localcall(const ::page_table_service::PSLockRequest* request,
                        ::page_table_service::PSLockResponse* response){
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // IR 锁检查
            if (page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->IsIRLockedNoBlock()) {
                response->set_ir_locked(true);
                response->set_wait_lock_release(true);
                return;
            }
            
            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockShared(node_id,table_id, valid_info);
  
            response->set_wait_lock_release(!lock_success);
            response->set_lsn((LLSN)-1);
            if(lock_success){
                bool need_from_storage = false;
                node_id_t newest_node = valid_info->GetValid(node_id , need_from_storage);
                response->set_need_storage_fetch(need_from_storage);

                LLSN now_lsn = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->getLsnIDNoBlock();
                response->set_lsn(now_lsn);

                if (!need_from_storage){
                    if (newest_node != INVALID_NODE_ID) {
                        // 通知目前持有锁的节点，把数据推送给请求的节点
                        page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , node_id , newest_node);
                    }
                    // newest_node == -1 且 need_from_storage == false：请求节点已持有最新页面，无需推送
                } 
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();


                response->set_newest_node(newest_node);

                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);

                //  LOG(INFO) << "LRPSLock LockSuccess Directly , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;
            }else{
                //  LOG(INFO) << "LRPSLock Wait Lock, node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id << " LockSuccess ? " << lock_success;
            }

            return;
        }


    // 缓冲池释放的时候调用的，需要和 LRPAnyUnlock 作区别处理，所以不放在一起了
    virtual void BufferReleaseUnlock(::google::protobuf::RpcController* controller,
                const ::page_table_service::BufferReleaseUnlockRequest* request,
                ::page_table_service::BufferReleaseUnlockResponse* response,
                ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        // LOG(INFO) << "BufferRelease Remote , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

        LR_GlobalPageLock *gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
        // 这里加锁，是为了确保获取 pending_src 和执行 Unlock 二者是连贯的，不能在二者中间让别人选中了一个新的 pending_src(在LockShared/Exclusive)
        gl->mutexLock();
        // IR Recovery: 页面可能已被 IR 锁接管（GPLM Reset），拒绝释放
        if (gl->IsIRLocked()) {
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        /*
                在主节点里面已经检查了 lock == 0 && !is_granting && !is_pending，然后隔绝了后续再申请锁的可能
                上面做的这些已经能够确保走到这里的节点一定不在请求队列里，也就是一定不在请求锁，也能确保后续不可能再来申请锁
                主节点没办法处理的是，如果远程已经让我推送页面了，那怎么把这种情况也排除掉
                远程可能的情况
                1. 远程已经把页面释放完了，此时本节点已经被释放了，节点此时(物理上同一时间)可能有几种状态：
                    1.1 被通知 NotifyPushPage，且还在 Push
                    1.2 被通知 NotifyPushPage，但是 Push 完了
                    1.3 仅仅是
                2. 远程正在释放页面的过程中，这种很危险，条件就是 is_pending
                3. 除了上面两种情况，就是节点正常的情况了，我觉得是没啥问题了
        */
        // 第一种情况：已经把页面释放完了(注意本节点的请求一定不会在请求队列里，所以不需要考虑请求队列的情况)
        if (!gl->CheckIsHoldNoBlock(node_id)){
            // std::cout << "Rejected " << ++reject_cnt << " agree_cnt = " << agree_cnt << "\n";
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        // 第二种情况：还在释放页面的过程中
        if (gl->getIsPendingNoBlock()){
            // std::cout << "Rejected " << ++reject_cnt << " agree_cnt = " << agree_cnt << "\n";
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        agree_cnt++;
        LLSN lsn = request->lsn();
        gl->setLsnIDNoBlock(lsn);

        // LOG(INFO) << "Agree Release , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

        // 把本节点的有效信息设置为false
        // IR Recovery 期间 MarkOnluInStorage 可能已清除所有节点的有效性
        if (valid_info->IsValid(node_id)) {
            valid_info->setNodeStatus(node_id , false);
        }
        
        // 第三种情况，可以安全释放锁了
        // 这里也先别释放 mutex ，在后面会自己释放锁,要么在 TransferControl里，要么在 TranfserPending 里
        bool need_validate = gl->UnlockAnyNoBlock(node_id);
        
        // 请求队列一定为空，否则 is_pending = true
        assert(page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->is_request_queue_empty());
        gl->mutexUnlock();
        response->set_agree(true);

        if (NetworkLatency != 0)  usleep(NetworkLatency);

        return;
    }

    virtual void BufferReleaseUnlock_LocalCall(
                const ::page_table_service::BufferReleaseUnlockRequest* request,
                ::page_table_service::BufferReleaseUnlockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        // LOG(INFO) << "BufferRelease Local , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

        LR_GlobalPageLock *gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
        // 这里加锁，是为了确保获取 pending_src 和执行 Unlock 二者是连贯的，不能在二者中间让别人选中了一个新的 pending_src(在LockShared/Exclusive)
        gl->mutexLock();
        // IR Recovery: 页面可能已被 IR 锁接管（GPLM Reset），拒绝释放
        if (gl->IsIRLocked()) {
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        // 第一种情况：已经把页面释放完了(注意本节点的请求一定不会在请求队列里，所以不需要考虑请求队列的情况)
        if (!gl->CheckIsHoldNoBlock(node_id)){
            // std::cout << "Rejected " << ++reject_cnt << " agree_cnt = " << agree_cnt << "\n";
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        // 第二种情况：还在释放页面的过程中
        if (gl->getIsPendingNoBlock()){
            // std::cout << "Rejected " << ++reject_cnt << " agree_cnt = " << agree_cnt << "\n";
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        LLSN lsn = request->lsn();
        gl->setLsnIDNoBlock(lsn);
        // LOG(INFO) << "Agree Release , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

        // 把本节点的有效信息设置为false
        // IR Recovery 期间 MarkOnluInStorage 可能已清除所有节点的有效性
        if (valid_info->IsValid(node_id)) {
            valid_info->setNodeStatus(node_id , false);
        }
        
        // 第三种情况，可以安全释放锁了
        // 这里也先别释放 mutex ，在后面会自己释放锁,要么在 TransferControl里，要么在 TranfserPending 里
        bool need_validate = gl->UnlockAnyNoBlock(node_id);
        
        // 请求队列一定为空，否则 is_pending = true
        assert(page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->is_request_queue_empty());
        gl->mutexUnlock();
        response->set_agree(true);

        return;
    }
    
    /*
        捋一下流程：
        1. 一个节点想要某个页面所有权，在本地检查，如果没有对应的远程锁，执行 LRPS/XLock，加锁
        2. 远程 LRPSLock 调用 LockShared/Exclusive，发现无法上锁，调用 SetComputeNodePending，给持有锁的节点发送 Pending 信号
        3. 节点收到 Pending 信号后，尽可能快地释放锁，释放先在本地(设置 is_pending = true，防止本地再加锁)，锁用完后调用 LRPAnyUnlock
        4. LRPAnyUnLock 先把当前节点的远程所有权给取消，如果自己解锁后，可以转移所有权给下一轮节点了，那就转移所有权
        5. 转移完成后，先向下一轮节点广播获得锁成功了，然后通知最后一个解锁的节点将页面推送给下一轮持有锁的节点(跳过本轮持有，下一轮也持有的)
        6. 由于 request_queue 中可能有很多节点的请求，这些请求可能无法在本轮获取锁中拿到锁，因此在解锁之后，如果 request_queue还有元素，需要再调用一次 SetComputeNodePending(Transfer Pending 做的事情)
    */
    virtual void LRPAnyUnLock(::google::protobuf::RpcController* controller,
                    const ::page_table_service::PAnyUnLockRequest* request,
                    ::page_table_service::PAnyUnLockResponse* response,
                    ::google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // LOG(INFO) << "LRPAnyUnlock Remote, node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

            // P4 修复：解锁时把页面最新 LSN 上报给 GPLM（取 max，避免旧值覆盖新值）。
            // GPLM 的 lsn_id 是 Phase 4 Redo 回放的目标 LSN 判断依据，
            // 此前 lazy 释放路径不上报 LSN，导致 gplm_lsn 偏旧或为 0
            {
                LLSN unlock_lsn = request->lsn();
                if (unlock_lsn > 0) {
                    LR_GlobalPageLock* gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
                    gl->mutexLock();
                    if (unlock_lsn > gl->getLsnIDNoBlock()) {
                        gl->setLsnIDNoBlock(unlock_lsn);
                    }
                    gl->mutexUnlock();
                }
            }

            // 简单粗暴：如果 X 锁，need_valid = true,否则 need_validate = false
            // 在这里加速，后面解锁
            bool need_valid = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockAny(node_id);
            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
                                                               
            // 当解锁了本节点后，能够进行下一轮所有权授予的时候，会做两件事情：
            // 1. request_queue：把下一轮的清除，2. hold_lock_nodes：添加下一轮节点
            bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);

            if(need_transfer){
                // 先取当前的 newest，用于通知下一轮节点的数据来源
                valid_info->Global_Lock();
                auto next_nodes = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes();
                assert(!next_nodes.empty());
                
                // true 表示需要等别人推送数据，这里是在锁释放里面的，就是需要 Push 的
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id , valid_info , true);

                valid_info->ReleasePageNoBlock(node_id);
                // 设置完了，更新有效性信息
                for(auto nid : next_nodes){
                    page_valid_table_list_->at(table_id)->setNodeValid(nid, page_id);
                }
                // 在这里解锁 valid_info
                page_valid_table_list_->at(table_id)->setNodeValidAndNewest(next_nodes.front(), page_id);
                // 在这里解锁 LR_Lock
                // std::cout << "Next Pending\n";
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id , immedia_transfer ,valid_info);
            } else{
                valid_info->ReleasePage(node_id);
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();
            }
            // valid_info->ReleasePage(node_id);
            // 添加模拟延迟
            if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        }

    void LRPAnyUnLock_Localcall(const ::page_table_service::PAnyUnLockRequest* request,
                    ::page_table_service::PAnyUnLockResponse* response){
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // LOG(INFO) << "LRPAnyUnlock Remote, node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

            // P4 修复：解锁时把页面最新 LSN 上报给 GPLM（取 max，避免旧值覆盖新值）
            {
                LLSN unlock_lsn = request->lsn();
                if (unlock_lsn > 0) {
                    LR_GlobalPageLock* gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
                    gl->mutexLock();
                    if (unlock_lsn > gl->getLsnIDNoBlock()) {
                        gl->setLsnIDNoBlock(unlock_lsn);
                    }
                    gl->mutexUnlock();
                }
            }

            // 简单粗暴：如果 X 锁，need_valid = true,否则 need_validate = false
            // 在这里加速，后面解锁
            bool need_valid = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockAny(node_id);
            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
                                                               
            // 当解锁了本节点后，能够进行下一轮所有权授予的时候，会做两件事情：
            // 1. request_queue：把下一轮的清除，2. hold_lock_nodes：添加下一轮节点
            bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);

            if(need_transfer){
                // 先取当前的 newest，用于通知下一轮节点的数据来源
                valid_info->Global_Lock();
                auto next_nodes = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes();
                assert(!next_nodes.empty());
                
                // true 表示需要等别人推送数据，这里是在锁释放里面的，就是需要 Push 的
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id , valid_info , true);

                valid_info->ReleasePageNoBlock(node_id);
                // 设置完了，更新有效性信息
                for(auto nid : next_nodes){
                    page_valid_table_list_->at(table_id)->setNodeValid(nid, page_id);
                }
                // 在这里解锁 valid_info
                page_valid_table_list_->at(table_id)->setNodeValidAndNewest(next_nodes.front(), page_id);
                // 在这里解锁 LR_Lock
                // std::cout << "Next Pending\n";
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id , immedia_transfer ,valid_info);
            } else{
                valid_info->ReleasePage(node_id);
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();
            }
        }


    virtual void LRPAnyUnLocks(::google::protobuf::RpcController* controller,
                       const ::page_table_service::PAnyUnLocksRequest* request,
                       ::page_table_service::PAnyUnLockResponse* response,
                       ::google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            for(int i=0; i<request->pages_id_size(); i++){
                page_id_t page_id = request->pages_id(i).page_no();
                table_id_t table_id = request->pages_id(i).table_id();
                node_id_t node_id = request->node_id();
                // bool need_valid = page_lock_table_->LR_GetLock(page_id)->UnlockAny(node_id);
                // LOG(INFO) << "**table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " try to release any lock";
                GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
                bool need_valid = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockAny(node_id);

                // node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id);
                bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);
                // mutex is not release
                if(need_transfer){
                    node_id_t newest_id;
                    for(auto n: page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes()){
                        newest_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(n);
                    }
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id, valid_info , true);
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id, immedia_transfer, valid_info);
                }
            }
            
            // 添加模拟延迟
            if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        }

    // ==================== Instance Recovery RPCs ====================
    virtual void ReportPageStatus(::google::protobuf::RpcController* controller,
                    const ::page_table_service::ReportPageStatusRequest* request,
                    ::page_table_service::ReportPageStatusResponse* response,
                    ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        ApplyReportPageStatus(request, response);
    }

    void ReportPageStatus_Localcall(
                    const ::page_table_service::ReportPageStatusRequest* request,
                    ::page_table_service::ReportPageStatusResponse* response){
        ApplyReportPageStatus(request, response);
    }

    // P0 修复（有效副本判定）：原实现仅凭"LPLM 持锁 + lock_mode!=NONE"就
    // 把汇报者设为 newest 并清除 IR——stale 副本（本地页 LSN 落后于 GPLM
    // 记录的 LSN）会被当成最新数据源，造成数据回退/提前释放 IR。
    // 新契约：汇报者必须携带本地副本页头 LSN（reporter_lsn>0），且
    // reporter_lsn >= GPLM 记录 LSN 才认可为有效副本并清 IR；
    // 否则仅注册 holder，IR 保留给 Phase 4 存储分析兜底。
    //（GPLM LSN 为 0 表示无记录，任何 LSN 都满足）
    void ApplyReportPageStatus(
                    const ::page_table_service::ReportPageStatusRequest* request,
                    ::page_table_service::ReportPageStatusResponse* response){
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t reporter = request->reporter_node_id();
        int lock_mode = request->lock_mode();
        // 有效副本的最低门槛：确有锁持有（非 granting）且能提供页 LSN
        const bool has_valid = request->has_valid_copy() && (lock_mode == 1 || lock_mode == 2) &&
                               request->reporter_lsn() > 0;

        LR_GlobalPageLock* gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);

        gl->mutexLock();
        bool newest_accepted = false;
        if (has_valid) {
            bool exclusive = (lock_mode == 2);
            // 无论 IR 锁是否已释放都要注册 holder（S 锁存在多个 holder 依次汇报的情况）
            gl->RecoverAddHolder(reporter, exclusive);
            const LLSN gplm_lsn = gl->getLsnIDNoBlock();
            if (request->reporter_lsn() >= gplm_lsn) {
                newest_accepted = true;
                if (gl->IsIRLockedNoBlock()) {
                    // 第一个汇报者：加 valid_info 锁，设置 newest，SetValidAndUpdateNewest 内部解锁
                    valid_info->Global_Lock();
                    valid_info->SetValidAndUpdateNewest(reporter);
                } else {
                    // 后续汇报者：使用线程安全版本设置有效性（不改变 newest）
                    valid_info->setNodeStatus(reporter, true);
                }
            } else {
                LOG(WARNING) << "[IR Recovery] ReportPageStatus: reporter " << reporter
                             << " copy is STALE for table=" << table_id << " page=" << page_id
                             << " (reporter_lsn=" << request->reporter_lsn()
                             << " < gplm_lsn=" << gplm_lsn << ") — IR retained for storage analysis";
            }
        }
        bool was_ir_locked = gl->IsIRLockedNoBlock();
        if (was_ir_locked && newest_accepted) {
            gl->ClearIRLock();
        }
        response->set_ir_released(was_ir_locked && newest_accepted);
        gl->mutexUnlock();
    }

    virtual void IRScanComplete(::google::protobuf::RpcController* controller,
                    const ::page_table_service::IRScanCompleteRequest* request,
                    ::page_table_service::IRScanCompleteResponse* response,
                    ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        node_id_t reporter = request->reporter_node_id();
        node_id_t failed_node = request->failed_node_id();

        std::lock_guard<std::mutex> lk(ir_scan_mutex_);
        // P0 修复（barrier 契约）：按故障节点分桶 + reporter 去重。
        // 原实现只做全局计数：早到通知（对端 MarkNodeFailed 先于本节点
        // SetIRScanExpected 执行）会被随后的 Set 重置丢弃，导致本节点
        // 永远等不齐 1/2 → 30s 超时（r2-fault-small-004 根因）；且同一
        // 节点重复通知会多计数、提前放行。
        IRScanState& st = ir_scan_by_failed_[failed_node];
        if (!st.expected_set.empty() && st.expected_set.count(reporter) == 0) {
            // P0 修复（名单验证）：名单外 reporter（旧代/错误配置/已故障节点）
            // 不计入 barrier——防伪造计数提前放行
            LOG(ERROR) << "[IR Recovery] IRScanComplete from UNEXPECTED reporter "
                       << reporter << " for failed node " << failed_node
                       << " — ignored (not in survivor set)";
            return;
        }
        const bool inserted = st.reporters.insert(reporter).second;
        if (inserted) {
            LOG(INFO) << "[IR Recovery] Node " << reporter << " scan complete for failed node "
                      << failed_node << " (" << st.reporters.size() << "/" << st.expected << ")";
        }
        MaybeCompleteIRScanLocked(failed_node);
    }

    void IRScanComplete_Localcall(
                    const ::page_table_service::IRScanCompleteRequest* request,
                    ::page_table_service::IRScanCompleteResponse* response){
        node_id_t reporter = request->reporter_node_id();
        node_id_t failed_node = request->failed_node_id();

        std::lock_guard<std::mutex> lk(ir_scan_mutex_);
        IRScanState& st = ir_scan_by_failed_[failed_node];
        if (!st.expected_set.empty() && st.expected_set.count(reporter) == 0) {
            LOG(ERROR) << "[IR Recovery] IRScanComplete (local) from UNEXPECTED reporter "
                       << reporter << " for failed node " << failed_node
                       << " — ignored (not in survivor set)";
            return;
        }
        const bool inserted = st.reporters.insert(reporter).second;
        if (inserted) {
            LOG(INFO) << "[IR Recovery] Node " << reporter << " scan complete (local) for failed node "
                      << failed_node << " (" << st.reporters.size() << "/" << st.expected << ")";
        }
        MaybeCompleteIRScanLocked(failed_node);
    }

    // 设置期望收到扫描完成通知的存活节点集合（P0 修复：按故障节点分桶，
    // 不清空已到达的 reporter——早到通知依然有效；reporter 去重保证
    // Set 与通知的先后顺序无关）。P0 续（名单验证）：保存完整 survivor
    // 名单，IRScanComplete 只接受名单内 reporter。
    void SetIRScanExpected(node_id_t failed_node, const std::vector<node_id_t>& survivors) {
        std::lock_guard<std::mutex> lk(ir_scan_mutex_);
        IRScanState& st = ir_scan_by_failed_[failed_node];
        st.expected = static_cast<int>(survivors.size());
        st.expected_set.clear();
        st.expected_set.insert(survivors.begin(), survivors.end());
        // 名单变更后复查：剔除不在名单内的已到达 reporter（防旧代/错误
        // 节点的通知提前凑满 barrier）；若名单内 reporter 已满员，
        // MaybeCompleteIRScanLocked 立即放行（Set 晚于通知的情形）
        for (auto it = st.reporters.begin(); it != st.reporters.end();) {
            if (st.expected_set.count(*it) == 0) it = st.reporters.erase(it);
            else ++it;
        }
        st.phase2_complete = false;
        st.remaining_ir_pages.clear();
        MaybeCompleteIRScanLocked(failed_node);
    }

    // Phase 3: 等待 Phase 2 完成，并获取所有剩余 IR 锁页面的信息
    struct IRLockedPageInfo {
        table_id_t table_id;
        page_id_t page_id;
        LLSN gplm_lsn;      // GPLM 中记录的该页面最后已知 LSN
    };

    // 阻塞等待 Phase 2 完成（P0 修复：按故障节点分桶等待 + 失败语义）。
    // 返回 nullopt 表示 barrier 超时/失败——此时存活节点扫描未齐，
    // 剩余 IR 页集合不可信，调用方必须保持隔离、不得继续 Phase 4 发布
    //（原实现"强制完成"会带着不完整信息放行，违反正确性契约）。
    std::optional<std::vector<IRLockedPageInfo>> WaitPhase2AndGetRemainingIRPages(
            node_id_t failed_node, int timeout_seconds = 30) {
        std::unique_lock<std::mutex> lk(ir_scan_mutex_);
        IRScanState& st = ir_scan_by_failed_[failed_node];
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(timeout_seconds);
        while (!st.phase2_complete) {
            if (ir_scan_cv_.wait_until(lk, deadline) == std::cv_status::timeout) {
                if (!st.phase2_complete) {
                    LOG(ERROR) << "[IR Recovery] Phase 2 barrier TIMEOUT ("
                               << timeout_seconds << "s) for failed node " << failed_node
                               << ": " << st.reporters.size() << "/" << st.expected
                               << " nodes reported. Reporting FAILURE — recovery stays "
                                  "isolated (no forced completion).";
                    return std::nullopt;
                }
            }
        }
        return st.remaining_ir_pages;
    }

    // Phase 3: 根据存储层分析结果释放 IR 锁
    void ReleaseIRLockForPage(table_id_t table_id, page_id_t page_id) {
        LR_GlobalPageLock* gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
        VLOG(1) << "[IR Recovery] ReleaseIRLockForPage: table=" << table_id << " page=" << page_id;
        gl->mutexLock();
        if (gl->IsIRLockedNoBlock()) {
            page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->MarkOnluInStorage();
            gl->ClearIRLock();
            recovery_observation::Emit("ir_released", table_id, page_id, 0, 0, 0, "not_a_ready_certificate");
        }
        gl->mutexUnlock();
    }

    // 恢复完成清理（R2 零页根因的第二半）：故障窗口内幸存者可能正常加锁
    // 取到尚未物化的页面（replay 滞后 → 存储返回零页/旧页），并在本页表
    // 注册了 valid 副本（GetValid 置 newest/本节点 status）。这类页不持有
    // IR 锁、不在 Phase 3 清单里，ReleaseIRLockForPage 的 MarkOnluInStorage
    // 覆盖不到——之后 LRPSLock 的 GetValid 命中残留注册会返回
    // need_storage=false / newest=残留节点，取页又指回脏缓冲（fault-011
    // 实测页 2 持续返回全零页）。恢复完成时对本节点接管的故障分区页
    // 无条件标回 storage-only：下次取页 need_storage=true，从 replay 后
    // 的存储取真实页并由 put_page_into_buffer 覆盖旧缓冲。幂等，可与
    // ReleaseIRLockForPage 重复执行。返回是否清掉了残留注册。
    bool InvalidateValidCopiesForPage(table_id_t table_id, page_id_t page_id) {
        LR_GlobalPageLock* gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
        gl->mutexLock();
        bool had_residue = (valid_info->HasAnyValid() != INVALID_NODE_ID);
        if (had_residue) {
            valid_info->MarkOnluInStorage();
            recovery_observation::Emit("valid_residue_cleared", table_id, page_id, 0, 0, 0, "recovery_cleanup");
        }
        gl->mutexUnlock();
        return had_residue;
    }

    private:
    // P0 修复：按故障节点分桶的 Phase 2 barrier 状态（reporter 去重 +
    // 早到通知缓存）。结构仅经 ir_scan_mutex_ 访问。
    struct IRScanState {
        int expected = 0;                 // SetIRScanExpected 设定的存活节点数
        std::set<node_id_t> expected_set; // 本轮故障视图的 survivor 名单（空=尚未 Set）
        std::set<node_id_t> reporters;    // 已汇报节点（去重；Set 之前到达亦保留）
        bool phase2_complete = false;
        std::vector<IRLockedPageInfo> remaining_ir_pages;
    };
    std::map<node_id_t, IRScanState> ir_scan_by_failed_;

    // 桶内 reporters.size() >= expected 且 expected>0 时收集并放行。
    // 要求持有 ir_scan_mutex_
    void MaybeCompleteIRScanLocked(node_id_t failed_node) {
        IRScanState& st = ir_scan_by_failed_[failed_node];
        if (st.phase2_complete || st.expected <= 0) return;
        if ((int)st.reporters.size() < st.expected) return;
        CollectRemainingIRLockedPages(failed_node, st);
    }

    // 收集所有剩余的 IR 锁页面信息，不释放锁，由 Phase 3 处理。
    // 要求持有 ir_scan_mutex_
    void CollectRemainingIRLockedPages(node_id_t failed_node, IRScanState& st) {
        st.remaining_ir_pages.clear();
        int ir_count = 0;
        for (size_t t = 0; t < page_lock_table_list_->size(); t++) {
            if (page_lock_table_list_->at(t) == nullptr) continue;
            for (page_id_t p = 0; p < ComputeNodeBufferPageSize; p++) {
                LR_GlobalPageLock* gl = page_lock_table_list_->at(t)->LR_GetLock(p);
                if (gl->IsIRLockedNoBlock()) {
                    gl->mutexLock();
                    if (gl->IsIRLockedNoBlock()) {
                        LLSN lsn = gl->getLsnIDNoBlock();
                        st.remaining_ir_pages.push_back({(table_id_t)t, p, lsn});
                        ir_count++;
                    }
                    gl->mutexUnlock();
                }
            }
        }
        LOG(INFO) << "[IR Recovery] Phase 2 complete (failed node " << failed_node
                  << "): " << ir_count << " pages still have IR locks, pending Phase 3 analysis";
        st.phase2_complete = true;
        ir_scan_cv_.notify_all();
    }

    private:
    std::vector<GlobalLockTable*>* page_lock_table_list_;
    std::vector<GlobalValidTable*>* page_valid_table_list_;

    // IR Recovery 扫描计数（P0 修复：全部分桶到 ir_scan_by_failed_）
    std::mutex ir_scan_mutex_;
    std::condition_variable ir_scan_cv_;

    public:
    std::atomic<int> immedia_transfer{0};
};
};