#include "server.h"
#include "worker/handler.h"
#include <unistd.h>
#include "workload/ycsb/ycsb_db.h"

namespace twopc_service{
    void TwoPCServiceImpl::GetDataItem(::google::protobuf::RpcController* controller,
                        const ::twopc_service::GetDataItemRequest* request,
                        ::twopc_service::GetDataItemResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        // 从本地取数据
        table_id_t table_id = request->item_id().table_id();
        page_id_t page_id = request->item_id().page_no();
        int slot_id = request->item_id().slot_id();
        bool lock = request->item_id().lock_data();

        // S 锁 
        if(!lock){
            if (SYSTEM_MODE == 2){
                Page* page = server->local_fetch_s_page(table_id, page_id);
                response->set_data(page->get_data(), PAGE_SIZE);
                server->local_release_s_page(table_id, page_id);
            }else if (SYSTEM_MODE == 4){
                Page* page = server->rpc_lazy_fetch_s_page(table_id , page_id);
                response->set_data(page->get_data(), PAGE_SIZE);
                server->rpc_lazy_release_s_page(table_id, page_id);
            }else {
                assert(false);
            }
        }else{
            Page* page = nullptr;
            char* data = nullptr;
            if (SYSTEM_MODE == 2){
                page = server->local_fetch_x_page(table_id, page_id);
            }else if (SYSTEM_MODE == 4){
                page = server->rpc_lazy_fetch_x_page(table_id , page_id);
            }else {
                assert(false);
            }
            data = page->get_data();

            assert(page);
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            RmFileHdr::ptr file_hdr = server->get_file_hdr_cached(table_id);
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));

            // 需要给这个元组加上排他锁
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            if(item->lock == UNLOCKED){
                item->lock = EXCLUSIVE_LOCKED;
                page->set_dirty(true);
                response->set_data(data, PAGE_SIZE);

                tx_id_t tx_id = request->transaction_id();
                LLSN page_new_lsn = server->AddLockLog(tx_id, table_id, {.page_no_ = page_id, .slot_no_ = slot_id}, EXCLUSIVE_LOCKED, (RmPageHdr*)(data));
                if (SYSTEM_MODE == 2){
                    server->get_node()->getLocalPageLockTables(table_id)->GetLock(page_id)->set_newest_lsn(page_new_lsn);
                }
            } else {
                // abort
                response->set_abort(true);
            }

            if (SYSTEM_MODE == 2){
                server->local_release_x_page(table_id, page_id);
            }else {
                server->rpc_lazy_release_x_page(table_id , page_id);
            }
        }

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); 
        return;
    };

    // maybe unused
    void TwoPCServiceImpl::WriteDataItem(::google::protobuf::RpcController* controller,
                        const ::twopc_service::WriteDataItemRequest* request,
                        ::twopc_service::WriteDataItemResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        // 从本地取数据
        table_id_t table_id = request->item_id().table_id();
        page_id_t page_id = request->item_id().page_no();
        int slot_id = request->item_id().slot_id();
        RmFileHdr::ptr file_hdr = server->get_file_hdr_cached(table_id);
        assert(request->data().size() == file_hdr->record_size_);
        char* write_remote_data = (char*)request->data().c_str();

        Page* page = server->local_fetch_x_page(table_id, page_id);
        char* data = page->get_data();
        char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        char *slots = bitmap + file_hdr->bitmap_size_;
        char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));
        DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
        assert(item->lock == EXCLUSIVE_LOCKED);
        // Fix: Use correct pointer arithmetic and offsets. Skip header.
        memcpy((char*)item + sizeof(DataItem), write_remote_data + sizeof(DataItem), file_hdr->record_size_ - sizeof(DataItem));
        item->lock = UNLOCKED;
        server->local_release_x_page(table_id, page_id);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); 
        return;
    };

    void TwoPCServiceImpl::Prepare(::google::protobuf::RpcController* controller,
                       const ::twopc_service::PrepareRequest* request,
                       ::twopc_service::PrepareResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);

        uint64_t tx_id = request->transaction_id();
        TxnLog txn_log;

        LLSN prepare_lsn = server->generate_next_llsn_with_lock();
        BatchEndLogRecord* prepare_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        prepare_log->lsn_ = prepare_lsn;
        global_prepare_log_count++;
        server->AddToLog(prepare_log);
                        
        // Prepare 之前，也需要等待日志落盘
        server->wait_log_flush(prepare_lsn, 1);

        response->set_ok(true);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); 
    };

    void add_milliseconds(struct timespec& ts, long long ms) {
        ts.tv_sec += ms / 1000;
        ts.tv_nsec += (ms % 1000) * 1000000;

        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec += ts.tv_nsec / 1000000000;
            ts.tv_nsec %= 1000000000;
        }
    }

    void TwoPCServiceImpl::Commit(::google::protobuf::RpcController* controller,
                        const ::twopc_service::CommitRequest* request,
                        ::twopc_service::CommitResponse* response,
                        ::google::protobuf::Closure* done){
        uint64_t tx_id = request->transaction_id();
        assert(tx_id >= 0);

        int item_size = request->item_id_size();
        assert(item_size == request->data_size());
        for(int i = 0; i < item_size; i++){
            table_id_t table_id = request->item_id(i).table_id();
            page_id_t page_id = request->item_id(i).page_no();
            assert(table_id < 10000);
            int slot_id = request->item_id(i).slot_id();
            char* write_remote_data = (char*)request->data(i).c_str();
            
            Page* page = nullptr;
            if (SYSTEM_MODE == 2){
                page = server->local_fetch_x_page(table_id, page_id);
            }else if (SYSTEM_MODE == 4){
                page = server->rpc_lazy_fetch_x_page(table_id , page_id);
            }

            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            RmFileHdr::ptr file_hdr = server->get_file_hdr_cached(table_id);
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));

            itemkey_t* pri_key = (itemkey_t*)tuple;
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(item->lock == EXCLUSIVE_LOCKED);
            // Fix: Use correct pointer arithmetic. request->data() contains ONLY value.
            memcpy((char*)item + sizeof(DataItem) , write_remote_data , file_hdr->record_size_ - sizeof(DataItem));

            // memcpy(item->value, write_remote_data, file_hdr->record_size_);
            item->lock = UNLOCKED;

            // 同样的，这里也需要刷一个日志到存储层
            item->value = (uint8_t*)reinterpret_cast<char*>(item) + sizeof(DataItem);
            page->set_dirty(true);
            LLSN page_new_lsn = server->AddUpdateLog(tx_id , item , pri_key , {.page_no_ = page_id , .slot_no_ = slot_id} , (char*)item + sizeof(DataItem) , (RmPageHdr*)data); 
            if (SYSTEM_MODE == 2){
                server->get_node()->getLocalPageLockTables(table_id)->GetLock(page_id)->set_newest_lsn(page_new_lsn);
                server->local_release_x_page(table_id, page_id);
            }else {
                server->rpc_lazy_release_x_page(table_id , page_id);
            }
        }

        // 刷一个 batchEnd 日志
        LLSN batch_end_llsn = server->generate_next_llsn_with_lock();
        BatchEndLogRecord* commit_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        commit_log->lsn_ = batch_end_llsn;
        global_commit_log_count++;
        server->AddToLog(commit_log);  

        // 等待日志刷下去
        server->wait_log_flush(batch_end_llsn , 0);
        
        brpc::ClosureGuard done_guard(done);
        response->set_latency_commit(0);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); 
    };

    void TwoPCServiceImpl::Abort(::google::protobuf::RpcController* controller,
                        const ::twopc_service::AbortRequest* request,
                        ::twopc_service::AbortResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        uint64_t tx_id = request->transaction_id();

        int item_size = request->item_id_size();
        
        for(int i = 0; i < item_size; i++){
            table_id_t table_id = request->item_id(i).table_id();
            page_id_t page_id = request->item_id(i).page_no();
            int slot_id = request->item_id(i).slot_id();

            
            Page* page;
            if (SYSTEM_MODE == 2){
                page = server->local_fetch_x_page(table_id, page_id);
            }else if (SYSTEM_MODE == 4){
                page = server->rpc_lazy_fetch_x_page(table_id , page_id);
            }else {
                assert(false);
            }
            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            
            RmFileHdr::ptr file_hdr = server->get_file_hdr_cached(table_id);
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(item->lock == EXCLUSIVE_LOCKED);
            item->lock = UNLOCKED;

            itemkey_t key = *reinterpret_cast<itemkey_t*>(tuple);
            item->value = (uint8_t*)reinterpret_cast<char*>(item) + sizeof(DataItem);
            page->set_dirty(true);
            LLSN page_new_lsn = server->AddUpdateLog(tx_id , item , &key , {.page_no_ = page_id , .slot_no_ = slot_id} , (char*)item + sizeof(DataItem) , (RmPageHdr*)data);
            
            if (SYSTEM_MODE == 2){
                server->get_node()->getLocalPageLockTables(table_id)->GetLock(page_id)->set_newest_lsn(page_new_lsn);
                server->local_release_x_page(table_id, page_id);
            }else {
                server->rpc_lazy_release_x_page(table_id , page_id);
            }
        }

        // 将日志写入共享log_records
        LLSN batch_end_lsn = server->generate_next_llsn_with_lock();
        BatchEndLogRecord* abort_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        abort_log->lsn_ = batch_end_lsn;
        server->AddToLog(abort_log);  // 写入节点共享的log_records

        server->wait_log_flush(batch_end_lsn);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); 
    };
};

static std::atomic<int> fetch_cnt{0};

Page* ComputeServer::local_fetch_s_page(table_id_t table_id, page_id_t page_id){
    assert(is_partitioned_page(table_id , page_id , node_->getNodeID()));
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    Page* page = node_->getBufferPoolByIndex(table_id)->try_fetch_page(page_id);
    if (page == nullptr){
        LLSN page_newest_lsn = node_->local_page_lock_tables[table_id]->GetLock(page_id)->get_newest_lsn();
        // LOG(INFO) << "fetch S page from storage , table_id = " << table_id << " page_id = " << page_id << " newest lsn = " << page_newest_lsn;
        std::string data = rpc_fetch_page_from_storage_with_lsn(table_id , page_id , page_newest_lsn);
        page = put_page_into_buffer(table_id , page_id , data.c_str() , SYSTEM_MODE);
    }
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    assert(page);
    assert(page->get_page_id().page_no == page_id && page->get_page_id().table_id == table_id);
    return page;
}

Page* ComputeServer::local_fetch_x_page(table_id_t table_id, page_id_t page_id){
    assert(is_partitioned_page(table_id , page_id , node_->getNodeID()));
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    Page* page = node_->local_buffer_pools[table_id]->try_fetch_page(page_id);
    if (page == nullptr){
        // std::string data = rpc_fetch_page_from_storage(table_id , page_id , true);
        LLSN newest_lsn = node_->local_page_lock_tables[table_id]->GetLock(page_id)->get_newest_lsn();
        // LOG(INFO) << "fetch X page from storage , table_id = " << table_id << " page_id = " << page_id << " newest lsn = " << newest_lsn;
        std::string data = rpc_fetch_page_from_storage_with_lsn(table_id , page_id , newest_lsn);
        page = put_page_into_buffer(table_id , page_id , data.c_str() , SYSTEM_MODE);
    }
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    assert(page);
    assert(page->get_page_id().page_no == page_id && page->get_page_id().table_id == table_id);
    return page;
}

void ComputeServer::local_release_s_page(table_id_t table_id, page_id_t page_id){
    // LOG(INFO) << "Release S , table_id = " << table_id << " page_id = " << page_id;
    int lock = node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    if (lock == 0){
        node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
    }
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    // LOG(INFO) << "Release S Over , table_id = " << table_id << " page_id = " << page_id;
    return;
}

void ComputeServer::local_release_x_page(table_id_t table_id, page_id_t page_id){
    // LOG(INFO) << "Release X , table_id = " << table_id << " page_id = " << page_id;
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    // LOG(INFO) << "Release X Over , table_id = " << table_id << " page_id = " << page_id;
    return;
}
