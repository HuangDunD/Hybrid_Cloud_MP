#include "server.h"
#include "storage/prepared_log_record.h"
#include "worker/handler.h"
#include <unistd.h>
#include "workload/ycsb/ycsb_db.h"

namespace twopc_service{
    void TwoPCServiceImpl::AllPrepared(::google::protobuf::RpcController*,
                       const ::twopc_service::AllPreparedRequest* request,
                       ::twopc_service::AllPreparedResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // Signal only: never execute Commit or wait for other RPCs in this callback.
        response->set_ok(server->prepare_notifications_.Accept(*request));
    }

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
            tx_id_t tx_id = request->transaction_id();
            tx_id_t start_ts = request->start_ts();
            if(item->lock == UNLOCKED){
                item->lock = EXCLUSIVE_LOCKED;
                // 在元组内记录加锁事务的时间戳，后续访问可据此识别“本事务自持写锁”
                item->timeStamp = start_ts;
                // holder_node = 持有者事务所在节点（协调者），喂给等待状态的死锁链式走查
                item->holder_node = (uint8_t)request->requester_node();
                // 【修复】lock_owner_node = 真正管理这把锁的节点 = 本参与者：
                // 解锁/过户都发生在这里，等待方必须把队列注册到本节点。
                item->lock_owner_node = (uint8_t)server->get_node()->getNodeID();
                // 只在真正拿到锁之后才登记"本节点活跃事务"：
                // 若在检查锁之前登记，冲突路径（未拿锁就 abort）会让 active_txs_ 永久泄漏，
                // 而协调者的 Abort 只遍历 is_fetched 的写项，永远不会来 FinishTx。
                // 登记必须放在 ReleaseXPage 之前：等待方只有拿到页所有权后才能读到锁。
                server->lock_wait_mgr()->OnTxnBegin(start_ts);
                page->set_dirty(true);
                response->set_data(data, PAGE_SIZE);

                LLSN page_new_lsn = server->AddLockLog(tx_id, table_id, {.page_no_ = page_id, .slot_no_ = slot_id}, EXCLUSIVE_LOCKED, (RmPageHdr*)(data), start_ts, (node_id_t)request->requester_node(), server->get_node()->getNodeID());
                if (SYSTEM_MODE == 2){
                    server->get_node()->getLocalPageLockTables(table_id)->GetLock(page_id)->set_newest_lsn(page_new_lsn);
                }
            } else if (item->lock == EXCLUSIVE_LOCKED && item->timeStamp == start_ts) {
                // 写锁是本事务自己加的，允许继续
                response->set_data(data, PAGE_SIZE);
            } else {
                // 写锁是其它事务加的
                // 如果是 NO-WAIT 模式，那么直接回滚
                response->set_abort(true);
                // 否则就记录下当前持有锁的是谁，做锁等待关系图
                response->set_holder_ts(item->timeStamp);
                response->set_holder_node((int32_t)item->holder_node);
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

        PendingHandoff lw_handoff;
        Page* page = server->FetchXPageForUnlock(
            table_id, Rid{page_id, slot_id}, &lw_handoff,
            [&] { return server->local_fetch_x_page(table_id, page_id); },
            [&] { server->local_release_x_page(table_id, page_id); });
        char* data = page->get_data();
        char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        char *slots = bitmap + file_hdr->bitmap_size_;
        char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));
        DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
        assert(item->lock == EXCLUSIVE_LOCKED);
        // Fix: Use correct pointer arithmetic and offsets. Skip header.
        memcpy((char*)item + sizeof(DataItem), write_remote_data + sizeof(DataItem), file_hdr->record_size_ - sizeof(DataItem));
        // 方案B：解锁必须走过户，否则等待队列排不空（holder_ts 取元组里的锁 owner ts）
        const uint64_t lw_holder_ts = item->timeStamp;
        const bool lw_handed = server->UnlockTupleWithHandoff2PC(
            table_id, {.page_no_ = page_id, .slot_no_ = slot_id}, item, lw_holder_ts, &lw_handoff);
        if (!lw_handed) {
            item->lock = UNLOCKED;
        }
        server->local_release_x_page(table_id, page_id);
        if (lw_handed) server->DeliverHandoff2PC(lw_handoff);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); 
        return;
    };

    void TwoPCServiceImpl::Prepare(::google::protobuf::RpcController* controller,
                       const ::twopc_service::PrepareRequest* request,
                       ::twopc_service::PrepareResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        const bool sample = server->OnlineCostActive() &&
            (prepared_2pc::IsLegacyPrepare(*request) ||
             request->protocol_version() == prepared_2pc::kParticipantReplyVersion);
        page_cost::CommitCostReply cost_reply(sample ? &server->page_cost_model_.online : nullptr,
                                             sample ? response->mutable_cost() : nullptr);

        response->set_ok(false);
        if (SYSTEM_MODE != 2 && SYSTEM_MODE != 4) return;
        // The coordinator's request selects the transaction protocol. A node
        // can participate in either protocol regardless of its own coordinator
        // setting; Commit likewise uses its explicit LEGACY/PREPARED kind.
        if (prepared_2pc::IsLegacyPrepare(*request)) {
            const uint64_t tx_id = request->transaction_id();
            auto* log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
            global_prepare_log_count++;
            const LLSN prepare_lsn = server->AddMetadataLog(
                log, static_cast<int>(page_cost::LogPhase::Prepare));
            server->wait_log_flush(prepare_lsn, 1);
            response->set_ok(true);
            if (NetworkLatency != 0) usleep(NetworkLatency);
            return;
        }
        if (!server->IsLogEnabled() ||
            !prepared_2pc::ValidRequest(*request) || !PreparedLogRecord::Fits(*request) ||
            request->participant_node() != server->get_node()->getNodeID() ||
            request->identity().coordinator_node() >= ComputeNodeCount) return;
        for (int node : request->participants()) {
            if (node >= ComputeNodeCount) return;
        }

        bool inserted = false;
        auto context = server->prepared_txns_.Begin(*request, &inserted);
        if (!context) return; // same identity, conflicting write set/metadata
        if (!inserted) {
            response->set_ok(server->prepared_txns_.IsPrepared(context));
            return; // an in-flight duplicate is not another YES
        }
        for (const auto& write : context->request.writes()) {
            const auto& id = write.item_id();
            const table_id_t table_id = id.table_id();
            const page_id_t page_id = id.page_no();
            // Validate IDs before consulting metadata or touching a page.
            if (table_id >= server->table_name_meta.size() ||
                write.table_name() != server->getTableNameByTableID(table_id) ||
                server->get_node_id_by_page_id(table_id, page_id) != request->participant_node()) {
                server->prepared_txns_.Erase(context);
                return;
            }
            const auto hdr = server->get_file_hdr_cached(table_id);
            if (hdr->record_size_ < sizeof(DataItem) ||
                write.value().size() != hdr->record_size_ - sizeof(DataItem) ||
                id.slot_id() >= hdr->num_records_per_page_ || page_id >= hdr->num_pages_) {
                server->prepared_txns_.Erase(context);
                return;
            }
            Page* page = SYSTEM_MODE == 2 ? server->local_fetch_x_page(table_id, page_id)
                                         : server->rpc_lazy_fetch_x_page(table_id, page_id);
            char* bitmap = page->get_data() + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            char* tuple = bitmap + hdr->bitmap_size_ + id.slot_id() * (hdr->record_size_ + sizeof(itemkey_t));
            const auto* item = reinterpret_cast<const DataItem*>(tuple + sizeof(itemkey_t));
            itemkey_t key;
            memcpy(&key, tuple, sizeof(key));
            const bool valid = Bitmap::is_set(bitmap, id.slot_id()) && key == write.key() &&
                item->valid && item->table_id == table_id && item->value_size == write.value().size() &&
                item->lock == EXCLUSIVE_LOCKED && item->timeStamp == request->start_ts() &&
                item->holder_node == request->identity().coordinator_node() &&
                item->lock_owner_node == request->participant_node();
            if (SYSTEM_MODE == 2) server->local_release_x_page(table_id, page_id);
            else server->rpc_lazy_release_x_page(table_id, page_id);
            if (!valid) {
                server->prepared_txns_.Erase(context);
                return;
            }
        }
        // Only owned business bytes are logged. Never touch a page LSN, change
        // business values or release tuple locks while recording the vote.
        auto* log = new PreparedLogRecord(context->request);
        const LLSN prepare_lsn = server->AddMetadataLog(log,
            sample ? static_cast<int>(page_cost::LogPhase::Prepare) : -1);
        global_prepare_log_count++;
        server->wait_log_flush(prepare_lsn, 1);
        server->prepared_txns_.MarkPrepared(context, prepare_lsn);
        response->set_ok(true);
        if (NetworkLatency != 0) usleep(NetworkLatency);
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
        brpc::ClosureGuard done_guard(done);
        const bool prepared = request->kind() == CommitRequest::PREPARED;
        if (request->kind() != CommitRequest::LEGACY && !prepared) return;
        std::shared_ptr<prepared_2pc::Context> context;
        if (prepared) {
            if (!server->IsLogEnabled() || !request->has_identity() ||
                !prepared_2pc::ValidIdentity(request->identity()) ||
                request->item_id_size() != 0 || request->data_size() != 0) return;
            context = server->prepared_txns_.TakeForCommit(*request);
            if (!context) {
                LOG(ERROR) << "Prepared Commit has no matching PREPARED context";
                return; // never fall back to legacy or pretend it committed
            }
        }
        const bool sample = server->OnlineCostActive() &&
            (!prepared || context->request.protocol_version() == prepared_2pc::kParticipantReplyVersion);
        page_cost::CommitCostReply cost_reply(sample ? &server->page_cost_model_.online : nullptr,
                                             sample ? response->mutable_cost() : nullptr);
        uint64_t tx_id = request->transaction_id();
        assert(tx_id >= 0);
        uint64_t commit_ts = request->commit_ts();

        int item_size = prepared ? context->request.writes_size() : request->item_id_size();
        if (!prepared) assert(item_size == request->data_size());
        uint64_t tx_holder_ts = 0;   // 【修复】该事务在本参与者持有的锁的 owner ts
        for(int i = 0; i < item_size; i++){
            const auto& id = prepared ? context->request.writes(i).item_id() : request->item_id(i);
            table_id_t table_id = id.table_id();
            page_id_t page_id = id.page_no();
            assert(table_id < 10000);
            int slot_id = id.slot_id();
            const char* write_remote_data = prepared ? context->request.writes(i).value().data()
                                                     : request->data(i).data();
            
            PendingHandoff lw_handoff;
            CHECK(SYSTEM_MODE == 2 || SYSTEM_MODE == 4);
            Page* page = server->FetchXPageForUnlock(
                table_id, Rid{page_id, slot_id}, &lw_handoff,
                [&] { return SYSTEM_MODE == 2 ? server->local_fetch_x_page(table_id, page_id)
                                             : server->rpc_lazy_fetch_x_page(table_id, page_id); },
                [&] { if (SYSTEM_MODE == 2) server->local_release_x_page(table_id, page_id);
                      else server->rpc_lazy_release_x_page(table_id, page_id); });

            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            RmFileHdr::ptr file_hdr = server->get_file_hdr_cached(table_id);
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));

            itemkey_t* pri_key = (itemkey_t*)tuple;
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(item->lock == EXCLUSIVE_LOCKED);
            if (prepared) {
                CHECK(item->lock == EXCLUSIVE_LOCKED && item->timeStamp == request->start_ts() &&
                      item->holder_node == request->identity().coordinator_node() &&
                      item->lock_owner_node == server->get_node()->getNodeID());
                CHECK(*pri_key == context->request.writes(i).key());
            }
            // Fix: Use correct pointer arithmetic. request->data() contains ONLY value.
            memcpy((char*)item + sizeof(DataItem) , write_remote_data , file_hdr->record_size_ - sizeof(DataItem));

            // memcpy(item->value, write_remote_data, file_hdr->record_size_);
            item->commitTimeStamp = commit_ts;
            // 方案B：解锁必须走过户，否则等待队列排不空（holder_ts 取元组里的锁 owner ts）
            const uint64_t lw_holder_ts = item->timeStamp;
            tx_holder_ts = lw_holder_ts;
            const bool lw_handed = server->UnlockTupleWithHandoff2PC(
                table_id, {.page_no_ = page_id, .slot_no_ = slot_id}, item, lw_holder_ts, &lw_handoff);
            if (!lw_handed) {
                item->lock = UNLOCKED;
            }

            // 同样的，这里也需要刷一个日志到存储层
            item->value = (uint8_t*)reinterpret_cast<char*>(item) + sizeof(DataItem);
            page->set_dirty(true);
            LLSN page_new_lsn = server->AddUpdateLog(tx_id , item , pri_key , {.page_no_ = page_id , .slot_no_ = slot_id} , (char*)item + sizeof(DataItem) , (RmPageHdr*)data , commit_ts);
            if (SYSTEM_MODE == 2){
                server->get_node()->getLocalPageLockTables(table_id)->GetLock(page_id)->set_newest_lsn(page_new_lsn);
                server->local_release_x_page(table_id, page_id);
            }else {
                server->rpc_lazy_release_x_page(table_id , page_id);
            }
            if (lw_handed) server->DeliverHandoff2PC(lw_handoff);
        }
        // 【修复】本事务在本参与者的锁已全部过户/释放，注销它的活跃身份；
        // FinishTx 会体检"还有没有挂在我名下的等待队列"，有就 assert 暴露
        if (tx_holder_ts != 0) server->lock_wait_mgr()->FinishTx(tx_holder_ts);

        auto* commit_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        const LLSN batch_end_llsn = server->AddMetadataLog(commit_log,
            sample ? static_cast<int>(page_cost::LogPhase::Commit) : -1);
        global_commit_log_count++;
        if (prepared) {
            // Both baseline v1 and optimized v2 persisted the full write set in
            // Prepare. UPDATE records own copies and all handoffs are complete;
            // the end log remains queued without delaying the Commit response.
            server->prepared_txns_.Erase(context);
        } else {
            // Includes the single remote writer: retain its original durability.
            server->wait_log_flush(batch_end_llsn, 0);
        }
        response->set_ok(true);
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
        uint64_t tx_holder_ts = 0;   // 【修复】该事务在本参与者持有的锁的 owner ts
        
        for(int i = 0; i < item_size; i++){
            table_id_t table_id = request->item_id(i).table_id();
            page_id_t page_id = request->item_id(i).page_no();
            int slot_id = request->item_id(i).slot_id();

            
            PendingHandoff lw_handoff;
            CHECK(SYSTEM_MODE == 2 || SYSTEM_MODE == 4);
            Page* page = server->FetchXPageForUnlock(
                table_id, Rid{page_id, slot_id}, &lw_handoff,
                [&] { return SYSTEM_MODE == 2 ? server->local_fetch_x_page(table_id, page_id)
                                             : server->rpc_lazy_fetch_x_page(table_id, page_id); },
                [&] { if (SYSTEM_MODE == 2) server->local_release_x_page(table_id, page_id);
                      else server->rpc_lazy_release_x_page(table_id, page_id); });

            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            
            RmFileHdr::ptr file_hdr = server->get_file_hdr_cached(table_id);
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(item->lock == EXCLUSIVE_LOCKED);
            // 方案B：解锁必须走过户，否则等待队列排不空（holder_ts 取元组里的锁 owner ts）
            const uint64_t lw_holder_ts = item->timeStamp;
            tx_holder_ts = lw_holder_ts;
            const bool lw_handed = server->UnlockTupleWithHandoff2PC(
                table_id, {.page_no_ = page_id, .slot_no_ = slot_id}, item, lw_holder_ts, &lw_handoff);
            if (!lw_handed) {
                item->lock = UNLOCKED;
            }

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
            if (lw_handed) server->DeliverHandoff2PC(lw_handoff);
        }
        // 【修复】abort 同样要在本参与者注销该事务的活跃身份
        if (tx_holder_ts != 0) server->lock_wait_mgr()->FinishTx(tx_holder_ts);

        // 将日志写入共享log_records
        BatchEndLogRecord* abort_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        const LLSN batch_end_lsn = server->AddMetadataLog(abort_log);

        server->wait_log_flush(batch_end_lsn, benchmark_stats::AbortLog);

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
        node_->fetch_from_storage_cnt++;
    } else {
        node_->fetch_from_local_cnt++;
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
        node_->fetch_from_storage_cnt++;
    } else {
        node_->fetch_from_local_cnt++;
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
