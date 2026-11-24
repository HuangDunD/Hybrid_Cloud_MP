#include "server.h"
#include "dtx/mvcc_2pc.h"
#include "mvcc/row.h"
#include "storage/prepared_log_record.h"
#include "worker/handler.h"
#include <unistd.h>
#include "workload/ycsb/ycsb_db.h"

namespace twopc_service {
namespace {
// The participant uses the same page-access primitive as its transaction mode.
Page* FetchPage(ComputeServer& server, table_id_t table, page_id_t page, bool exclusive,
                benchmark_stats::Stage phase = benchmark_stats::StageCount) {
    if (SYSTEM_MODE == 2)
        return exclusive ? server.local_fetch_x_page(table, page) : server.local_fetch_s_page(table, page);
    CHECK(SYSTEM_MODE == 4);
    return exclusive ? server.rpc_lazy_fetch_x_page(table, page, phase) : server.rpc_lazy_fetch_s_page(table, page, phase);
}
void ReleasePage(ComputeServer& server, table_id_t table, page_id_t page, bool exclusive) {
    if (SYSTEM_MODE == 2) {
        if (exclusive) server.local_release_x_page(table, page);
        else server.local_release_s_page(table, page);
    } else {
        CHECK(SYSTEM_MODE == 4);
        if (exclusive) server.rpc_lazy_release_x_page(table, page);
        else server.rpc_lazy_release_s_page(table, page);
    }
}
size_t TupleOffset(const RmFileHdr& hdr, int slot) {
    CHECK(slot >= 0 && slot < hdr.num_records_per_page_);
    return sizeof(RmPageHdr) + OFFSET_PAGE_HDR + hdr.bitmap_size_ +
           size_t(slot) * (hdr.record_size_ + sizeof(itemkey_t));
}
itemkey_t KeyAt(const char* data, const RmFileHdr& hdr, int slot) {
    itemkey_t key;
    memcpy(&key, data + TupleOffset(hdr, slot), sizeof(key));
    return key;
}
DataItem* ItemAt(char* data, const RmFileHdr& hdr, int slot) {
    return reinterpret_cast<DataItem*>(data + TupleOffset(hdr, slot) + sizeof(itemkey_t));
}
undo::RowId Identify(table_id_t table, const Rid& rid, itemkey_t key) {
    return {uint32_t(table), uint32_t(rid.page_no_), uint32_t(rid.slot_no_), key};
}
char* EncodeRow(const undo::RowImage& row, const RmFileHdr& hdr) {
    CHECK(sizeof(DataItem) + row.value.size() == size_t(hdr.record_size_));
    char* out = new char[hdr.record_size_];
    DataItem copy(row.id.table, row.value.size());
    mvcc::CopyToItem(copy, row);
    memcpy(out, &copy, sizeof(DataItem));
    memcpy(out + sizeof(DataItem), row.value.data(), row.value.size());
    auto* item = reinterpret_cast<DataItem*>(out);
    item->value = reinterpret_cast<uint8_t*>(out + sizeof(DataItem));
    return out;
}
void RecordPageLsn(ComputeServer& server, table_id_t table, page_id_t page, LLSN lsn) {
    // Mode 2 re-fetches an evicted page relative to this local progress point.
    if (SYSTEM_MODE == 2)
        server.get_node()->getLocalPageLockTables(table)->GetLock(page)->set_newest_lsn(lsn);
}
void CheckParticipantLock(DataItem* item, uint64_t start_ts, node_id_t coordinator,
                          node_id_t participant) {
    CHECK(item->lock == EXCLUSIVE_LOCKED && item->timeStamp == start_ts &&
          item->txn_home_node == coordinator && item->lock_wait_record_node == participant)
        << "participant row lock does not belong to this transaction: lock=" << item->lock
        << " ts=" << item->timeStamp << " home=" << int(item->txn_home_node)
        << " queue=" << int(item->lock_wait_record_node) << " expected_ts=" << start_ts
        << " expected_home=" << coordinator << " expected_queue=" << participant;
}
}  // namespace

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

    if (request->has_mvcc()) {
        server->HandleMvccGetDataItem(request, response, table_id, page_id, slot_id, lock);
        if (NetworkLatency != 0) usleep(NetworkLatency);
        return;
    }

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
        if (!server->ResolveFinishedWriter(table_id, Rid{page_id, slot_id},
                request->transaction_id(), request->start_ts(), request->requester_node(), page)) {
            server->ReleaseXPage(table_id, page_id);
            response->set_abort(true);
            return;
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
            // txn_home_node = 持有者事务所在节点（协调者），喂给等待状态的死锁链式走查
            item->txn_home_node = (uint8_t)request->requester_node();
            // 【修复】lock_wait_record_node = 真正管理这把锁的节点 = 本参与者：
            // 解锁/过户都发生在这里，等待方必须把队列注册到本节点。
            item->lock_wait_record_node = (uint8_t)server->get_node()->getNodeID();
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
        } else if (item->lock == EXCLUSIVE_LOCKED && item->timeStamp == start_ts &&
                   item->txn_home_node == (uint8_t)request->requester_node()) {
            // 写锁是本事务自己加的，允许继续
            response->set_data(data, PAGE_SIZE);
        } else {
            // 写锁是其它事务加的
            // 如果是 NO-WAIT 模式，那么直接回滚
            response->set_abort(true);
            // 否则就记录下当前持有锁的是谁，做锁等待关系图
            response->set_holder_ts(item->timeStamp);
            response->set_txn_home_node((int32_t)item->txn_home_node);
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
    if (server->IsMvccWorkload() && !request->has_mvcc()) return;
    // The coordinator's request selects the transaction protocol. A node
    // can participate in either protocol regardless of its own coordinator
    // setting; Commit likewise uses its explicit LEGACY/PREPARED kind.
    if (prepared_2pc::IsLegacyPrepare(*request)) {
        const uint64_t tx_id = request->transaction_id();
        auto* log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        global_prepare_log_count++;
        const LLSN prepare_lsn = server->AppendLogAndWait(
            log, benchmark_stats::PrepareLog, static_cast<int>(page_cost::LogPhase::Prepare));
        response->set_ok(true);
        response->set_durable_lsn(prepare_lsn);
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
    const bool mvcc = request->has_mvcc();
    mvcc::Context mvcc_context{};
    bool inserted = false;
    auto context = server->prepared_txns_.Begin(*request, &inserted);
    if (!context) return; // same identity, conflicting write set/metadata
    if (!inserted) {
        const auto prefix = server->prepared_txns_.PreparePrefix(context);
        response->set_ok(prefix != 0);
        response->set_durable_lsn(prefix);
        return;
    }
    if (mvcc) {
        mvcc_context = mvcc_2pc::DecodeContext(request->mvcc());
        server->Mvcc().Participant(mvcc_context);
    }
    for (int i = 0; i < context->request.writes_size(); ++i) {
        const auto& write = context->request.writes(i);
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
        Page* page = FetchPage(*server, table_id, page_id, true, benchmark_stats::Commit);
        char* bitmap = page->get_data() + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        char* tuple = bitmap + hdr->bitmap_size_ + id.slot_id() * (hdr->record_size_ + sizeof(itemkey_t));
        const auto* item = reinterpret_cast<const DataItem*>(tuple + sizeof(itemkey_t));
        itemkey_t key;
        memcpy(&key, tuple, sizeof(key));
        const bool valid = Bitmap::is_set(bitmap, id.slot_id()) && key == write.key() &&
            item->valid && item->table_id == table_id && item->value_size == write.value().size() &&
            item->lock == EXCLUSIVE_LOCKED && item->timeStamp == request->start_ts() &&
            item->txn_home_node == request->identity().coordinator_node() &&
            item->lock_wait_record_node == request->participant_node();
        undo::RowImage old;
        if (valid && mvcc) old = mvcc::CopyRow(page->get_data(), *hdr, Identify(table_id, Rid{page_id, id.slot_id()}, key));
        ReleasePage(*server, table_id, page_id, true);
        if (!valid) {
            server->prepared_txns_.Erase(context);
            return;
        }
        if (mvcc) {
            CHECK(!old.version.deleted) << "prepared overwrite of an existing tombstone";
            // Append outside the main page latch; the row lock keeps the
            // pre-image stable and the refetch below proves it.
            const auto ref = server->Mvcc().Undo().Append(mvcc_context.id, old);
            CHECK(!ref.empty()) << "2PC Prepare lost a reserved undo page";
            page = FetchPage(*server, table_id, page_id, true, benchmark_stats::Commit);
            const auto* current = reinterpret_cast<const DataItem*>(
                page->get_data() + sizeof(RmPageHdr) + OFFSET_PAGE_HDR + hdr->bitmap_size_ +
                id.slot_id() * (hdr->record_size_ + sizeof(itemkey_t)) + sizeof(itemkey_t));
            CheckParticipantLock(const_cast<DataItem*>(current), request->start_ts(),
                                 request->identity().coordinator_node(), request->participant_node());
            const auto unchanged = mvcc::CopyRow(page->get_data(), *hdr,
                Identify(table_id, Rid{page_id, id.slot_id()}, key));
            CHECK(undo::EncodeRow(unchanged) == undo::EncodeRow(old))
                << "prepared pre-image changed while the row lock was held";
            ReleasePage(*server, table_id, page_id, true);
            context->request.mutable_writes(i)->set_undo(undo::EncodeRef(ref));
        }
    }
    // Only owned business bytes are logged. Never touch a page LSN, change
    // business values or release tuple locks while recording the vote.
    if (mvcc && !server->mvcc_releases_.ExpectWriter(request->mvcc(), true)) {
        server->prepared_txns_.Erase(context);
        return;
    }
    auto* log = new PreparedLogRecord(context->request);
    const LLSN prepare_lsn = server->AppendLogAndWait(log, benchmark_stats::PrepareLog,
        sample ? static_cast<int>(page_cost::LogPhase::Prepare) : -1);
    global_prepare_log_count++;
    server->prepared_txns_.MarkPrepared(context, prepare_lsn);
    response->set_ok(true);
    response->set_durable_lsn(prepare_lsn);
    if (mvcc) server->RunMvccGate(mvcc_context.id, request->participant_node(), "prepare");
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
    if (server->IsMvccWorkload() && !request->has_mvcc()) return;
    if (request->has_mvcc()) {
        server->HandleMvccCommit(request, response, prepared);
        if (NetworkLatency != 0) usleep(NetworkLatency);
        return;
    }
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
                                         : server->rpc_lazy_fetch_x_page(table_id, page_id, benchmark_stats::Commit); },
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
                  item->txn_home_node == request->identity().coordinator_node() &&
                  item->lock_wait_record_node == server->get_node()->getNodeID());
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
    const int cost_phase = sample ? static_cast<int>(page_cost::LogPhase::Commit) : -1;
    global_commit_log_count++;
    if (prepared) {
        // Both baseline v1 and optimized v2 persisted the full write set in
        // Prepare. UPDATE records own copies and all handoffs are complete;
        // the end log remains queued without delaying the Commit response.
        server->AddMetadataLog(commit_log, cost_phase);
        server->prepared_txns_.Erase(context);
    } else {
        // Includes the single remote writer: retain its original durability.
        server->AppendLogAndWait(commit_log, benchmark_stats::CommitLog, cost_phase);
    }
    response->set_ok(true);
    response->set_latency_commit(0);

    // 添加模拟延迟
    if (NetworkLatency != 0)  usleep(NetworkLatency);
};

void TwoPCServiceImpl::Release(::google::protobuf::RpcController*,
                               const ::twopc_service::ReleaseRequest* request,
                               ::twopc_service::ReleaseResponse* response,
                               ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_ok(false);
    if (!request->has_mvcc()) return;
    server->HandleMvccRelease(request, response);
    if (NetworkLatency != 0) usleep(NetworkLatency);
}

void TwoPCServiceImpl::Abort(::google::protobuf::RpcController* controller,
                    const ::twopc_service::AbortRequest* request,
                    ::twopc_service::AbortResponse* response,
                    ::google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);
    uint64_t tx_id = request->transaction_id();

    if (server->IsMvccWorkload()) {
        if (!request->has_mvcc() || !mvcc_2pc::ValidContext(request->mvcc())) return;
        const auto context = mvcc_2pc::DecodeContext(request->mvcc());
        ReleaseRequest release;
        release.set_transaction_id(tx_id);
        release.set_start_ts((context.id.local << 8) | context.id.home);
        *release.mutable_mvcc() = request->mvcc();
        release.set_finish_only(true);
        *release.mutable_item_id() = request->item_id();
        ReleaseResponse released;
        server->HandleMvccRelease(&release, &released);
        response->set_ok(released.ok());
        return;
    }
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
                                         : server->rpc_lazy_fetch_x_page(table_id, page_id, benchmark_stats::Abort); },
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
    server->AppendLogAndWait(abort_log, benchmark_stats::AbortLog);

    // 添加模拟延迟
    if (NetworkLatency != 0)  usleep(NetworkLatency);
};
};

// **************** participant MVCC helpers ****************
char* ComputeServer::Fetch2pcReadRow(table_id_t table_id, const Rid& rid, const mvcc::Context& context,
                                    bool* not_found) {
    CHECK(IsMvccWorkload());
    const auto snapshot = Mvcc().Participant(context);
    const auto hdr = get_file_hdr_cached(table_id);
    Page* page = twopc_service::FetchPage(*this, table_id, rid.page_no_, false);
    const auto key = twopc_service::KeyAt(page->get_data(), *hdr, rid.slot_no_);
    const auto id = twopc_service::Identify(table_id, rid, key);
    auto row = mvcc::CopyRow(page->get_data(), *hdr, id);
    twopc_service::ReleasePage(*this, table_id, rid.page_no_, false);
    row = Mvcc().VisibleRow(snapshot, std::move(row));
    *not_found = row.version.deleted;
    return twopc_service::EncodeRow(row, *hdr);
}

char* ComputeServer::Fetch2pcWriteRow(table_id_t table_id, const Rid& rid, const mvcc::Context& context,
                                     tx_id_t tx_id, tx_id_t start_ts, node_id_t requester,
                                     uint64_t* holder_ts, node_id_t* holder_home, bool* conflict,
                                     itemkey_t* out_key, bool* full) {
    CHECK(IsMvccWorkload());
    Mvcc().Participant(context);
    if (!Mvcc().Undo().AdmitNewWrite(context.id, undo::Manager::WriteSlot{table_id, rid.page_no_, rid.slot_no_})) {
        if (full) *full = true;
        *conflict = true;
        return nullptr;
    }
    const auto hdr = get_file_hdr_cached(table_id);
    const auto self = node_->getNodeID();
    Page* page = twopc_service::FetchPage(*this, table_id, rid.page_no_, true);
    // Mix can meet a Lazy writer's deferred committed lock; 2PC always releases
    // its own locks through Release.
    if (SYSTEM_MODE == 4 &&
        !ResolveFinishedWriter(table_id, rid, tx_id, start_ts, requester, page)) {
        twopc_service::ReleasePage(*this, table_id, rid.page_no_, true);
        ++mvcc_lock_conflicts_;
        *conflict = true;
        return nullptr;
    }
    auto* item = twopc_service::ItemAt(page->get_data(), *hdr, rid.slot_no_);
    const auto key = twopc_service::KeyAt(page->get_data(), *hdr, rid.slot_no_);
    if (item->lock == UNLOCKED) {
        item->lock = EXCLUSIVE_LOCKED;
        item->timeStamp = start_ts;
        item->txn_home_node = (uint8_t)requester;
        item->lock_wait_record_node = (uint8_t)self;
        page->set_dirty(true);
        const LLSN lsn = AddLockLog(tx_id, table_id, rid, EXCLUSIVE_LOCKED,
            reinterpret_cast<RmPageHdr*>(page->get_data()), start_ts, requester, self);
        if (SYSTEM_MODE == 2)
            node_->getLocalPageLockTables(table_id)->GetLock(rid.page_no_)->set_newest_lsn(lsn);
        lock_wait_mgr()->OnTxnBegin(start_ts);
    } else if (!(item->lock == EXCLUSIVE_LOCKED && item->timeStamp == start_ts &&
                 item->txn_home_node == requester)) {
        if (holder_ts != nullptr) *holder_ts = item->timeStamp;
        if (holder_home != nullptr) *holder_home = item->txn_home_node;
        twopc_service::ReleasePage(*this, table_id, rid.page_no_, true);
        ++mvcc_lock_conflicts_;
        *conflict = true;
        return nullptr;
    }
    const auto row = mvcc::CopyRow(page->get_data(), *hdr,
                                  twopc_service::Identify(table_id, rid, key));
    twopc_service::ReleasePage(*this, table_id, rid.page_no_, true);
    if (out_key != nullptr) *out_key = key;
    return twopc_service::EncodeRow(row, *hdr);
}

void ComputeServer::Finish2pcParticipant(const mvcc::Context& context) {
    CHECK(IsMvccWorkload());
    if (context.id.home != node_->getNodeID()) Mvcc().FinishParticipant(context);
    else Mvcc().SealParticipantUndo(context.id);
}

LLSN ComputeServer::Install2pcMvccWrite(const twopc_service::PreparedWrite& write,
                                        const mvcc::Context& context, tx_id_t start_ts,
                                        node_id_t coordinator, node_id_t participant,
                                        const std::string& value, bool deleted,
                                        const undo::Ref& undo) {
    const auto& item_id = write.item_id();
    const table_id_t table_id = item_id.table_id();
    const Rid rid{item_id.page_no(), item_id.slot_id()};
    const auto hdr = get_file_hdr_cached(table_id);
    CHECK(value.size() == size_t(hdr->record_size_ - sizeof(DataItem)));
    CHECK(undo.page.writer == uint32_t(participant) && undo.page.epoch == context.id.epoch);
    Page* page = twopc_service::FetchPage(*this, table_id, rid.page_no_, true, benchmark_stats::Commit);
    auto* item = twopc_service::ItemAt(page->get_data(), *hdr, rid.slot_no_);
    twopc_service::CheckParticipantLock(item, start_ts, coordinator, participant);
    auto key = twopc_service::KeyAt(page->get_data(), *hdr, rid.slot_no_);
    CHECK(key == write.key());
    const auto current = mvcc::CopyRow(page->get_data(), *hdr,
                                       twopc_service::Identify(table_id, rid, key));
    twopc_service::ReleasePage(*this, table_id, rid.page_no_, true);
    const auto record = Mvcc().Undo().Read(undo, current.id);
    CHECK(!record.delta && undo::EncodeRow(record.old) == undo::EncodeRow(current))
        << "prepared pre-image differs from the locked row";
    auto after = current;
    after.value = value;
    after.version.deleted = deleted;
    after.version.creator = context.id;
    after.version.initial = false;
    after.previous = undo;
    page = twopc_service::FetchPage(*this, table_id, rid.page_no_, true, benchmark_stats::Commit);
    item = twopc_service::ItemAt(page->get_data(), *hdr, rid.slot_no_);
    twopc_service::CheckParticipantLock(item, start_ts, coordinator, participant);
    CHECK(undo::EncodeRow(mvcc::CopyRow(page->get_data(), *hdr, after.id)) ==
          undo::EncodeRow(current)) << "prepared row changed while the lock was held";
    auto* disk = mvcc::RowAt(page->get_data(), *hdr, after.id);
    mvcc::InstallRow(*disk, after);
    page->set_dirty(true);
    const auto lsn = AddUpdateLog(context.id.local, disk, &key, rid,
                 reinterpret_cast<char*>(disk) + sizeof(DataItem),
                 reinterpret_cast<RmPageHdr*>(page->get_data()), 0, UpdateHistory::Install);
    twopc_service::RecordPageLsn(*this, table_id, rid.page_no_, lsn);
    twopc_service::ReleasePage(*this, table_id, rid.page_no_, true);
    return lsn;
}

void ComputeServer::HandleMvccGetDataItem(const twopc_service::GetDataItemRequest* request,
                                          twopc_service::GetDataItemResponse* response,
                                          table_id_t table_id, page_id_t page_id, int slot_id,
                                          bool lock) {
    const auto context = mvcc_2pc::DecodeContext(request->mvcc());
    bool not_found = false;
    if (!lock) {
        char* row = Fetch2pcReadRow(table_id, Rid{page_id, slot_id}, context, &not_found);
        response->set_data(row, get_file_hdr_cached(table_id)->record_size_);
        delete[] row;
        response->set_not_found(not_found);
        return;
    }
    bool conflict = false, full = false;
    uint64_t holder_ts = 0;
    node_id_t holder_home = -1;
    char* row = Fetch2pcWriteRow(table_id, Rid{page_id, slot_id}, context,
                                 request->transaction_id(), request->start_ts(),
                                 request->requester_node(), &holder_ts, &holder_home, &conflict, nullptr, &full);
    if (row == nullptr) {
        response->set_abort(true);
        response->set_undo_full(full);
        response->set_holder_ts(holder_ts);
        response->set_txn_home_node((int32_t)holder_home);
        return;
    }
    response->set_data(row, get_file_hdr_cached(table_id)->record_size_);
    delete[] row;
}

void ComputeServer::HandleMvccCommit(const twopc_service::CommitRequest* request,
                                     twopc_service::CommitResponse* response, bool prepared) {
    const auto context = mvcc_2pc::DecodeContext(request->mvcc());
    CHECK(IsMvccWorkload());
    const auto participant = node_->getNodeID();
    if (request->transaction_id() != context.id.local ||
        request->start_ts() != ((context.id.local << 8) | context.id.home)) {
        response->set_ok(false);
        return;
    }
    if (prepared) {
        CHECK(request->has_identity() && prepared_2pc::ValidIdentity(request->identity()));
        CHECK(request->item_id_size() == 0 && request->data_size() == 0);
        std::shared_ptr<prepared_2pc::Context> entry = prepared_txns_.TakeForCommit(*request);
        if (!entry) { response->set_ok(false); return; }
        CHECK(entry->request.has_mvcc() &&
              mvcc_2pc::SameContext(entry->request.mvcc(), request->mvcc()));
        {
            std::lock_guard<bthread::Mutex> guard(entry->mutex);
            if (entry->state == prepared_2pc::Context::APPLIED) {
                response->set_ok(true);
                response->set_durable_lsn(entry->applied_lsn);
                response->set_installed(true);
                return;
            }
        }
        LLSN last_install = 0;
        for (const auto& write : entry->request.writes()) {
            const auto ref = undo::DecodeRef(write.undo());
            CHECK(!ref.empty()) << "prepared write lacks its pre-image undo reference";
            last_install = Install2pcMvccWrite(write, context, request->start_ts(),
                                request->identity().coordinator_node(), participant,
                                write.value(), write.deleted(), ref);
        }
        RunMvccGate(context.id, participant, "commit");
        CHECK(last_install > entry->prepare_lsn);
        wait_log_flush(last_install, benchmark_stats::CommitLog);
        prepared_txns_.MarkApplied(entry, last_install);
        response->set_ok(true);
        response->set_durable_lsn(last_install);
        response->set_installed(true);
        return;
    }
    if (request->has_identity()) {
        response->set_ok(false);
        return;
    }
    CHECK(request->deleted_size() == 0 || request->deleted_size() == request->item_id_size());
    CHECK(request->item_id_size() == request->data_size());
    bool inserted = false;
    auto single = single_writes_.Begin(*request, &inserted);
    if (!single) { response->set_ok(false); return; }
    if (!inserted) {
        uint64_t durable = 0;
        if (!single_writes_.AppliedPrefix(single, &durable)) durable = single_writes_.WaitApplied(single);
        response->set_ok(durable != 0);
        response->set_durable_lsn(durable);
        response->set_installed(durable != 0);
        return;
    }
    if (!mvcc_releases_.ExpectWriter(request->mvcc(), false)) {
        single_writes_.Reject(single);
        response->set_ok(false);
        return;
    }
    LLSN last_install = 0;
    for (int i = 0; i < request->item_id_size(); ++i) {
        const auto& id = request->item_id(i);
        const bool deleted = request->deleted_size() != 0 && request->deleted(i);
        const Rid rid{id.page_no(), id.slot_id()};
        bool conflict = false;
        uint64_t holder_ts = 0;
        node_id_t holder_home = -1;
        itemkey_t key = 0;
        char* row_bytes = Fetch2pcWriteRow(id.table_id(), rid, context, request->transaction_id(),
            request->start_ts(), context.id.home, &holder_ts, &holder_home, &conflict, &key);
        CHECK(row_bytes != nullptr && !conflict)
            << "single-writer install lost the participant row lock";
        auto* serialized = reinterpret_cast<DataItem*>(row_bytes);
        serialized->value = reinterpret_cast<uint8_t*>(row_bytes) + sizeof(DataItem);
        const auto row = mvcc::RowImageFromItem(*serialized,
            twopc_service::Identify(id.table_id(), rid, key));
        delete[] row_bytes;
        const auto snapshot = Mvcc().Participant(context);
        CHECK(Mvcc().WriteAllowed(snapshot, row))
            << "single-writer install found a newer committed version after the home SI check";
        CHECK(!row.version.deleted) << "single-writer install over an existing tombstone";
        twopc_service::PreparedWrite write;
        *write.mutable_item_id() = id;
        write.set_table_name(getTableNameByTableID(id.table_id()));
        write.set_key(key);
        write.set_operation(twopc_service::PreparedWrite::UPDATE);
        write.set_value(request->data(i));
        write.set_deleted(deleted);
        undo::Ref ref = Mvcc().Undo().Append(context.id, row);
        CHECK(!ref.empty()) << "single-writer install lost a reserved undo page";
        last_install = Install2pcMvccWrite(write, context, request->start_ts(), context.id.home, participant,
                            request->data(i), deleted, ref);
    }
    RunMvccGate(context.id, participant, "install");
    CHECK(last_install > 0);
    wait_log_flush(last_install, benchmark_stats::CommitLog);
    single_writes_.Applied(single, last_install);
    response->set_ok(true);
    response->set_durable_lsn(last_install);
    response->set_installed(true);
}

void ComputeServer::HandleMvccRelease(const twopc_service::ReleaseRequest* request,
                                      twopc_service::ReleaseResponse* response) {
    const auto context = mvcc_2pc::DecodeContext(request->mvcc());
    const auto participant = node_->getNodeID();
    const bool finish_only = request->finish_only();
    if (request->transaction_id() != context.id.local ||
        request->start_ts() != ((context.id.local << 8) | context.id.home) ||
        request->has_identity() != request->prepared() ||
        (finish_only && (request->prepared() || request->commit_ts() != 0))) {
        response->set_ok(false);
        return;
    }
    if (!finish_only) {
        const bool applied = request->prepared() ? prepared_txns_.CanRelease(*request)
                                                 : single_writes_.CanRelease(*request);
        const auto status = Mvcc().LookupStatus(context.id);
        if (!applied || request->commit_ts() <= context.snapshot || !status ||
            status->state != mvcc::State::Committed || status->commit_ts != request->commit_ts()) {
            response->set_ok(false);
            return;
        }
    }
    bool owner;
    auto release = mvcc_releases_.Begin(*request, owner);
    if (!release) { response->set_ok(false); return; }
    if (!owner) { response->set_ok(true); return; }

    for (const auto& id : request->item_id()) {
        const table_id_t table_id = id.table_id();
        const Rid rid{id.page_no(), id.slot_id()};
        const auto hdr = get_file_hdr_cached(table_id);
        PendingHandoff handoff;
        lock_wait_mgr()->PrepareHandoff(TupleWaitKey{table_id, rid.page_no_, rid.slot_no_},
            request->start_ts(), participant, nodes_channel, nullptr, nullptr, &handoff);
        Page* page = twopc_service::FetchPage(*this, table_id, rid.page_no_, true);
        auto* item = twopc_service::ItemAt(page->get_data(), *hdr, rid.slot_no_);
        const bool still_ours = item->lock == EXCLUSIVE_LOCKED &&
            item->timeStamp == request->start_ts() &&
            item->txn_home_node == context.id.home &&
            item->lock_wait_record_node == participant;
        CHECK(still_ours) << "explicit Release lost its tuple before handoff";
        bool handed = UnlockTupleWithHandoff2PC(table_id, rid, item, request->start_ts(), &handoff);
        if (!handed) item->lock = UNLOCKED;
        auto key_bytes = twopc_service::KeyAt(page->get_data(), *hdr, rid.slot_no_);
        item->value = reinterpret_cast<uint8_t*>(item) + sizeof(DataItem);
        page->set_dirty(true);
        const auto lsn = AddUpdateLog(request->transaction_id(), item, &key_bytes, rid,
                     reinterpret_cast<char*>(item) + sizeof(DataItem),
                     reinterpret_cast<RmPageHdr*>(page->get_data()),
                     finish_only ? 0 : request->commit_ts());
        twopc_service::RecordPageLsn(*this, table_id, rid.page_no_, lsn);
        twopc_service::ReleasePage(*this, table_id, rid.page_no_, true);
        if (handed) DeliverHandoff2PC(handoff);
    }
    if (participant != context.id.home && !request->item_id().empty())
        lock_wait_mgr()->FinishTx(request->start_ts());
    Finish2pcParticipant(context);
    mvcc_releases_.Complete(release);
    response->set_ok(true);
}

void ComputeServer::SetMvccGate(
        std::function<void(const mvcc::TxnId&, mvcc::NodeId, const char*)> gate) {
    std::lock_guard<std::mutex> guard(mvcc_gate_mutex_);
    mvcc_gate_ = std::move(gate);
    mvcc_gate_enabled_.store(bool(mvcc_gate_), std::memory_order_release);
}

void ComputeServer::RunMvccGate(const mvcc::TxnId& id, mvcc::NodeId participant, const char* phase) {
    if (!mvcc_gate_enabled_.load(std::memory_order_acquire)) return;
    std::function<void(const mvcc::TxnId&, mvcc::NodeId, const char*)> gate;
    {
        std::lock_guard<std::mutex> guard(mvcc_gate_mutex_);
        gate = mvcc_gate_;
    }
    if (gate) gate(id, participant, phase);
}

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
