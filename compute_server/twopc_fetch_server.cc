#include <openssl/rand.h>
#include "dtx/mvcc_2pc.h"
#include "storage/prepared_log_record.h"
// Author: Chunyue Huang
// Copyright (c) 2024

#include "server.h"
#include "workload/ycsb/ycsb_db.h"

void ComputeServer::Get_2pc_Local_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data , itemkey_t item_key , tx_id_t tx_id, const mvcc::Context* context, tx_id_t start_ts, uint64_t* holder_ts, node_id_t* txn_home_node, bool* not_found, bool* mvcc_conflict){
    assert(SYSTEM_MODE == 2 || SYSTEM_MODE == 4);
    assert(node_->get_node_id() == node_id);
    if (context != nullptr) {
        CHECK(IsMvccWorkload());
        if (!lock) {
            bool missing = false;
            data = Fetch2pcReadRow(table_id, rid, *context, &missing);
            if (not_found != nullptr) *not_found = missing;
        } else {
            bool conflict = false;
            data = Fetch2pcWriteRow(table_id, rid, *context, tx_id, start_ts,
                                    node_->getNodeID(), holder_ts, txn_home_node, &conflict);
            if (mvcc_conflict != nullptr) *mvcc_conflict = conflict;
        }
        return;
    }
    bool lock_success = true;
    // Get local page data
    page_id_t page_id = rid.page_no_;
    int slot_id = rid.slot_no_;
    // lock 代表是否是写锁
    if(!lock){
        Page* page = local_fetch_s_page(table_id, page_id);
        char* page_data = page->get_data();
        char* bitmap = page_data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        RmFileHdr::ptr file_hdr = get_file_hdr_cached(table_id);
        char* slots = bitmap + file_hdr->bitmap_size_;
        char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));

        // DataItem *debug_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
        itemkey_t key = *reinterpret_cast<itemkey_t*>(tuple);
        assert(key == item_key);
        // No need to lock, just return the data
        data = new char[file_hdr->record_size_];
        memcpy(data, tuple + sizeof(itemkey_t), file_hdr->record_size_);
        local_release_s_page(table_id, page_id);
    }else{
        Page* page = local_fetch_x_page(table_id, page_id);
        char* page_data = page->get_data();
        char *bitmap = page_data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        RmFileHdr::ptr file_hdr = get_file_hdr_cached(table_id);
        char *slots = bitmap + file_hdr->bitmap_size_;
        char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));
        DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
        itemkey_t key = *reinterpret_cast<itemkey_t*>(tuple);
        assert(key == item_key);
        // lock the data
        if(item->lock == UNLOCKED){
            item->lock = EXCLUSIVE_LOCKED;
            // 在元组内记录加锁事务的时间戳，后续访问可据此识别“本事务自持写锁”
            item->timeStamp = start_ts;
            item->txn_home_node = (uint8_t)node_->getNodeID();
            item->lock_wait_record_node = (uint8_t)node_->getNodeID();   // 锁由本节点管理/解锁
            page->set_dirty(true);
            // 在这里刷一个 LockLog
            item->value = (uint8_t*)reinterpret_cast<char*>(item) + sizeof(DataItem);
            LLSN page_new_lsn = AddLockLog(tx_id, table_id, rid, EXCLUSIVE_LOCKED, (RmPageHdr*)(page_data), start_ts, node_->getNodeID(), node_->getNodeID());
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->set_newest_lsn(page_new_lsn);

            data = new char[file_hdr->record_size_];
            memcpy(data, tuple + sizeof(itemkey_t), file_hdr->record_size_);
        } else if (item->lock == EXCLUSIVE_LOCKED && item->timeStamp == start_ts &&
                   item->txn_home_node == node_->getNodeID()) {
            // 写锁是本事务自己加的，允许继续
            item->value = (uint8_t*)reinterpret_cast<char*>(item) + sizeof(DataItem);
            data = new char[file_hdr->record_size_];
            memcpy(data, tuple + sizeof(itemkey_t), file_hdr->record_size_);
        } else {
            // 写锁是其他事务加的，abort，data 置为 nullptr，同时记录下目前的持有者是谁
            if (holder_ts != nullptr) *holder_ts = item->timeStamp;
            if (txn_home_node != nullptr) *txn_home_node = (node_id_t)item->txn_home_node;
            data = nullptr;
        }
        local_release_x_page(table_id, page_id);
    }
}

void ComputeServer::Get_2pc_Remote_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data , tx_id_t tx_id, const mvcc::Context* context, tx_id_t start_ts, uint64_t* holder_ts, node_id_t* txn_home_node, bool* not_found, bool* mvcc_conflict, bool* undo_full){
    assert(SYSTEM_MODE == 2 || SYSTEM_MODE == 4);
    const auto cost_start = CostModelActive() ? page_cost::PageCostModel::NowNs() : 0;
    auto start_time = std::chrono::high_resolution_clock::now();
    twopc_service::GetDataItemRequest request;
    twopc_service::GetDataItemResponse response;
    twopc_service::ItemID* item_id = new twopc_service::ItemID();
    item_id->set_table_id(table_id);
    item_id->set_page_no(rid.page_no_);
    item_id->set_slot_id(rid.slot_no_);
    item_id->set_lock_data(lock);
    request.set_transaction_id(tx_id);
    request.set_start_ts(start_ts);
    request.set_requester_node((int32_t)node_->getNodeID());
    if (context != nullptr) mvcc_2pc::EncodeContext(*context, request.mutable_mvcc());
    request.set_allocated_item_id(item_id);
    twopc_service::TwoPCService_Stub stub(&nodes_channel[node_id]);
    brpc::Controller cntl;
    stub.GetDataItem(&cntl, &request, &response, NULL);
    CHECK(!cntl.Failed()) << "GetDataItem failed: " << cntl.ErrorText();
    if(response.abort()){
        assert(lock == true);
        if (holder_ts != nullptr) *holder_ts = response.holder_ts();
        if (txn_home_node != nullptr) *txn_home_node = (node_id_t)response.txn_home_node();
        if (undo_full) *undo_full = response.undo_full();
        if (mvcc_conflict != nullptr && response.mvcc_conflict()) *mvcc_conflict = true;
        data = nullptr;
    } else if (context != nullptr) {
        const auto hdr = get_file_hdr_cached(table_id);
        CHECK(response.data().size() == size_t(hdr->record_size_))
            << "participant returned a non-record MVCC row";
        data = new char[hdr->record_size_];
        memcpy(data, response.data().data(), hdr->record_size_);
        if (not_found != nullptr) *not_found = response.not_found();
    } else {
        char* page = (char*)response.data().c_str();
        char *bitmap = page + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        RmFileHdr::ptr file_hdr = get_file_hdr_cached(table_id);
        char *slots = bitmap + file_hdr->bitmap_size_;
        char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
        data = new char[file_hdr->record_size_];
        memcpy(data, tuple + sizeof(itemkey_t), file_hdr->record_size_);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    const auto fetch_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    twopc_remote_fetch_time_ns += fetch_ns;
    benchmark_stats::remote_fetch[response.abort() ? 1 : 0].Add(fetch_ns);
    twopc_remote_fetch_count++;
    node_->page_cost_stats_.RecordRemote(table_id, rid.page_no_, lock, response.abort(), fetch_ns);
    if (cost_start && !response.abort()) {
        page_cost_model_.ObserveRemote(lock, (page_cost::PageCostModel::NowNs() - cost_start) / 1e6);
    }
    return;
}

void ComputeServer::AbortRPCDone(twopc_service::AbortResponse* response, brpc::Controller* cntl){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<twopc_service::AbortResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::CommitRPCDone(twopc_service::CommitResponse* response, brpc::Controller* cntl,
                                  int* add_latency, ComputeServer* server, int peer){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<twopc_service::CommitResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        // *add_latency += response->latency_commit();
        if (server && server->OnlineCostActive() && response->ok())
            page_cost::ObserveControlReply(server->page_cost_model_.online, peer, cntl->latency_us(), response->cost());
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

namespace {
template <typename Request, typename Response>
struct PreparedCall {
    Request request;
    Response response;
    brpc::Controller controller;
    brpc::CallId id;
    int peer = -1;
    uint64_t finished_ns = 0;
};
void MvccRPCDone() {}
using PrepareCostCall = PreparedCall<twopc_service::PrepareRequest, twopc_service::PrepareResponse>;
void PrepareRPCDone(page_cost::OnlineCommitCost* model, PrepareCostCall* call) {
    call->finished_ns = benchmark_stats::NowNs();
    if (model && !call->controller.Failed() && call->response.ok())
        page_cost::ObserveControlReply(*model, call->peer, call->controller.latency_us(), call->response.cost());
}
}

const std::string& ComputeServer::ProcessIncarnation() {
    // Random startup identity, shared by all server instances in this process.
    // Uniqueness is probabilistic; no durable epoch/LogOnce service is provided.
    static const std::string incarnation = [] {
        std::string bytes(16, '\0');
        CHECK(RAND_bytes(reinterpret_cast<unsigned char*>(&bytes[0]), bytes.size()) == 1);
        return bytes;
    }();
    return incarnation;
}

bool ComputeServer::PrepareLegacy_2pc(const std::unordered_set<node_id_t>& nodes, uint64_t txn_id) {
    prepared_2pc::Requests requests;
    for (auto node : nodes) requests[node].set_transaction_id(txn_id);
    return Prepare_2pc(requests);
}

bool ComputeServer::Prepare_2pc(const prepared_2pc::Requests& requests,
                                std::map<node_id_t, LLSN>* prepare_lsn,
                                benchmark_stats::PrepareSample* sample){
    CHECK(SYSTEM_MODE == 2 || SYSTEM_MODE == 4);
    CHECK(requests.size() > 1);
    for (const auto& entry : requests) {
        CHECK(entry.first >= 0 && entry.first < ComputeNodeCount);
        if (!prepared_2pc::IsLegacyPrepare(entry.second)) {
            CHECK(IsLogEnabled());
            CHECK(prepared_2pc::ValidRequest(entry.second) && PreparedLogRecord::Fits(entry.second))
                << "Invalid or oversized Prepare payload";
        }
    }
    if (prepared_2pc::StorageNotifies(requests.begin()->second.protocol_version())) {
        // Storage's durable all-writer notification starts Commit. Outstanding
        // Prepare responses have independent ownership and are drained at exit.
        if (prepare_lsn != nullptr) prepare_lsn->clear();
        return prepare_notifications_.Prepare(requests, nodes_channel);
    }
    using Call = PreparedCall<twopc_service::PrepareRequest, twopc_service::PrepareResponse>;
    std::vector<std::unique_ptr<Call>> calls;
    const auto prepare_start = benchmark_stats::NowNs();
    for (const auto& entry : requests) {
        auto call = std::make_unique<Call>();
        call->request = entry.second;
        call->peer = entry.first;
        call->id = call->controller.call_id();
        twopc_service::TwoPCService_Stub stub(&nodes_channel[entry.first]);
        auto* online = OnlineCostActive() &&
                           (prepared_2pc::IsLegacyPrepare(call->request) ||
                            prepared_2pc::ParticipantReplies(call->request.protocol_version()))
                           ? &page_cost_model_.online : nullptr;
        stub.Prepare(&call->controller, &call->request, &call->response,
                     brpc::NewCallback(PrepareRPCDone, online, call.get()));
        calls.push_back(std::move(call));
    }
    bool all_yes = true;
    for (const auto& call : calls) {
        brpc::Join(call->id);
        if (call->controller.Failed() || !call->response.ok()) {
            LOG(ERROR) << "Prepare missing YES: " << call->controller.ErrorText();
            all_yes = false;
        }
    }
    if (all_yes && prepare_lsn != nullptr) {
        for (const auto& call : calls) {
            CHECK(call->response.durable_lsn() > 0) << "participant YES lacks its durable prefix";
            CHECK(prepare_lsn->emplace(call->peer, call->response.durable_lsn()).second);
        }
    }
    if (sample && all_yes) {
        uint64_t first = calls.front()->finished_ns, last = first;
        for (const auto& call : calls) {
            first = std::min(first, call->finished_ns); last = std::max(last, call->finished_ns);
        }
        *sample = {1, first - prepare_start, last - first, last - prepare_start};
    }
    return all_yes;
}

void ComputeServer::CommitPrepared_2pc(const prepared_2pc::Requests& requests,
                                       std::map<node_id_t, LLSN>* durable) {
    CHECK(!requests.empty() && durable != nullptr);
    auto* sample = OnlineCostActive() &&
                       prepared_2pc::ParticipantReplies(requests.begin()->second.protocol_version())
                       ? &page_cost_model_.online : nullptr;
    using Call = PreparedCall<twopc_service::CommitRequest, twopc_service::CommitResponse>;
    std::vector<std::unique_ptr<Call>> calls;
    for (const auto& entry : requests) {
        const auto& prepared = entry.second;
        CHECK(prepared.has_mvcc() && prepared.protocol_version() != 0);
        auto call = std::make_unique<Call>();
        call->peer = entry.first;
        auto& request = call->request;
        request.set_kind(twopc_service::CommitRequest::PREPARED);
        *request.mutable_identity() = prepared.identity();
        request.set_transaction_id(prepared.transaction_id());
        request.set_start_ts(prepared.start_ts());
        request.set_commit_ts(prepared.commit_ts());
        *request.mutable_mvcc() = prepared.mvcc();
        call->id = call->controller.call_id();
        twopc_service::TwoPCService_Stub stub(&nodes_channel[entry.first]);
        stub.Commit(&call->controller, &request, &call->response, brpc::NewCallback(MvccRPCDone));
        calls.push_back(std::move(call));
    }
    for (const auto& call : calls) {
        brpc::Join(call->id);
        CHECK(!call->controller.Failed()) << "Prepared Commit RPC failed: "
                                          << call->controller.ErrorText();
        CHECK(call->response.ok() && call->response.installed() &&
              call->response.durable_lsn() > 0)
            << "Prepared Commit did not durably install its write set";
        CHECK(durable->emplace(call->peer, call->response.durable_lsn()).second);
        if (sample) {
            page_cost::ObserveControlReply(*sample, call->peer, call->controller.latency_us(),
                                           call->response.cost());
        }
    }
    CHECK(durable->size() == requests.size()) << "missing prepared install evidence";
}

void ComputeServer::CommitRemoteSingleMvcc_2pc(node_id_t writer, const twopc_service::CommitRequest& request) {
    CHECK(writer != getNodeID());
    twopc_service::TwoPCService_Stub stub(&nodes_channel[writer]);
    brpc::Controller controller;
    twopc_service::CommitResponse response;
    stub.Commit(&controller, &request, &response, nullptr);
    CHECK(!controller.Failed()) << "single-writer Commit RPC failed: " << controller.ErrorText();
    CHECK(response.ok() && response.installed() && response.durable_lsn() > 0)
        << "remote single-writer Commit did not durably install its rows";
}

void ComputeServer::ReleaseMvcc_2pc(
        const mvcc::Context& context, uint64_t start_ts, uint64_t commit_ts,
        const std::map<node_id_t, std::vector<twopc_service::ItemID>>& locked,
        const std::set<node_id_t>& nodes, const std::set<mvcc::NodeId>& writers,
        bool deferred_unlock, const prepared_2pc::Requests* prepared) {
    CHECK(writers.empty() ? commit_ts == 0 : commit_ts > context.snapshot);
    for (const auto writer : writers) CHECK(nodes.count(writer) && locked.count(writer));
    if (deferred_unlock) {
        // The decision is durable. A woken writer can now resolve the old lock at its home.
        lock_wait_mgr()->FinishDeferredTxn(start_ts, commit_ts, getNodeID(), nodes_channel, true);
    }
    using Call = PreparedCall<twopc_service::ReleaseRequest, twopc_service::ReleaseResponse>;
    std::vector<std::unique_ptr<Call>> calls;
    std::unique_ptr<Call> local;
    for (node_id_t node : nodes) {
        auto call = std::make_unique<Call>();
        auto& request = call->request;
        request.set_transaction_id(context.id.local);
        request.set_start_ts(start_ts);
        mvcc_2pc::EncodeContext(context, request.mutable_mvcc());
        request.set_deferred_unlock(deferred_unlock);
        if (deferred_unlock || writers.count(node)) request.set_commit_ts(commit_ts);
        if (writers.count(node)) {
            if (prepared) {
                const auto& entry = prepared->at(node);
                CHECK(entry.has_identity());
                request.set_prepared(true);
                *request.mutable_identity() = entry.identity();
            }
        } else {
            request.set_finish_only(true);
        }
        auto items = locked.find(node);
        if (items != locked.end())
            for (const auto& item : items->second) *request.add_item_id() = item;
        if (node == context.id.home) {
            CHECK(node == getNodeID());
            local = std::move(call);
            continue;
        }
        call->id = call->controller.call_id();
        twopc_service::TwoPCService_Stub stub(&nodes_channel[node]);
        stub.Release(&call->controller, &request, &call->response, brpc::NewCallback(MvccRPCDone));
        calls.push_back(std::move(call));
    }
    // Remote releases are already in flight while the home releases its own rows.
    if (local) {
        HandleMvccRelease(&local->request, &local->response);
        CHECK(local->response.ok()) << "local MVCC Release failed";
    }
    for (const auto& call : calls) {
        brpc::Join(call->id);
        CHECK(!call->controller.Failed() && call->response.ok())
            << "MVCC participant Release failed: " << call->controller.ErrorText();
    }
}

void ComputeServer::AbortMvcc_2pc(
        const mvcc::Context& context, uint64_t start_ts,
        const std::map<node_id_t, std::vector<std::pair<table_id_t, Rid>>>& items,
        const std::set<node_id_t>& exported) {
    using Call = PreparedCall<twopc_service::AbortRequest, twopc_service::AbortResponse>;
    std::vector<std::unique_ptr<Call>> calls;
    for (node_id_t node : exported) {
        auto it = items.find(node);
        auto call = std::make_unique<Call>();
        call->peer = node;
        call->request.set_transaction_id(context.id.local);
        mvcc_2pc::EncodeContext(context, call->request.mutable_mvcc());
        if (it != items.end()) {
            for (const auto& item : it->second) {
                auto* id = call->request.add_item_id();
                id->set_table_id(item.first);
                id->set_page_no(item.second.page_no_);
                id->set_slot_id(item.second.slot_no_);
            }
        }
        call->id = call->controller.call_id();
        twopc_service::TwoPCService_Stub stub(&nodes_channel[node]);
        stub.Abort(&call->controller, &call->request, &call->response, nullptr);
        calls.push_back(std::move(call));
    }
    for (const auto& call : calls) {
        CHECK(!call->controller.Failed() && call->response.ok())
            << "Abort RPC failed: " << call->controller.ErrorText();
    }
}

void ComputeServer::Abort_2pc(std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map, uint64_t txn_id, bool sync){
    assert(SYSTEM_MODE == 2 || SYSTEM_MODE == 4);
    std::vector<brpc::CallId> cids;
    for(auto node_data: node_data_map){
        node_id_t node_id = node_data.first;
        twopc_service::TwoPCService_Stub stub(&nodes_channel[node_id]);
        twopc_service::AbortRequest request;
        twopc_service::AbortResponse * response = new twopc_service::AbortResponse();
        request.set_transaction_id(txn_id);
        for(auto item: node_data.second){
            twopc_service::ItemID* item_id = request.add_item_id();
            item_id->set_table_id(item.first);
            item_id->set_page_no(item.second.page_no_);
            item_id->set_slot_id(item.second.slot_no_);
        }
        brpc::Controller *cntl = new brpc::Controller();
        cids.push_back(cntl->call_id());
        stub.Abort(cntl, &request, response, brpc::NewCallback(AbortRPCDone, response, cntl));
    }
    if(sync){
        for(auto cid: cids){
            brpc::Join(cid);
        }
    }
    return;
}

int ComputeServer::Commit_2pc(std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map, uint64_t txn_id, uint64_t commit_ts, bool sync){
    assert(SYSTEM_MODE == 2 || SYSTEM_MODE == 4);
    std::vector<brpc::CallId> cids;
    int c = 0;
    for(auto node_data : node_data_map){
        node_id_t node_id = node_data.first;
        twopc_service::TwoPCService_Stub stub(&nodes_channel[node_id]);
        twopc_service::CommitRequest request;
        twopc_service::CommitResponse * response = new twopc_service::CommitResponse();
        request.set_transaction_id(txn_id);
        request.set_commit_ts(commit_ts);
        for(auto item: node_data.second){
            twopc_service::ItemID* item_id = request.add_item_id();
            item_id->set_table_id(item.first.first);
            item_id->set_page_no(item.first.second.page_no_);
            item_id->set_slot_id(item.first.second.slot_no_);
            RmFileHdr::ptr file_hdr = get_file_hdr_cached(item.first.first);
            request.add_data(item.second, file_hdr->record_size_ - sizeof(DataItem));
        }
        brpc::Controller *cntl = new brpc::Controller();
        cids.push_back(cntl->call_id());
        stub.Commit(cntl, &request, response,
                    brpc::NewCallback(CommitRPCDone, response, cntl, &c, sync ? this : nullptr, node_id));
    }
    if(sync){
        for(auto cid: cids){
            brpc::Join(cid);
        }
    }
    if(c == 0)
        return 0;
    return c/node_data_map.size();
}
