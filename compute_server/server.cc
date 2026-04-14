#include "server.h"
#include "config.h"

std::atomic<int64_t> global_wait_log_flush_time_ns{0};
std::atomic<int64_t> global_log_flush_count{0};
std::atomic<int64_t> global_log_flush_total_time_ns{0};
std::atomic<int64_t> global_log_flush_to_lock_done_time_ns{0};
std::atomic<int64_t> global_log_flush_to_max_lsn_time_ns{0};
std::atomic<int64_t> global_log_flush_to_serialize_done_time_ns{0};
std::atomic<int64_t> global_log_flush_storage_rpc_time_ns{0};
std::atomic<int64_t> global_log_flush_update_persist_lsn_time_ns{0};
std::atomic<int64_t> global_log_flush_total_batch_size{0};
std::atomic<int64_t> global_log_flush_max_batch_size{0};
std::atomic<int64_t> global_wait_log_flush_count{0};
std::atomic<int64_t> global_wait_log_flush_push_page_time_ns{0};
std::atomic<int64_t> global_wait_log_flush_evict_page_time_ns{0};

std::atomic<int64_t> twopc_remote_fetch_time_ns{0};
std::atomic<int64_t> twopc_remote_fetch_count{0};

std::atomic<int64_t> global_wait_commit_log_time_ns{0};
std::atomic<int64_t> global_wait_prepare_log_time_ns{0};
std::atomic<int64_t> global_wait_backup_log_time_ns{0};
std::atomic<int64_t> global_update_log_count{0};
std::atomic<int64_t> global_fetch_storage_page_time_ns{0};
std::atomic<int64_t> ownership_transfer_count{0};
std::atomic<int64_t> ownership_transfer_time_total{0};
std::atomic<int64_t> lazy_getpage_dire{0};
// 这里 lazy_getpage_dire + lazy_getpage_wait 不应该等于 ownershiptranscount
// 因为存在这种情况：节点 1 持有 x 锁，节点 0,2,3,4,5,6 同时申请s 锁，这个时候，0，2,3,4,5,6 只有第一个申请的能统计，其他的没法统计
std::atomic<int64_t> lazy_getpage_wait{0};
std::atomic<int64_t> lazy_2RTT_count{0};
std::atomic<int64_t> lazy_3RTT_count{0};

int TryOperationCnt = 10000;  // only for micro experiment

#include <unistd.h>
#include <vector>
#include <utility>
#include "workload/ycsb/ycsb_db.h"

#include "sql_executor/parser/parser_defs.h"

int socket_start_client(std::string ip, int port){
    // 创建套接字
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    // 设置服务器地址和端口
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);

    if(inet_pton(AF_INET, ip.c_str(), &(serverAddress.sin_addr)) <= 0){
        std::cerr << "Invalid address" << std::endl;
        return -1;
    }

    if(clientSocket < 0){
        std::cerr << "Socket creation failed" << std::endl;
        close(clientSocket);
        return -1;
    }

    // 连接到服务器
    if(connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0){
        std::cerr << "Connection failed" << std::endl;
        close(clientSocket);
        return -1;
    }
    // 发送 节点数目 到服务器
    send(clientSocket, &ComputeNodeCount, sizeof(ComputeNodeCount), 0);

    // 接收服务器发送的 SYN 消息
    char buffer[10];
    recv(clientSocket, buffer, 9, 0);
    buffer[9] = '\0';
    assert(strcmp(buffer, "SYN-BEGIN") == 0);

    // std::cout << "Remote server has build brpc channel with compute nodes" << std::endl;

    // 关闭套接字
    close(clientSocket);

    return 0;
}

int socket_finish_client(std::string ip, int port){
    // 创建套接字
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    // 设置服务器地址和端口
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);

    if(inet_pton(AF_INET, ip.c_str(), &(serverAddress.sin_addr)) <= 0){
        std::cerr << "Invalid address" << std::endl;
        return -1;
    }

    if(clientSocket < 0){
        std::cerr << "Socket creation failed" << std::endl;
        close(clientSocket);
        return -1;
    }

    // 连接到服务器
    if(connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0){
        std::cerr << "Connection failed" << std::endl;
        close(clientSocket);
        return -1;
    }
    // 发送 节点数目 到服务器
    send(clientSocket, &ComputeNodeCount, sizeof(ComputeNodeCount), 0);

    // 接收服务器发送的 SYN 消息
    char buffer[11];
    recv(clientSocket, buffer, 10, 0);
    buffer[10] = '\0';
    assert(strcmp(buffer, "SYN-FINISH") == 0);

    // std::cout << "Remote server has build brpc channel with compute nodes" << std::endl;


    // 关闭套接字
    close(clientSocket);

    return 0;
}

namespace compute_node_service{
// Lock Fusion 调用的，要求本节点推送数据               
void ComputeNodeServiceImpl::NotifyPushPage(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NotifyPushPageRequest* request,
                       ::compute_node_service::NotifyPushPageResponse* response,
                       ::google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);

    page_id_t page_id = request->page_id().page_no();
    table_id_t table_id = request->page_id().table_id();
    node_id_t src_node_id = request->src_node_id();
    assert(src_node_id == this->server->get_node()->getNodeID());

    if (table_id < 10000){
        // LOG(INFO) << "Remote NotifyPushPage , table_id = " << table_id << " page_id = " << page_id;
    }
    
    // 这里是一个边界条件：我目前持有所有权，但是还在存储里面拿，此时另外一个 S 锁进来了，通知我把页面推送给它
    // 因此在这里等待，节点把页面从存储拿上来以后，再推送给目标节点
    {
        server->get_node()->getBufferPoolByIndex(table_id)->wait_in_bufferPool(page_id);
    }
    // Page* page = server->get_node()->getBufferPoolByIndex(table_id)->fetch_page(page_id);
    int dest_node_id_size = request->dest_node_ids_size();
    assert(dest_node_id_size != 0);

    // 这里其实可以优化一下，把推页面的任务交给后台线程
    for (int i = 0 ; i < dest_node_id_size ; i++){
        node_id_t dest_node = request->dest_node_ids(i);
        assert(dest_node != src_node_id);

        if (table_id < 10000){
            // LOG(INFO) << "Remote NotifyPushPage , table_id = " << table_id << " page_id = " << page_id << " Pushing Page To Node : " << dest_node;
        }
        server->PushPageToOther(table_id, page_id, dest_node , true , false , 0);
    }

    if (NetworkLatency != 0)  usleep(NetworkLatency); 
}


void ComputeNodeServiceImpl::Pending(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::PendingRequest* request,
                       ::compute_node_service::PendingResponse* response,
                       ::google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);

    page_id_t page_id = request->page_id().page_no();
    table_id_t table_id = request->page_id().table_id();
    bool xpending = (request->pending_type() == PendingType::XPending);
    int dest_node_id = request->dest_node_id();

    assert(dest_node_id != server->get_node()->getNodeID());
    if (table_id < 10000){
        // LOG(INFO) << "Receive Pending , table_id = " << table_id << " page_id = " << page_id << " dest_node_id = " << dest_node_id ;
    }

    int unlock_remote = server->get_node()->PendingPage(page_id, xpending, table_id);

    assert(server->get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->getDestNodeIDNoBlock() == INVALID_NODE_ID);

    if(unlock_remote > 0){
        // unlock_remote == 3 是一种很特殊的情况，表示本节点已经释放掉页面了，但是还没同步到 GPLM，因此 GPLM 还以为我还在用
        // 只有两个主节点的时候，不会出现 unlock_remote = 3 的情况，这里先 assert 一下 debug
        assert(unlock_remote != 3);
        if (table_id < 10000){
            // LOG(INFO) << "Remote Pending Release , table_id = " << table_id << " page_id = " << page_id << " dest_node_id = " << dest_node_id;
            if (dest_node_id != INVALID_PAGE_ID){
                lazy_getpage_dire++;
            }
        }

        // 如果锁已经用完了，那就先向下一轮获得锁的某个节点发送一次 Push 数据
        if (dest_node_id != -1){
            server->PushPageToOther(table_id , page_id , dest_node_id , true , false , 0);
        }

        // 在这里就得把页面给淘汰了，不然就有下面这个问题：
        /*
            捋一遍流程：
            1. 我现在远程持有 S 锁，我希望升级为 X 锁，于是向远程申请
            2. 在我的申请到达之前，一个节点发了 X 请求，远程让我Pending，并把我升级的那个请求放到请求队列里
            3. Pending到我这的时候，发现我在升级，于是直接把我本地的锁给放了(不放会死锁)，然后执行 LRPAnyUnlock
            4. LRPAnyUnlock 把锁给了另外一个节点，由于请求队列里还有我，所以同时会给另外一个节点发Pending，同时告诉它需要向我Push数据
            5. 另外一个节点跑完后，把数据页推给了我，关键点来了，此时我第3步的Pending还没跑完，最后我把这个数据页给扔了，就导致这里找不到数据页
        */
        if (table_id < 10000){
            // LOG(INFO) << "Remote Pending Release Page , table_id = " << table_id << " page_id = " << page_id;
        }
        server->get_node()->getBufferPoolByIndex(table_id)->releaseBufferPage(table_id , page_id);

        int dest_node_id = server->get_node_id_by_page_id(table_id , page_id);
        assert(dest_node_id != server->get_node()->get_node_id());

        brpc::Channel* page_table_channel =  server->get_compute_channel() + server->get_node_id_by_page_id(table_id , page_id);
        page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
        page_table_service::PAnyUnLockRequest unlock_request;
        page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        unlock_request.set_allocated_page_id(page_id_pb);
        unlock_request.set_node_id(server->get_node()->getNodeID());

        brpc::Controller cntl;
        /*
            这里是同步的，等待 LRPAnyUnlock 执行完
            此时持有 LocalPageLock 的锁，等待
        */
        pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        server->get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->UnlockRemoteOK();
        delete unlock_response;
    }else {
        // 之前有个想法，如果是读锁，可以直接把页面给推出去，不需要等到 release 的时候，但是发现不行，原因是就算有所有权，页面也不一定在内存里，可能正在淘汰页面，或者正在从存储拿
        // 所以这里只能先标记一下需要向谁推送页面，然后等到 lazy_release 的时候，再把页面给推出去
        if (table_id < 10000){
            // LOG(INFO) << "Remote Pending Wait Lock Release , table_id = " << table_id << " page_id = " << page_id;
        }
        if (dest_node_id != INVALID_NODE_ID){
            // 保存下来，等到 lazy_release 的时候再 Push
            server->get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->setDestNodeIDNoBlock(dest_node_id);
            if (table_id < 10000){
                lazy_getpage_wait++;
            }
        }
        server->get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->UnlockMtx();
    }

    // 添加模拟延迟
    if (NetworkLatency != 0)  usleep(NetworkLatency); 
    return;
}

void ComputeNodeServiceImpl::NotifyCreateTable(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NotifyCreateTableRequest* request,
                       ::compute_node_service::NotifyCreateTableResponse* response,
                       ::google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);
    std::string tab_name = request->tab_name();
    if (server->table_exist(tab_name)){
        return ;
    }
}

void ComputeNodeServiceImpl::NotifyDropTable(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NotifyDropTableRequest* request,
                       ::compute_node_service::NotifyDropTableResponse* response,
                       ::google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);
    std::string tab_name = request->tab_name();
    if (server->tryDropTable(tab_name)){
        response->set_ok(true);
    }else{
        response->set_ok(false);
    }

    return ;
}

void ComputeNodeServiceImpl::quitDropTable(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::quitDropTableRequest* request,
                       ::compute_node_service::quitDropTableResponse* response,
                       ::google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);
    std::string tab_name = request->tab_name();
    server->NotifyDropTableOver();
}

void ComputeNodeServiceImpl::ClearTable(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::ClearTableRequest* request,
                       ::compute_node_service::ClearTableResponse* response,
                       ::google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);
    table_id_t table_id = request->table_id();
    server->clearTable(table_id);
    server->NotifyDropTableOver();
}

void ComputeNodeServiceImpl::GetPage(::google::protobuf::RpcController* controller,
                    const ::compute_node_service::GetPageRequest* request,
                    ::compute_node_service::GetPageResponse* response,
                    ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();

        table_id_t table_id = request->page_id().table_id();
        std::string data = server->get_node()->try_fetch_page_ret_string(table_id , page_id);
        if (data.size() == 0){
            response->set_need_to_storage(true);
            return;
        }

        response->set_need_to_storage(false);
        response->set_page_data(data.c_str() , PAGE_SIZE);

        if (NetworkLatency != 0)  usleep(NetworkLatency); 
        return;
    }

void ComputeNodeServiceImpl::PushPage(::google::protobuf::RpcController* controller,
                    const ::compute_node_service::PushPageRequest* request,
                    ::compute_node_service::PushPageResponse* response,
                ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();

        node_id_t src_node_id = request->src_node_id();
        node_id_t dest_node_id = request->dest_node_id();

        assert(src_node_id != dest_node_id);
        assert(server->get_node()->getNodeID() == dest_node_id);

        server->put_page_into_buffer(table_id , page_id , request->page_data().c_str() , 1);

        if (table_id < 10000){
            // LOG(INFO) << "Receive Page Over , table_id = " << table_id << " page_id = " << page_id;
        }

        server->get_node()->NotifyPushPageSuccess(table_id, page_id);
        
        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

void ComputeNodeServiceImpl::LockSuccess(::google::protobuf::RpcController* controller,
                    const ::compute_node_service::LockSuccessRequest* request,
                    ::compute_node_service::LockSuccessResponse* response,
                    ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        bool xlock = request->xlock_succeess();

        // hcy TODO: 更改为 is_newest
        bool is_newest = request->is_newest();

        server->get_node()->NotifyLockPageSuccess(table_id, page_id, xlock, is_newest);

        for (int i = 0 ; i < request->dest_node_ids_size() ; i++){
            node_id_t dest_node = request->dest_node_ids(i);
            assert(dest_node != server->get_node()->getNodeID());
            server->get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->addToPushListNoBlock(dest_node);
        }

        server->get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->UnlockMtx();

        if (NetworkLatency != 0){
            usleep(NetworkLatency);
        }
        return;
    }

void ComputeNodeServiceImpl::TransferDTX(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::TransferDTXRequest* request,
                       ::compute_node_service::TransferDTXResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        return;
    }

void ComputeNodeServiceImpl::TransferHotLocate(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::TransferHotLocateRequest* request,
                       ::compute_node_service::TransferHotLocateResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        node_id_t dest_node_id = request->dest_node_id();
        assert(dest_node_id == server->get_node()->getNodeID());
        for (int i = 0 ; i < request->entries_size() ; i++){
            auto &entry = request->entries(i);
            table_id_t table_id = entry.page_id().table_id();
            page_id_t page_id = entry.page_id().page_no();
            node_id_t newest = entry.newest_node_id();
            server->get_node()->getLocalPageLockTables(table_id)->GetLock(page_id)->SetNewestNode(newest);
        }
        server->get_node()->notifyRemoteOK();
        return;
    }
}

int ComputeServer::Pending(page_id_t page_id, bool xpending, table_id_t table_id , node_id_t dest_node_id){
    assert(dest_node_id != get_node()->getNodeID());

    int unlock_remote = get_node()->PendingPage(page_id, xpending, table_id);

    if (table_id < 10000){
        // LOG(INFO) << "Local Pending , table_id = " << table_id << " page_id = " << page_id << " dest_node_id = " << dest_node_id;
    }

    assert(get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->getDestNodeIDNoBlock() == INVALID_NODE_ID);

    if(unlock_remote > 0){
        // unlock_remote == 3 是一种很特殊的情况，表示本节点已经释放掉页面了，但是还没同步到 GPLM，因此 GPLM 还以为我还在用
        // 只有两个主节点的时候，不会出现 unlock_remote = 3 的情况，这里先 assert 一下 debug
        assert(unlock_remote != 3);
        if(unlock_remote != 3){

            // 如果锁已经用完了，那就先向下一轮获得锁的某个节点发送一次 Push 数据
            if (dest_node_id != -1){
                if (table_id < 10000){
                    // LOG(INFO) << "Local Pending , Send Page To node : " << dest_node_id << " table_id = " << table_id << " page_id = " << page_id;
                    PushPageToOther(table_id , page_id , dest_node_id , true , false , 1);
                }else {
                    PushPageToOther(table_id , page_id , dest_node_id , true , false , 0);
                }
            }

            // 在这里就得把页面给淘汰了，不然就有下面这个问题：
            /*
                捋一遍流程：
                1. 我现在正在远程持有 S 锁，我希望升级为 X 锁，于是向远程申请
                2. 在我的申请到达之前，一个节点发了 X 请求，远程让我Pending，并把我升级的那个请求放到请求队列里
                3. Pending到我这的时候，发现我在升级，于是直接把我本地的锁给放了(不放会死锁)，然后执行 LRPAnyUnlock
                4. LRPAnyUnlock 把锁给了另外一个节点，由于请求队列里还有我，所以同时会给另外一个节点发Pending，同时告诉它需要向我Push数据
                5. 另外一个节点跑完后，把数据页推给了我，关键点来了，此时我第3步的Pending还没跑完，最后我把这个数据页给扔了，就导致这里找不到数据页
            */
            if (table_id < 10000){
                // LOG(INFO) << "Local Receive Pending , Immediate Release Buffer , table_id = " << table_id << " page_id = " << page_id;
            }
            get_node()->getBufferPoolByIndex(table_id)->releaseBufferPage(table_id , page_id);

            node_id_t node_id = get_node_id_by_page_id(table_id , page_id);
            assert(node_id == get_node()->get_node_id());

            // brpc::Controller cntl;
            // brpc::Channel* page_table_channel =  get_compute_channel() + get_node_id_by_page_id(table_id , page_id);
            // page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
            page_table_service::PAnyUnLockRequest unlock_request;
            page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
            page_table_service::PageID* page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            unlock_request.set_allocated_page_id(page_id_pb);
            unlock_request.set_node_id(get_node()->getNodeID());

            page_table_service_impl_->LRPAnyUnLock_Localcall(&unlock_request , unlock_response);
            get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->UnlockRemoteOK();
            // delete response;
            delete unlock_response;
        }
    }else {
        // 之前有个想法，如果是读锁，可以直接把页面给推出去，不需要等到 release 的时候，但是发现不行，原因是就算有所有权，页面也不一定在内存里，可能正在淘汰页面，或者正在从存储拿
        // 所以这里只能先标记一下需要向谁推送页面，然后等到 lazy_release 的时候，再把页面给推出去
        if (table_id < 10000){
            // LOG(INFO) << "Local Pending , Wait Lock Release , table_id = " << table_id << " page_id = " << page_id;
        }
        if (dest_node_id != INVALID_NODE_ID){
            // 保存下来，等到 lazy_release 的时候再 Push
            get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->setDestNodeIDNoBlock(dest_node_id);
        }
        get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->UnlockMtx();
    }

    // 添加模拟延迟
    if (NetworkLatency != 0)  usleep(NetworkLatency); 
    return 0;
}

void ComputeServer::PushPageToOther(table_id_t table_id , page_id_t page_id , node_id_t dest_node_id , bool need_to_wait_log , bool from_global , int push_type){
    if (from_global){
        /*
            假如节点 1拿到了页面所有权(Read)，但是需要去存储拿，也就是此时这个页面不在缓冲区里
            假设此时，节点 2也拿到了页面所有权，GPLM 通知其去节点 1 拿，但是节点 1 还在从存储拿呢，所以需要等待一下
        */
        get_node()->getBufferPoolByIndex(table_id)->wait_in_bufferPool(page_id);
    }
    Page *page = node_->getBufferPoolByIndex(table_id)->fetch_page(page_id);
    node_id_t src_node_id = node_->getNodeID();
    assert(dest_node_id != -1);
    assert(dest_node_id != src_node_id);

    if (table_id >= 10000 || !node_->push_page_with_scheduler || push_type == 0){
        compute_node_service::PushPageRequest push_request;
        compute_node_service::PushPageResponse* push_response = new compute_node_service::PushPageResponse();
        compute_node_service::PageID* page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        push_request.set_allocated_page_id(page_id_pb);
        push_request.set_page_data(page->get_data(), PAGE_SIZE);
        push_request.set_src_node_id(src_node_id);
        push_request.set_dest_node_id(dest_node_id);

        brpc::Controller* push_cntl = new brpc::Controller();
        compute_node_service::ComputeNodeService_Stub compute_node_stub(get_compute_channel() + dest_node_id);

        // 等待页面日志刷下去之后，再传走页面
        if (table_id < 10000){
            if (need_to_wait_log && IsLogEnabled()){
                wait_log_flush(page);
            }
        }

        compute_node_stub.PushPage(push_cntl, &push_request, push_response,
            brpc::NewCallback(PushPageRPCDone, push_response, push_cntl, table_id, page_id, this));
    }else {
        assert(push_type == 1);
        LLSN wait_lsn = 0;
        if (need_to_wait_log){
            RmPageHdr *hdr = reinterpret_cast<RmPageHdr*>(page->get_data());
            wait_lsn = hdr->LLSN_;
        }
        std::string page_data(page->get_data(), PAGE_SIZE);
        auto push_task = [this, table_id, page_id, dest_node_id, src_node_id, page_data](){
            compute_node_service::PushPageRequest push_request;
            compute_node_service::PushPageResponse* push_response = new compute_node_service::PushPageResponse();
            compute_node_service::PageID* page_id_pb = new compute_node_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            push_request.set_allocated_page_id(page_id_pb);
            push_request.set_page_data(page_data.c_str(), PAGE_SIZE);
            push_request.set_src_node_id(src_node_id);
            push_request.set_dest_node_id(dest_node_id);

            // LOG(INFO) << "Scheduler Push Page To Node : " << dest_node_id << " table_id = " << table_id << " page_id = " << page_id;

            brpc::Controller* push_cntl = new brpc::Controller();
            compute_node_service::ComputeNodeService_Stub compute_node_stub(get_compute_channel() + dest_node_id);
            compute_node_stub.PushPage(push_cntl, &push_request, push_response,
                brpc::NewCallback(PushPageRPCDone, push_response, push_cntl, table_id, page_id, this));
        };

        bool can_push = true;
        if (page->is_dirty() && need_to_wait_log){
            // 如果页面是脏的，且这个页面的日志还没刷下去，那才需要等待日志刷新
            std::unique_lock<bthread::Mutex> lock(persist_lsn_mtx);
            if (wait_lsn > persist_lsn){
                can_push = false;
            }
        }

        // LOG(INFO) << "PushPage Request Send Scheduler , table_id = " << table_id << " page_id = " << page_id << " dest_node_id = " << dest_node_id << " can push = " << can_push << " wait_lsn = " << wait_lsn;

        assert(node_->getPushPageScheduler());
        node_->getPushPageScheduler()->schedule(push_task , -1 , can_push , wait_lsn);
    }
}

std::string ComputeServer:: UpdatePageFromRemoteCompute(table_id_t table_id, page_id_t page_id, node_id_t node_id){
    // std::cout << "Fetch From Remote\n";
    // 从远程取数据页
    assert(node_id != node_->get_node_id());
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    brpc::Controller cntl;
    if (table_id < 10000){
        node_->fetch_remote_cnt++;
        node_->fetch_from_remote_cnt++;
    }

    // 使用远程compute node进行更新
    compute_node_service::ComputeNodeService_Stub compute_node_stub(&nodes_channel[node_id]);
    compute_node_service::GetPageRequest request;
    compute_node_service::GetPageResponse* response = new compute_node_service::GetPageResponse();
    compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_id(table_id);
    request.set_allocated_page_id(page_id_pb);
    compute_node_stub.GetPage(&cntl, &request, response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to fetch page " << page_id << " from remote compute node";
    }
    std::string ret;
    // 如果对方提前把数据页给丢掉了，那你就自己去存储拿
    if (response->need_to_storage()){
        ret = rpc_fetch_page_from_storage(table_id , page_id);
    }else {
        assert(response->page_data().size() == PAGE_SIZE);
        ret = response->page_data();
        node_->fetch_from_remote_cnt++;
    }

    delete response;
    clock_gettime(CLOCK_REALTIME, &end_time);
    // update_m.lock();
    // this->tx_update_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
    // update_m.unlock();

    return ret;
}

void ComputeServer::InitTableNameMeta(){
    /*
        目前的想法是，10000~20000存 Blink，20000到 30000 存 FSM
    */
    table_name_meta.resize(30000);
    if(WORKLOAD_MODE == 0){
        table_name_meta[0] = "../storage_server/smallbank_savings";
        table_name_meta[1] = "../storage_server/smallbank_checking";
        table_name_meta[10000] = "../storage_server/smallbank_savings_bl";
        table_name_meta[10001] = "../storage_server/smallbank_checking_bl";
        table_name_meta[20000] = "../storage_server/smallbank_savings_fsm";
        table_name_meta[20001] = "../storage_server/smallbank_checking_fsm";
    } else if(WORKLOAD_MODE == 1){
        // 11 张原始表，11 张 B+ 树楔形协议表，11 张 BLink 表
        table_name_meta[0] = "../storage_server/tpcc_warehouse";
        table_name_meta[1] = "../storage_server/tpcc_district";
        table_name_meta[2] = "../storage_server/tpcc_customer";
        table_name_meta[3] = "../storage_server/tpcc_customerhistory";
        table_name_meta[4] = "../storage_server/tpcc_ordernew";
        table_name_meta[5] = "../storage_server/tpcc_order";
        table_name_meta[6] = "../storage_server/tpcc_orderline";
        table_name_meta[7] = "../storage_server/tpcc_item";
        table_name_meta[8] = "../storage_server/tpcc_stock";
        table_name_meta[9] = "../storage_server/tpcc_customerindex";
        table_name_meta[10] = "../storage_server/tpcc_orderindex";
        table_name_meta[10000] = "../storage_server/tpcc_warehouse_bl";
        table_name_meta[10001] = "../storage_server/tpcc_district_bl";
        table_name_meta[10002] = "../storage_server/tpcc_customer_bl";
        table_name_meta[10003] = "../storage_server/tpcc_customerhistory_bl";
        table_name_meta[10004] = "../storage_server/tpcc_ordernew_bl";
        table_name_meta[10005] = "../storage_server/tpcc_order_bl";
        table_name_meta[10006] = "../storage_server/tpcc_orderline_bl";
        table_name_meta[10007] = "../storage_server/tpcc_item_bl";
        table_name_meta[10008] = "../storage_server/tpcc_stock_bl";
        table_name_meta[10009] = "../storage_server/tpcc_customerindex_bl";
        table_name_meta[10010] = "../storage_server/tpcc_orderindex_bl";    
        table_name_meta[20000] = "../storage_server/tpcc_warehouse_fsm";
        table_name_meta[20001] = "../storage_server/tpcc_district_fsm";
        table_name_meta[20002] = "../storage_server/tpcc_customer_fsm";
        table_name_meta[20003] = "../storage_server/tpcc_customerhistory_fsm";
        table_name_meta[20004] = "../storage_server/tpcc_ordernew_fsm";
        table_name_meta[20005] = "../storage_server/tpcc_order_fsm";
        table_name_meta[20006] = "../storage_server/tpcc_orderline_fsm";
        table_name_meta[20007] = "../storage_server/tpcc_item_fsm";
        table_name_meta[20008] = "../storage_server/tpcc_stock_fsm";
        table_name_meta[20009] = "../storage_server/tpcc_customerindex_fsm";
        table_name_meta[20010] = "../storage_server/tpcc_orderindex_fsm";
    }else if (WORKLOAD_MODE == 2){
        table_name_meta[0] = "../storage_server/ycsb_user_table";
        table_name_meta[10000] = "../storage_server/ycsb_user_table_bl";
        table_name_meta[20000] = "../storage_server/ycsb_user_table_fsm";
    }else {
        assert(false);
    }
}

std::string ComputeServer::rpc_fetch_page_from_storage_with_lsn(table_id_t table_id , page_id_t page_id , LLSN page_lsn){
    auto start_time = std::chrono::high_resolution_clock::now();
    storage_service::StorageService_Stub storage_stub(get_storage_channel());
    storage_service::GetPageWithLsnRequest request;
    storage_service::GetPageWithLsnResponse response;
    auto page_id_pb = request.add_page_id();
    page_id_pb->set_page_no(page_id);

    std::string table_name = getTableNameByTableID(table_id);
    page_id_pb->set_table_name(table_name);

    if (IsLogEnabled()){
        request.set_require_lsn(page_lsn);
    }else {
        request.set_require_lsn(0);
    }

    brpc::Controller cntl;
    // LOG(INFO) << "Fetch Page From Stoage With Lsn , table_id = " << table_id << " page_id = " << page_id << " lsn = " << page_lsn;
    storage_stub.GetPageWithLsn(&cntl , &request , &response , NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to fetch page " << page_id << " from remote storage server";
    }
    assert(response.data().size() == PAGE_SIZE);
    if (table_id < 10000){
        node_->fetch_from_storage_cnt++;
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    global_fetch_storage_page_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    return response.data(); 
}

std::string ComputeServer::rpc_fetch_page_from_storage(table_id_t table_id, page_id_t page_id){    
    auto start_time = std::chrono::high_resolution_clock::now();
    storage_service::StorageService_Stub storage_stub(get_storage_channel());
    storage_service::GetPageRequest request;
    storage_service::GetPageResponse response;
    auto page_id_pb = request.add_page_id();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_name(getTableNameByTableID(table_id));

    brpc::Controller cntl;
    storage_stub.GetPage(&cntl, &request, &response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to fetch page " << page_id << " from remote storage server";
    }
    assert(response.data().size() == PAGE_SIZE);
    if (table_id < 10000){
        node_->fetch_from_storage_cnt++;
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    global_fetch_storage_page_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    return response.data(); 
}

void ComputeServer::InvalidRPCDone(partition_table_service::InvalidResponse* response, brpc::Controller* cntl) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<partition_table_service::InvalidResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::LazyReleaseRPCDone(page_table_service::PAnyUnLockResponse* response, brpc::Controller* cntl){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<page_table_service::PAnyUnLockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::PSlockRPCDone(page_table_service::PSLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    // std::unique_ptr<page_table_service::PSLockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        *finish = true;
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::PXlockRPCDone(page_table_service::PXLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    // std::unique_ptr<page_table_service::PXLockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    // std::unique_ptr<bool> finish_guard(finish);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        *finish = true;
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::PushPageRPCDone(compute_node_service::PushPageResponse* response,
                                    brpc::Controller* cntl,
                                    table_id_t table_id,
                                    page_id_t page_id,
                                    ComputeServer* server){
    std::unique_ptr<compute_node_service::PushPageResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "PushPageRPC failed";
    }

    LRLocalPageLock *lr_lock = server->get_node()->getLazyPageLockTable(table_id)->GetLock(page_id);
    /*
        在这里面有 Bug，在这里面的时候页面不在缓冲区里面，排除一下换出的情况
        1. 被缓冲区换出了？
        2. 自己主动换出？这个不可能，因为一轮只会释放一次页面，而本轮页面没释放的话，下一轮是拿不到锁的
    */
    // server->get_node()->getBufferPoolByIndex(table_id)->DecrementPendingOperations(table_id , page_id , lr_lock);
}

void ComputeServer::NotifyCreateTableRPCDone(compute_node_service::NotifyCreateTableResponse* response,
                                             brpc::Controller* cntl,
                                             std::atomic<bool>* has_error){
    std::unique_ptr<compute_node_service::NotifyCreateTableResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "NotifyCreateTable RPC failed";
        *has_error = true;
    }
}

void ComputeServer::NotifyDropTableRPCDone(compute_node_service::NotifyDropTableResponse* response,
                                           brpc::Controller* cntl,
                                           std::atomic<bool>* has_error){
    std::unique_ptr<compute_node_service::NotifyDropTableResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "NotifyDropTable RPC failed";
        *has_error = true;
    }
}


LLSN ComputeServer::AddUpdateLog(uint64_t tx_id , 
                                  DataItem* item,
                                  itemkey_t *key,
                                  Rid rid,
                                  const void* value,
                                  RmPageHdr* pagehdr ,
                                    bool generate_next){
    const size_t item_size = item->GetSerializeSize();
    char* item_buf = (char*)malloc(item_size);
    memcpy(item_buf, (char*)item, sizeof(DataItem));
    memcpy(item_buf + sizeof(DataItem), value, item->value_size);


    itemkey_t pri_key;
    // 如果这个表没有设置主键，那主键位置填充负无穷
    if (key == nullptr){
        // 负无穷
        pri_key = (itemkey_t)(-1);
    }else{
        pri_key = *key;
    }
    RmRecord new_record(pri_key, item_size, item_buf);
    free(item_buf);

    std::string table_name = getTableNameByTableID(item->table_id);
    table_id_t table_id = item->table_id;

    // 目前 batch_id 都是 0，后面用到了再改
    UpdateLogRecord* log = new UpdateLogRecord(0 , node_->getMetaManager()->local_machine_id , 
        tx_id , new_record , rid , table_name , nullptr);
    
    global_update_log_count++;

    log->prev_lsn_ = pagehdr->LLSN_;
    LLSN lsn = UpdatePageLLSN(pagehdr);
    assert(lsn > 0);
    if (generate_next){
        LLSN next_lsn = generate_next_llsn();
        assert(next_lsn == lsn + 1);
    }
    log->lsn_ = lsn;

    // LOG(INFO) << "Add UpdateLog , table_id = " << table_id << " page_id = " << rid.page_no_ << " slot_no = " << rid.slot_no_ << " log lsn = " << log->lsn_ << " log prev lsn = " << log->prev_lsn_;

    AddToLogNoBlock(log);

    return lsn;
}

LLSN ComputeServer::AddLockLog(uint64_t tx_id, table_id_t table_id,
                               const Rid& rid, lock_t lock_type, RmPageHdr* pagehdr) {
    std::string table_name = getTableNameByTableID(table_id);

    LockLogRecord* log = new LockLogRecord(0,
                                           node_->getMetaManager()->local_machine_id,
                                           tx_id,
                                           table_id,
                                           table_name,
                                           rid,
                                           lock_type);

    log->prev_lsn_ = pagehdr->LLSN_;
    LLSN lsn = UpdatePageLLSN(pagehdr);
    log->lsn_ = lsn;
    // LOG(INFO) << "Add LockLog , table_id = " << table_id << " page_id = " << rid.page_no_ << " slot_no = " << rid.slot_no_ << " log lsn = " << log->lsn_ << " log prev lsn = " << log->prev_lsn_;
    AddToLogNoBlock(log);
    

    return lsn;
}

LLSN ComputeServer::AddDeleteLog(uint64_t tx_id , table_id_t table_id,
            itemkey_t* key,
            int page_no,
            int slot_no,RmPageHdr* pagehdr){
    std::string table_name = getTableNameByTableID(table_id);

    DeleteLogRecord* log = new DeleteLogRecord(0,
                                               node_->getMetaManager()->local_machine_id,
                                               tx_id,
                                               table_id,
                                               table_name,
                                               page_no,
                                               slot_no);

    log->prev_lsn_ = pagehdr->LLSN_;
    LLSN lsn = UpdatePageLLSN(pagehdr);
    log->lsn_ = lsn;
    AddToLogNoBlock(log);

    // LOG(INFO) << "Add LockLog , table_id = " << table_id << " page_id = " << page_no << " slot_no = " << slot_no << " log lsn = " << log->lsn_ << " log prev lsn = " << log->prev_lsn_;

    return lsn;
}

LLSN ComputeServer::AddInsertLog(uint64_t tx_id , DataItem* item,
                     itemkey_t* key,
                     const void* value,
                     const Rid& rid,
                     RmPageHdr* pagehdr){
    const size_t item_size = item->GetSerializeSize();
    char* item_buf = (char*)malloc(item_size);
    memcpy(item_buf, (char*)item, sizeof(DataItem));
    memcpy(item_buf + sizeof(DataItem), value, item->value_size);

    itemkey_t pri_key;
    if (key == nullptr){
        pri_key = (itemkey_t)(-1);
    }else{
        pri_key = *key;
    }

    RmRecord new_record(pri_key, item_size, item_buf);
    free(item_buf);

    table_id_t table_id = item->table_id;
    std::string table_name = getTableNameByTableID(table_id);

    InsertLogRecord* log = new InsertLogRecord(0,
                                               get_node()->getMetaManager()->local_machine_id,
                                               tx_id,
                                               new_record,
                                               rid.page_no_,
                                               rid.slot_no_,
                                               table_name);

    log->prev_lsn_ = pagehdr->LLSN_;
    LLSN lsn = UpdatePageLLSN(pagehdr);
    log->lsn_ = lsn;
    AddToLogNoBlock(log);

    return lsn;
}

const std::string ComputeServer::getTableNameByTableID(table_id_t table_id) {
    // SQL 模式下，通过 db_meta 获取表名字
    if (WORKLOAD_MODE == 4){
        // B+ 树存在 10000 - 20000，FSM 存在 20000 到 30000
        int tab_id = 0;
        if (table_id < 10000){
            tab_id = table_id;
        }else if (table_id < 20000){
            tab_id = table_id - 10000;
        }else if (table_id < 30000){
            tab_id = table_id - 20000;
        }else {
            assert(false);
        }

        std::string tab_name = getTableNameFromTableIDSQL(tab_id);
        assert(tab_name != "");

        if (table_id >= 10000 && table_id < 20000){
            tab_name += "_bl";
        }else if (table_id >= 20000 && table_id < 30000){
            tab_name += "_fsm";
        }

        return tab_name;
    }else{
        return table_name_meta[table_id];
    }
    assert(false);
}
