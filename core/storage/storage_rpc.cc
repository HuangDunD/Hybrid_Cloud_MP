#include "storage_rpc.h"
#include "core/recovery/observation.h"
#include "config.h"
#include "record/record.h"
#include "common.h"
#include "base/data_item.h"
#include "storage/blink_tree/blink_tree.h"

#include "core/index/bp_tree/bp_tree_defs.h"

#include <mutex>
#include <filesystem>
#include <set>
#include <shared_mutex>
#include <sys/stat.h>

namespace storage_service{

    void StoragePoolImpl::RegisterComputeIndex(const std::string& logical_path, const std::string& page_path) {
        const auto logical = std::filesystem::absolute(logical_path).lexically_normal().string();
        const auto physical = std::filesystem::absolute(page_path).lexically_normal().string();
        if (logical == physical || !disk_manager_->is_file(logical) || !disk_manager_->is_file(physical))
            throw std::runtime_error("invalid compute index page space");
        if (!compute_index_files_.emplace(logical, physical).second)
            throw std::runtime_error("duplicate compute index page space");
    }

    std::string StoragePoolImpl::ComputePagePath(const std::string& path) const {
        const auto logical = std::filesystem::absolute(path).lexically_normal().string();
        const auto it = compute_index_files_.find(logical);
        return it == compute_index_files_.end() ? path : it->second;
    }

    void StoragePoolImpl::FreezePhysicalWrites() { physical_write_mtx_.lock(); }
    void StoragePoolImpl::ResumePhysicalWrites() { physical_write_mtx_.unlock(); }

    StoragePoolImpl::StoragePoolImpl(LogManager* log_manager, DiskManager* disk_manager, RmManager* rm_manager, brpc::Channel* raft_channels, int raft_num , SmManager *sm_manager_)
        :log_manager_(log_manager), disk_manager_(disk_manager), rm_manager_(rm_manager), raft_channels_(raft_channels), raft_num_(raft_num) , sm_manager(sm_manager_){

        };

    StoragePoolImpl::~StoragePoolImpl(){};

    static void LogOnRPCDone(storage_service::LogWriteResponse* response, brpc::Controller* cntl) {
        // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
        std::unique_ptr<storage_service::LogWriteResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            // RPC失败了. response里的值是未定义的，勿用。
            LOG(ERROR) << "Fail to send log: " << cntl->ErrorText();
        } else {
            // RPC成功了，response里有我们想要的数据。开始RPC的后续处理。
        }
        // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
    }


    // 计算层向存储层写日志
    void StoragePoolImpl::LogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::LogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done){
            
        brpc::ClosureGuard done_guard(done);
        std::shared_lock<std::shared_mutex> write_guard(physical_write_mtx_);
        log_manager_->write_batch_log_to_disk(request->log());

# if RAFT
        // write raft prepare log
        std::vector<brpc::CallId> cids1;
        for(int i=0; i<raft_num_; i++){
            storage_service::StorageService_Stub raft_node_stub(&raft_channels_[i]);
            storage_service::RaftLogWriteRequest raft_request;
            storage_service::LogWriteResponse* raft_response = new storage_service::LogWriteResponse();
            raft_request.set_raft_log(request->log());
            brpc::Controller* cntl = new brpc::Controller();
            cids1.push_back(cntl->call_id());
            raft_node_stub.RaftLogWrite(cntl, &raft_request, response, 
                brpc::NewCallback(LogOnRPCDone, raft_response, cntl));
        }
        for(auto cid:cids1){
            brpc::Join(cid);
            // // LOG(INFO) << "storage node write raft prepare log. ";
        }

        // write raft commit log
        std::vector<brpc::CallId> cids2;
        for(int i=0; i<raft_num_; i++){
            storage_service::StorageService_Stub raft_node_stub(&raft_channels_[i]);
            storage_service::RaftLogWriteRequest raft_request;
            storage_service::LogWriteResponse* raft_response = new storage_service::LogWriteResponse();
            raft_request.set_raft_log(request->log());
            brpc::Controller* cntl = new brpc::Controller();
            cids2.push_back(cntl->call_id());
            raft_node_stub.RaftLogWrite(cntl, &raft_request, response, 
                brpc::NewCallback(LogOnRPCDone, raft_response, cntl));
        }
        for(auto cid:cids2){
            brpc::Join(cid);
            // // LOG(INFO) << "storage node write raft commit log. ";
        }
# endif

        for(int i = 0; i < request->page_id_size(); i++){
            page_id_t page_no = request->page_id()[i].page_no();
            std::string table_name = request->page_id()[i].table_name();
            int fd = disk_manager_->open_file(table_name);

            PageId page_id(fd, page_no);
            log_manager_->log_replay_->pageid_batch_count_[page_id].first.lock();
            log_manager_->log_replay_->pageid_batch_count_[page_id].second++;
            log_manager_->log_replay_->pageid_batch_count_[page_id].first.unlock();
        }

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    };

    void StoragePoolImpl::RaftLogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::RaftLogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        // RDMA_// LOG(INFO) << "handle write log request, log is " << request->log();
        log_manager_->write_raft_log_to_disk(request->raft_log());
        // LOG(INFO) << "Receive Raft log";

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    };

    void StoragePoolImpl::GetPageWithLsn(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageWithLsnRequest* request,
                       ::storage_service::GetPageWithLsnResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);

        LLSN lsn = request->require_lsn();
        std::string return_data;
        for(int i = 0; i < request->page_id().size(); i++){
            const std::string logical_name = request->page_id()[i].table_name();
            const std::string table_name = ComputePagePath(logical_name);
            if (table_name != logical_name && lsn != 0) {
                controller->SetFailed("compute BLink page space has no heap LSN");
                return;
            }
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0) { controller->SetFailed("page file does not exist"); return; }
            
            page_id_t page_no = request->page_id()[i].page_no();
            PageId page_id(fd, page_no);
            batch_id_t request_batch_id = request->require_batch_id();
            LogReplay* log_replay = log_manager_->log_replay_;

            char data[PAGE_SIZE];

            // 第 17 层（early24 fault-0302）：原 pageid_batch_count_ 屏障是
            // 死代码（compute 端 LogWrite 从不填 page_id 列表，计数恒 0，
            // 且全仓库无递减路径），取页与 replay 之间实际无任何同步——
            // only-in-storage 回源遇 replay 积压时读到旧版页，之后写回的
            // 日志 prev 链与已应用版本断裂，storage 回放抛错 abort（实测
            // ycsb_user_table page=1036 have=173 expected=0 next=176）。
            // 改为有界等待 replay 追平已接收 WAL 再读页；replay 单调前进，
            // 追平后的读页只会更新不会更旧。超时 fail-closed：取页失败由
            // compute 端 PageUnavailable → 事务确定性中止契约处理，绝不把
            // 未追平的旧页当权威副本发出。
            const int catchup_ms_17 = [] {
                const char* env = ::getenv("HCM_GETPAGE_CATCHUP_MS");
                return env ? std::max(1000, atoi(env)) : 60000;
            }();
            if (!log_replay->WaitReplayCaughtUp(catchup_ms_17)) {
                LOG(WARNING) << "[StorageNode] GetPageWithLsn replay catch-up timeout: table="
                             << table_name << " page=" << page_no;
                controller->SetFailed("storage replay did not catch up before GetPageWithLsn");
                return;
            }
            page_id_t total_pages = disk_manager_->get_fd2pageno(fd);

            if (table_name != logical_name)
                disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);
            else
                disk_manager_->read_page_with_lsn(fd, page_no, data, PAGE_SIZE, lsn);
            return_data.append(std::string(data, PAGE_SIZE));
        }

        response->set_data(return_data);

        return;
    }

    void StoragePoolImpl::GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);

        std::string return_data;
        for(int i = 0; i < request->page_id().size(); i++){
            const std::string table_name = ComputePagePath(request->page_id()[i].table_name());
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0) { controller->SetFailed("page file does not exist"); return; }
            
            page_id_t page_no = request->page_id()[i].page_no();
            PageId page_id(fd, page_no);
            batch_id_t request_batch_id = request->require_batch_id();
            LogReplay* log_replay = log_manager_->log_replay_;

            char data[PAGE_SIZE];

            // 第 17 层（early24 fault-0302）：与 GetPageWithLsn 同款修复——
            // 死代码 pageid_batch_count_ 屏障替换为有界 replay 追平等待。
            // 本 RPC 是 only-in-storage/存储 fallback 回源的主路径，无 LSN
            // 下界校验，读旧页直接成为"权威副本"，屏障是正确性的唯一防线。
            const int catchup_ms_17 = [] {
                const char* env = ::getenv("HCM_GETPAGE_CATCHUP_MS");
                return env ? std::max(1000, atoi(env)) : 60000;
            }();
            if (!log_replay->WaitReplayCaughtUp(catchup_ms_17)) {
                LOG(WARNING) << "[StorageNode] GetPage replay catch-up timeout: table="
                             << table_name << " page=" << page_no;
                controller->SetFailed("storage replay did not catch up before GetPage");
                return;
            }
            page_id_t total_pages = disk_manager_->get_fd2pageno(fd);

            disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);
            response->add_allocated_pages(total_pages);
            return_data.append(std::string(data, PAGE_SIZE));
        }

        response->set_data(return_data);

        return;
    };

    void StoragePoolImpl::PrefetchIndex(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetBatchIndexRequest* request,
                       ::storage_service::GetBatchIndexResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::unordered_map<std::string, int> table_fd_map;
        // // LOG(INFO) << "handle PrefetchIndex request" << request->table_name() << "  " << request->batch_id();
        std::string table_name = request->table_name();
        std::string index_file_name = table_name + "_index.txt";
        std::ifstream file(index_file_name);
        int batch_id = request->batch_id();
        int offset_line = batch_id * BATCH_INDEX_PREFETCH_SIZE;
        // 定位到文件的行
        // Skip the first offset_line lines
        for (int i = 0; i < offset_line; ++i) {
            std::string line;
            std::getline(file, line);
        }
        // Read the next BATCH_INDEX_PREFETCH_SIZE lines
        std::string line;
        itemkey_t key;
        page_id_t page_id;
        int slot_id;
        int count = 0;
        while (std::getline(file, line)) {
            if (line.empty() || count >= BATCH_INDEX_PREFETCH_SIZE) {
                break;
            }
            count++;
            std::istringstream iss(line);
            iss >> key;
            iss >> page_id;
            iss >> slot_id;
            response->add_itemkey(key);
            response->add_pageid(page_id);
            response->add_slotid(slot_id);
            // std::cout << "Read line: " << line << std::endl;
        }
        file.close();
        return;
    };

    void StoragePoolImpl::WritePage(::google::protobuf::RpcController* controller,
                       const ::storage_service::WritePageRequest* request,
                       ::storage_service::WritePageResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::shared_lock<std::shared_mutex> write_guard(physical_write_mtx_);
        const std::string table_name = ComputePagePath(request->page_id().table_name());
        int fd = disk_manager_->open_file(table_name);
        page_id_t page_no = request->page_id().page_no();
        const std::string& payload = request->data();
        if (fd < 0 || page_no < 0 || page_no >= disk_manager_->get_fd2pageno(fd) || payload.size() != PAGE_SIZE) {
            controller->SetFailed("invalid physical page write");
            return;
        }
        // 第 17 层续（early25 fault-0302 have=173 expected=172）：缓冲淘汰
        // 整页写回（rpc_flush_page_to_storage）可把页头 LLSN 前移到 WAL 中
        // 不存在/未到达的位置（如 victim in-flight 的恢复重建副本被淘汰），
        // 撕裂 replay 的 prev 链（实测 INSERT replay predecessor missing 致
        // storage abort）。heap 数据页的版本前进必须由 replay 单一驱动：
        // 已 commit 增量 WAL 里有、replay 会应用；victim in-flight 增量本就
        // 该随事务中止丢弃。LLSN 不等于盘上版本的写回一律拒绝（请求方
        // 淘汰路径失败仅记日志，丢弃副本安全）；等版本写回跳过落盘直接
        // 成功（同 LLSN 即同日志序列，内容等价，且避免与 replay flush 的
        // 读-判-写竞态）。索引页空间（ComputePagePath 映射表）不走此分支。
        if (table_name == request->page_id().table_name()) {
            char cur_page_17[PAGE_SIZE];
            disk_manager_->read_page(fd, page_no, cur_page_17, PAGE_SIZE);
            const auto* req_hdr_17 = reinterpret_cast<const RmPageHdr*>(payload.data());
            const auto* cur_hdr_17 = reinterpret_cast<const RmPageHdr*>(cur_page_17);
            if (req_hdr_17->LLSN_ != cur_hdr_17->LLSN_) {
                LOG(WARNING) << "[StorageNode] WritePage version guard rejected: table="
                             << table_name << " page=" << page_no
                             << " disk_llsn=" << cur_hdr_17->LLSN_
                             << " req_llsn=" << req_hdr_17->LLSN_;
                controller->SetFailed("heap page version change must come from WAL replay, not WritePage");
                return;
            }
            return;  // 等版本：内容等价，无需落盘
        }
        disk_manager_->write_page(fd, page_no, payload.data(), PAGE_SIZE);

        return;
    };

    void StoragePoolImpl::OpenDb(::google::protobuf::RpcController* controller,
                       const ::storage_service::OpendbRequest* request,
                       ::storage_service::OpendbResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::lock_guard<std::mutex> lk(mutex);

        assert(sm_manager);
        std::string db_name = request->db_name();
        int node_id = request->node_id();

        // std::cout << "Node ID = " << node_id <<  " open DB : " << db_name << "\n";

        // 正确性修复：open_db 会删除并重建全部 blink/FSM 文件。
        // 若重放线程此时持有这些文件的句柄/页面缓存，重放将写入已被
        // 销毁的 fd 或与重建产生竞态。重建期间暂停重放线程，
        // 重建完成后恢复（重放从文件头断点继续，不会丢日志）。
        // RAII 保证任何路径（含 open_db 内部 return）都会恢复重放
        struct ReplayPauseGuard {
            LogReplay* lr;
            explicit ReplayPauseGuard(LogReplay* l) : lr(l) { if (lr) lr->PauseReplay(); }
            ~ReplayPauseGuard() { if (lr) lr->ResumeReplay(); }
        } replay_guard(log_manager_->log_replay_);

        int error_code = sm_manager->open_db(db_name);
        response->set_error_code(error_code);

        // P0 修复（缓存代际）：open_db 删除并重建同名 blink/FSM 文件后，
        // replay 缓存中旧文件代的残留内容（表名哈希相同）会被后续 flush
        // 写回新文件——重建后必须整体失效缓存（pause 窗口内缓存已
        // flush clean，移除无数据损失）
        if (log_manager_ && log_manager_->log_replay_) {
            log_manager_->log_replay_->InvalidateAllReplayPages();
        }

        for (auto &entry : sm_manager->db.m_tabs) {
            response->add_table_names(entry.first);
            response->add_table_id(entry.second.get_table_id());
        }

        response->set_table_num(sm_manager->db.m_tabs.size());

        std::cout << "Node ID = " << node_id <<  " open DB : " << db_name << " error code : " << error_code << "\n";

        return;
    }

    void StoragePoolImpl::CreateTable(::google::protobuf::RpcController *controller,
                        const ::storage_service::CreateTableRequest *request ,
                        ::storage_service::CreateTableResponse *response ,
                      ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);
        assert(sm_manager);

        std::lock_guard<std::mutex> lk(mutex);

        std::string tab_name = request->tab_name();

        std::vector<ColDef> col_defs;
        std::string pri_key = "";

        for (int i = 0 ; i < request->cols_len_size() ; i++){
            int type = request->cols_type(i);
            std::string name = request->cols_name(i);
            int len = request->cols_len(i);

            ColType col_type;
            if (type == 0){
                col_type = ColType::TYPE_INT;
            }else if (type == 1){
                col_type = ColType::TYPE_FLOAT;
            }else if (type == 2){
                col_type = ColType::TYPE_STRING;
            }else if (type == 3){
                // 如果某个参数的类型是 TYPE_ITEMKEY，那就认为这列是主键
                // 主键不放在 DataItem里，元组的结构是 主键 + DataItem + 数据(DataItem 存的是 lock , version 那些东西)
                pri_key = name;
                col_type = ColType::TYPE_ITEMKEY;
            }else {
                assert(false);
            }

            ColDef col_def(name , col_type , len);
            col_defs.emplace_back(col_def);
        }

        // 必须带上主键，否则不给过
        // assert(pri_key != "");

        int error_code = sm_manager->create_table(tab_name , col_defs , pri_key);
        response->set_error_code(error_code);
    }

    void StoragePoolImpl::DropTable(::google::protobuf::RpcController* controller,
                       const ::storage_service::DropTableRequest* request,
                       ::storage_service::DropTableResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::lock_guard<std::mutex> lk(mutex);
        assert(sm_manager);
        std::string tab_name = request->tab_name();


        int error_code = sm_manager->drop_table(tab_name);
        std::cout << "Drop Table : " << tab_name << " Error Code = " << error_code << "\n";
        response->set_error_code(error_code);
    }

    void StoragePoolImpl::TableExist(::google::protobuf::RpcController* controller,
                       const ::storage_service::TableExistRequest* request,
                       ::storage_service::TableExistResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::lock_guard<std::mutex> lk(mutex);
        assert(sm_manager);
        std::string table_name = request->table_name();
        bool exist = sm_manager->db.is_table(table_name);
        response->set_ans(exist);
        if (exist){
            auto &tab = sm_manager->db.get_table(table_name);
            for (auto &col : tab.cols){
                response->add_col_names(col.name);
                response->add_col_lens(col.len);
                if (col.type == ColType::TYPE_FLOAT){
                    response->add_col_types("TYPE_FLOAT");
                }else if (col.type == ColType::TYPE_INT){
                    response->add_col_types("TYPE_INT");
                }else if (col.type == ColType::TYPE_STRING){
                    response->add_col_types("TYPE_STRING");
                }else if (col.type == ColType::TYPE_ITEMKEY){
                    response->add_col_types("TYPE_ITEMKEY");
                }else {
                    assert(false);
                }
            }

            response->set_table_id(tab.table_id);
            // assert(tab.primary_key != "");
            // std::cout << "Read Table Pkey = " << tab.primary_key << "\n";
            if (tab.primary_key != "") {
                response->add_primary(tab.primary_key);
            }
        }
        return;
    }

    void StoragePoolImpl::ShowTable(::google::protobuf::RpcController* controller,
                       const ::storage_service::ShowTableRequest* request,
                       ::storage_service::ShowTableResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::lock_guard<std::mutex> lk(mutex);
        for (auto it : sm_manager->db.m_tabs){
            response->add_tab_name(it.first);
        }

        return ;
    }

    void StoragePoolImpl::CreatePage(::google::protobuf::RpcController* controller , 
                        const ::storage_service::CreatePageRequest *request ,
                        ::storage_service::CreatePageResponse *response , 
                        ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);
        std::shared_lock<std::shared_mutex> write_guard(physical_write_mtx_);
        std::lock_guard<std::mutex> allocation_guard(mutex);
        table_id_t table_id = request->table_id();
        const std::string table_path = ComputePagePath(request->table_name());
        int fd = disk_manager_->open_file(table_path);
        if (fd < 0) { controller->SetFailed("page allocation file does not exist"); return; }
        page_id_t new_page_no = disk_manager_->allocate_page(fd);

        // 如果不是 fsm 或者 blink，写入 file_hdr
        bool is_fsm = (table_path.find("_fsm") != std::string::npos);
        bool is_blink = (table_path.find("_bl") != std::string::npos);

        // 初始化整页为 0
        char zero_page[PAGE_SIZE];
        memset(zero_page, 0, PAGE_SIZE);
        if (!is_fsm && !is_blink) {
            // RM 堆数据页页头必须与 rm_manager/rm_file_handle 的初始化一致
            // （next_free_page_no_ = RM_NO_PAGE）；零值 0 会被只读检查器
            // （tree_stats valid_free_link）判定为越界空闲链指针。
            RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(zero_page);
            page_hdr->next_free_page_no_ = RM_NO_PAGE;
            page_hdr->num_records_ = 0;
            page_hdr->LLSN_ = 0;
            page_hdr->pre_LLSN_ = 0;
        }
        disk_manager_->write_page(fd, new_page_no, zero_page, PAGE_SIZE);

        if (!is_fsm && !is_blink) {
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, RM_FILE_HDR_PAGE, page0_buf, sizeof(page0_buf));
            RmFileHdr* file_hdr = reinterpret_cast<RmFileHdr*>(page0_buf + sizeof(RmPageHdr));
            file_hdr->num_pages_ = new_page_no + 1;
            disk_manager_->update_value(fd, RM_FILE_HDR_PAGE, sizeof(RmPageHdr), reinterpret_cast<char*>(file_hdr), sizeof(RmFileHdr));
        }

        // std::cout << "Create a Page , table_id = " << table_id << " page_id = " << new_page_no << "\n";
        
        response->set_page_no(new_page_no);
        response->set_success(true);
        return;
    }

    void StoragePoolImpl::DeletePage(::google::protobuf::RpcController *controller , 
                const ::storage_service::DeletePageRequest *request ,
                ::storage_service::DeletePageResponse *response ,
                ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);

        std::shared_lock<std::shared_mutex> write_guard(physical_write_mtx_);
        std::lock_guard<std::mutex> allocation_guard(mutex);
        table_id_t table_id = request->table_id();
        page_id_t page_no = request->page_no();
        const std::string table_path = ComputePagePath(request->table_name());
        if (table_path != request->table_name()) {
            controller->SetFailed("compute BLink page deallocation is unsupported");
            return;
        }

        std::cout << "Delete Page , page_no = " << page_no << "\n"; 

        int fd = disk_manager_->open_file(table_path);

        // 判断是否为 B+ 索引文件
        bool is_bp_index = (table_path.find("_bp") != std::string::npos);

        // 将整页清零
        char zero_page[PAGE_SIZE];
        memset(zero_page, 0, PAGE_SIZE);
        disk_manager_->write_page(fd, page_no, zero_page, PAGE_SIZE);

        if (!is_bp_index) {
            // RM 文件：维护页头和文件头的空闲链表
            // 读取 Page 0 获取当前的 first_free_page_no
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            RmFileHdr* file_hdr = reinterpret_cast<RmFileHdr*>(page0_buf + sizeof(RmPageHdr));
            int next_free = file_hdr->first_free_page_no_;

            // 将被删除页面的 next_free 指向旧的 first_free
            disk_manager_->update_value(fd, page_no, OFFSET_NEXT_FREE_PAGE_NO, reinterpret_cast<char*>(&next_free), sizeof(int));
        
            // 更新 Page 0 的 first_free 指向当前被删除的页面
            int new_first_free = static_cast<int>(page_no);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, sizeof(RmPageHdr) + OFFSET_FIRST_FREE_PAGE_NO, reinterpret_cast<char*>(&new_first_free), sizeof(int));
        }

        // disk_manager_->close_file(fd);

        response->set_successs(true);
        return;
    }

    void StoragePoolImpl::ReplayCatchUp(::google::protobuf::RpcController* controller,
                       const ::storage_service::ReplayCatchUpRequest* request,
                       ::storage_service::ReplayCatchUpResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        LogReplay* log_replay = log_manager_->log_replay_;
        const int timeout_ms = request->timeout_ms() > 0 ? request->timeout_ms() : 3600000;

        // 进入时的积压量（计量用，不参与判定）
        const auto entry = log_replay->ReplayBoundaries();
        response->set_backlog_bytes_at_entry(entry.first > entry.second ? entry.first - entry.second : 0);

        const auto started = std::chrono::steady_clock::now();
        const bool caught_up = log_replay->WaitReplayCaughtUp(timeout_ms);
        const auto waited_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started).count();

        // 返回追平后的实际边界（无论成败都填，供调用方计量）
        const auto boundaries = log_replay->ReplayBoundaries();
        response->set_caught_up(caught_up);
        response->set_wal_tail_inclusive(boundaries.first);
        response->set_replay_inclusive(boundaries.second);
        response->set_waited_ms(static_cast<int32_t>(waited_ms));
        if (!caught_up)
            controller->SetFailed("storage replay did not catch up within the requested budget");
        return;
    };

    void StoragePoolImpl::NotifyNodeFailure(::google::protobuf::RpcController* controller,
                       const ::storage_service::StorageNodeFailureNotification* request,
                       ::storage_service::StorageNodeFailureAck* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        node_id_t failed_node_id = request->failed_node_id();
        int64_t detection_ts = request->detection_timestamp();

        LOG(INFO) << "[StorageNode] Received failure notification: compute node "
                   << failed_node_id << " is dead (detected at " << detection_ts << ")";
        std::cerr << "[StorageNode] Compute node " << failed_node_id
                  << " failure detected." << std::endl;

        // 递增恢复代数：AnalyzeRecoveryPages 中的 Undo 依据代数去重，
        // 新的一次故障（不同于上次的故障节点）触发新的一轮 Undo。
        // 同一节点的重复通知（心跳重发）不递增，保证同一次恢复只执行一次 Undo。
        bool first_notification = false;
        {
            std::lock_guard<std::mutex> lk(recovery_undo_mtx_);
            if (failed_node_id != recovery_failed_node_) {
                recovery_generation_++;
                recovery_failed_node_ = failed_node_id;
                first_notification = true;
                recovery_observation::Recorder::Get().SetEpoch(recovery_generation_);
                LOG(INFO) << "[StorageNode] Recovery generation bumped to "
                          << recovery_generation_ << " (failed node " << failed_node_id << ")";
            }
        }
        if (first_notification) log_manager_->log_replay_->ObserveRecoveryBacklog("failure_notification");
    }

    void StoragePoolImpl::AnalyzeRecoveryPages(::google::protobuf::RpcController* controller,
                       const ::storage_service::AnalyzeRecoveryPagesRequest* request,
                       ::storage_service::AnalyzeRecoveryPagesResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        node_id_t failed_node_id = request->failed_node_id();
        LogReplay* log_replay = log_manager_->log_replay_;

        LOG(INFO) << "[StorageNode] Phase 4: Analyzing " << request->pages_size()
                  << " recovery pages for failed node " << failed_node_id
                  << " (alive nodes reported: " << request->alive_node_ids_size() << ")";

        // C4 修复：Phase 4 的 RedoForPages（write_page）与 Undo（update_value）
        // 都会直写数据文件，与 replayFun 并发会产生页面级撕裂。
        // 整个 Phase 4 期间：先等重放追平日志尾，再暂停重放线程。
        // replay_pause_mtx_ 为递归锁，与 UndoForFailedNode 内部暂停嵌套安全
        recovery_observation::Span recovery_span("storage_recovery_rpc");
        if (!log_replay->WaitReplayCaughtUp()) {
            controller->SetFailed("replay not caught up; recovery pages remain isolated");
            return;
        }
        struct Phase4ReplayGuard {
            LogReplay* lr;
            explicit Phase4ReplayGuard(LogReplay* l) : lr(l) { lr->PauseReplay(); }
            ~Phase4ReplayGuard() { lr->ResumeReplay(); }
        } phase4_guard(log_replay);
        if (!log_replay->WaitReplayCaughtUp(1)) {
            controller->SetFailed("WAL advanced before replay pause; retry recovery");
            return;
        }

        // 全局超时计数器：所有页面的日志等待总共不超过 5 秒
        const int GLOBAL_MAX_WAIT_MS = 5000;
        int global_waited_ms = 0;

        int redo_count = 0;
        int no_modify_count = 0;

        // ===== 第一轮：等待页面日志批次排空 + 读取磁盘 LSN，收集需要 Redo 的页面 =====
        // 性能修复：RedoForPage 每页全量扫描日志（O(页数x日志大小)），
        // 改为收集后单次扫描批量回放（RedoForPages）
        std::vector<LogReplay::RecoveryRedoRequest> redo_requests;
        std::vector<int> redo_result_indexes;  // 待回填的响应下标，与 redo_requests 一一对应
        // 第 11 层修复（live-early10）：待从回放树重建的 BLink 索引页。
        // 第一轮捕获（跳过原 derived_page 静默 no-modify），Undo 之后统一执行
        // 回放树→计算页空间的整页拷贝（见拷贝阶段注释）。
        struct BlinkReplaySlot {
            int result_index;
            std::string table_name;    // 回放树文件（请求表名原文件，如 ycsb_user_table_bl）
            std::string resolved_path; // 计算页空间文件（ComputePagePath 解析，如 _bl_compute）
            uint32_t page_no;
        };
        std::vector<BlinkReplaySlot> blink_replay_slots;

        for (int i = 0; i < request->pages_size(); i++) {
            const auto& page_info = request->pages(i);
            std::string table_name = page_info.table_name();
            page_id_t page_no = page_info.page_no();
            LLSN gplm_lsn = page_info.gplm_lsn();
            table_id_t table_id = page_info.table_id();

            auto* result = response->add_results();
            result->set_table_id(table_id);
            result->set_page_no(page_no);

            if (table_name.empty()) {
                LOG(ERROR) << "[StorageNode] Phase 4: empty table name for table_id=" << table_id
                           << " page=" << page_no << " — status=-1, IR retained";
                result->set_status(-1);
                result->set_recovered_lsn(0);
                no_modify_count++;
                continue;
            }

            // P0 修复（页空间混用）：计算端 IR 页号属于计算页空间（如
            // ycsb_user_table_bl → ycsb_user_table_bl_compute），必须与普通
            // 取页/写页共用同一 ComputePagePath 映射后再打开物理文件。
            // 原实现直接打开请求表名（回放树 _bl），计算空间页号超出其文件
            // 范围时命中下方越界分支——静默 status=-1 → 计算端校验失败静默
            // return → IR 锁永久保留、admission 永久拒绝（r2-fault-small-003
            // 与本次 fault-001 的恢复卡死即此路径）。
            const std::string resolved_path = ComputePagePath(table_name);

            int fd = disk_manager_->open_file(resolved_path);
            if (fd < 0) {
                LOG(WARNING) << "[StorageNode] Phase 4: Cannot open file for table " << table_name
                             << " (resolved=" << resolved_path << ")";
                result->set_status(-1);
                result->set_recovered_lsn(0);
                no_modify_count++;
                continue;
            }

            // 等待该页面上的日志批次完成（使用引用避免迭代器失效导致崩溃）
            PageId page_id(fd, page_no);
            bool page_batch_timed_out = false;
            {
                log_replay->latch3_.lock();
                auto it = log_replay->pageid_batch_count_.find(page_id);
                if (it != log_replay->pageid_batch_count_.end()) {
                    auto& batch_mutex = it->second.first;
                    auto& batch_count = it->second.second;
                    batch_mutex.lock();
                    while (batch_count > 0) {
                        batch_mutex.unlock();
                        log_replay->latch3_.unlock();

                        if (global_waited_ms >= GLOBAL_MAX_WAIT_MS) {
                            // 超时处理修复：不再强制清零 batch_count（清零会让在途 replay
                            // 与恢复结果产生竞争）。改为跳过该页面的 Redo，降级为
                            // storage-only，由 replay 线程随后自然追平
                            // （replay 按 prev_lsn 链保证最终正确性）
                            LOG(WARNING) << "[StorageNode] Phase 4: Global timeout, skip redo for page (table="
                                         << table_id << ", page=" << page_no << "), remaining batches="
                                         << batch_count;
                            page_batch_timed_out = true;
                            break;
                        }

                        usleep(1000); // 1ms
                        global_waited_ms++;
                        log_replay->latch3_.lock();
                        batch_mutex.lock();
                    }
                    if (!page_batch_timed_out) {
                        batch_mutex.unlock();
                        log_replay->latch3_.unlock();
                    }
                    // 超时 break 时两把锁均已在循环体内释放
                } else {
                    log_replay->latch3_.unlock();
                }
            }

            if (page_batch_timed_out) {
                LOG(ERROR) << "[StorageNode] Phase 4: page batch wait timeout (table=" << table_name
                           << " page=" << page_no << ") — status=-1, IR retained";
                result->set_status(-1);
                result->set_recovered_lsn(0);
                no_modify_count++;
                continue;
            }

            // 检查页面是否在文件范围内（按 ComputePagePath 解析后的物理文件）
            page_id_t total_pages = disk_manager_->get_fd2pageno(fd);
            if (page_no >= total_pages) {
                // P0 修复（未物化虚拟页）：计算端 GPLM 页号覆盖整个分区虚拟
                // 空间，故障接管会对尚未物化的槽位（如 heap page 32..36、
                // FSM 高槽位）也上 IR 锁。CreatePage/写页都会扩展文件并抬升
                // 水位，进程故障模型下文件内容只增不减 ⇒ 页号超出物理文件
                // 末尾即"从未物化"，无数据可恢复。
                // 原实现对该分支静默 status=-1：计算端校验失败静默 return，
                // IR 锁永久保留、admission 永久拒绝（fault-001/fault-002 的
                // 恢复卡死）。正确语义：无恢复动作（status=0）并显式记录；
                // 仅当物理文件确实含该页而水位偏低（异常状态）才 fail-closed。
                struct stat st;
                const uint64_t file_pages =
                    (::fstat(fd, &st) == 0) ? (uint64_t)st.st_size / PAGE_SIZE : 0;
                if ((uint64_t)page_no >= file_pages) {
                    LOG(WARNING) << "[StorageNode] Phase 4: page beyond materialized extent "
                                 << "(table=" << table_name << " resolved=" << resolved_path
                                 << " page=" << page_no << " file_pages=" << file_pages
                                 << ") — never materialized, no recovery action (status=0)";
                    result->set_status(0);
                    result->set_recovered_lsn(0);
                    no_modify_count++;
                    continue;
                }
                LOG(ERROR) << "[StorageNode] Phase 4: page out of watermark range "
                           << "(table=" << table_name << " resolved=" << resolved_path
                           << " page=" << page_no << " total_pages=" << total_pages
                           << " file_pages=" << file_pages
                           << ") — status=-1, IR retained (fail-closed)";
                result->set_status(-1);
                result->set_recovered_lsn(0);
                no_modify_count++;
                continue;
            }

            // 安全读取页面
            char data[PAGE_SIZE];
            try {
                disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);
            } catch (const std::exception& e) {
                LOG(WARNING) << "[StorageNode] Phase 4: Failed to read page (table=" << table_id
                             << ", page=" << page_no << "): " << e.what();
                result->set_status(-1);
                result->set_recovered_lsn(0);
                no_modify_count++;
                continue;
            }

            const bool derived_page = (table_id >= 10000 && table_id < 30000) ||
                (table_name.size() >= 3 && table_name.compare(table_name.size() - 3, 3, "_bl") == 0) ||
                (table_name.size() >= 4 && table_name.compare(table_name.size() - 4, 4, "_fsm") == 0);
            const bool fsm_page = table_name.size() >= 4 &&
                table_name.compare(table_name.size() - 4, 4, "_fsm") == 0;
            if (derived_page && !fsm_page) {
                // 第 11 层缺陷修复（live-early10）：BLink 索引页无 LSN 物理回放
                // 可行性（BLINKINSERT/DELETE 是逻辑日志，无目标页号），原实现
                // 无条件 no-modify 使死节点缓冲中的已提交索引条目永久丢失
                //（探针 NOT_FOUND，no-loss 契约破坏）。修复：从存储侧回放树
                //（请求表名原文件）按页号取 WAL 当前页像——回放树与计算页空间
                // 同构同编号（同一 BLink 实现、同一操作序列的确定性重建；页级
                // 比对已验证：early10 26/27 页相同、唯一差异页回放树领先 1 条目）
                //，且函数入口已 WaitReplayCaughtUp+Pause，回放树=WAL 权威。
                // 实际拷贝推迟到 Undo 之后（撤销未提交幻影条目会修改回放树）。
                blink_replay_slots.push_back({i, table_name, resolved_path, page_no});
                result->set_status(1);
                result->set_recovered_lsn(0);
                redo_count++;
                continue;
            }
            if (derived_page) {
                // FSM：建议性空间管理结构，不参与正确性，维持 no-modify。
                result->set_status(0);
                result->set_recovered_lsn(0);
                no_modify_count++;
                continue;
            }
            RmPageHdr* page_hdr = reinterpret_cast<RmPageHdr*>(data);
            LLSN disk_lsn = page_hdr->LLSN_;

            // 尝试日志回放：gplm_lsn=0 表示 LSN 信息丢失，回放到所有日志末尾
            // disk_lsn < gplm_lsn 表示页面落后于已知状态，需要回放
            if (gplm_lsn == 0 || disk_lsn < gplm_lsn) {
                // 延迟到第二轮批量回放
                LogReplay::RecoveryRedoRequest redo_req;
                redo_req.table_name = table_name;
                redo_req.page_no = page_no;
                redo_req.disk_lsn = disk_lsn;
                redo_req.target_lsn = gplm_lsn;
                redo_requests.push_back(std::move(redo_req));
                redo_result_indexes.push_back(i);
            } else {
                // disk_lsn >= gplm_lsn：页面已是最新，无需回放
                result->set_status(0);
                result->set_recovered_lsn(disk_lsn);
                no_modify_count++;
            }
        }

        // ===== 第二轮：单次扫描日志文件，批量执行定向 Redo，并按记录的下标回填结果 =====
        if (!redo_requests.empty()) {
            std::vector<LogReplay::RecoveryRedoResult> redo_results =
                log_replay->RedoForPages(redo_requests);
            for (size_t k = 0; k < redo_result_indexes.size(); k++) {
                auto* result = response->mutable_results(redo_result_indexes[k]);
                const auto& rr = redo_results[k];
                if (rr.success) {
                    // 回放成功，返回回放后的数据
                    result->set_status(1);
                    result->set_page_data(rr.page_data);
                    result->set_recovered_lsn(rr.recovered_lsn);
                    redo_count++;
                } else {
                    const auto& redo_request = redo_requests[k];
                    // P0 修复（日志语义）：GPLM 无该页 LSN（target_lsn=0）且全量
                    // 扫描确认无匹配日志，是合法的"无需回放"结论（status=0），
                    // 不是失败——原实现统一打 "targeted redo FAILED" 误导排障
                    // （r2-fault-small-004 即此组合）。真实失败（扫描不完整/
                    // 有匹配日志但回放失败）才保留 ERROR 并 fail-closed。
                    const bool no_redo_needed = rr.scan_complete && rr.no_matching_redo &&
                                                redo_request.target_lsn == 0;
                    if (no_redo_needed) {
                        LOG(WARNING) << "[StorageNode] Phase 4: no matching redo for table="
                                     << redo_request.table_name << " page=" << redo_request.page_no
                                     << " (GPLM LSN unknown, scan complete) — treated as no-modify";
                    } else {
                        LOG(ERROR) << "[StorageNode] Phase 4: targeted redo FAILED for table="
                                   << redo_request.table_name << " page=" << redo_request.page_no
                                   << " disk_lsn=" << redo_request.disk_lsn
                                   << " target_lsn=" << redo_request.target_lsn
                                   << " scan_complete=" << rr.scan_complete
                                   << " no_matching_redo=" << rr.no_matching_redo;
                    }
                    result->set_status(no_redo_needed ? 0 : -1);
                    result->set_recovered_lsn(redo_request.disk_lsn);
                    no_modify_count++;
                }
            }
        }

        // 执行 Undo：撤销所有未提交事务的修改
        // P10 修复（替代 static std::once_flag）：按恢复代数去重。
        // 多个存活节点会各自发送 AnalyzeRecoveryPages，同一次故障恢复只执行一次 Undo；
        // 之后发生新的故障（generation 递增）会重新执行。
        // 注意：不能用 static std::once_flag —— 它是进程级单次触发，
        // 第二次节点故障时 Undo 将被永久跳过，导致未提交脏数据残留。
        // P0 修复：传入存活节点集合，存活节点前缀事务跳过 undo
        //（终局由其自身负责；防止扫描窗口内半截事务被误撤销）
        int undo_count = 0;
        {
            std::lock_guard<std::mutex> lk(recovery_undo_mtx_);
            if (undo_done_generation_ != recovery_generation_) {
                std::set<node_id_t> alive_nodes(request->alive_node_ids().begin(),
                                                request->alive_node_ids().end());
                // P0 修复（异常防护）：UndoForFailedNode 内部（ApplyUndoWalRecord
                // 的布局校验/BLink 处理）可能抛出 runtime_error——异常若逃逸
                // RPC handler 会杀死存储进程。捕获并转为 RPC 失败：恢复保持
                // 隔离，绝不把异常路径当作 undo 成功
                try {
                    shared_undo_count_ = log_replay->UndoForFailedNode(failed_node_id, alive_nodes);
                } catch (const std::exception& e) {
                    LOG(ERROR) << "[StorageNode] Phase 4: UndoForFailedNode exception: "
                               << e.what() << " — recovery stays isolated";
                    controller->SetFailed("undo exception; recovery pages remain isolated");
                    return;
                }
                if (shared_undo_count_ < 0) {
                    controller->SetFailed("undo failed; recovery pages remain isolated");
                    return;
                }
                undo_done_generation_ = recovery_generation_;
            }
            undo_count = shared_undo_count_;
            }

            // ===== IR 隔离页孤儿行锁清扫（smoke-023 实证）=====
            // victim 的已提交事务存在「COMMIT 日志 flush（durable 提交点）」
            // 到「行级落实日志（lock=UNLOCKED + version=commit_ts）」之间的
            // 窗口：kill 落在窗口内时 WAL 只含到 COMMIT 为止的日志，Redo
            // 重放后行保持执行期锁状态（实测 page 1031 slot0：
            // lock=0xff00000000000000、version=0——行对所有后续写事务永久
            // 冲突，且无任何日志会来清理它）。Undo 只处理未提交事务、
            // 提交落实日志根本不存在，redo/undo 都无法覆盖。
            // 本请求页集合均为 IR 隔离页：恢复期间所有节点（含存活者）都
            // 无法访问，Undo/Redo 完成后页上任何残余行锁必为 victim 未竟
            // 事务的孤儿锁，无条件清扫安全（幂等：重复清扫无副作用）。
            // 仅清扫堆数据页（_bl/_fsm 页空间无元组锁）；处于 replay 暂停
            // 临界区内（Phase4ReplayGuard），与重放线程无并发写。
            int swept_locks = 0;
            for (int i = 0; i < request->pages_size(); i++) {
            const auto& sweep_info = request->pages(i);
            const std::string sweep_table = sweep_info.table_name();
            if (sweep_table.empty()) continue;
            const size_t tlen = sweep_table.size();
            if (tlen > 3 && sweep_table.compare(tlen - 3, 3, "_bl") == 0) continue;
            if (tlen > 4 && sweep_table.compare(tlen - 4, 4, "_fsm") == 0) continue;

            const std::string sweep_path = ComputePagePath(sweep_table);
            int sweep_fd = disk_manager_->open_file(sweep_path.c_str());
            if (sweep_fd < 0) continue;
            page_id_t sweep_page = sweep_info.page_no();
            if ((uint64_t)sweep_page >= (uint64_t)disk_manager_->get_fd2pageno(sweep_fd)) continue;

            char sweep_buf[PAGE_SIZE];
            try {
            disk_manager_->read_page(sweep_fd, sweep_page, sweep_buf, PAGE_SIZE);
            } catch (const std::exception& e) {
            LOG(WARNING) << "[IR-Sweep] read failed: table=" << sweep_table
                         << " page=" << sweep_page << ": " << e.what();
            continue;
            }
            char hdr_buf[PAGE_SIZE];
            disk_manager_->read_page(sweep_fd, PAGE_NO_RM_FILE_HDR, hdr_buf, PAGE_SIZE);
            const RmFileHdr& sweep_hdr =
            *reinterpret_cast<const RmFileHdr*>(hdr_buf + OFFSET_FILE_HDR);

            char* bitmap = sweep_buf + sizeof(RmPageHdr);
            char* slots = bitmap + sweep_hdr.bitmap_size_;
            const size_t slot_size =
            size_t(sweep_hdr.record_size_) + sizeof(itemkey_t);
            int swept_page = 0;
            for (int slot = 0; slot < sweep_hdr.num_records_per_page_; ++slot) {
            const bool occupied =
                (bitmap[slot >> 3] & (0x80 >> (slot & 7))) != 0;
            if (!occupied) continue;
            DataItem* item = reinterpret_cast<DataItem*>(
                slots + size_t(slot) * slot_size + sizeof(itemkey_t));
            if (item->lock != UNLOCKED) {
                LOG(WARNING) << "[IR-Sweep] orphan tuple lock cleared: table="
                             << sweep_table << " page=" << sweep_page
                             << " slot=" << slot;
                item->lock = UNLOCKED;
                ++swept_page;
            }
            }
            if (swept_page > 0) {
            disk_manager_->write_page(sweep_fd, sweep_page, sweep_buf, PAGE_SIZE);
            swept_locks += swept_page;
            }
            }
            if (swept_locks > 0) {
            LOG(WARNING) << "[StorageNode] Phase 4: swept " << swept_locks
                     << " orphan tuple lock(s) on IR-isolated heap pages"
                     << " (failed node " << failed_node_id << ")";
            }

            // ===== 第 11 层修复执行段：BLink 索引页从回放树重建计算页空间副本 =====
        // 必须在 Undo 之后执行：死节点未提交事务的 BLINKINSERT 幻影条目由
        // UndoForFailedNode 从回放树撤销（Phase 4 的 redo/undo 直写盘，读回放
        // 树文件即撤销后状态），此处拷贝最终权威页像。整页覆盖写使计算页空间
        //（bl_compute，即计算端 lazy fetch 的取页来源）达到 WAL 当前状态；
        // 计算端按 status=1 走 released_replayed 处置，第三轮重读自动回填
        // page_data。拷贝失败（回放树缺页/IO 异常）降级为 no-modify 并显式
        // 记日志——不静默、不隔离整个恢复。
        //
        // 第 13 层修复：ApplyBLinkInsert/Delete 写入的是 storage 侧 buffer
        // pool 的内存页（write-back；小数据集下无驱逐，磁盘 bl 文件停留在
        // initial 旧像）。拷贝源是磁盘文件，必须先把 buffer pool 全部脏页
        // 落盘 + fsync，拷到的才是「WAL 已应用」的权威页像；否则 bl_compute
        // 被 initial 旧像覆盖，故障后 post-load 键全部"消失"（heap 有行、
        // 索引无条目 → READ NOT_FOUND）。
        if (!log_replay->FlushStorageBackedTree()) {
            LOG(WARNING) << "[StorageNode] Phase 4: sm_manager absent, "
                            "blink copy source may be stale";
        }
        for (const auto &slot : blink_replay_slots) {
            auto *result = response->mutable_results(slot.result_index);
            if (!disk_manager_->is_file(slot.table_name)) {
                LOG(WARNING) << "[StorageNode] Phase 4: blink replay tree missing, table="
                             << slot.table_name << " page=" << slot.page_no << " — no-modify";
                result->set_status(0);
                no_modify_count++; redo_count--;
                continue;
            }
            char bl_data[PAGE_SIZE];
            try {
                int bl_fd = disk_manager_->open_file(slot.table_name);
                disk_manager_->read_page(bl_fd, slot.page_no, bl_data, PAGE_SIZE);
                // 第 13 层观测：拷贝源页内容指纹（叶键数/首末键），用于比对
                //「buffer pool 内存树」与「磁盘文件」的新旧差异
                {
                    int32_t prev_, next_, right_; int32_t num_k = 0;
                    memcpy(&prev_, bl_data, 4); memcpy(&next_, bl_data + 4, 4);
                    memcpy(&right_, bl_data + 8, 4); memcpy(&num_k, bl_data + 12, 4);
                    int8_t is_leaf = bl_data[24];
                    int64_t k0 = 0, kn = 0;
                    if (is_leaf && num_k > 0) {
                        memcpy(&k0, bl_data + 32, 8);
                        memcpy(&kn, bl_data + 32 + (int64_t)(num_k - 1) * 8, 8);
                    }
                    LOG(INFO) << "[StorageNode] Phase 4: blink copy source page=" << slot.page_no
                              << " leaf=" << (int)is_leaf << " num_key=" << num_k
                              << " first_key=" << k0 << " last_key=" << kn;
                }
                int cp_fd = disk_manager_->open_file(slot.resolved_path);
                disk_manager_->write_page(cp_fd, slot.page_no, bl_data, PAGE_SIZE);
            } catch (const std::exception &e) {
                LOG(ERROR) << "[StorageNode] Phase 4: blink replay copy failed, table="
                           << slot.table_name << " page=" << slot.page_no
                           << " resolved=" << slot.resolved_path << ": " << e.what()
                           << " — no-modify";
                result->set_status(0);
                no_modify_count++; redo_count--;
                continue;
            }
            LOG(INFO) << "[StorageNode] Phase 4: blink index page recovered from replay tree, "
                      << "table=" << slot.table_name << " page=" << slot.page_no
                      << " -> " << slot.resolved_path;
        }

        // 第 13 层修复(b)：post-load 键集中在最右叶链与上层内节点（分裂
        // 产生的新根/新页），这些页大多属于存活节点的页集、不在 IR（死
        // 节点）页集内——lazy 模式下其最新像只存在于回放树（存储内存 +
        // 上述 flush 后的 bl 文件），bl_compute 仍是旧像。恢复语义：计算
        // 页空间整体推进到「WAL 已应用 + Undo 后」的权威像。对每个 blink
        // 表做整文件拷贝（幂等，含 IR 页；回放树是全局权威，不会降级
        // 存活节点已提交的修改）。pwrite 自动扩展 bl_compute 的新增页。
        {
            std::set<std::pair<std::string, std::string>> bl_tables;
            for (const auto &s : blink_replay_slots)
                bl_tables.emplace(s.table_name, s.resolved_path);
            for (const auto &bl : bl_tables) {
                try {
                    if (!disk_manager_->is_file(bl.first)) continue;
                    int bl_fd = disk_manager_->open_file(bl.first);
                    uint64_t bl_pages = disk_manager_->get_file_size(bl.first) / PAGE_SIZE;
                    int cp_fd = disk_manager_->open_file(bl.second);
                    char sync_buf[PAGE_SIZE];
                    for (uint64_t pg = 0; pg < bl_pages; ++pg) {
                        disk_manager_->read_page(bl_fd, (int)pg, sync_buf, PAGE_SIZE);
                        disk_manager_->write_page(cp_fd, (int)pg, sync_buf, PAGE_SIZE);
                    }
                    LOG(INFO) << "[StorageNode] Phase 4: blink whole-file sync, table="
                              << bl.first << " -> " << bl.second
                              << " pages=" << bl_pages;
                } catch (const std::exception &e) {
                    LOG(ERROR) << "[StorageNode] Phase 4: blink whole-file sync failed, table="
                               << bl.first << ": " << e.what();
                }
            }
        }

        for (int i = 0; i < response->results_size(); ++i) {
            auto* result = response->mutable_results(i);
            if (result->status() < 0) continue;
            // P0 修复（页空间一致性 + 消灭静默降级）：
            // 1) 重读必须与普通取页共用 ComputePagePath 映射，否则回放树/
            //    计算页空间混用（原实现直接打开请求表名）。
            // 2) 从未物化的虚拟页（页号超出物理文件末尾）没有内容可重读，
            //    保持 status=0（no-modify）即可；原实现对其 read_page 抛异常
            //    后被 catch 静默改成 status=-1，覆盖前面的正确结论，
            //    导致计算端校验失败、IR 永久保留（本次 fault-003 复现）。
            // 3) 真实重读失败保持 fail-closed（status=-1）但必须显式记录。
            const std::string resolved = ComputePagePath(request->pages(i).table_name());
            int fd = disk_manager_->open_file(resolved);
            if (fd < 0) {
                LOG(ERROR) << "[StorageNode] Phase 4: post-undo re-read cannot open table="
                           << request->pages(i).table_name() << " (resolved=" << resolved
                           << ") page=" << result->page_no() << " — status=-1, IR retained";
                result->set_status(-1);
                result->clear_page_data();
                continue;
            }
            struct stat st;
            const uint64_t file_pages =
                (::fstat(fd, &st) == 0) ? (uint64_t)st.st_size / PAGE_SIZE : 0;
            if ((uint64_t)result->page_no() >= file_pages) {
                // 未物化页：无内容可发布，保持 status=0
                recovery_observation::Emit("storage_post_undo", result->table_id(),
                                           result->page_no(), result->status(), 0, 0,
                                           "not_a_distributed_ready_certificate");
                continue;
            }
            try {
                char final_page[PAGE_SIZE];
                disk_manager_->read_page(fd, result->page_no(), final_page, PAGE_SIZE);
                if (result->status() == 1) {
                    result->set_page_data(final_page, PAGE_SIZE);
                    result->set_recovered_lsn(reinterpret_cast<RmPageHdr*>(final_page)->LLSN_);
                }
                recovery_observation::Emit("storage_post_undo", result->table_id(), result->page_no(), result->status(), 0, 0, "not_a_distributed_ready_certificate");
            } catch (const std::exception& e) {
                LOG(ERROR) << "[StorageNode] Phase 4: post-undo re-read failed table="
                           << request->pages(i).table_name() << " page=" << result->page_no()
                           << ": " << e.what() << " — status=-1, IR retained";
                result->set_status(-1);
                result->clear_page_data();
            }
        }

        LOG(INFO) << "[StorageNode] Phase 4: Analysis complete for " << request->pages_size()
                  << " pages. Redo: " << redo_count << ", NoModify: " << no_modify_count
                  << ", Undo transactions: " << undo_count;
        // P0 修复：RedoForPages/Undo 本轮直写过磁盘；即使 Undo 因 generation
        // 去重未执行，redo 直写后缓存也可能残留旧内容。pause 窗口关闭前
        // 统一失效 replay 缓存（此时缓存已 flush clean，移除无数据损失）
        log_replay->InvalidateAllReplayPages();
    }
}
