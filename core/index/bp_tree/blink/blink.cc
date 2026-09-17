#include "blink.h"
#include "common.h"
#include "core/index/bp_tree/bp_tree_defs.h"
#include "compute_server/server.h"
#include "assert.h"
#include "core/recovery/observation.h"
#include "core/util/bench_control.h"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <sys/stat.h>


int BLinkNodeHandle::lower_bound(const itemkey_t *target){
    int l = 0 , r = get_size() , mid;
    while (l < r){
        mid = (l + r) / 2;
        if (bl_compare(get_key(mid) , target) >= 0){
            r = mid;
        }else {
            l = mid + 1;
        }
    }
    return l;
}

int BLinkNodeHandle::upper_bound(const itemkey_t *target){
    int l = 1 , r = get_size() , mid;
    while(l < r){
        mid = (l + r) / 2;
        if (bl_compare(get_key(mid) , target) > 0){
            r = mid;
        }else{
            l = mid + 1;
        }
    }
    return l;
}

void BLinkNodeHandle::insert_pairs(int pos , const itemkey_t *keys , const Rid *rids , int n){
    int size = get_size();
    assert(!(pos > size || pos < 0));
    if (size == 0){
        memmove(get_key(0) , keys , n * key_size);
        memmove(get_rid(0) , rids , n * sizeof(Rid));
    }else {
        int move_num = (size - pos);
        memmove(get_key(pos + n) , get_key(pos) , move_num * key_size);
        memmove(get_key(pos) , keys , n * key_size);

        memmove(get_rid(pos + n) , get_rid(pos) , move_num * sizeof(Rid));
        memmove(get_rid(pos) , rids , n * sizeof(Rid));
    }

    node_hdr->num_key += n;
}

void BLinkNodeHandle::insert_pair(int pos , const itemkey_t *key , const Rid *rid){
    insert_pairs(pos , key , rid , 1);
}

bool BLinkNodeHandle::leaf_lookup(const itemkey_t* target, Rid** value){
    assert(is_leaf());
    int pos = lower_bound(target);
    if (get_size() == pos || !isIt(pos , target)){
        return false;
    }
    *value = get_rid(pos);
    return true;
}

bool BLinkNodeHandle::isIt(int pos, const itemkey_t* key){
    return bl_compare(get_key(pos) , key) == 0;
}

int BLinkNodeHandle::insert(const itemkey_t* key, const Rid& value){
    if (get_size() == 0){
        insert_pair(0 , key , &value);
        assert(node_hdr->num_key == 1);
        return node_hdr->num_key;
    }

    int pos = lower_bound(key);
    // 如果插入的key 已经存在，那就直接返回
    if (pos != get_size() && isIt(pos , key)){
        return node_hdr->num_key;
    }

    insert_pair(pos , key , &value);
    return node_hdr->num_key;
}

void BLinkNodeHandle::erase_pair(int pos){
    assert(pos >= 0 && pos < get_size());

    int move_num = get_size() - pos - 1;
    itemkey_t *erase_key = get_key(pos);
    itemkey_t *next_key = get_key(pos + 1);
    memmove(erase_key , next_key , move_num * key_size);

    Rid *erase_rid = get_rid(pos);
    Rid *next_rid = get_rid(pos + 1);
    memmove(erase_rid , next_rid , move_num * sizeof(Rid));

    node_hdr->num_key--;
}


int BLinkNodeHandle::remove(const itemkey_t* key){
    int pos = lower_bound(key);
    if (pos == get_size() || !isIt(pos , key)){
        return node_hdr->num_key;
    }
    erase_pair(pos);
    return node_hdr->num_key;
}

bool BLinkNodeHandle::need_delete(const itemkey_t *key){
    int pos = lower_bound(key);
    if (pos == get_size() || !isIt(pos , key)){
        return false;
    }
    return true;
}

int BLinkNodeHandle::find_child(page_id_t child_page_id){
    for (int i = 0 ; i < get_size() ; i++){
        if (value_at(i) == child_page_id){
            return i;
        }
    }
    return -1;
}

// BLIndex
page_id_t BLinkIndexHandle::create_node(){
    page_id_t ret = server->rpc_create_page(table_id);
    return ret;
}
void BLinkIndexHandle::destroy_node(page_id_t page_id){
    server->rpc_delete_node(table_id , page_id);
}

void BLinkIndexHandle::s_get_file_hdr(){
    recovery_observation::PathPage(table_id, BL_HEAD_PAGE_ID, 1);
    Page *page;
    if (SYSTEM_MODE == 0){
        page = server->rpc_fetch_s_page(table_id , BL_HEAD_PAGE_ID);
    }else {
        page = server->rpc_lazy_fetch_s_page(table_id , BL_HEAD_PAGE_ID);
    }
    file_hdr->deserialize(page->get_data());
}
Page* BLinkIndexHandle::x_get_file_hdr(){
    Page *page;
    if (SYSTEM_MODE == 0){
        page = server->rpc_fetch_x_page(table_id , BL_HEAD_PAGE_ID);
    }else {
        page = server->rpc_lazy_fetch_x_page(table_id , BL_HEAD_PAGE_ID);
    }
    file_hdr->deserialize(page->get_data());
    return page;
}
void BLinkIndexHandle::s_release_file_hdr(){
    if (SYSTEM_MODE == 0){
        server->rpc_release_s_page(table_id , BL_HEAD_PAGE_ID);
    }else {
        server->rpc_lazy_release_s_page(table_id , BL_HEAD_PAGE_ID);
    }
}
void BLinkIndexHandle::x_release_file_hdr(Page *page){
    assert(page->get_page_id().page_no == BL_HEAD_PAGE_ID);
    file_hdr->serialize(page->get_data());
    if (SYSTEM_MODE == 0){
        server->rpc_release_x_page(table_id , BL_HEAD_PAGE_ID);
    }else {
        server->rpc_lazy_release_x_page(table_id , BL_HEAD_PAGE_ID);
    }
}

BLinkNodeHandle *BLinkIndexHandle::fetch_node(page_id_t page_id , BPOperation opera){
    recovery_observation::PathPage(table_id, page_id, 0);
    BLinkNodeHandle *ret = nullptr;
    if (opera == BPOperation::SEARCH_OPERA){
        Page *page;
        if (SYSTEM_MODE == 0){
            page = server->rpc_fetch_s_page(table_id , page_id);
        }else {
            page = server->rpc_lazy_fetch_s_page(table_id , page_id);
        }
        ret = new BLinkNodeHandle(page);
    }else{
        Page *page;
        if (SYSTEM_MODE == 0){
            page = server->rpc_fetch_x_page(table_id , page_id);
        }else{
            page = server->rpc_lazy_fetch_x_page(table_id , page_id);
        }
        ret = new BLinkNodeHandle(page);
    }
    return ret;
}

void BLinkIndexHandle::release_node(page_id_t page_id , BPOperation opera){
    if (opera == BPOperation::SEARCH_OPERA){
        if (SYSTEM_MODE == 0){
            server->rpc_release_s_page(table_id , page_id);
        }else {
            server->rpc_lazy_release_s_page(table_id , page_id);
        }
    }else {
        if (SYSTEM_MODE == 0){
            server->rpc_release_x_page(table_id , page_id);
        }else {
            server->rpc_lazy_release_x_page(table_id , page_id);
        }
    }
}

BLinkNodeHandle* BLinkIndexHandle::find_leaf_for_search(const itemkey_t * key){
    BLinkNodeHandle *node = nullptr;
    page_id_t root_page_id = INVALID_PAGE_ID;
    while (true){
        s_get_file_hdr();
        root_page_id = file_hdr->root_page_id;
        s_release_file_hdr();

        node = fetch_node(root_page_id , BPOperation::SEARCH_OPERA);
        // 可能在我获取到根节点的这段时间里面，根节点变了，那就需要去找到新的根节点
        if (node->is_root()){
            break;
        }else {
            release_node(root_page_id , BPOperation::SEARCH_OPERA);
        }
    }

    while (!node->is_leaf()){
        while (node->need_to_right(*key)){
            page_id_t sib = node->get_right_sibling();
            release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
            delete node; // 先删旧句柄
            node = fetch_node(sib , BPOperation::SEARCH_OPERA); // 再取兄弟
        }

        int pos = node->upper_bound(key);
        page_id_t child_page_no = node->value_at(pos - 1);
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node;
        node = fetch_node(child_page_no , BPOperation::SEARCH_OPERA);
    }

    while (node->need_to_right(*key)){
        page_id_t sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node; // 先删旧句柄
        node = fetch_node(sib , BPOperation::SEARCH_OPERA); // 再取兄弟
    }

    return node;
}

// 打印路径上的一些信息，DEBUG
void BLinkIndexHandle::find_leaf_for_search_with_print(const itemkey_t *key , std::stringstream &ss){
    BLinkNodeHandle *node = nullptr;
    page_id_t root_page_id = INVALID_PAGE_ID;
    while (true){
        s_get_file_hdr();
        root_page_id = file_hdr->root_page_id;
        s_release_file_hdr();

        node = fetch_node(root_page_id , BPOperation::SEARCH_OPERA);
        // 可能在我获取到根节点的这段时间里面，根节点变了，那就需要去找到新的根节点
        if (node->is_root()){
            break;
        }else {
            release_node(root_page_id , BPOperation::SEARCH_OPERA);
        }
    }

    while (!node->is_leaf()){
        while (node->need_to_right(*key)){
            page_id_t sib = node->get_right_sibling();

            ss << "Need To Right Sibling , page_id = " << node->get_page_no() << " node_size = " << node->get_size() << " Search Key = " << *key << "\n";
            for (int i = 0 ; i < node->get_size() ; i++){
                ss << *node->get_key(i) << " ";
            }
            ss << "\n\n";

            release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
            delete node; // 先删旧句柄

            node = fetch_node(sib , BPOperation::SEARCH_OPERA); // 再取兄弟
        }

        int pos = node->upper_bound(key);
        page_id_t child_page_no = node->value_at(pos - 1);

        ss << "Internal Look Up , page_id = " << node->get_page_no() << 
            " Node Size = " << node->get_size() << " Search Key = " << *key 
            << " pos = " << pos
            << " chosen key = " << *node->get_key(pos - 1)
            << " child page no = " << child_page_no << "\n";
        for (int i = 0 ; i < node->get_size() ; i++){
            ss << *node->get_key(i) << " ";
        }
        ss << "\n\n";

        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node;

        node = fetch_node(child_page_no , BPOperation::SEARCH_OPERA);
    }

    while (node->need_to_right(*key)){
        ss << "Leaf Need To Right Sibling , page_id = " 
            << node->get_page_no() << " node_size = " << node->get_size() << "\n";
        for (int i = 0 ; i < node->get_size() ; i++){
            ss << *node->get_key(i) << " ";
        }
        ss << "\n\n";

        page_id_t sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node; // 先删旧句柄
        node = fetch_node(sib , BPOperation::SEARCH_OPERA); // 再取兄弟
    }

    ss << "Leaf Look Up , page_id = " << node->get_page_no() << " Node Size = " << node->get_size() << "\n";
    for (int i = 0 ; i < node->get_size() ; i++){
        ss << *node->get_key(i) << " ";
    }
    ss << "\n\n";
}

BLinkNodeHandle* BLinkIndexHandle::find_leaf_for_insert(const itemkey_t * key , std::vector<page_id_t>& trace){
    BLinkNodeHandle *node = nullptr;
    page_id_t root_page_id = INVALID_PAGE_ID;
    
    while (true){
        s_get_file_hdr();
        root_page_id = file_hdr->root_page_id;
        s_release_file_hdr();

        node = fetch_node(root_page_id , BPOperation::SEARCH_OPERA);
        if (node->is_root()){
            break;
        }else {
            release_node(root_page_id , BPOperation::SEARCH_OPERA);
            delete node; 
        }
    }

    while (!node->is_leaf()){
        while (node->need_to_right(*key)){
            page_id_t sib = node->get_right_sibling();
            release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
            delete node;
            node = fetch_node(sib , BPOperation::SEARCH_OPERA);
        }
        trace.emplace_back(node->get_page_no()); 

        int pos = node->upper_bound(key);
        page_id_t child_page_no = node->value_at(pos - 1);
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node;
        node = fetch_node(child_page_no , BPOperation::SEARCH_OPERA);
    }

    page_id_t leaf_id = node->get_page_no();
    release_node(leaf_id , BPOperation::SEARCH_OPERA);

    node = fetch_node(leaf_id , BPOperation::INSERT_OPERA);

    while (node->need_to_right(*key)){ 
        page_id_t sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::INSERT_OPERA);
        delete node;
        node = fetch_node(sib , BPOperation::INSERT_OPERA);
    }
    return node;
}

BLinkNodeHandle* BLinkIndexHandle::find_leaf_for_delete(const itemkey_t * key){
    BLinkNodeHandle *node = nullptr;
    page_id_t root_page_id = INVALID_PAGE_ID;
    while (true){
        s_get_file_hdr();
        root_page_id = file_hdr->root_page_id;
        s_release_file_hdr();

        node = fetch_node(root_page_id , BPOperation::SEARCH_OPERA);
        if (node->is_root()){
            break;
        }else {
            release_node(root_page_id , BPOperation::SEARCH_OPERA);
            delete node; 
        }
    }

    while (!node->is_leaf()){
        while (node->need_to_right(*key)){
            page_id_t sib = node->get_right_sibling();
            release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
            delete node;
            node = fetch_node(sib , BPOperation::SEARCH_OPERA);
        }

        int pos = node->upper_bound(key);
        page_id_t child_page_no = node->value_at(pos - 1);
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node;
        node = fetch_node(child_page_no , BPOperation::SEARCH_OPERA);
    }

    page_id_t leaf_id = node->get_page_no();
    release_node(leaf_id , BPOperation::SEARCH_OPERA);

    node = fetch_node(leaf_id , BPOperation::DELETE_OPERA);

    while (node->need_to_right(*key)){ 
        page_id_t sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::DELETE_OPERA);
        delete node;
        node = fetch_node(sib , BPOperation::DELETE_OPERA);
    }
    return node;
}


void BLinkIndexHandle::StableSnapshot(const std::string& directory, uint64_t byte_budget) {
    if (!bench_control::enabled() || SYSTEM_MODE != 1 || WORKLOAD_MODE != 2 ||
        table_id != 10000 || byte_budget > uint64_t(6) * 1024 * 1024 * 1024)
        throw std::runtime_error("compute snapshot requires supervised YCSB lazy mode");
    const std::filesystem::path destination(directory);
    if (!destination.is_absolute() || destination.parent_path() != bench_control::directory() ||
        destination.filename() == "." || destination.filename() == "..")
        throw std::runtime_error("snapshot must be an exclusive child of the control directory");

    // P1 修复：快照逐页取 heap 页走 GetPageWithLsn，页 LSN 由后台 replay 推进；
    // QUIESCE 只停计算端，不保证 replay 已追平 WAL 尾。若直接开始快照，每个
    // 页面请求都会在存储端自旋等待该页 LSN 达标（read_page_with_lsn），快照被
    // replay 拖着走且自旋重读与 replay 争抢 IO（r1c-natural-load-002 cp4 的
    // TREE_SNAPSHOT 1800s 超时即此机制）。改为快照前一次性等待 replay 追平：
    // 追平后所有页 LSN 达标，取页恢复为纯 RPC 往返；等待有界，超时 fail-closed
    // 立即返回明确错误，不再长时间挂起后才发现失败。
    struct SnapshotTiming {
        uint64_t catchup_ms = 0;
        uint64_t catchup_backlog_bytes_at_entry = 0;
        uint64_t catchup_backlog_bytes_left = 0;
        // 每类 dump 的分项计时（微秒）
        struct Phase {
            uint64_t pages = 0;
            uint64_t fetch_us = 0;        // rpc_lazy_fetch_s_page（含缓存命中/存储取）
            uint64_t persist_us = 0;      // 索引页 flush_page_to_storage + GetPage readback
            uint64_t write_us = 0;        // write_all 到快照文件
            uint64_t fsync_us = 0;        // cache_window fdatasync + posix_fadvise + finish
            std::vector<std::pair<uint64_t, uint64_t>> progress;  // 每 16384 页 {pages_done, elapsed_ms}
        } index, heap, fsm;
    };
    SnapshotTiming timing;
    const auto snapshot_started = std::chrono::steady_clock::now();
    {
        storage_service::StorageService_Stub stub(server->get_storage_channel());
        storage_service::ReplayCatchUpRequest request;
        const char* timeout_env = ::getenv("HCM_SNAPSHOT_CATCHUP_TIMEOUT_MS");
        request.set_timeout_ms(timeout_env ? std::max(1, atoi(timeout_env)) : 900000);
        storage_service::ReplayCatchUpResponse response;
        brpc::Controller controller;
        controller.set_timeout_ms(request.timeout_ms() + 30000);
        controller.set_max_retry(0);
        stub.ReplayCatchUp(&controller, &request, &response, nullptr);
        if (controller.Failed() || !response.caught_up())
            throw std::runtime_error("compute snapshot pre-catchup failed: " +
                (controller.Failed() ? controller.ErrorText() : std::string("storage replay did not catch up")));
        timing.catchup_ms = static_cast<uint64_t>(response.waited_ms());
        timing.catchup_backlog_bytes_at_entry = response.backlog_bytes_at_entry();
        const uint64_t tail = response.wal_tail_inclusive(), applied = response.replay_inclusive();
        timing.catchup_backlog_bytes_left = tail > applied ? tail - applied : 0;
    }

    using Image = std::array<char, PAGE_SIZE>;
    // 堆头页（page 0）由存储侧 CreatePage 扩展并维护，compute 持有的缓存副本会滞后
    // 于权威分配；分配 RPC 本身返回存储当前的 page 0，因此堆头部元数据与快照中的
    // page 0 必须取自该权威副本。索引/FSM 头页由 compute 维护，仍走 compute 视图。
    auto storage_page0 = [&](const char* name, uint64_t& allocated_pages, Image* image) {
        storage_service::StorageService_Stub stub(server->get_storage_channel());
        storage_service::GetPageRequest request;
        auto* page = request.add_page_id();
        page->set_table_name(name);
        page->set_page_no(0);
        storage_service::GetPageResponse response;
        brpc::Controller controller;
        controller.set_timeout_ms(5000);
        controller.set_max_retry(0);
        stub.GetPage(&controller, &request, &response, nullptr);
        if (controller.Failed() || response.allocated_pages_size() != 1 ||
            response.data().size() != PAGE_SIZE || response.allocated_pages(0) == 0)
            throw std::runtime_error("compute snapshot allocation RPC failed");
        allocated_pages = response.allocated_pages(0);
        if (image) memcpy(image->data(), response.data().data(), PAGE_SIZE);
    };
    uint64_t index_pages = 0, heap_allocated_pages = 0, fsm_allocated_pages = 0;
    Image heap_header{};
    // 必须使用与 rpc_create_page 完全相同的表名字符串（table_name_meta）：
    // open_file 以路径字符串为键缓存 fd 与 fd2pageno 计数，裸名会命中初始化时
    // 打开的另一个 fd，读到从未跟随扩展递增的过期分配计数。
    const char* const index_name = server->table_name_meta[10000].c_str();
    const char* const heap_name = server->table_name_meta[0].c_str();
    const char* const fsm_name = server->table_name_meta[20000].c_str();
    storage_page0(index_name, index_pages, nullptr);
    storage_page0(heap_name, heap_allocated_pages, &heap_header);
    storage_page0(fsm_name, fsm_allocated_pages, nullptr);
    auto read_page = [&](table_id_t table, page_id_t id, Image& image, bool persist_index = false) {
        const auto fetch_started = std::chrono::steady_clock::now();
        Page* page = server->rpc_lazy_fetch_s_page(table, id, table < 10000);
        if (page == nullptr) throw std::runtime_error("compute snapshot page unavailable");
        const auto persist_started = std::chrono::steady_clock::now();
        try {
            memcpy(image.data(), page->get_data(), PAGE_SIZE);
            if (persist_index) {
                server->rpc_flush_page_to_storage(table, id);
                storage_service::StorageService_Stub stub(server->get_storage_channel());
                storage_service::GetPageRequest request;
                auto* target = request.add_page_id();
                target->set_table_name("ycsb_user_table_bl");
                target->set_page_no(id);
                storage_service::GetPageResponse response;
                brpc::Controller controller;
                controller.set_timeout_ms(5000);
                controller.set_max_retry(0);
                stub.GetPage(&controller, &request, &response, nullptr);
                if (controller.Failed() || response.data().size() != PAGE_SIZE ||
                    memcmp(response.data().data(), image.data(), PAGE_SIZE) != 0)
                    throw std::runtime_error("compute index backing flush verification failed");
            }
        } catch (...) {
            server->rpc_lazy_release_s_page(table, id);
            throw;
        }
        const auto phase_done = std::chrono::steady_clock::now();
        auto* bucket = table == table_id ? &timing.index : (table == 0 ? &timing.heap : &timing.fsm);
        bucket->fetch_us += std::chrono::duration_cast<std::chrono::microseconds>(persist_started - fetch_started).count();
        bucket->persist_us += std::chrono::duration_cast<std::chrono::microseconds>(phase_done - persist_started).count();
        server->rpc_lazy_release_s_page(table, id);
    };
    Image index_header, fsm_header;
    read_page(table_id, BL_HEAD_PAGE_ID, index_header);
    read_page(20000, FSM_META_PAGE_ID, fsm_header);
    BLFileHdr index_meta(-1, -1, -1);
    index_meta.deserialize(index_header.data());
    RmFileHdr heap_meta;
    memcpy(&heap_meta, heap_header.data() + sizeof(RmPageHdr), sizeof(heap_meta));
    FSMMetaData fsm_meta;
    memcpy(&fsm_meta, fsm_header.data(), sizeof(fsm_meta));
    const uint64_t fsm_pages = uint64_t(fsm_meta.next_fsm_page_id) + 1;
    if (heap_meta.num_pages_ < 1 || uint64_t(heap_meta.num_pages_) != heap_allocated_pages)
        throw std::runtime_error("compute heap page-count header differs from physical allocation: header=" +
                                 std::to_string(heap_meta.num_pages_) + " allocated=" + std::to_string(heap_allocated_pages));
    if (fsm_pages != fsm_allocated_pages)
        throw std::runtime_error("compute FSM metadata differs from physical allocation: meta=" +
                                 std::to_string(fsm_pages) + " allocated=" + std::to_string(fsm_allocated_pages));
    if (index_pages < BL_INIT_PAGE_NUM || index_pages > ComputeNodeBufferPageSize ||
        heap_meta.num_pages_ < 1 || heap_meta.num_pages_ > ComputeNodeBufferPageSize ||
        fsm_meta.magic_number != 0x46534D54 || fsm_meta.version != 1 ||
        fsm_pages < 3 || fsm_pages > ComputeNodeBufferPageSize ||
        fsm_meta.total_fsm_pages != fsm_pages - 2 ||
        fsm_meta.root_page_id < FSM_ROOT_PAGE_ID || fsm_meta.root_page_id >= fsm_pages)
        throw std::runtime_error("invalid compute snapshot allocation metadata");
    const uint64_t total_bytes = (index_pages + uint64_t(heap_meta.num_pages_) + fsm_pages) * PAGE_SIZE;
    if (total_bytes > byte_budget) throw std::runtime_error("compute snapshot exceeds byte budget");
    if (::mkdir(destination.c_str(), 0700) != 0)
        throw std::runtime_error("cannot create exclusive compute snapshot directory");

    struct Output {
        int fd;
        explicit Output(const std::filesystem::path& path)
            : fd(::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600)) {
            if (fd < 0) throw std::runtime_error("cannot create compute snapshot file");
        }
        ~Output() { ::close(fd); }
        void finish() {
            if (::fsync(fd) != 0) throw std::runtime_error("compute snapshot fsync failed");
        }
    };
    auto dump = [&](const char* name, table_id_t table, uint64_t pages, const Image* page0 = nullptr,
                    SnapshotTiming::Phase* phase = nullptr) {
        Output output(destination / name);
        Image image;
        constexpr uint64_t cache_window = uint64_t(64) * 1024 * 1024;
        uint64_t written = 0;
        const auto dump_started = std::chrono::steady_clock::now();
        for (uint64_t id = 0; id < pages; ++id) {
            if (id == 0 && page0 != nullptr) {
                image = *page0;
            } else {
                read_page(table, static_cast<page_id_t>(id), image, table == table_id);
            }
            const auto write_started = std::chrono::steady_clock::now();
            bench_control::write_all(output.fd, image.data(), image.size());
            if (phase) {
                phase->write_us += std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - write_started).count();
                if (++phase->pages % 16384 == 0)
                    phase->progress.emplace_back(phase->pages, static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - dump_started).count()));
            }
            written += image.size();
            if (written % cache_window == 0) {
                const auto sync_started = std::chrono::steady_clock::now();
                if (::fdatasync(output.fd) != 0) throw std::runtime_error("compute snapshot sync failed");
                ::posix_fadvise(output.fd, written - cache_window, cache_window, POSIX_FADV_DONTNEED);
                if (phase) {
                    phase->fsync_us += std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - sync_started).count();
                }
            }
        }
        const auto finish_started = std::chrono::steady_clock::now();
        output.finish();
        ::posix_fadvise(output.fd, 0, 0, POSIX_FADV_DONTNEED);
        if (phase) {
            phase->fsync_us += std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - finish_started).count();
        }
    };
    dump("ycsb_user_table_bl", table_id, index_pages, nullptr, &timing.index);
    dump("ycsb_user_table", 0, heap_meta.num_pages_, &heap_header, &timing.heap);
    dump("ycsb_user_table_fsm", 20000, fsm_pages, nullptr, &timing.fsm);

    Image verification;
    read_page(table_id, BL_HEAD_PAGE_ID, verification);
    if (verification != index_header) throw std::runtime_error("compute index header changed during snapshot");
    {
        uint64_t heap_pages_now = 0;
        Image heap_header_now{};
        storage_page0(heap_name, heap_pages_now, &heap_header_now);
        if (heap_header_now != heap_header || heap_pages_now != heap_allocated_pages)
            throw std::runtime_error("compute heap header changed during snapshot");
    }
    read_page(20000, FSM_META_PAGE_ID, verification);
    if (verification != fsm_header) throw std::runtime_error("compute FSM metadata changed during snapshot");
    {
        uint64_t index_pages_now = 0, heap_pages_now = 0, fsm_pages_now = 0;
        storage_page0(index_name, index_pages_now, nullptr);
        storage_page0(heap_name, heap_pages_now, nullptr);
        storage_page0(fsm_name, fsm_pages_now, nullptr);
        if (index_pages_now != index_pages || heap_pages_now != heap_allocated_pages ||
            fsm_pages_now != fsm_allocated_pages)
            throw std::runtime_error("compute file allocation changed during snapshot");
    }

    auto manifest = JsonConfig::empty_dict("compute-snapshot");
    manifest.insert_string("format", "hcm-raw-pages-v1");
    manifest.insert_string("source", "compute-lazy-fetch");
    manifest.insert_bool("index_backing_verified", true);
    manifest.insert_string("run_id", bench_control::run_id());
    manifest.insert_int64("node", server->get_node()->getNodeID());
    manifest.insert_int64("page_size", PAGE_SIZE);
    manifest.insert_uint64("index_pages", index_pages);
    manifest.insert_uint64("heap_pages", heap_meta.num_pages_);
    manifest.insert_uint64("fsm_pages", fsm_pages);
    manifest.insert_uint64("total_bytes", total_bytes);
    manifest.insert_int64("root_page", index_meta.root_page_id);
    manifest.insert_int64("first_leaf", index_meta.first_leaf);
    manifest.insert_int64("last_leaf", index_meta.last_leaf);
    manifest.insert_bool("complete", true);
    // P1 计量：快照分项计时与进度（低开销：每页 2 个 steady_clock 取样，
    // 每 16384 页一条进度记录）。pre_catchup_* 记录快照前的 replay 追平
    // 等待；各 phase 的 fetch/persist/write/fsync 均为累计微秒。
    manifest.insert_uint64("pre_catchup_waited_ms", timing.catchup_ms);
    manifest.insert_uint64("pre_catchup_backlog_bytes_at_entry", timing.catchup_backlog_bytes_at_entry);
    manifest.insert_uint64("pre_catchup_backlog_bytes_left", timing.catchup_backlog_bytes_left);
    manifest.insert_uint64("total_wall_ms", static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - snapshot_started).count()));
    auto phase_json = [](const char* name, const SnapshotTiming::Phase& phase) {
        JsonConfig json = JsonConfig::empty_dict(name);
        json.insert_uint64("pages", phase.pages);
        json.insert_uint64("fetch_us", phase.fetch_us);
        json.insert_uint64("persist_us", phase.persist_us);
        json.insert_uint64("write_us", phase.write_us);
        json.insert_uint64("fsync_us", phase.fsync_us);
        auto progress = JsonConfig::empty_array("progress");
        for (const auto& [pages_done, elapsed_ms] : phase.progress) {
            auto item = JsonConfig::empty_dict("p");
            item.insert_uint64("pages", pages_done);
            item.insert_uint64("elapsed_ms", elapsed_ms);
            progress.push_back_dict(item);
        }
        json.insert_array("progress", progress);
        return json;
    };
    manifest.insert_dict("timing_index", phase_json("timing_index", timing.index));
    manifest.insert_dict("timing_heap", phase_json("timing_heap", timing.heap));
    manifest.insert_dict("timing_fsm", phase_json("timing_fsm", timing.fsm));
    const std::string text = manifest.dump() + "\n";
    {
        Output output(destination / "manifest.json.tmp");
        bench_control::write_all(output.fd, text.data(), text.size());
        output.finish();
    }
    if (::rename((destination / "manifest.json.tmp").c_str(), (destination / "manifest.json").c_str()) != 0)
        throw std::runtime_error("compute snapshot manifest publish failed");
    const int dirfd = ::open(destination.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0) throw std::runtime_error("compute snapshot directory open failed");
    const int synced = ::fsync(dirfd);
    ::close(dirfd);
    if (synced != 0) throw std::runtime_error("compute snapshot directory fsync failed");
}

BLinkStableStats BLinkIndexHandle::StableStats() {
    BLinkStableStats stats;
    s_get_file_hdr();
    const page_id_t root = file_hdr->root_page_id;
    s_release_file_hdr();
    if (root < BL_INIT_ROOT_PAGE_ID) throw std::runtime_error("invalid compute BLink root");
    stats.root_page = root;
    std::unordered_set<page_id_t> visited;
    std::queue<std::pair<page_id_t, int>> work;
    work.emplace(root, 1);
    while (!work.empty()) {
        const auto [page_id, level] = work.front();
        work.pop();
        if (!visited.insert(page_id).second) throw std::runtime_error("compute BLink cycle or duplicate page");
        if (page_id < BL_INIT_ROOT_PAGE_ID || page_id >= ComputeNodeBufferPageSize || level > 64)
            throw std::runtime_error("compute BLink page or depth exceeds budget");
        auto* node = fetch_node(page_id, BPOperation::SEARCH_OPERA);
        try {
            if (node->get_size() < 0 || node->get_size() > node->get_order())
                throw std::runtime_error("compute BLink invalid node size");
            if (static_cast<int>(stats.level_pages.size()) < level) stats.level_pages.resize(level, 0);
            ++stats.level_pages[level - 1];
            if (node->is_leaf()) {
                ++stats.leaf_pages;
                stats.total_keys += node->get_size();
            } else {
                if (node->get_size() <= 0) throw std::runtime_error("compute BLink empty internal node");
                for (int i = 0; i < node->get_size(); ++i) {
                    const page_id_t child = node->get_rid(i)->page_no_;
                    if (child < BL_INIT_ROOT_PAGE_ID || child >= ComputeNodeBufferPageSize)
                        throw std::runtime_error("compute BLink invalid child page");
                    work.emplace(child, level + 1);
                }
            }
        } catch (...) {
            release_node(page_id, BPOperation::SEARCH_OPERA);
            delete node;
            throw;
        }
        release_node(page_id, BPOperation::SEARCH_OPERA);
        delete node;
    }
    stats.height = static_cast<int>(stats.level_pages.size());
    return stats;
}

std::pair<BLinkNodeHandle* , itemkey_t> BLinkIndexHandle::split(BLinkNodeHandle *node){
    observation_generation_.fetch_add(1, std::memory_order_relaxed);
    recovery_observation::Emit("path_invalidate", table_id, node->get_page_no(), observation_generation_.load(), 0, 0, "local_split_only");
    assert(node->get_size() == node->get_max_size());
    page_id_t new_node_id = create_node();
    BLinkNodeHandle *new_node = fetch_node(new_node_id , BPOperation::INSERT_OPERA);

    new_node->set_is_leaf(node->is_leaf());
    new_node->set_size(0);
    new_node->set_prev_leaf(INVALID_PAGE_ID);
    new_node->set_next_leaf(INVALID_PAGE_ID);
    new_node->set_right_sibling(node->get_right_sibling());
    if (node->has_high_key()) new_node->set_high_key(node->get_high_key());
    else new_node->reset_high_key();
    
    // std::cout << "Split a new node , page_no = " << new_node->get_page_no() << "\n";

    int old_node_size = node->get_size() / 2;
    int new_node_size = node->get_size() - old_node_size;
    assert(old_node_size > 0 && new_node_size > 0);
    node->set_size(old_node_size);

    itemkey_t *new_keys = node->get_key(old_node_size);
    Rid *new_rids = node->get_rid(old_node_size);
    new_node->insert_pairs(0 , new_keys , new_rids , new_node_size);

    // 先把左节点需要设置的 high_key 保存下来
    itemkey_t old_node_high_key = *new_node->get_key(0);
    if (new_node->is_leaf()){
        new_node->set_next_leaf(node->get_next_leaf());
        new_node->set_prev_leaf(node->get_page_no());
        if (node->get_next_leaf() != INVALID_PAGE_ID){
            BLinkNodeHandle *prev_next = fetch_node(node->get_next_leaf() , BPOperation::UPDATE_OPERA);
            prev_next->set_prev_leaf(new_node->get_page_no());
            release_node(node->get_next_leaf() , BPOperation::UPDATE_OPERA);
            delete prev_next;
        }
        node->set_next_leaf(new_node->get_page_no());
    }else {
        // 内部节点分裂，第一个 key 为-无穷
        new_node->set_key(0 , NEG_KEY);
    }

    node->set_high_key(old_node_high_key);
    node->set_has_high_key(true);
    node->set_right_sibling(new_node->get_page_no());

    // old_node_high_key 不仅仅是旧节点的 high_key , 还是 new_node 所有子树的最小值
    return std::make_pair(new_node , old_node_high_key);
}

void BLinkIndexHandle::insert_into_parent(BLinkNodeHandle *old_node , const itemkey_t sep_key ,
                                      BLinkNodeHandle *new_node ,
                                      std::vector<page_id_t> &trace){
    // 如果旧的节点是一个根节点，那就需要去创建一个新的 Root，然后把旧的 Root 拆分为两个节点
    if (old_node->is_root()){
        page_id_t new_root_id = create_node();
        BLinkNodeHandle *new_root = fetch_node(new_root_id , BPOperation::INSERT_OPERA);

        std::cout << "Create A New Root , page_no = " << new_root->get_page_no() << "\n";
        
        new_root->set_is_leaf(false);
        new_root->set_next_leaf(INVALID_PAGE_ID);
        new_root->set_prev_leaf(INVALID_PAGE_ID);
        new_root->set_is_root(true);
        new_root->set_size(0);
        new_root->init_internal_node(); // 第一个 key = NEG_KEY
        new_root->set_rid(0 , {.page_no_ = old_node->get_page_no() , .slot_no_ = -1});

        Rid rid1 = {.page_no_ = new_node->get_page_no() , -1};
        new_root->insert_pair(1 , &sep_key , &rid1);

        old_node->set_is_root(false);
        new_node->set_is_root(false);

        // 释放三个节点
        release_node(new_node->get_page_no() , BPOperation::INSERT_OPERA);
        release_node(old_node->get_page_no() , BPOperation::INSERT_OPERA);
        release_node(new_root_id , BPOperation::INSERT_OPERA);
        
        // 根节点变化后，需要同步到 file_hdr 中，不然别的节点看不到
        Page *page = x_get_file_hdr();
        file_hdr->root_page_id = new_root->get_page_no();
        x_release_file_hdr(page);

        delete new_root;
        return ;
    }

    page_id_t old_page_id = old_node->get_page_no();
    page_id_t new_page_id = new_node->get_page_no();
    release_node(new_page_id , BPOperation::INSERT_OPERA);
    
    page_id_t parent_page_id = trace.back();
    trace.pop_back();

    BLinkNodeHandle *parent = fetch_node(parent_page_id , BPOperation::INSERT_OPERA);
    release_node(old_page_id , BPOperation::INSERT_OPERA);

    int idx = parent->find_child(old_page_id);
    // 如果没找到，那一定在右边
    if (idx == -1){
        assert(parent->need_to_right(sep_key));
    }
    while (idx == -1 && parent->need_to_right(sep_key)){
        page_id_t right_sib = parent->get_right_sibling();
        assert(right_sib != INVALID_PAGE_ID);
        release_node(parent->get_page_no() , BPOperation::INSERT_OPERA);
        delete parent;
        parent = fetch_node(right_sib , BPOperation::INSERT_OPERA);
        continue;
    }
    int old_child_idx = parent->find_child(old_page_id);
    Rid rid2 = {.page_no_ = new_page_id , -1};
    parent->insert_pairs(old_child_idx + 1 , &sep_key , &rid2 , 1);

    delete old_node;
    delete new_node;

    if (parent->get_size() == parent->get_max_size()){
        auto res = split(parent);
        // 分裂出来的，如果是内部节点，还需要去维护其孩子
        BLinkNodeHandle *parent_right_bro = res.first;

        itemkey_t min_key = res.second;
        insert_into_parent(parent , min_key , parent_right_bro , trace);

        return ;
    }else {
        // 如果父亲不需要分裂了，那就释放父亲的锁，然后直接返回
        release_node(parent->get_page_no() , BPOperation::INSERT_OPERA);
        return ;
    }
    assert(false);
}

bool BLinkIndexHandle::checkIfDirectlyGetPage(const itemkey_t *key , Rid &result){
    Rid *rid;
    key2leaf_mtx.lock();
    auto it = key2leaf.find(*key);
    if (it != key2leaf.end()){
        page_id_t page_id = it->second;
        key2leaf_mtx.unlock();

        BLinkNodeHandle *tar_leaf = fetch_node(page_id , BPOperation::SEARCH_OPERA);
        assert(tar_leaf->is_leaf());
        if (tar_leaf->leaf_lookup(key , &rid)){
            result = *rid;
            recovery_observation::Emit("key2leaf", table_id, page_id, 1, 0, 0, "hit");
            release_node(tar_leaf->get_page_no() , BPOperation::SEARCH_OPERA);
            delete tar_leaf;
            return true;
        }else {
            recovery_observation::Emit("key2leaf", table_id, page_id, 0, 0, 0, "fallback");
            key2leaf_mtx.lock();
            key2leaf.erase(*key);   // 过期了，删掉
            key2leaf_mtx.unlock();
            release_node(tar_leaf->get_page_no() , BPOperation::SEARCH_OPERA);
            delete tar_leaf;
        }
    }else {
        key2leaf_mtx.unlock();
        recovery_observation::Emit("key2leaf", table_id, -1, 0, 0, 0, "miss");
    }

    return false;
}

bool BLinkIndexHandle::search(const itemkey_t *key , Rid &result){
    recovery_observation::LookupScope observation(table_id, *key, observation_generation_.load());
    if (checkIfDirectlyGetPage(key , result)){
        observation.Finish(true, result.page_no_, result.slot_no_, observation_generation_.load());
        return true;
    }
    
    Rid *rid;
    BLinkNodeHandle *leaf = find_leaf_for_search(key);
    bool exist = leaf->leaf_lookup(key , &rid);
    if (exist){
        key2leaf_mtx.lock();
        key2leaf[*key] = leaf->get_page_no();
        key2leaf_mtx.unlock();
        result = *rid;
    }
    release_node(leaf->get_page_no() , BPOperation::SEARCH_OPERA);
    delete leaf;
    observation.Finish(exist, exist ? result.page_no_ : -1, exist ? result.slot_no_ : -1, observation_generation_.load());
    return exist;
}

page_id_t BLinkIndexHandle::update_entry(const itemkey_t *key , const Rid &value){
    assert(value != INDEX_NOT_FOUND);
    std::vector<page_id_t> trace;
    // 其实写操作访问叶子节点的逻辑都一样，就不区分 insert 和 update 了
    BLinkNodeHandle *leaf = find_leaf_for_insert(key , trace);
    assert(leaf->is_leaf());

    int pos = leaf->lower_bound(key);

    // 先默认 update 时，key 一定存在
    assert(pos != leaf->get_size() && leaf->isIt(pos , key));

    Rid *rid = leaf->get_rid(pos);
    assert(*rid == INDEX_NOT_FOUND);
    *rid = value;

    release_node(leaf->get_page_no() , BPOperation::UPDATE_OPERA);

    page_id_t ret = leaf->get_page_no();
    delete leaf;
    return ret;
}

// 向 BLink 插入一个新的 pkey
page_id_t BLinkIndexHandle::insert_entry(const itemkey_t *key , const Rid &value){
    std::vector<page_id_t> trace;
    BLinkNodeHandle *leaf = find_leaf_for_insert(key , trace);
    assert(leaf->is_leaf());

    int pos = leaf->lower_bound(key);
    if (pos != leaf->get_size() && leaf->isIt(pos , key)){
        page_id_t ret = leaf->get_page_no();
        release_node(leaf->get_page_no() , BPOperation::INSERT_OPERA);
        delete leaf;
        return INVALID_PAGE_ID;
    }

    int old_size = leaf->get_size();
    int new_size = leaf->insert(key , value);
    assert(old_size != new_size);

    page_id_t ret = leaf->get_page_no();

    if (leaf->get_size() == leaf->get_max_size()){
        BLinkNodeHandle *bro = split(leaf).first;
        // 如果 bro 是最后一个叶子节点，那就更新 file_hdr 的 last_leaf。
        // 注意叶链终止于哨兵页 0（BP_LEAF_HEADER_PAGE_ID），不是 INVALID_PAGE_ID：
        // 原条件 (next_leaf == INVALID_PAGE_ID) 永假，last_leaf 从不更新（持久化为初始值）
        if (bro->get_next_leaf() == BP_LEAF_HEADER_PAGE_ID){
            Page *page = x_get_file_hdr();
            assert(file_hdr->last_leaf == leaf->get_page_no());
            file_hdr->last_leaf = bro->get_page_no();
            x_release_file_hdr(page);
        }
        insert_into_parent(leaf , *bro->get_key(0), bro , trace);
        return ret;
    }
    
    release_node(leaf->get_page_no() , BPOperation::INSERT_OPERA);
    delete leaf;
    return ret;
}

/*
    删除一个 key，这里是简化的版本，直接在叶子节点中把 key 给删了
    为什么不按照 B+ 树的做法，调整树的结构的原因是，BLink 不允许自上向下的加锁
    这样做没有正确性的问题，但是会有空间浪费的问题，pg 的做法是后台线程定期清理，后续可以优化下
*/
Rid BLinkIndexHandle::delete_entry(const itemkey_t *key){
    BLinkNodeHandle *leaf = find_leaf_for_delete(key);
    assert(leaf->is_leaf());

    if (leaf->get_size() == 0 || !leaf->need_delete(key)){
        release_node(leaf->get_page_no() , BPOperation::DELETE_OPERA);
        delete leaf;
        return {-1 , -1};
    }

    int pos = leaf->lower_bound(key);
    // 这里 ret 是一定存在的，因为前面已经检查了 leaf->need_delete
    Rid ret = *leaf->get_rid(pos);
    assert(ret.page_no_ != -1);
    leaf->remove(key);

    release_node(leaf->get_page_no() , BPOperation::DELETE_OPERA);
    delete leaf;
    return ret;
}
