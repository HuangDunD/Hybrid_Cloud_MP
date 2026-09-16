#include "fsm_tree.h"
#include <algorithm>
#include <exception>
#include <iostream>
#include <limits>
#include <queue>
#include "common.h"
#include "compute_server/server.h"

SecFSM::SecFSM(ComputeServer* compute_server, table_id_t table_id)
    : table_id_(table_id), initialized_(false) {
    server = compute_server;
    // 初始化元数据
    meta_.magic_number = 0x46534D54; // "FSMT"
    meta_.version = 1;
    meta_.total_heap_pages = 0;
    meta_.total_fsm_pages = 0;
    meta_.tree_height = 0;
    meta_.root_page_id = FSM_ROOT_PAGE_ID;
    meta_.next_fsm_page_id = FSM_ROOT_PAGE_ID - 1;
    meta_.leaves_per_page = LEAVES_PER_PAGE;
    meta_.children_per_page = CHILDREN_PER_PAGE;
    meta_.timestamp = 0;
    meta_.table_id = table_id; 
}

SecFSM::~SecFSM() {
    if (initialized_) {
        
    }
}

// ====================================================================
// 元数据：懒加载 + 快照
// ====================================================================

void SecFSM::ensure_initialized() {
    // call_once 保证并发下仅一个线程执行懒加载，其余线程同步等待；
    // call_once 返回后与初始化动作建立 happens-before，meta_ 可安全快照
    std::call_once(init_once_, [this]() {
        load_meta_page();
        std::lock_guard<std::mutex> lock(meta_mutex_);
        initialized_ = true;
    });
}

void SecFSM::load_meta_page() {
    Page* page = nullptr;
    if (SYSTEM_MODE == 0) {
        page = server->rpc_fetch_s_page(table_id_, FSM_META_PAGE_ID);
    } else {
        page = server->rpc_lazy_fetch_s_page(table_id_, FSM_META_PAGE_ID);
    }
    if (page == nullptr) {
        return; // 拉取失败：沿用默认 meta_（与原实现的容错行为一致）
    }
    {
        // deserialize_metadata 内部先校验 magic 再覆盖 meta_
        std::lock_guard<std::mutex> lock(meta_mutex_);
        deserialize_metadata(page->get_data(), PAGE_SIZE);
    }
    if (SYSTEM_MODE == 0) {
        server->rpc_release_s_page(table_id_, FSM_META_PAGE_ID);
    } else {
        server->rpc_lazy_release_s_page(table_id_, FSM_META_PAGE_ID);
    }
}

FSMMetaData SecFSM::snapshot_meta() const {
    std::lock_guard<std::mutex> lock(meta_mutex_);
    return meta_;
}

bool SecFSM::initialize(table_id_t table_id) {
    if (table_id != INVALID_TABLE_ID) {
        std::lock_guard<std::mutex> lock(meta_mutex_);
        meta_.table_id = table_id;
        assert(false);
    }

    // 懒加载 meta 页（call_once，并发安全；原实现在此处隐式依赖全局锁）
    ensure_initialized();
    return true;
}

// ====================================================================
// 页面 IO（副本语义）
// ====================================================================

bool SecFSM::sfetch_page(uint32_t page_id, FSMPageData& out) {
    Page* page = nullptr;
    if (SYSTEM_MODE == 0) {
        page = server->rpc_fetch_s_page(table_id_, page_id);
    } else {
        page = server->rpc_lazy_fetch_s_page(table_id_, page_id);
    }
    if (page == nullptr) {
        return false;
    }
    // S 锁多读者：各自反序列化到调用方的独立局部副本，先拷贝后释放锁，
    // 不再触碰任何共享缓存（原实现写入共享 fsm_pages_，S 路径并发即数据竞争）
    bool ok = deserialize_page(out, page->get_data(), PAGE_SIZE);
    if (SYSTEM_MODE == 0) {
        server->rpc_release_s_page(table_id_, page_id);
    } else {
        server->rpc_lazy_release_s_page(table_id_, page_id);
    }
    return ok;
}

SecFSM::XPageGuard::XPageGuard(SecFSM* fsm, uint32_t page_id)
    : fsm_(fsm), page_id_(page_id), page_(nullptr) {
    if (SYSTEM_MODE == 0) {
        page_ = fsm_->server->rpc_fetch_x_page(fsm_->table_id_, page_id_);
    } else {
        page_ = fsm_->server->rpc_lazy_fetch_x_page(fsm_->table_id_, page_id_);
    }
    if (page_ == nullptr) {
        return;
    }
    if (!fsm_->deserialize_page(data_, page_->get_data(), PAGE_SIZE)) {
        // 数据无效：放弃修改并立即释放 X 锁
        fsm_->xunlock_page(page_id_);
        page_ = nullptr;
    }
}

SecFSM::XPageGuard::~XPageGuard() {
    if (page_ != nullptr) {
        fsm_->xunlock_page(page_id_);
    }
}

void SecFSM::xwrite_page(FSMPageData& data, Page* page) {
    serialize_page(data, page->get_data(), PAGE_SIZE);
}

void SecFSM::xunlock_page(uint32_t page_id) {
    if (SYSTEM_MODE == 0) {
        server->rpc_release_x_page(table_id_, page_id);
    } else {
        server->rpc_lazy_release_x_page(table_id_, page_id);
    }
}

// ====================================================================
// 搜索：find_free_page
// ====================================================================

uint32_t SecFSM::find_free_page(uint32_t min_space_needed) {
    ensure_initialized();

    FSMMetaData m = snapshot_meta();
    uint8_t required_category = space_to_category(min_space_needed);

    // 从根页面开始搜索。读路径全程每次只持单页 S 锁（fetch→释放→下一
    // 层），不再持全局锁：多个事务的搜索完全并行，读到的聚合值即使
    // 短暂陈旧也无害（FSM 是启发式结构，分配失败由上层重试兜底）
    return search_from_page(m.root_page_id, required_category);
}

uint32_t SecFSM::search_from_page(uint32_t fsm_page_id, uint8_t required_category) {
    thread_local std::mt19937 rng{std::random_device{}()};

    FSMPageData page;
    if (!sfetch_page(fsm_page_id, page)) {
        return 0xFFFFFFFF;
    }

    if (page.header.page_type == FSMPageType::LEAF_PAGE) {
        // 在叶子页面中搜索具体的堆页面
        return search_in_leaf_page(page, required_category);
    }

    // 内部页面：本页聚合值（页根）都不满足则直接失败
    if (page.nodes.empty() || page.nodes[0].get_value() < required_category) {
        return 0xFFFFFFFF;
    }

    ensure_child_vector(page);
    uint32_t child_count = page.header.child_count;

    // 随机起点扫描满足条件的子页槽位：避免所有事务都涌向最左侧子树
    // 造成热点聚集（原实现固定从 offset 0 开始）
    uint32_t start = (child_count > 1) ? (rng() % child_count) : 0;
    for (uint32_t k = 0; k < child_count; ++k) {
        uint32_t offset = start + k;
        if (offset >= child_count) {
            offset -= child_count;
        }

        uint32_t node_index = page.header.leaf_start + offset;
        if (node_index >= page.header.node_count) {
            break;
        }
        if (page.nodes[node_index].get_value() < required_category) {
            continue;
        }

        uint32_t child_page_id = (offset < page.child_page_ids.size())
                                     ? page.child_page_ids[offset]
                                     : page.header.first_child_page + offset;

        uint32_t result = search_from_page(child_page_id, required_category);
        if (result != 0xFFFFFFFF) {
            return result;
        }
        // 该子页聚合值可能已陈旧（并发更新所致）：继续尝试下一个槽位
    }
    return 0xFFFFFFFF;
}

uint32_t SecFSM::search_in_leaf_page(const FSMPageData& leaf_page, uint8_t required_category) const {
    // 在叶子页面内部的二叉树中搜索（纯内存操作，作用于调用方局部副本）
    thread_local std::mt19937 rng{std::random_device{}()};

    uint32_t current_index = 0;

    while (!is_leaf_node(leaf_page, current_index)) {
        uint32_t left_child = get_left_child(current_index);
        uint32_t right_child = get_right_child(current_index);

        bool left_ok = left_child < leaf_page.header.node_count &&
                       leaf_page.nodes[left_child].get_value() >= required_category;
        bool right_ok = right_child < leaf_page.header.node_count &&
                        leaf_page.nodes[right_child].get_value() >= required_category;

        if (left_ok && right_ok) {
            // 两个子树都满足：随机选择，分散热点（原实现固定优先左子树）
            current_index = (rng() & 1) ? left_child : right_child;
        } else if (left_ok) {
            current_index = left_child;
        } else if (right_ok) {
            current_index = right_child;
        } else {
            // 两个子树都没有足够空间
            return 0xFFFFFFFF;
        }
    }

    // 找到叶子节点，计算对应的堆页面ID
    uint32_t heap_offset = current_index - leaf_page.header.leaf_start;
    uint32_t heap_page_id = leaf_page.header.first_heap_page + heap_offset;
    return heap_page_id;
}

// ====================================================================
// 更新：update_page_space（latch coupling 向上传播）
// ====================================================================

uint32_t SecFSM::update_page_space(uint32_t page_id, uint32_t free_space) {
    ensure_initialized();

    // O(1) 定位叶子页（原实现自根全树线性遍历，定位一次就要 O(FSM页数)
    // 次 S 锁 RPC，且全程持全局锁）
    uint32_t leaf_page_id = locate_leaf_page(page_id);
    if (leaf_page_id == 0xFFFFFFFF) {
        std::cerr << "Error: Could not find leaf page for heap page " << page_id << std::endl;
        return 0xFFFFFFFF;
    }

    XPageGuard leaf_guard(this, leaf_page_id);
    if (!leaf_guard.valid()) {
        return 0xFFFFFFFF;
    }
    FSMPageData& leaf = leaf_guard.data();

    // 计算在叶子页面内的节点索引
    uint32_t heap_offset = page_id - leaf.header.first_heap_page;
    uint32_t leaf_index = leaf.header.leaf_start + heap_offset;

    if (leaf_index >= leaf.header.node_count) {
        std::cerr << "Error: Leaf index out of range: " << leaf_index
                  << " >= " << leaf.header.node_count << std::endl;
        return 0xFFFFFFFF; // guard 析构自动释放 X 锁
    }

    uint8_t new_category = space_to_category(free_space);
    if (leaf.nodes[leaf_index].get_value() == new_category) {
        return 0xFFFFFFFF; // 空间类别未改变，无需更新（调用方无需记日志）
    }

    // 记录旧类别对应的空间值，供调用方生成 FSMUPDATE 日志的 undo 依据
    uint32_t old_free_space = category_to_space(leaf.nodes[leaf_index].get_value());

    // 持叶子页 X 锁一次完成：改值 → 页内向上传播 → 回写
    // （原实现先释放 X 锁再由 update_node_value 重新加锁，锁开销翻倍）
    leaf.nodes[leaf_index].set_value(new_category);
    bool root_changed = propagate_within_page(leaf, leaf_index);
    leaf_guard.commit();

    // 页根值变化才向父页传播。latch coupling：叶子页 X 锁保持到上层
    // 传播完成后才释放，父页槽位更新与子页根值变化被同一条锁链保护，
    // 消除并发传播间的丢失更新
    if (root_changed && leaf.header.parent_page != 0) {
        propagate_up(leaf_page_id, leaf);
    }
    return old_free_space;
}

bool SecFSM::propagate_within_page(FSMPageData& page, uint32_t node_index) {
    // 从 node_index 向上传播到页根；返回页根(nodes[0])值是否被修改。
    // 作用于调用方持有的 X 锁页面副本，纯内存操作
    bool root_changed = false;
    while (node_index > 0) {
        uint32_t parent_index = get_parent_index(node_index);
        uint32_t left_child = get_left_child(parent_index);
        uint32_t right_child = get_right_child(parent_index);

        // 父节点的新值 = 两个子节点的最大值
        uint8_t left_value = (left_child < page.header.node_count) ?
                            page.nodes[left_child].get_value() : 0;
        uint8_t right_value = (right_child < page.header.node_count) ?
                             page.nodes[right_child].get_value() : 0;
        uint8_t new_parent_value = std::max(left_value, right_value);

        // 父节点值不变：更上层不受影响，停止传播
        if (new_parent_value == page.nodes[parent_index].get_value()) {
            break;
        }

        page.nodes[parent_index].set_value(new_parent_value);
        if (parent_index == 0) {
            root_changed = true;
        }
        node_index = parent_index;
    }
    return root_changed;
}

void SecFSM::propagate_up(uint32_t child_page_id, const FSMPageData& child) {
    // 前置条件：child 的修改已 commit 回其页面，但 child 的 X 锁仍由调
    // 用方持有。锁获取顺序恒为“子页→父页”（低层→高层），全路径单向、
    // 无环，不会死锁；父页更新完成后回溯（自根向叶）逐层释放。
    uint32_t parent_page_id = child.header.parent_page;
    if (parent_page_id == 0) {
        return; // child 即根页
    }
    uint8_t child_root_value = child.nodes[0].get_value();

    XPageGuard parent_guard(this, parent_page_id);
    if (!parent_guard.valid()) {
        return; // 父页获取失败：放弃上行（FSM 允许暂时陈旧，子页已提交）
    }
    FSMPageData& parent = parent_guard.data();

    ensure_child_vector(parent);
    uint32_t child_offset = find_child_index(parent, child_page_id);
    if (child_offset == std::numeric_limits<uint32_t>::max()) {
        return; // guard 析构自动释放父页锁
    }

    uint32_t parent_node_index = parent.header.leaf_start + child_offset;
    if (parent_node_index >= parent.header.node_count) {
        return;
    }

    if (parent.nodes[parent_node_index].get_value() == child_root_value) {
        return; // 父页槽位已是最新值，无需继续上行
    }

    parent.nodes[parent_node_index].set_value(child_root_value);
    bool root_changed = propagate_within_page(parent, parent_node_index);
    parent_guard.commit();

    if (root_changed && parent.header.parent_page != 0) {
        // 仍持本层锁继续上行（递归），锁链保持“低层→高层”单向顺序
        propagate_up(parent_page_id, parent);
    }
    // parent_guard 析构：释放本层父页 X 锁
}

// ====================================================================
// 树导航：叶子页定位
// ====================================================================

uint32_t SecFSM::locate_leaf_page(uint32_t heap_page_id) {
    FSMMetaData m = snapshot_meta();

    // 快速路径：build_fsm_tree 中叶子页自 FSM_ROOT_PAGE_ID 起按序连续分
    // 配，且每个叶子页管理连续的 leaves_per_page 个堆页面，故可直接算
    // 术定位；fetch 后校验页面头部，确保布局假设成立
    if (m.leaves_per_page > 0 && heap_page_id < m.total_heap_pages) {
        uint32_t guess = FSM_ROOT_PAGE_ID + heap_page_id / m.leaves_per_page;
        FSMPageData tmp;
        if (sfetch_page(guess, tmp) &&
            tmp.header.page_type == FSMPageType::LEAF_PAGE &&
            heap_page_id >= tmp.header.first_heap_page &&
            heap_page_id < tmp.header.first_heap_page + tmp.header.heap_pages_count) {
            return guess;
        }
    }

    // 回退路径：自根向下遍历定位（O(树高)），保证布局变化时仍然正确
    return search_leaf_page_from(m.root_page_id, heap_page_id);
}

uint32_t SecFSM::search_leaf_page_from(uint32_t fsm_page_id, uint32_t heap_page_id) {
    FSMPageData page;
    if (!sfetch_page(fsm_page_id, page)) {
        return 0xFFFFFFFF;
    }

    if (page.header.page_type == FSMPageType::LEAF_PAGE) {
        // 检查这个叶子页面是否管理目标堆页面
        if (heap_page_id >= page.header.first_heap_page && 
            heap_page_id < page.header.first_heap_page + page.header.heap_pages_count) {
            return fsm_page_id;
        }
        return 0xFFFFFFFF;
    } else {
        // 在内部页面中查找合适的子页面
        ensure_child_vector(page);
        for (uint32_t child_page_id : page.child_page_ids) {
            uint32_t result = search_leaf_page_from(child_page_id, heap_page_id);
            if (result != 0xFFFFFFFF) {
                return result;
            }
        }
        return 0xFFFFFFFF;
    }
}

uint32_t SecFSM::get_page_space(uint32_t page_id) {
    ensure_initialized();

    FSMMetaData m = snapshot_meta();
    if (page_id >= m.total_heap_pages) {
        return 0;
    }

    uint32_t leaf_page_id = locate_leaf_page(page_id);
    if (leaf_page_id == 0xFFFFFFFF) {
        return 0;
    }

    FSMPageData leaf;
    if (!sfetch_page(leaf_page_id, leaf)) {
        return 0;
    }

    uint32_t heap_offset = page_id - leaf.header.first_heap_page;
    uint32_t leaf_index = leaf.header.leaf_start + heap_offset;

    if (leaf_index >= leaf.header.node_count) {
        return 0;
    }

    return category_to_space(leaf.nodes[leaf_index].get_value());
}

// ====================================================================
// 遗留路径：树构建 / 扩展（计算端不在事务热路径调用，保持原实现）
// ====================================================================

bool SecFSM::build_fsm_tree() {
   // fsm_pages_.clear();
    meta_.total_fsm_pages = 0;
    meta_.tree_height = 0;

    if (meta_.total_heap_pages == 0) {
        return false;
    }
    
    // 计算需要的叶子页面数
    uint32_t leaf_pages_needed = (meta_.total_heap_pages + LEAVES_PER_PAGE - 1) / LEAVES_PER_PAGE;
    
    // 创建叶子页面
    std::vector<uint32_t> current_level_pages;
    for (uint32_t i = 0; i < leaf_pages_needed; ++i) {
        uint32_t first_heap_page = i * LEAVES_PER_PAGE;
        uint32_t heap_pages_count = std::min(LEAVES_PER_PAGE, meta_.total_heap_pages - first_heap_page);
        
        uint32_t leaf_page_id = allocate_fsm_page();
        if (leaf_page_id == 0) {
            return false;
        }
        
        FSMPageData leaf_page;
        leaf_page.initialize_leaf_page(leaf_page_id, 0, 0, first_heap_page, heap_pages_count);
        fsm_pages_[leaf_page_id] = leaf_page;
        current_level_pages.push_back(leaf_page_id);
    }
    meta_.tree_height = 1; // 至少有一层叶子页面
    
    // 自底向上构建内部节点层
    uint32_t current_level = 1;
    while (current_level_pages.size() > 1) {
        std::vector<uint32_t> next_level_pages;
        uint32_t pages_in_level = current_level_pages.size();
        // 将当前层的页面分组，每组创建父页面
        for (uint32_t i = 0; i < pages_in_level; i += CHILDREN_PER_PAGE) {
            uint32_t group_size = std::min(CHILDREN_PER_PAGE, pages_in_level - i);
            std::vector<uint32_t> child_pages(current_level_pages.begin() + i, 
                                             current_level_pages.begin() + i + group_size);
            
            uint32_t parent_page_id = allocate_fsm_page();
            if (parent_page_id == 0) {
                std::cout << "333"<< std::endl;
                return false;
            }
            
            if (!create_internal_pages(child_pages, parent_page_id, current_level)) {
                std::cout << "444"<< std::endl;
                return false;
            }
            
            next_level_pages.push_back(parent_page_id);
        }
        
        current_level_pages = next_level_pages;
        meta_.tree_height++;
        current_level++;
    }
    
    // 设置根页面
    if (current_level_pages.size() == 1) {
        meta_.root_page_id = current_level_pages[0];
    } else {
        std::cerr << "Error: Expected exactly one root page, got " 
                  << current_level_pages.size() << std::endl;
        return false;
    }
    
    return true;
}

bool SecFSM::create_internal_pages(const std::vector<uint32_t>& child_pages, uint32_t parent_page_id, uint32_t level) {
    if (child_pages.empty()) {
        return false;
    }
    
    FSMPageData internal_page;
    internal_page.initialize_internal_page(parent_page_id, 0, level, child_pages);
    //fsm_pages_[parent_page_id] = internal_page;
    
    // 设置子页面的父指针
    for (uint32_t child_page_id : child_pages) {
        auto it = fsm_pages_.find(child_page_id);
        if (it != fsm_pages_.end()) {
            it->second.header.parent_page = parent_page_id;
            it->second.is_dirty = true;
        }
    }
    
    return true;
}

bool SecFSM::extend(uint32_t additional_pages) {
    std::unique_lock<std::mutex> lock(mutex_);
    (void)additional_pages;
    lock.unlock();
    return true;
}

uint32_t SecFSM::allocate_fsm_page() {
    uint32_t page_id = meta_.next_fsm_page_id + 1;
    if (page_id < FSM_ROOT_PAGE_ID) {
        page_id = FSM_ROOT_PAGE_ID;
    }
    meta_.next_fsm_page_id = page_id;
    meta_.total_fsm_pages++;
    return page_id;
}

// ====================================================================
// 序列化/反序列化实现（页面级，操作均为调用方局部副本）
// ====================================================================

bool SecFSM::serialize_metadata(char* buffer, uint32_t size) {
    if (size < sizeof(FSMMetaData)) {
        return false;
    }
    
    std::memcpy(buffer, &meta_, sizeof(FSMMetaData));
    return true;
}

bool SecFSM::deserialize_metadata(const char* buffer, uint32_t size) {
    if (size < sizeof(FSMMetaData)) {
        return false;
    }
    
    FSMMetaData loaded_meta;
    std::memcpy(&loaded_meta, buffer, sizeof(FSMMetaData));
    
    if (loaded_meta.magic_number != 0x46534D54) {
        return false;
    }
    
    meta_ = loaded_meta;
    return true;
}

bool SecFSM::serialize_page(FSMPageData& page_data, char* buffer, uint32_t size) {
    if (size < PAGE_SIZE) {
        return false;
    }
    
    // 序列化头部
    if (page_data.header.page_type == FSMPageType::INTERNAL_PAGE) {
        ensure_child_vector(page_data);
        page_data.header.child_count = static_cast<uint32_t>(page_data.child_page_ids.size());
        page_data.header.first_child_page = page_data.child_page_ids.empty() ? 0 : page_data.child_page_ids.front();
    } else {
        page_data.child_page_ids.clear();
        page_data.header.child_count = 0;
        page_data.header.first_child_page = 0;
    }

    size_t child_bytes = (page_data.header.page_type == FSMPageType::INTERNAL_PAGE)
                             ? static_cast<size_t>(page_data.header.child_count) * sizeof(uint32_t)
                             : 0;
    size_t node_region_capacity = (size > sizeof(FSMPageHeader) + child_bytes)
                                      ? size - sizeof(FSMPageHeader) - child_bytes
                                      : 0;
    if (page_data.nodes.size() > node_region_capacity) {
        std::cerr << "FSM serialization error: node data exceeds page capacity" << std::endl;
        return false;
    }

    page_data.header.magic_number = 0x46535047;
    page_data.header.timestamp = static_cast<uint64_t>(time(nullptr));
    std::memcpy(buffer, &page_data.header, sizeof(FSMPageHeader));
    
    // 序列化节点数据
    char* node_data = buffer + sizeof(FSMPageHeader);
    size_t node_bytes = std::min(page_data.nodes.size(), node_region_capacity);
    for (size_t i = 0; i < node_bytes; ++i) {
        node_data[i] = static_cast<char>(page_data.nodes[i].get_value());
        page_data.nodes[i].clear_dirty();
    }

    if (page_data.header.page_type == FSMPageType::INTERNAL_PAGE && page_data.header.child_count > 0) {
        char* child_ptr = node_data + node_bytes;
        std::memcpy(child_ptr, page_data.child_page_ids.data(), child_bytes);
    }
    
    return true;
}

bool SecFSM::deserialize_page(FSMPageData& page_data, const char* buffer, uint32_t size) {
    if (size < sizeof(FSMPageHeader)) {
        return false;
    }
    
    // 反序列化头部
    FSMPageHeader header;
    std::memcpy(&header, buffer, sizeof(FSMPageHeader));
    
    if (header.magic_number != 0x46535047) {
        return false;
    }
    
    page_data.header = header;
    
    // 反序列化节点数据
    const size_t payload_bytes = size - sizeof(FSMPageHeader);
    const size_t child_bytes_needed = static_cast<size_t>(header.child_count) * sizeof(uint32_t);
    bool has_child_blob = (header.page_type == FSMPageType::INTERNAL_PAGE) &&
                         (payload_bytes >= header.node_count + child_bytes_needed);

    size_t node_bytes_limit = has_child_blob ? (payload_bytes - child_bytes_needed) : payload_bytes;
    const char* node_data = buffer + sizeof(FSMPageHeader);
    page_data.nodes.resize(std::min(static_cast<size_t>(header.node_count), node_bytes_limit));
    
    for (size_t i = 0; i < page_data.nodes.size(); ++i) {
        page_data.nodes[i] = FSMNode(static_cast<SpaceCategory>(static_cast<uint8_t>(node_data[i])));
    }

    const size_t node_bytes = page_data.nodes.size();
    size_t remaining = payload_bytes > node_bytes ? payload_bytes - node_bytes : 0;
    page_data.child_page_ids.clear();
    if (header.page_type == FSMPageType::INTERNAL_PAGE && header.child_count > 0) {
        if (has_child_blob && remaining >= child_bytes_needed) {
            page_data.child_page_ids.resize(header.child_count);
            const char* child_ptr = node_data + node_bytes;
            std::memcpy(page_data.child_page_ids.data(), child_ptr, child_bytes_needed);
        } else {
            page_data.child_page_ids.resize(header.child_count);
            for (uint32_t i = 0; i < header.child_count; ++i) {
                page_data.child_page_ids[i] = header.first_child_page + i;
            }
        }
    }

    page_data.is_loaded = true;
    page_data.is_dirty = false;
    return true;
}

// ====================================================================
// 工具函数
// ====================================================================

uint8_t SecFSM::space_to_category(uint32_t free_space) const {
    if (free_space == 0) return static_cast<uint8_t>(SpaceCategory::NO_SPACE);
    if (free_space >= PAGE_SIZE * 9 / 10) return static_cast<uint8_t>(SpaceCategory::EMPTY);
    if (free_space >= PAGE_SIZE * 2 / 3) return static_cast<uint8_t>(SpaceCategory::ALMOST_EMPTY);
    if (free_space >= PAGE_SIZE / 3) return static_cast<uint8_t>(SpaceCategory::HALF_FULL);
    if (free_space >= PAGE_SIZE / 10) return static_cast<uint8_t>(SpaceCategory::ALMOST_FULL);
    // 有小于 10% 但大于 0 的剩余空间
    return static_cast<uint8_t>(SpaceCategory::ALMOST_FULL);
}

uint32_t SecFSM::category_to_space(uint8_t category) const {
    switch (static_cast<SpaceCategory>(category)) {
        case SpaceCategory::EMPTY: return PAGE_SIZE * 9 / 10;
        case SpaceCategory::ALMOST_EMPTY: return PAGE_SIZE * 2 / 3;
        case SpaceCategory::HALF_FULL: return PAGE_SIZE / 3;
        case SpaceCategory::ALMOST_FULL: return PAGE_SIZE / 10;
        case SpaceCategory::NO_SPACE: return 0;
        default: return 0;
    }
}

uint32_t SecFSM::get_parent_index(uint32_t child) const {
    return (child - 1) / 2;
}

uint32_t SecFSM::get_left_child(uint32_t parent) const {
    return 2 * parent + 1;
}

uint32_t SecFSM::get_right_child(uint32_t parent) const {
    return 2 * parent + 2;
}

bool SecFSM::is_leaf_node(const FSMPageData& page_data, uint32_t index) const {
    return index >= page_data.header.leaf_start;
}

void SecFSM::ensure_child_vector(FSMPageData& page_data) {
    if (page_data.header.page_type != FSMPageType::INTERNAL_PAGE) {
        page_data.child_page_ids.clear();
        return;
    }
    if (!page_data.child_page_ids.empty()) {
        return;
    }
    page_data.child_page_ids.reserve(page_data.header.child_count);
    for (uint32_t i = 0; i < page_data.header.child_count; ++i) {
        page_data.child_page_ids.push_back(page_data.header.first_child_page + i);
    }
}

uint32_t SecFSM::find_child_index(FSMPageData& parent_page, uint32_t child_page_id) const {
    if (parent_page.header.page_type != FSMPageType::INTERNAL_PAGE) {
        return std::numeric_limits<uint32_t>::max();
    }
    for (uint32_t i = 0; i < parent_page.child_page_ids.size(); ++i) {
        if (parent_page.child_page_ids[i] == child_page_id) {
            return i;
        }
    }
    if (parent_page.child_page_ids.empty() && parent_page.header.child_count > 0) {
        if (child_page_id >= parent_page.header.first_child_page &&
            child_page_id < parent_page.header.first_child_page + parent_page.header.child_count) {
            return child_page_id - parent_page.header.first_child_page;
        }
    }
    return std::numeric_limits<uint32_t>::max();
}

// ====================================================================
// 调试信息
// ====================================================================

void SecFSM::print_tree_structure() {
    // 结构打印保留为空（原实现整体注释）
}

void SecFSM::print_debug_info() {
    ensure_initialized();
    std::lock_guard<std::mutex> lock(meta_mutex_);
    
    if (!initialized_) {
        std::cout << "FSM not initialized" << std::endl;
        return;
    }
    
    std::cout << "=== FSM Debug Info ===" << std::endl;
    std::cout << "Total heap pages: " << meta_.total_heap_pages << std::endl;
    std::cout << "Total fsm pages: " << meta_.total_fsm_pages << std::endl;
    std::cout << "Tree height: " << meta_.tree_height << std::endl;
    std::cout << "Root page ID: " << meta_.root_page_id << std::endl;
    std::cout << "Next FSM page ID: " << meta_.next_fsm_page_id << std::endl;
    std::cout << "Cached FSM pages: " << fsm_pages_.size() << std::endl;
    
    // 统计各类型页面数量
    uint32_t leaf_count = 0, internal_count = 0;
    for (const auto& pair : fsm_pages_) {
        if (pair.second.header.page_type == FSMPageType::LEAF_PAGE) {
            leaf_count++;
        } else {
            internal_count++;
        }
    }
    
    std::cout << "Leaf pages: " << leaf_count << std::endl;
    std::cout << "Internal pages: " << internal_count << std::endl;
}
