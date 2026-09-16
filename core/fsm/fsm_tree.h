#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <mutex>
#include <random>
#include <unordered_map>
#include <queue>
#include <cmath>
#include "common.h"
#include "core/storage/buffer/storage_bufferpool.h"
#include <iostream>

class ComputeServer;

// 常量定义
constexpr uint32_t FSM_META_PAGE_ID = 1;
constexpr uint32_t FSM_ROOT_PAGE_ID = 2;

// 每个FSM页面管理的最大叶子节点数
constexpr uint32_t LEAVES_PER_PAGE = 1024;
// 每个内部FSM页面管理的最大子FSM页面数
constexpr uint32_t CHILDREN_PER_PAGE = 512;

// FSM页面类型
enum class FSMPageType : uint8_t {
    META_PAGE = 0,      // 元数据页面
    INTERNAL_PAGE = 1,  // 内部节点页面
    LEAF_PAGE = 2       // 叶子节点页面
};

// 空间分类
enum class SpaceCategory : uint8_t {
    NO_SPACE = 0,       // 0 bytes 空闲（没有剩余空间）
    ALMOST_FULL = 64,   // 很少空间（>0 且 <= 10% 空闲）
    HALF_FULL = 128,    // 中等空间（约 33%-66%）
    ALMOST_EMPTY = 192, // 较多空间（大约 66%-90%）
    EMPTY = 255         // 几乎全空（>90% 空闲）
};

// FSM元数据
struct FSMMetaData {
    uint32_t magic_number;          // 标识
    uint32_t version;               // 版本号
    uint32_t total_heap_pages;      // 管理的页面总数
    uint32_t total_fsm_pages;       // FSM页面总数
    uint32_t tree_height;           // 树的总高度
    uint32_t root_page_id;          // 根页面ID
    uint32_t next_fsm_page_id;      // 下一个可用的FSM页面ID
    uint32_t leaves_per_page;       // 每页叶子节点数
    uint32_t children_per_page;     // 每页子页面数
    uint64_t timestamp;             // 最后更新时间戳，之后换成llsn
    table_id_t table_id;            // 关联的表ID
    uint8_t  reserved[64];          // 保留区域
};

// FSM页面头
struct FSMPageHeader {
    uint32_t magic_number;          // 标识
    uint32_t page_id;               // 页面ID
    FSMPageType page_type;          // 页面类型
    uint32_t parent_page;           // 父页面ID
    uint32_t level;                 // 在树中的层级 (0=叶子层)
    
    // 对于叶子页面
    uint32_t first_heap_page;       // 管理的第一个堆页面ID
    uint32_t heap_pages_count;      // 管理的堆页面数量
    
    // 对于内部页面
    uint32_t first_child_page;      // 第一个子页面ID
    uint32_t child_count;           // 子页面数量
    
    // 页面内部结构
    uint32_t node_count;            // 节点总数
    uint32_t leaf_start;            // 叶子节点起始索引
    uint64_t timestamp;             // 最后更新时间戳
};

// FSM树节点
class FSMNode {
public:
    FSMNode() : value(static_cast<uint8_t>(SpaceCategory::NO_SPACE)), is_dirty(false) {}
    explicit FSMNode(SpaceCategory cat) : value(static_cast<uint8_t>(cat)), is_dirty(false) {}
    
    uint8_t get_value() const { return value; }
    void set_value(uint8_t val) { 
        if (value != val) {
            value = val; 
            is_dirty = true;
        }
    }
    void set_value(SpaceCategory cat) { set_value(static_cast<uint8_t>(cat)); }
    bool is_dirty_node() const { return is_dirty; }
    void clear_dirty() { is_dirty = false; }

private:
    uint8_t value;      // 空间值 (0-255)
    bool is_dirty;      // 脏标记
};

// FSM页面数据
class FSMPageData {
public:
    FSMPageHeader header;
    std::vector<FSMNode> nodes;
    std::vector<uint32_t> child_page_ids;
    bool is_loaded;
    bool is_dirty;
    
    FSMPageData() : is_loaded(false), is_dirty(false) {
        header.magic_number = 0x46535047; 
    }
    
    // 初始化叶子页面
    void initialize_leaf_page(uint32_t page_id, uint32_t parent_id, uint32_t level,
                             uint32_t first_heap_page, uint32_t heap_pages_count) {
        header.page_id = page_id;
        header.page_type = FSMPageType::LEAF_PAGE;
        header.parent_page = parent_id;
        header.level = level;
        header.first_heap_page = first_heap_page;
        header.heap_pages_count = heap_pages_count;
        header.first_child_page = 0;
        header.child_count = 0;
        
        calculate_leaf_structure();
        nodes.resize(header.node_count, FSMNode(SpaceCategory::EMPTY));
        recalc_in_page_aggregates();
        is_loaded = true;
        is_dirty = true;
    }
    
    // 初始化内部页面
    void initialize_internal_page(uint32_t page_id, uint32_t parent_id, uint32_t level,
                                 const std::vector<uint32_t>& child_pages) {
        header.page_id = page_id;
        header.page_type = FSMPageType::INTERNAL_PAGE;
        header.parent_page = parent_id;
        header.level = level;
        header.first_heap_page = 0;
        header.heap_pages_count = 0;
        header.child_count = static_cast<uint32_t>(child_pages.size());
        header.first_child_page = child_pages.empty() ? 0 : child_pages.front();
        
        calculate_internal_structure();
        nodes.resize(header.node_count, FSMNode(SpaceCategory::EMPTY));
        recalc_in_page_aggregates();
        child_page_ids = child_pages;
        is_loaded = true;
        is_dirty = true;
    }
    
private:
    // 槽数非 2 的幂时，页内二叉树存在“悬空”内部节点（孩子索引越界）。
    // 它们没有任何叶子后代，语义上应视为无空间（NO_SPACE）；若保持初始
    // 的 EMPTY(255)，会虚高祖先聚合值并诱导搜索走进死路分支。
    // 建树时自底向上重算页内聚合（父 = max(有效孩子, 缺失按 0)），
    // 同时将悬空节点归零
    void recalc_in_page_aggregates() {
        for (int p = static_cast<int>(header.leaf_start) - 1; p >= 0; --p) {
            uint32_t l = 2 * p + 1, r = 2 * p + 2;
            uint8_t lv = (l < header.node_count) ? nodes[l].get_value() : 0;
            uint8_t rv = (r < header.node_count) ? nodes[r].get_value() : 0;
            nodes[p].set_value(std::max(lv, rv));
        }
    }
    
    void calculate_leaf_structure() {
        // 计算叶子页面内部的二叉树结构
        uint32_t leaves_needed = header.heap_pages_count;
        uint32_t height = 1;
        uint32_t max_leaves = 1;
        
        // 找到能容纳所有叶子节点的最小完全二叉树高度
        while (max_leaves < leaves_needed) {
            height++;
            max_leaves = (1 << (height - 1));
        }
        
        header.leaf_start = (1 << (height - 1)) - 1;
        header.node_count = header.leaf_start + leaves_needed;
    }
    
    void calculate_internal_structure() {
        // 内部页面也使用二叉树结构，但叶子节点指向子页面的根节点值
        uint32_t leaves_needed = header.child_count;
        uint32_t height = 1;
        uint32_t max_leaves = 1;
        
        while (max_leaves < leaves_needed) {
            height++;
            max_leaves = (1 << (height - 1));
        }
        
        header.leaf_start = (1 << (height - 1)) - 1;
        header.node_count = header.leaf_start + leaves_needed;
    }
};


class SecFSM {
public:
    explicit SecFSM(ComputeServer* compute_server, table_id_t table_id);
    ~SecFSM();
    
    // 初始化FSM树
    bool initialize(table_id_t table_id);
    
    // 查找空闲页面
    uint32_t find_free_page(uint32_t min_space_needed);
    
    // 更新页面空间信息
    // 返回值：UINT32_MAX 表示空间类别未变化或更新失败（调用方无需记日志）；
    //         否则返回更新前的空间估计值（类别还原），供 FSMUPDATE 日志 undo 使用
    uint32_t update_page_space(uint32_t page_id, uint32_t free_space);
    
    // 获取页面空间信息
    uint32_t get_page_space(uint32_t page_id);
    
    
    // 扩展FSM以支持更多堆页面
    bool extend(uint32_t additional_pages);
    
    // 调试信息
    void print_tree_structure();
    void print_debug_info();

    // ====================================================================
    // 并发设计说明（原实现：单把 std::mutex 串行化全部 FSM 操作，且持锁
    // 期间完成整条路径的多次同步 RPC —— 并发度为 1）：
    //
    // 1. 页面数据副本化：S/X 锁 fetch 到的页面一律反序列化到调用方的
    //    局部 FSMPageData，不再写入共享的 fsm_pages_ 缓存，也不再用
    //    成员 pageinuse 回写，彻底消除跨线程共享可变状态；
    // 2. 页面互斥完全由底层页面锁（rpc_fetch_s/x_page + release）保证：
    //    S 路径多读者并发读各自独立副本，X 路径独占页面，与 BLink 索
    //    引使用同一套并发模型；
    // 3. meta_ 用 meta_mutex_ 保护（临界区纯内存、无 RPC），meta 页懒
    //    加载由 std::call_once 保证全局仅执行一次；
    // 4. 更新传播采用 latch coupling：自叶向根逐层“先持子页 X 锁、再
    //    取父页 X 锁”，锁获取顺序恒为低层→高层（单向，无环，不会死
    //    锁），父页槽位更新在子页锁保护下进行，杜绝并发传播的丢失更
    //    新；回溯时自根向叶逐层释放锁；
    // 5. 多数更新因“父值不变早停”不会上行到根；不同叶子页/子树的读
    //    与写完全并行，仅在上层页面短暂串行。
    // ====================================================================

private:
    // ---------------- 元数据 ----------------
    void ensure_initialized();            // call_once 懒加载 meta 页
    void load_meta_page();                // S 锁拉取并反序列化 meta 页
    FSMMetaData snapshot_meta() const;    // 拷贝一份元数据快照（调用方持局部快照工作）

    // ---------------- 页面 IO（副本语义，无共享可变状态） ----------------
    // S 锁读：fetch → 反序列化到 out → 释放；多个读者各自持有独立副本
    bool sfetch_page(uint32_t page_id, FSMPageData& out);

    // 将局部副本序列化回 X 锁页面（不释放锁，供 latch coupling 持锁上行）
    void xwrite_page(FSMPageData& data, Page* page);

    // 释放页面 X 锁
    void xunlock_page(uint32_t page_id);

    // X 锁页面守卫：构造时 fetch X + 反序列化到局部副本；commit() 回写
    // 页面；析构自动释放 X 锁（异常安全，杜绝锁泄漏）
    class XPageGuard {
    public:
        XPageGuard(SecFSM* fsm, uint32_t page_id);
        ~XPageGuard();
        XPageGuard(const XPageGuard&) = delete;
        XPageGuard& operator=(const XPageGuard&) = delete;

        bool valid() const { return page_ != nullptr; }
        FSMPageData& data() { return data_; }
        const FSMPageData& data() const { return data_; }
        void commit() { if (page_ != nullptr) fsm_->xwrite_page(data_, page_); }

    private:
        SecFSM* fsm_;
        uint32_t page_id_;
        Page* page_;
        FSMPageData data_;
    };

    // ---------------- 树导航 ----------------
    // O(1) 定位管理 heap_page_id 的叶子页：叶子页自 FSM_ROOT_PAGE_ID 起
    // 连续分配、每页管理连续 leaves_per_page 个堆页面，可直接算术定位；
    // 定位后校验页面头部，不匹配则回退到自根向下的遍历（保证布局变化
    // 时仍然正确）
    uint32_t locate_leaf_page(uint32_t heap_page_id);

    // 回退路径：自根向下遍历定位叶子页（每页一次 S 锁 fetch）
    uint32_t search_leaf_page_from(uint32_t fsm_page_id, uint32_t heap_page_id);

    // ---------------- 搜索 ----------------
    uint32_t search_from_page(uint32_t fsm_page_id, uint8_t required_category);
    uint32_t search_in_leaf_page(const FSMPageData& leaf_page, uint8_t required_category) const;

    // ---------------- 更新传播 ----------------
    // 页内自 node_index 向上传播到页根；返回页根(nodes[0])值是否变化
    bool propagate_within_page(FSMPageData& page, uint32_t node_index);

    // 持有 child 页 X 锁（修改已 commit、尚未 release）时向上更新父页：
    // 取父页 X 锁 → 更新对应槽位 → 递归上行；回溯时自根向叶逐层释放
    void propagate_up(uint32_t child_page_id, const FSMPageData& child);

    // ---------------- 树构建（遗留路径，计算端不在事务热路径调用） ----------------
    bool build_fsm_tree();
    bool create_internal_pages(const std::vector<uint32_t>& child_pages, 
                              uint32_t parent_page_id, uint32_t level);
    uint32_t allocate_fsm_page();

    // ---------------- 序列化 ----------------
    bool serialize_metadata(char* buffer, uint32_t size);
    bool deserialize_metadata(const char* buffer, uint32_t size);
    bool serialize_page(FSMPageData& page_data, char* buffer, uint32_t size);
    bool deserialize_page(FSMPageData& page_data, const char* buffer, uint32_t size);

    // ---------------- 工具函数 ----------------
    uint8_t space_to_category(uint32_t free_space) const;
    uint32_t category_to_space(uint8_t category) const;
    uint32_t get_parent_index(uint32_t child) const;
    uint32_t get_left_child(uint32_t parent) const;
    uint32_t get_right_child(uint32_t parent) const;
    bool is_leaf_node(const FSMPageData& page_data, uint32_t index) const;
    void ensure_child_vector(FSMPageData& page_data);
    uint32_t find_child_index(FSMPageData& parent_page, uint32_t child_page_id) const;

private:
    ComputeServer *server;
    const table_id_t table_id_;     // RPC 路由用的表 ID（构造时固定，运行期只读）

    FSMMetaData meta_;
    mutable std::mutex meta_mutex_; // 仅保护 meta_ 的读写（纯内存临界区，无 RPC）
    std::once_flag init_once_;      // meta 页懒加载仅执行一次
    bool initialized_;

    // 遗留成员：计算端热路径已不再使用（页面一律走局部副本），仅为
    // build_fsm_tree / extend / print_debug_info 等遗留路径保留
    std::unordered_map<uint32_t, FSMPageData> fsm_pages_;
    std::mutex mutex_;
};

// 简易断言辅助
static void Assert(bool cond, const char* msg) {
    if (!cond) {
        std::cerr << "[ASSERT FAIL] " << msg << std::endl;
        std::exit(1);
    }
}
