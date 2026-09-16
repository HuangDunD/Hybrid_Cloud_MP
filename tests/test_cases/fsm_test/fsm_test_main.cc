// ============================================================================
// FSM 并发单元测试
//
// 测试方式：用桩（stub）替换 ComputeServer 的 8 个页面 RPC 方法，内部实
// 现一个"模拟页面服务器"：
//   - 每页一份权威数据 + 一把读写锁（模拟远程页面锁表的 S/X 语义）
//   - fetch 返回线程本地 Page 缓冲副本（模拟 compute 端 buffer pool）
//   - release X 时把本地缓冲写回（模拟 release_x 的 flush）
//   - 可配置每次 fetch 的延迟（模拟 RPC 往返）
//
// 覆盖：
//   Test A  单线程基础功能（find/update/get + 全空/恢复边界）
//   Test B  多线程并发正确性
//           B1: 不相交区域并发更新 → 逐页终值校验 + 全树自洽校验
//           B2: 全范围随机竞争更新 + 并发搜索 → 全树自洽校验（无死锁/崩溃）
//   Test C  并发扩展性（模拟 100us RPC 延迟，1 线程 vs 8 线程耗时对比）
//
// "全树自洽"校验（树形不变式，能抓住并发传播的丢失更新）：
//   - 每页内二叉树：父节点 == max(有效子节点)
//   - 内部页槽位 == 对应子页根节点值
//   - 叶子值 == 期望终值（仅 B1）
// ============================================================================

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "config.h"
#include "base/page.h"
#include "core/fsm/fsm_tree.h"
// 仅为获得 ComputeServer 的方法声明以提供桩实现，不使用其真实实现
#include "compute_server/server.h"

namespace {

// ---------------- 与 FSM 相同的分类逻辑（独立实现，交叉验证） ----------------
uint8_t cat_of(uint32_t free_space) {
    if (free_space == 0) return 0;                       // NO_SPACE
    if (free_space >= PAGE_SIZE * 9 / 10) return 255;    // EMPTY
    if (free_space >= PAGE_SIZE * 2 / 3) return 192;     // ALMOST_EMPTY
    if (free_space >= PAGE_SIZE / 3) return 128;         // HALF_FULL
    return 64;                                           // ALMOST_FULL
}

// ---------------- 模拟页面服务器 ----------------
struct PageEntry {
    std::shared_mutex latch;   // 远程页面锁（S/X）
    std::vector<char> data;    // storage 端权威数据
};

class MockPageServer {
public:
    static MockPageServer& instance() {
        static MockPageServer s;
        return s;
    }

    // ---- build 阶段（单线程调用） ----
    void reset(int latency_us = 0) {
        pages_.clear();
        latency_us_ = latency_us;
        s_fetch_cnt_ = 0;
        x_fetch_cnt_ = 0;
    }

    void put_page(uint32_t page_id, const char* data, size_t len) {
        ensure_size(page_id + 1);
        PageEntry& e = *pages_[page_id];
        e.data.assign(data, data + len);
    }

    // ---- run 阶段（并发调用；pages_ 结构已冻结，仅原子读索引） ----
    Page* fetch(table_id_t tid, page_id_t pid, bool exclusive) {
        (void)tid;
        int lat = latency_us_.load(std::memory_order_relaxed);
        if (lat > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(lat));
        }
        if (pid < 0 || static_cast<uint32_t>(pid) >= pages_.size() || !pages_[pid] ||
            pages_[pid]->data.empty()) {
            return nullptr;
        }
        PageEntry& e = *pages_[pid];
        if (exclusive) {
            e.latch.lock();
            x_fetch_cnt_.fetch_add(1, std::memory_order_relaxed);
        } else {
            e.latch.lock_shared();
            s_fetch_cnt_.fetch_add(1, std::memory_order_relaxed);
        }
        // 数据拷入本线程"compute buffer"副本后返回
        Page* p = thread_page(pid);
        std::memcpy(p->get_data(), e.data.data(),
                    std::min<size_t>(PAGE_SIZE, e.data.size()));
        return p;
    }

    void release(table_id_t tid, page_id_t pid, bool exclusive) {
        (void)tid;
        if (pid < 0 || static_cast<uint32_t>(pid) >= pages_.size() || !pages_[pid]) {
            return;
        }
        PageEntry& e = *pages_[pid];
        if (exclusive) {
            // X 释放：本地缓冲写回 storage（模拟 release_x 的 flush）
            Page* p = thread_page(pid);
            std::memcpy(e.data.data(), p->get_data(),
                        std::min<size_t>(PAGE_SIZE, e.data.size()));
            e.latch.unlock();
        } else {
            e.latch.unlock_shared();
        }
    }

    // ---- 校验阶段（单线程、无并发时调用） ----
    bool copy_page(uint32_t page_id, char* out, size_t len) const {
        if (page_id >= pages_.size() || !pages_[page_id] || pages_[page_id]->data.empty()) {
            return false;
        }
        std::memcpy(out, pages_[page_id]->data.data(),
                    std::min(len, pages_[page_id]->data.size()));
        return true;
    }

    uint64_t s_fetch_cnt() const { return s_fetch_cnt_.load(); }
    uint64_t x_fetch_cnt() const { return x_fetch_cnt_.load(); }

private:
    void ensure_size(size_t n) {
        if (pages_.size() < n) pages_.resize(n);
        if (!pages_[n - 1]) pages_[n - 1].reset(new PageEntry());
    }

    static Page* thread_page(page_id_t pid) {
        thread_local std::unordered_map<page_id_t, std::unique_ptr<Page>> buf;
        auto it = buf.find(pid);
        if (it == buf.end()) {
            it = buf.emplace(pid, std::make_unique<Page>()).first;
        }
        return it->second.get();
    }

    std::vector<std::unique_ptr<PageEntry>> pages_;  // build 后结构冻结
    std::atomic<int> latency_us_{0};
    std::atomic<uint64_t> s_fetch_cnt_{0};
    std::atomic<uint64_t> x_fetch_cnt_{0};
};

}  // namespace

// ==================== ComputeServer 页面 RPC 桩实现 ====================
// 不解引用 this（测试中 SecFSM 的 server 指针为空），路由到全局 MockPageServer

Page* ComputeServer::rpc_fetch_s_page(table_id_t table_id, page_id_t page_id) {
    return MockPageServer::instance().fetch(table_id, page_id, false);
}

Page* ComputeServer::rpc_fetch_x_page(table_id_t table_id, page_id_t page_id) {
    return MockPageServer::instance().fetch(table_id, page_id, true);
}

void ComputeServer::rpc_release_s_page(table_id_t table_id, page_id_t page_id) {
    MockPageServer::instance().release(table_id, page_id, false);
}

void ComputeServer::rpc_release_x_page(table_id_t table_id, page_id_t page_id) {
    MockPageServer::instance().release(table_id, page_id, true);
}

Page* ComputeServer::rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id,
                                           bool need_to_record) {
    (void)need_to_record;
    return MockPageServer::instance().fetch(table_id, page_id, false);
}

Page* ComputeServer::rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id,
                                           bool need_to_record) {
    (void)need_to_record;
    return MockPageServer::instance().fetch(table_id, page_id, true);
}

void ComputeServer::rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id) {
    MockPageServer::instance().release(table_id, page_id, false);
}

void ComputeServer::rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id) {
    MockPageServer::instance().release(table_id, page_id, true);
}

namespace {

// ==================== 测试侧树构建器（模拟 storage 端 build_fsm_tree） ====================

constexpr uint32_t T_LEAVES_PER_PAGE = 1024;
constexpr uint32_t T_CHILDREN_PER_PAGE = 512;
constexpr table_id_t T_FSM_TABLE_ID = 20000;

struct TreeInfo {
    uint32_t root_page_id = 0;
    uint32_t total_heap_pages = 0;
    uint32_t height = 0;
    std::vector<uint32_t> leaf_page_ids;
};

void calc_structure(uint32_t leaves_needed, uint32_t& leaf_start, uint32_t& node_count) {
    uint32_t height = 1, max_leaves = 1;
    while (max_leaves < leaves_needed) {
        height++;
        max_leaves = (1u << (height - 1));
    }
    leaf_start = (1u << (height - 1)) - 1;
    node_count = leaf_start + leaves_needed;
}

// 页内二叉树聚合：自底向上重算（父 = max(有效孩子, 缺失孩子按 0)）。
// 槽数非 2 的幂时存在悬空内部节点（孩子索引越界），天然被重算为 0
void fill_aggregates(std::vector<uint8_t>& vals, uint32_t leaf_start) {
    uint32_t begin = std::min<uint32_t>(leaf_start, vals.size());
    for (int p = static_cast<int>(begin) - 1; p >= 0; --p) {
        uint32_t l = 2 * p + 1, r = 2 * p + 2;
        uint8_t lv = (l < vals.size()) ? vals[l] : 0;
        uint8_t rv = (r < vals.size()) ? vals[r] : 0;
        vals[p] = std::max(lv, rv);
    }
}

// 按 SecFSM::deserialize_page 的格式序列化一页
void serialize_image(const FSMPageHeader& header, const std::vector<uint8_t>& vals,
                     const std::vector<uint32_t>& child_ids, char* buf) {
    std::memset(buf, 0, PAGE_SIZE);
    FSMPageHeader h = header;
    h.magic_number = 0x46535047;
    std::memcpy(buf, &h, sizeof(FSMPageHeader));
    char* node_data = buf + sizeof(FSMPageHeader);
    for (size_t i = 0; i < vals.size(); ++i) node_data[i] = static_cast<char>(vals[i]);
    if (h.page_type == FSMPageType::INTERNAL_PAGE && !child_ids.empty()) {
        std::memcpy(node_data + vals.size(), child_ids.data(), child_ids.size() * sizeof(uint32_t));
    }
}

TreeInfo build_tree(uint32_t total_heap_pages, uint8_t init_category) {
    MockPageServer& srv = MockPageServer::instance();
    srv.reset();
    TreeInfo info;
    info.total_heap_pages = total_heap_pages;

    // 先确定整棵树的结构（页面 ID、父子关系），再统一序列化放置。
    // 与 storage 端 build_fsm_tree 语义一致：叶子页自 FSM_ROOT_PAGE_ID 起
    // 连续分配，之后自底向上分配内部页。
    struct Spec {
        uint32_t id;
        FSMPageType type;
        uint32_t parent = 0;
        uint32_t level = 0;
        uint32_t first_heap = 0, heap_cnt = 0;   // 叶子页
        std::vector<uint32_t> kids;              // 内部页
    };

    uint32_t next_id = FSM_ROOT_PAGE_ID;
    std::vector<Spec> specs;
    std::vector<uint32_t> current_level;

    // 叶子层
    uint32_t leaf_pages =
        (total_heap_pages + T_LEAVES_PER_PAGE - 1) / T_LEAVES_PER_PAGE;
    for (uint32_t i = 0; i < leaf_pages; ++i) {
        Spec s;
        s.id = next_id++;
        s.type = FSMPageType::LEAF_PAGE;
        s.first_heap = i * T_LEAVES_PER_PAGE;
        s.heap_cnt = std::min(T_LEAVES_PER_PAGE, total_heap_pages - s.first_heap);
        current_level.push_back(s.id);
        specs.push_back(std::move(s));
        info.leaf_page_ids.push_back(specs.back().id);
    }

    // 内部层（自底向上）
    uint32_t level = 1;
    while (current_level.size() > 1) {
        std::vector<uint32_t> next_level;
        for (size_t i = 0; i < current_level.size(); i += T_CHILDREN_PER_PAGE) {
            size_t gs = std::min<size_t>(T_CHILDREN_PER_PAGE, current_level.size() - i);
            std::vector<uint32_t> kids(current_level.begin() + i,
                                       current_level.begin() + i + gs);
            Spec s;
            s.id = next_id++;
            s.type = FSMPageType::INTERNAL_PAGE;
            s.level = level;
            s.kids = std::move(kids);
            next_level.push_back(s.id);
            specs.push_back(std::move(s));
        }
        current_level = std::move(next_level);
        level++;
    }

    info.root_page_id = current_level.empty() ? 0 : current_level.front();
    info.height = level;

    // 填充父指针
    std::unordered_map<uint32_t, uint32_t> parent_of;  // child_page_id -> parent_page_id
    for (auto& s : specs) {
        if (s.type == FSMPageType::INTERNAL_PAGE) {
            for (uint32_t k : s.kids) parent_of[k] = s.id;
        }
    }
    for (auto& s : specs) {
        auto it = parent_of.find(s.id);
        s.parent = (it != parent_of.end()) ? it->second : 0;
    }

    // 统一序列化放置
    for (auto& s : specs) {
        FSMPageHeader h = {};
        h.page_id = s.id;
        h.page_type = s.type;
        h.parent_page = s.parent;
        h.level = s.level;
        if (s.type == FSMPageType::LEAF_PAGE) {
            h.first_heap_page = s.first_heap;
            h.heap_pages_count = s.heap_cnt;
            calc_structure(s.heap_cnt, h.leaf_start, h.node_count);
        } else {
            h.child_count = static_cast<uint32_t>(s.kids.size());
            h.first_child_page = s.kids.front();
            calc_structure(h.child_count, h.leaf_start, h.node_count);
        }
        std::vector<uint8_t> vals(h.node_count, init_category);
        fill_aggregates(vals, h.leaf_start);
        char buf[PAGE_SIZE];
        serialize_image(h, vals, s.kids, buf);
        srv.put_page(s.id, buf, PAGE_SIZE);
    }

    // meta 页（page 1）
    FSMMetaData md = {};
    md.magic_number = 0x46534D54;
    md.version = 1;
    md.total_heap_pages = total_heap_pages;
    md.total_fsm_pages = static_cast<uint32_t>(specs.size());
    md.tree_height = info.height;
    md.root_page_id = info.root_page_id;
    md.next_fsm_page_id = next_id - 1;
    md.leaves_per_page = T_LEAVES_PER_PAGE;
    md.children_per_page = T_CHILDREN_PER_PAGE;
    md.timestamp = 0;
    md.table_id = T_FSM_TABLE_ID;
    char mbuf[PAGE_SIZE];
    std::memset(mbuf, 0, PAGE_SIZE);
    std::memcpy(mbuf, &md, sizeof(FSMMetaData));
    srv.put_page(FSM_META_PAGE_ID, mbuf, PAGE_SIZE);
    return info;
}

// ==================== 全树自洽校验 ====================
// expected_leaf 为空时跳过叶子终值校验（随机竞争阶段无法确定终值）
bool verify_tree(const TreeInfo& info, const std::vector<uint8_t>* expected_leaf,
                 std::string& err) {
    MockPageServer& srv = MockPageServer::instance();
    char buf[PAGE_SIZE];

    // meta 页校验
    if (!srv.copy_page(FSM_META_PAGE_ID, buf, PAGE_SIZE)) {
        err = "meta page missing";
        return false;
    }
    FSMMetaData md;
    std::memcpy(&md, buf, sizeof(FSMMetaData));
    if (md.magic_number != 0x46534D54 || md.root_page_id != info.root_page_id ||
        md.total_heap_pages != info.total_heap_pages) {
        err = "meta page corrupted";
        return false;
    }

    std::function<bool(uint32_t, uint32_t)> check = [&](uint32_t pid,
                                                        uint32_t depth) -> bool {
        if (depth > 16) {
            err = "tree too deep (cycle?)";
            return false;
        }
        // 每层递归使用独立缓冲：递归返回后本层数据不能被覆盖
        char pbuf[PAGE_SIZE];
        if (!srv.copy_page(pid, pbuf, PAGE_SIZE)) {
            err = "page " + std::to_string(pid) + " missing";
            return false;
        }
        FSMPageHeader h;
        std::memcpy(&h, pbuf, sizeof(FSMPageHeader));
        if (h.magic_number != 0x46535047) {
            err = "page " + std::to_string(pid) + " bad magic";
            return false;
        }
        std::vector<uint8_t> vals(pbuf + sizeof(FSMPageHeader),
                                  pbuf + sizeof(FSMPageHeader) + h.node_count);

        // 页内不变式：
        //   - 悬空内部节点（孩子索引越界）== 0
        //   - 有孩子的节点：父 == max(有效孩子, 缺失孩子按 0)
        for (uint32_t pidx = 0; pidx < h.leaf_start && pidx < h.node_count; ++pidx) {
            uint32_t l = 2 * pidx + 1, r = 2 * pidx + 2;
            if (l >= h.node_count) {
                if (vals[pidx] != 0) {
                    err = "page " + std::to_string(pid) + " dangling node " +
                          std::to_string(pidx) + " != 0";
                    return false;
                }
                continue;
            }
            uint8_t lv = vals[l];
            uint8_t rv = (r < h.node_count) ? vals[r] : 0;
            if (vals[pidx] != std::max(lv, rv)) {
                err = "page " + std::to_string(pid) + " node " + std::to_string(pidx) +
                      " != max(children) (in-page invariant broken)";
                return false;
            }
        }

        if (h.page_type == FSMPageType::LEAF_PAGE) {
            if (h.first_heap_page + h.heap_pages_count > info.total_heap_pages) {
                err = "leaf page " + std::to_string(pid) + " range overflow";
                return false;
            }
            if (expected_leaf) {
                for (uint32_t k = 0; k < h.heap_pages_count; ++k) {
                    uint8_t expect = (*expected_leaf)[h.first_heap_page + k];
                    if (vals[h.leaf_start + k] != expect) {
                        err = "heap page " + std::to_string(h.first_heap_page + k) +
                              " category " + std::to_string(vals[h.leaf_start + k]) +
                              " != expected " + std::to_string(expect);
                        return false;
                    }
                }
            }
            return true;
        }

        // 内部页：槽位 == 子页根值
        const char* child_blob = pbuf + sizeof(FSMPageHeader) + h.node_count;
        for (uint32_t k = 0; k < h.child_count; ++k) {
            uint32_t child;
            std::memcpy(&child, child_blob + k * sizeof(uint32_t), sizeof(uint32_t));
            char cbuf[PAGE_SIZE];
            if (!srv.copy_page(child, cbuf, PAGE_SIZE)) {
                err = "child page " + std::to_string(child) + " missing (parent=" +
                      std::to_string(pid) + " k=" + std::to_string(k) +
                      " node_count=" + std::to_string(h.node_count) +
                      " child_count=" + std::to_string(h.child_count) +
                      " first_child=" + std::to_string(h.first_child_page) +
                      " leaf_start=" + std::to_string(h.leaf_start) + ")";
                return false;
            }
            FSMPageHeader ch;
            std::memcpy(&ch, cbuf, sizeof(FSMPageHeader));
            uint8_t child_root = static_cast<uint8_t>(cbuf[sizeof(FSMPageHeader)]);
            if (h.leaf_start + k >= h.node_count ||
                vals[h.leaf_start + k] != child_root) {
                err = "internal page " + std::to_string(pid) + " slot " +
                      std::to_string(k) + " (" + std::to_string(vals[h.leaf_start + k]) +
                      ") != child " + std::to_string(child) + " root (" +
                      std::to_string(child_root) + ")  [丢失更新/传播错误]";
                return false;
            }
            if (!check(child, depth + 1)) return false;
        }
        return true;
    };

    return check(info.root_page_id, 0);
}

// ==================== 死锁看门狗 ====================
class Watchdog {
public:
    explicit Watchdog(int seconds, const char* tag) {
        finished_.store(false);
        th_ = std::thread([this, seconds, tag]() {
            for (int i = 0; i < seconds * 10; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (finished_.load()) return;
            }
            std::fprintf(stderr, "\n[FATAL] %s 疑似死锁/挂起（>%ds），强制退出\n", tag,
                         seconds);
            std::_Exit(2);
        });
    }
    ~Watchdog() {
        finished_.store(true);
        if (th_.joinable()) th_.join();
    }

private:
    std::atomic<bool> finished_{false};
    std::thread th_;
};

std::atomic<int> g_failures{0};

#define CHECK(cond, msg)                                                    \
    do {                                                                    \
        if (cond) {                                                         \
            std::printf("  [PASS] %s\n", msg);                              \
        } else {                                                            \
            std::printf("  [FAIL] %s\n", msg);                              \
            g_failures++;                                                   \
        }                                                                   \
    } while (0)

// ==================== Test A: 单线程基础功能 ====================
void test_basic() {
    std::printf("\n===== Test A: 单线程基础功能 =====\n");
    const uint32_t HP = 10000;
    TreeInfo info = build_tree(HP, 255);
    SecFSM fsm(nullptr, T_FSM_TABLE_ID);

    uint32_t p0 = fsm.find_free_page(PAGE_SIZE);
    CHECK(p0 != 0xFFFFFFFF && p0 < HP, "find_free_page 返回有效页面");

    fsm.update_page_space(p0, 0);
    CHECK(fsm.get_page_space(p0) == 0, "update 置空后 get_page_space == 0");

    uint32_t p1 = fsm.find_free_page(PAGE_SIZE);
    CHECK(p1 != 0xFFFFFFFF && p1 != p0, "置空后 find_free_page 跳过该页");

    fsm.update_page_space(p0, PAGE_SIZE);
    CHECK(cat_of(fsm.get_page_space(p0)) == 255, "恢复空间后类别恢复为 EMPTY");

    // 全部置空
    for (uint32_t p = 0; p < HP; ++p) fsm.update_page_space(p, 0);
    CHECK(fsm.find_free_page(1) == 0xFFFFFFFF, "全部页面无空间时返回无效值");

    // 恢复单页后精确命中
    fsm.update_page_space(5, PAGE_SIZE);
    CHECK(fsm.find_free_page(PAGE_SIZE) == 5, "唯一有空闲页被精确返回");

    // 树自洽 + 终值
    std::vector<uint8_t> expected(HP, 0);
    expected[5] = 255;
    std::string err;
    bool ok = verify_tree(info, &expected, err);
    CHECK(ok, ("全树自洽校验: " + (ok ? std::string("OK") : err)).c_str());
}

// ==================== Test B: 并发正确性 ====================
void test_concurrent_correctness() {
    std::printf("\n===== Test B: 并发正确性 =====\n");
    const uint32_t HP = 20000;   // 20 个叶子页 + 1 个根内部页
    const int NT = 8;
    TreeInfo info = build_tree(HP, 255);
    SecFSM fsm(nullptr, T_FSM_TABLE_ID);

    Watchdog dog(60, "Test B");

    // ---------- B1: 不相交区域并发更新（可精确校验终值） ----------
    std::printf("  [B1] %d 线程不相交区域各 500 次随机更新 + 2 线程并发搜索...\n", NT);
    std::vector<uint8_t> expected(HP, 255);
    {
        std::vector<std::thread> threads;
        const uint32_t CHUNK = HP / NT;
        for (int t = 0; t < NT; ++t) {
            threads.emplace_back([&, t]() {
                std::mt19937 rng(1234 + t);
                uint32_t begin = t * CHUNK;
                uint32_t end = (t == NT - 1) ? HP : (t + 1) * CHUNK;
                for (int i = 0; i < 500; ++i) {
                    uint32_t page = begin + rng() % (end - begin);
                    uint32_t space = rng() % (PAGE_SIZE + 1);
                    fsm.update_page_space(page, space);
                    expected[page] = cat_of(space);  // 区域独占，无竞争
                }
            });
        }
        // 干扰线程：并发搜索
        std::atomic<bool> stop(false);
        std::vector<std::thread> finders;
        for (int t = 0; t < 2; ++t) {
            finders.emplace_back([&]() {
                std::mt19937 rng(777 + t);
                uint64_t ok_cnt = 0, total = 0;
                while (!stop.load()) {
                    uint32_t r = fsm.find_free_page(rng() % (PAGE_SIZE + 1));
                    total++;
                    if (r == 0xFFFFFFFF || r < HP) ok_cnt++;
                }
                if (ok_cnt != total) {
                    std::printf("  [FAIL] 并发搜索返回越界页面\n");
                    g_failures++;
                }
            });
        }
        for (auto& th : threads) th.join();
        stop.store(true);
        for (auto& th : finders) th.join();
    }

    std::string err;
    bool ok1 = verify_tree(info, &expected, err);
    CHECK(ok1, ("B1 终值 + 全树自洽: " + (ok1 ? std::string("OK") : err)).c_str());

    // get_page_space 逐线程抽样校验终值（走 FSM 读接口）
    {
        bool all_ok = true;
        std::mt19937 rng(42);
        for (int i = 0; i < 500; ++i) {
            uint32_t page = rng() % HP;
            if (cat_of(fsm.get_page_space(page)) != expected[page]) {
                all_ok = false;
                break;
            }
        }
        CHECK(all_ok, "B1 get_page_space 抽样终值校验");
    }

    // ---------- B2: 全范围随机竞争更新（只校验树自洽 + 无死锁） ----------
    std::printf("  [B2] %d 线程全范围随机竞争更新各 300 次...\n", NT);
    {
        std::vector<std::thread> threads;
        for (int t = 0; t < NT; ++t) {
            threads.emplace_back([&, t]() {
                std::mt19937 rng(4321 + t);
                for (int i = 0; i < 300; ++i) {
                    uint32_t page = rng() % HP;
                    uint32_t space = rng() % (PAGE_SIZE + 1);
                    fsm.update_page_space(page, space);
                }
            });
        }
        for (auto& th : threads) th.join();
    }
    bool ok2 = verify_tree(info, nullptr, err);
    CHECK(ok2, ("B2 竞争更新后全树自洽（无丢失更新）: " +
                (ok2 ? std::string("OK") : err)).c_str());
}

// ==================== Test C: 并发扩展性 ====================
void test_scalability() {
    std::printf("\n===== Test C: 并发扩展性（模拟 RPC 延迟 100us） =====\n");
    const uint32_t HP = 81920;   // 80 个叶子页 + 1 个根内部页
    const int NT = 8;
    const int OPS = 256;         // 每轮总更新次数
    const int FIND_OPS = 400;    // 每轮总搜索次数

    auto timed_update = [&](int threads) {
        TreeInfo info = build_tree(HP, 255);
        SecFSM fsm(nullptr, T_FSM_TABLE_ID);
        Watchdog dog(60, "Test C update");
        auto t0 = std::chrono::steady_clock::now();
        std::vector<std::thread> ts;
        int per = OPS / threads;
        for (int t = 0; t < threads; ++t) {
            ts.emplace_back([&, t]() {
                std::mt19937 rng(1000 + t);
                for (int i = 0; i < per; ++i) {
                    fsm.update_page_space(rng() % HP, rng() % (PAGE_SIZE + 1));
                }
            });
        }
        for (auto& th : ts) th.join();
        auto t1 = std::chrono::steady_clock::now();
        std::string err;
        bool ok = verify_tree(info, nullptr, err);
        if (!ok) {
            std::printf("  [FAIL] 扩展性测试树自洽失败: %s\n", err.c_str());
            g_failures++;
        }
        return std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    };

    auto timed_find = [&](int threads) {
        build_tree(HP, 255);
        SecFSM fsm(nullptr, T_FSM_TABLE_ID);
        Watchdog dog(60, "Test C find");
        auto t0 = std::chrono::steady_clock::now();
        std::vector<std::thread> ts;
        int per = FIND_OPS / threads;
        for (int t = 0; t < threads; ++t) {
            ts.emplace_back([&, t]() {
                std::mt19937 rng(2000 + t);
                for (int i = 0; i < per; ++i) {
                    uint32_t r = fsm.find_free_page(rng() % (PAGE_SIZE + 1));
                    (void)r;
                }
            });
        }
        for (auto& th : ts) th.join();
        auto t1 = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    };

    long long u1 = timed_update(1);
    long long u8 = timed_update(NT);
    std::printf("  update: 1 线程 %lldms vs %d 线程 %lldms -> 加速比 %.2fx\n", u1, NT, u8,
                (double)u1 / (double)u8);

    long long f1 = timed_find(1);
    long long f8 = timed_find(NT);
    std::printf("  find  : 1 线程 %lldms vs %d 线程 %lldms -> 加速比 %.2fx\n", f1, NT, f8,
                (double)f1 / (double)f8);

    // 旧实现（全局锁内做 RPC）理论上多线程加速比恒为 1x；
    // 新实现不同叶子页可完全并行，仅上层页面短暂串行。
    CHECK(u8 * 2 < u1, "update 多线程扩展性显著（加速比 > 2x）");
    CHECK(f8 * 2 < f1, "find 多线程扩展性显著（加速比 > 2x）");
}

}  // namespace

int main() {
    SYSTEM_MODE = 1;  // lazy 模式（与 start_all.sh 生产用法一致）

    std::printf("============================================================\n");
    std::printf(" FSM 并发单元测试（SYSTEM_MODE=%d, 模拟页面锁服务器）\n", SYSTEM_MODE);
    std::printf("============================================================\n");

    test_basic();
    test_concurrent_correctness();
    test_scalability();

    std::printf("\n============================================================\n");
    if (g_failures.load() == 0) {
        std::printf(" 结果: ALL PASSED\n");
    } else {
        std::printf(" 结果: %d 项 FAILED\n", g_failures.load());
    }
    std::printf("============================================================\n");
    return g_failures.load() == 0 ? 0 : 1;
}
