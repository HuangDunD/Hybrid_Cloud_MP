// R2c C1（32.3）：统一恢复影响目录。
//
// 目的：把分散在 GPLM ir_locked、Phase 1a/1b 清扫计数、Phase 2/3
// remaining_ir_pages、AnalyzeRecoveryPages 结果与 GVT 有效性位中的
// "页 × 恢复状态"信息，收敛为一个带恢复代（generation）与版本
//（version_lsn）的单一分类账本，供 C3 按页准入与验收取证使用。
//
// 契约（32.2/32.3 C1 验收）：
// 1. 四态分类 UNKNOWN / UNAFFECTED / AFFECTED / RECOVERED_READY；
//    UNAFFECTED（有证据无关）与 RECOVERED_READY（受影响后验证就绪）
//    不混算——服务证据分别记录。
// 2. 恢复代：每次 MarkNodeFailed 开启新代（BeginGeneration）；
//    旧代条目一律视为陈旧（Classify 返回 UNKNOWN），旧代报告不能
//    解隔离（MarkRecovered 拒绝旧代）。
// 3. fail-closed：没有任何证据的页（未扫描表 / 陈旧条目）分类为
//    UNKNOWN，不得当作 UNAFFECTED 服务。
// 4. 本目录是分类账本；强制隔离机构仍是 GPLM IR 锁。目录登记与
//    IR 上锁/释放同步进行（见 server.h MarkNodeFailed 各 Phase 与
//    ApplyReportPageStatus / RunIRRecoveryPhase3 的接线）。
//
// 线程安全：内部 mutex；登记发生在恢复线程，查询可来自任意 worker。

#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace recovery_catalog {

// 32.2 页分类契约（名称为已实现契约，不再是拟议）
enum class RPageState : uint8_t {
    UNKNOWN = 0,       // 无证据——fail-closed，核验前不得服务
    UNAFFECTED,        // 有证据与故障节点无关，可直接沿用正常访问协议
    AFFECTED,          // 涉及故障恢复且未就绪——隔离（服务前须恢复验证）
    RECOVERED_READY,   // 受影响后经恢复验证就绪（版本已核实）
};

// 受影响原因（登记时记录，供取证与 C3/C4 审计）
enum class RAffectedReason : uint8_t {
    NONE = 0,
    FAILED_NODE_X_HOLDER,   // 故障节点持 X 锁（Phase 1a）
    FAILED_NODE_S_HOLDER,   // 故障节点持 S 锁（Phase 1a，仅清份额不上 IR）
    FAILED_MANAGER,         // 原管理者为故障节点（Phase 1b 接管）
    FILE_HEADER,            // page 0 公共文件头页（Phase 1b 特判，随接管隔离）
    // 预留：在途授权（INFLIGHT_GRANT）与结构依赖（STRUCTURAL_DEP）
    // 的证据源在 C2/C3 接入时启用，目录结构已支持。
};

struct RPageEntry {
    RPageState state = RPageState::UNKNOWN;
    uint64_t generation = 0;   // 该分类所属恢复代
    uint64_t version_lsn = 0;  // RECOVERED_READY 时的已验证版本
    RAffectedReason reason = RAffectedReason::NONE;
};

struct RPageKey {
    uint64_t table_id;
    uint64_t page_id;
    bool operator==(const RPageKey& o) const {
        return table_id == o.table_id && page_id == o.page_id;
    }
};

struct RPageKeyHash {
    size_t operator()(const RPageKey& k) const {
        return std::hash<uint64_t>()(k.table_id * 1000003ull + k.page_id);
    }
};

class RecoveryPageCatalog {
public:
    // MarkNodeFailed 起点：开启新恢复代。旧代全部条目与表扫描标记作废
    //（保留在 map 中但 generation < current，Classify 按陈旧处理）。
    // 返回当前代。
    uint64_t BeginGeneration(uint64_t new_generation, int32_t failed_node) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (new_generation <= current_generation_) {
            // 幂等防御：MarkNodeFailed 重试时 recovery_epoch 可能未变；
            // 同代重入不重置（已登记的分类仍然有效）。真正的降代是
            // 程序错误，仅记录。
            return current_generation_;
        }
        current_generation_ = new_generation;
        failed_node_ = failed_node;
        swept_tables_.clear();
        return current_generation_;
    }

    uint64_t current_generation() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return current_generation_;
    }

    int32_t failed_node() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return failed_node_;
    }

    // Phase 1a/1b：登记受影响页。仅接受当前代（防旧代重试把新代状态
    // 改回 AFFECTED——重试路径先 BeginGeneration 同代幂等，不会触发）。
    // 对当前代已 RECOVERED_READY 的页重新 MarkAffected 是合法的
    //（重试场景 Phase 2 汇报先清了 IR、Phase 1 重新上 IR），回退为
    // AFFECTED 待重新验证（fail-closed）。
    bool MarkAffected(uint64_t gen, uint64_t table_id, uint64_t page_id,
                      RAffectedReason reason) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (gen != current_generation_) return false;
        RPageEntry& e = entries_[RPageKey{table_id, page_id}];
        e.state = RPageState::AFFECTED;
        e.generation = gen;
        e.version_lsn = 0;
        e.reason = reason;
        return true;
    }

    // Phase 1a+1b 对该表扫描完成：表内未登记页获得"已扫描无故障证据"
    // 背书，Classify 返回 UNAFFECTED。仅当前代有效。
    bool MarkTableSwept(uint64_t gen, uint64_t table_id) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (gen != current_generation_) return false;
        swept_tables_.insert(table_id);
        return true;
    }

    // 受影响页恢复验证就绪（Phase 4 status=0/1 或 Phase 2 有效副本确认）。
    // 契约：仅当前代 AFFECTED 条目可迁移为 RECOVERED_READY；
    //  - 旧代报告（gen != current）：拒绝——旧代报告不能解隔离；
    //  - 无条目/UNAFFECTED 默认页：拒绝（Phase 4 清单来自 IR 锁=受影响页，
    //    出现此情形说明上游不一致，宁可拒绝保留隔离）；
    //  - 已 RECOVERED_READY：幂等更新版本（取较大 LSN，防旧值回退）。
    bool MarkRecovered(uint64_t gen, uint64_t table_id, uint64_t page_id,
                       uint64_t verified_lsn) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (gen != current_generation_) return false;
        auto it = entries_.find(RPageKey{table_id, page_id});
        if (it == entries_.end()) return false;
        RPageEntry& e = it->second;
        if (e.state == RPageState::AFFECTED) {
            e.state = RPageState::RECOVERED_READY;
            e.version_lsn = verified_lsn;
            return true;
        }
        if (e.state == RPageState::RECOVERED_READY) {
            if (verified_lsn > e.version_lsn) e.version_lsn = verified_lsn;
            return true;
        }
        // UNAFFECTED 条目上出现恢复报告：上游不一致，拒绝并保持原分类。
        return false;
    }

    // 查询页分类。规则：
    //  - 条目存在且为当前代：返回条目状态；
    //  - 条目存在但为旧代（陈旧）：UNKNOWN——不以旧代 READY/UNAFFECTED
    //    替代当前状态（32.2 契约）；
    //  - 无条目、表已扫描（当前代）：UNAFFECTED（有扫描证据）；
    //  - 无条目、表未扫描：UNKNOWN（fail-closed）。
    RPageEntry Classify(uint64_t table_id, uint64_t page_id) const {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = entries_.find(RPageKey{table_id, page_id});
        if (it != entries_.end()) {
            const RPageEntry& e = it->second;
            if (e.generation == current_generation_) return e;
            RPageEntry stale;
            stale.state = RPageState::UNKNOWN;
            stale.generation = current_generation_;
            stale.reason = RAffectedReason::NONE;
            return stale;
        }
        RPageEntry e;
        e.generation = current_generation_;
        if (swept_tables_.count(table_id) > 0) {
            e.state = RPageState::UNAFFECTED;
        }
        return e;
    }

    // 当前代是否处于隔离（AFFECTED）——C3 admission 的按页检查入口
    bool IsIsolated(uint64_t table_id, uint64_t page_id) const {
        return Classify(table_id, page_id).state == RPageState::AFFECTED;
    }

    struct Counts {
        uint64_t generation = 0;
        size_t affected = 0;
        size_t recovered_ready = 0;
        size_t swept_tables = 0;
        size_t stale_entries = 0;   // 旧代残留条目（仅统计）
    };

    Counts GetCounts() const {
        std::lock_guard<std::mutex> lk(mutex_);
        Counts c;
        c.generation = current_generation_;
        c.swept_tables = swept_tables_.size();
        for (const auto& kv : entries_) {
            if (kv.second.generation != current_generation_) {
                c.stale_entries++;
            } else if (kv.second.state == RPageState::AFFECTED) {
                c.affected++;
            } else if (kv.second.state == RPageState::RECOVERED_READY) {
                c.recovered_ready++;
            }
        }
        return c;
    }

    // 恢复出口取证：仍处 AFFECTED 的当前代条目清单（Phase 4 失败页/漏页）
    std::vector<RPageKey> ListIsolated() const {
        std::lock_guard<std::mutex> lk(mutex_);
        std::vector<RPageKey> out;
        for (const auto& kv : entries_) {
            if (kv.second.generation == current_generation_ &&
                kv.second.state == RPageState::AFFECTED) {
                out.push_back(kv.first);
            }
        }
        return out;
    }

    // 诊断转储（glog/验收日志用）：不超过 limit 条
    std::vector<RPageEntry> DumpEntries(std::vector<RPageKey>* keys_out,
                                        size_t limit = 64) const {
        std::lock_guard<std::mutex> lk(mutex_);
        std::vector<RPageEntry> out;
        for (const auto& kv : entries_) {
            if (out.size() >= limit) break;
            if (keys_out != nullptr) keys_out->push_back(kv.first);
            out.push_back(kv.second);
        }
        return out;
    }

private:
    mutable std::mutex mutex_;
    uint64_t current_generation_ = 0;
    int32_t failed_node_ = -1;
    std::unordered_map<RPageKey, RPageEntry, RPageKeyHash> entries_;
    std::unordered_set<uint64_t> swept_tables_;
};

}  // namespace recovery_catalog
