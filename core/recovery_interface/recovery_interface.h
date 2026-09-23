// R3 C-1（22.8）：公共恢复底座可插拔策略接口——数据契约层。
//
// 目的：把"识别/调度"与"事务、日志、执行、发布"分离。本文件定义
// RecoveryContext / RecoveryTaskCatalog / DemandSnapshot 三个公共数据
// 契约；策略接口（IRecoveryPriorityPolicy）与调度校验（RecoveryScheduler）
// 见 recovery_policy.h / recovery_scheduler.h。
//
// 契约（22.8 接口表，逐行落地）：
// 1. RecoveryContext 由公共底座生成；算法不能延后截面、修改事务结局
//    或改变副本可信性——结构不可变（const 字段＋工厂构造）。
// 2. RecoveryTaskCatalog 物化前一次公共分析生成；索引任务不以故障前
//    计算端叶页号作为权威身份（权威身份=TaskId＋稳定键域）；同 key
//    依赖顺序不得打乱（前驱显式表达，Build 时校验无环＋前驱存在）。
// 3. DemandSnapshot 不含未来请求、模拟真值完整路径、A 丢失内存；
//    缺少信息必须为 unknown（BlockerInfo::kUnknown），不能猜最后
//    阻塞点。
// 4. 影响分类复用 R2c 四态命名（UNKNOWN/UNAFFECTED/AFFECTED/
//    RECOVERED_READY，recovery_catalog::RPageState），不另起状态体系。
// 5. R3 固定截面仅约束受影响恢复域；UNAFFECTED 在线域不因接口关闭
//    （global 屏障保留为显式对照模式，不进入本接口的默认路径）。
//
// 接口只决定合法工作的优先顺序，不拥有正确性权限：本层任何结构都
// 不携带 Redo/Undo/发布/改日志/清 IR 的执行能力。

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../recovery_page_catalog/recovery_page_catalog.h"

namespace recovery_iface {

using recovery_catalog::RPageState;

// ==================== RecoveryContext ====================
// 公共底座生成的恢复上下文。一次恢复一代（generation/epoch）。
struct RecoveryContext {
    const uint64_t recovery_id;      // 本次恢复实例唯一标识
    const uint64_t epoch;            // 恢复代；与 R2c catalog generation 同源
    const uint64_t fixed_wal_lsn;    // 固定 WAL 截面（含端语义由底座定义）
    const uint64_t topology_version; // 元数据/拓扑版本
    const uint64_t metadata_version; // 元信息版本
    const size_t max_group_size;     // 调度器组大小公共上限（策略不可放宽）
    // 预算与资源配置（只读视图；扣减只经 RecoveryScheduler）
    struct Budget {
        uint64_t total_cost_units = 0;
        uint64_t remaining_cost_units = 0;
        uint32_t executor_threads = 0;
        uint32_t io_weight = 0;
    };
    const Budget budget;

    RecoveryContext(uint64_t rid, uint64_t ep, uint64_t lsn, uint64_t topo,
                    uint64_t meta, size_t grp, Budget b)
        : recovery_id(rid), epoch(ep), fixed_wal_lsn(lsn), topology_version(topo),
          metadata_version(meta), max_group_size(grp), budget(b) {}
};

// 事务结局视图：固定截面处已决事务结局（只读）。算法不得修改。
enum class TxVerdict : uint8_t { UNKNOWN = 0, COMMITTED, ABORTED };
using TxOutcomeView = std::unordered_map<uint64_t, TxVerdict>;  // tx_id -> verdict

// ==================== RecoveryTaskCatalog ====================
// 一次公共分析物化的任务目录。Build 后不可变；前驱约束在 Build 时
// 校验（存在性＋无环＋同 key 顺序保持由前驱链表达）。
enum class TaskKind : uint8_t { HEAP_PAGE = 0, INDEX_KEY_RANGE, STRUCTURE_GROUP };
enum class TaskState : uint8_t { PENDING = 0, READY, DISPATCHED, EXECUTING, DONE, FAILED };

struct TaskSpec {
    uint64_t task_id = 0;             // 目录内唯一
    TaskKind kind = TaskKind::HEAP_PAGE;
    uint32_t table_id = 0;
    // 键域（权威身份＝TaskId＋稳定键域；不以故障前计算端叶页号为准）
    uint64_t key_lo = 0;
    uint64_t key_hi = 0;              // 含 key_lo、不含 key_hi
    std::vector<uint64_t> predecessors;  // 合法前驱 TaskId（同 key 依赖顺序）
    // 日志范围（固定截面内）
    uint64_t wal_begin_lsn = 0;
    uint64_t wal_end_lsn = 0;
    uint64_t wal_bytes = 0;
    uint64_t shared_cost_estimate = 0;   // 共享成本估计（公共口径）
    TaskState state = TaskState::PENDING;
    RPageState influence = RPageState::UNKNOWN;  // 复用 R2c 四态命名
};

class RecoveryTaskCatalog {
public:
    class Builder {
    public:
        void Add(TaskSpec spec) { specs_.push_back(std::move(spec)); }
        // Build 校验：TaskId 唯一、前驱存在、无环。失败返回错误说明。
        std::optional<std::string> Build(std::unique_ptr<RecoveryTaskCatalog>* out);
    private:
        std::vector<TaskSpec> specs_;
    };

    // 只读视图——策略与调度器均不可修改目录。
    const TaskSpec* Find(uint64_t task_id) const;
    size_t Size() const { return tasks_.size(); }
    std::vector<uint64_t> AllTaskIds() const;
    // 依赖闭包（含自身）；遇到不存在/失败前驱返回 false＋err。
    bool ClosureOf(uint64_t task_id, std::vector<uint64_t>* closure,
                   std::string* err) const;
    uint64_t TotalCost() const;

private:
    friend class Builder;
    std::unordered_map<uint64_t, TaskSpec> tasks_;
    RecoveryTaskCatalog() = default;
};

// ==================== DemandSnapshot ====================
// 已接纳请求的当前阻塞视图。缺信息必须为 unknown，不能猜。
struct BlockerInfo {
    enum class Kind : uint8_t {
        kUnknown = 0,   // 缺少信息——显式 unknown，不得猜最后阻塞点
        kTask,          // 阻塞在恢复任务（task_id 有效）
        kPageLock,      // 阻塞在页锁（table_id/page_id 有效）
        kRemoteFetch,   // 阻塞在远端取页（node_id 有效）
    };
    Kind kind = Kind::kUnknown;
    uint64_t task_id = 0;
    uint32_t table_id = 0;
    uint64_t page_id = 0;
    uint32_t node_id = 0;
};

struct DemandSnapshot {
    uint64_t version = 0;                       // 快照版本（单调）
    uint64_t epoch = 0;                         // 所属恢复代
    // 已接纳请求（去重计数>0）当前 blocker；不含未来请求
    struct Demand {
        uint64_t demand_key = 0;                // 去重键（如 table+key 域）
        uint32_t dedup_count = 0;
        BlockerInfo blocker;
        bool cancelled = false;                 // 已取消
        bool migrated = false;                  // 已迁移
        // 允许使用的 B/C 有界摘要（节点 id -> 摘要字节；有界）
        std::map<uint32_t, std::string> bounded_node_summaries;
    };
    std::vector<Demand> demands;
    // 快照级 unknown 声明：哪些摘要缺失（不能以空串冒充已知）
    std::unordered_set<uint32_t> summaries_missing;
};

}  // namespace recovery_iface
