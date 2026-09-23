// R3 C-1（22.8）：策略接口层。
//
// IRecoveryPriorityPolicy：只读快照＋剩余预算下返回 PriorityProposal。
// 边界（22.8）：不执行 Redo、取页探树、Undo、发布、改日志或清 IR；
// 预算耗尽/无建议时回公共 B0，保留背景恢复进度。
//
// 策略注册：OFF、B0、B1、B3、P1、P1F、C0 显式名称；默认 OFF；未知
// 算法/非法参数启动失败；实验中冻结，不静默替换历史算法。

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "recovery_interface.h"

namespace recovery_iface {

class RecoveryScheduler;  // fwd

// ==================== PriorityProposal ====================
// 策略产出：合法任务的优先顺序建议。建议不是发布证书——仅含 TaskId
// 与顺序/理由，不携带任何执行能力。
struct PriorityProposal {
    uint64_t epoch = 0;                 // 必须匹配当前恢复代
    struct Group {
        std::vector<uint64_t> task_ids; // 组内顺序即建议顺序
        std::string rationale;          // 理由（进 RecoveryMetrics 轨迹）
    };
    std::vector<Group> groups;          // 组间顺序即建议优先级
    bool empty() const { return groups.empty(); }
};

// ==================== IRecoveryPriorityPolicy ====================
class IRecoveryPriorityPolicy {
public:
    virtual ~IRecoveryPriorityPolicy() = default;
    // 初始化（公共底座生成的上下文；不可变）
    virtual bool Initialize(const RecoveryContext& ctx, std::string* err) = 0;
    // 接收阻塞/取消/就绪事件（公共底座推送；只读）
    virtual void OnDemandSnapshot(const DemandSnapshot& snapshot) = 0;
    // 在只读快照（需求事件＋任务目录）和剩余预算下返回建议；无建议/
    // 预算耗尽返回空提案（空提案＝回公共 B0 默认顺序，由公共底座解释）。
    // 目录为公共底座物化的只读视图；策略只读、不得缓存可变执行状态。
    virtual PriorityProposal Propose(const DemandSnapshot& snapshot,
                                     const RecoveryTaskCatalog& catalog,
                                     uint64_t remaining_budget) = 0;
    virtual const char* name() const = 0;
};

// ==================== B0：接口版公共基线 ====================
// 空提案——全部任务按公共默认顺序（目录序）执行。与 R2 未启用接口
// 的原普通恢复作正确性与公共改造开销对照的基线策略；B1/B3/P1 都对
// 这个公共 B0 比较。
class B0Policy : public IRecoveryPriorityPolicy {
public:
    bool Initialize(const RecoveryContext& ctx, std::string* err) override;
    void OnDemandSnapshot(const DemandSnapshot&) override {}
    PriorityProposal Propose(const DemandSnapshot&, const RecoveryTaskCatalog&,
                             uint64_t) override {
        return PriorityProposal{};  // 空＝回公共默认顺序
    }
    const char* name() const override { return "B0"; }
};

// OFF：接口未启用（默认）。存在于此仅为注册表完备——创建即拒绝执行。
class OffPolicy : public IRecoveryPriorityPolicy {
public:
    bool Initialize(const RecoveryContext&, std::string* err) override {
        if (err) *err = "OFF: recovery policy interface disabled";
        return false;
    }
    void OnDemandSnapshot(const DemandSnapshot&) override {}
    PriorityProposal Propose(const DemandSnapshot&, const RecoveryTaskCatalog&,
                             uint64_t) override {
        return PriorityProposal{};
    }
    const char* name() const override { return "OFF"; }
};

// ==================== B1：可信任务类型/已有角色排序 ====================
// 22.9：仅按可信任务类型/已有角色排序——只使用目录已有字段（TaskKind、
// R2c influence 角色、WAL 段范围、table 归属），不采样、不取需求信息、
// 不做任何额外树/层级扫描（不能为获层级故障后免费全树扫描）。
// 排序键（依次）：TaskKind（数据页先于索引/结构组）→ influence（有待
// 恢复工作的 AFFECTED 先于已就绪的 RECOVERED_READY）→ WAL 段（页
// wal_end_lsn 升序，日志段早先）→ task_id（稳定）。逐任务单元素组，
// 不聚合（聚合需要跨任务角色证据，B1 无此信息源）。
class B1RoleOrderPolicy : public IRecoveryPriorityPolicy {
public:
    bool Initialize(const RecoveryContext& ctx, std::string* err) override;
    void OnDemandSnapshot(const DemandSnapshot&) override {}  // B1 不使用需求
    PriorityProposal Propose(const DemandSnapshot& snapshot,
                             const RecoveryTaskCatalog& catalog,
                             uint64_t remaining_budget) override;
    const char* name() const override { return "B1"; }
    // 排序键提取（公开为静态纯函数，供契约测试直接验证）
    static uint64_t KindRank(const TaskSpec& t);
    static uint64_t InfluenceRank(const TaskSpec& t);
};

// ==================== P1：关键路径页面优先 ====================
// 22.8/22.9：已到达请求当前 blocker 的去重需求（B3 原则）＋B/C 故障前
// 有效采样＋小组＋预算；路径不完整/过期降级。
// 关键路径页识别：需求 blocker（kPageLock→页域命中/kTask→任务号命中）
// 映射到覆盖该页的恢复任务；kRemoteFetch/kUnknown 不映射（不猜）。
// 需求可信＝!cancelled && !migrated；采样降级＝需求有界摘要任一来源
// 节点缺失（缺摘要＝显式 unknown）。覆盖分＝可信命中需求的最大去重
// 计数；排序＝覆盖任务（分值降序）先于未覆盖（B1 背景序）。
class P1CriticalPathPolicy : public IRecoveryPriorityPolicy {
public:
    bool Initialize(const RecoveryContext& ctx, std::string* err) override;
    void OnDemandSnapshot(const DemandSnapshot& snapshot) override;  // 记录版本（P1F 复用）
    PriorityProposal Propose(const DemandSnapshot& snapshot,
                             const RecoveryTaskCatalog& catalog,
                             uint64_t remaining_budget) override;
    const char* name() const override { return "P1"; }
    // 覆盖判定（公开静态纯函数，供契约测试直接验证）
    static bool DemandUsable(const DemandSnapshot::Demand& d);
    static bool DemandDegraded(const DemandSnapshot::Demand& d, const DemandSnapshot& snap);
    static bool BlockerHitsTask(const BlockerInfo& b, const TaskSpec& t);
    static uint32_t DemandCoverage(const TaskSpec& t, const DemandSnapshot& snap);

private:
    uint64_t last_snapshot_version_ = 0;  // OnDemandSnapshot 记录（诊断/P1F 判据）
};

// ==================== P1F：同类识别信息下减少重复扫描 ====================
// 22.9：P1 命名变体——保留 P1 排序语义；缓存有效期/容量、内存与 CPU
// 都计费（缓存命中/未命中计数暴露给公共底座日志）。缓存键＝
// （快照版本，目录身份，任务数）；键变即失效重算。
class P1FReducedRescanPolicy : public IRecoveryPriorityPolicy {
public:
    bool Initialize(const RecoveryContext& ctx, std::string* err) override;
    void OnDemandSnapshot(const DemandSnapshot& snapshot) override;
    PriorityProposal Propose(const DemandSnapshot& snapshot,
                             const RecoveryTaskCatalog& catalog,
                             uint64_t remaining_budget) override;
    const char* name() const override { return "P1F"; }
    // 计费（22.9：缓存有效期/容量、内存与 CPU 都计费）
    uint64_t proposes_total() const { return proposes_total_; }
    uint64_t cache_hits() const { return cache_hits_; }

private:
    uint64_t last_snapshot_version_ = 0;
    P1CriticalPathPolicy inner_;  // 未命中时的全量识别/排序引擎（同 P1 语义）
    const RecoveryTaskCatalog* cached_catalog_ = nullptr;
    uint64_t cached_version_ = 0;
    size_t cached_task_count_ = 0;
    uint64_t cached_budget_ = 0;
    PriorityProposal cached_proposal_;
    uint64_t proposes_total_ = 0;
    uint64_t cache_hits_ = 0;
};

// ==================== C0：被对照算法的采样/识别，按 B0 执行 ====================
// 22.9：进行被对照算法（P1，见 shadow_of）的采样/识别，但仍按公共 B0
// 执行——实际优先执行量必须为零（恒空提案）。识别开销单列计数；其
// 轨迹与 P1 不同，成本不是 P1/P1F 的精确替身（C0−B0 只解释识别成本）。
class C0ControlPolicy : public IRecoveryPriorityPolicy {
public:
    bool Initialize(const RecoveryContext& ctx, std::string* err) override;
    void OnDemandSnapshot(const DemandSnapshot& snapshot) override;
    PriorityProposal Propose(const DemandSnapshot& snapshot,
                             const RecoveryTaskCatalog& catalog,
                             uint64_t remaining_budget) override;
    const char* name() const override { return "C0"; }
    const char* shadow_of() const { return "P1"; }  // 指定所对照算法版本
    uint64_t identifies_total() const { return identifies_total_; }

private:
    uint64_t last_snapshot_version_ = 0;
    uint64_t identifies_total_ = 0;  // 已执行的识别调用数（＝任务数×Propose 次数）
};

// ==================== 策略注册/配置 ====================
// 显式名称注册；未知算法启动失败（不静默回退）；共同安全配置不可由
// 某算法放宽（max_group_size 等公共上限在 RecoveryContext，策略侧
// 无修改入口）。
class PolicyRegistry {
public:
    using Factory = std::function<std::unique_ptr<IRecoveryPriorityPolicy>()>;
    static PolicyRegistry& Instance();
    void Register(const std::string& name, Factory f);   // 重复注册拒绝
    // 未知名称返回 nullptr＋err（启动失败语义由调用方执行）
    std::unique_ptr<IRecoveryPriorityPolicy> Create(const std::string& name,
                                                    std::string* err) const;
    std::vector<std::string> RegisteredNames() const;

private:
    PolicyRegistry();
    std::map<std::string, Factory> factories_;
};

}  // namespace recovery_iface
