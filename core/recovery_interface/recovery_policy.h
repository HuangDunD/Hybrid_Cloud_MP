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
    // 在只读快照和剩余预算下返回建议；无建议/预算耗尽返回空提案
    //（空提案＝回公共 B0 默认顺序，由公共底座解释）
    virtual PriorityProposal Propose(const DemandSnapshot& snapshot,
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
    PriorityProposal Propose(const DemandSnapshot&, uint64_t) override {
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
    PriorityProposal Propose(const DemandSnapshot&, uint64_t) override {
        return PriorityProposal{};
    }
    const char* name() const override { return "OFF"; }
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
