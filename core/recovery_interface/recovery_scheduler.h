// R3 C-1（22.8）：公共调度契约层。
//
// RecoveryScheduler：校验策略建议，转换为公共执行队列。接口只决定
// 合法工作的优先顺序，不拥有正确性权限——调度器是建议到执行的唯一
// 关口，逐组校验（22.8）：
//   - 旧代：epoch != ctx.epoch 拒绝整批；
//   - 未知/重复 TaskId、依赖失败任务、循环或非法闭包、超大组
//    （组闭包超 ctx.max_group_size）、预算不可行（闭包成本和超剩余
//     预算）——逐组拒绝并隔离；
//   - 部分失败保持隔离：合法组照常入队，非法组记入 rejected 轨迹，
//     任务可继续被后续建议合法提出（重复拒绝仅对已入队/已完成任务）。
//   - 空建议＝回公共 B0：按目录序把 READY 任务编为默认组入队。
//
// RecoveryMetrics：所有策略的建议理由/成本/轨迹记录；冒烟和回归
// 契约测试中做差异断言（后续 B1/B3/P1 接入时不可删改）。
//
// RecoveryExecutor：公共执行器桩——真实日志工作、Undo、验证、可发布
// 结果判定全部由公共底座实现；本接口不提供执行入口给策略。

#pragma once

#include <deque>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "recovery_interface.h"
#include "recovery_policy.h"

namespace recovery_iface {

// ==================== RecoveryMetrics ====================
struct RecoveryMetrics {
    struct ProposalTrace {
        uint64_t seq = 0;
        uint64_t epoch = 0;
        std::string policy_name;
        size_t groups_total = 0;
        size_t groups_accepted = 0;
        std::vector<std::string> rejected_reasons;  // 逐组拒绝理由
        uint64_t cost_reserved = 0;
    };
    std::vector<ProposalTrace> traces;
    uint64_t total_proposals = 0;
    uint64_t total_accepted_groups = 0;
    uint64_t total_rejected_groups = 0;
    uint64_t total_cost_reserved = 0;
    void Record(ProposalTrace t) {
        total_proposals += 1;
        total_accepted_groups += t.groups_accepted;
        total_rejected_groups += t.rejected_reasons.size();
        total_cost_reserved += t.cost_reserved;
        traces.push_back(std::move(t));
        if (traces.size() > 128) traces.erase(traces.begin());  // 有界
    }
};

// ==================== RecoveryScheduler ====================
class RecoveryScheduler {
public:
    struct Options {
        size_t max_group_size = 64;        // 组闭包上限（共同安全配置）
        bool allow_default_b0_fallback = true;  // 空建议回公共 B0
    };

    RecoveryScheduler(RecoveryContext ctx, std::shared_ptr<const RecoveryTaskCatalog> catalog,
                      Options opts)
        : ctx_(std::move(ctx)), catalog_(std::move(catalog)), opts_(opts) {
        InitRuntime_();
    }
    RecoveryScheduler(RecoveryContext ctx, std::shared_ptr<const RecoveryTaskCatalog> catalog)
        : ctx_(std::move(ctx)), catalog_(std::move(catalog)) {
        InitRuntime_();
    }

    // 提交策略建议：返回接受的组数；逐组拒绝理由进 metrics。
    // 旧代（epoch 不匹配）整批拒绝返回 SIZE_MAX。
    size_t Submit(const PriorityProposal& proposal, const std::string& policy_name);

    // 空/无建议时由底座调用：公共 B0 默认序（目录序 READY 任务）。
    size_t SubmitDefaultB0(const std::string& reason);

    // 公共执行队列（弹出即执行；Executor 由公共底座接线）
    struct QueueItem {
        uint64_t task_id = 0;
        uint64_t group_seq = 0;
        std::string rationale;
    };
    bool PopReady(QueueItem* out);
    size_t QueueDepth() const { return queue_.size(); }
    // 模拟执行完成（测试/桩用；真实执行由公共底座驱动并回调）
    void MarkDone(uint64_t task_id);
    uint64_t RemainingBudget() const { return remaining_budget_; }
    const RecoveryMetrics& metrics() const { return metrics_; }
    TaskState StateOf(uint64_t task_id) const;   // catalog 副本视图

private:
    friend struct SchedulerAccessForTest;
    void InitRuntime_() {
        remaining_budget_ = ctx_.budget.remaining_cost_units;
        for (const auto& id : catalog_->AllTaskIds()) {
            const TaskSpec* s = catalog_->Find(id);
            runtime_state_[id] = s ? s->state : TaskState::PENDING;
        }
    }
    struct GroupDecision {
        bool ok = false;
        std::string reason;
        std::vector<uint64_t> closure;   // 入队任务（有序、去重）
        uint64_t cost = 0;
    };
    GroupDecision ValidateGroup_(const std::vector<uint64_t>& task_ids) const;
    void EnqueueGroup_(const std::vector<uint64_t>& closure, const std::string& rationale);

    RecoveryContext ctx_;   // 不可变上下文副本（预算数字除外，见下）
    std::shared_ptr<const RecoveryTaskCatalog> catalog_;
    Options opts_;
    // 任务运行态（catalog 保持只读；运行态在此）
    std::unordered_map<uint64_t, TaskState> runtime_state_;
    std::unordered_set<uint64_t> dispatched_or_done_;
    uint64_t remaining_budget_;
    uint64_t next_group_seq_ = 0;
    uint64_t trace_seq_ = 0;
    std::deque<QueueItem> queue_;
    RecoveryMetrics metrics_;
};

}  // namespace recovery_iface
