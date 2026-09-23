// R3 C-1（22.8）：接口契约层实现（数据契约＋策略注册＋调度校验）。

#include "recovery_scheduler.h"

#include <algorithm>

namespace recovery_iface {

// ==================== RecoveryTaskCatalog ====================

std::optional<std::string> RecoveryTaskCatalog::Builder::Build(
    std::unique_ptr<RecoveryTaskCatalog>* out) {
    std::unordered_map<uint64_t, TaskSpec> tmp;
    for (auto& s : specs_) {
        if (tmp.count(s.task_id)) {
            return std::string("duplicate task_id ") + std::to_string(s.task_id);
        }
        tmp.emplace(s.task_id, std::move(s));
    }
    specs_.clear();
    // 前驱存在性
    for (const auto& [id, s] : tmp) {
        for (uint64_t p : s.predecessors) {
            if (!tmp.count(p)) {
                return std::string("task ") + std::to_string(id) + " unknown predecessor " +
                       std::to_string(p);
            }
        }
    }
    // 环检测（迭代三色）
    enum { WHITE = 0, GRAY = 1, BLACK = 2 };
    std::unordered_map<uint64_t, int> color;
    for (const auto& [id, _] : tmp) {
        if (color[id] != WHITE) continue;
        std::vector<uint64_t> stack{id};
        while (!stack.empty()) {
            uint64_t cur = stack.back();
            if (color[cur] == WHITE) {
                color[cur] = GRAY;
                for (uint64_t p : tmp.at(cur).predecessors) {
                    if (color[p] == GRAY) {
                        return std::string("dependency cycle at task ") + std::to_string(p);
                    }
                    if (color[p] == WHITE) stack.push_back(p);
                }
            } else if (color[cur] == GRAY) {
                color[cur] = BLACK;
                stack.pop_back();
            } else {
                stack.pop_back();
            }
        }
    }
    auto cat = std::unique_ptr<RecoveryTaskCatalog>(new RecoveryTaskCatalog());
    cat->tasks_ = std::move(tmp);
    *out = std::move(cat);
    return std::nullopt;
}

const TaskSpec* RecoveryTaskCatalog::Find(uint64_t task_id) const {
    auto it = tasks_.find(task_id);
    return it == tasks_.end() ? nullptr : &it->second;
}

std::vector<uint64_t> RecoveryTaskCatalog::AllTaskIds() const {
    std::vector<uint64_t> ids;
    ids.reserve(tasks_.size());
    for (const auto& [id, _] : tasks_) ids.push_back(id);
    return ids;
}

bool RecoveryTaskCatalog::ClosureOf(uint64_t task_id, std::vector<uint64_t>* closure,
                                    std::string* err) const {
    closure->clear();
    std::vector<uint64_t> stack{task_id};
    std::unordered_set<uint64_t> seen;
    while (!stack.empty()) {
        uint64_t cur = stack.back();
        stack.pop_back();
        if (seen.count(cur)) continue;
        const TaskSpec* s = Find(cur);
        if (s == nullptr) {
            if (err) *err = "unknown task " + std::to_string(cur) + " in closure";
            return false;
        }
        if (s->state == TaskState::FAILED) {
            if (err) *err = "task " + std::to_string(cur) + " failed; dependency illegal";
            return false;
        }
        seen.insert(cur);
        closure->push_back(cur);
        for (uint64_t p : s->predecessors) stack.push_back(p);
    }
    // 稳定顺序：自身最先（其余前驱按加入序即可，无环已由 Build 保证）
    std::reverse(closure->begin(), closure->end());
    return true;
}

uint64_t RecoveryTaskCatalog::TotalCost() const {
    uint64_t c = 0;
    for (const auto& [_, s] : tasks_) c += s.shared_cost_estimate;
    return c;
}

// ==================== B0 ====================

bool B0Policy::Initialize(const RecoveryContext&, std::string*) { return true; }

// ==================== PolicyRegistry ====================

PolicyRegistry& PolicyRegistry::Instance() {
    static PolicyRegistry inst;
    return inst;
}

void PolicyRegistry::Register(const std::string& name, Factory f) {
    if (factories_.count(name)) return;  // 重复注册拒绝（保留首个）
    factories_[name] = std::move(f);
}

std::unique_ptr<IRecoveryPriorityPolicy> PolicyRegistry::Create(const std::string& name,
                                                                std::string* err) const {
    auto it = factories_.find(name);
    if (it == factories_.end()) {
        if (err) *err = "unknown policy name: " + name + " (registered: " +
                            [&] {
                                std::string s;
                                for (const auto& [n, _] : factories_) {
                                    if (!s.empty()) s += ",";
                                    s += n;
                                }
                                return s;
                            }() +
                            ")";
        return nullptr;
    }
    return it->second();
}

std::vector<std::string> PolicyRegistry::RegisteredNames() const {
    std::vector<std::string> names;
    for (const auto& [n, _] : factories_) names.push_back(n);
    return names;
}

PolicyRegistry::PolicyRegistry() {
    Register("OFF", [] { return std::unique_ptr<IRecoveryPriorityPolicy>(new OffPolicy()); });
    Register("B0", [] { return std::unique_ptr<IRecoveryPriorityPolicy>(new B0Policy()); });
}

// ==================== RecoveryScheduler ====================
//（构造为头文件内联：InitRuntime_）

RecoveryScheduler::GroupDecision RecoveryScheduler::ValidateGroup_(
    const std::vector<uint64_t>& task_ids) const {
    GroupDecision d;
    // 未知 TaskId
    std::unordered_set<uint64_t> in_group;
    for (uint64_t t : task_ids) {
        if (catalog_->Find(t) == nullptr) {
            d.reason = "unknown task " + std::to_string(t);
            return d;
        }
        if (!in_group.insert(t).second) {
            d.reason = "duplicate task in group " + std::to_string(t);
            return d;
        }
    }
    // 重复调度：组目标已入队/已完成（22.8 拒绝重复）
    for (uint64_t t : task_ids) {
        if (dispatched_or_done_.count(t)) {
            d.reason = "task already dispatched/done " + std::to_string(t);
            return d;
        }
    }
    // 依赖闭包（未知/失败前驱拒绝）；闭包序＝拓扑序（前驱先执行）
    std::vector<uint64_t> ordered;
    std::unordered_set<uint64_t> seen;
    for (uint64_t t : task_ids) {
        std::vector<uint64_t> clo;
        std::string err;
        if (!catalog_->ClosureOf(t, &clo, &err)) {
            d.reason = err;
            return d;
        }
        for (uint64_t c : clo) {
            if (!seen.count(c)) {
                seen.insert(c);
                // 已派发/已完成前驱跳过（依赖处理中/完成，非重复调度）
                if (!dispatched_or_done_.count(c)) ordered.push_back(c);
            }
        }
    }
    // 循环/非法闭包（环由 Build 保证；FAILED 依赖已在 ClosureOf 拒绝）
    // 超大组（闭包大小）
    if (ordered.size() > ctx_.max_group_size) {
        d.reason = "group closure too large: " + std::to_string(ordered.size()) + " > " +
                   std::to_string(ctx_.max_group_size);
        return d;
    }
    // 预算
    uint64_t cost = 0;
    for (uint64_t t : ordered) cost += catalog_->Find(t)->shared_cost_estimate;
    if (cost > remaining_budget_) {
        d.reason = "budget infeasible: need " + std::to_string(cost) + " have " +
                   std::to_string(remaining_budget_);
        return d;
    }
    d.ok = true;
    d.closure = std::move(ordered);
    d.cost = cost;
    return d;
}

void RecoveryScheduler::EnqueueGroup_(const std::vector<uint64_t>& closure,
                                      const std::string& rationale) {
    for (uint64_t t : closure) {
        runtime_state_[t] = TaskState::DISPATCHED;
        dispatched_or_done_.insert(t);
        queue_.push_back(QueueItem{t, next_group_seq_, rationale});
    }
    next_group_seq_ += 1;
}

size_t RecoveryScheduler::Submit(const PriorityProposal& proposal, const std::string& policy_name) {
    if (proposal.epoch != ctx_.epoch) {
        RecoveryMetrics::ProposalTrace tr;
        tr.seq = ++trace_seq_;
        tr.epoch = proposal.epoch;
        tr.policy_name = policy_name;
        tr.groups_total = proposal.groups.size();
        tr.rejected_reasons.push_back("stale epoch " + std::to_string(proposal.epoch) +
                                      " != " + std::to_string(ctx_.epoch));
        metrics_.Record(std::move(tr));
        return SIZE_MAX;  // 旧代整批拒绝
    }
    if (proposal.empty() && opts_.allow_default_b0_fallback) {
        return SubmitDefaultB0("empty proposal from " + policy_name);
    }
    RecoveryMetrics::ProposalTrace tr;
    tr.seq = ++trace_seq_;
    tr.epoch = proposal.epoch;
    tr.policy_name = policy_name;
    tr.groups_total = proposal.groups.size();
    size_t accepted = 0;
    for (const auto& g : proposal.groups) {
        GroupDecision d = ValidateGroup_(g.task_ids);
        if (d.ok) {
            remaining_budget_ -= d.cost;
            tr.cost_reserved += d.cost;
            EnqueueGroup_(d.closure, g.rationale);
            accepted += 1;
        } else {
            tr.rejected_reasons.push_back(d.reason);
        }
    }
    tr.groups_accepted = accepted;
    metrics_.Record(std::move(tr));
    return accepted;
}

size_t RecoveryScheduler::SubmitDefaultB0(const std::string& reason) {
    // 公共 B0：目录序（task_id 升序）把未派发任务编为默认组入队
    std::vector<uint64_t> ready;
    for (uint64_t id : catalog_->AllTaskIds()) {
        if (!dispatched_or_done_.count(id)) ready.push_back(id);
    }
    if (ready.empty()) return 0;
    std::sort(ready.begin(), ready.end());
    RecoveryMetrics::ProposalTrace tr;
    tr.seq = ++trace_seq_;
    tr.epoch = ctx_.epoch;
    tr.policy_name = "B0-default";
    tr.groups_total = 1;
    // 按 max_group_size 分片为默认组
    size_t accepted = 0;
    for (size_t i = 0; i < ready.size(); i += ctx_.max_group_size) {
        std::vector<uint64_t> slice(ready.begin() + i,
                                    ready.begin() + std::min(ready.size(), i + ctx_.max_group_size));
        GroupDecision d = ValidateGroup_(slice);
        if (d.ok) {
            remaining_budget_ -= d.cost;
            tr.cost_reserved += d.cost;
            EnqueueGroup_(d.closure, reason);
            accepted += 1;
        } else {
            tr.rejected_reasons.push_back(d.reason);
        }
    }
    tr.groups_accepted = accepted;
    metrics_.Record(std::move(tr));
    return accepted;
}

bool RecoveryScheduler::PopReady(QueueItem* out) {
    if (queue_.empty()) return false;
    *out = queue_.front();
    queue_.pop_front();
    runtime_state_[out->task_id] = TaskState::EXECUTING;
    return true;
}

void RecoveryScheduler::MarkDone(uint64_t task_id) {
    runtime_state_[task_id] = TaskState::DONE;
    (void)task_id;
}

TaskState RecoveryScheduler::StateOf(uint64_t task_id) const {
    auto it = runtime_state_.find(task_id);
    return it == runtime_state_.end() ? TaskState::FAILED : it->second;
}

}  // namespace recovery_iface
