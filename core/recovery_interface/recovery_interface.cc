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

// ==================== P1 ====================

bool P1CriticalPathPolicy::Initialize(const RecoveryContext&, std::string*) { return true; }

void P1CriticalPathPolicy::OnDemandSnapshot(const DemandSnapshot& snapshot) {
    // P1 本体在 Propose 的快照参数上计算（无跨调用状态）；记录最新
    // 版本号供 P1F 差量缓存判据与诊断。
    last_snapshot_version_ = snapshot.version;
}

bool P1CriticalPathPolicy::DemandUsable(const DemandSnapshot::Demand& d) {
    // 路径完整：未取消、未迁移、blocker 类型可映射到恢复任务。
    // kRemoteFetch/kUnknown 是显式 unknown——不猜最后阻塞点（22.8）。
    if (d.cancelled || d.migrated) return false;
    return d.blocker.kind == BlockerInfo::Kind::kTask ||
           d.blocker.kind == BlockerInfo::Kind::kPageLock;
}

bool P1CriticalPathPolicy::DemandDegraded(const DemandSnapshot::Demand& d,
                                          const DemandSnapshot& snap) {
    // B/C 故障前有效采样：需求有界摘要的任一来源节点缺失 → 该需求
    // 降级（缺摘要＝显式 unknown，不能以空串冒充已知）。空摘要集＝
    // 无外部节点依赖，不降级。
    for (auto& [node, _] : d.bounded_node_summaries) {
        if (snap.summaries_missing.count(node)) return true;
    }
    return false;
}

bool P1CriticalPathPolicy::BlockerHitsTask(const BlockerInfo& b, const TaskSpec& t) {
    if (b.kind == BlockerInfo::Kind::kTask) return b.task_id == t.task_id;
    if (b.kind == BlockerInfo::Kind::kPageLock) {
        if (t.table_id != b.table_id) return false;
        if (t.key_lo >= t.key_hi) return false;  // 空键域（结构组等）不映射页锁
        return b.page_id >= t.key_lo && b.page_id < t.key_hi;
    }
    return false;  // kRemoteFetch/kUnknown 不映射
}

uint32_t P1CriticalPathPolicy::DemandCoverage(const TaskSpec& t,
                                              const DemandSnapshot& snap) {
    uint32_t best = 0;
    for (const auto& d : snap.demands) {
        if (!DemandUsable(d) || DemandDegraded(d, snap)) continue;
        if (!BlockerHitsTask(d.blocker, t)) continue;
        if (d.dedup_count > best) best = d.dedup_count;  // B3：去重计数＝公共剩余成本消减
    }
    return best;
}

PriorityProposal P1CriticalPathPolicy::Propose(const DemandSnapshot& snapshot,
                                               const RecoveryTaskCatalog& catalog,
                                               uint64_t remaining_budget) {
    PriorityProposal p;
    std::vector<uint64_t> ids = catalog.AllTaskIds();
    if (ids.empty() || remaining_budget == 0) return p;  // 空＝回公共 B0
    // 覆盖分预计算；排序：覆盖（分降序）→ B1 背景序（kind→influence→wal→id）
    std::unordered_map<uint64_t, uint32_t> cov;
    for (uint64_t id : ids) cov[id] = DemandCoverage(*catalog.Find(id), snapshot);
    std::sort(ids.begin(), ids.end(), [&](uint64_t a, uint64_t b) {
        if (cov[a] != cov[b]) return cov[a] > cov[b];  // 覆盖任务在前、分值降序
        const TaskSpec* sa = catalog.Find(a);
        const TaskSpec* sb = catalog.Find(b);
        uint64_t ka = B1RoleOrderPolicy::KindRank(*sa), kb = B1RoleOrderPolicy::KindRank(*sb);
        if (ka != kb) return ka < kb;
        uint64_t ia = B1RoleOrderPolicy::InfluenceRank(*sa), ib = B1RoleOrderPolicy::InfluenceRank(*sb);
        if (ia != ib) return ia < ib;
        if (sa->wal_end_lsn != sb->wal_end_lsn) return sa->wal_end_lsn < sb->wal_end_lsn;
        return a < b;
    });
    p.epoch = snapshot.epoch;  // 与公共底座当前恢复代一致；不一致由调度器拒收
    for (uint64_t id : ids) {
        p.groups.push_back(PriorityProposal::Group{
            {id},
            "P1 cov=" + std::to_string(cov[id])});
    }
    return p;
}

// ==================== P1F ====================

bool P1FReducedRescanPolicy::Initialize(const RecoveryContext&, std::string*) { return true; }

void P1FReducedRescanPolicy::OnDemandSnapshot(const DemandSnapshot& snapshot) {
    last_snapshot_version_ = snapshot.version;
}

PriorityProposal P1FReducedRescanPolicy::Propose(const DemandSnapshot& snapshot,
                                                 const RecoveryTaskCatalog& catalog,
                                                 uint64_t remaining_budget) {
    ++proposes_total_;
    // 缓存键＝（快照版本，目录身份，任务数，预算）。命中＝同输入免重扫
    //（CPU 计费：cache_hits_ 单列；22.9 缓存有效期＝版本变即失效）。
    if (cached_catalog_ == &catalog && cached_version_ == snapshot.version &&
        cached_task_count_ == catalog.Size() && cached_budget_ == remaining_budget &&
        !cached_proposal_.groups.empty()) {
        ++cache_hits_;
        PriorityProposal p = cached_proposal_;
        for (auto& g : p.groups) g.rationale += " [p1f cache_hit]";
        return p;
    }
    // 未命中：P1 全量识别＋排序（同 P1 语义）
    PriorityProposal p = inner_.Propose(snapshot, catalog, remaining_budget);
    if (!p.groups.empty()) {  // 空提案不缓存（预算 0/空目录语义恒定）
        cached_catalog_ = &catalog;
        cached_version_ = snapshot.version;
        cached_task_count_ = catalog.Size();
        cached_budget_ = remaining_budget;
        cached_proposal_ = p;
    }
    return p;
}

// ==================== C0 ====================

bool C0ControlPolicy::Initialize(const RecoveryContext&, std::string*) { return true; }

void C0ControlPolicy::OnDemandSnapshot(const DemandSnapshot& snapshot) {
    last_snapshot_version_ = snapshot.version;
}

PriorityProposal C0ControlPolicy::Propose(const DemandSnapshot& snapshot,
                                          const RecoveryTaskCatalog& catalog,
                                          uint64_t remaining_budget) {
    // 被对照算法（shadow_of=P1）的采样/识别照做——覆盖率逐任务计算并
    // 计数（识别开销单列）；随后恒返空提案：实际优先执行量为零，
    // 公共底座按 B0 默认序执行。
    std::vector<uint64_t> ids = catalog.AllTaskIds();
    for (uint64_t id : ids) {
        (void)P1CriticalPathPolicy::DemandCoverage(*catalog.Find(id), snapshot);
        ++identifies_total_;
    }
    (void)remaining_budget;
    return PriorityProposal{};  // 恒空——C0 的执行序永远是公共 B0
}

// ==================== B1 ====================

bool B0Policy::Initialize(const RecoveryContext&, std::string*) { return true; }

bool B1RoleOrderPolicy::Initialize(const RecoveryContext&, std::string*) { return true; }

uint64_t B1RoleOrderPolicy::KindRank(const TaskSpec& t) {
    // 可信任务类型序：数据页（0）先于索引（1）先于结构组（2）——与
    // TaskKind 枚举数值一致（recovery_interface.h 固定序：HEAP_PAGE=0,
    // INDEX_KEY_RANGE=1, STRUCTURE_GROUP=2）。策略不依赖隐式枚举序，
    // 显式映射防御枚举重排。
    switch (t.kind) {
        case TaskKind::HEAP_PAGE: return 0;
        case TaskKind::INDEX_KEY_RANGE: return 1;
        case TaskKind::STRUCTURE_GROUP: return 2;
    }
    return 3;
}

uint64_t B1RoleOrderPolicy::InfluenceRank(const TaskSpec& t) {
    // 已有角色序：有待恢复工作的 AFFECTED（0）先于已就绪的
    // RECOVERED_READY（1）——先消减残余恢复工作量，已就绪角色的发布
    // 依赖公共发布器节奏，先派发不缩短关键恢复路径。
    if (t.influence == recovery_catalog::RPageState::AFFECTED) return 0;
    if (t.influence == recovery_catalog::RPageState::RECOVERED_READY) return 1;
    return 2;  // UNAFFECTED/UNKNOWN 不应出现在恢复目录（Build 不拒绝，
               // 但排序置后；契约测试另证物化只含 AFFECTED/READY）
}

PriorityProposal B1RoleOrderPolicy::Propose(const DemandSnapshot& snapshot,
                                            const RecoveryTaskCatalog& catalog,
                                            uint64_t remaining_budget) {
    (void)snapshot;  // B1 不使用需求信息（22.9：仅可信任务类型/已有角色）
    PriorityProposal p;
    std::vector<uint64_t> ids = catalog.AllTaskIds();
    if (ids.empty() || remaining_budget == 0) return p;  // 空＝回公共 B0
    std::sort(ids.begin(), ids.end(), [&](uint64_t a, uint64_t b) {
        const TaskSpec* sa = catalog.Find(a);
        const TaskSpec* sb = catalog.Find(b);
        uint64_t ka = KindRank(*sa), kb = KindRank(*sb);
        if (ka != kb) return ka < kb;
        uint64_t ia = InfluenceRank(*sa), ib = InfluenceRank(*sb);
        if (ia != ib) return ia < ib;
        if (sa->wal_end_lsn != sb->wal_end_lsn) return sa->wal_end_lsn < sb->wal_end_lsn;
        return a < b;
    });
    p.epoch = snapshot.epoch;  // 与公共底座当前恢复代一致（server 侧
                                // snap.epoch=ctx.epoch；不一致由调度器拒收）
    for (uint64_t id : ids) {
        const TaskSpec* s = catalog.Find(id);
        p.groups.push_back(PriorityProposal::Group{
            {id},
            "B1 kind=" + std::to_string(KindRank(*s)) +
                " influence=" + std::to_string(InfluenceRank(*s)) +
                " wal=" + std::to_string(s->wal_end_lsn)});
    }
    return p;
}

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
    Register("B1", [] { return std::unique_ptr<IRecoveryPriorityPolicy>(new B1RoleOrderPolicy()); });
    Register("P1", [] { return std::unique_ptr<IRecoveryPriorityPolicy>(new P1CriticalPathPolicy()); });
    Register("P1F", [] { return std::unique_ptr<IRecoveryPriorityPolicy>(new P1FReducedRescanPolicy()); });
    Register("C0", [] { return std::unique_ptr<IRecoveryPriorityPolicy>(new C0ControlPolicy()); });
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
    // 空建议（如 B0Policy 的空提案）先于代数检查：空＝无内容，无从谈
    // 代数新旧，直接回公共 B0 默认序（b0-iface-002 实证：空提案 epoch=0
    // 被误判旧代整批拒绝 → 队列空 → fail-closed）。
    if (proposal.empty() && opts_.allow_default_b0_fallback) {
        return SubmitDefaultB0("empty proposal from " + policy_name);
    }
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
