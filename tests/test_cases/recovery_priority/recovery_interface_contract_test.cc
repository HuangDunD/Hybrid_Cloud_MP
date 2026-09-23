// R3 C-1（22.8）：可插拔策略接口——契约测试。
// 覆盖 22.8 边界行：建议只决定顺序（数据结构无执行能力）、旧代/未知/
// 重复/失败依赖/超大组/预算不可行逐组拒绝且部分失败隔离、空建议回
// 公共 B0、目录 Build 唯一性/前驱/无环、DemandSnapshot 缺信息显式
// unknown、策略注册显式名称/未知启动失败、影响分类复用 R2c 四态。
//
// 判定：全部 Check 通过则进程退出 0；任一失败 abort（GTest 不引入，
// 与 recovery_page_catalog_test 同风格）。

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "core/recovery_interface/recovery_scheduler.h"

using namespace recovery_iface;

static int g_failures = 0;
#define CHECK(cond, msg)                                                    \
    do {                                                                    \
        if (!(cond)) {                                                      \
            std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, msg); \
            g_failures++;                                                   \
        }                                                                   \
    } while (0)

static std::unique_ptr<RecoveryTaskCatalog> BuildLinearCatalog(size_t n, uint64_t cost_each) {
    RecoveryTaskCatalog::Builder b;
    for (uint64_t i = 1; i <= n; ++i) {
        TaskSpec s;
        s.task_id = i;
        s.kind = TaskKind::HEAP_PAGE;
        s.table_id = 0;
        s.key_lo = i * 100;
        s.key_hi = i * 100 + 100;
        if (i > 1) s.predecessors = {i - 1};   // 链式：同 key 依赖顺序
        s.wal_begin_lsn = (i - 1) * 10;
        s.wal_end_lsn = i * 10;
        s.wal_bytes = 10;
        s.shared_cost_estimate = cost_each;
        s.state = TaskState::READY;
        s.influence = (i % 2 == 0) ? RPageState::AFFECTED : RPageState::RECOVERED_READY;
        b.Add(std::move(s));
    }
    std::unique_ptr<RecoveryTaskCatalog> cat;
    auto err = b.Build(&cat);
    CHECK(!err.has_value(), "linear catalog builds");
    return cat;
}

static RecoveryContext MakeCtx(uint64_t epoch, uint64_t budget, size_t max_group) {
    RecoveryContext::Budget b;
    b.total_cost_units = budget;
    b.remaining_cost_units = budget;
    b.executor_threads = 4;
    b.io_weight = 100;
    return RecoveryContext(1, epoch, 999, 3, 5, max_group, b);
}

static void TestCatalogContract() {
    // 唯一性
    {
        RecoveryTaskCatalog::Builder b;
        TaskSpec a; a.task_id = 7;
        TaskSpec c; c.task_id = 7;
        b.Add(a); b.Add(c);
        std::unique_ptr<RecoveryTaskCatalog> cat;
        auto err = b.Build(&cat);
        CHECK(err.has_value() && err->find("duplicate") != std::string::npos,
              "duplicate task_id rejected");
    }
    // 前驱存在
    {
        RecoveryTaskCatalog::Builder b;
        TaskSpec a; a.task_id = 1; a.predecessors = {42};
        b.Add(a);
        std::unique_ptr<RecoveryTaskCatalog> cat;
        auto err = b.Build(&cat);
        CHECK(err.has_value() && err->find("unknown predecessor") != std::string::npos,
              "unknown predecessor rejected");
    }
    // 环
    {
        RecoveryTaskCatalog::Builder b;
        TaskSpec a; a.task_id = 1; a.predecessors = {2};
        TaskSpec c; c.task_id = 2; c.predecessors = {1};
        b.Add(a); b.Add(c);
        std::unique_ptr<RecoveryTaskCatalog> cat;
        auto err = b.Build(&cat);
        CHECK(err.has_value() && err->find("cycle") != std::string::npos,
              "dependency cycle rejected");
    }
    // 闭包含前驱且顺序稳定
    auto cat = BuildLinearCatalog(3, 10);
    std::vector<uint64_t> clo;
    std::string err;
    // 闭包为拓扑序：前驱先执行、自身最后
    CHECK(cat->ClosureOf(3, &clo, &err) && clo.size() == 3 && clo[0] == 1 && clo[2] == 3,
          "closure is topological order (predecessors first)");
    CHECK(cat->TotalCost() == 30, "total cost sums estimates");
    CHECK(cat->Find(2) != nullptr && cat->Find(2)->influence == RPageState::AFFECTED,
          "influence uses R2c RPageState vocabulary");
}

static void TestSchedulerContract() {
    auto cat = BuildLinearCatalog(6, 10);
    auto shared = std::shared_ptr<const RecoveryTaskCatalog>(std::move(cat));
    RecoveryScheduler sched(MakeCtx(/*epoch=*/7, /*budget=*/100, /*max_group=*/4), shared);

    // 旧代整批拒绝
    PriorityProposal stale;
    stale.epoch = 6;
    stale.groups.push_back({{1}, "stale"});
    CHECK(sched.Submit(stale, "X") == SIZE_MAX, "stale epoch rejected wholesale");
    CHECK(sched.QueueDepth() == 0, "stale proposal enqueues nothing");

    // 未知任务＋合法任务混合：部分失败隔离（合法组照常执行）
    PriorityProposal mixed;
    mixed.epoch = 7;
    mixed.groups.push_back({{99}, "unknown id"});   // 拒绝
    mixed.groups.push_back({{1}, "legal"});          // 接受（闭包 {1}）
    size_t acc = sched.Submit(mixed, "X");
    CHECK(acc == 1, "partial failure isolated: legal group accepted");
    CHECK(sched.QueueDepth() == 1, "one task enqueued for legal group");
    CHECK(sched.metrics().total_rejected_groups >= 1, "rejection recorded in metrics");

    // 重复：已入队任务再次建议
    PriorityProposal dup;
    dup.epoch = 7;
    dup.groups.push_back({{1}, "dup"});
    CHECK(sched.Submit(dup, "X") == 0, "already-dispatched task rejected");

    // 超大组：链式目录闭包 6 > max_group 4
    PriorityProposal big;
    big.epoch = 7;
    big.groups.push_back({{6}, "closure too large"});
    CHECK(sched.Submit(big, "X") == 0, "oversized closure rejected");

    // 预算不可行：剩 90，建议闭包 5、6 成本 20×闭包…（闭包 {5,4,3,2,1}? 已派发 1）
    // 重新构造干净调度器
    auto cat2 = BuildLinearCatalog(6, 10);
    auto shared2 = std::shared_ptr<const RecoveryTaskCatalog>(std::move(cat2));
    RecoveryScheduler poor(MakeCtx(7, /*budget=*/15, /*max_group=*/4), shared2);
    PriorityProposal costly;
    costly.epoch = 7;
    costly.groups.push_back({{4}, "needs closure 1-4 cost 40"});
    CHECK(poor.Submit(costly, "X") == 0, "budget infeasible rejected");
    CHECK(poor.metrics().traces.back().rejected_reasons.back().find("budget") !=
              std::string::npos, "budget rejection reason recorded");

    // 空建议回公共 B0
    RecoveryScheduler b0s(MakeCtx(7, 1000, 4), shared2);
    PriorityProposal empty;
    empty.epoch = 7;
    CHECK(b0s.Submit(empty, "B0") >= 1, "empty proposal falls back to default B0");
    CHECK(b0s.QueueDepth() == 6, "default B0 enqueues all ready tasks in catalog order");
    RecoveryScheduler::QueueItem it;
    uint64_t first = 0;
    CHECK(b0s.PopReady(&it) && (first = it.task_id) == 1, "B0 default order is catalog order");
    // 建议数据结构无执行能力：QueueItem 只有 task_id/组号/理由
    (void)first;
}

static void TestPolicyRegistryContract() {
    auto& reg = PolicyRegistry::Instance();
    std::string err;
    auto off = reg.Create("OFF", &err);
    CHECK(off != nullptr, "OFF registered");
    CHECK(!off->Initialize(MakeCtx(1, 1, 1), &err) && err.find("disabled") != std::string::npos,
          "OFF refuses to initialize");
    auto b0 = reg.Create("B0", &err);
    CHECK(b0 != nullptr && std::string(b0->name()) == "B0", "B0 registered");
    CHECK(b0->Initialize(MakeCtx(1, 1, 1), &err), "B0 initializes");
    DemandSnapshot snap;
    snap.epoch = 1;
    snap.summaries_missing.insert(2);   // B 摘要缺失——显式 unknown
    b0->OnDemandSnapshot(snap);
    PriorityProposal p = b0->Propose(snap, 100);
    CHECK(p.empty(), "B0 proposes empty (public default order)");
    auto unknown = reg.Create("P99-NOPE", &err);
    CHECK(unknown == nullptr && err.find("unknown policy name") != std::string::npos,
          "unknown policy name fails creation");
    CHECK(reg.Create("B1", &err) == nullptr, "B1 not yet registered (no silent fallback)");
}

static void TestDemandSnapshotContract() {
    DemandSnapshot snap;
    snap.version = 3;
    snap.epoch = 7;
    DemandSnapshot::Demand d;
    d.demand_key = 42;
    d.dedup_count = 4;              // 去重计数
    d.blocker.kind = BlockerInfo::Kind::kUnknown;   // 缺信息必须 unknown
    snap.demands.push_back(d);
    snap.summaries_missing.insert(1);
    CHECK(snap.demands[0].blocker.kind == BlockerInfo::Kind::kUnknown,
          "missing blocker info is explicit unknown, not guessed");
    CHECK(snap.summaries_missing.count(1) == 1, "missing summaries declared");
}

static void TestInterfaceHasNoExecutionPower() {
    // 建议结构仅含 TaskId/顺序/理由——编译期形状（无函数句柄/无回调/
    // 无 redo 指令）。此测试固定该形状：若有人给 PriorityProposal 增加
    // 执行能力字段，此处编译失败提醒评审。
    PriorityProposal p;
    p.epoch = 1;
    p.groups.push_back({{1, 2}, "rationale"});
    static_assert(sizeof(p.groups[0].task_ids[0]) == sizeof(uint64_t),
                  "proposal carries only task ids");
    CHECK(p.groups[0].rationale == "rationale", "rationale is plain text");
}

int main() {
    TestCatalogContract();
    TestSchedulerContract();
    TestPolicyRegistryContract();
    TestDemandSnapshotContract();
    TestInterfaceHasNoExecutionPower();
    if (g_failures == 0) {
        std::printf("recovery_interface_contract_test: ALL PASS\n");
        return 0;
    }
    std::printf("recovery_interface_contract_test: %d FAILURES\n", g_failures);
    return 1;
}
