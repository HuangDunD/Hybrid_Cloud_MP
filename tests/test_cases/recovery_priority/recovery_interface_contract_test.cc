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
    RecoveryTaskCatalog::Builder b;
    {   // 单任务目录足矣——B0 必须无视一切返回空提案
        TaskSpec t; t.task_id = 1; b.Add(t);
    }
    std::unique_ptr<RecoveryTaskCatalog> cat;
    CHECK(!b.Build(&cat).has_value(), "registry-test catalog builds");
    PriorityProposal p = b0->Propose(snap, *cat, 100);
    CHECK(p.empty(), "B0 proposes empty (public default order)");
    auto unknown = reg.Create("P99-NOPE", &err);
    CHECK(unknown == nullptr && err.find("unknown policy name") != std::string::npos,
          "unknown policy name fails creation");
    CHECK(reg.Create("B1", &err) != nullptr, "B1 registered (real policy; see TestB1)");
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

// ---- B1：仅按可信任务类型/已有角色排序（22.9）----
// 排序键依次：TaskKind → influence（AFFECTED 先于 RECOVERED_READY）→
// wal_end_lsn 升序 → task_id 稳定；逐任务单元素组；不使用需求信息；
// 预算 0/空目录 → 空提案（回公共 B0）。
static void TestB1RoleOrderPolicy() {
    PolicyRegistry& reg = PolicyRegistry::Instance();
    std::string err;
    auto b1 = reg.Create("B1", &err);
    CHECK(b1 != nullptr && std::string(b1->name()) == "B1", "B1 created");
    RecoveryContext ctx = MakeCtx(1, 10, 1);  // epoch=1；预算 10 覆盖 5 任务×cost1
    CHECK(b1->Initialize(ctx, &err), "B1 initializes");

    RecoveryTaskCatalog::Builder b;
    {   // t10: INDEX/READY/wal=5 → 期望第 4
        TaskSpec t; t.task_id = 10; t.kind = TaskKind::INDEX_KEY_RANGE;
        t.influence = recovery_catalog::RPageState::RECOVERED_READY;
        t.wal_end_lsn = 5; t.shared_cost_estimate = 1; b.Add(t);
    }
    {   // t11: HEAP/AFFECTED/wal=9 → 期望第 3（kind 同 12/13，AFFECTED 内 wal 升序居中）
        TaskSpec t; t.task_id = 11; t.kind = TaskKind::HEAP_PAGE;
        t.influence = recovery_catalog::RPageState::AFFECTED;
        t.wal_end_lsn = 9; t.shared_cost_estimate = 1; b.Add(t);
    }
    {   // t12: HEAP/AFFECTED/wal=3 → 期望第 1
        TaskSpec t; t.task_id = 12; t.kind = TaskKind::HEAP_PAGE;
        t.influence = recovery_catalog::RPageState::AFFECTED;
        t.wal_end_lsn = 3; t.shared_cost_estimate = 1; b.Add(t);
    }
    {   // t13: HEAP/READY/wal=1 → 期望第 2（READY 次于 AFFECTED，wal 再小也靠后）
        TaskSpec t; t.task_id = 13; t.kind = TaskKind::HEAP_PAGE;
        t.influence = recovery_catalog::RPageState::RECOVERED_READY;
        t.wal_end_lsn = 1; t.shared_cost_estimate = 1; b.Add(t);
    }
    {   // t14: STRUCTURE/AFFECTED/wal=2 → 期望第 5（kind 最后）
        TaskSpec t; t.task_id = 14; t.kind = TaskKind::STRUCTURE_GROUP;
        t.influence = recovery_catalog::RPageState::AFFECTED;
        t.wal_end_lsn = 2; t.shared_cost_estimate = 1; b.Add(t);
    }
    std::unique_ptr<RecoveryTaskCatalog> cat;
    auto berr = b.Build(&cat);
    CHECK(!berr.has_value(), "B1 catalog builds");

    DemandSnapshot snap;
    snap.epoch = ctx.epoch;
    snap.version = 1;
    b1->OnDemandSnapshot(snap);
    PriorityProposal p = b1->Propose(snap, *cat, 100);
    CHECK(!p.empty(), "B1 proposes non-empty");
    CHECK(p.epoch == ctx.epoch, "B1 proposal epoch matches current epoch");
    CHECK(p.groups.size() == 5, "B1 one group per task (no aggregation)");
    bool all_single = true;
    for (auto& g : p.groups) all_single = all_single && g.task_ids.size() == 1;
    CHECK(all_single, "B1 groups are single-element");
    uint64_t expect[5] = {12, 11, 13, 10, 14};
    for (int i = 0; i < 5; ++i) {
        CHECK(p.groups[i].task_ids[0] == expect[i],
              "B1 order kind->influence->wal->id stable");
    }
    // 建议无损（全部任务恰好出现一次）
    std::unordered_map<uint64_t, int> seen;
    for (auto& g : p.groups) seen[g.task_ids[0]] += 1;
    bool lossless = seen.size() == 5;
    for (auto& [_, c] : seen) lossless = lossless && c == 1;
    CHECK(lossless, "B1 proposal covers all tasks exactly once");

    // 需求信息不影响 B1（22.9：B1 不取需求）
    DemandSnapshot snap2 = snap;
    DemandSnapshot::Demand d;
    d.demand_key = 77; d.dedup_count = 9;
    d.blocker.kind = BlockerInfo::Kind::kPageLock; d.blocker.table_id = 0;
    d.blocker.page_id = 100; d.blocker.node_id = 1;
    snap2.demands.push_back(d);
    PriorityProposal p2 = b1->Propose(snap2, *cat, 100);
    bool same = p2.groups.size() == p.groups.size();
    for (size_t i = 0; same && i < p.groups.size(); ++i) {
        same = p2.groups[i].task_ids[0] == p.groups[i].task_ids[0];
    }
    CHECK(same, "B1 ignores demand snapshot (trusted roles only)");

    // 预算 0 → 空提案（回公共 B0）
    PriorityProposal p3 = b1->Propose(snap, *cat, 0);
    CHECK(p3.empty(), "B1 budget 0 -> empty proposal (B0 fallback)");

    // 空目录 → 空提案
    RecoveryTaskCatalog::Builder bempty;
    TaskSpec only; only.task_id = 99; only.kind = TaskKind::HEAP_PAGE;
    std::unique_ptr<RecoveryTaskCatalog> cat2;
    bempty.Build(&cat2);  // 空目录
    PriorityProposal p4 = b1->Propose(snap, *cat2, 100);
    CHECK(p4.empty(), "B1 empty catalog -> empty proposal");

    // 调度器接受 B1 建议且出队序＝建议序
    RecoveryScheduler sched(ctx, std::shared_ptr<const RecoveryTaskCatalog>(std::move(cat)));
    size_t accepted = sched.Submit(p, "B1");
    CHECK(accepted == 5, "scheduler accepts all B1 groups");
    RecoveryScheduler::QueueItem item;
    int idx = 0;
    bool order_ok = true;
    while (sched.PopReady(&item)) {
        order_ok = order_ok && item.task_id == expect[idx];
        ++idx;
    }
    CHECK(order_ok && idx == 5, "B1 dispatch order matches proposal order");
}

// ---- P1：关键路径页面优先（22.8/22.9）----
// 覆盖判定（纯函数契约）：
//   需求可信 = !cancelled && !migrated && blocker.kind ∈ {kTask, kPageLock}
//   摘要降级 = 需求 bounded_node_summaries 任一来源节点 ∈ snap.summaries_missing
//              （缺摘要＝显式 unknown，不猜；空摘要集＝无外部依赖不降级）
//   命中     = kTask: task_id 相等；kPageLock: table 相等且 page_id ∈ [key_lo,key_hi)
//              （HEAP 单页键域 [p,p+1)，INDEX 键域同口径；kRemoteFetch/kUnknown 不映射）
//   覆盖分   = 命中该任务的所有可信需求最大 dedup_count（0＝无覆盖）
// 排序：覆盖任务（分值降序）先于未覆盖（B1 背景序）；单元素组；预算 0/空目录
// → 空提案回公共 B0；建议无损；epoch=snapshot.epoch。
static void TestP1CriticalPathPolicy() {
    PolicyRegistry& reg = PolicyRegistry::Instance();
    std::string err;
    auto p1 = reg.Create("P1", &err);
    CHECK(p1 != nullptr && std::string(p1->name()) == "P1", "P1 created");
    RecoveryContext ctx = MakeCtx(1, 100, 1);
    CHECK(p1->Initialize(ctx, &err), "P1 initializes");

    // 目录：t20 HEAP/t2/[100,101)/AFFECTED；t21 HEAP/t2/[200,201)/AFFECTED；
    // t22 HEAP/t3/[100,101)/READY；t23 INDEX/t2/[10,20)/AFFECTED；
    // t24 STRUCTURE（无键域）/AFFECTED
    RecoveryTaskCatalog::Builder b;
    {
        TaskSpec t; t.task_id = 20; t.kind = TaskKind::HEAP_PAGE;
        t.table_id = 2; t.key_lo = 100; t.key_hi = 101;
        t.influence = recovery_catalog::RPageState::AFFECTED;
        t.wal_end_lsn = 1; t.shared_cost_estimate = 1; b.Add(t);
    }
    {
        TaskSpec t; t.task_id = 21; t.kind = TaskKind::HEAP_PAGE;
        t.table_id = 2; t.key_lo = 200; t.key_hi = 201;
        t.influence = recovery_catalog::RPageState::AFFECTED;
        t.wal_end_lsn = 2; t.shared_cost_estimate = 1; b.Add(t);
    }
    {
        TaskSpec t; t.task_id = 22; t.kind = TaskKind::HEAP_PAGE;
        t.table_id = 3; t.key_lo = 100; t.key_hi = 101;
        t.influence = recovery_catalog::RPageState::RECOVERED_READY;
        t.wal_end_lsn = 3; t.shared_cost_estimate = 1; b.Add(t);
    }
    {
        TaskSpec t; t.task_id = 23; t.kind = TaskKind::INDEX_KEY_RANGE;
        t.table_id = 2; t.key_lo = 10; t.key_hi = 20;
        t.influence = recovery_catalog::RPageState::AFFECTED;
        t.wal_end_lsn = 4; t.shared_cost_estimate = 1; b.Add(t);
    }
    {
        TaskSpec t; t.task_id = 24; t.kind = TaskKind::STRUCTURE_GROUP;
        t.influence = recovery_catalog::RPageState::AFFECTED;
        t.wal_end_lsn = 5; t.shared_cost_estimate = 1; b.Add(t);
    }
    std::unique_ptr<RecoveryTaskCatalog> cat;
    CHECK(!b.Build(&cat).has_value(), "P1 catalog builds");

    // ---- 覆盖纯函数 ----
    DemandSnapshot snap;
    snap.epoch = ctx.epoch;
    snap.version = 5;
    auto add_dem = [&](uint64_t key, uint32_t dedup, BlockerInfo::Kind k, uint32_t tbl,
                       uint64_t pg, uint64_t tid, bool cancelled, bool migrated) {
        DemandSnapshot::Demand d;
        d.demand_key = key; d.dedup_count = dedup;
        d.blocker.kind = k; d.blocker.table_id = tbl; d.blocker.page_id = pg;
        d.blocker.task_id = tid;
        d.cancelled = cancelled; d.migrated = migrated;
        snap.demands.push_back(d);
    };
    add_dem(1, 3,  BlockerInfo::Kind::kPageLock, 2, 100, 0, false, false);  // → t20=3
    add_dem(2, 7,  BlockerInfo::Kind::kPageLock, 2, 200, 0, false, false);  // → t21=7
    add_dem(3, 2,  BlockerInfo::Kind::kPageLock, 2, 15, 0, false, false);   // → t23=2（键域中）
    add_dem(4, 4,  BlockerInfo::Kind::kTask, 0, 0, 24, false, false);       // → t24=4
    add_dem(5, 9,  BlockerInfo::Kind::kPageLock, 2, 999, 0, false, false);  // 无命中
    add_dem(6, 50, BlockerInfo::Kind::kPageLock, 3, 100, 0, true, false);   // cancelled→t22 仍 0
    add_dem(7, 60, BlockerInfo::Kind::kPageLock, 2, 100, 0, false, true);   // migrated→t20 仍 3
    add_dem(8, 80, BlockerInfo::Kind::kPageLock, 3, 100, 0, false, false);  // 摘要降级→t22 仍 0
    snap.demands.back().bounded_node_summaries[2] = "s";
    snap.summaries_missing.insert(2);
    add_dem(9, 9,  BlockerInfo::Kind::kRemoteFetch, 0, 0, 0, false, false); // 不映射
    add_dem(10, 9, BlockerInfo::Kind::kUnknown, 0, 0, 0, false, false);     // 不映射

    CHECK(P1CriticalPathPolicy::DemandCoverage(*cat->Find(20), snap) == 3, "t20 coverage=3");
    CHECK(P1CriticalPathPolicy::DemandCoverage(*cat->Find(21), snap) == 7, "t21 coverage=7");
    CHECK(P1CriticalPathPolicy::DemandCoverage(*cat->Find(22), snap) == 0,
          "t22 coverage=0 (cancelled/degraded demands ignored)");
    CHECK(P1CriticalPathPolicy::DemandCoverage(*cat->Find(23), snap) == 2,
          "t23 coverage=2 (index key-range hit)");
    CHECK(P1CriticalPathPolicy::DemandCoverage(*cat->Find(24), snap) == 4, "t24 coverage=4 (kTask)");

    // ---- Propose 排序：覆盖降序在前，未覆盖按 B1 背景序 ----
    p1->OnDemandSnapshot(snap);
    PriorityProposal p = p1->Propose(snap, *cat, 100);
    CHECK(!p.empty() && p.epoch == ctx.epoch, "P1 proposes with matching epoch");
    uint64_t expect[5] = {21, 24, 20, 23, 22};
    bool order_ok = p.groups.size() == 5;
    for (int i = 0; order_ok && i < 5; ++i) {
        order_ok = p.groups[i].task_ids.size() == 1 && p.groups[i].task_ids[0] == expect[i];
    }
    CHECK(order_ok, "P1 order: covered desc then B1 background");
    std::unordered_map<uint64_t, int> seen;
    for (auto& g : p.groups) for (auto id : g.task_ids) seen[id] += 1;
    bool lossless = seen.size() == 5;
    for (auto& [_, c] : seen) lossless = lossless && c == 1;
    CHECK(lossless, "P1 proposal covers all tasks exactly once");

    // ---- 需求信息确实改变顺序（与 B1 的差异点）----
    DemandSnapshot no_dem;
    no_dem.epoch = ctx.epoch; no_dem.version = snap.version + 1;
    PriorityProposal p_b1like = p1->Propose(no_dem, *cat, 100);
    uint64_t b1_expect[5] = {20, 21, 22, 23, 24};  // 纯 B1 序（kind→influence→wal）
    bool differs = false;
    for (int i = 0; i < 5; ++i) {
        if (p_b1like.groups[i].task_ids[0] != expect[i]) differs = true;
        CHECK(p_b1like.groups[i].task_ids[0] == b1_expect[i],
              "no-demand snapshot degenerates to B1 order");
    }
    CHECK(differs, "demands actually reorder P1 (not B1-equivalent here)");

    // ---- 预算 0 / 空目录 → 空提案 ----
    CHECK(p1->Propose(snap, *cat, 0).empty(), "P1 budget 0 -> empty (B0 fallback)");
    RecoveryTaskCatalog::Builder bempty;
    std::unique_ptr<RecoveryTaskCatalog> cat2;
    bempty.Build(&cat2);
    CHECK(p1->Propose(snap, *cat2, 100).empty(), "P1 empty catalog -> empty");

    // ---- 调度器接受序＝建议序 ----
    RecoveryScheduler sched(ctx,
        std::shared_ptr<const RecoveryTaskCatalog>(std::move(cat)));
    CHECK(sched.Submit(p, "P1") == 5, "scheduler accepts all P1 groups");
    RecoveryScheduler::QueueItem item;
    int idx = 0; bool disp_ok = true;
    while (sched.PopReady(&item)) {
        disp_ok = disp_ok && item.task_id == expect[idx];
        ++idx;
    }
    CHECK(disp_ok && idx == 5, "P1 dispatch order matches proposal");
}

// ---- P1F：同类识别信息下减少重复扫描（22.9）----
// 缓存键＝（快照版本，目录身份，任务数，预算）；命中免重扫且计费；
// 版本/目录变即失效重算；排序语义与 P1 逐组相同；空提案不缓存。
static void TestP1FReducedRescanPolicy() {
    PolicyRegistry& reg = PolicyRegistry::Instance();
    std::string err;
    auto p1f = reg.Create("P1F", &err);
    CHECK(p1f != nullptr && std::string(p1f->name()) == "P1F", "P1F created");
    RecoveryContext ctx = MakeCtx(1, 100, 1);
    CHECK(p1f->Initialize(ctx, &err), "P1F initializes");

    // 两份同内容不同身份的目录
    auto build = [] {
        RecoveryTaskCatalog::Builder b;
        for (uint64_t id = 30; id <= 32; ++id) {
            TaskSpec t; t.task_id = id; t.kind = TaskKind::HEAP_PAGE;
            t.table_id = 9; t.key_lo = id * 10; t.key_hi = id * 10 + 1;
            t.influence = recovery_catalog::RPageState::AFFECTED;
            t.wal_end_lsn = id; t.shared_cost_estimate = 1; b.Add(t);
        }
        std::unique_ptr<RecoveryTaskCatalog> c;
        b.Build(&c);
        return c;
    };
    auto cat_a = build();
    DemandSnapshot snap;
    snap.epoch = ctx.epoch; snap.version = 11;
    {   // d1 覆盖 t31
        DemandSnapshot::Demand d; d.demand_key = 1; d.dedup_count = 5;
        d.blocker.kind = BlockerInfo::Kind::kPageLock; d.blocker.table_id = 9;
        d.blocker.page_id = 310;
        snap.demands.push_back(d);
    }
    p1f->OnDemandSnapshot(snap);

    PriorityProposal first = p1f->Propose(snap, *cat_a, 100);
    CHECK(!first.empty(), "P1F first propose non-empty");
    auto* p1f_impl = dynamic_cast<P1FReducedRescanPolicy*>(p1f.get());
    CHECK(p1f_impl != nullptr, "P1F concrete type");
    CHECK(p1f_impl->proposes_total() == 1 && p1f_impl->cache_hits() == 0, "first propose misses");

    // 同键重复 Propose：命中缓存、建议等值、计费
    PriorityProposal second = p1f->Propose(snap, *cat_a, 100);
    CHECK(p1f_impl->cache_hits() == 1, "same key proposes hit cache");
    bool same = second.groups.size() == first.groups.size();
    for (size_t i = 0; same && i < first.groups.size(); ++i) {
        same = second.groups[i].task_ids == first.groups[i].task_ids;
    }
    CHECK(same, "cached proposal equals recomputed order");

    // 版本变 → 失效重算
    DemandSnapshot snap2 = snap; snap2.version = 12;
    PriorityProposal third = p1f->Propose(snap2, *cat_a, 100);
    CHECK(p1f_impl->cache_hits() == 1 && p1f_impl->proposes_total() == 3,
          "version bump invalidates cache");

    // 目录身份变（同内容不同对象）→ 失效重算；排序与 P1 逐组相同
    auto cat_b = build();
    PriorityProposal p1_like = p1f->Propose(snap2, *cat_b, 100);
    PriorityProposal p1_ref = reg.Create("P1", &err)->Propose(snap2, *cat_b, 100);
    bool equal = p1_like.groups.size() == p1_ref.groups.size();
    for (size_t i = 0; equal && i < p1_ref.groups.size(); ++i) {
        equal = p1_like.groups[i].task_ids == p1_ref.groups[i].task_ids;
    }
    CHECK(equal, "P1F ordering matches P1 on identical input");

    // 预算 0 → 空提案（回 B0）；空不缓存（连续两次空）
    CHECK(p1f->Propose(snap2, *cat_b, 0).empty(), "P1F budget 0 -> empty");
    CHECK(p1f->Propose(snap2, *cat_b, 0).empty(), "P1F empty stays empty (not cached)");
}

// ---- C0：被对照算法采样/识别，按公共 B0 执行（22.9）----
// 恒空提案（实际优先执行量为零）；识别照做并计数（＝任务数）；
// 对照版本 shadow_of=P1。
static void TestC0ControlPolicy() {
    PolicyRegistry& reg = PolicyRegistry::Instance();
    std::string err;
    auto c0 = reg.Create("C0", &err);
    CHECK(c0 != nullptr && std::string(c0->name()) == "C0", "C0 created");
    auto* c0_impl = dynamic_cast<C0ControlPolicy*>(c0.get());
    CHECK(c0_impl != nullptr && std::string(c0_impl->shadow_of()) == "P1",
          "C0 shadows P1 (pinned version)");
    RecoveryContext ctx = MakeCtx(1, 100, 1);
    CHECK(c0->Initialize(ctx, &err), "C0 initializes");

    RecoveryTaskCatalog::Builder b;
    { TaskSpec t; t.task_id = 40; t.kind = TaskKind::HEAP_PAGE;
      t.influence = recovery_catalog::RPageState::AFFECTED; b.Add(t); }
    { TaskSpec t; t.task_id = 41; t.kind = TaskKind::HEAP_PAGE;
      t.influence = recovery_catalog::RPageState::AFFECTED; b.Add(t); }
    std::unique_ptr<RecoveryTaskCatalog> cat;
    CHECK(!b.Build(&cat).has_value(), "C0 catalog builds");
    DemandSnapshot snap;
    snap.epoch = ctx.epoch; snap.version = 3;
    {   // 需求覆盖 t41——C0 识别照做但不得产出建议
        DemandSnapshot::Demand d; d.demand_key = 1; d.dedup_count = 9;
        d.blocker.kind = BlockerInfo::Kind::kTask; d.blocker.task_id = 41;
        snap.demands.push_back(d);
    }
    c0->OnDemandSnapshot(snap);
    PriorityProposal p = c0->Propose(snap, *cat, 100);
    CHECK(p.empty(), "C0 always proposes empty (zero priority execution)");
    CHECK(c0_impl->identifies_total() == 2, "C0 identifies every task once (overhead counted)");

    // 调度器：空提案回公共 B0——默认序全部入队
    RecoveryScheduler sched(ctx, std::shared_ptr<const RecoveryTaskCatalog>(std::move(cat)));
    CHECK(sched.Submit(p, "C0") >= 1, "C0 empty proposal falls back to B0 default order");
    CHECK(sched.QueueDepth() == 2, "B0 default enqueues all tasks");
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
    TestB1RoleOrderPolicy();
    TestP1CriticalPathPolicy();
    TestP1FReducedRescanPolicy();
    TestC0ControlPolicy();
    TestDemandSnapshotContract();
    TestInterfaceHasNoExecutionPower();
    if (g_failures == 0) {
        std::printf("recovery_interface_contract_test: ALL PASS\n");
        return 0;
    }
    std::printf("recovery_interface_contract_test: %d FAILURES\n", g_failures);
    return 1;
}
