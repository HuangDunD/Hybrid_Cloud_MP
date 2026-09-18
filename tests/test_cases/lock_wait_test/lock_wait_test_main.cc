// 元组锁等待 / 死锁检测（WAIT_DETECT）+ 锁过户单元测试
//
// 两层验证：
// 1. 组件级（Case 3-4）：不联网，确定性验证元组 holder 节点字段
//    （DataItem::holder_node）的拷贝/序列化，以及本地等待/唤醒/活跃性判断。
// 2. 端到端（Case 5-11）：本进程内启动真实 brpc 服务（检测器 + 两个模拟
//    计算节点 FakeComputeNodeService），waiter 跑在真实 Scheduler 的 fiber 上
//    （生产里 WaitForLock 只在 fiber 里调用），验证：
//    跨节点死锁的**惰性链式走查**自动回滚、跨节点长等待链按序唤醒、
//    holder 已结束直接重试、方案 B 的「解锁点过户 + 接力包携带整条剩余队列」，
//    以及「合法长等待不会被检查间隔误杀」的看门狗语义。
//
// 注意：LOCK_WAIT_TIMEOUT_MS 现在是**看门狗**（到期直接 assert 暴露 bug），
// 不再有"超时自动回滚"这条路径，因此没有对应用例（它一旦触发就是崩）。
//
// 运行：./build/tests/lock_wait_test   （退出码 0 = 全部通过）

#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "common.h"
#include "config.h"
#include "base/data_item.h"
#include "dtx/lock_wait.h"
#include "remote_page_table/timestamp_rpc.h"

// ------------------------- 简易断言框架 -------------------------
static int g_pass = 0, g_fail = 0;
#define CHECK(cond, msg) do { \
    if (cond) { ++g_pass; printf("[PASS] %s\n", msg); } \
    else { ++g_fail; printf("[FAIL] %s  (%s:%d)\n", msg, __FILE__, __LINE__); } \
} while (0)

static bool wait_until(std::function<bool()> pred, int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return true;
}

// ------------------------- 协程上下文 -------------------------
// 生产里 WaitForLock 只在 fiber 里被调用（"挂起" = Scheduler::YieldWithTimeOnThread），
// 所以测试必须把 waiter 跑在真实 Scheduler 的 fiber 上，而不是裸 std::thread。
// 每个 waiter 轮流钉到一条调度线程上，唤醒 RPC 由 brpc 线程回调 schedule 回原线程。
static Scheduler* g_sched = nullptr;
static std::vector<int> g_tids;
static size_t g_tid_rr = 0;

static void SpawnWaiter(std::function<void()> body) {
    assert(g_sched != nullptr && !g_tids.empty());
    g_sched->schedule(std::move(body), g_tids[g_tid_rr++ % g_tids.size()]);
}

// 模拟真实代码里 holder 在解锁点逐个过户（dtx_exe.cc 的 UnlockTupleWithHandoff）：
// 在解锁点把元组字节改写成队首、拿到接力包，然后立刻 DeliverHandoff 送达
// （真实代码里紧接着本元组的 ReleaseXPage；这里没有页锁，直接送达）。
// 生产路径不兜底：holder 收尾前必须把名下所有队列过户掉，否则 FinishTx 会 assert。
// 没有等待者的元组返回 false，但队列（可能已空）会被清掉，等价于普通解锁。
static void HolderUnlockWithHandoff(LockWaitManager& mgr, uint64_t holder_ts,
                                    std::initializer_list<TupleWaitKey> keys,
                                    node_id_t self_node = 0,
                                    brpc::Channel* nodes = nullptr) {
    for (const auto& k : keys) {
        uint64_t nts = 0;
        uint8_t  nnode = 0;
        PendingHandoff ph;
        // 同步确认过户：接手者已经不在等待 -> 直接拒收，不会把锁写进死人
        if (mgr.HandoffLockSync(k, holder_ts, self_node, nodes, &nts, &nnode, &ph)) {
            mgr.DeliverHandoff(ph, self_node, nodes);
        }
    }
}

// 注：原「组件级检测器封装」（DetectorLocal，直接调 AddWaitEdge 验证环检测）
// 随集中式 wait-for 图一并删除。死锁检测改为惰性分布式链式走查，
// 需要真实的节点间 RPC，因此由端到端用例（Case 5 / Case 10）覆盖。

// ------------------------- 端到端：模拟计算节点服务 -------------------------
// 过户确认 RPC 计数：过户是"先同步确认再接"，一次过户最多发 1 次（候选被拒才多发）。
// 回执 accepted=true 才写元组字节，所以不会把锁交给已经回滚的事务。
static std::atomic<int> g_handoff_rpc_cnt{0};
static std::atomic<int> g_handoff_tail_cnt{0};
// 队尾 holder 更新消息计数（按目的节点合并，一次过户最多一个节点一条）
static std::atomic<int> g_holder_update_rpc_cnt{0};
// 登记 RPC 计数：只在**入队完成之后**自增（见 handler），所以 "计数到 N" 就等价于
// "这 N 个 waiter 已经真的排在 FIFO 队列里了"，用它替代"睡一会儿再猜"。
static std::atomic<int> g_register_rpc_cnt{0};

class FakeComputeNodeService : public compute_node_service::ComputeNodeService {
public:
    explicit FakeComputeNodeService(LockWaitManager* mgr, node_id_t self_node)
        : mgr_(mgr), self_node_(self_node) {}
    // 过户落地后要往"队尾等待者所在的别的节点"发 holder 更新消息，所以需要通道
    void SetNodes(brpc::Channel* nodes) { nodes_ = nodes; }

    virtual void RegisterTupleWait(::google::protobuf::RpcController*,
                       const ::compute_node_service::RegisterTupleWaitRequest* request,
                       ::compute_node_service::RegisterTupleWaitResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        TupleWaitKey key;
        key.table_id = (table_id_t)request->table_id();
        key.page_no  = (page_id_t)request->page_no();
        key.slot_no  = request->slot_no();
        // 必须【先入队、后计数】：计数自增早于 OnRegisterTupleWait 的话，
        // 主线程可能在"计数已到、队列还空"的窗口里做过户 -> 误判无人等待。
        const bool granted = mgr_->OnRegisterTupleWait(
            key, request->holder_ts(), request->waiter_ts(), (node_id_t)request->waiter_node());
        g_register_rpc_cnt.fetch_add(1, std::memory_order_relaxed);
        response->set_granted(granted);
    }

    // 锁过户：接收等待队列接力包并回执（接受 = 装队列 + 抢占 flag + 唤醒新 holder）
    virtual void HandoffQueue(::google::protobuf::RpcController*,
                       const ::compute_node_service::HandoffQueueRequest* request,
                       ::compute_node_service::HandoffQueueResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        g_handoff_rpc_cnt.fetch_add(1, std::memory_order_relaxed);
        g_handoff_tail_cnt.fetch_add(request->tail_ts_size(), std::memory_order_relaxed);
        TupleWaitKey key;
        key.table_id = (table_id_t)request->table_id();
        key.page_no  = (page_id_t)request->page_no();
        key.slot_no  = request->slot_no();
        std::vector<std::pair<uint64_t, node_id_t>> tail;
        const int n = request->tail_ts_size();
        for (int i = 0; i < n; ++i) {
            tail.emplace_back(request->tail_ts(i), (node_id_t)request->tail_node(i));
        }
        // 回执：接手者还在等 -> 接受并装队列；已经不在等 -> 拒收，发送方换下一个候选
        response->set_accepted(mgr_->TryAcceptHandoff(
            key, request->owner_ts(), request->version(), tail, self_node_, nodes_));
    }

    // 过户落地后：把队尾等待者的「我在等谁」改成新 holder（死锁检测的链跟着锁走）
    virtual void UpdateWaiterHolder(::google::protobuf::RpcController*,
                       const ::compute_node_service::UpdateWaiterHolderRequest* request,
                       ::compute_node_service::UpdateWaiterHolderResponse*,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        g_holder_update_rpc_cnt.fetch_add(1, std::memory_order_relaxed);
        std::vector<uint64_t> waiter_ts(request->waiter_ts().begin(), request->waiter_ts().end());
        mgr_->OnUpdateWaiterHolder(waiter_ts, request->new_holder_ts(),
                                   (node_id_t)request->new_holder_node());
    }
    // 分布式死锁检测的单跳：告诉询问方本节点上某事务当前在等谁
    virtual void QueryWaiterHolder(::google::protobuf::RpcController*,
                       const ::compute_node_service::QueryWaiterHolderRequest* request,
                       ::compute_node_service::QueryWaiterHolderResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        uint64_t holder_ts = 0;
        node_id_t holder_node = -1;
        if (mgr_->QueryWaiterHolder(request->waiter_ts(), &holder_ts, &holder_node)) {
            response->set_waiting(true);
            response->set_holder_ts(holder_ts);
            response->set_holder_node((int32_t)holder_node);
        } else {
            response->set_waiting(false);   // 没在等 -> 链断
        }
    }
private:
    LockWaitManager* mgr_;
    node_id_t self_node_;
    brpc::Channel* nodes_ = nullptr;
};

// 通过 RPC 向检测器取号（验证 proto 链路），放大为 start_ts
static uint64_t alloc_start_ts(brpc::Channel* det_chan, uint32_t node_id) {
    timestamp_service::TimeStampService_Stub stub(det_chan);
    timestamp_service::GetTimeStampRequest req;
    req.set_node_id(node_id);
    timestamp_service::GetTimeStampResponse resp;
    brpc::Controller cntl;
    stub.GetTimeStamp(&cntl, &req, &resp, nullptr);
    return cntl.Failed() ? 0 : resp.timestamp() * BatchTimeStamp;
}

// ========================================================================
// Case 3：元组内 holder 节点字段（DataItem::holder_node）
//   holder 所在节点现在随元组一起传递，冲突方本地即可读出，不再向检测器反查。
//   验证：字段落在对齐空洞里（sizeof 不增长）、拷贝/赋值保留、序列化往返保留。
// ========================================================================
static void case3_tuple_holder_node() {
    printf("---- Case 3: 元组内 holder 节点字段 ----\n");
    static_assert(sizeof(DataItem) == 72,
                  "holder_node 应落在 user_insert 与 timeStamp 之间的对齐空洞里，sizeof(DataItem) 不应增长");

    DataItem it(7, 16);
    it.lock = 1;
    it.timeStamp = 123456;
    it.holder_node = 3;
    CHECK(it.holder_node == 3, "C3: holder_node 可写入");

    DataItem copied(it);
    CHECK(copied.holder_node == 3 && copied.timeStamp == 123456, "C3: 拷贝构造保留 holder_node");

    DataItem assigned(8, 16);
    assigned = it;
    CHECK(assigned.holder_node == 3, "C3: 赋值运算符保留 holder_node");

    // undo 序列化是裸 memcpy(sizeof(DataItem))，字段必须原样落盘再读回。
    // 用 reinterpret_cast 读回，避免把序列化出的指针装进活的 DataItem 造成 double free。
    std::vector<char> buf(it.GetSerializeSize());
    it.Serialize(buf.data());
    const DataItem* rt = reinterpret_cast<const DataItem*>(buf.data());
    CHECK(rt->holder_node == 3 && rt->timeStamp == 123456, "C3: 序列化往返保留 holder_node");
}

// ========================================================================
// Case 4：LockWaitManager 组件级：活跃性判断、本地注册/唤醒、迟到唤醒丢弃
// ========================================================================
static void case4_local_manager() {
    printf("---- Case 4: LockWaitManager 本地行为 ----\n");
    LockWaitManager mgr;
    const node_id_t self = 0;
    const uint64_t T1 = 100001, T2 = 100002, T3 = 100003;
    const TupleWaitKey K1{0, 1, 1};

    mgr.OnTxnBegin(T1);
    mgr.OnTxnBegin(T2);
    CHECK(!mgr.OnRegisterTupleWait(K1, T1, T2, self), "C4: 活跃 holder -> 入队 granted=false");
    CHECK(mgr.OnRegisterTupleWait(K1, T3, T2, self), "C4: 不活跃 holder -> granted=true（无需等待）");

    auto st2 = std::make_shared<LockWaitManager::WaitState>();
    mgr.RegisterLocalWaiter(T2, st2);
    // 解锁点：把 K1 过户给队首 T2（同节点 -> 就地过继 + 放页后唤醒）
    HolderUnlockWithHandoff(mgr, T1, {K1});
    CHECK(st2->flag.load() == 1, "C4: 解锁点过户后立即唤醒本节点 waiter");
    mgr.FinishTx(T1);                  // T1 收尾
    HolderUnlockWithHandoff(mgr, T2, {K1});   // T2 名下已空队列，解锁点清掉
    mgr.FinishTx(T2);
    CHECK(mgr.OnRegisterTupleWait(K1, T1, 100009, self), "C4: FinishTx 后 holder 不再活跃");

    mgr.OnWakeTupleWaiter(999999);     // 未知 waiter_ts（已超时/不存在）：安全丢弃
    CHECK(true, "C4: 迟到的唤醒被安全丢弃不崩溃");
}

// ========================================================================
// Case 5：跨节点死锁 -> 惰性链式走查自动破环
//   A@node0 等 B@node1（挂起）；B@node1 再等 A@node0 -> 成环
//   注意语义变化：加边即检时 victim 必然是"后发起等待的 B"；改为惰性检测后，
//   victim 取决于谁的检查点先到，可能是 A 也可能是 B。因此这里只断言
//   "环被打破且不是靠硬上限兜底"，并校验幸存方的语义。
// ========================================================================
static void case5_cross_node_deadlock(LockWaitManager& mgr0, LockWaitManager& mgr1,
                                      brpc::Channel* det_chan, brpc::Channel* nodes) {
    printf("---- Case 5: 跨节点死锁自动回滚（端到端） ----\n");
    uint64_t ta = alloc_start_ts(det_chan, 0);
    uint64_t tb = alloc_start_ts(det_chan, 1);
    mgr0.OnTxnBegin(ta);
    mgr1.OnTxnBegin(tb);

    const TupleWaitKey K5{0, 1, 1};
    std::atomic<int> retA{-1}, retB{-1};
    SpawnWaiter([&] {
        retA = (int)mgr0.WaitForLock(ta, 0, tb, /*holder_node*/1, nodes,
                                     /*table*/0, /*page*/1, /*slot*/1, /*key*/1001);
        HolderUnlockWithHandoff(mgr0, ta, {K5}, 0, nodes);   // ta 的解锁点过户
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));   // 让 A 先挂起

    SpawnWaiter([&] {
        retB = (int)mgr1.WaitForLock(tb, 1, ta, /*holder_node*/0, nodes,
                                     0, 1, 1, 1002);
        HolderUnlockWithHandoff(mgr1, tb, {K5}, 1, nodes);   // tb 的解锁点过户
    });

    bool done = wait_until([&] { return retA.load() != -1 && retB.load() != -1; }, 5000);
    const int ra = retA.load(), rb = retB.load();
    const bool a_victim = (ra == (int)LockWaitResult::VICTIM_ABORT);
    const bool b_victim = (rb == (int)LockWaitResult::VICTIM_ABORT);
    const bool a_granted = (ra == (int)LockWaitResult::GRANTED_RETRY);
    const bool b_granted = (rb == (int)LockWaitResult::GRANTED_RETRY);

    CHECK(done, "C5: 环被打破，双方都拿到结果（未挂死）");
    CHECK(a_victim || b_victim, "C5: 至少一方被判 victim 回滚（成功破环）");
    CHECK(ra != (int)LockWaitResult::TIMEOUT_ABORT && rb != (int)LockWaitResult::TIMEOUT_ABORT,
          "C5: 双方都不是靠硬上限兜底（链式走查确实生效）");
    if (a_victim != b_victim) {   // 正常情形：恰好一方牺牲
        CHECK((a_victim && b_granted) || (b_victim && a_granted),
              "C5: 幸存方被跨节点唤醒，返回 GRANTED_RETRY");
    } else {                      // 双方同时判环（安全方向：多杀不死锁）
        CHECK(a_victim && b_victim, "C5: 双方均回滚（过度破环，安全方向）");
    }

    // 两个解锁点的过户都已在 fiber 里发生（各自留了墓碑）。同步确认过户在"接手者已退出"
    // 时会直接拒收并把空队列就地删掉，所以这里不用再补清理：各自注销即可。
    // （补第二次 HolderUnlockWithHandoff 就变成"同一事务重复解锁同一元组"，会被断言抓。）
    mgr0.FinishTx(ta);
    mgr1.FinishTx(tb);
}

// ========================================================================
// Case 6：跨节点长等待链一个接一个完成
//   T1@n0 <- T2@n1 <- T3@n0 <- T4@n1 <- T5@n0（T(i+1) 等 Ti）
//   T1 提交 -> 只唤醒 T2 -> T2 提交 -> 只唤醒 T3 -> ... 顺序必须严格
// ========================================================================
static void case6_long_chain_ordered_wakeup(LockWaitManager& mgr0, LockWaitManager& mgr1,
                                            brpc::Channel* det_chan, brpc::Channel* nodes) {
    printf("---- Case 6: 跨节点长等待链顺序完成（端到端） ----\n");
    constexpr int N = 5;
    LockWaitManager* mgrs[2] = {&mgr0, &mgr1};
    uint64_t ts[N + 1];
    std::atomic<int> woke[N + 1];
    std::atomic<int> ret[N + 1];
    for (int i = 1; i <= N; i++) {
        node_id_t home = (node_id_t)((i - 1) % 2);   // T1@n0, T2@n1, T3@n0 ...
        ts[i] = alloc_start_ts(det_chan, home);
        mgrs[home]->OnTxnBegin(ts[i]);
        woke[i] = 0;
        ret[i] = -1;
    }

    for (int i = 2; i <= N; i++) {
        node_id_t self = (node_id_t)((i - 1) % 2);
        SpawnWaiter([&, i, self] {
            auto r = mgrs[self]->WaitForLock(ts[i], self, ts[i - 1],
                                             (node_id_t)((i - 2) % 2),   // T(i-1) 所在节点
                                             nodes, 0, (page_id_t)(100 + i), 1, (itemkey_t)(2000 + i));
            ret[i] = (int)r;
            woke[i] = 1;
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));   // 确保全部挂起

    bool chain_ok = true;
    for (int i = 1; i < N; i++) {
        // Ti 解锁点：把锁过户给等它的 T(i+1)（waiter i+1 用的是 key {0, 100+i+1, 1}）。
        // 同时 Ti 名下还托管着它自己刚接手的那条空队列 {0, 100+i, 1}（i=1 时不存在），
        // 解锁点一并过一遍把它清掉——生产代码里 unlock point 也是逐元组走一遍。
        HolderUnlockWithHandoff(*mgrs[(i - 1) % 2], ts[i],
                                {TupleWaitKey{0, 100 + i, 1}, TupleWaitKey{0, 100 + i + 1, 1}},
                                (node_id_t)((i - 1) % 2), nodes);
        mgrs[(i - 1) % 2]->FinishTx(ts[i]);   // Ti 提交
        if (!wait_until([&] { return woke[i + 1].load() == 1; }, 3000)) {
            chain_ok = false;
            break;
        }
        if (ret[i + 1].load() != (int)LockWaitResult::GRANTED_RETRY) chain_ok = false;
        for (int j = i + 2; j <= N; j++) {   // 顺序性：后面的绝不允许提前醒
            if (woke[j].load() != 0) chain_ok = false;
        }
        if (!chain_ok) break;
    }
    // TN 本身无 waiter（key {0, 100+N+1, 1} 不存在），只需清掉它名下刚过继来的空队列
    HolderUnlockWithHandoff(*mgrs[(N - 1) % 2], ts[N],
                            {TupleWaitKey{0, 100 + N, 1}, TupleWaitKey{0, 100 + N + 1, 1}},
                            (node_id_t)((N - 1) % 2), nodes);
    mgrs[(N - 1) % 2]->FinishTx(ts[N]);   // 收尾
    CHECK(chain_ok, "C6: 5 事务跨节点等待链 T1->T5 严格按序一个接一个唤醒完成");
}

// ========================================================================
// Case 12（组件级）：同步确认过户 —— 接手者已退出等待时必须拒收
//   1) 队首已经回滚退出 -> 跳过它，把锁交给下一个还活着的人
//   2) 过户之后才决定回滚 -> UnregisterLocalWaiter 返回 adopted，调用方必须改判 GRANTED_RETRY
//      （否则这把锁就留给了一个退出等待的事务，元组永久锁死）
// ========================================================================
static void case12_reject_dead_successor() {
    printf("---- Case 12: 接手者已退出等待 -> 拒收/改判 ----\n");
    LockWaitManager mgr;
    const uint64_t H = 810001, W1 = 810002, W2 = 810003;
    const TupleWaitKey K{0, 77, 1};
    mgr.OnTxnBegin(H);
    CHECK(!mgr.OnRegisterTupleWait(K, H, W1, 0), "C12: W1 入队");
    CHECK(!mgr.OnRegisterTupleWait(K, H, W2, 0), "C12: W2 入队");
    auto st1 = std::make_shared<LockWaitManager::WaitState>();
    auto st2 = std::make_shared<LockWaitManager::WaitState>();
    mgr.RegisterLocalWaiter(W1, st1);
    mgr.RegisterLocalWaiter(W2, st2);

    // W1 决定回滚、退出等待（此刻还没被过户）
    CHECK(!mgr.UnregisterLocalWaiter(W1), "C12: W1 退出等待时还没被过户");

    // 解锁点过户：队首 W1 已经死了 -> 必须跳过它，把锁交给 W2
    uint64_t nts = 0;
    uint8_t  nnode = 255;
    PendingHandoff ph;
    const bool handed = mgr.HandoffLockSync(K, H, /*self_node*/0, nullptr, &nts, &nnode, &ph);
    CHECK(handed && nts == W2 && nnode == 0, "C12: 跳过已退出的 W1，过户给 W2");
    CHECK(ph.local_wake && st2->flag.load() == 1, "C12: 本地接手者 flag 在下发时被抢占");
    mgr.DeliverHandoff(ph, 0, nullptr);
    mgr.FinishTx(H);                       // 旧 holder 收尾：不该再有它名下的队列

    // 反向：被过户之后才决定回滚 -> 必须改判为接受这把锁
    const uint64_t H2 = 810004, W3 = 810005;
    const TupleWaitKey K2{0, 78, 1};
    mgr.OnTxnBegin(H2);
    CHECK(!mgr.OnRegisterTupleWait(K2, H2, W3, 0), "C12: W3 入队");
    auto st3 = std::make_shared<LockWaitManager::WaitState>();
    mgr.RegisterLocalWaiter(W3, st3);
    uint64_t nts2 = 0;
    uint8_t  nnode2 = 255;
    PendingHandoff ph2;
    CHECK(mgr.HandoffLockSync(K2, H2, 0, nullptr, &nts2, &nnode2, &ph2) && nts2 == W3,
          "C12: 过户给 W3");
    CHECK(mgr.UnregisterLocalWaiter(W3),
          "C12: 过户后注销 -> adopted=true，调用方据此把回滚改判 GRANTED_RETRY");
    mgr.FinishTx(H2);
}

// ========================================================================
// Case 13（组件级）：登记晚于解锁 —— holder 解锁时没人等，也必须留下墓碑
//   复现日志里 270/468 那次事故：holder 解锁某元组时队列还不存在（没人等），
//   随后等待者的登记才到（它读到的是 holder 还持锁时的旧字节）。这条登记必须被挡回去
//   重读元组、绝不能入队 —— 否则 holder 已经走过这个元组的解锁点，永远不会来唤醒它
//   （等待者永久挂起），FinishTx 体检还会 assert。
// ========================================================================
static void case13_register_after_unlock() {
    printf("---- Case 13: 登记晚于解锁 -> 墓碑挡回重读 ----\n");
    LockWaitManager mgr;
    const uint64_t H = 820001, W = 820002;
    const TupleWaitKey K{0, 91, 1};
    mgr.OnTxnBegin(H);

    // H 解锁 K：此刻没人等 -> 不过户（调用方会写 lock=UNLOCKED），但必须留下墓碑
    uint64_t nts = 0;
    uint8_t  nnode = 255;
    PendingHandoff ph;
    CHECK(!mgr.HandoffLockSync(K, H, /*self_node*/0, nullptr, &nts, &nnode, &ph),
          "C13: 解锁时没人等 -> 不过户");

    // 迟到的登记：读到的是"H 还持锁"的旧字节 -> 必须 granted（重读元组），不能入队
    CHECK(mgr.OnRegisterTupleWait(K, H, W, 0),
          "C13: 解锁后的迟到登记 -> granted=true（重读元组自己拿锁）");

    // 所以 H 名下不会残留队列：这行不炸就说明没有孤儿队列（旧实现在这里 assert）
    mgr.FinishTx(H);
    CHECK(true, "C13: FinishTx 名下无残留队列");

    // 收尾之后（墓碑已清 + 已不活跃）：仍然是 granted
    CHECK(mgr.OnRegisterTupleWait(K, H, W, 0), "C13: FinishTx 之后登记同样 granted");

    // 墓碑只挡"读到旧 holder"的登记：登记里写的是另一个 holder -> 必须放行走正常入队
    LockWaitManager mgr2;
    const uint64_t HA = 820003, HB = 820004, WC = 820005;
    mgr2.OnTxnBegin(HB);
    uint64_t nt2 = 0;
    uint8_t  nn2 = 255;
    PendingHandoff ph2;
    CHECK(!mgr2.HandoffLockSync(K, HA, 0, nullptr, &nt2, &nn2, &ph2), "C13: HA 解锁 K（墓碑=HA）");
    CHECK(!mgr2.OnRegisterTupleWait(K, HB, WC, 0),
          "C13: 登记写的是新 holder HB（≠墓碑 HA）-> 正常入队，不被误挡");
}

// ========================================================================
// Case 14（组件级）：过户落地时，队尾等待者的「我在等谁」必须一起改成新 owner
//   复现死锁漏检事故：W1 排在队首、W2 排在队尾，持有者 H 把锁过户给 W1 之后，
//   W2 记录里还写着"我在等 H"（H 已经走人）-> 链式走查在 H 处断链 -> 真环看不见
//   -> 双方一直等到看门狗 assert。这里断言 W2 的记录被更新成 W1。
// ========================================================================
static void case14_tail_holder_updated() {
    printf("---- Case 14: 过户时队尾等待者的 holder 一起更新 ----\n");
    LockWaitManager mgr;
    const uint64_t H = 840001, W1 = 840002, W2 = 840003;
    const TupleWaitKey K{0, 93, 1};
    mgr.OnTxnBegin(H);
    CHECK(!mgr.OnRegisterTupleWait(K, H, W1, 0), "C14: W1 入队（队首）");
    CHECK(!mgr.OnRegisterTupleWait(K, H, W2, 0), "C14: W2 入队（队尾）");
    auto st1 = std::make_shared<LockWaitManager::WaitState>();
    auto st2 = std::make_shared<LockWaitManager::WaitState>();
    st1->holder_ts = H;
    st1->holder_node = 0;
    st2->holder_ts = H;          // W2 登记时记录的就是 H
    st2->holder_node = 0;
    mgr.RegisterLocalWaiter(W1, st1);
    mgr.RegisterLocalWaiter(W2, st2);

    uint64_t nts = 0;
    uint8_t  nnode = 255;
    PendingHandoff ph;
    CHECK(mgr.HandoffLockSync(K, H, /*self_node*/0, nullptr, &nts, &nnode, &ph) && nts == W1,
          "C14: 过户给队首 W1");
    CHECK(st2->holder_ts == W1 && st2->holder_node == 0,
          "C14: 队尾 W2 的记录一起被改成 W1（修复点）");
    CHECK(st1->holder_ts == H, "C14: 队首 W1 自己的记录不动（它被唤醒，不再等待）");
    mgr.DeliverHandoff(ph, 0, nullptr);
    CHECK(st1->flag.load() == 1, "C14: 队首被唤醒");

    // 清理：W1 解锁 -> 队里只剩 W2；W2 再解锁 -> 队列清空
    HolderUnlockWithHandoff(mgr, W1, {K}, 0, nullptr);
    HolderUnlockWithHandoff(mgr, W2, {K}, 0, nullptr);
    mgr.FinishTx(H);
    mgr.FinishTx(W1);
    mgr.FinishTx(W2);
}

// ========================================================================
// Case 16（组件级）：放锁记录必须【按 holder 记账】，挡住"记录被后来者覆盖后"的旧 holder 登记
//   20:01 双节点实机事故的形状（FinishTx 发现未过户的队列 -> assert -> 对端连锁 watchdog）：
//     ① H 解锁 K（此刻没人等）-> 记下"H 放掉过 K"
//     ② 别人随后也放掉 K —— 旧实现是 key -> 最后一个放锁人 的【单值墓碑】，这一步会把 H 的
//        记录覆盖成 H2；TryAcceptHandoff 里还会把整条记录 erase
//     ③ H 还没收尾（active_txs_ 里还活着：放锁在 TxCommitSingle 的解锁循环里，FinishTx 在循环之后），
//        一条读到"H 还持锁"旧字节的迟到登记到了
//        -> 旧实现放它入队，在 H 名下建出一条 H 永远不会唤醒的孤儿队列 -> FinishTx assert。
//   修复后判据是"登记里那个 holder 自己放过这把锁吗"，②覆盖不掉①，所以必须挡回去重读。
//   （旧实现：第一个 CHECK 就会 FAIL + 下一行 FinishTx 直接 assert。）
// ========================================================================
static void case16_stale_register_after_overwrite() {
    printf("---- Case 16: 放锁记录被后来者覆盖后，旧 holder 的迟到登记仍须挡回 ----\n");
    LockWaitManager mgr;
    const uint64_t H = 860001, H2 = 860002, W = 860003;
    const TupleWaitKey K{0, 95, 1};
    mgr.OnTxnBegin(H);
    mgr.OnTxnBegin(H2);

    uint64_t nts = 0;
    uint8_t  nnode = 255;
    PendingHandoff ph;

    // ① H 解锁 K：没人等 -> 不过户，但必须记下"H 放掉过 K"
    CHECK(!mgr.HandoffLockSync(K, H, /*self_node*/0, nullptr, &nts, &nnode, &ph),
          "C16: H 解锁 K（没人等）");

    // ② H2 随后也放掉同一把 K（旧实现会把"H 放掉过 K"覆盖成 H2）
    CHECK(!mgr.HandoffLockSync(K, H2, /*self_node*/0, nullptr, &nts, &nnode, &ph),
          "C16: H2 放掉同一把 K（旧实现在这里覆盖掉 H 的墓碑）");

    // ③ H 仍活跃，迟到登记写的是 H -> 必须挡回去重读元组，绝不能入队
    CHECK(mgr.OnRegisterTupleWait(K, H, W, /*waiter_node*/0),
          "C16: 迟到的旧 holder H 登记 -> granted=true（修复点：旧实现会入队建孤儿队列）");

    // 所以 H 名下没有残留队列：这行不炸就说明没有孤儿队列（旧实现在这里 assert）
    mgr.FinishTx(H);
    CHECK(true, "C16: FinishTx(H) 名下无残留队列");
    mgr.FinishTx(H2);
    CHECK(mgr.OnRegisterTupleWait(K, H, W, 0), "C16: 两个 holder 都收尾后登记同样 granted");

    // ---- 变体：过户落地（TryAcceptHandoff）也不能把这条 key 的保护整条抹掉 ----
    // 旧实现在 TryAcceptHandoff 里 forwarded_at_.erase(key)，于是旧 holder 的迟到登记
    // 会挂进新 owner 的队列（等待者记录 != 队列 owner，不变式当场破）。
    LockWaitManager mgr2;
    const uint64_t HA = 860004, WA = 860005, WB = 860006;
    const TupleWaitKey K2{0, 96, 1};
    mgr2.OnTxnBegin(HA);
    CHECK(!mgr2.HandoffLockSync(K2, HA, /*self_node*/0, nullptr, &nts, &nnode, &ph),
          "C16: HA 放掉 K2（放锁记录=HA）");
    auto stw = std::make_shared<LockWaitManager::WaitState>();
    stw->holder_ts = HA;
    stw->holder_node = 0;
    mgr2.RegisterLocalWaiter(WA, stw);
    CHECK(mgr2.TryAcceptHandoff(K2, WA, /*version*/1, {}, /*self_node*/0, nullptr),
          "C16: WA 收下接力包成为新 owner（旧实现在这里 erase 掉 K2 的保护）");
    CHECK(stw->flag.load() == 1, "C16: WA 被唤醒");
    CHECK(mgr2.OnRegisterTupleWait(K2, HA, WB, 0),
          "C16: 过户落地后旧 holder HA 的迟到登记 -> granted=true（旧实现会挂进 WA 的队列）");
    CHECK(mgr2.WaitQueueDepth(K2) == 0, "C16: WA 的队列没有被脏登记污染（depth 仍 0）");

    HolderUnlockWithHandoff(mgr2, WA, {K2}, 0, nullptr);   // WA 收尾放锁，清掉空队列
    mgr2.FinishTx(HA);
    mgr2.FinishTx(WA);
}

// ========================================================================
// Case 8：holder 已结束（不活跃）-> 不挂起，直接重试
// ========================================================================
static void case8_holder_already_done(LockWaitManager& mgr0, LockWaitManager& mgr1,
                                      brpc::Channel* det_chan, brpc::Channel* nodes) {
    printf("---- Case 8: holder 已结束直接重试（端到端） ----\n");
    uint64_t t_dead = alloc_start_ts(det_chan, 0);   // 号段有效但从未 OnTxnBegin
    uint64_t tw = alloc_start_ts(det_chan, 1);

    auto t0 = std::chrono::steady_clock::now();
    auto r = mgr1.WaitForLock(tw, 1, t_dead, /*holder_node*/0, nodes,
                              0, 8, 1, 8001);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - t0).count();
    CHECK(r == LockWaitResult::GRANTED_RETRY && ms < 2000,
          "C8: holder 不活跃 -> 立即 GRANTED_RETRY 不挂起");
}

// ========================================================================
// Case 9：单次过户的接力包携带整条剩余队列（方案 B）
//   holder@n0 持有一个元组 K，K 上有 5 个 waiter：2 个在 n0（本地）+ 3 个在 n1。
//   方案 B 把"唤醒"绑在**每个元组的过户**上，而不是收尾时按 holder 批量广播：
//     - 解锁点只对 K 做一次过户，弹出队首，其余 4 个作为 tail 随接力包装进
//       同一个 HandoffQueue 包（跨节点时就是 1 次 RPC 带 4 条）
//     - 被唤醒的只有队首，其余仍留在接力到新 holder 节点上的队列里
//   随后按接力顺序逐个清空队列，验证严格 FIFO 且每次只醒一个。
// ========================================================================
static void case9_handoff_packet_carries_queue(LockWaitManager& mgr0, LockWaitManager& mgr1,
                                               brpc::Channel* det_chan, brpc::Channel* nodes) {
    printf("---- Case 9: 接力包携带整条剩余队列（方案 B） ----\n");
    constexpr int N_LOCAL = 2, N_REMOTE = 3, N_ALL = N_LOCAL + N_REMOTE;
    const TupleWaitKey K{0, 900, 1};
    uint64_t th = alloc_start_ts(det_chan, 0);   // holder 在 n0
    mgr0.OnTxnBegin(th);

    std::atomic<int> woke[N_ALL];
    std::atomic<int> ret[N_ALL];
    uint64_t tws[N_ALL];
    const int reg_before = g_register_rpc_cnt.load();
    for (int i = 0; i < N_ALL; i++) {
        woke[i] = 0;
        ret[i] = -1;
        const bool local = (i < N_LOCAL);
        tws[i] = alloc_start_ts(det_chan, local ? 0 : 1);
        SpawnWaiter([&, i, local] {
            auto r = (local ? mgr0 : mgr1).WaitForLock(tws[i], local ? 0 : 1, th, /*holder_node*/0,
                nodes, K.table_id, K.page_no, K.slot_no, (itemkey_t)(9000 + i));
            ret[i] = (int)r;
            woke[i] = 1;
        });
    }
    auto idx_of = [&](uint64_t t) {
        for (int i = 0; i < N_ALL; ++i) if (tws[i] == t) return i;
        return -1;
    };

    // 5 个 waiter 全部排进 n0 上 K 的 FIFO 队列。远端 3 个走登记 RPC，RPC 返回即已入队；
    // 本地 2 个直接入队（不发 RPC），再用队列深度高水位（此前用例最多只有 1）确认一下。
    bool queued = wait_until([&] {
        return g_register_rpc_cnt.load() >= reg_before + N_REMOTE &&
               mgr0.stat_wq_depth_max.load() >= N_ALL;
    }, 3000);
    CHECK(queued, "C9: 5 个 waiter 都排进 n0 上 K 的 FIFO 队列");
    const int rpc_before = g_handoff_rpc_cnt.load();
    const int tail_before = g_handoff_tail_cnt.load();

    // holder 解锁点：对 K 做一次过户
    PendingHandoff ph;
    uint64_t cur_ts = 0;
    uint8_t  cur_node = 255;
    const bool handed = mgr0.HandoffLockSync(K, th, /*self_node*/0, nodes, &cur_ts, &cur_node, &ph);
    if (handed) mgr0.DeliverHandoff(ph, 0, nodes);
    CHECK(handed, "C9: 解锁点把 K 过户给队首");
    CHECK(ph.tail.size() == N_ALL - 1, "C9: 接力包带着剩余 4 个 waiter（tail 完整）");
    const bool head_remote = (cur_node == 1);
    mgr0.FinishTx(th);

    const int head = idx_of(cur_ts);
    CHECK(wait_until([&] { return head >= 0 && woke[head].load() == 1; }, 3000),
          "C9: 队首被唤醒，返回 GRANTED_RETRY");
    CHECK(head >= 0 && ret[head].load() == (int)LockWaitResult::GRANTED_RETRY,
          "C9: 队首结果是 GRANTED_RETRY");
    // 队首是本地还是远端决定 RPC 次数：远端才有 1 次 HandoffQueue RPC（带 4 条 tail）
    const int rpc_delta = g_handoff_rpc_cnt.load() - rpc_before;
    const int tail_delta = g_handoff_tail_cnt.load() - tail_before;
    printf("       [info] 队首节点=%d，HandoffQueue RPC 次数=%d，随包 tail 条数=%d\n",
           (int)cur_node, rpc_delta, tail_delta);
    CHECK(rpc_delta == (head_remote ? 1 : 0),
          "C9: 队首在远端则整条队列只需 1 次接力 RPC，在本地则 0 次");
    CHECK(tail_delta == (head_remote ? N_ALL - 1 : 0),
          "C9: 接力 RPC 一次携带全部 4 条 tail");

    // 顺序性：其余 4 个此时不许醒
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    bool others_still_waiting = true;
    for (int i = 0; i < N_ALL; ++i) {
        if (i != head && woke[i].load() != 0) others_still_waiting = false;
    }
    CHECK(others_still_waiting, "C9: 其余 waiter 仍在队列中等待（未惊群）");

    // 依次接力，逐个唤醒并清空队列（否则这些等待线程无法 join）。
    // 期望顺序 = 队首 + 接力包里的 tail：这就是 FIFO 顺序本身。
    std::vector<uint64_t> expect_order;
    expect_order.push_back(cur_ts);
    for (const auto& w : ph.tail) expect_order.push_back(w.first);

    LockWaitManager* mgrs[2] = {&mgr0, &mgr1};
    bool relay_ok = true;
    size_t relayed = 0;                     // 已被唤醒的 waiter 个数（含队首）
    for (;;) {
        PendingHandoff p2;
        uint64_t n2 = 0;
        uint8_t  nn2 = 255;
        // 队列已空：HandoffLockSync 会就地清掉这条空队列并返回 false，接力结束
        if (!mgrs[cur_node]->HandoffLockSync(K, cur_ts, cur_node, nodes, &n2, &nn2, &p2)) break;
        if (relayed + 1 >= expect_order.size() || n2 != expect_order[relayed + 1]) {
            relay_ok = false;                                    // 队尾之后不该再有人 / FIFO 顺序必须一致
        }
        mgrs[cur_node]->DeliverHandoff(p2, cur_node, nodes);
        mgrs[cur_node]->FinishTx(cur_ts);
        const int wi = idx_of(n2);
        if (wi < 0 || !wait_until([&] { return woke[wi].load() == 1; }, 3000) ||
            ret[wi].load() != (int)LockWaitResult::GRANTED_RETRY) {
            relay_ok = false;
        }
        ++relayed;
        cur_node = nn2;
        cur_ts = n2;
        if (!relay_ok) break;
    }
    mgrs[cur_node]->FinishTx(cur_ts);
    CHECK(relay_ok && relayed == expect_order.size() - 1,
          "C9: 队列按接力包顺序逐个唤醒，严格 FIFO 清空");
}

// ========================================================================
// Case 10：超时语义 —— 合法的长等待不会被"检查间隔"误杀
//   holder 在 N 倍检查间隔内都不释放：waiter 期间会触发多次链式走查，
//   但每次都查不出环，因此必须**继续等**而不是回滚；直到 holder 真提交
//   才被唤醒返回 GRANTED_RETRY。
// ========================================================================
static void case10_long_wait_not_killed(LockWaitManager& mgr0, LockWaitManager& mgr1,
                                        brpc::Channel* det_chan, brpc::Channel* nodes) {
    printf("---- Case 10: 合法长等待不被误杀（超时语义） ----\n");
    const int saved_check = DEADLOCK_CHECK_INTERVAL_MS;
    DEADLOCK_CHECK_INTERVAL_MS = 50;          // 检查很密集
    const int hold_ms = 600;                  // holder 坚持 12 倍检查间隔

    uint64_t th = alloc_start_ts(det_chan, 0);
    uint64_t tw = alloc_start_ts(det_chan, 1);
    mgr0.OnTxnBegin(th);
    const uint64_t checks_before = mgr1.stat_deadlock_check.load();

    std::atomic<int> ret{-1};
    auto t0 = std::chrono::steady_clock::now();
    SpawnWaiter([&] {
        ret = (int)mgr1.WaitForLock(tw, 1, th, /*holder_node*/0,
                                    nodes, 0, 11, 1, 11001);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(hold_ms));
    CHECK(ret.load() == -1, "C10: 远超多个检查间隔仍未被误杀（仍挂在等待中）");
    CHECK(mgr1.stat_deadlock_check.load() > checks_before, "C10: 期间确实触发过链式走查");

    // holder 终于提交：先在解锁点把锁过户给 tw（远端 -> 唤醒），再收尾
    HolderUnlockWithHandoff(mgr0, th, {TupleWaitKey{0, 11, 1}}, 0, nodes);
    mgr0.FinishTx(th);
    bool woke = wait_until([&] { return ret.load() != -1; }, 3000);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - t0).count();
    CHECK(woke && ret.load() == (int)LockWaitResult::GRANTED_RETRY,
          "C10: holder 提交后被正常唤醒，返回 GRANTED_RETRY");
    CHECK(ms >= hold_ms, "C10: 实际等待时长确实超过了检查间隔");

    DEADLOCK_CHECK_INTERVAL_MS = saved_check;
}

// ========================================================================
// Case 15（端到端）：队尾等待者在别的节点 -> holder 更新走 RPC；
//   更新之后，死锁检测必须能看见"经过队尾等待者的环"（修复前这里是活锁，只能等看门狗）
//   H@n0 持 K1；W1@n1 队首、W2@n0 队尾。H 过户 K1 给 W1 -> n1 装队列 + 通知 n0 更新 W2。
//   W1 拿到 K1 后去等 W2 持有的 K2 -> 环 W2->W1->W2 必须被判出来。
// ========================================================================
static void case15_tail_holder_cycle(LockWaitManager& mgr0, LockWaitManager& mgr1,
                                     brpc::Channel* det_chan, brpc::Channel* nodes) {
    printf("---- Case 15: 队尾 holder 跨节点更新 + 环能被查出 ----\n");
    const uint64_t H = 850001, W1 = 850002, W2 = 850003;
    const TupleWaitKey K1{0, 95, 1};   // H 持有；W1(队首)、W2(队尾) 等它
    const TupleWaitKey K2{0, 96, 1};   // W2 "持有"；W1 等它 -> 成环
    mgr0.OnTxnBegin(H);
    mgr0.OnTxnBegin(W2);               // W2 作为 K2 的 holder 要能被登记

    std::atomic<int> ret1a{-1}, ret1b{-1}, ret2{-1};
    const int reg_before = g_register_rpc_cnt.load();
    // W1@n1：先等 K1（跨节点登记），拿到后转去等 W2 持有的 K2
    SpawnWaiter([&] {
        ret1a = (int)mgr1.WaitForLock(W1, 1, H, 0, nodes, K1.table_id, K1.page_no, K1.slot_no, 0);
        if (ret1a == (int)LockWaitResult::GRANTED_RETRY) {
            ret1b = (int)mgr1.WaitForLock(W1, 1, W2, 0, nodes,
                                          K2.table_id, K2.page_no, K2.slot_no, 0);
        }
    });
    CHECK(wait_until([&] { return g_register_rpc_cnt.load() >= reg_before + 1; }, 3000),
          "C15: W1 跨节点登记进 n0 上 K1 的队列（队首）");

    // W2@n0：本地登记到队尾
    SpawnWaiter([&] {
        ret2 = (int)mgr0.WaitForLock(W2, 0, H, 0, nodes, K1.table_id, K1.page_no, K1.slot_no, 0);
    });
    CHECK(wait_until([&] { return mgr0.WaitQueueDepth(K1) == 2; }, 3000),
          "C15: K1 队列深度 2（W1 队首 / W2 队尾）");

    // H 过户 K1 给队首 W1（跨节点确认）：n1 装队列 tail=[W2@n0] -> 给 n0 发一条 holder 更新
    const int upd_before = g_holder_update_rpc_cnt.load();
    uint64_t nts = 0;
    uint8_t  nnode = 255;
    PendingHandoff ph;
    CHECK(mgr0.HandoffLockSync(K1, H, 0, nodes, &nts, &nnode, &ph) && nts == W1 && nnode == 1,
          "C15: K1 过户给 W1@n1");
    mgr0.DeliverHandoff(ph, 0, nodes);
    CHECK(g_holder_update_rpc_cnt.load() > upd_before,
          "C15: 队尾在别的节点 -> 发了一条 holder 更新消息");

    uint64_t wh = 0;
    node_id_t wn = -1;
    CHECK(wait_until([&] { return mgr0.QueryWaiterHolder(W2, &wh, &wn) && wh == W1; }, 3000),
          "C15: W2（在 n0）的记录被远端更新成新 owner W1");

    // 环：W2 等 K1 的新 owner W1；W1 拿到 K1 后等 W2 持有的 K2
    const bool saw_cycle = wait_until([&] {
        return ret2.load() == (int)LockWaitResult::VICTIM_ABORT ||
               ret1b.load() == (int)LockWaitResult::VICTIM_ABORT;
    }, 3000);
    CHECK(saw_cycle,
          "C15: 检测器看见了经过队尾等待者的环并判出 victim（修复前查不出，只能等看门狗）");

    // 收工：把还没返回的等待者直接叫醒（本用例是最后一个，不再管队列残留）
    if (ret1b.load() == -1) mgr1.OnWakeTupleWaiter(W1);
    if (ret2.load() == -1)  mgr0.OnWakeTupleWaiter(W2);
    wait_until([&] { return ret1b.load() != -1 && ret2.load() != -1; }, 3000);
}

// ========================================================================
// Case 11：锁过户 + 队列接力（端到端，方案 B 的送达时机）
//   holder H 在 node0，元组 K 上有 2 个等待者 W1、W2（都在 node1）。
//   H 在解锁点 HandoffLockSync 确认把锁【就地过户】给队首 W1，随即（ReleaseXPage 之后）
//   DeliverHandoff 把接力包 [W2] 送到 node1；node1 唤醒 W1。
//   之后 W1 在自己的解锁点再接力，把 W2 唤醒。
//   验证：过户选中队首、接力包在解锁点就送达（不再等到收尾）、队尾继续排队、
//         最终按序接棒，且每一步都是"只醒一个"。
// ========================================================================
static void case11_handoff(LockWaitManager& mgr0, LockWaitManager& mgr1,
                           brpc::Channel* det_chan, brpc::Channel* nodes) {
    printf("---- Case 11: 锁过户 + 队列接力（端到端） ----\n");
    const int saved_check = DEADLOCK_CHECK_INTERVAL_MS;
    DEADLOCK_CHECK_INTERVAL_MS = 5000;   // 期间不触发链式走查，隔离变量

    const uint64_t th = alloc_start_ts(det_chan, 0);   // holder 在 node0
    mgr0.OnTxnBegin(th);

    const TupleWaitKey K{0, 55, 1};
    const uint64_t w1 = alloc_start_ts(det_chan, 1);
    const uint64_t w2 = alloc_start_ts(det_chan, 1);

    const int reg_before = g_register_rpc_cnt.load();
    std::atomic<int> ret1{-1}, ret2{-1};
    // 有意让 W1 先排进队列、再放 W2：两个 waiter 并发发登记 RPC 的话 FIFO 队首是随机的，
    // 用例里"队首=W1、队尾=W2"的断言就成了靠运气的。计数在入队后才 +1，所以按计数分段即可。
    SpawnWaiter([&] {
        ret1 = (int)mgr1.WaitForLock(w1, 1, th, /*holder_node*/0, nodes, K.table_id, K.page_no, K.slot_no, 0);
    });
    bool w1_queued = wait_until([&] { return g_register_rpc_cnt.load() >= reg_before + 1; }, 3000);
    CHECK(w1_queued, "C11: W1 先排进 node0 上 K 的 FIFO 队列");
    SpawnWaiter([&] {
        ret2 = (int)mgr1.WaitForLock(w2, 1, th, /*holder_node*/0, nodes, K.table_id, K.page_no, K.slot_no, 0);
    });

    // 两个 waiter 都登记到 node0（holder 所在节点）的 K 队列上，且顺序确定：W1 -> W2。
    // 不能用 stat_wq_depth_max（历史高水位）判断——早前用例早就把它顶上去了。
    bool queued = wait_until([&] { return g_register_rpc_cnt.load() >= reg_before + 2; }, 3000);
    CHECK(queued, "C11: 两个 waiter 都排进 node0 上 K 的 FIFO 队列");

    // H 的解锁点：就地过户（页在手），拿回接力包
    const uint64_t handoff_before = mgr0.stat_handoff.load();   // 计数是累计值，取基线
    uint64_t new_ts = 0;
    uint8_t  new_node = 255;
    PendingHandoff ph;
    bool handed = mgr0.HandoffLockSync(K, th, /*self_node*/0, nodes, &new_ts, &new_node, &ph);
    CHECK(handed && new_ts == w1 && new_node == 1,
          "C11: 锁就地过户给队首 W1（node1），字节改写为 W1");
    CHECK(ph.tail.size() == 1 && ph.tail[0].first == w2,
          "C11: 接力包带着队尾 W2");
    CHECK(mgr0.stat_handoff.load() == handoff_before + 1, "C11: 过户计数 +1");

    // 紧跟着 ReleaseXPage 的送达点：这里立刻发到 node1（此时 H 还没 FinishTx）
    if (handed) mgr0.DeliverHandoff(ph, 0, nodes);

    bool w1_woke = wait_until([&] { return ret1.load() != -1; }, 3000);
    CHECK(w1_woke && ret1.load() == (int)LockWaitResult::GRANTED_RETRY,
          "C11: 送达点之后队首 W1 立刻被唤醒，返回 GRANTED_RETRY");
    // 远端过户的墓碑必须活到 FinishTx：这段窗口里读到旧 holder 的迟到登记要被挡掉，
    // 否则会往"已经交出去队列的旧 holder"名下重建一条孤儿队列。
    CHECK(mgr0.OnRegisterTupleWait(K, th, 999998, 0),
          "C11: 过户在途时读到旧 holder 的迟到登记 -> 直接 GRANTED_RETRY（墓碑生效）");
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    CHECK(ret2.load() == -1, "C11: 队尾 W2 仍在等（队列已接力到 node1，未丢）");

    mgr0.FinishTx(th);   // H 收尾（接力早已送达）

    // W1 的解锁点：接力把 W2 唤醒（此时队列归 node1 上的 W1 托管）
    HolderUnlockWithHandoff(mgr1, w1, {K}, 1, nodes);
    bool w2_woke = wait_until([&] { return ret2.load() != -1; }, 3000);
    CHECK(w2_woke && ret2.load() == (int)LockWaitResult::GRANTED_RETRY,
          "C11: W1 解锁点接力后 W2 接棒被唤醒");
    mgr1.FinishTx(w1);

    DEADLOCK_CHECK_INTERVAL_MS = saved_check;
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // 检查间隔只负责触发死锁走查；LOCK_WAIT_TIMEOUT_MS 是看门狗（到期 assert）
    DEADLOCK_CHECK_INTERVAL_MS = 100;
    LOCK_WAIT_TIMEOUT_MS = 5000;

    // ---------- 组件级 ----------
    case3_tuple_holder_node();
    case4_local_manager();
    case12_reject_dead_successor();
    case13_register_after_unlock();
    case14_tail_holder_updated();
    case16_stale_register_after_overwrite();

    // ---------- 协程调度器：waiter 必须跑在 fiber 里 ----------
    g_sched = new Scheduler(6, false, "LockWaitTestSched");
    g_sched->start();
    g_tids = g_sched->getThreadIds();
    if (g_tids.empty()) { printf("[FATAL] scheduler thread ids empty\n"); return 2; }

    // ---------- 端到端：启动真实服务 ----------
    timestamp_service::TimeStampServiceImpl ts_svc;
    LockWaitManager mgr0, mgr1;
    FakeComputeNodeService fake0(&mgr0, 0), fake1(&mgr1, 1);

    brpc::Server det_server, node0_server, node1_server;
    if (det_server.AddService(&ts_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0 ||
        node0_server.AddService(&fake0, brpc::SERVER_DOESNT_OWN_SERVICE) != 0 ||
        node1_server.AddService(&fake1, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        printf("[FATAL] AddService failed\n");
        return 2;
    }
    brpc::ServerOptions opt;
    opt.num_threads = 8;
    if (det_server.Start(43800, &opt) != 0 ||
        node0_server.Start(43801, &opt) != 0 ||
        node1_server.Start(43802, &opt) != 0) {
        printf("[FATAL] server start failed\n");
        return 2;
    }

    brpc::Channel det_chan;
    brpc::Channel nodes[2];
    if (det_chan.Init("127.0.0.1:43800", nullptr) != 0 ||
        nodes[0].Init("127.0.0.1:43801", nullptr) != 0 ||
        nodes[1].Init("127.0.0.1:43802", nullptr) != 0) {
        printf("[FATAL] channel init failed\n");
        return 2;
    }

    fake0.SetNodes(nodes);   // 过户落地后要往别的节点发 holder 更新消息
    fake1.SetNodes(nodes);

    case5_cross_node_deadlock(mgr0, mgr1, &det_chan, nodes);
    case6_long_chain_ordered_wakeup(mgr0, mgr1, &det_chan, nodes);
    case8_holder_already_done(mgr0, mgr1, &det_chan, nodes);
    case9_handoff_packet_carries_queue(mgr0, mgr1, &det_chan, nodes);
    case10_long_wait_not_killed(mgr0, mgr1, &det_chan, nodes);
    case11_handoff(mgr0, mgr1, &det_chan, nodes);
    case15_tail_holder_cycle(mgr0, mgr1, &det_chan, nodes);   // 放最后：会留下队列残留

    det_server.Stop(0);  det_server.Join();
    node0_server.Stop(0); node0_server.Join();
    node1_server.Stop(0); node1_server.Join();
    // 不调 g_sched->stop()：Scheduler::run() 的退出分支被注释掉了、tickle() 也是空实现，
    // stop() 里的 join() 永远等不到线程退出（必挂）。测试进程到此就结束，交给 OS 回收。
    // 同理也不 delete g_sched：~Scheduler 会 assert(m_stopping)。

    printf("========================================\n");
    printf("lock_wait_test: %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
