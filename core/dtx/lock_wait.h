#pragma once

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <brpc/channel.h>
#include <butil/logging.h>

#include "common.h"
#include "config.h"
#include "compute_node/compute_node.pb.h"
#include "remote_page_table/timestamp.pb.h"
#include "fiber/scheduler.h"   


/*
    自己都有点绕晕了，总结下几个锁等待相关的数据结构：
    1. local_waiters_：
        std::unordered_map<uint64_t, std::shared_ptr<WaitState>> local_waiters_;
                            waiter_ts                  它的等待状态
        表示某个正在等待着锁的等待状态，即一个元素代表了一个等待

    2. local_queues_：
        std::unordered_map<TupleWaitKey, TupleWaitQueue, TupleWaitKeyHash> local_queues_;
        TupleWaitKey：具体的 key，TupleWaitQueue：等待队列
        代表了谁在等我这个锁，按照什么顺序来

    3. active_txs_：活跃事务列表，不是每个事务一份，是一个本节点内部的存活事务分类
    4. holder_keys：其实就是 local_queues 的方向索引，按照 holder 来找到当前等待我这个事务的有哪些
        std::unordered_map<uint64_t, std::unordered_set<TupleWaitKey, TupleWaitKeyHash>> holder_keys_;
                            holder_ts           它名下的等待队列的元组
    
    5. forwarded_keys_：holder_ts -> 【这个事务已经放掉的 key 集合】（放锁记录/墓碑）。
        即：这条 key 已经不在 holder_ts 手里了（我把锁过户给了别人，或者解锁时根本没人在等）。
        OnRegisterTupleWait 的判据是【登记里写的那个 holder 有没有放掉过这条 key】：
        放过 -> 对方读到的是放锁之前的旧字节 -> 直接 granted 让它重读元组，绝不能入队
        （我已经走过这个元组的解锁点，永远不会再来唤醒它）。这一条同时盖住两种竞态：
        ①过户在途；②登记晚于解锁（holder 还在 active_txs_ 里）。
        生命周期跟 holder 的活跃期一致：FinishTx 整条删掉，之后由 active_txs_ 兜住。
        【为什么不用 key -> holder_ts 的单个墓碑】单个墓碑只能记住"最后一个放锁人"，
        会被后来者放同一把锁时覆盖，也会在 TryAcceptHandoff 里被整条 erase；于是
        "更早放锁、但事务还活着"的旧 holder 就能穿透挡板，在它名下建出一条它永远不会
        唤醒的孤儿队列 -> 等待者永久挂起 + FinishTx assert（2026-09-17 20:01 双节点实机事故）。
        反向索引按 holder 记账，覆盖/误删都不会发生。
        注意：放锁点在 TxCommitSingle / TxAbortWorkLoad 的解锁循环里，FinishTx 在循环之后，
        所以"已经放掉某把锁"的事务仍会在 active_txs_ 里存活毫秒级（~7 个写元组 + 等 commit log）。
        正确性依赖一条不变式：一个事务在提交/回滚前不会放掉再重新拿同一把元组锁
        （解锁点只在 commit/abort 路径，见 dtx_exe.cc 的 6 处 UnlockTupleWithHandoff），
        所以"已放掉 + 仍活跃"永远不会是一个合法 holder。
*/

enum class LockWaitResult {
    GRANTED_RETRY,   // holder 已释放（或被授予重试资格）：调用方重新拉页并重检锁
    VICTIM_ABORT,    // 检测器判定等待成环，本事务为 victim：立即回滚
    TIMEOUT_ABORT,   // 等待超时 / 检测器或 holder 节点不可达：保守回滚
};

// 元组标识 = (表, 页, 槽)。槽位在一页内唯一确定元组，且 holder 的解锁点和等待方
// 都能从各自的 Rid 直接拿到，不会出现"两边算出的 key 不一致、队列对不上"的问题。
// 等待队列按它排队，队列 owner = 当前 holder 所在节点；过户时队列随锁接力到新 holder。
struct TupleWaitKey {
    table_id_t table_id = 0;
    page_id_t  page_no  = 0;
    int        slot_no  = 0;

    bool operator==(const TupleWaitKey& o) const {
        return table_id == o.table_id && page_no == o.page_no && slot_no == o.slot_no;
    }
};

struct TupleWaitKeyHash {
    size_t operator()(const TupleWaitKey& k) const {
        uint64_t h = 1469598103934665603ULL;   // FNV-1a
        auto mix = [&h](uint64_t v) { h ^= v; h *= 1099511628211ULL; };
        mix((uint64_t)k.table_id);
        mix((uint64_t)k.page_no);
        mix((uint64_t)(uint32_t)k.slot_no);
        return (size_t)h;
    }
};

// 一次锁过户：在解锁点（页在手）同步确认接手者后产生。
// local_wake=true：接手者在本节点，队列已就地过继，唤醒要等 ReleaseXPage 之后
//                 （见 DeliverHandoff）；remote：接手者节点在确认 RPC 里已经装好队列并唤醒。
struct PendingHandoff {
    TupleWaitKey key;
    uint64_t new_holder_ts = 0;
    node_id_t new_holder_node = -1;
    uint64_t version = 0;
    std::vector<std::pair<uint64_t, node_id_t>> tail;   // 队首已被 pop，其余随包接力
    bool local_wake = false;                            // 是否需要本节点在放页后补唤醒
};

// ===================== 锁等待 / 过户：链路调试日志 =====================
// 打开后，把「登记 -> 排队 -> 解锁点挑队首 -> 送达 -> 落地 -> 唤醒 -> 收尾」整条链路
// 逐条打出来（每行带全局单调序号 + 线程号，跨线程也能还原先后），用来定位
// "这把锁到底交给了谁 / 卡在哪一步"。
// 【当前状态：所有 LwTrace 调用点都已注释掉】，因为它们在高争用下每笔解锁都会打一条
// （实测占日志 99% 的量、~1300 行/秒），压测时有明显开销。
// 恢复调试：把这些调用点取消注释即可（开关保持 true）；只想临时关就置 false。
inline bool LOCK_WAIT_TRACE = true;

inline uint64_t LwNextSeq() {
    static std::atomic<uint64_t> seq{0};
    return seq.fetch_add(1, std::memory_order_relaxed) + 1;
}

inline std::string LwKeyStr(const TupleWaitKey& k) {
    std::ostringstream os;
    os << "(t" << k.table_id << ",p" << k.page_no << ",s" << k.slot_no << ")";
    return os.str();
}

// 统一格式：[LW#序号 tid=线程] 事件 字段=值 ...
template <typename... Args>
inline void LwTrace(const char* event, Args&&... args) {
    if (!LOCK_WAIT_TRACE) return;
    std::ostringstream os;
    os << "[LW#" << LwNextSeq() << " tid=" << getThreadID() << "] " << event;
    (void)std::initializer_list<int>{((void)(os << " " << args), 0)...};
    LOG(INFO) << os.str();
}

class LockWaitManager {
public:
    // 表示一次等待关系
    struct WaitState {
        std::atomic<int> flag{0};  // 0 等待中，1 granted
        std::mutex m;
        std::condition_variable cv;
        // 本等待者当前在等谁。惰性死锁检测（链式走查）靠它逐跳询问。
        // 【会被修改】：锁过户时，新 owner 所在节点会把队列里后面这些人的 holder 改成新 owner
        // （否则走查会沿着"已经走人的旧 holder"断链，环永远查不出来）。
        // 约定：只在持有 LockWaitManager::mtx_ 时读写（登记时写下、过户时更新、
        // QueryWaiterHolder/GetWaiterHolder 读取）。
        uint64_t holder_ts = 0;
        node_id_t holder_node = -1;
        // fiber 上下文（bench/SQL 模式）：唤醒方用它把等待中的 fiber 重新入队。
        // 必须在发布到 local_waiters_ 之前写好、之后不再修改，这样 brpc 线程
        // 读到的值一定是完整的。非 fiber 上下文（单元测试）保持 nullptr，走 cv 兜底。
        Scheduler* sched = nullptr;
        Fiber::ptr fiber;
        int        thread_id = -1;
    };

    LockWaitManager() = default;

    // 事务开始钩子（DTX::TxBegin）：登记本节点活跃事务
    void OnTxnBegin(uint64_t start_ts) {
        std::lock_guard<std::mutex> lk(mtx_);
        active_txs_.insert(start_ts);
// [DEBUG 日志已关] LwTrace("txn_begin", "ts=", start_ts, "active_txs=", active_txs_.size());
    }

    // 登记锁等待关系时候调用的，会返回两个结果之一：
    // 1. 你继续等着，我还在用呢
    // 2. 我没在用了，但是我也不知道它跑哪去了，你可以再尝试一下获取
    bool OnRegisterTupleWait(const TupleWaitKey& key, uint64_t holder_ts, uint64_t waiter_ts,
                             node_id_t waiter_node) {
        std::lock_guard<std::mutex> lk(mtx_);
        // 判据：登记里写的这个 holder 【自己】有没有放掉过这条 key（过户走了，或者解锁时
        // 根本没人等）。放掉过就说明对方读到的是放锁之前的旧字节，必须挡回去重读元组；
        // 登记写的是别人（没放过这把锁的 holder）要放行，否则会被一直挡着空转。
        // 注意判据必须落在 holder_ts 自己的记录上：key -> 最后一个放锁人 的单个墓碑
        // 会被后来者覆盖/被 TryAcceptHandoff erase，挡不住"更早放锁但事务还活着"的旧 holder。
        auto fwd_it = forwarded_keys_.find(holder_ts);
        if (fwd_it != forwarded_keys_.end() && fwd_it->second.count(key) != 0) {
            stat_wait_forwarded.fetch_add(1, std::memory_order_relaxed);
// [DEBUG 日志已关] LwTrace("reg", "key=", LwKeyStr(key), "holder=", holder_ts, "waiter=", waiter_ts,
//                    "waiter_node=", (int)waiter_node, "verdict=HOLDER_ALREADY_RELEASED->granted",
//                    "released_keys=", fwd_it->second.size());
            return true;
        }

        // 之前拿着这个元组锁的事务已经提交 or 回滚了，那就重试吧
        if (active_txs_.find(holder_ts) == active_txs_.end()) {
// [DEBUG 日志已关] LwTrace("reg", "key=", LwKeyStr(key), "holder=", holder_ts, "waiter=", waiter_ts,
//                    "waiter_node=", (int)waiter_node, "verdict=HOLDER_NOT_ACTIVE->granted",
//                    "q_exists=", (local_queues_.count(key) != 0));
            return true;
        }

        // 挂到等待队列里
        auto& q = local_queues_[key];
        const bool brand_new_queue = (q.owner_holder_ts == 0);
        // 只有【全新的队列】才认主并登记反向索引。
        // 注意不能用 q.waiters.empty() 判断：过户就地安装时可能装进来一条空队列
        // （tail 为空），它的 owner 已经是新 holder，此时若被迟到登记抢走 owner，
        // 会往已结束的 holder 名下挂队列 -> FinishTx 体检误炸。
        if (brand_new_queue) {
            q.owner_holder_ts = holder_ts;
            holder_keys_[holder_ts].insert(key);
        }
        q.waiters.emplace_back(waiter_ts, waiter_node);
// [DEBUG 日志已关] LwTrace("reg", "key=", LwKeyStr(key), "holder=", holder_ts, "waiter=", waiter_ts,
//                "waiter_node=", (int)waiter_node, "verdict=QUEUED",
//                "new_queue=", brand_new_queue, "q_owner=", q.owner_holder_ts,
//                "depth=", (int)q.waiters.size(), "version=", q.version);
        if (q.owner_holder_ts != holder_ts) {
            // 迟到登记：元组字节上的 holder 已经换人了，但它排进了新 owner 的队列（不去抢 owner）。
            // 这条本身不是错误，但排查"队列归谁"时是关键线索。
            LOG(WARNING) << "[LW] reg attached to another owner: key=" << LwKeyStr(key)
                         << " reg_holder=" << holder_ts << " q_owner=" << q.owner_holder_ts
                         << " waiter=" << waiter_ts << " depth=" << q.waiters.size();
        }

        // DEBUG 信息
        const uint64_t depth = (uint64_t)q.waiters.size();
        uint64_t prev = stat_wq_depth_max.load(std::memory_order_relaxed);
        while (depth > prev &&
               !stat_wq_depth_max.compare_exchange_weak(prev, depth, std::memory_order_relaxed)) {
        }

        return false;
    }

    // ============ 解锁点：把锁过户给下一个「确认还活着」的等待者 ============
    // 为什么必须"先问再接"：FIFO 里可能躺着一具尸体 —— 死锁 victim 判环后只注销了本地
    // local_waiters_，还留在 holder 节点那条队列里。异步过户（先把字节写给队首、再发通知）
    // 会把锁写进这个死人：元组永久锁死、它后面的等待者永久挂起、新来的人只能空转。
    // 所以改成同步确认：
    //   1) 取队首候选（本地候选直接查 local_waiters_；远端候选发一次 HandoffQueue 同步问）
    //   2) 只有 accepted=true 才把字节写给这个候选；accepted=false 说明候选已经不在等待，
    //      把这具尸体从队里丢掉，继续问下一个
    //   3) 所有候选都被拒 -> 返回 false，调用方按"没人等"处理（lock=UNLOCKED）
    // 字节只写给已确认的接手者，所以不存在"事后修复字节"的需求。
    //
    // 配套不变式（见 UnregisterLocalWaiter）：被接受的接手者如果此刻正好决定回滚，
    // 注销时会看到 flag 已置位，必须改为接受这把锁 —— 否则同样会把锁留给死人。
    bool HandoffLockSync(const TupleWaitKey& key, uint64_t holder_ts, node_id_t self_node,
                         brpc::Channel* nodes_channel,
                         uint64_t* out_timeStamp, uint8_t* out_holder_node,
                         PendingHandoff* out_handoff) {
        // 先立放锁记录（只立一次）：这条 key 从此刻起"已经不在我手里"。必须放在任何 return 之前，
        // 否则"解锁时没人等"这条路径不留痕迹，迟到的登记就会在 holder 名下建出一条
        // "主人永远不会来唤醒"的孤儿队列（等待者永久挂起 + FinishTx 体检 assert）。
        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto fit = forwarded_keys_.find(holder_ts);
            if (fit != forwarded_keys_.end() && fit->second.count(key) != 0) {
                // 同一个事务对同一个元组解锁了两次：第二次会把接手者的锁抹掉（或撞上已换
                // owner 的队列），属于真 bug，直接暴露（commit/abort 路径已按去重修过）。
                LOG(ERROR) << "[LW] duplicate unlock: holder=" << holder_ts
                           << " key=" << LwKeyStr(key);
                assert(false);
            }
            MarkForwardedLocked(key, holder_ts);
        }

        while (true) {
            PendingHandoff ph;
            // 队尾里在别的节点的等待者：出了临界区再按节点合并发消息（本地部分直接改）
            std::unordered_map<node_id_t, std::vector<uint64_t>> remote_updates;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                auto it = local_queues_.find(key);
                // 根本没人在等我提交，直接返回就行（墓碑已立，迟到登记会被挡回去重读）
                if (it == local_queues_.end()) {
// [DEBUG 日志已关] LwTrace("handoff_in", "key=", LwKeyStr(key), "unlocker=", holder_ts,
//                            "queue=NONE->false", "tomb_now=", holder_ts);
                    return false;
                }
                auto& q = it->second;
                if (q.owner_holder_ts != holder_ts) {
                    // 断言前的完整现场：谁在解锁 vs 这条队列归谁、队列里还有谁、双方是否还活跃
                    std::ostringstream os;
                    os << "[LW] !!! handoff owner mismatch: key=" << LwKeyStr(key)
                       << " unlocker=" << holder_ts << " q_owner=" << q.owner_holder_ts
                       << " depth=" << q.waiters.size() << " version=" << q.version
                       << " unlocker_active=" << (active_txs_.count(holder_ts) != 0)
                       << " q_owner_active=" << (active_txs_.count(q.owner_holder_ts) != 0)
                       << " q_owner_released_key="
                       << ((forwarded_keys_.count(q.owner_holder_ts) != 0 &&
                            forwarded_keys_.at(q.owner_holder_ts).count(key) != 0) ? 1 : 0)
                       << " waiters=[";
                    size_t shown = 0;
                    for (const auto& w : q.waiters) {
                        if (shown++ >= 8) { os << " ..."; break; }
                        os << " (ts" << w.first << "@n" << (int)w.second << ")";
                    }
                    os << " ]";
                    LOG(ERROR) << os.str();
                }
                assert(q.owner_holder_ts == holder_ts);

                // （墓碑已在入口处立好，见上）
                if (q.waiters.empty()) {
                    const uint64_t empty_version = q.version;   // erase 之后 q 就悬空了，先取出来
                    local_queues_.erase(it);
                    auto kit = holder_keys_.find(holder_ts);
                    if (kit != holder_keys_.end()) kit->second.erase(key);
// [DEBUG 日志已关] LwTrace("handoff_in", "key=", LwKeyStr(key), "unlocker=", holder_ts,
//                            "queue_EMPTY->false", "version=", empty_version);
                    return false;
                }
                const auto head = q.waiters.front();
                q.waiters.pop_front();

                ph.key = key;
                ph.new_holder_ts = head.first;
                ph.new_holder_node = head.second;
                ph.version = ++q.version;
                ph.tail.assign(q.waiters.begin(), q.waiters.end());
// [DEBUG 日志已关] LwTrace("handoff_pick", "key=", LwKeyStr(key), "unlocker=", holder_ts,
//                        "new_holder=", ph.new_holder_ts, "new_holder_node=", (int)ph.new_holder_node,
//                        "version=", ph.version, "tail=", ph.tail.size());

                if (head.second == self_node) {
                    // 本地候选：自己就能判定死活
                    auto wit = local_waiters_.find(head.first);
                    if (wit == local_waiters_.end()) {
// [DEBUG 日志已关] LwTrace("handoff_skip_dead", "key=", LwKeyStr(key),
//                                "dead_candidate=", head.first, "where=LOCAL");
                        continue;       // 尸体丢掉，继续问下一个
                    }
                    // 接受：就地过继队列 + 抢先置 flag（adoption），
                    // 这样它此刻若正好决定回滚，注销时会看到 flag 并改为接受这把锁
                    wit->second->flag.store(1, std::memory_order_release);
                    q.owner_holder_ts = head.first;
                    holder_keys_[head.first].insert(key);
                    {
                        auto kit = holder_keys_.find(holder_ts);
                        if (kit != holder_keys_.end()) kit->second.erase(key);
                    }
                    // 队伍后面那些人等的已经不是我了，是新的 owner：记录必须跟着改，
                    // 否则死锁检测的走查会沿着我这具"已经走人的旧 holder"断链。
                    UpdateTailHolderLocked(ph.tail, head.first, self_node, holder_ts,
                                           self_node, &remote_updates);
                    ph.local_wake = true;    // 真正唤醒要等 ReleaseXPage 之后
// [DEBUG 日志已关] LwTrace("handoff_accept", "key=", LwKeyStr(key), "new_holder=", head.first,
//                            "route=LOCAL", "tail=", ph.tail.size(),
//                            "depth_after=", (int)q.waiters.size());
                } else {
// [DEBUG 日志已关] LwTrace("handoff_confirm", "key=", LwKeyStr(key), "candidate=", head.first,
//                            "candidate_node=", (int)head.second, "route=RPC");
                }
            }

            if (ph.local_wake) {
                SendHolderUpdateRPCs(remote_updates, ph.new_holder_ts, ph.new_holder_node,
                                     nodes_channel);
                if (out_timeStamp) *out_timeStamp = ph.new_holder_ts;
                if (out_holder_node) *out_holder_node = (uint8_t)ph.new_holder_node;
                if (out_handoff) *out_handoff = std::move(ph);
                stat_handoff.fetch_add(1, std::memory_order_relaxed);
                return true;
            }

            // 远端候选：同步确认。对方确认"这个人还在等"才会在那边装队列 + 唤醒；
            // 唤醒时页还在我手里没关系——它拿不到页，会等我 ReleaseXPage。
            if (!ConfirmHandoffRemote(ph, nodes_channel)) {
// [DEBUG 日志已关] LwTrace("handoff_rejected", "key=", LwKeyStr(key),
//                        "dead_candidate=", ph.new_holder_ts, "where=REMOTE");
                continue;               // 尸体丢掉，继续问下一个
            }
            {
                std::lock_guard<std::mutex> lk(mtx_);
                // 远端已经收下队列：本地这条删掉（墓碑留到 FinishTx）
                auto it = local_queues_.find(key);
                assert(it != local_queues_.end() && it->second.owner_holder_ts == holder_ts);
                local_queues_.erase(it);
                auto kit = holder_keys_.find(holder_ts);
                if (kit != holder_keys_.end()) kit->second.erase(key);
            }
// [DEBUG 日志已关] LwTrace("handoff_accept", "key=", LwKeyStr(key), "new_holder=", ph.new_holder_ts,
//                    "new_holder_node=", (int)ph.new_holder_node, "route=RPC",
//                    "tail=", ph.tail.size());
            SendHolderUpdateRPCs(remote_updates, ph.new_holder_ts, ph.new_holder_node,
                                 nodes_channel);
            if (out_timeStamp) *out_timeStamp = ph.new_holder_ts;
            if (out_holder_node) *out_holder_node = (uint8_t)ph.new_holder_node;
            if (out_handoff) *out_handoff = std::move(ph);
            stat_handoff.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
    }

    // ================= 解锁点第二步：本地接手者的唤醒 =================
    // 调用时机：**该元组的 ReleaseXPage 之后**。不能更早——接手者一被唤醒就会回来
    // 拉这个页；若页还被本事务占着，同节点同线程下会协程级死锁（holder 让不出线程，
    // 接手者也拿不到页）。远端过户在 HandoffLockSync 的确认 RPC 里已经由接手者节点
    // 装好队列并唤醒，这里只处理本地接手者。
    void DeliverHandoff(const PendingHandoff& ph, node_id_t self_node,
                        brpc::Channel* /*nodes_channel*/) {
// [DEBUG 日志已关] LwTrace("deliver", "key=", LwKeyStr(ph.key), "new_holder=", ph.new_holder_ts,
//                "new_holder_node=", (int)ph.new_holder_node, "self_node=", (int)self_node,
//                "local_wake=", ph.local_wake);
        if (ph.local_wake) {
            // flag 已在过户时抢占，这里补 notify + 把 fiber 重新入队
            OnWakeTupleWaiter(ph.new_holder_ts);
        }
    }

    // 接力包到达接手者所在节点：只有确认「接手者还在 local_waiters_」才接受。
    // 返回 false = 这个人已经不在等待（已回滚/已超时），发送方会把这具尸体丢掉、问下一个。
    bool TryAcceptHandoff(const TupleWaitKey& key, uint64_t owner_ts, uint64_t version,
                          const std::vector<std::pair<uint64_t, node_id_t>>& tail,
                          node_id_t self_node, brpc::Channel* nodes_channel) {
        bool accepted = false;
        // 队尾里在别的节点的等待者：出了临界区再按节点合并发消息
        std::unordered_map<node_id_t, std::vector<uint64_t>> remote_updates;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto wit = local_waiters_.find(owner_ts);
            if (wit != local_waiters_.end()) {
                accepted = true;
                // 抢占 flag（adoption）：它若此刻刚决定回滚，注销时会看到 flag 而改为
                // 接受这把锁，不会变成幽灵接手者。
                wit->second->flag.store(1, std::memory_order_release);
                // 这里【不要】去清"本 key 的放锁记录"：原来那版会 forwarded_at_.erase(key)，
                // 等于把这条 key 的全部保护一起抹掉 —— 之后任何"读到旧 holder 字节"的迟到
                // 登记都能重新入队（包括已经放过这把锁的老 holder），正是孤儿队列的来源。
                // 本方（owner_ts）自己的放锁记录不需要清：一个事务不会放了锁再回来拿同一把
                // 元组锁（解锁点只在 commit/abort），所以 owner_ts 不可能在自己的放锁集合里。
                auto& q = local_queues_[key];
                // 不兜底：接力包版本必须单调，否则说明包乱序/重放，直接暴露
                assert(version >= q.version);
                q.version = version;
                // tail 全是比"早到本地的登记"更早的等待者 -> 插到前面，保持 FIFO。
                q.waiters.insert(q.waiters.begin(), tail.begin(), tail.end());
                q.owner_holder_ts = owner_ts;
                holder_keys_[owner_ts].insert(key);
                // 队尾这些人等的已经不是发件人了，是新的 owner：记录跟着改，
                // 否则死锁检测的走查会沿着"已经走人的旧 holder"断链，环看不见。
                // 这里不知道发件人是谁，所以不做事后校验（old_owner_ts=0）。
                UpdateTailHolderLocked(tail, owner_ts, self_node, 0, self_node, &remote_updates);
// [DEBUG 日志已关] LwTrace("accept_handoff", "key=", LwKeyStr(key), "owner=", owner_ts,
//                        "version=", version, "tail=", tail.size(),
//                        "depth_after=", (int)q.waiters.size());
            } else {
// [DEBUG 日志已关] LwTrace("reject_handoff_dead", "key=", LwKeyStr(key), "owner=", owner_ts,
//                        "version=", version, "tail=", tail.size());
            }
        }
        if (accepted) {
            SendHolderUpdateRPCs(remote_updates, owner_ts, self_node, nodes_channel);
            OnWakeTupleWaiter(owner_ts);
        }
        return accepted;
    }

    // 唤醒本节点某个等待者：把锁交给它了。
    // 只有过户确认（本地候选 / 远端的 TryAcceptHandoff）会走到这里，所以只按单个 ts 唤醒。
    void OnWakeTupleWaiter(uint64_t waiter_ts) {
        std::shared_ptr<WaitState> st;
        uint64_t recorded_holder = 0;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = local_waiters_.find(waiter_ts);
            if (it == local_waiters_.end()) {
                // 过户前已经确认过它还在等待（先问再接），所以这里只剩一种可能：被过户者
                // 在这个窗口里自己醒来（YieldWithTimeOnThread 到期 -> 看到 flag 已置位 ->
                // 走 adoption 分支注销）。adoption 会把它的结果改判成 GRANTED_RETRY，而且
                // 元组字节早已写成它的 ts，所以【锁没有丢】：它重试加锁时会命中
                // "lock==EXCLUSIVE_LOCKED && timeStamp==start_ts" 的自持锁分支直接拿到。
                // 保留成 WARNING（不静默）只是用来观察这个窗口出现的频率，不是不变式违反。
                LOG(WARNING) << "[LW] wake target already self-exited via adoption "
                                "(lock NOT dropped): ts="
                             << waiter_ts << " local_waiters=" << local_waiters_.size();
                return;
            }
            st = it->second;
            recorded_holder = st->holder_ts;   // holder_ts 是可更新字段，须在锁内读
        }
// [DEBUG 日志已关] LwTrace("wake", "waiter=", waiter_ts, "was_waiting_for=", recorded_holder,
//                "via_fiber=", (st->sched != nullptr && st->fiber != nullptr));
        st->flag.store(1, std::memory_order_release);
        st->cv.notify_all();
        // fiber 上下文：把等待中的 fiber 重新入队（钉在它自己的线程上）。
        if (st->sched != nullptr && st->fiber != nullptr) {
            st->sched->schedule(st->fiber, st->thread_id);
        }
    }

    // 询问下，waiter_ts 目前在等谁
    bool QueryWaiterHolder(uint64_t waiter_ts, uint64_t* holder_ts, node_id_t* holder_node) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = local_waiters_.find(waiter_ts);
        if (it == local_waiters_.end()) return false;
        *holder_ts = it->second->holder_ts;
        *holder_node = it->second->holder_node;
        return true;
    }
    
    // 过户落地：把队列里"排在后面的人"记录的「我在等谁」改成新 owner。
    // 不更新的话，死锁检测的链式走查会沿着【已经走人的旧 holder】断链 -> 真环看不见
    // -> 双方一直等到看门狗 assert（真实事故：队尾 W2 的记录停在旧 holder H 上）。
    // 必须持有 mtx_ 时调用；本地的就地改，远端的按节点收集到 remote_out 里出去再发。
    void UpdateTailHolderLocked(const std::vector<std::pair<uint64_t, node_id_t>>& tail,
                                uint64_t new_owner_ts, node_id_t new_owner_node,
                                uint64_t old_owner_ts, node_id_t self_node,
                                std::unordered_map<node_id_t, std::vector<uint64_t>>* remote_out) {
        for (const auto& w : tail) {
            if (w.second == self_node) {
                auto it = local_waiters_.find(w.first);
                if (it == local_waiters_.end()) continue;   // 本地但已不在等待（死人），跳过
                const uint64_t recorded = it->second->holder_ts;
                if (old_owner_ts != 0 && recorded != old_owner_ts) {
                    // 不变式：排在我后面的人，记录应该正好是"我"（本次过户前的 owner）。
                    // 对不上说明历史上还有别的地方漏更新了，打出来。
                    LOG(WARNING) << "[LW] tail waiter holder mismatch: waiter=" << w.first
                                 << " recorded=" << recorded
                                 << " expected_old_owner=" << old_owner_ts
                                 << " new_owner=" << new_owner_ts;
                }
                it->second->holder_ts = new_owner_ts;
                it->second->holder_node = new_owner_node;
// [DEBUG 日志已关] LwTrace("update_tail_holder", "waiter=", w.first, "old_holder=", recorded,
//                        "new_holder=", new_owner_ts, "route=LOCAL");
            } else {
                (*remote_out)[w.second].push_back(w.first);
            }
        }
    }

    // 把"你们在等的人换成 X 了"通知给远端等待者所在节点：一次过户每个节点最多一条消息。
    // 不兜底：要发就得有通道，RPC 失败直接崩。
    void SendHolderUpdateRPCs(const std::unordered_map<node_id_t, std::vector<uint64_t>>& remote,
                              uint64_t new_owner_ts, node_id_t new_owner_node,
                              brpc::Channel* nodes_channel) {
        if (remote.empty()) return;
        assert(nodes_channel != nullptr);
        assert(new_owner_node >= 0 && new_owner_node < ComputeNodeCount);
        for (const auto& kv : remote) {
            assert(kv.first >= 0 && kv.first < ComputeNodeCount);
            compute_node_service::UpdateWaiterHolderRequest req;
            compute_node_service::UpdateWaiterHolderResponse resp;
            brpc::Controller cntl;
            cntl.set_timeout_ms(kRpcTimeoutMs);
            req.set_new_holder_ts(new_owner_ts);
            req.set_new_holder_node((int32_t)new_owner_node);
            for (uint64_t ts : kv.second) req.add_waiter_ts(ts);
            compute_node_service::ComputeNodeService_Stub stub(&nodes_channel[kv.first]);
            stub.UpdateWaiterHolder(&cntl, &req, &resp, nullptr);
            if (cntl.Failed()) {
                LOG(ERROR) << "[LOCK_WAIT] UpdateWaiterHolder rpc failed: " << cntl.ErrorText()
                           << ", to_node=" << (int)kv.first
                           << ", count=" << kv.second.size()
                           << ", new_holder=" << new_owner_ts;
                assert(false);
            }
// [DEBUG 日志已关] LwTrace("send_holder_update", "to_node=", (int)kv.first, "count=", kv.second.size(),
//                    "new_holder=", new_owner_ts);
        }
    }

    // 收到"你们在等的人换成 X 了"：把本地这些等待者的记录改掉（死锁检测的链跟着锁走）
    void OnUpdateWaiterHolder(const std::vector<uint64_t>& waiter_ts_list,
                              uint64_t new_holder_ts, node_id_t new_holder_node) {
        std::lock_guard<std::mutex> lk(mtx_);
        for (uint64_t ts : waiter_ts_list) {
            auto it = local_waiters_.find(ts);
            if (it == local_waiters_.end()) continue;   // 已经不在了（死人/已唤醒），跳过
            const uint64_t recorded = it->second->holder_ts;
            it->second->holder_ts = new_holder_ts;
            it->second->holder_node = new_holder_node;
// [DEBUG 日志已关] LwTrace("recv_holder_update", "waiter=", ts, "old_holder=", recorded,
//                    "new_holder=", new_holder_ts);
        }
    }

    // 记"key 已经从 holder_ts 手里放掉了"（必须持有 mtx_）：过户走了，或者解锁时根本没人在等。
    // 迟到登记只要看到【登记里写的那个 holder】放过这把锁，就会被挡回去重读元组。
    // 记录按 holder 记账，生命周期恰好等于它的活跃期。
    void MarkForwardedLocked(const TupleWaitKey& key, uint64_t holder_ts) {
        forwarded_keys_[holder_ts].insert(key);
    }

    // 事务收尾：
    //   1) 注销活跃表 —— 此后读到旧 holder 的迟到登记会被 OnRegisterTupleWait 的
    //      active_txs_ 检查挡掉（返回 granted，让调用方重读元组）
    //   2) 体检：holder_keys_ 里不该再有它名下的队列，有就 assert 暴露
    //   3) 整条清掉本事务的放锁记录（forwarded_keys_[holder_ts]）
    void FinishTx(uint64_t holder_ts) {
        std::lock_guard<std::mutex> lk(mtx_);
        active_txs_.erase(holder_ts);

        // holder_keys_[holder_ts] 代表了挂在 holder_ts 下的等待队列
        // 按道理这里不应该有等待队列了，因为前边要么转移走了，要么清空了
        auto kit = holder_keys_.find(holder_ts);
        if (kit != holder_keys_.end() && !kit->second.empty()) {
            // 归因用的现场：放锁记录还在（体检放在清理之前），可以判定这条残留队列
            // 是不是挂在一个【已经放过这把锁】的 holder 名下：
            //   holder_released_this_key=1 -> 迟到登记穿透了挡板（旧单值墓碑被覆盖/erase）
            //   holder_released_this_key=0 -> holder 从没放过这把锁 = 漏解锁
            auto released_it = forwarded_keys_.find(holder_ts);
            for (const auto& k : kit->second) {
                auto qit = local_queues_.find(k);
                const bool qexists = (qit != local_queues_.end());
                const bool released = (released_it != forwarded_keys_.end() &&
                                       released_it->second.count(k) != 0);
                LOG(ERROR) << "[LOCK_WAIT] FinishTx 发现未过户的队列: holder_ts=" << holder_ts
                           << " key=(table=" << k.table_id << ",page=" << k.page_no
                           << ",slot=" << k.slot_no << ")"
                           << " queue_exists=" << qexists
                           << " owner=" << (qexists ? qit->second.owner_holder_ts : 0)
                           << " depth=" << (qexists ? (int)qit->second.waiters.size() : 0)
                           << " holder_released_this_key=" << released;
            }
            assert(false);
        }
        // 放锁记录只属于 holder_ts 自己，整条删即可（不需要像旧单值墓碑那样按值校验，
        // 因为不存在"这条 key 的记录已经属于更新的 holder"这种情况）。删掉之后，同一批
        // 迟到登记由上面的 active_txs_ 检查兜住（holder 已不活跃 -> granted）。
        forwarded_keys_.erase(holder_ts);
// [DEBUG 日志已关] LwTrace("finish", "ts=", holder_ts, "active_txs_left=", active_txs_.size(),
//                "still_has_queues=", (holder_keys_.count(holder_ts) ? (int)holder_keys_[holder_ts].size() : 0));

        holder_keys_.erase(holder_ts);
    }

    // 同步把接力包发给候选接手者节点，拿回执：对方只在确认「接手者还在 local_waiters_」
    // 时才接受（并在那边装队列 + 唤醒），否则回 accepted=false，我们换个候选继续问。
    // 不兜底：RPC 失败直接崩，暴露环境/逻辑问题。
    bool ConfirmHandoffRemote(const PendingHandoff& ph, brpc::Channel* nodes_channel) {
        assert(nodes_channel != nullptr);
        assert(ph.new_holder_node >= 0 && ph.new_holder_node < ComputeNodeCount);
        compute_node_service::HandoffQueueRequest req;
        compute_node_service::HandoffQueueResponse resp;
        brpc::Controller cntl;
        req.set_table_id((int32_t)ph.key.table_id);
        req.set_page_no((int32_t)ph.key.page_no);
        req.set_slot_no(ph.key.slot_no);
        req.set_owner_ts(ph.new_holder_ts);
        req.set_version(ph.version);
        for (const auto& w : ph.tail) {
            req.add_tail_ts(w.first);
            req.add_tail_node((int32_t)w.second);
        }
        // RPC 超时与等待看门狗解耦，固定值
        cntl.set_timeout_ms(kRpcTimeoutMs);
        compute_node_service::ComputeNodeService_Stub stub(&nodes_channel[ph.new_holder_node]);
        stub.HandoffQueue(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "[LOCK_WAIT] HandoffQueue rpc failed: " << cntl.ErrorText()
                       << ", to_node=" << (int)ph.new_holder_node
                       << ", owner_ts=" << ph.new_holder_ts << ", tail=" << ph.tail.size();
            assert(false);
            return false;   // NDEBUG 下才会走到
        }
// [DEBUG 日志已关] LwTrace("handoff_rpc_result", "key=", LwKeyStr(ph.key),
//                "to_node=", (int)ph.new_holder_node, "owner=", ph.new_holder_ts,
//                "version=", ph.version, "tail=", ph.tail.size(),
//                "accepted=", resp.accepted());
        return resp.accepted();
    }

    // 调试用：这条 key 在本节点等待队列的当前深度（0 = 没有队列）
    size_t WaitQueueDepth(const TupleWaitKey& key) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = local_queues_.find(key);
        return it == local_queues_.end() ? 0 : it->second.waiters.size();
    }

    // 调试用：这条 key 在本节点有没有等待队列（有 = 现在有人等这把锁）
    bool HasLocalQueue(const TupleWaitKey& key) {
        std::lock_guard<std::mutex> lk(mtx_);
        return local_queues_.count(key) != 0;
    }

    // 调试用：把某条 key 在本节点的队列状态 + 相关事务是否还活跃打成一行。
    // 用于"等待者为什么一直没被唤醒"这类问题的现场取证（不改变任何行为）。
    void DumpKeyState(const char* tag, const TupleWaitKey& key, uint64_t holder_ts) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = local_queues_.find(key);
        std::ostringstream os;
        os << "[LW] " << tag << " key=" << LwKeyStr(key)
           << " q_exists=" << (it != local_queues_.end());
        if (it != local_queues_.end()) {
            os << " q_owner=" << it->second.owner_holder_ts
               << " depth=" << it->second.waiters.size()
               << " version=" << it->second.version << " waiters=[";
            size_t shown = 0;
            for (const auto& w : it->second.waiters) {
                if (shown++ >= 8) { os << " ..."; break; }
                os << " (ts" << w.first << "@n" << (int)w.second << ")";
            }
            os << " ]";
        }
        os << " holder_active=" << (active_txs_.count(holder_ts) != 0)
           << " holder_released_key="
           << ((forwarded_keys_.count(holder_ts) != 0 &&
                forwarded_keys_.at(holder_ts).count(key) != 0) ? 1 : 0)
           << " local_waiters=" << local_waiters_.size()
           << " active_txs=" << active_txs_.size();
        LOG(ERROR) << os.str();
    }

    // 注册/摘除本节点等待者的唤醒状态（WaitForLock 内部使用；
    // 测试可绕过 WaitForLock 直接驱动 OnWakeTupleWaiter 唤醒路径）。
    void RegisterLocalWaiter(uint64_t waiter_ts, std::shared_ptr<WaitState> state) {
        std::lock_guard<std::mutex> lk(mtx_);
        assert(local_waiters_.find(waiter_ts) == local_waiters_.end());
        local_waiters_[waiter_ts] = std::move(state);
    }
    // 摘除本节点等待者。返回 true = 摘除时 flag 已置位（期间有人把这把锁过户给了它）。
    // 调用方拿到 true 必须【接受这把锁】（重试加锁），不要回滚 —— 否则这把锁就留给了一个
    // 已经退出等待的事务，元组会被永久锁死（这正是"幽灵接手者"的来源）。
    bool UnregisterLocalWaiter(uint64_t waiter_ts) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = local_waiters_.find(waiter_ts);
        assert(it != local_waiters_.end());
        const bool adopted = (it->second->flag.load(std::memory_order_acquire) != 0);
        local_waiters_.erase(it);
        return adopted;
    }

    // 遇到锁冲突时调用：登记等待关系，然后等待 holder 释放元组锁。
    LockWaitResult WaitForLock(uint64_t waiter_ts, node_id_t self_node, uint64_t holder_ts,
                               node_id_t holder_node,
                               brpc::Channel* nodes_channel,
                               table_id_t table_id, page_id_t page_no, int slot_no, itemkey_t item_key) {
        const auto t_enter = std::chrono::steady_clock::now();
        stat_wait_total.fetch_add(1, std::memory_order_relaxed);
// [DEBUG 日志已关] LwTrace("wait_enter", "waiter=", waiter_ts, "holder=", holder_ts,
//                "holder_node=", (int)holder_node, "self_node=", (int)self_node,
//                "key=", LwKeyStr(TupleWaitKey{table_id, page_no, slot_no}), "item_key=", item_key);

        assert(holder_node >= 0 && holder_node < ComputeNodeCount);

        // 目前持锁的，是否是我自己？
        const bool holder_is_local = (holder_node == self_node);

        auto wait_state = std::make_shared<WaitState>();
        wait_state->holder_ts = holder_ts;
        wait_state->holder_node = holder_node;
        wait_state->sched = Scheduler::GetThis();
        if (wait_state->sched != nullptr) {
            wait_state->fiber = Fiber::GetThis();
            wait_state->thread_id = getThreadID();
        }
        // 必须先在自己本地注册，再向 holder 发登记 RPC：
        // 否则会出现我给对边发送锁等待 rpc，对面先一步 commit 释放锁，然后 rpc 给我发现我没有等待关系，以为我回滚了的问题
        RegisterLocalWaiter(waiter_ts, wait_state);

        bool granted = false;
        const TupleWaitKey wait_key{table_id, page_no, slot_no};
        if (holder_is_local) {
            granted = OnRegisterTupleWait(wait_key, holder_ts, waiter_ts, self_node);
        } else {
            compute_node_service::RegisterTupleWaitRequest reg_req;
            compute_node_service::RegisterTupleWaitResponse reg_resp;
            brpc::Controller reg_cntl;
            reg_cntl.set_timeout_ms(5000);
            reg_req.set_table_id(table_id);
            reg_req.set_page_no(page_no);
            reg_req.set_slot_no(slot_no);
            reg_req.set_item_key(item_key);
            reg_req.set_holder_ts(holder_ts);
            reg_req.set_waiter_ts(waiter_ts);
            reg_req.set_waiter_node(self_node);
            compute_node_service::ComputeNodeService_Stub reg_stub(&nodes_channel[holder_node]);
            const auto t_rpc0 = std::chrono::steady_clock::now();
            reg_stub.RegisterTupleWait(&reg_cntl, &reg_req, &reg_resp, nullptr);
            stat_wait_reg_rpc_time_ns.fetch_add(
                NsBetween(t_rpc0, std::chrono::steady_clock::now()),
                std::memory_order_relaxed);
            if (reg_cntl.Failed()) {
                // 不兜底：登记 RPC 失败直接崩，暴露环境/逻辑问题，不做软回滚降级
                stat_wait_reg_fail.fetch_add(1, std::memory_order_relaxed);
                LOG(ERROR) << "[LOCK_WAIT] RegisterTupleWait rpc failed: " << reg_cntl.ErrorText()
                           << ", holder_node=" << holder_node << ", waiter_ts=" << waiter_ts;
                assert(false);
                return LockWaitResult::TIMEOUT_ABORT;   // NDEBUG 下才会走到
            }
            granted = reg_resp.granted();
        }
// [DEBUG 日志已关] LwTrace("wait_reg_done", "waiter=", waiter_ts, "holder=", holder_ts,
//                "key=", LwKeyStr(wait_key), "holder_is_local=", holder_is_local,
//                "granted=", granted);

        const auto t_reg_end = std::chrono::steady_clock::now();
        const uint64_t reg_ns = NsBetween(t_enter, t_reg_end);

        // 如果授予我锁了，那就把本地锁等待的那个关系给它去掉
        // 两种情况会授予：
        // 1. 正在锁转移
        // 2. 目前持锁的人，已经把锁放了
        if (granted) {
            UnregisterLocalWaiter(waiter_ts);
            stat_wait_direct_granted.fetch_add(1, std::memory_order_relaxed);
            RecordEpisode(NsBetween(t_enter, std::chrono::steady_clock::now()),
                          reg_ns, 0, 0, false, holder_is_local);
            // 重试一下，再尝试去拿这个元组的锁
            return LockWaitResult::GRANTED_RETRY;
        }

        // DEBUG 信息
        const int check_interval_ms = DEADLOCK_CHECK_INTERVAL_MS > 0 ? DEADLOCK_CHECK_INTERVAL_MS : 100;
        const int watchdog_ms = LOCK_WAIT_TIMEOUT_MS > 0 ? LOCK_WAIT_TIMEOUT_MS : 30000;
        const auto watchdog_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(watchdog_ms);
        auto next_check = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(check_interval_ms);
        int backoff_ms = check_interval_ms;
        uint64_t suspend_ns = 0;
        uint64_t detect_ns = 0;

        bool did_suspend = false;
        LockWaitResult ret = LockWaitResult::GRANTED_RETRY;
        while (true) {
            if (wait_state->flag.load(std::memory_order_acquire) != 0) {
                ret = LockWaitResult::GRANTED_RETRY;      // holder 已释放，醒来重试加锁
                break;
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= watchdog_deadline) {
                // 目前我们跑的都是 TP 的负载，按道理不应该超时，这里打个断言来 DEBUG
                // TODO：后续如果负载变重了，单个 SQL 执行时间长的话，这个要去掉
                LOG(ERROR) << "[LOCK_WAIT] watchdog tripped: waiter_ts=" << waiter_ts
                           << " holder_ts=" << holder_ts << " holder_node=" << holder_node
                           << " key=(table=" << table_id << ",page=" << page_no
                           << ",slot=" << slot_no << ")"
                           << " jumped=" << watchdog_ms << "ms"
                           << " suspended=" << did_suspend;
                // 现场取证：这条 key 的队列现在归谁、还有谁在等、holder 是否还活跃
                DumpKeyState("watchdog_state", wait_key, holder_ts);
                assert(false);
                ret = LockWaitResult::TIMEOUT_ABORT;   // NDEBUG 下才会走到
                break;
            }

            if (now >= next_check) {
                // 主动去各节点收集锁等待关系，成环就回滚其中一个事务
                stat_deadlock_check.fetch_add(1, std::memory_order_relaxed);
                const auto t_det0 = std::chrono::steady_clock::now();

                // 我来主动收集一下各个节点上的事务信息，判断是否有环
                // 有环的话，就回滚本事务
                // 每次检查都重新读一次"我在等谁"：过户落地时新 owner 的节点会把这条记录
                // 更新成新 owner；用 WaitForLock 入参里的那份快照会沿着"已经走人的旧 holder"
                // 断链，环就永远查不出来（真实事故）。读不到就退回入口那份。
                uint64_t cur_holder_ts = holder_ts;
                node_id_t cur_holder_node = holder_node;
                QueryWaiterHolder(waiter_ts, &cur_holder_ts, &cur_holder_node);
                const bool cycle = DetectDeadlockByChainWalk(waiter_ts, self_node, cur_holder_ts,
                                                             cur_holder_node, nodes_channel);
                detect_ns += NsBetween(t_det0, std::chrono::steady_clock::now());
                if (cycle) {
                    ret = LockWaitResult::VICTIM_ABORT;
                    break;
                }

                // 没有环的话，就用退避算法，避免重复 rpc
                backoff_ms = std::min(backoff_ms * 2, kMaxCheckBackoffMs);
                next_check = now + std::chrono::milliseconds(backoff_ms);
                continue;
            }

            // DEBUG
            const auto wake_at = next_check;
            did_suspend = true;
            const auto t_sus0 = std::chrono::steady_clock::now();

            assert(wait_state->sched != nullptr);

            // 通过协程把这个事务给挂起
            // 在目前这个框架里，协程挂起了之后，协程调度器其实也没有任务可以做了
            // 后续可以添加一下，协程挂起之后，继续去处理别的事务，但是这样对比又显得不太公平
            // 并且有可能会持续地、不断地产生新的事务，因此这里先不管了
            auto sleep_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                    wake_at - std::chrono::steady_clock::now()).count();
            assert(sleep_us >= 0);
            wait_state->sched->YieldWithTimeOnThread((uint64_t)sleep_us,
                                                        wait_state->thread_id);

            suspend_ns += NsBetween(t_sus0, std::chrono::steady_clock::now());
        }
        
        // 摘除本地等待状态。返回 true = 期间有人把这把锁过户给了我（flag 已置位）。
        const bool adopted = UnregisterLocalWaiter(waiter_ts);
        if (adopted && ret != LockWaitResult::GRANTED_RETRY) {
            // 我本来要回滚（victim/超时），但已经有人把锁写给了我：必须改为接受这把锁。
            // 否则这把锁就留给了一个退出等待的事务 -> 元组永久锁死。
            LOG(WARNING) << "[LW] abort overridden by adoption: waiter=" << waiter_ts
                         << " holder=" << holder_ts << " key=" << LwKeyStr(wait_key)
                         << " old_result=" << (int)ret;
            ret = LockWaitResult::GRANTED_RETRY;
        }
// [DEBUG 日志已关] LwTrace("wait_exit", "waiter=", waiter_ts, "holder=", holder_ts,
//                "key=", LwKeyStr(wait_key), "result=", (int)ret, "suspended=", did_suspend,
//                "adopted=", adopted,
//                "held_ns=", NsBetween(t_enter, std::chrono::steady_clock::now()),
//                "suspend_ns=", suspend_ns, "detect_ns=", detect_ns);
        switch (ret) {
            case LockWaitResult::GRANTED_RETRY:
                stat_wait_woken.fetch_add(1, std::memory_order_relaxed);
                break;
            case LockWaitResult::VICTIM_ABORT:
                stat_wait_victim.fetch_add(1, std::memory_order_relaxed);
                // 回滚离开等待时不会把自己从 holder 的 FIFO 里摘掉；但过户是"先问再接"的，
                // holder 节点会来问"这个人还在等吗"，我这边已经查不到了 -> 拒收并换下一个。
                LOG(WARNING) << "[LW] victim leaves wait while still queued at holder: waiter="
                             << waiter_ts << " holder=" << holder_ts
                             << " holder_node=" << holder_node << " key=" << LwKeyStr(wait_key);
                break;
            case LockWaitResult::TIMEOUT_ABORT:
                stat_wait_timeout.fetch_add(1, std::memory_order_relaxed);
                break;
        }
        RecordEpisode(NsBetween(t_enter, std::chrono::steady_clock::now()),
                      reg_ns, suspend_ns, detect_ns, did_suspend, holder_is_local);
        return ret;
    }

private:
    // 去收集一下各个节点的锁等待关系，看下是否有构成环
    bool DetectDeadlockByChainWalk(uint64_t my_ts, node_id_t self_node,
                                   uint64_t holder_ts, node_id_t holder_node,
                                   brpc::Channel* nodes_channel) {
        // 遍历的集合
        // 判断的逻辑是，走的过程中一旦碰到了自己，那就说明构成环了
        std::unordered_set<uint64_t> visited;
        visited.insert(my_ts);
        uint64_t cur_ts = holder_ts;
        node_id_t cur_node = holder_node;

        // 最多走 kMaxChainHops，防止无限查找
        for (int hop = 0; hop < kMaxChainHops; ++hop) {
            // 目前我们是 TP 负载，应该不会出现锁等待链这么长的，打个断言 DEBUG
            assert(hop != kMaxChainHops - 1);
            if (!visited.insert(cur_ts).second) {
                // std::cout << "DEAD LOCK OCCUR , ts = " << my_ts << "\n";
                LOG(WARNING) << "[DEADLOCK] cycle detected by chain walk, victim waiter_ts="
                             << my_ts << " hop=" << hop;
                return true;
            }
            uint64_t next_ts = 0;
            node_id_t next_node = -1;
            // 这里的逻辑是去判断下，当前的持有者是否也在等别的事务提交
            // 如果当前持有者没在等的话，那就直接返回 false，否则 next_ts 和 next_node 就代表了当前持有者在等谁
            // 通过不断循环，就能判断出来是否存在链了
            if (!QueryHolderOnNode(cur_ts, cur_node, self_node, nodes_channel,
                                   &next_ts, &next_node)) {
                return false;   
            }
            if (next_ts == my_ts) {
                // std::cout << "[DEADLOCK] cycle closed back to waiter, victim waiter_ts="
                //              << my_ts << " hop=" << hop << "\n";
                LOG(WARNING) << "[DEADLOCK] cycle closed back to waiter, victim waiter_ts="
                             << my_ts << " hop=" << hop;
                return true;
            }
            cur_ts = next_ts;
            cur_node = next_node;
        }
        return false;
    }

    // 单词
    bool QueryHolderOnNode(uint64_t cur_ts, node_id_t cur_node, node_id_t self_node,
                           brpc::Channel* nodes_channel,
                           uint64_t* next_ts, node_id_t* next_node) {
        if (cur_node == self_node) {
            return QueryWaiterHolder(cur_ts, next_ts, next_node);
        }
        if (cur_node < 0 || cur_node >= ComputeNodeCount) return false;
        compute_node_service::QueryWaiterHolderRequest req;
        compute_node_service::QueryWaiterHolderResponse resp;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kChainHopTimeoutMs);
        req.set_waiter_ts(cur_ts);
        compute_node_service::ComputeNodeService_Stub stub(&nodes_channel[cur_node]);
        stub.QueryWaiterHolder(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "[DEADLOCK] QueryWaiterHolder rpc failed: " << cntl.ErrorText()
                       << ", node=" << cur_node << ", ts=" << cur_ts;
            return false;   // 问不到 -> 按链断处理，下个检查点再查
        }
        if (!resp.waiting()) return false;
        *next_ts = resp.holder_ts();
        *next_node = (node_id_t)resp.holder_node();
        return true;
    }

    // 检查触发后的退避上限：合法长等待不应反复触发链式走查
    static constexpr int kMaxCheckBackoffMs = 1000;
    static constexpr int kRpcTimeoutMs = 5000;
    static constexpr int kMaxChainHops = 128;
    // 单跳询问的超时
    static constexpr int kChainHopTimeoutMs = 200;

public:
    // ---- 次数 ----
    std::atomic<uint64_t> stat_wait_total{0};          // 进入 WaitForLock 的总次数
    std::atomic<uint64_t> stat_wait_direct_granted{0}; // 登记时返回 granted=true（0 挂起直接重试）
    // 上面这个 granted 有两个来源，分开计数：
    //   forwarded_keys_ 命中 = 登记里写的 holder【已经放掉过这把锁】（过户在途 / 登记晚于解锁）
    //   其余                 = holder 确实已经收尾了（active_txs_ 查不到）
    std::atomic<uint64_t> stat_wait_forwarded{0};      // "holder 已放掉这把锁"挡回重读的次数
    std::atomic<uint64_t> stat_wait_reg_fail{0};       // 登记 RPC 失败
    std::atomic<uint64_t> stat_wait_woken{0};          // 挂起/等待后醒来获准重试
    std::atomic<uint64_t> stat_wait_timeout{0};        // 看门狗兜底（调试期恒为 0）
    std::atomic<uint64_t> stat_wait_victim{0};         // 判环选中 victim 回滚
    std::atomic<uint64_t> stat_wait_suspended{0};      // 真正挂起过至少一次的次数
    std::atomic<uint64_t> stat_deadlock_check{0};      // 链式走查触发次数

    // ---- 时间分解（纳秒累计；输出秒时 /1e9）----
    // 三个互斥桶，可直接堆叠：
    std::atomic<uint64_t> stat_wait_reg_time_ns{0};     // ① 登记阶段（构造+本地注册+远端 RPC）
    std::atomic<uint64_t> stat_wait_suspend_time_ns{0}; // ② 纯挂起（不含检测）
    std::atomic<uint64_t> stat_wait_detect_time_ns{0};  // ③ 死锁链式走查
    // 明细（子集 / 分组，不可与上面重复堆叠）：
    std::atomic<uint64_t> stat_wait_time_ns{0};           // episode 总墙钟（≈ ①+②+③+收尾）
    std::atomic<uint64_t> stat_wait_reg_rpc_time_ns{0};   // ① 中远程 RegisterTupleWait RPC 的部分
    std::atomic<uint64_t> stat_wait_suspend_local_ns{0};  // ② 按 holder 位置拆分：本地 holder
    std::atomic<uint64_t> stat_wait_suspend_remote_ns{0}; // ② 按 holder 位置拆分：远程 holder

    // ---- 单次等待时长（只统计真正挂起过的 episode，单位微秒）----
    std::atomic<uint64_t> stat_wait_us_max{0};         // 单次最长"锁冲突让事务多花了多久"

    // ---- 锁过户观测 ----
    std::atomic<uint64_t> stat_handoff{0};   // 过户次数（把锁直接交给队首，无抢锁）
    std::atomic<uint64_t> stat_wq_depth_max{0}; // 观测到的元组等待队列最大长度（惊群规模）

    // 分位数（p ∈ (0,1]）：基于「每倍频 2^kBucketBits 格」的 log2 直方图，单位微秒。
    // 返回桶上界（相对真实值高估 < 9%），并钳到实测最大值，保证单调且不超过 max。
    uint64_t WaitUsPercentile(double p) const {
        uint64_t counts[kHistBuckets];
        uint64_t total = 0;
        for (int i = 0; i < kHistBuckets; ++i) {
            counts[i] = wait_us_hist_[i].load(std::memory_order_relaxed);
            total += counts[i];
        }
        if (total == 0) return 0;
        uint64_t target = (uint64_t)std::ceil(p * (double)total);
        if (target == 0) target = 1;
        if (target > total) target = total;
        uint64_t cum = 0;
        const uint64_t mx = stat_wait_us_max.load(std::memory_order_relaxed);
        for (int i = 0; i < kHistBuckets; ++i) {
            cum += counts[i];
            if (cum >= target) {
                const double upper_us =
                    std::pow(2.0, (i + 1) / (double)(1 << kBucketBits));
                const uint64_t v = (uint64_t)std::ceil(upper_us);
                return (mx > 0 && v > mx) ? mx : v;
            }
        }
        return mx;
    }

    // 当前在飞的唤醒 RPC 数（合并后为"目的节点"级别，而非 waiter 级）。
    // 异步发出后可能瞬时非 0；可据此观测背压、判断唤醒是否已全部送达。

private:
    // DEBUG
    static constexpr int kBucketBits = 3;                        // 每倍频 8 格（~9% 分辨率）
    static constexpr int kOctaves = 24;                          // 1us * 2^24 ≈ 16.7s
    static constexpr int kHistBuckets = (kOctaves + 1) << kBucketBits;
    std::atomic<uint64_t> wait_us_hist_[kHistBuckets]{};

    static uint64_t NsBetween(const std::chrono::steady_clock::time_point& a,
                              const std::chrono::steady_clock::time_point& b) {
        return (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count();
    }

    static int HistBucketOfNs(uint64_t ns) {
        uint64_t us = (ns + 999) / 1000;   // 向上取整到微秒
        if (us == 0) us = 1;
        int e = 0;
        while (e + 1 < 63 && (us >> (e + 1)) != 0) ++e;   // e = floor(log2(us))
        if (e < kBucketBits) {                            // us ∈ [1, 2^kBucketBits)
            const int idx = (int)us - 1;
            return idx < kHistBuckets ? idx : kHistBuckets - 1;
        }
        const int mant = (int)((us >> (e - kBucketBits)) & ((1u << kBucketBits) - 1));
        const int idx = (e << kBucketBits) + mant;
        return idx < kHistBuckets ? idx : kHistBuckets - 1;
    }

    // 统一出口记账：一次 WaitForLock episode 的最终耗时分解。
    void RecordEpisode(uint64_t total_ns, uint64_t reg_ns, uint64_t suspend_ns,
                       uint64_t detect_ns, bool suspended, bool holder_is_local) {
        stat_wait_time_ns.fetch_add(total_ns, std::memory_order_relaxed);
        stat_wait_reg_time_ns.fetch_add(reg_ns, std::memory_order_relaxed);
        stat_wait_suspend_time_ns.fetch_add(suspend_ns, std::memory_order_relaxed);
        stat_wait_detect_time_ns.fetch_add(detect_ns, std::memory_order_relaxed);
        if (holder_is_local) {
            stat_wait_suspend_local_ns.fetch_add(suspend_ns, std::memory_order_relaxed);
        } else {
            stat_wait_suspend_remote_ns.fetch_add(suspend_ns, std::memory_order_relaxed);
        }
        if (!suspended) return;   // 分位数只看真正挂起过的 episode
        stat_wait_suspended.fetch_add(1, std::memory_order_relaxed);
        wait_us_hist_[HistBucketOfNs(total_ns)].fetch_add(1, std::memory_order_relaxed);
        uint64_t us = (total_ns + 999) / 1000;
        if (us == 0) us = 1;
        uint64_t prev = stat_wait_us_max.load(std::memory_order_relaxed);
        while (us > prev &&
               !stat_wait_us_max.compare_exchange_weak(prev, us, std::memory_order_relaxed)) {
        }
    }

    std::mutex mtx_;
    // 本节点活跃事务（TxBegin 注册，FinishTx 注销）。注册时用它判断 holder 是否还活着。
    std::unordered_set<uint64_t> active_txs_;

    // ---- 锁过户 + 队列接力 ----
    // 本节点作为 owner 的「元组 -> FIFO 等待队列」。队伍 owner = 当前 holder；
    // 过户时整个队列随锁接力到新 holder 的节点，所以队列不会被遗弃在已结束的 holder 上。
    struct TupleWaitQueue {
        std::deque<std::pair<uint64_t, node_id_t>> waiters;   // (waiter_ts, waiter_node)
        uint64_t owner_holder_ts = 0;                          // 当前托管本队列的 holder
        uint64_t version = 0;                                  // 接力版本号（去重/防过期）
    };
    std::unordered_map<TupleWaitKey, TupleWaitQueue, TupleWaitKeyHash> local_queues_;
    // holder_ts -> 本节点上归它托管（尚有等待者）的元组集合，收尾时按此定位队列
    std::unordered_map<uint64_t, std::unordered_set<TupleWaitKey, TupleWaitKeyHash>> holder_keys_;
    // 已经被我放掉的元组（过户走了 / 解锁时没人等）：holder_ts -> 它放掉的 key 集合。
    // 权威判据是"登记里写的 holder 自己有没有放掉过这条 key" —— 按 holder 记账，所以
    // 后来者放同一把锁不会覆盖它，也不会被 TryAcceptHandoff 误删（旧版 key -> 单值墓碑
    // 的两个洞都会漏掉"更早放锁、事务还活着"的旧 holder，从而建出孤儿队列）。
    // 生命周期 = holder 的活跃期；FinishTx 整条删掉。
    std::unordered_map<uint64_t, std::unordered_set<TupleWaitKey, TupleWaitKeyHash>> forwarded_keys_;

    // waiter 视角（本节点挂起的事务）：waiter_ts -> 唤醒标志（0 等待, 1 granted）
    std::unordered_map<uint64_t, std::shared_ptr<WaitState>> local_waiters_;
};
