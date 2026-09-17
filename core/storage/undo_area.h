#pragma once

// ============================================================================
// 独立 undo 区（设计文档 docs/UNDO_DESIGN.md）
//
// 借鉴 oGRAC undo segment 设计（pkg/src/kernel/xact/knl_undo.c），
// 适配本系统 "redo-all + undo-uncommitted" 模型：
//
//   undo/
//     txn_table.log            事务状态变迁日志（COMMITTED/UNDONE，append-only）
//     useg_<seg_id:016x>.log   undo 数据段，定长滚动，append-only
//
// 核心思想：
//   1. undo 信息脱离 WAL 独立存放——WAL 段回收/checkpoint 不受活跃事务牵制；
//   2. undo 记录按事务串链（prev_addr 指向同事务前一条）——undo 免全扫 WAL；
//   3. 事务表精确记录提交状态——替代"全扫 BATCHEND"的提交判定；
//   4. 提交即回收——undo 区只保留未提交事务的 undo，空间有界。
//
// 生成时机：replay 线程 apply 日志前 TrackLog（undo 与数据页同等 durability，
// process 崩溃模型下 write 即可）；BATCHEND 重放时 CommitTxn 回收链空间。
// 崩溃重建：扫描 undo 段重建事务链（记录自描述），重放 txn_table.log 剔除
// 已提交/已回滚事务——"索引即缓存"，全部结构可从盘内容重建。
// ============================================================================

#include <cstdint>
#include <cstring>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "log_record.h"

namespace undofmt {

constexpr const char* UNDO_DIR = "undo";
constexpr const char* TXN_TABLE_FILE = "undo/txn_table.log";
constexpr uint64_t UNDO_SEG_SIZE = 16ull * 1024 * 1024;  // undo 段 16MB（WAL 段的 1/4）
constexpr uint32_t UNDO_REC_MAGIC = 0x554E4431;          // "UND1"

// undo 数据记录磁盘布局：
//   [magic(4) | rec_len(4) | crc32(4) | prev_addr(8) | wal_rec(rec_len-20)]
//   crc32c 覆盖 [prev_addr .. 尾]；prev_addr = 同事务前一条 undo 记录地址（0=链头）；
//   wal_rec = 完整内嵌 WAL 记录（44B 头 + payload），undo 应用时直接 deserialize 复用
constexpr uint32_t UNDO_REC_HEADER_SIZE = 20;

// txn_table.log 记录布局（21B）：
//   [rec_len(4) | crc32(4) | node_id(4) | tid(8) | status(1)]
//   crc32c 覆盖 [node_id .. 尾]
constexpr uint32_t TXN_REC_SIZE = 21;
constexpr uint8_t TXN_STATUS_COMMITTED = 1;
constexpr uint8_t TXN_STATUS_UNDONE = 2;  // 崩溃恢复 undo 完成（语义等同 oGRAC 的 ABORTED）

// undo 地址：seg_id * UNDO_SEG_SIZE + 段内偏移（与 logfmt::LogAddr 同构）
inline uint64_t MakeUndoAddr(uint64_t seg_id, uint64_t off) { return seg_id * UNDO_SEG_SIZE + off; }
inline uint64_t UndoAddrSegId(uint64_t addr) { return addr / UNDO_SEG_SIZE; }
inline uint64_t UndoAddrSegOff(uint64_t addr) { return addr % UNDO_SEG_SIZE; }

inline std::string UndoSegFileName(uint64_t seg_id) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%s/useg_%016lx.log", UNDO_DIR, (unsigned long)seg_id);
    return std::string(buf);
}
inline std::string UndoSegFileName(const std::string& dir, uint64_t seg_id) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%s/useg_%016lx.log", dir.c_str(), (unsigned long)seg_id);
    return std::string(buf);
}

}  // namespace undofmt

// 事务键：(node_id, tid)——多主架构下 tid 仅节点内唯一
struct TxnKey {
    node_id_t node_id;
    tx_id_t tid;
    bool operator==(const TxnKey& o) const { return node_id == o.node_id && tid == o.tid; }
};
struct TxnKeyHash {
    size_t operator()(const TxnKey& k) const {
        return std::hash<uint64_t>()(
            (static_cast<uint64_t>(static_cast<uint32_t>(k.node_id)) << 32) ^ k.tid);
    }
};

// 活跃事务的 undo 链（内存事务表条目；仅未提交事务驻留）
struct TxnUndoChain {
    node_id_t node_id = INVALID_NODE_ID;
    tx_id_t tid = INVALID_TXN_ID;
    uint64_t last_addr = 0;                   // 链尾（最新一条；undo 从此反向回溯）
    uint32_t count = 0;                       // 链内记录数（含重复重放的幂等副本）
    std::unordered_set<uint64_t> seg_ids;     // 占用的 undo 段集合（段回收引用计数用）
};

class UndoArea {
public:
    // dir：undo 区目录（默认 undofmt::UNDO_DIR，相对数据目录；测试可传临时目录）
    // seg_size：段大小（默认 16MB，测试可调小以触发滚动）
    explicit UndoArea(std::string dir = undofmt::UNDO_DIR,
                      uint64_t seg_size = undofmt::UNDO_SEG_SIZE);
    ~UndoArea();

    bool IsEnabled() const { return enabled_; }

    // replay 线程：apply 数据日志前调用。内部只处理数据类型
    // （UPDATE/INSERT/DELETE/BLINKINSERT/BLINKDELETE/FSMUPDATE），其余直接返回。
    // 重复 TrackLog 同一 WAL 记录安全（undo 应用幂等）。
    void TrackLog(const LogRecord* rec);

    // replay 线程：BATCHEND 重放时调用。持久化 COMMITTED 状态并回收链空间。
    // 无 undo 记录的事务（只读/控制）为 no-op。
    void CommitTxn(node_id_t node_id, tx_id_t tid);
    void AbortTxn(node_id_t node_id, tx_id_t tid);

    // 崩溃恢复：对所有未提交事务按全局地址降序（= 日志流降序）执行 undo。
    // apply_cb 收到内嵌的完整 WAL 记录字节流，由调用方（LogReplay）应用。
    // 返回 undo 的记录条数；-1 表示 undo 区不可用（调用方应回退 WAL 全扫路径）。
    // P0 修复：alive_node_ids 中的存活节点事务跳过 undo——其终局由节点
    // 自身负责（BATCHEND/ABORTEND 随后到达）；链保留，若该节点随后故障，
    // 下一轮 undo（alive 列表不再含它）仍可沿链撤销。
    int UndoAllActiveTxns(node_id_t failed_node_id,
                          const std::function<void(const char* wal_rec, uint32_t len)>& apply_cb,
                          const std::set<node_id_t>& alive_node_ids = {});

    // ---- 测试/观测接口 ----
    size_t ActiveTxnCount();
    bool HasChain(node_id_t node_id, tx_id_t tid);
    uint64_t ChainLastAddr(node_id_t node_id, tx_id_t tid);
    uint64_t CurSegId();
    uint64_t TxnTableSize();   // txn_table.log 当前字节数

private:
    void Init();                                        // 打开/创建 undo 区 + 崩溃重建
    std::vector<uint64_t> ListSegments();               // 列出 undo 段号（升序）
    uint64_t ScanSegment(uint64_t seg_id);              // 扫一段重建链，返回有效数据尾偏移
    void ReplayTxnTable();                              // 重放事务状态，剔除已提交/已回滚
    void RebuildSegRefsAndCleanup(const std::vector<uint64_t>& segs);

    // 以下均需持有 mtx_
    uint64_t AppendRecordLocked(const char* wal_rec, uint32_t wal_len, uint64_t prev_addr);
    void AppendTxnStatusLocked(node_id_t node_id, tx_id_t tid, uint8_t status);
    void ReleaseChainLocked(const TxnUndoChain& chain); // 段引用计数--，归零删段
    void DeleteSegLocked(uint64_t seg_id);
    void OpenCurSegLocked();
    void MaybeCompactTxnTableLocked();                  // 无活跃事务时截断 txn_table
    int GetReadFdLocked(uint64_t seg_id);
    bool ReadPrevAddrLocked(uint64_t addr, uint64_t* prev_addr);
    bool ReadWalRecordLocked(uint64_t addr, std::vector<char>& out, uint32_t* wal_len);

    std::string dir_;
    uint64_t seg_size_;
    bool enabled_ = false;

    std::mutex mtx_;
    std::unordered_map<TxnKey, TxnUndoChain, TxnKeyHash> chains_;  // 活跃事务表
    std::unordered_map<uint64_t, uint32_t> seg_refs_;              // 段 → 引用它的事务数

    uint64_t cur_seg_id_ = 0;
    uint64_t cur_seg_off_ = 0;
    int cur_seg_fd_ = -1;
    int txn_table_fd_ = -1;
    std::unordered_map<uint64_t, int> read_fds_;  // 读侧 fd 缓存（undo/重建用）
};
