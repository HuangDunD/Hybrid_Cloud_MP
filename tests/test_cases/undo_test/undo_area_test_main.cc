// ============================================================================
// UndoArea 单元测试（独立 undo 区，docs/UNDO_DESIGN.md）
//
// 覆盖：
//   Test 1  TrackLog 建链 + 事务表观测
//   Test 2  CommitTxn 回收（链移除 + 零引用段删除 + txn_table 压缩）
//   Test 3  UndoAllActiveTxns：多事务交错 → 全局降序 undo、内容完整、
//           undo 后链清空段回收
//   Test 4  崩溃重建：未提交事务链从 undo 段精确恢复
//   Test 5  崩溃重建：已提交事务经 txn_table.log 重放剔除
//   Test 6  CRC 损坏注入：损坏点截断，之前记录完好
//   Test 7  段滚动与部分回收（小 seg_size 强制滚动）
//   Test 8  undo 区地址单调性 = 日志流序（跨段也成立）
// ============================================================================

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "core/storage/undo_area.h"

namespace {

int g_pass = 0;
int g_fail = 0;

#define CHECK(cond, msg) do { \
    if (cond) { g_pass++; printf("  [PASS] %s\n", msg); } \
    else { g_fail++; printf("  [FAIL] %s (line %d)\n", msg, __LINE__); } \
} while (0)

char kTestDir[64] = {};

void CleanTestDir() {
    char pattern[] = "undo-test-XXXXXX";
    if (mkdtemp(pattern) == nullptr) {
        perror("mkdtemp undo test");
        abort();
    }
    snprintf(kTestDir, sizeof(kTestDir), "%s", pattern);
}

std::string SegPath(uint64_t seg_id) {
    return undofmt::UndoSegFileName(kTestDir, seg_id);
}

bool SegExists(uint64_t seg_id) {
    return ::access(SegPath(seg_id).c_str(), F_OK) == 0;
}

// ---------------- 测试用日志记录构造 ----------------

RmRecord MakeValue(uint64_t key, const char* data) {
    return RmRecord(key, strlen(data) + 1, const_cast<char*>(data));
}

UpdateLogRecord* MakeUpdate(node_id_t node, tx_id_t tid, uint64_t key,
                            const char* new_data, const char* old_data,
                            int page_no = 1, int slot_no = 0) {
    RmRecord new_val = MakeValue(key, new_data);
    RmRecord old_val = MakeValue(key, old_data);
    Rid rid{page_no, slot_no};
    return new UpdateLogRecord(1, node, tid, new_val, rid, "test_tbl", &old_val);
}

InsertLogRecord* MakeInsert(node_id_t node, tx_id_t tid, uint64_t key,
                            const char* data, int page_no = 1, int slot_no = 0) {
    RmRecord val = MakeValue(key, data);
    return new InsertLogRecord(1, node, tid, val, page_no, slot_no, "test_tbl");
}

DeleteLogRecord* MakeDelete(node_id_t node, tx_id_t tid, int page_no = 1, int slot_no = 0) {
    auto* rec = new DeleteLogRecord(1, node, tid, /*table_id=*/100, "test_tbl", page_no, slot_no);
    RmPageHdr hdr{}; hdr.num_records_ = 5; hdr.LLSN_ = 42;
    rec->set_meta(/*bucket_offset=*/0, /*bucket_value=*/1, hdr, /*first_free=*/-1,
                  /*undo_bucket=*/1, hdr, /*undo_first_free=*/-1);
    return rec;
}

BatchEndLogRecord* MakeBatchEnd(node_id_t node, tx_id_t tid) {
    auto* rec = new BatchEndLogRecord();
    rec->log_node_id_ = node;
    rec->log_tid_ = tid;
    return rec;
}

// 从 undo 回调收到的 WAL 字节流还原记录类型与事务 id
struct AppliedRec {
    LogType type;
    node_id_t node_id;
    tx_id_t tid;
    uint64_t key;      // UPDATE/INSERT 的 key
    std::string value; // UPDATE 的 old_value / INSERT 的 value
};

AppliedRec DecodeWal(const char* wal, uint32_t len) {
    AppliedRec out{};
    out.type = *reinterpret_cast<const LogType*>(wal + OFFSET_LOG_TYPE);
    memcpy(&out.tid, wal + OFFSET_LOG_TID, sizeof(tx_id_t));
    memcpy(&out.node_id, wal + OFFSET_LOG_NODE_ID, sizeof(node_id_t));
    if (out.type == LogType::UPDATE) {
        UpdateLogRecord rec;
        rec.deserialize(wal);
        out.key = rec.old_value().key_;
        out.value.assign(rec.old_value().value_, rec.old_value().value_size_);
    } else if (out.type == LogType::INSERT) {
        InsertLogRecord rec;
        rec.deserialize(wal);
        out.key = rec.insert_value_.key_;
        out.value.assign(rec.insert_value_.value_, rec.insert_value_.value_size_);
    }
    return out;
}

// ---------------- Test 1：TrackLog 建链 ----------------
void Test1_TrackLog() {
    printf("Test 1: TrackLog builds per-txn chains\n");
    CleanTestDir();
    {
        UndoArea area(kTestDir);
        CHECK(area.IsEnabled(), "undo area enabled");
        CHECK(area.ActiveTxnCount() == 0, "no active txn initially");

        auto* u1 = MakeUpdate(1, 100, 1, "new1", "old1");
        auto* u2 = MakeUpdate(1, 100, 2, "new2", "old2");
        auto* i1 = MakeInsert(2, 200, 3, "ins3");
        area.TrackLog(u1);
        area.TrackLog(u2);
        area.TrackLog(i1);

        CHECK(area.ActiveTxnCount() == 2, "two active txns");
        CHECK(area.HasChain(1, 100), "txn (1,100) tracked");
        CHECK(area.HasChain(2, 200), "txn (2,200) tracked");
        uint64_t a1 = area.ChainLastAddr(1, 100);
        uint64_t a2 = area.ChainLastAddr(2, 200);
        CHECK(a1 != 0 && a2 != 0, "chain tails recorded");
        // 写入序 u1, u2, i1：i1 地址最大（单写者 ⇒ 地址单调 = 日志流序）
        CHECK(a2 > a1, "later track has larger addr (global order)");

        // 控制记录不产生 undo
        auto* b = MakeBatchEnd(1, 999);
        area.TrackLog(b);
        CHECK(!area.HasChain(1, 999), "BATCHEND not tracked as data");

        delete u1; delete u2; delete i1; delete b;
    }
}

// ---------------- Test 2：CommitTxn 回收 ----------------
void Test2_CommitTxn() {
    printf("Test 2: CommitTxn reclaims chain and segment\n");
    CleanTestDir();
    {
        UndoArea area(kTestDir);
        auto* u1 = MakeUpdate(1, 100, 1, "new1", "old1");
        auto* u2 = MakeUpdate(1, 101, 2, "new2", "old2");
        area.TrackLog(u1);
        area.TrackLog(u2);
        CHECK(SegExists(1), "seg 1 exists after track");

        area.CommitTxn(1, 100);
        CHECK(!area.HasChain(1, 100), "committed txn chain removed");
        CHECK(area.HasChain(1, 101), "other txn still active");
        CHECK(SegExists(1), "seg 1 kept (still referenced by txn 101)");

        area.CommitTxn(1, 101);
        CHECK(area.ActiveTxnCount() == 0, "no active txn after all commit");
        CHECK(!SegExists(1), "zero-ref current segment reclaimed (rolled to seg 2)");
        CHECK(area.CurSegId() == 2, "writer rolled to next segment");
        CHECK(area.TxnTableSize() == 0, "txn_table compacted when no active txn");
        delete u1; delete u2;
    }
}

// ---------------- Test 3：UndoAllActiveTxns ----------------
void Test3_UndoAll() {
    printf("Test 3: UndoAllActiveTxns applies in global descending order\n");
    CleanTestDir();
    std::vector<AppliedRec> applied;
    {
        UndoArea area(kTestDir);
        // 交错写入：T100.u1, T200.i1, T100.u2, T200.d1, T100.i2
        auto* u1 = MakeUpdate(1, 100, 1, "n1", "o1");
        auto* i1 = MakeInsert(2, 200, 2, "i2val");
        auto* u2 = MakeUpdate(1, 100, 3, "n3", "o3");
        auto* d1 = MakeDelete(2, 200);
        auto* i2 = MakeInsert(1, 100, 4, "i4val");
        for (auto* r : {static_cast<LogRecord*>(u1), static_cast<LogRecord*>(i1),
                        static_cast<LogRecord*>(u2), static_cast<LogRecord*>(d1),
                        static_cast<LogRecord*>(i2)}) {
            area.TrackLog(r);
        }

        int undone = area.UndoAllActiveTxns(1,
            [&](const char* wal, uint32_t len) { applied.push_back(DecodeWal(wal, len)); return 1; });
        CHECK(undone == 5, "all 5 records undone");
        CHECK(area.ActiveTxnCount() == 0, "chains cleared after undo");
        CHECK(area.TxnTableSize() == 0, "txn_table compacted when empty");

        delete u1; delete i1; delete u2; delete d1; delete i2;
    }
    // 全局降序：写入逆序 i2, d1, u2, i1, u1
    CHECK(applied.size() == 5, "callback received 5 records");
    if (applied.size() == 5) {
        CHECK(applied[0].type == LogType::INSERT && applied[0].tid == 100, "1st undone = last written (i2)");
        CHECK(applied[1].type == LogType::DELETE && applied[1].tid == 200, "2nd = d1");
        CHECK(applied[2].type == LogType::UPDATE && applied[2].tid == 100 &&
              applied[2].value == std::string("o3") + '\0', "3rd = u2 with old value o3");
        CHECK(applied[3].type == LogType::INSERT && applied[3].tid == 200, "4th = i1");
        CHECK(applied[4].type == LogType::UPDATE && applied[4].tid == 100 &&
              applied[4].value == std::string("o1") + '\0', "5th = u1 with old value o1");
    }
    CHECK(!SegExists(1), "segment reclaimed after undo");
}

// ---------------- Test 4：崩溃重建保留未提交事务 ----------------
void Test4_RebuildActive() {
    printf("Test 4: crash rebuild preserves uncommitted chains\n");
    CleanTestDir();
    uint64_t tail_before = 0;
    {
        UndoArea area(kTestDir);
        auto* u1 = MakeUpdate(1, 100, 1, "n1", "o1");
        auto* i1 = MakeInsert(2, 200, 2, "iv");
        area.TrackLog(u1);
        area.TrackLog(i1);
        tail_before = area.ChainLastAddr(2, 200);
        delete u1; delete i1;
    }  // 析构 ≈ 进程退出
    {
        UndoArea area(kTestDir);  // 重建
        CHECK(area.IsEnabled(), "re-enabled after rebuild");
        CHECK(area.ActiveTxnCount() == 2, "both active txns rebuilt");
        CHECK(area.ChainLastAddr(2, 200) == tail_before, "chain tail addr stable across restart");

        std::vector<AppliedRec> applied;
        int undone = area.UndoAllActiveTxns(1,
            [&](const char* wal, uint32_t len) { applied.push_back(DecodeWal(wal, len)); return 1; });
        CHECK(undone == 2, "rebuilt chains fully undone");
        CHECK(applied.size() == 2 && applied[0].type == LogType::INSERT &&
              applied[1].type == LogType::UPDATE, "undo order preserved after rebuild");
    }
}

// ---------------- Test 5：崩溃重建剔除已提交事务 ----------------
void Test5_RebuildCommitted() {
    printf("Test 5: crash rebuild filters committed txns via txn_table\n");
    CleanTestDir();
    {
        UndoArea area(kTestDir);
        // T100 与 T101 同段；提交 T100（段有 T101 引用不能删）→ 模拟崩溃
        auto* u1 = MakeUpdate(1, 100, 1, "n1", "o1");
        auto* u2 = MakeUpdate(1, 101, 2, "n2", "o2");
        area.TrackLog(u1);
        area.TrackLog(u2);
        area.CommitTxn(1, 100);
        CHECK(area.ActiveTxnCount() == 1, "only T101 active");
        CHECK(SegExists(1), "seg 1 kept (referenced by T101)");
        delete u1; delete u2;
    }
    {
        UndoArea area(kTestDir);
        CHECK(area.ActiveTxnCount() == 1, "committed txn filtered after rebuild");
        CHECK(area.HasChain(1, 101), "T101 chain intact");
        CHECK(!area.HasChain(1, 100), "T100 not resurrected");
    }
}

// ---------------- Test 6：CRC 损坏截断 ----------------
void Test6_CrcCorruption() {
    printf("Test 6: corrupted record truncates segment scan\n");
    CleanTestDir();
    {
        UndoArea area(kTestDir);
        for (int i = 0; i < 3; i++) {
            auto* u = MakeUpdate(1, 100, i, "n", "o");
            area.TrackLog(u);
            delete u;
        }
    }
    // 破坏第 2 条记录区域的一个字节（在第 1 条之后）
    {
        // 读第 1 条长度以定位第 2 条起点
        int fd = ::open(SegPath(1).c_str(), O_RDONLY);
        char hdr[undofmt::UNDO_REC_HEADER_SIZE];
        pread(fd, hdr, sizeof(hdr), 0);
        uint32_t rec1_len;
        memcpy(&rec1_len, hdr + 4, 4);
        ::close(fd);
        fd = ::open(SegPath(1).c_str(), O_RDWR);
        char junk = 0x7F;
        // 翻转第 2 条记录 payload 区一字节（crc 覆盖区）
        pwrite(fd, &junk, 1, rec1_len + undofmt::UNDO_REC_HEADER_SIZE + 10);
        ::close(fd);
    }
    {
        UndoArea area(kTestDir);
        CHECK(area.ActiveTxnCount() == 1, "txn chain still rebuilt (truncated)");
        std::vector<AppliedRec> applied;
        // 重建时损坏尾部已被截断出链，链内（损坏前）记录仍可正常撤销
        int undone = area.UndoAllActiveTxns(1,
            [&](const char* wal, uint32_t len) { applied.push_back(DecodeWal(wal, len)); return 1; });
        CHECK(undone == 1, "only records before corruption survive");
    }
}

// ---------------- Test 6b：undo 应用硬失败不虚报成功 ----------------
void Test6b_UndoHardFailure() {
    printf("Test 6b: non-reversible record fails the whole undo (fail-closed)\n");
    CleanTestDir();
    {
        UndoArea area(kTestDir);
        auto* u1 = MakeUpdate(1, 100, 1, "n1", "o1");
        auto* i1 = MakeInsert(2, 200, 2, "iv");
        area.TrackLog(u1);
        area.TrackLog(i1);
        delete u1; delete i1;

        std::vector<AppliedRec> applied;
        // 回调返回 -1 = 该记录不可撤销（缺前镜像/文件不可用）
        int undone = area.UndoAllActiveTxns(1,
            [&](const char* wal, uint32_t len) { applied.push_back(DecodeWal(wal, len)); return -1; });
        CHECK(undone == -1, "hard failure fails the whole undo");
        CHECK(area.ActiveTxnCount() == 2, "no txn finalized after hard failure");
        CHECK(area.TxnTableSize() == 0, "no UNDONE status written after hard failure");
        CHECK(area.HasChain(1, 100) && area.HasChain(2, 200), "chains kept intact after hard failure");

        // 失败后链保持完整，可由下次恢复重来（undo 幂等）
        undone = area.UndoAllActiveTxns(1,
            [&](const char* wal, uint32_t len) { applied.push_back(DecodeWal(wal, len)); return 1; });
        CHECK(undone == 2, "retry after failure undoes all records");
        CHECK(area.ActiveTxnCount() == 0, "chains cleared after successful retry");
    }
}

// ---------------- Test 7：段滚动与部分回收 ----------------
void Test7_SegmentRolling() {
    printf("Test 7: segment rolling and partial reclaim\n");
    CleanTestDir();
    const uint64_t kSmallSeg = 8 * 1024;  // 8KB 强制滚动
    {
        // 每条 UPDATE 记录约 44+~60+20 ≈ 130B；写入 200 条必然跨段
        UndoArea area(kTestDir, kSmallSeg);
        std::vector<UpdateLogRecord*> recs;
        for (int i = 0; i < 200; i++) {
            // 前 100 条属 T100，后 100 条属 T200
            tx_id_t tid = (i < 100) ? 100 : 200;
            auto* u = MakeUpdate(1, tid, i, "new_value_padding_xxxx", "old_value_padding_yyyy");
            area.TrackLog(u);
            recs.push_back(u);
        }
        uint64_t segs_used = area.CurSegId();
        CHECK(segs_used >= 2, "multiple segments used");
        CHECK(SegExists(1), "seg 1 exists");

        // 提交 T100（其记录集中在老段）：老段应被回收，当前段保留
        area.CommitTxn(1, 100);
        CHECK(!area.HasChain(1, 100), "T100 committed");
        CHECK(area.HasChain(1, 200), "T200 still active");
        bool early_segs_gone = !SegExists(1);
        CHECK(early_segs_gone, "early segments reclaimed after T100 commits");
        CHECK(SegExists(area.CurSegId()), "current segment kept");

        for (auto* r : recs) delete r;
    }
    {
        // 重启：T200 的链从幸存段重建
        UndoArea area(kTestDir, kSmallSeg);
        CHECK(area.ActiveTxnCount() == 1 && area.HasChain(1, 200),
              "T200 rebuilt from surviving segments");
    }
}

// ---------------- Test 8：地址全局单调（跨段） ----------------
void Test8_GlobalOrderAcrossSegs() {
    printf("Test 8: undo addr monotone across segments (= log stream order)\n");
    CleanTestDir();
    const uint64_t kSmallSeg = 4 * 1024;
    {
        UndoArea area(kTestDir, kSmallSeg);
        std::vector<UpdateLogRecord*> recs;
        std::vector<uint64_t> tails;
        for (int i = 0; i < 60; i++) {
            auto* u = MakeUpdate(1, 100, i, "n_pad_xxxxxxxx", "o_pad_yyyyyyyy");
            area.TrackLog(u);
            tails.push_back(area.ChainLastAddr(1, 100));
            recs.push_back(u);
        }
        CHECK(area.CurSegId() >= 2, "spans multiple segments");
        bool monotone = true;
        for (size_t i = 1; i < tails.size(); i++) {
            if (tails[i] <= tails[i - 1]) { monotone = false; break; }
        }
        CHECK(monotone, "chain tail addr strictly monotone across segment rolling");
        for (auto* r : recs) delete r;
    }
}

}  // namespace

int main() {
    Test1_TrackLog();
    Test2_CommitTxn();
    Test3_UndoAll();
    Test4_RebuildActive();
    Test5_RebuildCommitted();
    Test6_CrcCorruption();
    Test6b_UndoHardFailure();
    Test7_SegmentRolling();
    Test8_GlobalOrderAcrossSegs();

    printf("\n===== undo_area_test: %d passed, %d failed =====\n", g_pass, g_fail);
    CleanTestDir();
    return g_fail == 0 ? 0 : 1;
}
