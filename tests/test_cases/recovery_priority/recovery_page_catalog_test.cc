// R2c C1 契约测试：统一恢复影响目录（32.3 C1 完成证据）
//
// 覆盖 32.3 C1 验收三要素：
//  1. P0 与受影响页分类契约（UNAFFECTED 默认有扫描证据、AFFECTED 登记、
//     RECOVERED_READY 迁移）；
//  2. 两个存活节点对权威状态一致（相同事件序列喂两个目录实例，
//     Classify 逐页一致）；
//  3. 旧代报告不能解隔离（旧代 MarkRecovered 拒绝、陈旧条目 UNKNOWN、
//     新代必须重新登记验证）。
// 另含 fail-closed 与状态机防御、并发读写基本安全。

#include <cassert>
#include <cstdio>
#include <thread>
#include <vector>

#include "core/recovery_page_catalog/recovery_page_catalog.h"

using recovery_catalog::RPageState;
using recovery_catalog::RecoveryPageCatalog;

static int g_failures = 0;

#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

// 1. 分类契约：P0（无关页）与受影响页
static void test_classification_contract() {
    RecoveryPageCatalog cat;
    // 代 1：故障节点 0，表 7 扫描完成
    cat.BeginGeneration(1, /*failed_node=*/0);
    CHECK(cat.MarkTableSwept(1, /*table=*/7));
    CHECK(cat.MarkAffected(1, 7, /*page=*/101,
                           recovery_catalog::RAffectedReason::FAILED_MANAGER));
    CHECK(cat.MarkAffected(1, 7, /*page=*/0,
                           recovery_catalog::RAffectedReason::FILE_HEADER));

    // P0 页面（表内未登记页）：有扫描证据 → UNAFFECTED，不进恢复状态机
    CHECK(cat.Classify(7, 55).state == RPageState::UNAFFECTED);
    CHECK(cat.Classify(7, 56).state == RPageState::UNAFFECTED);
    // 受影响页：AFFECTED（隔离中）
    CHECK(cat.Classify(7, 101).state == RPageState::AFFECTED);
    CHECK(cat.IsIsolated(7, 101));
    CHECK(cat.IsIsolated(7, 0));  // page 0 随接管登记
    // 未扫描表：UNKNOWN（fail-closed）
    CHECK(cat.Classify(8, 55).state == RPageState::UNKNOWN);
    // 恢复前 UNKNOWN 的页不得因扫描默认被当作可服务受影响页
    CHECK(!cat.IsIsolated(8, 55));

    // Phase 4 就绪迁移：AFFECTED → RECOVERED_READY（带验证版本）
    CHECK(cat.MarkRecovered(1, 7, 101, /*verified_lsn=*/900));
    CHECK(cat.Classify(7, 101).state == RPageState::RECOVERED_READY);
    CHECK(cat.Classify(7, 101).version_lsn == 900);
    // 幂等/版本单调：较小 LSN 不回退
    CHECK(cat.MarkRecovered(1, 7, 101, 800));
    CHECK(cat.Classify(7, 101).version_lsn == 900);

    // 状态机防御：无条目页 / UNAFFECTED 默认页不接受 MarkRecovered
    CHECK(!cat.MarkRecovered(1, 7, 200, 1));
    CHECK(!cat.MarkRecovered(1, 8, 55, 1));   // 未扫描表（UNKNOWN）
    CHECK(cat.Classify(7, 200).state == RPageState::UNAFFECTED);
    // 旧代登记拒绝
    CHECK(!cat.MarkAffected(/*gen=*/0, 7, 300,
                            recovery_catalog::RAffectedReason::FAILED_MANAGER));
    CHECK(!cat.MarkTableSwept(/*gen=*/0, 6));

    auto c = cat.GetCounts();
    CHECK(c.generation == 1);
    CHECK(c.affected == 1);           // page 0 仍隔离
    CHECK(c.recovered_ready == 1);    // page 101
}

// 2. 旧代报告不能解隔离（C1 验收红线）
static void test_stale_generation_cannot_release() {
    RecoveryPageCatalog cat;
    cat.BeginGeneration(1, 0);
    cat.MarkTableSwept(1, 7);
    cat.MarkAffected(1, 7, 42, recovery_catalog::RAffectedReason::FAILED_MANAGER);

    // 新一轮故障：代 2 开始，代 1 的 READY/条目全部作废
    cat.BeginGeneration(2, 1);
    // 旧代报告尝试解隔离：拒绝
    CHECK(!cat.MarkRecovered(1, 7, 42, 999));
    // 陈旧条目（代 1 的 AFFECTED）在新代视角是 UNKNOWN——fail-closed，
    // 不继承旧代状态（既不当 READY 也不当 AFFECTED，须重新核验）
    CHECK(cat.Classify(7, 42).state == RPageState::UNKNOWN);
    // 旧代的表扫描背书也作废：未在新代重扫的表 → UNKNOWN
    CHECK(cat.Classify(7, 55).state == RPageState::UNKNOWN);
    CHECK(cat.GetCounts().stale_entries >= 1);

    // 新代重新登记后恢复验证合法
    CHECK(cat.MarkAffected(2, 7, 42, recovery_catalog::RAffectedReason::FAILED_MANAGER));
    CHECK(cat.MarkRecovered(2, 7, 42, 1000));
    CHECK(cat.Classify(7, 42).state == RPageState::RECOVERED_READY);
    CHECK(cat.Classify(7, 42).version_lsn == 1000);

    // 同代幂等重入（MarkNodeFailed 重试场景）不重置状态
    uint64_t gen = cat.BeginGeneration(2, 1);
    CHECK(gen == 2);
    CHECK(cat.Classify(7, 42).state == RPageState::RECOVERED_READY);
}

// 3. 两个存活节点对权威状态一致：B/C 各自目录实例喂相同事件序列
static void test_two_survivors_consistency() {
    RecoveryPageCatalog cat_b, cat_c;
    for (auto* cat : {&cat_b, &cat_c}) {
        cat->BeginGeneration(5, 0);
        cat->MarkTableSwept(5, 7);
        cat->MarkTableSwept(5, 10007);
        cat->MarkAffected(5, 7, 3, recovery_catalog::RAffectedReason::FAILED_MANAGER);
        cat->MarkAffected(5, 10007, 3,
                          recovery_catalog::RAffectedReason::FAILED_NODE_X_HOLDER);
        cat->MarkRecovered(5, 7, 3, 77);
    }
    for (uint64_t page = 0; page < 40; page++) {
        CHECK(cat_b.Classify(7, page).state == cat_c.Classify(7, page).state);
        CHECK(cat_b.Classify(10007, page).state ==
              cat_c.Classify(10007, page).state);
    }
    CHECK(cat_b.GetCounts().affected == cat_c.GetCounts().affected);
    CHECK(cat_b.ListIsolated().size() == cat_c.ListIsolated().size());
    // page 3 在两侧同为 READY 且版本一致
    CHECK(cat_b.Classify(7, 3).version_lsn == cat_c.Classify(7, 3).version_lsn);
}

// 4. 并发读写基本安全（登记线程 + 查询线程）
static void test_concurrent_access() {
    RecoveryPageCatalog cat;
    cat.BeginGeneration(1, 0);
    cat.MarkTableSwept(1, 7);
    std::vector<std::thread> threads;
    for (int w = 0; w < 2; w++) {
        threads.emplace_back([&, w] {
            for (uint64_t p = 1; p <= 200; p++) {
                cat.MarkAffected(1, 7, p,
                                 recovery_catalog::RAffectedReason::FAILED_MANAGER);
                cat.MarkRecovered(1, 7, p, p * 10);
            }
        });
    }
    for (int r = 0; r < 2; r++) {
        threads.emplace_back([&] {
            for (uint64_t p = 1; p <= 200; p++) {
                RPageState s = cat.Classify(7, p).state;
                // 只允许在合法状态间观察：登记前瞬态可为 UNAFFECTED
                //（表已扫描、无条目默认），登记后 AFFECTED → READY
                CHECK(s == RPageState::AFFECTED ||
                      s == RPageState::RECOVERED_READY ||
                      s == RPageState::UNKNOWN ||
                      s == RPageState::UNAFFECTED);
            }
        });
    }
    for (auto& t : threads) t.join();
    // 收敛后全部 READY、版本为页号×10
    for (uint64_t p = 1; p <= 200; p++) {
        CHECK(cat.Classify(7, p).state == RPageState::RECOVERED_READY);
        CHECK(cat.Classify(7, p).version_lsn == p * 10);
    }
}

int main() {
    test_classification_contract();
    test_stale_generation_cannot_release();
    test_two_survivors_consistency();
    test_concurrent_access();
    if (g_failures == 0) {
        std::printf("recovery_page_catalog_test: ALL PASS\n");
        return 0;
    }
    std::printf("recovery_page_catalog_test: %d FAILURES\n", g_failures);
    return 1;
}
