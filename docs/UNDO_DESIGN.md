# Hybrid_Cloud_MP 独立 Undo 区设计

> 版本：v1.0
> 状态：已实现（代码 + 单元测试）
> 范围：存储端 undo 信息的存放、事务提交判定、崩溃恢复 undo 回放
> 关联文档：`docs/LOG_SYSTEM_DESIGN.md`（本设计落地其阶段三"事务表物化"的 undo 部分）

---

## 1. 现状与问题

原实现（redo-all + undo-uncommitted 模型）中 undo 信息**内嵌在 WAL 记录里**
（UPDATE 带 `old_value_`、DELETE 带 undo meta、FSMUPDATE 带 `old_free_space_`），
没有独立的 undo 存放区。由此产生三个结构性问题：

| # | 问题 | 后果 |
|---|------|------|
| U1 | 提交判定靠全扫 WAL 找 BATCHEND | `UndoForFailedNode` 需**两遍全量扫描**，恢复时间随日志总量线性增长 |
| U2 | undo 信息生命周期与 WAL 绑定 | 未来 checkpoint 后回收旧 WAL 段时，活跃长事务的 undo 信息随之丢失，段回收被活跃事务牵制 |
| U3 | 无事务表 | "哪些事务活跃、它们的日志在哪"无索引可查，undo 定位只能穷举 |

## 2. 参考设计分析

### 2.1 PostgreSQL：不采用

PostgreSQL **没有传统意义的 undo 日志**：堆内多版本（tuple 头带 xmin/xmax），
旧版本留在堆中由 VACUUM 清理，事务状态存 CLOG。该路线要求存储格式内嵌
版本信息并配套可见性判断与 vacuum，等于推翻本系统"单版本 + 物理 undo"的
存储模型，代价过大，不采用。

### 2.2 oGRAC：核心思想借鉴

oGRAC（`pkg/src/kernel/xact/knl_undo.c`）是经典的独立 undo 区设计：

```
UNDO 表空间
 └── undo segment × N
      ├── 段头页（空闲页链表 page_list + txn_page 数组）
      ├── txn page（事务表：事务槽位 = 状态 + undo 链位置）
      └── undo page（页内 undo_row 追加，页头 prev 串同事务页链）
```

- undo_row 自带 `xid` 与定位信息，同事务的 undo 记录**串链**；
- 事务提交时更新事务槽状态，undo 页回收到段空闲链表（空间复用）；
- 崩溃后事务状态精确可查（txn page 持久化），未提交事务由后台回滚线程
  **沿链反向**回滚；
- SMON 按 retention 收缩段（超保留期的空闲页还给表空间）。

**借鉴**：独立 undo 区（解耦 WAL 生命周期）、undo 链（按事务索引）、
事务表（精确提交判定）、提交即回收（空间有界）。
**简化**（适配本系统体量与 MVP 定位）：

| oGRAC | 本系统 |
|-------|--------|
| 段内页级空闲链表复用 | 段级引用计数，整段回收（MVP） |
| txn page 槽位式事务表 | append-only `txn_table.log`（21B/条状态变迁） |
| undo 页串链 | undo 记录内嵌 `prev_addr` 直接串链 |
| 后台回滚线程 | 沿用现有 Phase 4 `UndoForFailedNode` 触发点 |

## 3. 设计

### 3.1 物理布局

```
undo/
  txn_table.log               # 事务状态变迁（append-only）
  useg_0000000000000001.log   # undo 数据段，16MB 定长滚动
  useg_0000000000000002.log
```

**undo 数据记录**（`undofmt::UNDO_REC_HEADER_SIZE = 20B` 头）：

```
[magic(4)="UND1" | rec_len(4) | crc32(4) | prev_addr(8) | wal_rec(rec_len-20)]
```

- `prev_addr`：同事务前一条 undo 记录地址（0 = 链头）——**undo 链**；
- `wal_rec`：完整内嵌 WAL 记录（44B 头 + payload）。undo 所需信息 WAL 记录
  本身已带齐（old_value / undo meta / old_free_space / 被删 rid），内嵌整体
  避免逐类型抽取字段，应用侧 deserialize 后直接复用现有 `apply_undo_log`
  等逻辑，两条 undo 路径（undo 区 / WAL 全扫 fallback）语义完全一致；
- crc32c 覆盖 `prev_addr..尾`，半写/坏记录识别为段尾截断。

**undo 地址（UndoAddr）**：`seg_id * SEG_SIZE + 段内偏移`，与 `logfmt::LogAddr`
同构。单写者（replay 线程）⇒ 地址单调递增 ⇔ **地址序 = 日志流序**。

**事务状态记录**（`txn_table.log`，21B/条）：

```
[rec_len(4) | crc32(4) | node_id(4) | tid(8) | status(1)]
status: 1=COMMITTED  2=UNDONE（恢复 undo 完成）
```

### 3.2 内存事务表

```cpp
struct TxnUndoChain {
    node_id_t node_id;  tx_id_t tid;
    uint64_t last_addr;                  // 链尾（最新一条），undo 从此反向回溯
    uint32_t count;
    unordered_set<uint64_t> seg_ids;     // 占用段集合（段回收引用计数）
};
unordered_map<TxnKey, TxnUndoChain> chains_;       // 仅未提交事务
unordered_map<uint64_t, uint32_t> seg_refs_;       // 段 → 引用事务数
```

### 3.3 三条路径

**生成（replay 线程，`apply_sigle_log` 入口挂载点）**：
- 数据日志（UPDATE/INSERT/DELETE/BLINKINSERT/BLINKDELETE/FSMUPDATE）：
  apply 前 `TrackLog` → serialize 整条 WAL 记录追加到 undo 区、挂到
  该事务链尾。放在 apply 前使 undo 区内容与"日志已到达"语义一致
  （apply 幂等跳过的记录其 undo 同样入区，与 WAL 全扫语义等价）；
- BATCHEND：`CommitTxn` → 先写 `txn_table.log`（COMMITTED）再释放链空间
  （顺序保证：崩溃后提交状态可查，不会误 undo 已提交事务）。

**回收（提交即回收）**：
- 释放 = 链涉及段的引用计数各减 1；归零即整段删除；
- 归零的若是**当前写入段**，关闭删除并**滚动到下一段号**（而非原位复用）：
  否则新记录地址小于历史地址，破坏地址单调性；且保证盘上不留无归属
  残留记录，是 `txn_table.log` 压缩的前提；
- `txn_table.log` 压缩：无活跃事务时截断（全部状态已消费）。提交路径
  开销 = 一次 21B append write，process 崩溃模型下与系统整体 durability
  一致（掉电模型依赖后续 `log_durability=machine` 配套加固）。

**崩溃恢复 undo（`UndoForFailedNode` 重写）**：

```
WaitReplayCaughtUp → PauseReplay（沿用现有 C4 修复）
if undo 区可用:
    UndoAllActiveTxns:
      1. 从事务表取全部活跃事务，沿各自 prev_addr 链收集 undo 记录地址
      2. 按地址全局降序排序（= 原实现的"日志流降序"语义）
      3. 逐条读记录（crc 校验）→ ApplyUndoWalRecord 应用（幂等）
      4. 全部应用完后：逐事务写 UNDONE 状态 → 释放链空间
         （步骤 3 完成前崩溃：无状态无删除，下次恢复完整重来；
           步骤 4 中崩溃：已处理事务状态可查，剩余事务幂等重 undo）
else: 回退原两遍全扫 WAL 路径（功能不缺失）
```

### 3.4 启动重建（索引即缓存）

undo 区全部结构可从盘内容重建，不构成新故障点：

```
1. 按段号升序扫描 useg_*：逐条校验 magic/crc，尾部坏记录截断；
   记录自带 (node_id, tid, prev_addr) ⇒ 扫描序 = 写入序，
   逐条覆盖即重建 chains_ 与链尾
2. 重放 txn_table.log：剔除 COMMITTED/UNDONE 事务（修正段引用）
3. 重算段引用计数，删除零引用段
4. 写位置 = 最大存活段的有效数据尾（续写恰好覆盖坏尾）；
   无存活段则从最大段号 +1 开新段（地址单调）
5. 无活跃事务则截断 txn_table.log
```

## 4. 正确性论证

### 4.1 关键不变量

| # | 不变量 | 保障机制 |
|---|--------|----------|
| N1 | undo 区内容与"已到达 WAL 的数据日志"一致 | replay 线程 apply 前 TrackLog（含幂等跳过的记录） |
| N2 | 已提交事务不被 undo | CommitTxn 先写 txn_table 再释放空间；重建时状态重放剔除 |
| N3 | undo 顺序 = 日志流降序 | 单写者 ⇒ 地址单调；UndoAllActiveTxns 全局降序排序 |
| N4 | 崩溃于 undo 任意阶段可重恢复 | 步骤 3（应用）与步骤 4（状态/删除）严格先后；全部 undo 应用幂等 |
| N5 | undo 区故障不致死 | 初始化/写入失败 → enabled_=false → 回退 WAL 全扫路径 |
| N6 | txn_table 压缩不丢状态 | 截断前提 = 无活跃事务 ⇒ 无引用段已全部删除 ⇒ 盘上无 undo 记录 |

### 4.2 故障场景矩阵

| 场景 | 分析 |
|------|------|
| 计算节点崩溃 | Phase 4 走 undo 区路径：活跃事务链完整（N1），按链 undo（N3） |
| 存储进程崩溃 | undo 区 write 已进 page cache，重建恢复事务表与链（§3.4） |
| undo 区写半截记录 | 段扫描 crc 校验截断；该记录所属 WAL 未重放或未提交，由 redo/后续 undo 吸收 |
| undo 中途崩溃 | N4：未写状态 ⇒ 完整重 undo（幂等）；部分状态 ⇒ 剩余重 undo |
| 提交后立刻崩溃 | N2：txn_table 已落 page cache，重建剔除，不误 undo |
| undo 区整体损坏/写失败 | N5：fallback 到 WAL 全扫，功能等价 |
| 重复 TrackLog（断点重放） | 链内出现幂等副本，undo 应用幂等，结果不变 |

## 5. 与 LOG_SYSTEM_DESIGN 阶段三的关系

本设计落地了阶段三中"事务表物化"的 undo 维度，且更进一步把 undo 信息
从 WAL 物理剥离：

- **已解决**：U1（undo 免全扫）— `UndoForFailedNode` 复杂度从 O(全量日志)
  降为 O(未提交日志数)；U2（undo 与 WAL 解耦）— 为 WAL 段回收扫除
  活跃事务牵制；
- **仍待做**（阶段三剩余）：页前驱链 + PAGEIDX 物化（RedoForPages 加速）、
  checkpoint 全流程 + WAL 段回收（undo 区已就绪，checkpoint 后旧 WAL 段
  可直接删除，活跃事务 undo 不受影响）；
- **后续可选**：undo 记录紧凑格式（只存 undo 载荷而非整条 WAL 记录，
  体积约减半）；段内页级复用（oGRAC page_list 方式）；undo retention
  （若未来引入 MVCC/一致性读需要保留已提交事务的 undo）。

## 6. 测试

`tests/test_cases/undo_test/undo_area_test_main.cc`（47 项断言全 PASS）：

| # | 内容 |
|---|------|
| 1 | TrackLog 建链、控制记录不产生 undo |
| 2 | CommitTxn 回收：链移除、零引用段删除、当前段滚动、txn_table 压缩 |
| 3 | 多事务交错 undo：全局降序、内容完整（old_value 逐字节校验）、链与段清空 |
| 4 | 崩溃重建：未提交事务链精确恢复、链尾地址稳定、重建后可正常 undo |
| 5 | 崩溃重建：已提交事务经 txn_table 重放剔除，不复活 |
| 6 | CRC 损坏注入：损坏点截断，之前记录完好 |
| 7 | 段滚动 + 部分回收：老段随提交删除，幸存段重建 |
| 8 | 地址全局单调（跨段） |

回归：`fsm_test` 全 PASS；全量编译（storage_pool / compute_server /
remote_node）通过。

## 附：术语

| 术语 | 含义 |
|------|------|
| UndoAddr | undo 区全局地址 = seg_id × 16MB + 段内偏移 |
| undo 链 | 同事务 undo 记录按 prev_addr 串成的反向链 |
| 事务表 | 内存活跃事务链表（TxnUndoChain map），崩溃后从 undo 段 + txn_table.log 重建 |
| 提交即回收 | BATCHEND 重放时回收该事务 undo 空间（undo 区只存未提交事务） |
