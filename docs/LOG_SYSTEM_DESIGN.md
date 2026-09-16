# Hybrid_Cloud_MP 日志存储结构与日志系统优化设计

> 版本：v1.0
> 范围：计算端日志生成/发送、存储层日志写入/重放/恢复，覆盖 heap 页面、BLink 索引、FSM 三类数据的日志
> 目标：**正确性**（任何单点故障后数据不丢不错）、**高可用**（故障可检测、可恢复、恢复时间可控）、**可演进**（格式版本化）

---

## 1. 现状分析

### 1.1 现有日志链路

```
计算节点                                存储节点
─────────                              ─────────
GenXXXLog()  ──► log_records 队列 ──► LogFlush() ──RPC LogWrite──► LogManager::write_batch_log_to_disk()
(DTX 层)          (server.h)          (批量swap)                    (lseek+write 追加 LOG_FILE)
                                                                          │
                                              LogReplay::replayFun ◄──────┘
                                              (后台线程，从 persist_off_ 断点顺序重放到数据文件)
```

故障恢复：
- **计算节点崩溃**：Phase 1~4（锁状态清理）→ Phase 4 `AnalyzeRecoveryPages` → `RedoForPages` 定向补页 + `UndoForFailedNode` 撤销未提交事务；
- **存储层重启**：`LogReplay` 构造时从日志文件头读 `persist_off_` 断点续放；`open_db` 全量重建 BLink/FSM 派生结构；
- **blink/FSM**：逻辑日志（`BLINKINSERT/BLINKDELETE/FSMUPDATE`）在存储端幂等重放，undo 走"插入↔删除互逆、FSM 旧值恢复"。

### 1.2 问题清单

#### P0 正确性硬伤

| # | 问题 | 位置 | 后果 |
|---|------|------|------|
| C1 | `lseek(SEEK_END)+write` 追加非原子，brpc 多线程并发处理多节点 LogWrite 时互相覆盖 | `log_manager.cc:31` | 日志文件物理损坏 |
| C2 | `LogFlush` 被后台线程/提交路径/恢复 Phase2-3 并发调用，各 swap 一部分队列，RPC 到达顺序与 LSN 顺序可交错 | `server.h:1958/2261/2383` | 同页日志乱序 |
| C3 | UPDATE 重放 LSN 检查失效（`page_hdr->LLSN_ >= log_llsn` 时 assert 被注释，**仍继续应用**），乱序/重复重放用旧值覆盖新值 | `logreplay.cc:495` | 数据回退 |
| C4 | `UndoForFailedNode`（RPC 线程）与 `replayFun`（后台线程）并发写同一数据页，页面级撕裂无防护 | `logreplay.cc` | 页面损坏 |
| C5 | 无 checksum/魔数，尾部半条记录与"正常结尾"不可区分；垃圾 `tot_len` 可导致巨型分配/错位解析 | 全链路 | 解析崩溃/数据错乱 |
| C6 | 全程无 fsync/fdatasync，write 只进内核 page cache | `log_manager.cc` | 机器掉电时已提交事务回退 |

#### P1 结构性缺陷

| # | 问题 | 后果 |
|---|------|------|
| S1 | 无 checkpoint/截断（`checkpointFun` 空壳），日志无限增长 | 重启恢复 O(全量日志)，Undo 两遍全扫 |
| S2 | `persist_off_` 与数据混在同一文件头，每重放一条日志对文件头 write 两次 | 高频小写；撕裂时 batch_id/offset 组合错乱 |
| S3 | 单一日志文件混合所有计算节点 | Undo 被迫全扫；多节点 LSN 交错使"按 LSN 降序 undo"名不副实 |
| S4 | LSN 概念混淆：`current_llsn_` 是节点本地计数器却被当全局 WAL LSN 用 | 语义误导；真正可靠的全局序只有文件偏移 |
| S5 | `RedoForPage`/`RedoForPages`/`UndoForFailedNode` 三份几乎相同的全量扫描代码 | 恢复 O(页数×日志)；维护三份解析逻辑 |

#### P2 效率问题

| # | 问题 |
|---|------|
| E1 | 每条日志内嵌 `table_name` 字符串（~17B/条），且 DELETE 同时带 table_id 与表名，格式不统一 |
| E2 | 记录头固定 44B，控制记录与数据记录同头，空间浪费 |
| E3 | 计算端每条日志一次裸 `new` 派生类对象 + 手动 `delete[] char*` 表名 + 逐条 serialize 拼接 string，三次拷贝 |
| E4 | `txn_log->logs`（SendLogToStoragePool）与全局队列（AddToLogNoBlock）双发送路径并存，曾导致 GenFSMUpdateLog 成死代码 |

---

## 2. 设计目标与原则

### 2.1 目标

| 维度 | 目标 | 度量 |
|------|------|------|
| 正确性 | 任意单节点故障/存储层重启后：已提交事务不丢、未提交事务不留、blink/FSM 与 heap 一致 | 故障注入测试 + 一致性校验全绿 |
| 高可用 | 计算节点故障恢复时间可控；存储层重启恢复时间与日志增量成正比 | 恢复时间回归基准 |
| 持久性 | 支持进程崩溃模型；机器掉电模型可通过配置开启（fsync 模式） | 配置项 `log_durability = process / machine` |
| 可演进 | 日志格式带版本号，新旧格式可识别、可平滑切换 | 升级测试 |

### 2.2 原则

1. **WAL 铁律**：任何数据页落盘前，描述其修改的日志必须先持久化；
2. **提交点明确**：事务 commit 记录持久化成功前，不向客户端返回成功（沿用 `wait_log_flush_v2` 语义并加固）；
3. **重放幂等**：任意前缀重放 + 任意区间重复重放，结果收敛；
4. **redo-all + undo-uncommitted**：沿用现有"未提交日志也物化、故障时反向撤销"模型，不推翻重做；
5. **派生结构双保险**：blink/FSM 既靠逻辑日志增量维护，又保留 `open_db` 全量重建兜底；
6. **索引即缓存**：所有为恢复加速而建的持久化索引（页表、事务表）崩溃后可从日志重建，不构成新的故障点。

---

## 3. 日志存储结构 v2

### 3.1 总体布局：段式日志 + 独立 manifest

```
log_v2/
  manifest.0  manifest.1          # 元数据双写（防自身撕裂，epoch 大者生效）
  seg_0000000000000001.log        # 64MB 定长段，写满滚动
  seg_0000000000000002.log
  ...
```

**段（Segment）是日志的物理单元**：
- 段号 64 位单调递增；**段号 + 段内偏移构成全局日志地址（LogAddr）**，取代现在被滥用的 LSN 全局序角色；
- 段写满滚动；checkpoint 之后，满足"所有页表/事务表已物化且无任何 Phase4 引用"的旧段可整段删除（或归档）；
- 单一日志文件无限增长问题（S1）由此根治：重启恢复只需读活跃段，恢复时间与日志增量成正比。

**manifest 是唯一的元数据中心**（解决 S2）：

```
ManifestRecord {
  magic(4)=0x4D414E49          # "MANI"
  version(4)
  epoch(8)                      # 单调递增，双写取大者
  active_seg_id(8)              # 当前写入段
  checkpoint_addr: seg_id(8) + block_idx(4) + rec_off(4)   # 最近 checkpoint 点
  replayed_addr:   seg_id(8) + block_idx(4) + rec_off(4)   # 重放进度（原 persist_off_）
  page_table_snap_seg(8)        # 页表快照所在段（见 3.5）
  txn_table_snap_seg(8)         # 活跃事务表快照所在段
  crc32(4)
}
```

- `replayed_addr` **不再每重放一条就写盘**，改为批量更新（每 1024 条或每 100ms 或段切换时）；崩溃后多重放的部分由幂等机制吸收（§5.3）；
- manifest 双写 + epoch + CRC，单份撕裂可识别并回退到另一份。

### 3.2 段内结构：自描述 Block

```
SegFile = [SegHeader 4KB] + Block[0..N]
SegHeader {
  magic(4)=0x4C534547           # "LSEG"
  version(4)
  seg_id(8)
  create_epoch(8)
  crc32(4)
}
Block (32KB) {
  block_crc32(4)                # 覆盖 block_seq 之后全部内容
  block_seq(4)                  # 段内块序号，用于定位与连续性校验
  record_cnt(2)
  flags(2)                      # bit0: 段尾填充块
  records...
  pad 至 32KB
}
Record {
  crc32(4)                      # 覆盖 len..payload
  len(2)                        # payload 长度（<= 32KB-头；超长记录分片）
  type(1)                       # 记录类型
  flags(1)                      # bit0=FIRST, bit1=LAST（分片标记）
  payload
}
```

**设计要点**：
- **每块自校验、自描述**：块级 CRC 识别撕裂/位翻转，坏块整块跳过；块内记录完整边界清晰，尾部半条记录识别为"写中断"正常截断（解决 C5）；
- **可从任意块边界开始解析**——为并行恢复（§6.4）和按段局部扫描打底；
- 超长记录（>31KB 的 payload，本系统几乎不存在）按 FIRST/MIDDLE/LAST 分片。

### 3.3 记录布局：批级封装 + 紧凑编码

**v1 问题**：每条记录 44B 定长头 + 内嵌表名，控制/数据记录同头（E1/E2），批概念（batch_id）散落在每条记录里。

**v2 将"批"实体化**（group commit 天然一批一批写，批是 node 级物理单位）：

```
BatchRecord (type=BATCH) payload {
  batch_id(8)
  node_id(4)
  txn_chunk_cnt(varint)
  batch_payload_crc(4)
  TxnChunk × N {
    tid(varint)
    flags(1)                     # bit0=COMMITTED（替代独立 BATCHEND 记录）
    rec_cnt(varint)
    DataRec × M {
      type(1)                    # INSERT/UPDATE/DELETE/BLINKINSERT/BLINKDELETE/FSMUPDATE/NEWPAGE
      page_ref(8)                # table_id(4) + page_no(4) 打包；逻辑日志该字段为派生表 ref
      page_ver(8)                # 页版本号（原 lsn_，per-page 单调，仅页面日志使用）
      prev_page_addr(8)          # 同页面前驱日志地址（页前驱链，§3.5）
      undo_off(varint)           # undo 信息在 payload 中的偏移（0=无 undo 信息）
      data_len(varint)
      data[data_len]             # 页面日志：整行物理内容；blink/FSM：逻辑参数
    }
  }
}
```

**语义变化**：
1. **提交标记下沉到 TxnChunk.flags**：undo 判定 = "该 tid 是否存在 COMMITTED chunk"，与现在扫描 BATCHEND 等价但少一条记录；
2. **表名消灭**：日志只带 `table_id`；`table_id→表名` 映射由存储层在 OpenDb/建表时同步，新增控制记录 `TABLEMAP{table_id, name}` 写入日志供回放侧补全（解决 E1）；
3. **字段按需出现**：blink/FSM 逻辑日志没有页版本链语义，`page_ver/prev_page_addr` 置 0；`undo_off` 显式标记是否携带 undo 映像（UPDATE 的 old_value、DELETE 的 undo meta、FSM 的 old_free_space），消除现在"靠 has_old 标志位散落在各记录里"的不一致；
4. **varint**：tid/len 小值 1~2B。

**体积估算**：以 smallbank 一次转账（约 4 条 UPDATE + 提交标记）为例，v1 约 5×(44+17+~120) ≈ 900B；v2 约 批头 16 + chunk 头 3 + 4×(1+8+8+8+1+~120) ≈ 600B，**降约 1/3**；含 blink/FSM 日志的插入路径降幅更大。

### 3.4 页前驱链（解决 S5 的关键结构）

**问题**：`RedoForPages` 需要"某页在某版本区间内的全部日志"，现在是全量扫描 + 分桶。

**方案**：数据记录中的 `prev_page_addr` 指向**同一页面上一条日志的 LogAddr**，形成 per-page 反向链：

```
写入侧（存储层 LogWriter，内存态）：
  page_latest: HashMap<page_ref, LogAddr>     # 每页最新日志地址
  写记录时：rec.prev_page_addr = page_latest[rec.page_ref] （无则 0）
            page_latest[rec.page_ref] = rec.addr
物化：
  checkpoint 时将 page_latest 快照写入新段的 PAGEIDX 特殊块
```

**恢复侧**：
- 重启：读 manifest 定位最近 PAGEIDX 快照 → 继续重放过程中增量维护 → 崩溃后快照丢失则从其位置重扫一段重建（索引即缓存，不构成故障点）；
- Phase 4 定向 Redo：从 `page_latest[page]`（或磁盘页版本）沿链回溯，收集 `(disk_ver, target_ver]` 区间记录，O(该页日志数)；
- `UndoForFailedNode` 同理获得事务维度：**重放时在内存维护活跃事务表 `tid → [LogAddr]`**，checkpoint 时随页表一起物化（TXNIDX 块）；故障 undo 只按事务表反向读命中记录，不再两遍全扫（解决 S1/S3 的扫描代价）。

**正确性注意**：页前驱链只用于**加速定位**，应用时仍以"页版本比较"为准（`log.page_ver > page.disk_ver` 才应用，见 §5.3）；链断裂（快照陈旧）时回退为段内扫描，功能不缺失。

### 3.5 与 blink/FSM 逻辑日志的关系

blink/FSM 日志在 v2 中**保留独立逻辑记录类型**，理由：
- 已实现的幂等重放 + undo 互逆语义验证充分；
- 逻辑日志不挂页版本链（blink/FSM 页面无 RmPageHdr），与页前驱链正交；
- 存储端重放经 `SmManager::GetOrCreateBLinkHandle` 的树句柄 + `op_mutex` 串行化，与 redo 线程/undo 线程互斥（已实施）。

**后续可选优化（不在本期范围）**：blink 插入/删除并入 heap INSERT/DELETE（重放 heap 日志时若表有主键则顺手维护 blink，由 bitmap 重算 FSM），日志量再降 1/3~1/2；代价是重放逻辑耦合表元数据、undo 顺序约束变强。建议 v2 稳定后再评估。

---

## 4. 写入与持久化路径设计

### 4.1 计算端：单写者 + Arena 线格式（解决 C2/E3/E4）

```
GenXXXLog()                         LogFlushThread（唯一）
    │                                    │
    ▼                                    ▼
直接按 v2 线格式写入              swap active/flushing
active arena（4MB 双缓冲）   ──►   整块 RPC LogWrite
                                    失败整块回队重试
```

- **发送单写者**：`LogFlush` 增加 `flush_mtx_` 串行化（swap + RPC + 失败回队必须互斥），彻底消除并发 flush 导致的批间乱序（C2）；恢复 Phase2/3 里的 `LogFlush()` 调用走同一把锁；
- **Arena 线格式**：`GenXXXLog` 不再构造 LogRecord 对象，直接在 active 块上按 §3.3 布局写入；批与事务 chunk 的边界在 arena 内自然成形。消除逐对象分配、逐条 serialize、中间 string 三层开销（E3），顺带根除裸 `char*` 生命周期隐患；
- **通道收敛**：`txn_log->logs` 与全局队列统一为 arena 单通道，2PC 与单节点提交共用（E4）；
- 提交点不变：`AddLogToTxn` → `wait_log_flush_v2(max_ver, need_round)` 等待本事务日志（含 COMMITTED chunk）随某次成功 flush 发出。

### 4.2 存储层写入：O_APPEND + 批量落盘 + 可配 fsync（解决 C1/C6/S2）

```
StoragePoolImpl::LogWrite
    │
    ▼
LogManager::AppendBatch(bytes)          # append_mtx_ 互斥（C1）
    │  ① 写入当前段写缓冲（写满滚动新段）
    │  ② 更新内存 page_latest / 活跃事务表
    │  ③ 批内若含 COMMITTED chunk 且 durability=machine → fdatasync
    ▼
返回成功（进程崩溃模型下 write 即可；掉电模型下已 fdatasync）
```

- **append 原子性**：`append_mtx_` 互斥 + 文件 `O_APPEND` 双保险（C1）；
- **fsync 策略**（配置项 `log_durability`）：
  - `process`（默认，沿用现状语义）：write 即返回，进程崩溃不丢（内核 page cache 仍在）；
  - `machine`：含 COMMITTED chunk 的批次 fdatasync；段切换 fdatasync；manifest 写入 fsync。代价是吞吐下降，换来掉电不丢已提交事务（C6）；
- **持久化语义与提交点的闭环**：`wait_log_flush_v2` 返回 ⇒ 日志已在存储层 write（process）或 fsync（machine），事务方可应答提交成功。

### 4.3 重放器：统一 LogStreamReader（解决 S5）

抽取唯一的日志解析迭代器，供三处使用方复用：

```
class LogStreamReader {              // 新组件
  SeekTo(LogAddr)                    // 定位到段/块/记录
  Next() -> Result<RecordView>       // 块 CRC 校验、分片重组、坏块跳过
  // RecordView 为零拷贝视图，指向块内记录
};
```
- `replayFun`：从 `manifest.replayed_addr` 起持续 `Next()`（断点续放语义不变）；
- Phase 4 / Undo：页链回溯命中后按 LogAddr 直接 `SeekTo` 读单条；
- 旧格式（v1 LOG_FILE）由独立的 LegacyReader 处理，仅用于升级迁移（§7）。

### 4.4 重放的应用规则（正确性核心，解决 C3）

统一为一条规则：

```
对页面日志（INSERT/UPDATE/DELETE）：
  if (rec.page_ver > page.disk_ver) apply(rec)  else skip      # 幂等且容忍乱序
对逻辑日志（BLINK/FSMUPDATE）：
  apply（操作本身幂等：insert_entry 去重 / remove_entry 无操作 / FSM 类别设置收敛）
对控制日志：维护事务表/表名映射
```

- **废止 prev_lsn 严格链**（`prev_lsn != page.LLSN → assert` 的现行语义过脆），改为页版本比较：重复重放跳过、乱序到达的旧版本跳过、页面被计算端推送的新版本覆盖后旧日志跳过——全部收敛；
- C3 的 UPDATE 旧值覆盖新值问题由此根除；
- `page_ver` 由计算端 `GenLogLSN/UpdatePageLLSN` 分配（沿用现有机制，不重造）。

### 4.5 重放与 Undo 的互斥（解决 C4）

- `UndoForFailedNode` 全程持有 `PauseReplay()`（机制已存在）；undo 结束后 `ResumeReplay()`；
- 页表/事务表使 undo 窗口从"两遍全扫"缩到"按事务读命中记录"，停顿时间可接受；
- blink/FSM 的 undo 与 redo 经树句柄 `op_mutex` 互斥（已实施），与 PauseReplay 不叠加死锁（锁序：replay_pause → op_mutex，单向）。

---

## 5. Checkpoint 与恢复设计

### 5.1 Checkpoint 流程（实现 S1 的根治）

```
checkpointFun（周期触发 or 段大小阈值 or 恢复前）：
  1. PauseReplay()；阻塞新 LogWrite 落盘（writer 持有 append_mtx_ 时等待）
  2. 物化页表/活跃事务表：向当前段写 PAGEIDX / TXNIDX 特殊块
  3. flush 全部数据文件脏页（heap/blink/FSM）          # 保证磁盘数据 >= checkpoint 点
  4. checkpoint_addr = 当前写位置；写 manifest（双写、epoch++、fsync）
  5. ResumeReplay()；恢复写入
  6. 后台回收：checkpoint_addr 之前且无 Phase4 引用的旧段 → 删除/归档
```

- checkpoint 不要求"日志静默"：步骤 1 的停顿仅覆盖物化与 manifest 写入（毫秒级）；新日志在其后继续 append；
- 与 `open_db` 全量重建 blink/FSM 的关系：checkpoint 已保证 heap 物化到 checkpoint 点，重建基于的 heap 是最新的；重建后增量 blink/FSM 日志继续重放收敛（现有 PauseReplay + InvalidateAllBLinkHandles 机制沿用）。

### 5.2 存储层重启恢复

```
1. 读 manifest（双份取 epoch 大者，CRC 校验）
2. 装载最近 PAGEIDX/TXNIDX 快照（缺失则从 checkpoint_addr 起扫一段重建）
3. LogStreamReader 从 replayed_addr 续放至日志尾（§4.4 幂等规则保证
   "已物化但 replayed_addr 未更新"的重复区间安全跳过）
4. 重放完成后开放 LogWrite / OpenDb 服务
```

相对现状的改进：现在重启恢复 O(全量日志)（且旧代码直接跳过重放，已修）；v2 为 O(checkpoint 后增量)。

### 5.3 计算节点崩溃恢复（Phase 4 增强）

```
NotifyNodeFailure（沿用）
  → Phase 2 各存活节点 LogFlush + 上报持页（沿用）
  → AnalyzeRecoveryPages：
      对每页：页表查 page_latest → 沿前驱链回溯收集 (disk_ver, target_ver] 区间
             → 按版本序应用（§4.4）→ 返回页面数据
      blink/FSM 页：不再误读 RmPageHdr（已修 GPLM 上报为 0），
                   页表按类型区分后直接以存储端重放后状态应答
  → UndoForFailedNode（仅第一个到达的存活节点执行，沿用 recovery_generation 去重）：
      PauseReplay → 从事务表取故障节点未提交 tid → 按日志地址降序逐条 undo
      （UPDATE 恢复 old_value；DELETE 恢复 undo meta；BLINKINSERT→remove；
       BLINKDELETE→重插；FSMUPDATE→old_free_space）→ ResumeReplay
```

### 5.4 并行恢复（高可用的恢复时间保障）

- 段/块自描述 ⇒ 可按段划分扫描任务；
- 页间无依赖 ⇒ heap 页面 redo 按页桶多线程应用；
- blink/FSM 逻辑日志要求**文件序**（同 key 操作有序）⇒ 单独一个串行通道按序应用；
- Undo 按事务划分并行（不同 tid 的记录无共享页锁语义——undo 的是物理映像，需按页合并后再并行，保守起见首版保持串行，仅优化扫描）。

---

## 6. 正确性论证

### 6.1 关键不变量

| # | 不变量 | 保障机制 |
|---|--------|----------|
| I1 | 同一页面的日志在日志流中全序且可回溯 | 发送单写者（§4.1）+ append_mtx_（§4.2）+ 页前驱链（§3.5） |
| I2 | 事务提交应答前，其全部日志（含 COMMITTED 标记）已持久化到存储层 | `wait_log_flush_v2` + `log_durability` 语义（§4.2） |
| I3 | 任意日志区间重复重放收敛 | 页版本比较 + 逻辑日志幂等（§4.4） |
| I4 | 恢复终态 = 全部已提交事务的效果 | redo-all 物化 + 事务表判提交 + undo 按地址降序撤销（§5.3） |
| I5 | blink/FSM 与 heap 一致 | 逻辑日志增量维护（已实施）+ open_db 全量重建兜底 |
| I6 | 元数据（manifest）不引入新故障点 | 双写 + epoch + CRC；索引/表快照皆可重建（§3.1/§3.5） |

### 6.2 故障场景矩阵

| 场景 | 后果分析 |
|------|----------|
| 计算节点进程崩溃 | 未落盘日志丢失 ⇒ 对应事务未提交，Phase 4 undo 清理；blink/FSM 由逻辑日志在存储端重放收敛（I4/I5） |
| 存储层进程崩溃 | manifest.replayed_addr 可能落后于实际物化 ⇒ 重启后重复区间被 I3 跳过；页表快照缺失则重建（I6） |
| 写日志中途崩溃（尾部半条记录） | 块 CRC 识别 ⇒ 尾部截断；该批事务未提交 ⇒ undo/忽略（I2/I4） |
| manifest 单份撕裂 | epoch+CRC 取另一份（I6） |
| 机器掉电（durability=machine） | 已应答提交的事务日志已 fdatasync；数据页可旧 ⇒ 重启从 manifest 续放补齐（I2/I3） |
| 机器掉电（durability=process） | 不保证已提交事务不丢（与现状一致，文档明示此边界） |
| 旧段误删 | 删除条件 = checkpoint 物化完成 ∧ 无 Phase4 引用；恢复中页面一律从 checkpoint 后取（§5.1） |
| Undo 期间新日志到达 | PauseReplay 阻塞重放；写入端不受影响，恢复后重放追平（I3） |

### 6.3 与现有已实施修复的关系

本轮已落地、被本设计沿用而非推翻的部分：
- blink/FSM 逻辑日志全链路（生成→重放→undo）；
- `persist_off_` 断点续放（v2 中演化为 `replayed_addr`）；
- 重放幂等跳过（INSERT/DELETE/UPDATE，v2 中统一为页版本比较）；
- OpenDb 的 PauseReplay/ResumeReplay 与 blink 句柄失效；
- GPLM 对 blink/FSM 页面的 LSN 上报置 0。

---

## 7. 兼容与迁移

1. **双格式识别**：启动时若存在旧版 `LOG_FILE`（无 `log_v2/` 目录），用 LegacyReader 将其**完整重放完毕后**，做一次初始 checkpoint 物化数据，随后切换到 v2 段式写入；
2. **滚动升级**：存储层先升级（兼容读旧格式），计算端后升级（写 v2 格式）；升级窗口内存储层同时接受两种 RPC 载荷（以首字节 magic 区分）；
3. **回退**：v2 日志无法被旧代码读取 ⇒ 回退前执行一次 checkpoint + open_db 重建即可回到 v1 依赖面。

---

## 8. 实施计划

| 阶段 | 内容 | 依赖 | 状态 |
|------|------|------|------|
| 一：正确性兜底 | append 互斥（C1，`log_manager` append_mtx_）、LogFlush 单写者（C2，`log_flush_mtx_`）、UPDATE 页版本比较跳过（C3）、Undo/Phase4 期间追平+暂停 replay（C4，`WaitReplayCaughtUp` + 递归暂停锁） | 无 | ✅ 已完成（2026-09） |
| 二：格式 v2 核心 | 段式（64MB 滚动）+ 块化 CRC32C（32KB 自描述块）+ manifest 双写选举 + `LogStreamReader` 统一解析层（replayFunV2/RedoForPage(s)/UndoForFailedNode 三处复用）+ legacy 兼容（旧 LOG_FILE 只读续放）；v1 记录布局保持不变 | 阶段一 | ✅ 已完成（2026-09，适配 lazy 模式验证） |
| 二尾：格式 v2 精简 | 批级封装 + varint + 表名消灭（TABLEMAP） | 阶段二 | ⬜ 未做（需动 log_record.h 布局与计算端序列化） |
| 三：恢复加速 | 页前驱链 + 页表/事务表物化 + checkpoint 全流程 + 段回收（manifest.checkpoint_addr 字段已就位） | 阶段二 | ⬜ 未做 |
| 四：增强（可选） | Arena 线格式（E3/E4）、fsync=machine（C6）、并行恢复、blink/FSM 日志合并 | 阶段三 | ⬜ 未做 |

### 阶段一/二验证记录
- 全量编译通过；`fsm_test` 回归全 PASS
- v2 集成验证（临时测试，8 项全 PASS）：自动初始化、20 批写入重放、重启 write_end 精确恢复、重启后全量 1020 条扫描完整、`ReadLogRecordAt` 随机读、恢复位置继续追加、块 CRC 损坏安全截断且写位置回退、manifest 单份撕裂选举容忍
- blink 重放语义验证（临时测试，5 项全 PASS）：open_existing 装载、Redo 幂等含分裂、Undo 删除/重插、崩溃重开恢复、文件缺失优雅处理
- 日志序列化验证（临时测试，5 项全 PASS）：BLINKINSERT/BLINKDELETE/FSMUPDATE 往返、旧格式兼容、混合流解析

每阶段交付物：代码 + 本设计文档对应章节状态更新 + 测试报告。

---

## 9. 测试方案

### 9.1 单元测试
- 记录 serde 往返（含 varint 边界、分片重组、新旧格式兼容）；
- 块 CRC 损坏注入（翻转任意字节 ⇒ 该块被跳过且不污染后续块）；
- manifest 双写选举（单份撕裂 ⇒ 取另一份）；
- 段滚动边界（记录跨段、checkpoint 落在段边界）。

### 9.2 故障注入（沿用 `scripts/recovery/test_recovery.sh` 框架）
- kill -9 计算节点（redo/undo 正确性，blink/FSM 一致性）；
- kill -9 存储层后重启（断点续放、页表重建、open_db 重建与增量重放叠加）；
- 写撕裂注入（拦截 write 只写一半）；
- durability=machine 下的断电模拟（fdatasync 后拔电语义用 `kill -9 + 文件系统 freeze` 近似）。

### 9.3 一致性校验
- 扩展 `scripts/recovery/verify_consistency.py`：heap 全量扫描的主键集合 vs blink 索引全量遍历的 key 集合必须相等；FSM 报告的 free_space 与 bitmap 实际空闲槽换算值的类别必须一致。

### 9.4 性能回归
- 正常路径吞吐（group commit 频率不变，v2 体积下降应带来正收益）；
- 恢复时间：以 `scripts/recovery/analyze_recovery_time.py` 为基准，目标 = 随 checkpoint 间隔线性可控；
- checkpoint 停顿：P99 < 50ms（物化页表 + manifest）。

---

## 10. 附录：v2 格式常量与伪代码

```
SEG_SIZE        = 64MB
BLOCK_SIZE      = 32KB
MANI_MAGIC      = 0x4D414E49
LSEG_MAGIC      = 0x4C534547

enum RecType : uint8_t {
  BATCH = 1,          // 数据批次（内含 TxnChunk）
  TABLEMAP,           // table_id → name 映射
  PAGEIDX,            // 页表快照块
  TXNIDX,             // 活跃事务表快照块
  SEGMENT_SEAL        // 段密封标记（滚动时写入）
};

enum DataRecType : uint8_t {
  INSERT = 1, UPDATE, DELETE, NEWPAGE,
  BLINKINSERT, BLINKDELETE, FSMUPDATE
};

// 重放应用规则（唯一实现，LogStreamReader 之上）
Apply(Rec r, Page& p):
  if r 是页面日志:   if r.page_ver > p.disk_ver { p.apply(r); p.disk_ver = r.page_ver; }
  if r 是 BLINKINSERT: blink_handle(r.page_ref).insert_entry(r.key, r.rid)   // 幂等
  if r 是 BLINKDELETE: blink_handle(r.page_ref).remove_entry(r.key)          // 幂等
  if r 是 FSMUPDATE:   ApplyFsmUpdate(r.page_ref, r.heap_page, r.free_space) // 收敛
```

---

## 附：术语

| 术语 | 含义 |
|------|------|
| LogAddr | 全局日志地址 = seg_id + 段内偏移 |
| page_ver | 页版本号（节点内单调），v1 中的 `lsn_` |
| 页前驱链 | 同页面日志按 prev_page_addr 串成的反向链 |
| redo-all | 未提交事务日志也物化到数据文件的模型（本系统现有模型） |
| PAGEIDX/TXNIDX | checkpoint 时物化的页表/活跃事务表快照块 |
