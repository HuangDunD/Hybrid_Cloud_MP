# Undo / MVCC P7 整体验收报告

更新：2026-11-19（Asia/Shanghai）。验证环境：AliYun1，同机 loopback；源码基线 `09fed8e0f4f9eb1820e7a856d95f2e4de466ea71`。本报告记录 P7 当前范围的实际实现与运行结果，不把未运行项写成通过。P6 的 GC、历史链截断、事务状态回收、undo 页/槽复用按 2026-11-19 范围调整延期；本轮采用追加保留，不把空间回收列为通过条件。

## 1. 现场、同步和构建

- 本地仓库：`/Users/noctis/Documents/项目/Hybrid_Cloud_MP`，分支 `master`，HEAD `09fed8e`；50 个暂存文件、39 个已跟踪未暂存文件、21 个未跟踪文件（有重叠；新增本报告后未跟踪为 22）。未 reset、未提交、未推送，已有改动全部保留。
- 远端原 Git 工作区：`/data/workspace/Hybrid_Cloud_MP`，HEAD 同为 `09fed8e`。本轮把当前完整工作树同步到该目录，源码同步后 `git status --short --untracked-files=all` 为 88 项，新增本报告后为 89 项；关键修复文件已存在。同步前备份为 `/tmp/hybrid-original-before-p7.tgz`。
- 独立验证副本：`/data/workspace/mvcc-p7-accept-20261119.9f6e6c`，从空 `build/` 构建，不复用旧二进制。P6 的旧验证目录没有覆盖。
- 环境：GCC 11.4.0、CMake 3.22.1、protoc 3.12.4、brpc 1.9.0；Debug (`-O0`)。根 CMake 在 configure 阶段生成 protobuf，因此 `storage_service.proto`/`twoPC.proto` 改动后先重新 configure。

```bash
cd /data/workspace/mvcc-p7-accept-20261119.9f6e6c
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug   # exit 0
cmake --build build -j8                         # exit 0
```

## 2. 修复窗口的静态改动及本轮复核

修复窗口对本地工作树做了 14 项静态修正，本轮逐项编译、运行并确认调用点：

1. `UpdateHistory::Install/Retain` 写入现有 UPDATE flags；安装新版本才强制同 writer undo 依赖，Release/回滚/交接可保留另一 writer 的历史引用。`LogReplay::CheckHistory` 对 Install 校验完整 creator 身份、RID/key、引用 epoch 和同流 LSN；存储端同一 `UpdateLogRecord` 解析。`Install` 调用点在 `InstallMvcc` 和 `Install2pcMvccWrite`。
2. 2PC/mix 的 home 等全部参与者显式 Release 返回后，才通过 `FinishTx` 发布终态；重复 Prepare/Commit/Release 等待首次结果；多参与者 Commit/Release 并发发送；`CompleteTwoPhase` 用 bthread 条件变量等待 SCN 发布。
3. `Fetch2pcMvccBatch` 让并行工作线程只返回字节/结果，DTX 线程统一登记行锁、别名和业务回调；同批冲突仍清理已取得锁。连续探测 NOTFOUND 状态复位；TPC-C Payment district YTD 和 Delivery 订单行配送时间修正。
4. undo 首次取锁失败归还预留、页号耗尽返回空引用且不安装主行、小缓冲池全帧占用等待；`CheckLastUndoPage`、`CheckPinnedFrameWait` 覆盖。

上述改动此前只做静态核对。本轮在原 Git 工作区同步后从空构建，组件、单节点、两节点、负载和基线均重新运行；不是旧二进制结果。

### 2.1 P7 后追加：无需手写 mvcc_epoch

此前 `mvcc_epoch` 为空会启动失败。现在分两种模式：

- 显式配置 16 字节 `mvcc_epoch`：所有节点使用该值，测试脚本用这种方式保证可重复；
- 配置留空：compute 节点启动时从 `remote_server` 的 `timestamp_service.GetEpoch` 取得同一随机 16 字节运行身份，`remote_server` 每次进程启动生成一次。显式值优先；compute 在 remote 未就绪时重试约 3 秒后给出明确错误。

随机值由 remote_server 的 `TimeStampServiceImpl` 生成 16 个十六进制字符，仅作为运行身份，不是 SCN，也不是安全密钥。新增 `--auto-epoch` 测试入口；两节点 YCSB Lazy/2PC/mix 各 50 次均通过，node0 与 node1 的 `mvcc_epoch` 分别同为 `fb49cf066e40b42e`、`78292fec2d3a8663`、`f6b144d1e43986c5`。显式 epoch 的 `p5-2pc` 138 项和 `p5-mix` 154 项仍通过。启动顺序仍是 remote_server → storage_server → compute；留空模式不能绕过这个顺序。

### 2.2 P7 后追加：SQL 模式自动初始化 YCSB 表

SQL 模式节点 0 在首次启动、且 `ycsb_user_table` 不存在时，自动创建并插入 1000 行：

```text
id          PRIMARY_KEY
magic       INT              (125)
file_0..9   STRING           (char(90))
```

`char(90)` 是受 SQL `RM_MAX_RECORD_SIZE=1080` 和 `DataItem` 头限制；非 SQL 的 YCSB 负载仍使用 10×100 字节布局。初始化由节点 0 的本地 SQL 客户端执行，日志为：

```text
[SQL] bootstrapping ycsb_user_table: 1000 rows
[SQL] initialized ycsb_user_table with 1000 rows
```

如果表已存在则只打印 `[SQL] ycsb_user_table already exists; skip bootstrap`，不会重复插入。节点 1 不执行 bootstrap，通过 `NotifyCreateTable` 获得表元数据。

实测（单节点 SQL，`test_db`）：

- `show tables;` 返回 `ycsb_user_table`；
- `desc ycsb_user_table;` 返回 `id PRIMARY_KEY`、`magic INT`、`file_0..9 STRING`；
- `select * from ycsb_user_table;` 返回 `Total record(s): 1000`，最后一行 id=999、file_9=`ycsb_999_9`；
- 两节点 SQL 启动时节点 0 bootstrap 成功，节点 1 完成通知；查询 `where id=...` 会经过现有 SQL `BPTreeScanExecutor::Next` 的未实现路径并断言，这是既有 SQL 扫描限制，不由本初始化引入；`select *` 全表扫描正常。

### 2.3 P7 后追加：undo 耗时统计

`result.txt` 新增以下字段（全部为 ns 或次数，`undo_append/read/seal` 的区间可能包含 `undo_fetch`，不要重复相加）：

```text
undo_append_count / undo_append_ns   # Manager::Append
undo_read_count   / undo_read_ns     # Manager::Read + ReadRollback
undo_seal_count   / undo_seal_ns     # Manager::Finish
undo_fetch_count  / undo_fetch_ns    # ComputeServer::FetchUndoPage（包含在 append/read/seal 内）
undo_wal_waits    / undo_wal_wait_ns # undo 页等待 WAL 的次数/时间（包含在 fetch 内）
```

两节点、Lazy、YCSB、每节点 50 次尝试、自动 epoch 的实测：

| 节点 | 提交 | append | read | seal | fetch（子集） | WAL wait（子集） | append+read+seal | 平均/提交 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| node0 | 75 | 421 次 / 52.96 ms | 82 次 / 7.64 ms | 100 次 / 7.25 ms | 679 次 / 17.57 ms | 117 次 / 2.22 ms | 67.85 ms | 0.90 ms |
| node1 | 68 | 411 次 / 51.96 ms | 99 次 / 7.64 ms | 100 次 / 7.19 ms | 683 次 / 16.52 ms | 115 次 / 2.73 ms | 66.79 ms | 0.98 ms |

`mvcc_visible_ns` 仍是可见性/状态解析的时间，不能替代 undo 写入时间；上面的 append/read/seal 才是本轮新增的 undo 操作计时。

## 3. 确定性正确性场景

### 3.1 Lazy 单节点

`run_lazy_mvcc.py` 不带 `--workload` 时运行 `lazy_mvcc_test`，使用真实 storage/remote/compute 进程；三负载各退出码 0：

```bash
for b in ycsb smallbank tpcc; do
  python3 tests/integration/run_lazy_mvcc.py --source . \
    --output validation/lazy-unit-$b --bench $b
done
```

每个负载的 compute 日志均通过：

- `PASS real DTX: fixed 100/90/80 snapshots, uncommitted self-read, equality, SI conflict`
- `PASS real DTX: lock-only creator/history unchanged, private self-read/abort/writeback`
- `PASS real DTX: 40 overwrites, small undo LRU, main eviction/refetch, reverse abort`
- `PASS real DTX: visible tombstone stops, old snapshot retains RID/index/history`
- `PASS lazy_mvcc_test <bench>`

### 3.2 2PC / mix / Cornus 两节点

```bash
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-2pc --mode 2 --variant 2pc
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-mix --mode 4 --variant 2pc
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-cornus-opt --mode 2 --variant 2pc-cornus-opt
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-cornus --mode 2 --variant 2pc-cornus
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-2pc-undo1 --mode 2 --variant 2pc --undo-pages 1
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-mix-undo1 --mode 4 --variant 2pc --undo-pages 1
```

| 运行 | 检查数 | 结果 | 进程退出 |
|---|---:|---|---|
| `p5-2pc` | 138 | passed | node0/node1/storage/remote 全 0 |
| `p5-mix` | 154 | passed | 全 0 |
| `p5-cornus-opt` | 138 | passed | 全 0 |
| `p5-cornus` | 138 | passed | 全 0 |
| `p5-2pc-undo1` | 139 | passed | 全 0 |
| `p5-mix-undo1` | 155 | passed | 全 0 |

确定性场景逐项对应本轮验收要求：

| 验收项 | 状态 | 证据 |
|---|---|---|
| 固定快照、重复读旧值/新值 | 通过 | 单节点三负载；`p5-*` 的 `before_first`/`after_first` 检查 |
| `commit_scn == snapshot_scn` 的已提交版本可见 | 通过 | 单节点 equality；`p5-*` 的 `post['snapshot'] == tomb_scn` 和新快照读取 |
| 未提交写入、自读 | 通过 | 单节点 private self-read；`p5-*` 的 private write/abort 检查 |
| SI 写冲突不覆盖新提交 | 通过 | 单节点 SI；`p5-*` 的 stale writer `CONFLICT` 和旧值保持 |
| 重复更新、完整 undo 链、Abort 逆序恢复 | 通过 | 单节点 40 overwrites + reverse abort（三负载）；`p5-*` 的重复 2PC overwrite/abort |
| 墓碑：旧快照读旧值、新快照 NotFound、可见墓碑停止 | 通过 | 单节点 tombstone；`p5-*` 的 DELETE + 旧快照/新快照检查 |
| 多参与者原子可见、半安装不可见 | 通过 | `p5-*` 的 install gate：参与者安装后、发布前，全体旧快照仍读旧值，后续写者不能穿过未发布行 |
| SCN 发布：低位置未完成、高位置先到、缺 ACK、WAL 未完成、A 提交后 B Begin | 通过 | `mvcc_protocol_test` 的 64 次 barrier Begin/Advance、8 并发 Reserve、反向完成；`p5-*` 的暂停 participant WAL、held Stage ACK、发布后 Begin |

## 4. 页面、undo 与 redo

```bash
python3 tests/integration/run_lazy_mvcc_cluster.py --source . --output validation/p4
```

- P4 两节点 Lazy：57 项检查，passed，node0/node1/storage/remote 全 0。覆盖主数据页/undo 页跨节点取页、S/X 权限交接、所有权转移、缓存淘汰后旧快照读历史、写事务继续/回滚。
- P5 `undo1` 用 1 帧 undo 缓冲池强制淘汰：`undo_evictions` 为 44/8（2PC）和 48/9（mix），旧快照仍重建 10 个版本；`CLEAR_UNDO` + `EVICT_MAIN` 后仍通过。两节点 Lazy 性能运行中 `undo_remote_fetches=103/111`、`undo_transfers` 约 1.7 万，证明跨节点 undo 获取在负载中发生。
- 持久化 redo/undo 重建：`undo_storage_test`（普通与 NDEBUG 目标）退出 0，包含 `CheckReferenceReplay`、`CheckOutOfOrderReplay`、`CheckLastUndoPage`；验证同一流顺序、storage RPC 等待、重复回放幂等、不可变记录、页号耗尽、全帧等待。`prepared_2pc_test` 还覆盖 `partial tail`、重启和 v1/v2 PREPARED 回放。
- 主行先可用而 undo 未回放：P2 测试用异步 storage `GetUndoPage` 等待，确认读取在 undo 页可读前不返回；主行 redo 在 undo 之后才应用。
- 长快照：P5 中一个旧快照跨 10 次连续提交并清空 main/undo cache 后仍读固定版本；单节点 40 次覆盖同样保留。**没有 GC/历史截断/undo 页复用**，因此旧版本和事务终态一直保留。
- 受控容量：P5 场景 16 在运行时把每节点容量设到只剩一个 admission slot。新写事务被拒绝（`FULL/CONFLICT/ABORTING`），`capacity_rejects` 增加，`undo_pages` 不增加，行不被覆盖；已经在途的事务仍可完成并提交（2PC 和 mix Lazy 两条路径）。这是受控测试 seam；生产配置字段 `undo_capacity_pages` 已接入 Manager，但没有再用独立长负载单独验收。没有填满机器真实磁盘。

## 5. 负载与模式矩阵

两节点命令模板（每轮 fresh data/epoch/端口，独立 output）：

```bash
python3 tests/integration/run_mvcc_multinode_workload.py --source . \
  --output validation/<name> --bench <ycsb|smallbank|tpcc> \
  --system <lazy|2pc|mix> --attempts 100 --local-ratio 0.5 --nodes 2
```

单节点用 `--nodes 1 --local-ratio 1`；Cornus 用 `--variant 2pc-cornus` 或 `2pc-cornus-opt`。单节点 Lazy 另由上面的 `run_lazy_mvcc.py --workload`（三负载）覆盖。

| 负载 | 模式 | 节点 | 状态 | 关键路径计数（node0/node1） |
|---|---|---|---:|---|
| YCSB | Lazy | 2 | 通过 | lazy commit 144/139；单写 144/139 |
| SmallBank | Lazy | 2 | 通过 | lazy commit 200/198 |
| TPC-C | Lazy | 2 | 通过 | lazy commit 185/194；单写 94/88 |
| YCSB | 2PC | 2 | 通过 | 2PC commit 132/136；multi 47/55；single-local 84/79；single-remote 1/2 |
| SmallBank | 2PC | 2 | 通过 | 2PC commit 200/198；single-remote 5/3 |
| TPC-C | 2PC | 2 | 通过 | 2PC commit 189/186；multi 25/80；single-local 73/0 |
| YCSB | mix | 2 | 通过 | mix_2pc 135/126；multi 53/53；Lazy 0（热点选择 2PC） |
| SmallBank | mix | 2 | 通过 | mix_lazy 49/50 + mix_2pc 150/148；两分支均出现 |
| TPC-C | mix | 2 | 通过 | mix_2pc 189/191；multi 25/85；Lazy 0 |
| YCSB | 2PC | 1 | 通过 | 2PC commit 154；single-local 154；无 remote/multi |
| SmallBank | 2PC | 1 | 通过 | 2PC commit 200 |
| TPC-C | 2PC | 1 | 通过 | 2PC commit 195；single-local 105；无 remote/multi |
| YCSB | mix | 1 | 通过 | mix_2pc 152；single-local 152 |
| SmallBank | mix | 1 | 通过 | mix_lazy 70 + mix_2pc 130 |
| TPC-C | mix | 1 | 通过 | mix_2pc 195；single-local 105 |
| YCSB | 2PC-Cornus v1 | 2 | 通过 | 2PC commit 123/132；multi 42/55 |
| YCSB | 2PC-Cornus v2 | 2 | 通过 | 2PC commit 125/114；multi 48/52 |
| TPC-C | 2PC-Cornus v1 | 2 | 通过 | 2PC commit 188/184；multi 22/78 |
| TPC-C | 2PC-Cornus v2 | 2 | 通过 | 2PC commit 185/185；multi 20/79 |

所有 19 个矩阵运行的 `result.json` 均为 `passed: true`，node0/node1/storage/remote 退出码全部 0。运行配置：YCSB 2000 记录/100 热点，SmallBank 2000/100，TPC-C warehouse=2、district=5、customer=100、item=1000、stock=1000；每节点 2 worker、`attempts=100`（即每节点约 200 次尝试）；`generate_log=1`、`lock_mode=1`、`hybrid_cost_model.enabled=0`、`partition_size_per_table=8`、table pool 16、undo pool 64。

覆盖说明：

- 2PC 零写、单本地写、单远端写和多写：零写/单本地/单远端/多写由 `p5-2pc` 确定性场景分别覆盖；两节点 YCSB/SmallBank/TPC-C 负载观察到 single-local、single-remote 与 multi-writer 计数。
- mix 的 Lazy 腿在 `p5-mix` 被强制覆盖；负载中 YCSB/TPC-C 因热点选择全部走 2PC，SmallBank 两腿均出现。没有改动 mix 调度策略。
- 普通 2PC、Cornus v1（每 writer Prepare LSN 证据）、Cornus v2（存储 AllPrepared）均有确定性和负载运行。
- 单节点只可能单本地写；单远端/多写只在两节点运行覆盖。**没有跨物理机、3+ 节点或 HA 验证，不能把两节点 loopback 结果外推。**

## 6. 性能观察

### 6.1 条件

- 同机 loopback，Debug `-O0`，每节点 2 worker，相同数据量/分区/日志配置；不是生产性能测试。
- YCSB：`attempts=500`/线程（每节点约 1000），2000 记录、100 热点，`local_ratio=0.5`；SmallBank：`attempts=500`，2000 账户/100 热点；TPC-C：`attempts=200`（每节点约 400），上述表规模。
- 单节点 `local_ratio=1`；两节点 `local_ratio=0.5`。集群 TPS 为两节点 `benchmark_committed_tps` 之和；单节点为其本身。
- “成功事务平均延迟”用 `stats_txn_committed_ns / commits`；“commit 阶段平均”用 `stats_txn_commit_ns / commits`；“可见性解析平均”用 `mvcc_visible_ns / mvcc_visible_reads`，只覆盖版本解析（含 undo 读取），不是完整页面 I/O 的 per-op 读延迟。

### 6.2 绝对结果

| 运行 | 尝试/节点 | 提交/中止 | 集群 TPS | 成功事务平均 | commit 阶段平均 | 可见性解析平均 | SCN 等待平均 | WAL 字节 / flush | undo pages | prepared contexts |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2PC YCSB 2n | 1000 | 1302 / 698 | 648.3 | 5.23 ms | 3.83 ms | 5.70 µs | 291 µs | 30.9 MB / 3487 | 2754 | 1076 |
| Lazy YCSB 2n | 1000 | 1398 / 602 | 469.0 | 5.16 ms | 1.27 ms | 14.9 µs | 247 µs | 33.2 MB / 5184 | 3407 | 0 |
| mix YCSB 2n | 1000 | 1314 / 686 | 461.4 | 7.36 ms | 4.76 ms | 5.72 µs | 296 µs | 31.3 MB / 5016 | 2769 | 1098 |
| 2PC YCSB 1n | 1000 | 729 / 271 | 450.5 | 3.91 ms | 2.77 ms | 4.25 µs | 65 µs | 15.2 MB / 1498 | 1448 | 0 |
| mix YCSB 1n | 1000 | 736 / 264 | 459.7 | 3.80 ms | 2.45 ms | 4.36 µs | 64 µs | 15.4 MB / 2418 | 1458 | 0 |
| 2PC TPC-C 2n | 400 | 729 / 71 | 647.6 | 5.86 ms | 2.24 ms | 0.98 µs | 284 µs | 6.4 MB / 1479 | 756 | 424 |
| 2PC SmallBank 2n | 1000 | 1982 / 18 | 7153.3 | 0.320 ms | 0.320 ms | 2.03 µs | 250 µs | 1.1 MB / 465 | 320 | 0 |
| 基线 2PC YCSB 2n | 1000 | 1481 / 519 | 794.6 | 4.15 ms | 2.77 ms | 无此计数器 | 无 | 未计数 | 无 | 无 |
| 基线 2PC SmallBank 2n | 1000 | 1997 / 3 | 10154.7 | 0.382 ms | 0.282 ms | 无此计数器 | 无 | 未计数 | 无 | 无 |

基线为 HEAD `09fed8e` 的旧 2PC，没有 SCN/MVCC/undo，不执行固定快照和 SI；它和本轮不是语义等价的性能基线。上表只用于记录绝对量和条件，**不计算提升/下降比例，也不把基线 TPS 当成新实现的性能回归结论**。没有为 Lazy/mix/TPC-C 构造可比的旧基线（旧 Lazy 不是同一语义，旧 TPC-C 两节点还存在页 0 路由问题）。

### 6.3 冲突、WAL 与增长

- 2PC YCSB 2n：SI 冲突 95、锁冲突 603，恰好覆盖 698 次中止；2PC YCSB 1n：39 + 232 = 271；Lazy YCSB 2n：SI 598（其余 4 次非此二类），锁冲突 0；mix YCSB 2n：155 + 531 = 686。TPC-C 2n：10 + 61 = 71；SmallBank 2n：1 + 17 = 18。锁等待/死锁组件测试 `lock_wait_test` 退出 0；负载结果没有独立的死锁计数，未观察到死锁。
- 2PC 多写 Prepare 采样（两节点合计）：YCSB 538 个样本，prepare all 约 2.05 ms/事务；TPC-C 212 个样本，约 2.08 ms；decision log 约 0.5–0.6 ms，final commit 约 0.7–1.7 ms。SCN 等待与 prepare/安装分开记录；`scn_wait` 只统计有写提交的事务。
- WAL 字节是 `LogFlush` 实际序列化字节；flush 次数包含各参与者/协调点。2PC YCSB 2n 约 23.7 KB/提交，Lazy 约 23.8 KB，mix 约 23.8 KB。SmallBank/TPC-C 因每事务写集更小，绝对字节更低。
- 追加保留的增长：2PC YCSB 2n 约 1000 次尝试/节点后每节点 undo 1413/1341 页，prepared contexts 538/538，single writes 404/360，finished participants 441/445；Lazy 每节点 undo 1716/1691 页；mix 1413/1356 页 + 549/549 prepared。**没有 GC/事务状态回收**，这些量随运行线性增长；本轮只验证到上述规模，未给出长期运行上界。`undo_capacity_pages` 可在配置容量耗尽时拒绝新写，但不回收旧页。

## 7. 对照设计文档当前验收范围

| 设计/计划要求 | 状态 | 说明 |
|---|---|---|
| Lazy、2PC、mix 统一固定快照、事务身份、commit_scn 和提交结果 | 通过 | P5 复核 + P7 矩阵；跨节点两节点 loopback |
| 零/单本地/单远端/多写参与者；普通 2PC、Cornus 实际分支 | 通过 | P5 确定性 + 两节点负载；单节点不包括 remote/multi |
| Prepare/undo/WAL、发布后交锁、Abort 结果 | 通过 | P5 138/154 检查，P3 回滚，P2 回放 |
| 主行/undo 页跨节点、迁移、淘汰、清除缓存后历史 | 通过 | P4 57 检查；P5 undo1；P3 40 覆盖 |
| 从持久化 redo 重建、重复回放、未完成日志尾、主行/undo 依赖 | 通过 | P2 undo_storage；prepared partial tail |
| 长快照追加保留 | 通过 | P3 40 次覆盖；P5 10 次提交 + cache 清除 |
| 受控容量拒绝新写、旧 undo 不覆盖、在途完成 | 通过 | P5 场景 16（测试 seam） |
| 三类负载 × Lazy/2PC/mix 单/多节点 | 通过 | 19 个矩阵运行；单节点 Lazy 由 P3 覆盖 |
| 性能观察与增长 | 已完成（无等价基线） | 绝对 TPS/延迟/WAL/增长已记录；基线不语义等价 |
| P6 GC、历史截断、状态回收、undo 页/槽复用 | 延期，不列为缺口 | 2026-11-19 范围调整；追加保留 |

## 8. 未运行、未覆盖和限制

- **未运行**：跨物理机、3+ 计算节点、网络分区、协调点故障/HA、完整崩溃决议恢复、重启后继续业务、真实磁盘填满或生产容量压力；未做网络层重复 Prepare/Commit/Release 注入（注册表语义由 `prepared_2pc_test` 覆盖）。
- **未做**：P6 的 GC、历史截断、事务状态回收、undo 页/槽复用；旧历史和终态按追加保留。长跑空间上界未测。
- **性能**：没有语义等价的改造前基线；旧 2PC 基线不含 MVCC/SI/undo，且中止数不同，不能据此计算提升/下降比例。没有 per-op 读延迟百分位、CPU/内存曲线或跨机网络成本。undo 计时只覆盖正常运行的 append/read/seal/fetch/WAL 等待，不覆盖存储端回放。
- **SQL 初始化**：只验证单节点完整查询和两节点 bootstrap/建表通知；按主键 `where` 的扫描仍走既有未实现的 BPTreeScan 路径，不属于本功能修复范围。
- **删除标记**：单节点和 P5 墓碑场景直接断言旧快照、新快照、deleted 标志和停止链；TPC-C 负载执行现有 Delivery `valid=0` 路径并整体通过，但没有单独的“本次运行删除行数”计数器。
- **容量**：P5 受控 seam 通过；生产配置字段已接入，但没有单独用配置启动的长负载，也没有 GC 后的容量恢复。
- **死锁**：`lock_wait_test` 通过；负载结果没有单独死锁指标，不能给出死锁比例。
- **两节点 loopback 不是跨物理机/更多节点的证明**；单节点没有 remote/multi 写者。

## 9. 结论

本轮 P7 当前范围（Lazy/2PC/mix 的正常运行路径整体验收、正确性修复、单/两节点三类负载、受控容量、页面/undo/redo、性能观察）在该环境实际通过。P5 的退出条件重新复核通过；P6 按范围调整延期，不作为本轮缺口；旧历史和所需事务状态保持可读、不可回收。跨物理机、3+ 节点、HA、完整崩溃恢复、网络重复注入、长期空间增长和与旧系统的等价性能比较仍未验证，不能由本报告外推。

结果目录：`/data/workspace/mvcc-p7-accept-20261119.9f6e6c/validation/`；基线目录：`/data/workspace/mvcc-p7-baseline-20261119.28ac93`；原 Git 工作区：`/data/workspace/Hybrid_Cloud_MP`。未提交、未推送。
