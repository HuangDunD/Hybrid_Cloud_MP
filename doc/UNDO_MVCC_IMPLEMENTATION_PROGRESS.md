# Undo / MVCC 实施交接

## SQL Lazy 快照隔离验收（2026-11-19）

**本轮 SQL MVCC 退出条件已达到。** AliYun1 同机单计算节点的真实 SQL 多连接验收通过 172 项，双计算节点通过 203 项（含原有 SQL 回归）。另有双节点写者等待 8 项、单节点受控容量 4 项通过。四个测试驱动均退出 0。下文 P7 及更早章节保留原阶段的范围与结果。

### SQL 已接通的路径与行为

- `ConfigureComputeNodeRunSQL → StartDatabaseSQL → RunSQL` 为每个连接管理 DTX。显式 BEGIN 在返回应答前取得安全 SCN 并登记活跃快照。普通自动提交语句各用一个事务，远端取页沿用原快照。成功提交、主动回滚、解析或执行出错、SI 冲突及连接 EOF 都经过同一收尾路径，下一笔事务重新开始。初始化客户端创建的 1000 行也经真实 BEGIN、INSERT 和 COMMIT。
- `UsesMvcc` 控制共用的发布服务、undo 页空间、快照、WAL 和存储回放。`IsMvccWorkload` 继续控制 benchmark 执行与调度。SQL 自己的目录、规划器、BLink/FSM 和执行器按动态表名与表 ID 工作。只读事务经已有 `ReadOnlyFinished` 结束。
- `Scan → SeqScan/BPTree → JOIN/Projection → QlManager` 只将快照可见的逻辑行交给上层。按位图或主键找到 RID 后，在主数据页 S 权限内复制身份、完整业务值、创建者、删除标志和 undo 引用，释放页面后才查状态或历史。普通 SELECT 无行 S 锁、读键登记、读解锁 WAL。WHERE、JOIN、投影、打印及 UPDATE/DELETE 候选收集均使用该副本。已有语法用 `FROM a JOIN b WHERE ...` 表达连接条件。
- `InsertExecutor / UpdateExecutor / DeleteExecutor → DTX::SqlWrite → FetchMvccWrite / InstallMvcc` 在取得行写权限后检查快照冲突。每次实际覆盖将逻辑旧行写入既有 undo，再安装新行并生成主行 WAL。`TxCommitSingleSQL / TxAbortSQL` 复用 `CommitLazy / AbortLazy`，写者等批量 WAL 和提交 SCN 发布，过户后继写者。回滚按事务 undo 链逆序逐次恢复行和历史引用，保留当前写权限直到收尾。事务中途遇到冲突也回滚先前已安装的修改。
- 首次 INSERT 在页保护下占位，并为主键建立唯一索引入口。版本链起点是初始墓碑，首次可见值带真实 creator 和指向该墓碑的 undo。可见墓碑表示无行，其余非初始版本必须有有效前驱。提交 DELETE、插入后回滚、插入再删除都保留已占用的 bitmap 槽及对应 BLink 入口，FSM 不返还该空间。同键重插沿原 RID 的链追加新版本。无主键行以物理页号和槽号组成内部键。
- CREATE/DROP 使用独立自动提交命令。各计算节点按成员顺序取得 DDL 互斥保护。任一节点有活跃 SQL 事务（包括仅执行过 BEGIN）就向 DDL 客户端返回 busy。显式事务内执行 DDL 返回错误并回滚。DROP 在清缓存、关闭文件、复用表名或 ID 前排空旧表 WAL 回放，清除旧的文件描述符和页进度。节点收到建表通知时核对目录中的表身份。

### AliYun1 隔离部署与复核命令

本地工作树及远端原项目 `/data/workspace/Hybrid_Cloud_MP` 的基线均为 `09fed8e`。本轮在本地既有暂存和未暂存改动上实施。SSH 配置使用区分大小写的别名 `AliYun1`。原项目仅查询路径及 HEAD。独立副本是 `/data/workspace/sql-mvcc-20261119.zoQkyD`，使用系统 brpc。各次运行新建自己的 `validation/<运行名>/run/storage/sql_mvcc_db` 和配置，测试脚本拒绝覆盖已存在的输出目录。

```bash
# 本地仓库根目录执行。下面是本次已创建的远端目录。
ssh AliYun1 'ls -d /data/workspace/Hybrid_Cloud_MP && git -C /data/workspace/Hybrid_Cloud_MP rev-parse HEAD'
rsync -az --exclude=.git --exclude=.DS_Store --exclude=thirdparty/brpc/ \
  --exclude='*.pb.cc' --exclude='*.pb.h' ./ AliYun1:/data/workspace/sql-mvcc-20261119.zoQkyD/

# 以下在 AliYun1 的独立副本执行。再次运行测试时给 --output 新目录名。
cd /data/workspace/sql-mvcc-20261119.zoQkyD
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS_DEBUG=-g1
cmake --build build -j8
sha256sum -c proto.after
python3 tests/integration/run_sql_mvcc.py --source . --nodes 1 --output validation/sql-accept-single
python3 tests/integration/run_sql_mvcc.py --source . --nodes 2 --regressions --output validation/sql-accept-dual
python3 tests/integration/run_sql_mvcc.py --source . --nodes 2 --wait-writers --output validation/final-sql-wait
python3 tests/integration/run_sql_mvcc.py --source . --nodes 1 --capacity --output validation/final-sql-capacity
```

根配置、构建、protobuf 摘要核对均退出 0。配置前 `proto.before` 为空。根 CMake 对八份 proto 生成十六个 `.pb.cc/.pb.h` 文件，最终重新核对十六份 SHA-256 全部一致（`proto.after`、`proto.verified`）。构建日志在 `configure.log` 和 `build-release-check.log`。本地 Git 不跟踪这些生成文件，本轮未将远端生成物带回工作树。

| 独立运行 | SQL 端口 | 真实客户端验证 |
|---|---|---|
| `sql-accept-single` | 52487 | 172 项。覆盖显式/自动提交、首次读时刻、自读、多次写、整笔回滚、语法与类型错误、断连回滚、最宽行和磁盘回放 |
| `sql-accept-dual` | 38459、38460 | 203 项。A 写 B 读、远端旧快照与新快照、两端竞争同键，另运行原生 CLI 和并发/缓存客户端 |
| `final-sql-wait` | 43513、43514 | 8 项。在 WAIT_DETECT 下先观察持有者的写者队列由 0 到 1，第三个连接读到 125，持有者提交 80，等待者收到 SI 写冲突，下一笔读到 80 |
| `final-sql-capacity` | 54481 | 4 项。初始化事务获准进入并使用 334 个 undo 页。此后 UPDATE、INSERT 返回容量不足，原值仍为 125、新键不存在。WAL LSN 保持 3676，undo 页数保持 334 |

每条 SQL 与应答在对应目录的 `sql.log`，实际监听/RPC 端口在 `config/`，进程状态和计数在 `result.json`。测试以完整应答、并发插入 barrier、写者入队和回放完成为交错条件。轮询只等待已定义的状态。SQL 服务在全部断言结束时均存活，驱动随后仅对本次启动的常驻进程发 SIGTERM，记录的服务退出码为 -15。

### 退出条件的直接证据

- R 的 BEGIN 已应答，W 将 100 改为 80。W 自读 80，R 在 W 提交前首次点查得到 100。R 保持打开时 W 提交成功。另一个仅 BEGIN 的读者在提交后才首次读表，仍得到 100。新事务得到 80。顺序扫描、主键点查的剩余谓词和 JOIN 均测试了旧、新匹配集合的双向变化。
- 同键先有值 80，再删除提交，再插入 77。保留的三笔快照用相同主键点查和顺序扫描同时得到 80、零行、77。覆盖删除者自读、插入前无行、插入/删除回滚、同事务插删、并发唯一键和无主键多行。快照后更新或删除的候选均返回 SI 冲突。在多行 UPDATE 后半段冲突时，前半段已安装的写入也被撤销。
- 主数据池每表 4 帧、undo 池 2 帧。双节点测试修改跨页的 20 个宽行并逆序回滚。A 的主数据页淘汰累计数从 363 到 442，undo 页从 343 到 376。B 的旧快照读 125，回滚后的当前值为 80。纯读区间 A 的 WAL LSN / undo 页数保持 4104 / 377，B 保持 3767 / 1。单节点同一纯读区间保持 4105 / 378。
- 存储追平后，`sql_mvcc_replay_check` 直接打开堆文件及连续两条 undo 引用，校验 RID、键、creator、页/槽身份和校验和。当前行的 904 字节业务值全量匹配 80 对应的记录，前一版本全量匹配 125 对应的记录，再前一版本为无前驱的初始墓碑。两种拓扑的校验器均退出 0。
- 双节点 SQL 回归运行 5 份基础查询、7 份并发、5 份跨节点缓存、建表装载和大 JOIN。原生 CLI 接收完整结果，JOIN 全部 350 行含重复次数与同一夹具的 SQLite 结果核对一致。并发客户端用逐命令应答执行指定交错，期望文件已对齐 SI。

共享代码在同一构建上的回归命令如下。三个 P3 测试、三种双节点实际负载、P4 的 57 项、P5 普通 2PC 的 138 项和 mix 的 154 项均通过。每组的驱动及其计算、存储、remote 进程均退出 0。协议、发布 RPC、集群与 undo/replay 的六个组件目标也均退出 0。

```bash
for b in ycsb smallbank tpcc; do
  python3 tests/integration/run_lazy_mvcc.py --source . --bench "$b" --output "validation/final-p3-$b"
  python3 tests/integration/run_mvcc_multinode_workload.py --source . --bench "$b" \
    --system lazy --nodes 2 --attempts 100 --auto-epoch --output "validation/final-load-$b"
done
python3 tests/integration/run_lazy_mvcc_cluster.py --source . --output validation/final-p4
for mode in 2 4; do
  python3 tests/integration/run_twopc_mvcc_cluster.py --source . --mode "$mode" --output "validation/final-p5-$mode"
done
for t in mvcc_protocol_test mvcc_protocol_ndebug_test mvcc_publication_rpc_test \
         undo_storage_test undo_storage_ndebug_test mvcc_cluster_test; do
  "build/tests/$t"
done
```

**格式与验证边界。** SQL 使用既有 RMV2 文件头、MVC2 行头及现有 undo 编码。当前构建的 1080 字节记录上限含 168 字节行头，业务列最多 912 字节，主键另存。验收用仅最后一字节不同的 912 字节值校验当前与历史版本，913 字节建表返回错误。旧 SQL 库缺少有效版本身份时需要新建或迁移，本轮以全新数据验收。运行期间保留历史槽、索引入口、事务终态和 undo 页。SQL 2PC/mix、事务性 DDL、改主键、历史 GC/页复用、跨物理机和 HA/崩溃重启均在本轮验证边界之外。

## 负载 P7 历史范围

更新：2026-11-19（Asia/Shanghai）。当前范围：**P7 整体验收已完成当前环境/当前范围；P6 的 GC、历史链截断、事务状态回收和 undo 页/槽复用按 2026-11-19 范围调整延期**。P5 的 Lazy、普通 2PC、Cornus 两代、mix 的 Lazy/分布式分支已复核通过；P7 又完成单节点 Lazy、两节点三类负载、Cornus、受控容量、页面/undo/redo 和性能观察。完整验收矩阵、命令、性能和未验证项见 [P7 整体验收报告](UNDO_MVCC_P7_ACCEPTANCE.md)。

## P7 验收交接（2026-11-19）

- **状态**：P7 当前范围通过；没有把未运行的跨物理机、3+ 节点、HA/崩溃恢复、网络重复注入或 P6 GC 写成通过。P5 退出条件重新复核通过；P6 延期不构成本轮缺口。
- **工作树**：本地 `09fed8e`，50 暂存/39 未暂存/21 未跟踪（有重叠；新增 P7 报告后未跟踪为 22）；未 reset、未 commit、未 push。修复窗口的 14 项静态改动已并入当前工作树，本轮在从空构建的独立副本实际构建并运行；不是旧二进制结果。
- **修复窗口改动**：WAL 回放用 `UpdateHistory::Install/Retain` 区分新版本和保留历史引用，`CheckHistory` 校验完整 creator 身份；2PC/mix 在全部参与者显式 Release 后才发布 home 终态，重复 Prepare/Commit/Release 等待首次结果；并行取页结果由工作线程返回、DTX 线程统一登记锁/回调；undo 预留归还、页号耗尽、小池全帧等待修复；TPC-C Payment/Delivery 修正。
- **本轮 P7 改动**：`undo_capacity_pages`/`Manager::AdmitNewWrite` 容量准入与拒绝计数；SCN 等待、WAL 字节、可见性读取/历史步数、SI/锁冲突、注册表规模等结果字段；单节点/两节点负载脚本；P7 墓碑、长快照、重复写、容量、撤销/回读场景；工作负载脚本检查 storage/remote 退出。
- **P7 后追加的启动易用性**：`mvcc_epoch` 留空时，compute 节点从 `remote_server` 的 `timestamp_service.GetEpoch` 获取同一个随机 16 字节运行身份；显式 16 字节配置仍优先。`remote_server` 必须先启动；compute 启动失败时重试约 3 秒并给出明确错误。结果文件输出 `mvcc_epoch`，两个计算节点可核对一致。两节点 `--auto-epoch` 的 Lazy/2PC/mix YCSB 各 50 次已通过；显式 epoch 的 `p5-2pc` 138 项、`p5-mix` 154 项仍通过。
- **undo 耗时统计**：`result.txt` 增加 `undo_append_*`、`undo_read_*`、`undo_seal_*`、`undo_fetch_*`、`undo_wal_wait_ns`；Lazy/YCSB 两节点 50 次尝试的 append+read+seal 分别约 67.85/66.79 ms。`undo_fetch`/`undo_wal_wait` 是子集，不能与前三项相加。
- **SQL 自动初始化**：节点 0 在 SQL 模式首次启动、表不存在时创建 `ycsb_user_table`（id 主键、magic、file_0..9 char(90)）并插入 1000 行；已存在则跳过，节点 1 通过 NotifyCreateTable 获取表。单节点 `show tables`/`desc`/`select *` 验证 1000 行；两节点 bootstrap 成功。按主键 `where` 查询仍走既有未实现的 BPTreeScan，属于既有 SQL 限制。
- **确定性结果**：P3 单节点三负载（含 40 次覆盖/逆序 Abort/墓碑/自读/SI）通过；P4 57 项通过；P5 `2pc` 138、mix 154、Cornus v1/v2 各 138、`undo1` 139/155，全部 passed 且四进程 0；组件测试 10 个目标全 0；P2 覆盖 replay/partial tail/CheckLastUndoPage。
- **负载结果**：两节点 Lazy/2PC/mix × YCSB/SmallBank/TPC-C 全部通过；单节点 2PC/mix × 三负载通过；Cornus v1/v2 × YCSB/TPC-C 通过；合计 19 个矩阵运行全 0。1 节点只覆盖单本地写；单远端/多写由两节点确定性/负载覆盖。跨物理机和 3+ 节点未验证。
- **性能**：受控同机 loopback、每节点 2 worker；2PC YCSB 2n 648 TPS，Lazy 469，mix 461；2PC YCSB 1n 451；2PC TPC-C 2n 648；2PC SmallBank 2n 7153。绝对延迟、SCN 等待、WAL 字节/flush、undo/终态增长见报告第 6 节。旧 HEAD 2PC 基线不含 MVCC/SI/undo，**不计算性能提升/下降比例**。
- **未解决/未运行**：P6 GC/截断/状态回收/页复用；跨物理机、3+ 节点、HA、完整崩溃恢复；网络层重复 RPC 注入；长期空间上界；等价性能基线；TPC-C 删除行数独立计数；死锁比例。详见报告第 8 节。
- **P7 退出结论**：三类负载 × Lazy/2PC/mix 的当前环境验收、单/两节点、Cornus、容量受控、页面/undo/redo 与性能观察完成。P6 延期，不把空间回收列为通过条件；本轮目标在当前范围内完成，不外推为生产/跨机保证。



## 本轮基线、工作方式和远端目录（P5）

- 本地仓库 `/Users/noctis/Documents/项目/Hybrid_Cloud_MP`，实际 HEAD 仍为 `09fed8e`；实现位于未提交的工作区/暂存区，未 reset、未回退、未 commit。检查了仓库及祖先 AGENTS.md，没有适用文件。
- 先重新读设计、阶段计划、SCN 协议、P3 说明和 P4 交接，再按源码核实 P1 可见性、P2 undo/redo、P3 单节点 Lazy、P4 两节点 Lazy。**在改动前先用 P4 既有产物 `/data/workspace/mvcc-p4-final-20261118.EGBTUI` 重跑两节点 Lazy 集成测试，退出码 0**，确认前置能力可用：

```bash
cd /data/workspace/mvcc-p4-final-20261118.EGBTUI
rm -rf /tmp/p4-baseline-check && mkdir -p /tmp/p4-baseline-check
python3 tests/integration/run_lazy_mvcc_cluster.py --source .   --output /tmp/p4-baseline-check/multinode
```
- 远端原项目 `/data/workspace/Hybrid_Cloud_MP`（HEAD 同为本提交）先同步全部源码，`git status` 能显示改动；构建与验证使用独立副本 **`/data/workspace/mvcc-p5-20261119.ObCEe1`**，从空 `build/` 全量构建。没有复用旧实验数据。
- 远端环境：GCC 11.4.0、CMake 3.22.1、protoc 3.12.4、`/usr/local` brpc 1.9.0。所有测试使用 `127.0.0.1`、新 epoch、新数据目录与随机端口；没有覆盖已有实验数据。
- **根 CMake 在 configure 阶段调用 protoc**。修改 `core/**/*.proto` 后必须重新执行 `cmake -S . -B build`，否则生成的 `.pb.*` 仍是旧协议；本轮 2PC 协议改动期间实际踩到过这一点，已通过重新 configure 解决。

## P5 实际改动

| 范围 / 文件 | 实现 |
|---|---|
| `core/mvcc/protocol.{h,cc}` | `Plan` 增加 `writers`；区分 SingleWriter / TwoPhase / Cornus 的证据要求；TwoPhase 必须有 home 决议记录，Cornus 必须由每个 writer 的 prepare 前缀或存储端持久 YES 集合（`PreparedQuorum`）证明；`Ready` 要求全部 writer 的 Installed 与 WAL/Prepare/决议证据，发布前不放行；新增 `SnapshotNode::Local` 支持 home 自参与者复用注册；重复 PreparedQuorum 幂等 |
| `core/mvcc/lazy_runtime.{h,cc}` | 新增 `Participant/Export/RemoteFinished/FinishParticipant/SealParticipantUndo`；新增 `ReserveTwoPhase`、`CompleteTwoPhase`、`SubmitTwoPhase`；`CheckWriteRow` 供 2PC 参与者/协调者做 SI 检查；`SubmitCommit` 继续作为 Lazy 快捷路径 |
| `core/mvcc/publication.proto`、`publication_rpc.{h,cc}` | 新增 `TwoPhaseRequest/TwoPhaseReply/TwoPhaseCompleteRequest`、`SubmitTwoPhase`、`ReserveTwoPhase`、`CompleteTwoPhase` 与本地点对点客户端；Coordinator 收齐证据并完成两轮 Stage/Advance 后才返回 committed |
| `core/compute_node/twoPC.proto` | `MvccContext`；GetDataItem 返回 record 字节并带 `not_found`/`mvcc_conflict`；PreparedWrite 带 `undo` Ref 与 `deleted`；Prepare 带 Context、返回 durable_lsn；Commit 带 commit_scn/Context/deleted、返回 durable_lsn/installed；新增 Release（含 `finish_only`、`prepared`、身份）|
| `core/compute_node/prepared_txn.h` | prepared 请求在持久化前填入参与者 undo Ref；状态增加 APPLIED；重复 Commit 核对身份、写集、commit_scn；重复 Release 幂等；新增单写 `SingleWrites` 注册表 |
| `core/dtx/mvcc_2pc.h` | 2PC Context 的 wire 编解码与合法性检查；prepared undo Ref 的非致命结构校验 |
| `core/dtx/dtx.h`、`dtx.cc`、`dtx_mvcc.cc` | SYSTEM_MODE 2/4 的 Benchmark 事务也固定 `mvcc::Snapshot`；`tx_id = TxnId.local`，锁/等待身份使用 `start_ts = (local << 8) | home`，避免不同 home 的本地 id 碰撞污染旧的锁等待表；monitor 两处 Lazy 页释放支持 mix；`PrivateMvcc` 作为 2PC 自读首选 |
| `core/dtx/dtx_exe.cc` | mix 在页访问前用同一成本输入决定 Lazy/2PC，Lazy 腿走 `ExecuteLazy`（调度不变）；2PC 读经参与者统一可见性；写取得锁后在协调者做 SI 检查；mix 本地写回调延迟到放页后；`TxPrepare` 改为按 changed writer 生成带 Context/deleted/undo 占位的请求；`Tx2PCCommit` 统一路由到新 MVCC 提交 |
| `core/dtx/dtx_twopc_mvcc.cc`（新增） | `Commit2PCMvcc`：零/单/多写分支、Prepare 证据、Cornus 决议、Reserve/Complete、发布后 Release、End；`Abort2PCMvcc`：决定前只释放锁和私有副本并封口 undo；自读、SI、private pre-image、锁集合与值/删除标记映射 |
| `compute_server/server.h`、`server.cc`、`twopc_server.cc`、`twopc_fetch_server.cc` | SYSTEM_MODE 2/4 建 LazyRuntime 并注册 PublicationService；ComputeNode 为 2/4 初始化 undo 表/缓冲池；参与者读按固定快照选版本；Prepare 在写锁下保存完整逻辑旧行到本节点 undo 子段并持久 PreparedLogRecord；Commit 用 prepared 旧版本/undo Ref 安装新行且保留行锁；Release 在发布后过户/解锁或安全跳过已被延迟清理的行；单写提交保留 undo 并要求自身 end log 持久；home 侧收集 durable/installed 证据；2PC 页更新同步本地 newest LSN；页 0 文件头路由修正 |
| `core/compute_node.h` | SYSTEM_MODE 2/4 初始化 undo 页锁表与独立缓冲池 |
| `core/mvcc/row.{h,cc}` | `RowImageFromItem` 从 `value` 指针取业务值，修复非连续 heap DataItem 被误读；`CopyRow` 仍按页内连续布局读取 |
| `core/undo/format.{h,cc}` | `ValidRefBytes` 供持久化 Prepared 记录校验 |
| `core/storage/logreplay.cc` | `CheckHistory` 只对新版本 redo 强制 P3 同 writer 撤销依赖；解锁/Release/回滚恢复的 redo 带完整行，允许旧历史 Ref 属于其他 writer |
| `tests/test_cases/lazy_mvcc_cluster_node.cc` | 支持 SYSTEM_MODE 参数、mix FORCE、OWNER/KEY_ON_NODE、参与者 prepare/commit 门控、2PC 页本地淘汰限制；BEGIN 报告 TxnId.local 供 QUERY_STATUS |
| `tests/integration/run_twopc_mvcc_cluster.py`（新增） | 两计算节点 + 存储 + remote 的 P5 确定性集成：零/单本地/单远端/多写、自读、SI 冲突、安装门、WAL 暂停、发布 ACK 门、Abort、锁交接、淘汰后历史、mix 双分支 |
| `tests/integration/run_mvcc_multinode_workload.py`（新增） | 两进程小规模 YCSB/SmallBank/TPC-C，`lazy`/`2pc`/`mix`，可调 local ratio、attempts、variant |
| `tests/test_cases/mvcc_protocol_test.cc`、`prepared_2pc_test.cc` | 增补 writer 集合、Cornus quorum、重复/矛盾 Prepare/Commit/Release、单写幂等测试 |

## P5 已成立的行为（正常运行路径）

1. **统一身份与快照**：home 在 Begin 固定 `Context(TxnId, snapshot_scn)`；所有参与者复制同一 Context，不重新 Begin；`tx_id` 保持逻辑身份，`start_ts` 额外编码 home 仅用于旧锁/等待表；版本 creator 与锁 owner 分离。
2. **执行**：2PC 保留 home 私有副本；自读优先 `PrivateMvcc`；写取得参与者行锁后在协调者做 SI 检查，其他事务在本快照后提交同一行时按正常冲突 Abort/retry；远端读取由参与者按统一可见性沿 undo 链选出历史，不把页中最新的行直接返回。mix 的 Lazy 腿走同一 `ExecuteLazy`，分布式腿走统一参与者协议。
3. **Prepare / undo / WAL**：实际写节点在自己的行写锁下复制完整逻辑旧行，释放主页面 latch 后追加 undo，再重取页面核对旧行未变；把 undo Ref、新值、deleted 写入 PreparedLogRecord 并在本节点 WAL 等待持久化后才返回 YES。Commit 使用 Prepare 保存的旧行/undo Ref，不在 Commit 时按当时页面重新生成另一份旧值。undo redo 与主行 redo 同一 writer 流，不增加 per-undo fsync；普通 2PC 另有 home 决议记录，Cornus v1 使用完整持久 YES 集合，Cornus v2 使用存储端 AllPrepared。
4. **Commit / 发布 / 解锁**：全体必要写项安装完成、Prepare/决议 WAL 证据、SCN Stage/Advance 两轮 ACK 全部到齐后，才按全局 commit_scn 发布并返回成功；发布前行锁不交后续写者；单写参与者不加 Prepare，但安装前保存 undo、等自身 end log 持久并经过同一发布；发布后 Release 才过户/解锁；若已发布的行被 `ResolveFinishedWriter` 延迟清理或由后续写者持有，Release 只跳过，不误解锁。
5. **Abort**：在允许 Abort 的阶段，2PC 尚未安装任何业务版本，只需逆序释放参与者锁、封口 undo 子段并结束导入快照；SI 冲突、正常锁冲突、用户 Abort 均走该路径。已 Prepare 持久化 YES 后缺证据不转成普通 Abort，而是显式失败（完整决议恢复留 P6）。
6. **幂等**：重复 Prepare 返回同一 durable 前缀；相同 commit_scn 的重复 Commit 返回原 installed/durable；不同 commit_scn 或不同写集显式失败；Release 和 finish-only drain 均记录身份并幂等。2PC 位置分配/证据提交重复也按同一 Plan/Resolution 核对。
7. **模式覆盖**：SYSTEM_MODE 2 的普通 `2pc`、`2pc-cornus`、`2pc-cornus-opt`，以及 SYSTEM_MODE 4 mix 的强制 Lazy 与分布式 2PC 分支；零写、单本地写、单远端写、多写参与者路径均验证。

## P5 在 Aliyun1 的部署、构建与验证

远端验证副本：`/data/workspace/mvcc-p5-20261119.ObCEe1`；原工作区 `/data/workspace/Hybrid_Cloud_MP` 同步同一份源码并核对 `git status --short`。构建：

```bash
cd /data/workspace/mvcc-p5-20261119.ObCEe1
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j8
```

组件回归（全部退出码 0）：

```bash
for t in mvcc_protocol_test mvcc_protocol_ndebug_test mvcc_publication_rpc_test \
         undo_storage_test undo_storage_ndebug_test prepared_2pc_test \
         prepare_notification_test lock_wait_test; do
  timeout 180s build/tests/$t > validation/$t.log 2>&1; echo "$t exit=$?"
done
```

P4 两节点 Lazy 回归（改动后重跑，结果 `validation/p4-regression/`）：57 项检查，`passed: true`，四个进程退出码均为 0。

P5 确定性集成（每轮 fresh data/epoch/端口；结果目录在副本 `validation/`）：

```bash
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-2pc-baseline   --mode 2 --variant 2pc
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-2pc-cornus-opt --mode 2 --variant 2pc-cornus-opt
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-2pc-cornus     --mode 2 --variant 2pc-cornus
python3 tests/integration/run_twopc_mvcc_cluster.py --source . \
  --output validation/p5-mix            --mode 4 --variant 2pc
```

| 运行 | 配置 | 检查数 | 结果 | 进程退出码 |
|---|---|---:|---|---|
| `p5-2pc-baseline` | mode 2, `2pc` | 73 | passed | node0/1/storage/remote 全部 0 |
| `p5-2pc-cornus-opt` | mode 2, Cornus v2 存储通知 | 73 | passed | 全部 0 |
| `p5-2pc-cornus` | mode 2, Cornus v1 持久 YES | 73 | passed | 全部 0 |
| `p5-mix` | mode 4, 强制 Lazy + 分布式 2PC | 83 | passed | 全部 0 |
| `p5-final-*`（同四组，最后一版二进制） | 同上 | 73/83/73/73 | 全部 passed | 全部 0 |

确定性用例实际覆盖：零写不分配位置/undo；单本地写与单远端写（含远端 home、实际写节点管理 undo 子段）；多写参与者一笔只占一个位置；参与者安装后暂停 → 新 Begin 不前进、两个旧快照只读旧值、后续写者不能越过未发布行；Abort 不占位置、两行保持旧值、锁释放后可再写；旧快照写新提交行返回 CONFLICT；暂停参与者 WAL → commit PENDING、快照不前进、FLUSH 后才成功；held Stage ACK → 新 Begin 不可见、释放后整笔可见；CLEAR_UNDO + 主页淘汰后旧快照仍重建历史；发布后新写者重新取得两行；mix 下 Lazy 写/提交、2PC 读旧快照、2PC 单/多写、Lazy 读 2PC 版本。

小规模真实负载（两计算进程；各自独立 config root，共享 fresh storage/remote）：

```bash
python3 tests/integration/run_mvcc_multinode_workload.py --source . \
  --output validation/wl-2pc-ycsb --bench ycsb --system 2pc --attempts 50 --local-ratio 1
python3 tests/integration/run_mvcc_multinode_workload.py --source . \
  --output validation/wl-2pc-smallbank --bench smallbank --system 2pc --attempts 50 --local-ratio 0.5
python3 tests/integration/run_mvcc_multinode_workload.py --source . \
  --output validation/wl-2pc-tpcc --bench tpcc --system 2pc --attempts 50 --local-ratio 0.5
```

| 负载 | 模式 | 每节点尝试 | 结果 | 关键统计 |
|---|---|---:|---|---|
| YCSB | 2PC | 100 | passed | node0 64 commit/36 abort，node1 76/24；均为 single-local 提交 |
| SmallBank | 2PC | 100 | passed | 两节点各 100 commit/0 abort，2PC 路径 |
| TPC-C | 2PC | 100 | passed | 两节点 87/13 与 91/9；共 30 笔 multi-writer Prepare/Commit；undo_pages 84/44 |
| SmallBank | mix | 100 | passed | 29 笔 Lazy + 91 笔 2PC，两分支均提交 |
| TPC-C | mix | 100 | passed | 120 笔全部 2PC（TPC-C 未产生热点计数），单/多写与 Abort 路径 |
| YCSB | mix | 60 | passed（修复后） | 120 笔 2PC，40/20 与 36/24 commit/abort |

最终收尾又用 `--attempts 20`、`--local-ratio 0.5` 重跑了 2PC TPC-C/SmallBank/YCSB 与 mix SmallBank/YCSB（`validation/wl-final-*`），确认 start_ts 编码、Release 延迟清理容忍、`CheckHistory` 修正和页 0 路由后仍然通过：

| 运行 | 每节点尝试 | 结果 | 关键统计（node0 / node1） |
|---|---:|---|---|
| `wl-final-2pc-tpcc` | 40 | passed，四进程 0 | 34/38 commit，6/2 abort；multi-writer 4/8，single-local 14/3；undo_pages 33/17 |
| `wl-final-2pc-smallbank` | 40 | passed，四进程 0 | 40/40 commit；single-remote 5/3；undo_pages 4/1 |
| `wl-final-2pc-ycsb` | 40 | passed，四进程 0 | 26/23 commit，14/17 abort；multi-writer 8/8，single-local 18/15；undo_pages 53/47 |
| `wl-final-mix-smallbank` | 40 | passed，四进程 0 | 40/40 commit；Lazy 12/9，2PC 28/31；undo_pages 4/1 |
| `wl-final-mix-ycsb` | 40 | passed，四进程 0 | 27/26 commit；全部走 2PC，multi-writer 9/9、single-local 17/16；undo_pages 57/53 |

负载测试的“passed”只表示进程退出码与结果文件成功，不作为性能收益结论。

所有 P5 结果文件位于远端验证副本的 `validation/` 下：`p5-2pc-baseline/`、`p5-2pc-cornus-opt/`、`p5-2pc-cornus/`、`p5-mix/`、`p5-final-*/`、`p4-regression/`、`wl-2pc-*/`、`wl-mix-*/`、`wl-final-*/`；每个目录含 `result.json`、进程日志和 `control-trace.log`（确定性测试）。本轮所有列出的命令均实际执行，未执行的部署/构建/测试没有写成通过。

## P5 中发现并修复的真实缺陷

1. **`RowImageFromItem` 读错 heap DataItem 业务值**：原实现按 `DataItem + sizeof` 取连续值，只适用于页内/序列化缓冲；home 私有副本的 `value` 是独立分配。mix 首次暴露为旧快照读到错误数值。现按 `item.value` 取，`CopyRow` 仍按页内布局取。
2. **2PC 主行更新未同步本节点 newest LSN**：`Install2pcMvccWrite` 和 Release 更新了页头 LSN，却没有更新锁表的 newest LSN；页被淘汰后可能从存储取旧页，再写出的日志 prev LSN 与页链不符。真实 YCSB 2PC 曾触发存储 `broken page LLSN chain` 并退出。现已与旧 2PC 提交/Abort 路径一致地 `set_newest_lsn`。
3. **页 0 文件头路由产生负数节点**：`(page_id - 1) / partition_size` 在 `page_id=0` 且 `partition_size=1`（TPC-C 小表）时得到 -1，`&nodes_channel[-1]` 段错误。现把页 0 固定路由到节点 0，`is_partitioned_page` 同样处理，并对 partition_size 做 CHECK。
4. **同 start_ts 不同 home 被误认自持锁**：mix 本地写分支只比较 `timeStamp`，没有比较 `txn_home_node`，不同 home 的同 local id 事务会把对方锁当自己的。现所有自持锁判断都带 home，并把 home 编入 `start_ts` 低字节，避免旧锁等待表键碰撞。
5. **发布后延迟清锁使 2PC Release 误失败**：已提交事务的物理锁可能被 `ResolveFinishedWriter` 在发布后清理、行被后续写者取得；Release 现在只在锁仍属于本事务时解锁，否则安全跳过，不误解锁。
6. **Prepared Release 的幂等键错误**：prepared 注册表按 `TxnIdentity`（含 coordinator incarnation）建键，Release 必须使用同一身份，不能只传 mvcc Context。
7. **finish-only drain 可被网络重复**：为无 applied 写集的 drain 增加身份集合，重复消息幂等。
8. **`CheckHistory` 把 P3 同 writer 规则套到所有主行 redo**：解锁/Release/回滚恢复的 redo 带的是既有历史 Ref，可能属于其他 writer；现在只对“本事务创建新版本”的 redo 强制同 writer undo 依赖，避免 mix 跨分支旧引用被误判为坏链。
9. **`ReleaseSPage/ReleaseXPage` 对 SYSTEM_MODE 4 断言**：mix Lazy 腿原未走 ExecuteLazy，现补齐 Lazy 页释放分派。

## P5 未执行、未验证或已知限制

- 没有完整崩溃恢复/协调点故障接管：已 Reserve/Decide 后 RPC 失败、Prepare YES 不完整、参与者/协调点重启均显式失败（CHECK），不自动 Abort 已决定事务，也不做决议恢复。P6 才处理。
- 没有跨物理机、3+ 节点、网络分区或 RDMA 验证；没有 HA、协调点切换。
- 没有 GC、历史空间复用、磁盘配额、长快照空间压力处理；prepared/single/finished 注册表、SnapshotNode 终态和 undo 段封口后不回收，内存/空间按本阶段可接受增长。
- 2PC 的主数据页按固定分区归属；mix 的 Lazy 腿可迁移页面。2PC 自身不做页面迁移。页面迁移与 undo 缓存淘汰验证主要由 P4 Lazy 与 mode 2 淘汰用例覆盖。
- **没有构造“2PC 持有者到已排队等待者”的显式交接**：本轮验证了安装暂停时后续写者不能越过、发布后新写者重新取得、mix 中 `ResolveFinishedWriter` 的延迟清理；Release 复用现有 `PrepareHandoff/UnlockTupleWithHandoff2PC`，P4 的 Lazy 等待者交接已有覆盖，但 P5 的 2PC→waiter 交接未单独运行。
- mix 的 `hybrid_cost_model.enabled=1` 成本模型分支没有在 P5 重新跑；默认/本轮配置使用显式 hot-key 选择，已验证 Lazy 与分布式 2PC 两条实际分支，调度策略代码未改但成本模型路径未验收。
- WAL 完成事件只对参与者 Prepare 前缀做了暂停门控；home 决议日志（TwoPhase）和单写自身 end log 的阻塞没有单独门控验证，代码路径仍等待这些前缀（单写 end log 由真实负载的正常路径覆盖）。
- 合法重复 Prepare/Commit/Release 的注册表语义有组件测试；没有在真实网络上注入重复 RPC。
- 负载规模很小（每节点 20–100 次尝试），没有性能结论；TPC-C 2 节点数据依赖小表 partition_size 的页 0 修正，若某表 `page_num < ComputeNodeCount` 仍会触发 `CHECK(partition_size > 0)`，本轮测试数据未出现。
- `start_ts` 低字节编码 home 要求节点数 < 256（实际上限 64）；这是锁身份细节，MVCC 身份仍是 `TxnId`。
- SQL/通用 INSERT/DELETE/改主键/范围扫描仍不在范围内；只覆盖三负载现有的更新和已有删除标记。

## P6 首要入口

> 设计与阶段计划在 2026-11-19 有一条范围调整：把 GC/页复用标为可选空间优化，允许 P5 后先做 P7 正确性验收。用户本轮仍明确要求把历史 GC、页复用和空间压力处理留给 P6；下面的 P6 入口按本轮要求列出，不改变 P5 的完成范围和 P7 的验收边界。

1. **安全 GC 与空间保留**：`SnapshotNode` 的 reg/unended snapshot、`RetentionFloor`、终态 outcomes 的保留边界；undo 段 `Finish` 后的页回收、`PageKey.generation` 复用、独立 undo 缓冲池的淘汰/重取；长快照不丢历史、空间压力限写。
2. **注册表清理**：prepared 的 APPLIED context、`SingleWrites`、`mvcc_finished_`、参与者 finish 集合目前保留到进程结束；P6 要给出可证明安全的删除/复用条件。
3. **故障恢复**：协调点决议持久化后的重放、PreparedLogRecord 重放、Abort/Release end 记录、`AllPrepared` 通知重放；把本轮 CHECK 失败的路径分类成可恢复决议。先看 `CommitCoordinator`、`prepared_2pc::Registry`、`LogReplay`、`StoragePoolImpl`。
4. **跨流/恢复历史依赖**：`CheckHistory` 现在只做新版本同 writer 检查；页面重建后历史 Ref 的跨 writer 等待、页迁移与 undo 页复用的交互。
5. **负载与性能**：P7 在已通过的正确性边界上做三类负载、三类模式的规模测试和成本记录；不要把本轮小规模 smoke 当性能结论。

## P5 退出条件结论

计划要求“所有参与者统一可见，覆盖 Prepare / Commit 和零、单、多写参与者路径”。本轮已在 Aliyun1 用真实两计算进程验证：普通 2PC、两个 Cornus 分支、mix 的 Lazy/分布式分支；零写、单本地写、单远端写、多写；自读、远端历史、SI 冲突、允许 Abort 阶段的回滚、延迟清锁/发布后取得、页面迁移相关历史读取、undo 缓存淘汰、少一个 WAL/安装/ACK 时不提前成功。**P5 的正常运行路径退出条件已达到**；完整崩溃决议、GC、页复用、空间压力和 HA 明确留给 P6/P7，本轮不宣称这些能力。

# P4 历史交接记录（2026-11-18，保留原结果）

## 本轮基线、工作方式和远端目录

- 本地目录 `/Users/noctis/Documents/项目/Hybrid_Cloud_MP`，实际 HEAD 为 `09fed8e`，实现仍在工作区/暂存区。检查了仓库及祖先 AGENTS.md，没有适用文件；未回退、未 reset、未 commit，保留已有暂存/未暂存/未跟踪文件。
- 先读取设计、阶段计划、SCN 协议、P3 说明和本交接的旧内容，再按源码核实 P3。**P3 的原有测试入口在修复一个真实缺陷后通过**，不是直接引用历史结果。
- 远端实际原项目为 `/data/workspace/Hybrid_Cloud_MP`，HEAD 同为 `09fed8e`。所有本地修改先完成，再同步到远端原 Git 工作区；实际构建/验证使用独立副本 `/data/workspace/mvcc-p4-20261118.aYPrVM`；最终又建立全新副本 **`/data/workspace/mvcc-p4-final-20261118.EGBTUI`**，从空 `build/` 全量构建并重跑全部测试。原工作区只同步源码，不覆盖实验数据。
- 远端环境：GCC 11.4.0、CMake 3.22.1、protoc 3.12.4、`/usr/local` brpc 1.9.0。验证全部使用 `127.0.0.1`、新 epoch、新数据目录和随机端口；没有复用旧实验数据。

## 相比 P3，本轮代码改了什么

| 范围 / 文件 | P4 当前实现 |
|---|---|
| `core/mvcc/protocol.{h,cc}` | `CommitCoordinator` 增加消息等待/完成等待、协调点 head/next 观测；所有 Stage/Advance 仍由完整 Resolution 驱动；未增加“收到大 SCN 即推进”的捷径 |
| `core/mvcc/publication.proto`、`publication_rpc.{h,cc}` | 新增 `Publish` 之外的 `Commit`、`QueryStatus` RPC；新增 `PublicationHost` 接口；Commit 返回前必须收齐两轮 ACK，失败/超时明确失败 |
| `core/mvcc/lazy_runtime.{h,cc}` | 从单成员改为显式成员集合；协调点固定为计算节点 0；每个节点有 `SnapshotNode`，只有协调点运行发布线程；`Complete` 对本地/远端事务分别走本地 `SubmitCommit` 或 `Commit` RPC；creator 状态先查本地 Stage 终态，未知再查 home，仍未知则显式失败 |
| `compute_server/server.{h,cc}` | 移除 Lazy 单节点启动 CHECK；注册 `PublicationService`；`FetchUndoPage` 使用 `UNDO_TABLE_ID` 路由；增加 undo 权限转移、远端取页、WAL 等待统计；主表 `table_id<10000` 分支保持原义，undo 走 `PageUsesWal` |
| `compute_server/run.cc` | `result.txt` 增加 undo 页数、淘汰、权限转移、远端取页、WAL 等待 |
| `core/LPLM/local_LR_page_lock.h` | **修复 P3 真实缺陷**：`getLock()` 原返回 `int`，`EXCLUSIVE_LOCKED=0xFF00000000000000` 被截断成 0，释放 X 页的断言错误；改为返回 `lock_t` |
| `core/dtx/dtx_mvcc.cc` | 去掉节点 0 身份假设：行锁 home、`WaitForLock` 的 self_node、`FinishDeferredTxn` 的 self_node、BatchEnd 日志 producer 都使用本计算节点；undo redo 与主行 redo 仍要求同一 writer |
| `compute_server/lazyrelease_server.cc` | 记录 undo 页的远端权限转移/远端取页；页面推送与淘汰继续使用 `PageUsesWal` 和实际页 LSN 等待 |
| `tests/test_cases/lazy_mvcc_cluster_node.cc`、`tests/integration/run_lazy_mvcc_cluster.py` | 新增两计算节点真实 DTX 驱动：显式控制 WAL flush、Stage ACK gate、主页迁移、main/undo 淘汰、并发追加/读取、creator 状态查询 |
| `tests/test_cases/mvcc_cluster_test.cc` | 新增真实 loopback brpc 的双运行时测试：Commit RPC、held Stage ACK、SCN 顺序、远端 ACTIVE/COMMITTED/ABORTED |
| `tests/test_cases/lazy_mvcc_test.cc`、`mvcc_publication_rpc_test.cc`、CMake | 更新构造/入口；新增 `mvcc_cluster_test`、`lazy_mvcc_cluster_node`、两个集成脚本目标 |
| `run_lazy_mvcc_multinode_workload.py` | 两计算进程的小规模 YCSB Lazy 负载；每进程独立配置根，避免 CLI 改 machine_id 的竞争 |

## 源码中已成立的多节点 Lazy 行为

1. **快照与提交**：每个节点在本地 `SnapshotNode` 的同一 mutex 内读取 safe_scn 并登记；Begin 不跨节点重新取快照。写事务的 commit_scn 由固定协调点连续分配；协调点先等 WAL/安装证据，形成 Resolution，再向全员 Stage，收齐 Stage ACK 后才 Advance，收齐 Advance ACK 后才成功。成功返回后任意节点的新 Begin 必含该提交，旧事务仍用原 Snapshot。
2. **远端 creator 状态**：版本候选的 creator 与锁 owner 分离。读取历史时，本节点已有 Stage 终态则直接使用；否则按 creator.home 调 `QueryStatus`。未知状态不会当成已提交/已回滚，也不会提前结束历史链，而是 CHECK 失败。P4 验证了远端 ACTIVE、COMMITTED、ABORTED 和 UNKNOWN。
3. **主数据页迁移**：页迁移只复制 4096 字节页内容；creator、deleted、undo Ref 和行历史链保持原值。迁移后的旧事务仍通过 RID/稳定 Ref 取页，不依赖原节点内存地址。主行 redo 仍进入实际写节点 WAL，页级前驱队列处理跨 writer 迁移后的回放依赖。
4. **undo 页访问**：`PageKey` 的 `(epoch,writer,number,generation)` 决定稳定地址；`UNDO_TABLE_ID=30000` 的交错页号接入原 LPLM/GPLM。读 S、写/创建 X；正在追加页也按整页 S/X 交接。读者在持有 X 的写者释放后才读整页，记录只有在完整写入后才被发布。
5. **WAL 与淘汰**：undo 初始化/追加/封口 redo 进入本 writer 原 WAL；主行 redo 在其后，保持同一 writer 的 `undo_lsn < main_lsn`。页面推送、交接、淘汰前按 `PageUsesWal` 等待实际页 LSN；undo 不会落入 `table_id>=10000` 的“跳过等待”分支。缓存淘汰只释放帧；后续 `GetUndoPage` 等回放/存储依赖。
6. **锁与等待**：行锁身份使用本节点 `TxnId.local` 加 `txn_home_node`；跨节点 `WaitForLock` 注册 waiter 的实际 home，`FinishDeferredTxn` 按实际 home 唤醒；不再有节点 1 事务把 home 写成 0 的错误。主行 X 保护在 undo I/O 前释放；Abort 先读 undo，再取主行恢复。
7. **快照保留**：Begin 在返回 Context 前登记；结束/提交后 `End`。P4 未实现 GC，但长快照跨多次提交、迁移、淘汰、并发追加仍保持可读；没有“快照已使用但尚未登记”的窗口。
8. **发布失败语义**：Commit RPC 只返回 committed+slot；缺 ACK、RPC 失败、未知状态都失败，不伪造成功。Stage 后、Advance 前节点本地状态虽已 COMMITTED，但 safe_scn 不推进；测试用 held Stage ACK 验证了这一点。

## Aliyun1 实际部署、构建与测试命令

最终工作目录为 `/data/workspace/mvcc-p4-final-20261118.EGBTUI`（全新独立副本，空 build）。以下命令和退出状态来自该副本；原 Git 工作区 `/data/workspace/Hybrid_Cloud_MP` 已同步，`git status --short` 显示 51 项改动/未跟踪源码。

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j6

for t in mvcc_protocol_test mvcc_protocol_ndebug_test \
         mvcc_publication_rpc_test mvcc_cluster_test \
         undo_storage_test undo_storage_ndebug_test; do
  timeout 180s build/tests/$t > validation/$t.log 2>&1
  echo "$t exit=$?" >> validation/results.txt
done

python3 tests/integration/run_lazy_mvcc.py \
  --source . --output validation/p3-ycsb --bench ycsb
python3 tests/integration/run_lazy_mvcc_cluster.py \
  --source . --output validation/multinode
python3 tests/integration/run_lazy_mvcc_multinode_workload.py \
  --source . --output validation/multinode-workload --attempts 100
```

实际退出状态：

| 目标 | 结果 |
|---|---|
| `mvcc_protocol_test` / `_ndebug` | exit=0；保留原有 5,040 排列、64 次 Begin/Advance barrier、8 并发 Reserve、14 死亡检查 |
| `mvcc_publication_rpc_test` | exit=0；真实 loopback Stage/Advance、held ACK、Context |
| `mvcc_cluster_test` | exit=0；新增真实 Commit RPC、held Stage ACK、remote ACTIVE/COMMITTED/ABORTED |
| `undo_storage_test` / `_ndebug` | exit=0；页格式、回放、依赖等待 |
| P3 单节点 `run_lazy_mvcc.py --bench ycsb` | exit=0；修复 `getLock()` 截断后通过，三条服务均 exit 0 |
| P4 两节点 `run_lazy_mvcc_cluster.py` | exit=0；57 项检查全部通过，node0/node1/storage/remote 均 exit 0 |
| 两节点小规模 YCSB Lazy 负载 | exit=0；两计算节点各 200 次尝试（合计 400），提交 151+152、Abort 49+48；四进程均 exit 0 |

P4 两节点测试的关键观测（`validation/multinode/result.json`、`control-trace.log`）：

- A 提交后 B 新 Begin 的 snapshot 等于新 SCN，B 旧事务重复读保持旧值；`scn+1` 严格递增，未出现空洞发布。
- 暂停 A 的 WAL flusher 后，`COMMIT_ASYNC` 保持 `PENDING`；B 新 Begin 不前进；显式 `FLUSH` 后 commit 才返回。
- 协调点 gate 住发往 B 的 Stage ACK 后，低位置 head 停在原处；随后 B 上的高位置 commit 可以 Reserve 但不能可见，也不能让 B 的 Begin 跳过低位置；释放 ACK 后两个位置按序成功。
- 写事务结束前把主行页 `FETCH_X_MAIN` 到 B；B 旧快照读到旧值，A 的 creator 状态跨节点查为 ACTIVE；清空 main/undo 缓存后 A Abort 仍恢复主行，creator 状态变为 ABORTED。
- 并发 `WRITE_LOOP`（A，64 次追加）与 `READ_LOOP`（B，旧快照 64 次读）均成功；B 的旧值不变，无 CHECK/死锁/半条记录。
- 最终统计：node1 `undo_remote_fetches=1291`、`undo_permission_transfers=1295`、`undo_evictions=1290`；node0 `undo_pages=28`、`undo_evictions=24`、`undo_wal_waits=33`。证明 undo 页跨节点读取/权限交接/淘汰真实发生。
- 小规模负载最终统计：node0 undo_pages=349、evictions=381、transfers=394、WAL waits=371；node1 undo_pages=352、evictions=378、transfers=391、WAL waits=368；提交 151+152、Abort 49+48（共 400 次尝试），throughput 295.9/309.4。该随机负载本身未发生跨节点 undo 历史读，故 `undo_remote_fetches=0`，跨节点 undo 读由确定性 cluster test 覆盖。

构建/验证过程中实际暴露并修正的问题：

1. P3 `LRLocalPageLock::getLock()` 返回 `int`，与 `EXCLUSIVE_LOCKED`（高位非零）比较恒为假；单节点 `lazy_mvcc_test` 因此 SIGABRT。改为 `lock_t` 后 P3 测试通过。
2. P4 初版 Lazy BatchEnd 仍写 `log_node_id_=0`，节点 1 的 WAL 批混入节点 0 producer，storage `LogManager` 直接 abort。改为本节点 ID。
3. 节点 1 的 `WaitForLock` self_node、`FinishDeferredTxn` self_node 仍写 0，导致本节点 waiter 收不到唤醒并触发 lock watchdog；改为 `getNodeID()`。
4. 启动阶段 storage meta 一次只 accept 一个 compute；两节点必须错开握手，否则第二个节点可能 Connection refused。验证脚本已显式 stagger；这是现有启动协议约束，不是 MVCC 语义。
5. 远端 remote meta 的 finish barrier 返回后 remote_node 可能已退出，`ShutdownRemoteServer()` 不应再被当成成功条件；测试只把 socket finish barrier 作为完成握手。

## 尚未验证的条件及原因

- 只在同一台 Aliyun1 上用两个 loopback 计算进程验证，未跨物理机、未经过真实网络延迟/丢包/RDMA；两台以上、三成员及以上未测。
- 协调点固定为节点 0，未实现协调点故障切换、重启恢复、ACK 丢失重试或成员增删；RPC 失败当前按项目约定直接失败。
- 没有 P6 GC/页槽复用/磁盘配额；长快照保留只依赖不回收，不能证明真实复用下的代次安全。
- 未验证“封口 undo 页同时被两个以上节点作为只读副本”的专门断言；已有跨节点 S 读取和权限转移统计，但多读者缓存是后续专项。
- 小规模负载只跑 YCSB Lazy；SmallBank/TPC-C 的 P4 多节点路径未在小规模负载中重跑，TPC-C 的 `valid=0` 只在 P3 单节点测试覆盖。
- 未覆盖提交 RPC 长时间不回、网络超时、进程崩溃后 WAL/undo 回放恢复；`LogWrite` 仍禁用自动重试，完整事务恢复不在本轮。
- 未做性能结论；undo 增加权限转移/WAL 等待是实际观测，不预设与改造前相同。

## 当时的 P5 入口（历史，已被本轮取代）

1. `DTX::TxPrepare` / `Tx2PCCommit` / `Tx2PCCommitLocal` / `Tx2PCCommitAll`、`core/compute_node/prepared_txn.h`、`compute_server/twopc_server.cc`、`twopc_fetch_server.cc`：把同一 `Context` 通过 Export/Attach 传给参与者，私有执行仍用 home snapshot；Prepare 保存完整 undo 和新值证据，Commit 安装后进入同一协调点发布。
2. `core/mvcc/protocol.h` 的 `CommitKind::TwoPhase/Cornus`、`Plan::prepare/decision`、`CommitCoordinator` 已有两轮发布基础；当前 `PublicationService::Commit` 只封装 Lazy SingleWriter Plan，P5 需要为多个 writer/证据/决议扩展 wire 协议或协调点本地适配，不能伪造 WAL/Installed 证据。
3. `LazyRuntime` 的成员集合、`SnapshotNode::Export/Attach/Finish`、远端状态查询可以复用；参与者的 creator 状态与 home 状态必须先一致，再允许 Stage/Advance。所有参与者安装完成前不能发布半笔事务。
4. 远端 2PC 读仍要经过统一 `VisibleRow`，不能把整页最新值直接返回。零/单/多写参与者、普通 TwoPhase 与 Cornus 变体逐个接入；mix 的 Lazy 分支继续复用 P4，2PC 分支按同一协议接入。
5. P4 的独立验证脚本/控制节点可扩展为 P5 的多参与者 barrier；但不要把 P4 的 Lazy 单 writer `SubmitCommit` 当成已经支持多 writer 的证据。

# P3 历史交接记录（后续重构前）

**以下保留原文中的部署路径、命令和结果，未在本次重新执行或核对远端产物。其“通过”“已完成”等描述仅适用于当时的代码；独立 undo 目录/缓存、lazy_mvcc 开关和历史等待队列已经被后续代码替换，不能据此判定当前版本完成 P3。**

## P3 基线与部署

- 本地 `/Users/noctis/Documents/项目/Hybrid_Cloud_MP`，HEAD `09fed8e0f4f9eb1820e7a856d95f2e4de466ea71`。检查了仓库状态、目录及祖先 AGENTS.md（无适用文件），按要求读取设计、计划、SCN 协议和 P2 进度；依据源码和重新执行的测试核实 P1/P2。未回退、未创建 commit，保留用户已有改动及暂存状态。D1—D9 不变。
- 所有修改先在本地进行，再通过实际 SSH 别名 `AliYun1` 同步。原项目核实为 `/data/workspace/Hybrid_Cloud_MP`，HEAD 同上；原 Git 工作区已同步本轮源码，`git status` 可见修改。收尾时原目录已经出现本轮中间版本的同步内容，逐文件比较后只补齐本轮后续改动；同步前后按 SHA-256 检查，没有覆盖其他内容。
- 独立构建/验证目录：**`/data/workspace/mvcc-p3-20261118.klAcLX`**。旧实验数据不参与本轮；每个测试用 `tests/integration/run_lazy_mvcc.py` 创建全新目录、配置、epoch、数据和五个独立端口。未在原 Git 工作区重建已有实验数据。
- 本地没有 protoc/brpc 开发依赖，未声称本地 C++ 编译通过。远端使用 GCC 11.4.0 / CMake 3.22.1 / protoc 3.12.4 及既有 `/usr/local` brpc，Debug 全量构建成功。源文件散列清单用于比较本地、验证副本及原 Git 工作区；构建生成的 protobuf 文件单独检查。

## P3 实际改动与路径

| 文件/入口 | 已接通的行为 |
|---|---|
| `core/mvcc/row_fields.h`、`row.{h,cc}`；`core/base/data_item.h` | MVC2 固定主行元信息，creator 与锁 owner 分离，完整候选复制/安装，稳定 Ref，序列化清除进程指针 |
| `core/record/{record.h,rm_file_handle.h,rm_manager.cc}`；`storage_server/server.cc` | RMV2 文件边界、加载器及页容量校验；DataItem 72→168 字节，旧数据必须重建 |
| `core/mvcc/lazy_runtime.{h,cc}` | 节点共享 P1 状态/快照保护、单成员发布、P2 Directory/Manager、历史重建和实际 storage 取页 |
| `core/dtx/dtx.h`、`dtx_exe.cc`、新增 `dtx_mvcc.cc` | TxBegin / TxExe / TxCommitSingle / TxAbortWorkLoad 接入；真实 BLink/Lazy 页权限、回调安装、旧私有副本写回、SI 冲突、逆序 undo 回滚及墓碑 |
| `compute_server/server.{h,cc}` | 生产注册 SCN/undo 服务，实际 WAL 适配、锁终态校验、Update redo 清除指针、明确启动就绪事件 |
| `compute_server/worker/handler.cc` | 以 WaitReady 替换负载启动固定 sleep |
| `core/undo/{format.h,format.cc,store.h,store.cc}` | 稳定 Ref 编解码；存储回放非阻塞依赖检查，验证完整记录和 RID |
| `core/storage/logreplay.{h,cc}` | 真实 MVC2 主行 redo 的 undo 依赖队列；页前驱/历史等待期间允许相同重复，矛盾内容失败；回放观察事件 |
| `config/compute_node_config.json`、两处 CMake | 单节点显式开关及 epoch、编译和测试目标 |
| `tests/test_cases/lazy_mvcc_test.cc`、`tests/integration/run_lazy_mvcc.py` | 真实三服务/DTX 验证；独立数据和端口；小缓存及正常 LRU 淘汰/重新取页 |
| `undo_storage_test.cc`、`prepared_2pc_test.cc`、`lock_wait_test_main.cc` | 真实加载器/主行 redo 验证、布局断言联动、确定性回放事件回归 |
| `LAZY_MVCC_P3.md`、`SCN_MVCC_PROTOCOL.md`、`UNDO_STORAGE_PROTOCOL.md` | 当前实现约定、P3 接入状态及后续边界 |

已成立的关键行为：

1. Begin 先登记固定 snapshot 的保护，再创建逻辑 undo 段；只读不分配页。初始化版本有独立身份。普通其他版本只有全局 COMMITTED 且 commit_scn <= snapshot 才可见；自身写入优先。
2. 回调实际覆盖前先 Append 完整旧逻辑行，释放主页 latch 后做 undo I/O，再重取 X 重检并一致安装；新行/undo 记录共用实际 writer WAL。无业务变化的加锁不产生版本，历史不依赖锁是否清除。
3. 未安装私有修改支持自读；Commit 仍经同一 undo 安装入口，`valid=0` 写成墓碑且保留 bitmap、RID 和 BLink 索引。后续回调同步较早私有视图，避免旧副本在 Commit 复活。
4. 持有写权限后检查 SI；正常锁冲突与 SI 冲突走明确失败/Abort 路径。`TxExe(false)` 的失败状态不会被下次执行重置成可提交状态。
5. 已安装修改按子段 sequence/tail 逆序重新取页回滚，仅恢复逻辑数据/creator/删除标记/旧 Ref。回滚 WAL durable 后结束 MVCC 状态，再清锁/过户；锁管理器终态最后发布，避免清锁竞态。
6. Commit 在实际安装和批量 WAL durable 后经过 P1 两阶段发布状态机的单成员 ACK，完成后才通知后继写者。新 Begin 包含已成功提交事务；旧快照不变。没有逐条 undo fsync。
7. 主行 redo 先于 undo 回放到达时，主行保持旧状态并排队；回放线程继续处理 undo。GetUndoPage 仍等准确回放依赖。孤立 undo 不构成业务更新，缓存淘汰后仍可从 storage 取历史并回滚。

详细顺序与配置见 `LAZY_MVCC_P3.md`。启用条件为根配置 `lazy_mvcc=1`、16 字节 `mvcc_epoch`、单节点 0 / Lazy / WAL 开启。仓库原多节点配置的开关默认 0；不隐式把 2PC/mix 当作已支持 MVCC。undo 默认每节点 10000 帧，测试覆盖 1 帧。最大现有逻辑值是 YCSB 的 1004 字节，TPC-C customer 664 字节，均小于 undo 上限 3750 字节。

## P3 实际验证命令和结果

以下命令在 `/data/workspace/mvcc-p3-20261118.klAcLX` 执行，均实际 exit=0；最终汇总在 `validation/verified-exits.json`。

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j6
# 最终全量构建日志：build-final2.log

build/tests/mvcc_protocol_test
build/tests/mvcc_protocol_ndebug_test
build/tests/mvcc_publication_rpc_test
build/tests/undo_storage_test
build/tests/undo_storage_ndebug_test
build/tests/prepared_2pc_test
build/tests/prepare_notification_test
build/tests/lock_wait_test

python3 tests/integration/run_lazy_mvcc.py --source . --output validation/ycsb-dtx-final --bench ycsb
python3 tests/integration/run_lazy_mvcc.py --source . --output validation/smallbank-dtx-final --bench smallbank
python3 tests/integration/run_lazy_mvcc.py --source . --output validation/tpcc-dtx-final --bench tpcc
python3 tests/integration/run_lazy_mvcc.py --source . --output validation/ycsb-workload-final --bench ycsb --workload
python3 tests/integration/run_lazy_mvcc.py --source . --output validation/smallbank-workload-final --bench smallbank --workload
python3 tests/integration/run_lazy_mvcc.py --source . --output validation/tpcc-workload-final --bench tpcc --workload
```

没有用 ctest 替代测试二进制。8 个组件/回归目标全部 exit=0；普通及 NDEBUG P1/P2 目标、真实 RPC、redo 初始页重建/重复回放/五种不完整 Append 尾、页/槽/代次/长度反例均通过。新增实际 RM 加载器边界、Serialize/creator/锁字段、9 个主行格式死亡检查；原 18 个 undo 死亡检查保留。`lock_wait_test` 为 **110 passed, 0 failed**。日志保存在 `validation/verified-<目标>.log`。

三套真实 `lazy_mvcc_test` 各自验证：100→90→80 的不同固定快照、commit_scn 等于快照、未提交自读和其他事务旧读、显式 SI/普通锁冲突、失败状态保持、仅加锁、回调与私有副本交错/无变化回调、私有 Abort、40 次两行交错更新及逆序 Abort、1 帧 undo 与 16 帧主表淘汰后历史读/回滚、墓碑及保留 RID/索引。TPC-C 用真实 NewOrder 表（table 4）验证 `valid=0`。进程退出均为 compute=0、storage=0、remote=0。

小规模负载均运行原 `compute_server <bench> lazy 2 0.5 1 0`，每线程 100 次，共 200 次；数据为 YCSB 2000 行、SmallBank 2000 账户、TPC-C 2 warehouse × 5 district × 100 customer，item/stock 各 1000。WAIT_DETECT 启用，1 帧 undo/16 帧主表。最终结果：

| 负载 | 尝试/提交/Abort | 已观察路径 | 服务退出 |
|---|---|---|---|
| YCSB | 200 / 148 / 52 | 原多读写负载与正常竞争/冲突 | 三服务均 0 |
| SmallBank | 200 / 200 / 0 | 六类普通事务均实际执行 | 三服务均 0 |
| TPC-C | 200 / 199 / 1 | NewOrder 56、Payment 50、Delivery 4、OrderStatus 41、StockLevel 49 次；Delivery 4 次均提交 | 三服务均 0 |

这些是正确性冒烟结果，不是性能对比或提升声明。各 `validation/<case>/result.json` 记录实际端口、绝对二进制命令和退出状态，`compute.log`/`storage.log`/`remote.log` 及其运行目录保留业务结果与 WAL。负载的正常 Abort 不等同于测试失败。

验证过程中保留了失败记录：初版回放测试用错位图位序，已改为 Bitmap::set；初版服务测试目录名不符合既有 `../storage_server` 路径，已修正隔离目录。旧 prepared_2pc_test 在并发测试下暴露 30ms 等待假设，改为实际 Dispatch 完成事件后再断言，未删减断言。最终结果以 `verified-*` 和 `*-final` 为准。

## 未验证条件、限制及 P4 首要入口

- 本轮只完成单节点 Lazy；真实多节点 SCN 发布、跨节点主行/undo 迁移、远端 creator 状态解析尚未接通/验证。P2 跨节点组件测试不能冒充 P4 验收。
- P4 从 `LazyRuntime`、`LocalAccess`、`ComputeServer` 服务配置和 `LogReplay::HistoryReady` 开始，接入真实成员集/发布等待/状态解析/目录路由；去掉单 writer 假设时必须使用 `(epoch, writer, LSN)` 依赖，不能直接比较不同 WAL 流的数值。沿用当前 DTX 及行格式。
- 2PC / mix / Cornus 的 MVCC 留 P5。现有 prepared/notification 回归通过只证明相关既有能力未被本轮破坏，不证明其具有新 MVCC。SQL、通用增删/改主键/范围扫描没有扩展。
- 存储 redo 可重建主行与 undo，但没有完整事务终态恢复/旧 epoch 接管/机器崩溃决议；不能对保留旧数据重启计算端后宣称恢复后 MVCC 可用。每个验证场景都生成新数据。
- GC/代次复用、磁盘历史配额、自动故障接管、长时间压力和网络故障未验证；现阶段只追加并保留状态。长快照登记不自动失效。
- 完整优化构建下的 DTX 矩阵、SmallBank LongTx 的逐函数独立运行、全部配置变体与长期性能矩阵未验证；正常入口已接入，完整矩阵属于后续 P7。本轮 NDEBUG 验证覆盖 P1 和 P2 实现，不冒充完整 Release DTX 测试。


---

# P2 历史交接记录

**以下是旧 P2 当轮记录；其中独立 Directory/CacheService/Buffer 文件和夹具已删除，真实 DTX 后来在 P3 接入。这里只保留历史命令和结果，不作为当前实现入口或验收结论。**

日期：2026-11-18（Asia/Shanghai）。本轮只实施 **P2**。**P2 的存储基础退出条件已达到**：按需分配、跨页追加与两套链、独立小缓存淘汰、跨节点取页、WAL/回放重建、幂等与依赖等待均有实际测试。P0/P1 基础已重新核验。**P3 尚未开始，真实业务行 MVCC 未完成。**

## 基线与本轮工作方式

- 本地目录：`/Users/noctis/Documents/项目/Hybrid_Cloud_MP`。设计基线和实际 HEAD 均为 `09fed8e0f4f9eb1820e7a856d95f2e4de466ea71`；未回退、未提交 Git commit。
- 开始时有上一轮 P0/P1 的未提交代码、协议/进度文档，以及用户提供的四份原始文档。本轮逐项读取设计、计划、SCN 协议和交接，再核对实际源码；未覆盖已有工作，未修改 D1—D9 或四份原文。没有适用的 AGENTS.md。未创建新聊天，未委派代理。
- 所有编辑均先在本地完成，再将源码同步到远端。SSH 配置实际别名为 **`AliYun1`**，所以使用 `ssh AliYun1`；用户给的大小写 `Aliyun1` 在此配置下不匹配。
- 远端实际原项目为 `/data/workspace/Hybrid_Cloud_MP`，已核对 HEAD 同基线、工作区干净。为保留原项目和数据，本轮部署到独立目录 **`/data/workspace/undo-p2-20261118.XHovCV`**。原项目未部署覆盖。
- 上轮副本 `/data/workspace/mvcc-p0p1-20261117.cDsqYr` 保留；实际检查其 build-final.log，确认上轮全量构建已完成。但上轮部分测试结论存在下述问题，本轮以重新验证结果为准。

## P0/P1 实际交付及必要修正

实际存在 `core/mvcc/protocol.{h,cc}`、`publication.proto`、`publication_rpc.{h,cc}` 和三个测试目标。实现固定 Context/快照登记、单协调点连续 SCN、Prepare/WAL/安装/决议门槛、全员 Stage/Advance 两轮 ACK、统一可见性和 SI 冲突判断。它们仍是独立基础组件；DTX/生产服务启动尚未调用这些协议，不能据此宣称旧业务行已支持 MVCC。

**本轮发现并修正旧测试假通过风险：**远端 brpc 的 `crash_on_fatal_log` 默认未启用。上轮日志中父进程出现 `invariant was not rejected`，但 FATAL/CHECK 只打印、进程仍退出 0；因此原进度文档关于死亡测试已通过的结论无效。新增 `common/fatal_checks.h`，在计算端、存储端、目录端启动及 P1/P2/相关回归测试入口启用实际 fail-fast，位于 flags 解析后、线程启动前。没有删断言或放宽非法状态。

修正后重新实际运行 P1 三项测试，全部 exit=0。普通与 NDEBUG 协议测试均确认 14 个子进程因 SIGABRT 退出，父进程断言也会终止；5,040 种事件排列、64 次 Begin/Advance barrier 竞争、8 线程 Reserve 和 RPC ACK 时序均重新通过。NDEBUG 目标编译协议实现本身。

P0 协议和源码入口清单继续见 `SCN_MVCC_PROTOCOL.md`。P2 的具体格式、权限、持久化与接入边界见新增 `UNDO_STORAGE_PROTOCOL.md`。

## P2 实际改动文件

| 文件 | 已实现内容 |
|---|---|
| `core/undo/format.{h,cc}` | 4 KiB 页、显式身份/代次、稳定 Ref、完整逻辑旧行、行历史与子段逆序前驱、边界/完成标记/校验、不可变追加和 Seal |
| `core/undo/redo.{h,cc}` | UNDO_PAGE Init/Append/Seal 编解码；页级前驱、writer/epoch/事务一致性；矛盾重复失败 |
| `core/undo/directory.{h,cc}` | 可增长 typed 页目录、幂等分配、现有 eager GlobalPageLock 的 S/X 权限、writer 专属 X、完整 grant/重复检查、页 LSN 发布 |
| `core/undo/buffer.{h,cc}` | 复用现有 BufferPool/LRU；私有 cache tag、短 latch/完整副本、节点共享帧预算、淘汰 WAL 等待、缓存前缀不可变检查 |
| `core/undo/manager.{h,cc}` | Begin 逻辑段、首次 Append 分配、满页扩展、每次覆盖独立记录、Finish 封口、历史读取/逆序读取、跨节点缓存导出/清空 |
| `core/undo/page.proto`、`page_rpc.{h,cc}` | 真实 brpc DirectoryService、CacheService 和 RpcPageAccess；按页身份路由及最新 LSN 校验 |
| `core/undo/store.{h,cc}` | 存储端空间文件、Init 创建页、前驱等待队列、精确重复验证、回放后取页等待；无常驻整页副本池 |
| `core/storage/log_record.h`、`logreplay.{h,cc}` | 新日志枚举、完整前缀解码与 undo 分派、pending/applied 统计、回放 Drain 同步 |
| `core/storage/storage_service.proto`、`storage_rpc.{h,cc}` | GetUndoPage：带 PageKey 和 required_lsn；缺页/回放未完成时等待，不返回空历史 |
| `core/compute_node.h`、`config/compute_node_config.json` | 两个计算节点构造入口的独立共享 undo Buffer；默认 `undo_buffer_pool_size_pages=10000` |
| `compute_server/server.h` | MakeUndoWal 适配现有 AddMetadataLog / wait_log_flush；不增加独立日志流或逐记录 fsync |
| `common/fatal_checks.h`、`compute_server/run.cc`、`remote_server/server.cc`、`storage_server/server.cc` | 使已有 CHECK/LOG(FATAL) 在实际运行环境中终止进程 |
| `core/CMakeLists.txt`、`tests/CMakeLists.txt` | undo_format、undo_compute、存储回放编译与两个测试目标 |
| `tests/test_cases/undo_storage_test.cc` | 页格式反例、权限/淘汰 barrier、真实 RPC/WAL/回放、小缓存、两套链、重建与截断尾测试 |
| P1 三个测试源码及 `prepared_2pc_test.cc` / `prepare_notification_test.cc` | 测试入口启用 fail-fast；保留原测试逻辑 |
| `doc/UNDO_STORAGE_PROTOCOL.md`、`SCN_MVCC_PROTOCOL.md`、本文件 | P2 协议、已实现边界、验证及下一阶段入口 |

没有改变 `DataItem` 布局（仍为 72 字节）、既有负载加载器、主行 Serialize、业务 memcpy 偏移或现有 BufferPool / GlobalPageLock 实现。逻辑 RowImage format=1 独立于旧主行格式。

## 已成立的行为与协议决定

1. `Manager::Begin(Context)` 只建逻辑段；只读 Begin/Finish 不取物理页。同一 TxnId 的快照不可变；2PC 各实际 writer 共享全局逻辑身份，各有自己的子段/WAL。
2. PageKey 显式包含 kind/epoch/writer/number/generation，Ref 另含 slot/record_generation/目标追加 LSN。物理 Init 同时记录分配身份；只追加、不复用，代次固定为 1。
3. 每次 Append 保存完整旧逻辑行（RID/key、creator、initial/deleted、旧前驱、value），不存锁 owner、等待队列或地址。行链可跨 writer；事务逆序链限制在对应 writer 子段，sequence 连续。发布的记录前缀不可改写。
4. 单条最大 value **3750 字节**；当前最大负载值 YCSB **1004 字节**，TPC-C customer **664 字节**（P3 核对纠正记录；P2 测试本来已遍历全部类型）。测试覆盖所有当前 workload value 类型和恰好填满页的最大记录，不截断。
5. 页权限每操作申请 S/X，只有 writer 可 X；缓存驻留不代表后续命中仍有效，每次核对目录 LSN。使用既有 eager 页锁与 BufferPool/LRU；没有借用 table_id>=10000 跳 WAL 的通路。
6. 独立节点帧预算在构造时生效。极小池正常淘汰；等待 WAL 时不持 cache latch。源 writer 导出缓存也先等 WAL；无缓存则向 storage 等正确回放依赖。计算端不写 undo 文件。
7. UNDO_PAGE Init/Append/Seal 经实际 writer 的已有批量 WAL 适配器；存储端回放创建 `UNDO_<epochhex>_<writer>.pages` 和段/记录元数据。重复 Append 比较完整原记录，后继先到进入页前驱队列。
8. 持久化和回放是两种事件。GetUndoPage 在准确 PageKey/LSN 未就绪时阻塞；回放完成才返回。Undo 存在本身不会改主行或产生提交。
9. 主行引用重建测试使用新建临时主表的 RowImage 编码与现有 UpdateLogRecord；重放的是真实 redo 和页文件，但这**不是生产 DataItem 挂链或业务事务 MVCC**。P3 仍需联动真实行格式、加载器和执行路径。
10. 长快照和状态保留继续使用 P1 协议；Finish undo 子段不注销快照。目录/段/请求信息暂不回收；磁盘历史只增长，P6 才处理 GC、复用与空间配额。

## 实际构建、部署和验证

本地没有可用的 protoc/brpc 开发依赖，本轮未声称本地 C++ 编译通过。所有代码、测试、文档和格式化在本地完成；执行 `git diff --check`，随后在 AliYun1 使用既有依赖构建。远端环境为 GCC 11.4.0、CMake 3.22.1、protoc 3.12.4、`/usr/local/include/brpc` 和 `/usr/local/lib/libbrpc.so`。

部署使用 `git ls-files --cached --others --exclude-standard` 的实际源码清单生成 tar.gz 和每文件 SHA-256。先核对原目录，再用 `mktemp -d /data/workspace/undo-p2-20261118.XXXXXX` 建立独立副本。修订都先改本地，再 scp/tar 覆盖这个验证副本。

实际运行命令（工作目录为 `/data/workspace/undo-p2-20261118.XHovCV`）：

```sh
cmake -S . -B build > configure.log 2>&1
cmake --build build -j4 --target undo_storage_test undo_storage_ndebug_test > build-p2.log 2>&1
cmake --build build -j4 > build-final.log 2>&1

# 直接运行二进制，不用没有 add_test 的 ctest 结果代替。
for test in undo_storage_test undo_storage_ndebug_test mvcc_protocol_test \
  mvcc_protocol_ndebug_test mvcc_publication_rpc_test prepared_2pc_test \
  prepare_notification_test lock_wait_test; do
  timeout 90s build/tests/$test > validation/$test.log 2>&1
  # 实际执行脚本记录每个退出状态并在非零时中止，见 validation/results.tsv。
done

# 最后补强了截断 Append 的 payload 中段/尾部验证，随后再次全量增量构建、重跑两个 P2 目标。
cmake --build build -j4 > build-final.log 2>&1
timeout 90s build/tests/undo_storage_test > validation/undo_storage_test.log 2>&1
timeout 90s build/tests/undo_storage_ndebug_test > validation/undo_storage_ndebug_test.log 2>&1
```

构建过程中修正过 CMake plain/keyword 签名混用、测试调用 RmRecord 的 const 类型，以及测试目标遗漏 load_table 链接；这些失败不计入通过结果。首次 P2 测试曾因 FATAL 不终止而产生无效 exit=0，本轮明确废弃该结果。**最终配置、全量构建与以下测试均 exit=0，使用启用 fail-fast 的最终源码。**

| 目标 | 实际结果 |
|---|---|
| undo_storage_test | 18 项死亡测试；只读 0 页；24 次交错覆盖跨页；1/2 帧预算反复淘汰；两 writer 子段与真实 RPC；行链 9 跳/子段逆序；WAL 初始页重建和重复回放；5 个截断 Append 边界与完整补入；主行引用/孤立 undo/等待依赖 |
| undo_storage_ndebug_test | P2 实现与同一测试另外以 NDEBUG 编译；上述边界全部通过，CHECK 仍终止子进程 |
| mvcc_protocol_test | 14 项死亡测试；5,040 个事件排列；64 个 Begin/Advance barrier 竞争；8 个并发 Reserve；SCN/可见性/SI/Abort 门槛 |
| mvcc_protocol_ndebug_test | P1 实现与测试以 NDEBUG 重新验证，全部通过 |
| mvcc_publication_rpc_test | 真实两端 Stage/Advance、保留 ACK、Context 导入/完成；普通组件测试，不表示业务集群已接入 |
| prepared_2pc_test | 既有 v1/v2 准备记录、上下文、在线回放与多流检查通过 |
| prepare_notification_test | 既有持久化通知、早到 Commit、汇合、协调者参与、身份隔离通过 |
| lock_wait_test | **110 passed, 0 failed** |

所有 P2 并发证据使用显式事件：目录请求到达后 S 阻止 X；缓存淘汰到达 WAL 等待屏障后帧尚未替换；WAL 已 fdatasync、主行已回放后 GetUndoPage 进入等待，人工发布 undo 回放前缀后才完成。没有用 sleep 证明这些条件。实际 LogWrite/fsync 批次数少于入队记录数。

新测试使用 `/tmp/undo-p2-test-XXXXXX` 独立数据目录，结束时只清理自己的目录。所有新 brpc 服务绑定 `127.0.0.1:0`；既有 lock_wait 的 43800–43802 端口在运行前实际 bind 检查空闲。无旧实验数据被改写。RPC 验证在 AliYun1 的同进程多个 loopback 服务间执行，不是多台主机验收。

日志保存在验证副本的 `configure.log`、`build-p2.log`、`build-final.log`、`validation/*.log`、`validation/results.tsv`。死亡测试 FATAL 来自预期反例，父进程确认 SIGABRT；最终日志没有父进程“断言未拒绝”的失败。根 CMake 会生成被忽略的 protobuf 文件；最终源码清单核对记录在 `SOURCE.sha256`、`source-check-final.log`，生成物清单见 `generated-protobuf.txt`。

## 未接入、未验证和下一阶段 P3

- 当前 DTX::TxBegin/TxExe/Commit/Abort、UndoDataItem、YCSB/SmallBank/TPC-C、Lazy/2PC/mix 仍走旧业务路径；没有用协议组件或临时主表测试冒充真实负载 MVCC。
- P1 PublicationService 与 P2 DirectoryService/CacheService 已实现并在真实 RPC 测试中注册；**生产服务注册、固定 epoch/成员配置、每节点 Manager/RpcPageAccess 生命周期和 DTX 调用尚待 P3/P4/P5**。ComputeNode 的独立缓存、ComputeServer 的 WAL 适配器和 storage 的 undo RPC/回放已进入实际构建。
- Begin/Finish 的逻辑段保留，以及页内容的 redo 重建已实现；没有恢复运行中目录分配器/请求号后继续接收事务，没有完整事务崩溃决议、自动故障接管、HA 或 torn page 修复。重放测试不能作为这些能力的证明。
- 未执行跨机器网络/RDMA、磁盘满/硬件故障、生产 workload 压测；没有性能收益结论。没有历史 GC、页复用、磁盘配额或完整逆序业务回滚。

P3 从以下接点继续，保持本轮边界，不重选 D1—D9：

1. `core/dtx/dtx.h:TxBegin/Clean` 接 P1 固定 Context 和 `undo::Manager::Begin`，明确生产宿主的 epoch、成员、目录与服务注册；单节点也须使用相同不变量。
2. `core/base/data_item.h`、Serialize、三负载加载器、RM 页容量和所有 memcpy 偏移联动设计当前行 creator/deleted/undo Ref。使用新格式和新测试数据，不能只改 sizeof 断言数字或静默兼容旧文件。
3. `dtx_exe.cc:ApplyWriteOperation`、`dtx.cc:GetDataItemFromPage*/UndoDataItem` 等真实 Lazy 入口：持行写权限检查 SI，调用 Append 保存完整旧行；不持主行页 latch 等待 undo 页/RPC/WAL；重检后挂链并记录同 writer 主行 redo。
4. 历史读取先复制候选，再调用 P1 Visible；FollowHistory 使用本轮 `Manager::Read(ref, expected RID/key)`，缺事务状态须解析，缺页须等回放；可见墓碑返回不存在。
5. Abort 用 `Tail/ReadRollback` 逆序恢复真实行并产生 redo；提交证据包含 undo/主行所需 WAL 和安装状态，全员发布完成前不交出行写权限。单写参与者不强加 Prepare。

本轮到 P2 存储基础交付结束。下一轮以真实单节点 Lazy 闭环的 P3 验收为目标，之后再推进多节点、2PC/mix 和 GC。
