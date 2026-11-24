# SCN / MVCC 正常运行协议（P0 / P1）

本文保留了旧 SCN 发布协议的设计记录。当前实现使用 remote_server TSO 和事务所在节点的提交状态表。[当前协议和计时说明](benchmark_stats.md#tso-和-undo-耗时)

制定于 2026-11-17，按 2026-11-19 P5 源码更新；HEAD / 设计基线为 `09fed8e`，实现位于未提交改动中。本协议细化 D1—D9。P1 状态机已接到单节点 Lazy 的 DTX、MVC2 主行和实际 WAL；P4 把两节点 Lazy 接到真实全员 Stage/Advance、Commit RPC 和跨节点 creator 状态查询；P5 已把普通 2PC、Cornus v1/v2 和 mix 的分布式分支接到同一 Context、Prepare/undo 证据和全员发布；mix 的 Lazy 腿复用 Lazy。P7 在当前环境完成整体验收，语义不变；矩阵、性能与未验证项见 [P7 验收报告](UNDO_MVCC_P7_ACCEPTANCE.md)。

## 1. 身份、字段和运行范围

- 集群有一个共同的非零 16 字节运行代次 `epoch`，所有成员与协调点一致；每次重建空集群使用新的代次。它既可在 `mvcc_epoch` 显式配置，也可留空由 `remote_server` 启动时生成一次并通过 `timestamp_service.GetEpoch` 提供给所有 compute 节点；显式值优先。它是正常运行的 fencing 身份，不提供重启恢复。不能给已有数据随意换代次后继续运行。
- `TxnId = (epoch, home_node, local_id)`；local_id 非零、home 内不复用，可以批量领取。MVCC 的 `tx_id` 就是 TxnId.local；历史锁/等待表身份使用 `start_ts = (local_id << 8) | home_node`，低字节带 home 以避免不同 home 的同 local id 碰撞，二者都不能当作 snapshot_scn。非 MVCC 的 SQL/eager 模式仍走旧时间戳入口。SCN 从 1 连续分配，0 是初始化边界；溢出直接 CHECK。
- `Context = (TxnId, snapshot_scn)`，Begin 在本地边界互斥区内登记后才返回，期间不通信。参与者必须复制完整 Context；相同身份的快照矛盾直接失败。
- 版本候选保存独立的 `creator`、`initial`、`deleted`。初始化版本显式 initial=true、creator 为空，无需状态查询。普通版本不从 lock owner 推导 creator。自己的已执行版本优先；可见墓碑返回 NotFound；不满足条件返回 FollowHistory；未知创建者返回 NeedStatus，调用者必须解析，不能跳过未知状态。
- WAL 位置是 `(epoch, writer_node, lsn)`，仅同流比较。snapshot_scn、commit_scn、tx_id 和 WAL LSN 不互换。
- 成员集合在 epoch 内固定，包含所有可能 Begin 的计算节点（也包含没有写参与的节点）；协调点不按超时剔除成员。缺 ACK 停止发布，RPC 失败明确失败，不自动 Abort 已决定提交的事务。新成员加入、恢复和 HA 留待后续。

## 2. 提交分配、状态与证据

唯一 `CommitCoordinator` 串行分配位置。Reserve 冻结 Context、写参与者及每个参与者所需 WAL 前缀；重复请求必须完整相等，返回原位置。读事务不 Reserve，结束后只注销快照。事务状态为 Active / Preparing / Installing / Aborting / Committed / Aborted / ReadOnlyFinished。

SingleWriter 只有一个实际写节点，不强加 Prepare。TwoPhase 和 Cornus 有至少两个写节点，要求每个写节点的 Prepare 持久化证据；TwoPhase 另要求 home 决议 WAL，Cornus 使用完整的持久化 YES 集合。写者 WAL 目标由具体适配器冻结：Lazy / 单写包含 undo、主行及结束记录；prepared 路径允许已有 Prepare 持久化的完整 undo、新值和映射作为重建证据，不自动为异步最终日志新增 fsync。P5 的 Plan 显式保存 `writers`：单写必须恰好一个 writer；TwoPhase 的 prepare/wal 前缀覆盖全体 writer 且必须有 home 决议位置；Cornus 可以用每 writer 的 Prepare LSN（v1）或存储端观测到的完整持久 YES 集合（v2，`PreparedQuorum`）证明，二者不能为空。协调点只消费证据，不伪造 WAL。

`DecideCommit` 是协调点不可逆选择，之后状态为 Installing，不能因延迟 Abort。Prepare / WAL / 决议持久化 / Installed 是独立完成事件，网络通知允许乱序；只有全部必要证据到齐才能发布。Installed 证明本写参与者全部写项已经安装并仍持有行写权限；它不证明 WAL 持久化。Prepare ACK 证明请求固定的准备前缀已持久化；Wal ACK 证明该写节点固定目标前缀持久化；决议 ACK 只证明 home 决议前缀持久化。接收较旧持久化前缀是合法乱序，不能倒退已观察水位，也不能满足较大目标。

允许 Abort 的阶段为 Active / Preparing、且尚未 DecideCommit。Abort 冻结每个写节点的回滚 WAL 目标，进入 Aborting；所有写节点显式报告 rollback 完成（包含其 WAL 等待）后，原位置成为永久 Aborted 空位。没有位置的事务可本地结束；已分配位置不回收、不重新编号、不当作提交。

## 3. 两轮发布与锁交接

每次只发布连续前缀的下一个位置，方便证明正确性；更高位置可并发准备、写日志和安装。P1 不预设吞吐优势。

1. head 的状态为 Installing 且所有提交证据到齐，或 Aborting 且回滚证据到齐，协调点形成不可变 `Resolution(Context, slot, outcome)`。提交的 commit_scn=slot；Aborted 的 slot 只填洞，不是可见提交 SCN。
2. 向固定全员发送 Stage。每个节点在本地 mutex 内安装完整终态及对应 SCN；相同位置、相同事务的合法重复严格核对所有内容后重 ACK。Stage ACK 代表终态已可供本节点查询，且保留到未来安全 GC；不代表页面回放、不代表 Begin 边界推进。
3. 协调点收齐不同成员的 Stage ACK 后，才发送 Advance。此时所有状态查询节点都有该终态，且所有写参与者安装完成。各节点在与 Begin 相同的 mutex 下将本地 safe_scn 从 slot-1 推到 slot。Advance ACK 代表该推进已完成；重复 ACK 不多计票。
4. 全部 Advance ACK 到齐后，`Succeeded` 才为真，`CanReleaseWrites` 才为真；此时所有后续 Begin 均 >=slot。之后才能释放/过户写权限或对外成功。Abort 填洞同样完成两轮发布，恢复完成前不交锁。

Stage 后、Advance 前，局部状态表已是 COMMITTED，但任何本地新快照仍 <slot，所以该版本仍不可见。一个节点 Advance 后、另一个节点尚未 Advance，前者可以 Begin 于 slot 并向后者导入 Context，因为所有节点 Stage 已完成。后者自己此时 Begin 于旧边界也合法：提交尚未成功返回。这不要求跨节点瞬间原子写计数器。

状态单调保留，不删除终态。协调点发布互斥区和节点 mutex 不覆盖网络或页操作；消息由 outbox 取走后发送。Stage / Advance 的 ACK 含 epoch、节点、阶段和完整 Resolution，矛盾重复内容 CHECK；合法迟到旧消息仍校验后确认。后一 slot 的 Stage 只在前一 slot 全部 Advance ACK 后产生，因此未见前驱的未来 Stage 是协议错误，而不是可忽略空洞。

锁清理必须查询 `CanReleaseWrites`，不能仅凭 `Status::Committed` 提前交锁：后者在发布轮开始时就已确定，而前者要等全员边界 ACK。Preparing 下的准备证据集合区分部分准备和全部 YES、尚未决议；无需另造可被倒退覆盖的 PREPARED 终态。未知状态保留为显式缺值；本地 Lookup 或协调点 Lookup 供后续状态解析使用，不能把锁等待表的旧 commit_ts 当成该状态。

## 4. 具体并发时序

- **低 11、高 12**：初始 safe=10。11 缺 WAL；12 已安装且持久化。Begin 仍为 10。11 就绪后先 Stage/Advance 11，所有 ACK 后才 Stage/Advance 12。任何已经发出的 10 都不会后来看到 11；不存在先发 12 再补 11。
- **Abort 填洞**：11 在 Preparing 决定 Abort，先等全部 rollback 与相应 WAL；11 广播 Aborted 并完成两轮后允许 12 发布。快照 11 不包含被中止版本；编号永不重新用。
- **乱序**：Installed 可早于 WAL 通知，某节点 Stage ACK 可比其他节点早到，重复 ACK 不能代替缺失节点。协调点不满足门槛便没有 Advance；没有完整 Advance ACK 就没有成功。旧 ACK 可跨后续 slot 迟到，只校验不改变当前轮。
- **并发 Begin**：Begin 与 Advance 在同一节点 mutex 上线性化：前者先则 snapshot=旧边界且已经登记，后者先则 snapshot=新边界且终态已在所有节点。快照对象只读，不随广播变化。
- **成功后跨节点 Begin**：成功检查依赖全员 Advance ACK，每个 ACK 都在本地 safe 已更新之后；因此成功返回后的任何 Begin 包含该提交。正在进行的老事务保留旧 snapshot。
- **安装半笔事务**：两写者之一完成 Installed 不足以 Stage；读候选仍不可见。所有参与者完成前没有任何快照包含本事务。

## 5. 快照登记与未来 GC

Begin 与本地 safe 推进、保留水位采样使用同一 mutex；home 在返回 Context 前登记。home 导出上下文时登记参与者引用；远端 Attach / Finish 后将完整 Context ACK 回 home；home 必须收齐所有导出引用完成才可 End。参与者不重新 Begin，也不能把导入快照换成本地 safe。合法重复 Attach / Finish / ACK 保持幂等，结束后的迟到 Attach 不重开事务。

本地保留报告为 `min(safe_scn, 所有尚活跃 home 快照)`，不活跃时仍报告 safe 而非无穷。新 Begin >=safe，因此报告只能单调前进。远端导入的老快照始终受 home 未解除的登记保护；home 不可在远端工作尚未排空时结束。全局 GC 将来取固定全员最新报告的 min，保留最老快照需要的边界版本及其到链头的路径，并额外保护未决事务、回滚、读取引用、回放依赖。旧报告只会保守滞后；失联节点不移除。P1 没有回收操作，状态表不删除；P6 才把这些约定接到真实 GC。

## 6. Undo 共享约定（P2 实现）

Begin 的逻辑段身份就是 TxnId，不分配物理页。子段为 `(TxnId, writer_node)`，回滚链按子段实际覆盖序号逆序，行历史链独立跨事务连接。完整旧逻辑行包含表/RID/key、业务字节、旧 creator、deleted、旧前驱，不包含锁、队列或地址；每次实际覆盖都保留。

页身份采用 `PageKind::Undo + epoch + writer + number + generation`。完整身份保存在页头/Ref，接入现有页协议时使用 `UNDO_TABLE_ID=30000`，页号映射为 `(number-1)*MAX_REMOTE_NODE_NUM+writer+1`，其中 MAX_REMOTE_NODE_NUM=100。Manager 按写节点维护所有事务共享的原子计数器，一次分配一张 4 KiB 页；不是每事务预分配 100 页。稳定引用另含 slot、记录代次和目标追加 LSN，日志流身份来自 epoch/writer。独立 `undo_buffer_pool_size_pages=10000` 为每节点帧预算；`undo_lock_table_initial_capacity=10000` 为原锁表预分配范围，越界条目按需建立。`PageUsesWal` 显式包含 undo，不能因大表号跳过 WAL。当前只追加、不复用，页/记录代次固定为 1；身份、长度、完整性和边界都检查。

持行写权限后预留 undo 资源；不持主行页 latch 等其他页/RPC/WAL。准备完整旧值及 redo，再在主行页保护下重检锁权限与旧版本，原子安装值/creator/deleted/undo 引用。读者在短页保护内复制候选后释放，再遍历历史。undo redo 与实际写者 WAL 共流，计算端没有独立脏页落盘；存储端回放写页。已持久化不等于已回放，缺历史等待准确依赖，不能返回最新行。

当前实现见 [Undo 存储协议](UNDO_STORAGE_PROTOCOL.md)：Manager 通过 ComputeServer 的 PageAccess 复用原 LPLM/GPLM、Lazy S/X、BufferPool、Pending/PushPage 和淘汰握手，原独立 DirectoryService/CacheService 已删除。Begin/Append/Finish/Read/ReadRollback 已接入单节点 Lazy；Finish 仍只封口，不注销快照或替代提交发布。P3 的同流主行 redo 用 CheckHistory 检查 undo 已完成，不再维护历史等待队列；存储 GetUndoPage 等目标回放的正常等待保留。后续代码尚未重新验证。生产宿主及测试入口继续启用 `common/fatal_checks.h`，不能把只打印 FATAL 当作断言通过。

## 7. 当前源码入口与阶段边界

| 路径 | 当前入口与行为 | 后续工作 |
|---|---|---|
| Begin / 清理 | `dtx.h:TxBegin/Clean`，LazyRuntime::Begin → SnapshotNode::Begin → Manager::Begin；固定快照与身份分离 | P5 远端 Context/2PC 参与者 |
| Lazy 读取 | `dtx_exe.cc:TxExe` → `dtx_mvcc.cc:ExecuteLazy`；PrivateMvcc 或主行 CopyRow → VisibleRow → Manager::Read | P4 已跑两节点跨节点历史；P5 2PC 读 |
| Lazy 写入 | `FetchMvccWrite/InstallMvcc`，原页 X/行锁、SI 检查、先 undo 后主行 | P4 已跑迁移/淘汰/并发追加；P5 2PC 安装 |
| 旧私有副本写回 | 2PC 的私有副本由各写参与者 `Commit` 安装：先按 Prepare 的 undo Ref 核对并把旧值写入本节点 undo，再写新值/删除标记；Lazy 走 `CommitLazy`；TPC-C valid=0 同入口 | P5 已接 undo/发布；单写捷径不加 Prepare |
| Lazy 回滚 / 延迟清锁 | `AbortLazy` 逆序读 undo；CommitLazy 在发布后调用 FinishDeferredTxn/TryCleanCommittedRecord | 当前 Lazy 已统一，其他模式的旧回滚仍保留 |
| undo 页面 | `Manager` → `ComputeServer::FetchUndoPage/ReleaseUndoPage` → 原 Lazy LPLM/GPLM/BufferPool | P4 已验证两节点迁移/淘汰/交接；P6 GC/复用 |
| 主行回放 | `LogReplay::CheckHistory`：本事务创建新版本的 redo 仍要求同 writer undo 先完成；解锁/Release/回滚恢复的 redo 带完整行，可引用其他 writer 的既有历史 | P5 已收窄检查范围；完整恢复等待留 P6 |
| 2PC 读 / 加锁 | 参与者按固定 Context 从页中复制候选；S 读沿 undo 出可见版本，X 写返回锁定行，协调者做 SI 检查 | P5 已验证本地/远端读、自读、SI |
| 2PC Prepare / Commit / Abort / Release | `TxPrepare`、`prepared_txn.h`、`twopc_server.cc:Prepare/Commit/Release/Abort`、`dtx_twopc_mvcc.cc` | P5 已接 undo 准备、统一 commit_scn/发布、发布后解锁；零/单/多写与 Cornus v1/v2 已验证 |
| mix | `DecideCommitMode/DecideMixWithCostModel/TxExe/TxCommit/TxAbortWorkLoad` 的 SYSTEM_MODE=4 路径 | P5 两分支共用 Context，调度策略不变；Lazy 腿走 ExecuteLazy，2PC 腿走统一参与者协议 |
| YCSB / SmallBank | 原读写集合和回调；SmallBank LongTx 还有先取私有副本、后添加回调的路径 | 已进入 Lazy 公共入口；逐函数完整矩阵仍待后续验收 |
| TPC-C | 现有预置记录更新，Delivery 的 valid=0 保留 RID/索引 | P3 单节点、P5 2PC/mix 两节点已接；小表页 0 路由已修 |
| worker / SQL | `worker.cc` 无回调副本修改由 Lazy 提交写回；WORKLOAD_MODE=4 不进入 LazyRuntime | SQL MVCC 不在范围内 |

当前 RMV2/MVC2 主行布局已扩展 DataItem 为 168 字节，旧 72 字节文件需重建；Serialize、Update redo、加载器和页容量使用该格式。详细字段与事务顺序见 [P3 接入说明](LAZY_MVCC_P3.md)。

## 8. 发布通信的实际接入程度

Lazy 写事务安装主行并封口 undo 后，以尾日志的目标 LSN 请求刷盘，同时到固定协调点预约一个 SCN。每笔预约立即向全员发送自己的 Pending，只记录事务身份和位置，允许比队首更高的位置先收到全部预通知确认。只有轮到队首、收齐本笔 Pending ACK 并取得对应写者的 WAL 持久化证据后，协调点才发送该笔的 Advance。成员在同一把锁内登记提交终态并推进可读 SCN，全员确认后才返回成功和开放后继写者。较快成员导出的新快照在较慢成员等待对应 Advance，已有快照保持原值。启用此路径的计算成员须统一支持 Pending 和 ReserveLazy；每轮只处理一笔事务。Begin 直接取本地有效边界；它不经 RPC 单独领快照号。

生产计算服务注册 `PublicationService`，`publication_rpc.{h,cc}` 提供 Stage/Advance、远端 Context Attach/Finish、Commit 和 QueryStatus 的服务和客户端适配。P4 LazyRuntime 显式持有全员成员集合；协调点固定为计算节点 0，本地提交走 `SubmitCommit`，其他节点提交走 `Commit` RPC，发布线程在 ACK 收齐前不完成。原独立 undo DirectoryService、CacheService 和 LocalAccess 已删除，不能作为下一阶段入口。

P5 已把远端 Context/Prepare/Commit/Release 接到 2PC/mix 业务参与者：home 用 `ReserveTwoPhase`/`CompleteTwoPhase`（或合并的 `SubmitTwoPhase`）冻结 writers、prepare 前缀、Cornus quorum、决议位置和 installed 集合，收齐两轮 ACK 后返回；单写捷径安装后一次性 `SubmitTwoPhase`。Commit RPC 携带全局 commit_scn，参与者用 Prepare 的 undo Ref 安装并保留行锁，Release 在发布后执行。home 在所有参与者完成显式行锁交接后，才将锁等待表从 ACTIVE 原子切换到终态。普通 Abort 先排空未安装的私有写锁，再登记 ABORTED。重复 Prepare、Commit 和 Release 共享各自的完成结果，并核对快照、提交位置和请求内容。协调点用 bthread 条件变量等待发布，每笔事务只唤醒等待其结果的提交者。此前的验证记录来自同机 loopback 两进程，未跨物理机、未做协调点故障切换或完整恢复。
