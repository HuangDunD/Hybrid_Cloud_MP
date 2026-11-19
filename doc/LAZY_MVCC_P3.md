# P3：单节点 Lazy 当前实现

更新：2026-11-18。D1—D9 不变；依据本地 `09fed8e` 之上的未提交源码同步。单节点 Lazy 的真实 DTX 入口已经接到 SCN 与 undo，之后又统一了 Lazy 分支、简化回放并替换了 undo 页面访问实现。**P4 已在 AliYun1 重新构建并运行本文件的单节点测试；两节点 Lazy 也已通过，详见 [实施交接](UNDO_MVCC_IMPLEMENTATION_PROGRESS.md)。** 本文件仍描述 P3 的单节点契约，P4 的成员/发布/跨节点页路径不在此重复。

## 启动和数据边界

已经删除 `lazy_mvcc` 开关。`ComputeServer::IsLazyWorkload()` 为 `SYSTEM_MODE == 1 && WORKLOAD_MODE != 4`：满足这一条件的业务 Lazy 事务统一使用本文件的 Begin、读写、Commit 和 Abort 路径，不再保留可切回旧 Lazy 语义的分支。

P3 当时的启动仍要求：一个计算节点、节点 0、开启 WAL、`mvcc_epoch` 是 16 字节非零运行身份。P4 已移除单节点/节点 0 限制，改为固定协调点节点 0 和显式成员集合；WAL/epoch 要求保留。仓库默认配置仍是空 epoch 和三节点地址，不能直接运行；实验配置需指定 epoch。eager、2PC、mix 与 SQL 仍未因 P4 接入 MVCC。

`undo_buffer_pool_size_pages` 默认 10000；新增 `undo_lock_table_initial_capacity` 默认 10000，前者是共享帧预算，后者是锁表数组覆盖的页号范围。两者独立；范围外页锁通过 map 按需建立。取页和淘汰使用原 LPLM/GPLM、BufferPool 和 Lazy 页协议，详见 [Undo 存储协议](UNDO_STORAGE_PROTOCOL.md)。

`DataItem` 从 72 变为 168 字节，新增固定 96 字节 RowFields：MVC2 格式、initial、creator 的 epoch/home/local、56 字节稳定 undo Ref。`valid` 继续表达墓碑。锁 owner / timeStamp / commitTimeStamp 与业务 creator 分离；旧 prev_lsn 不作为历史地址。Serialize 和 Update redo 不保存进程 value 指针。RM 文件用 RMV2 魔数，容量按真实行头及 RmPageHdr 计算；旧格式数据必须重建。

初始化行 creator 全零、initial=1、Ref 为空。更新后的非初始化行有独立 creator 和非空 Ref。RowAt/CopyRow/InstallRow 核对 bitmap、RID/key、长度、格式与链头。YCSB value 为 1004 字节，TPC-C customer 为 664 字节，undo 最大 value 为 3750 字节。

## 当前函数与调用关系

| 入口 | 当前行为 |
|---|---|
| `DTX::TxBegin` → `LazyRuntime::Begin` | 先在 SnapshotNode 登记固定快照保护，再建立逻辑 undo 段；只读不分配物理页 |
| `DTX::TxExe` → `ExecuteLazy` | 处理未获取的读写项，读自身私有修改或固定快照版本，写项走行锁与 SI 检查 |
| `PrivateMvcc` | 倒序找同一行尚未安装的私有修改，让旧副本写回路径也能自读 |
| `FetchMvccWrite` | 原 Lazy 页 X 保护下取得元组锁、解析已结束锁持有者；释放页保护后检查版本创建者的提交 SCN |
| `InstallMvcc` | 一次真实覆盖：保存完整旧行到 undo，再安装新逻辑行并生成主行 redo |
| `DTX::TxCommitSingle` → `CommitLazy` | 补写私有修改、封口、等 WAL、发布提交，之后允许后继写者进入 |
| `DTX::TxAbortWorkLoad` → `AbortLazy` | 按已安装覆盖的逆序读 undo 恢复主行，等回滚 WAL 后结束事务并释放行锁 |

旧名称 `ExecuteMvcc / CommitMvcc / AbortMvcc` 已改为上述 Lazy 入口；没有“普通 Lazy Abort”和“MVCC Abort”两套可选行为。eager/2PC/mix 的旧执行与回滚代码不能在其 undo 尚未接通前一起删除。`dtx_mvcc.cc` 中已补各函数及关键顺序的中文注释。

## 固定快照与读取

Begin 不请求一个新的唯一快照号，而是在 SnapshotNode 的 mutex 内复制当前 `safe_scn`，同时登记历史保护，再返回不可变 Snapshot。同一个安全边界上开始的多个事务可以使用同一 snapshot_scn。P3 把 TxnId.local 放进 `start_ts/tx_id` 供事务和锁识别；这些数字不参与可见性比较。

读者先通过 BLink 找 RID。优先读本事务尚未写回的私有修改；否则在原 Lazy 页 S 保护下复制当前业务值、creator、删除标记和 Ref 的一致组合，释放主页面保护后再调用 VisibleRow。候选判定顺序为自身版本、初始化版本、其他已提交且 `commit_scn <= snapshot_scn` 的版本；不可见就沿 undo 读前一版本。可见墓碑立即返回不存在，不再往前找未删除值。

Unknown creator、错误 epoch/RID/代次、断链明确失败；正常页面权限或回放等待继续等正确依赖。读取不以行锁是否清理作为版本提交依据。

例如初值 100，R1 在快照 10 开始。W1 更新 90 并在 SCN 11 提交，R2 随后取得快照 11。W2 再更新 80 时，自读为 80，R1 仍读 100，R2 仍读 90；W2 在 SCN 12 提交后新快照读 80，R1/R2 保持原结果。

## 更新、提交和 Abort 的顺序

1. FetchMvccWrite 在页保护内取得行写锁。普通锁冲突按 NO_WAIT / WAIT_DETECT 处理；取得锁后，如果其他创建者已在本快照之后提交，返回正常 SI 冲突并标记事务失败。`TxExe(false)` 只延后执行 Abort，不把失败事务重新变成可提交状态。
2. 回调在 ExecuteLazy 中执行并经 InstallMvcc 安装；无回调的私有副本修改在 CommitLazy 按读写集顺序经同一入口安装。私有副本可以被后续回调读取、再安装；安装后同步当前及更早条目的基准，避免旧副本在提交时覆盖后续结果。
3. InstallMvcc 复制旧主行后释放页保护，保留行写锁；Append 保存完整旧值并先入队 undo redo；重新取主页 X，重检锁和旧逻辑行，再一致安装新值/creator/deleted/Ref，并入队同 writer 的主行 redo。`main_lsn > undo_append_lsn`。每次实际变化独立追加；仅加锁或最终逻辑值不变时不制造新版本。
4. CommitLazy 补齐未安装修改后 Finish 封口。若取得过行锁，入队 BatchEnd 并等待此前 undo/主行/锁日志所需的 WAL 前缀持久化。实际修改过业务行才调用协调点 Reserve/Decide/Installed/WalDurable；本地单成员 Stage/Advance ACK 全部完成后才结束快照、通知后继写者并尝试延迟清锁。只读或只加锁不占提交 SCN。
5. AbortLazy 从 `mvcc_tail_` 按 sequence 和 `previous_txn` 逆序读取旧行，重新取主页 X，校验当前 creator/链头后恢复旧值、旧 creator、墓碑和旧链头，每次恢复生成 Update redo。锁 owner、等待队列不从历史恢复。已淘汰的主行和 undo 都重新经正常页面路径取得，不依赖原私有内存副本。
6. 回滚完成后封口，等待回滚所需 WAL，再把 MVCC 状态设为 Aborted；随后释放/交接行锁，最后发布锁管理器 ABORTED 并清理等待关系。尚未安装的私有修改不在回滚链上，直接撤销。

TPC-C 的 `valid=0` 经同一安装路径形成有 creator/Ref 的墓碑，不清 bitmap、不删 BLink 唯一入口、不复用 RID。通用 insert_set/delete_set 不在本阶段，ExecuteLazy 对其明确 CHECK。

## 回放、持久化与测试状态

当前 `LogReplay::CheckHistory` 替代了原 HistoryReady/历史等待队列。P3 的 undo 和引用它的主行 redo 位于同一顺序 WAL 流，存储按流内顺序同步回放，所以主行应用时对应 undo 应已完成；不满足就 CHECK。普通主页面的跨流前驱队列仍保留。

存储 GetUndoPage 仍需等目标页回放。日志 durable 只是已有重建依据，不能代替目标 undo 页已可读。`Store::Ready` 是回放侧非阻塞不变量检查，`Store::ReadPage` 才是缺页/回放未到的等待入口。不能在唯一回放线程里阻塞等待它自己还没执行的同流记录。

主行通用 CheckSameRedo 已删除，LogWrite 自动重试关闭；undo 页级重复回放校验仍在。计算端批量刷新仍在，存储端每个非空 LogWrite 批次一次 fdatasync，没有 Leader/Follower 跨请求合并。Store::Sync 只在排空及显式组件测试调用，不逐条 undo 同步。

当前测试源码覆盖固定 100→90→80、自读、SI/普通锁冲突、重复覆盖、私有写回、仅加锁、逆序 Abort、墓碑及保留入口。`run_lazy_mvcc.py` 使用新 epoch、新数据、独立端口、1 帧 undo/16 帧主表；旧目录/缓存夹具已删除，真实 DTX 用原 Lazy 淘汰路径。**这些是当前待运行的测试入口，不是本次通过结果。** 新锁表容量边界也需要补充实际验证。

## 当前限制与后续入口

先在用户允许验证后重新验收 P3，不能用之前独立 undo 页协议版本的成功记录证明当前 LPLM/GPLM 接入已通过。没有完整事务终态恢复、旧 epoch 接管、GC/页槽复用、历史磁盘配额或性能结论；每次隔离验证仍生成新数据。

P4 已把这些入口接成两节点 Lazy：LazyRuntime 持有全员成员集合、协调点固定节点 0、跨节点 creator 状态走 `QueryStatus`；ComputeServer 去掉单节点启动约束；原 LPLM/GPLM 的正在追加页/封口页迁移、取页、淘汰和 main/undo 淘汰后回滚已实际验证。**没有需要继续扩展的 LocalAccess 或独立 undo Directory/CacheService。** 2PC/mix 留 P5，GC 留 P6。
