# Undo 存储协议与当前接入边界

更新：2026-11-19。依据本地 `09fed8e` 之上的未提交源码同步；D1—D9 不变。P7 已复核页面/undo/redo、回放和受控容量；P6 GC/页复用仍延期。P2 存储组件已接到 P3 单节点 Lazy，P4 又接到两节点 Lazy 的真实主行/undo 页迁移、淘汰、历史读和回滚。**P4 已在 AliYun1 上重新构建并运行组件、P3 单节点和两节点集成测试；跨物理机、3+ 节点、协调点故障和 P6 GC 未验证。** 旧版运行记录保留在 [实施进度](UNDO_MVCC_IMPLEMENTATION_PROGRESS.md) 的历史部分。

## 1. 身份、逻辑段与分配

- `TxnId = (epoch, home, local)`；epoch 是 16 字节非零运行身份，可显式配置，也可由 `remote_server` 生成一次后经 `GetEpoch` 共享给所有 compute 节点（显式配置优先）。`Manager::Begin(Context)` 只登记固定快照和逻辑段，不分配物理页。相同事务再次 Begin 必须带相同快照，已经 Finish 的段不能继续追加。
- 全局逻辑段属于 TxnId，各实际写节点管理 `(TxnId, writer)` 子段，各用自己的 WAL、覆盖序号和逆序链。P4 已接通两节点 Lazy；每节点只创建自己的 undo 子段，跨节点读取通过稳定 PageKey 路由。2PC 各参与者的接入留 P5。
- `PageKey = (PageKind::Undo, epoch, writer, number, generation)`。每个节点/epoch 有一个 Manager，`next_page_` 是所有本地事务共用的原子计数器。首次 Append 才递增并分配一张 **4 KiB** 页；页满先 Seal，再申请下一张，不预分配固定大小的事务区间。
- Init redo 同时记录分配和初始化，包含完整页身份及段 TxnId。Finish 只封口子段，不替代 Commit/Abort 或快照注销。
- 当前页与记录代次固定为 1，只追加、不复用、不 GC；Manager 段信息和事务终态也保留。计数器是运行时状态，没有恢复后重新接收写入的分配器恢复流程。

### 复用页协议时的编号

undo 在现有页路由中使用独立的 `UNDO_TABLE_ID = 30000`，`PAGE_TABLE_SLOT_COUNT = 30001`。完整 PageKey 仍保存在页头和 Ref 中，交给 LPLM/GPLM 的整数页号由 `format.cc` 映射：

```text
page_id = (number - 1) * MAX_REMOTE_NODE_NUM + writer + 1
MAX_REMOTE_NODE_NUM = 100
```

例如三个节点同时 Begin，都不分配物理页。首次实际写入时，各自的本地 number 都可以是 1，映射页号分别为 1、2、3；下一张分别为 101、102、103。**这是交错编号，不是每次分配连续 100 页。** writer 由页号反解，因此各节点不需要为每张新页向中心申请号码，也不会因本地 number 相同而冲突。页号上限、writer 范围和代次都检查；这个编号例子不表示当前已经支持三节点业务运行。

`ComputeServer::get_node_id_by_page_id` 将 undo 页的 GPLM 路由到其固定 writer。存储文件仍按 epoch/writer 分开，文件内使用本地 number；映射后的 page_id 不直接作为该文件的偏移。新页先取得现有 GPLM 的 X 权限，要求其目录 LSN 为 0，再在缓冲帧中初始化。不能用这个入口覆盖已经存在的页。

## 2. 页、记录与两套链

数据页固定为 4096 字节。payload 使用显式小端整数和长度字段，不序列化 C++ 对象或指针。WAL 外层仍使用项目现有日志头，不声明跨 ABI 兼容。

| 部分 | 大小 / 内容 |
|---|---|
| 页头 | 128 字节：magic、format=1、40 字节页身份、28 字节段 TxnId、init/current/seal/seal_previous LSN、used、count、保留区 |
| 记录页内头 | 32 字节：payload 长度、完整标记、记录代次、slot、追加 LSN、页级前驱 LSN |
| Record 固定字段 | 178 字节：子段覆盖序号、56 字节 previous_txn、逻辑旧行固定字段 |
| 逻辑旧行固定字段 | 114 字节：format、table/page/slot/key、initial/deleted、旧 creator、56 字节行历史前驱、value 长度 |
| 页尾 | 8 字节 FNV-1a 校验；undo redo 也有独立校验 |
| 最大 value | `4096 - 128 - 32 - 178 - 8 = 3750` 字节；记录不跨页、不截断 |

完整旧行包含业务值、RID/key、旧创建者、初始化/删除标记、旧历史引用，不含锁 owner、等待队列或进程指针。当前主行采用 MVC2/RMV2，`DataItem` 是 168 字节；旧 72 字节格式需重建。YCSB value 为 1004 字节，TPC-C customer 为 664 字节，当前负载值的尺寸检查仍在 `undo_storage_test::CheckFormats` 中。

稳定 Ref 包含完整 PageKey、slot、记录代次及追加 LSN。LSN 是目标记录的就绪依赖和校验字段，不是独立历史地址；日志流身份来自 `(page.epoch, page.writer)`。编码后的空引用为全零 56 字节。读取核对页身份、代次、完整标记、长度、边界、目标追加 LSN 和预期 RID/key。

两条链分开：`Record.old.previous` 是同一行的旧版本链，可以跨事务；`Record.previous_txn` 与连续 sequence 是本写节点子段的逆序覆盖链，可以交错包含多行。`ReadRollback` 另核对段 TxnId 和 sequence。每次实际覆盖单独保存旧行；Seal 后不能追加，已有记录内容不能改成另一条历史。

## 3. 缓冲池、LPLM/GPLM 与页面迁移

已删除原来的 `undo/directory.{h,cc}`、`undo/buffer.{h,cc}`、`undo/page_rpc.{h,cc}` 和 `undo/page.proto`，不再使用独立的 DirectoryService、CacheService、grant 或私有 cache tag。

当前 `Manager` 只通过 `PageAccess` 调用宿主；生产实现是 `ComputeServer`：

```text
Manager → ComputeServer::FetchUndoPage / ReleaseUndoPage
        → rpc_lazy_fetch_s/x_page / rpc_lazy_release_s/x_page
        → 现有 LPLM、GPLM、GlobalValidTable、Pending / PushPage / BufferReleaseUnlock
```

`PageAccess` 是接口适配，不另实现权限协议。读取申请 S，追加/封口申请 X，只有固定 writer 能通过 undo 接口申请 X。Fetch 返回受保护的真实 BufferPool 帧，Release 结束本次访问；远端权限和缓存可按 Lazy 规则保留，缓存命中不要求每次重新发网络请求。正在追加页和封口页使用同一套整页权限。P4 已用两节点真实 DTX 验证：B 读取 A 正在追加的 undo 页会等整页 X 交接；A 继续追加/回滚不依赖旧内存地址；远端缓存缺失时走原权限/存储依赖。跨节点的持页者推送入口识别 undo 页头并等待实际页 LSN。

负载模式的 ComputeNode 为 `UNDO_TABLE_ID` 建一个独立的原有 `BufferPool`，供本节点所有表、事务共享；不占主表/BLink/FSM 的预算。SQL 构造入口未启用这一套 undo/MVCC。

| 配置 | 默认值 | 含义 |
|---|---|---|
| `undo_buffer_pool_size_pages` | 10000，必须大于 0 | 本节点 undo 缓冲帧总数，不是事务页数或磁盘历史上限 |
| `undo_lock_table_initial_capacity` | 10000，允许为 0 | LPLM/GPLM/GlobalValidTable 初始数组覆盖的映射页号范围；与帧数独立 |

锁表不再有 `sparse_` 开关：页号在 `[0, capacity)` 内直接查预分配数组，超出范围才在带 mutex 的 `entries_` map 中按需创建。`entries_` 保存页锁或有效性信息，不保存页内容。capacity=10000 表示覆盖页号 0..9999，**不是每个 writer 预留 10000 张页**；capacity=0 则全部按需创建。越界条目暂不回收，未测量性能收益。

现有 BufferPool 的 unpin 会将 pin_count 清零，不能把它理解成通用引用计数。当前访问互斥和淘汰资格仍由原 LPLM/LRU 协作控制。本轮相关改动是：

- `checkIfDirectlyPutInBuffer` 先找本分区空闲帧，再逐个找其他分区；例如 1 帧池不再因页号落到另一分区而误判无空闲帧。
- undo 调用 `replace_page(..., wait_for_frame=true)`；暂时没有可淘汰帧时等待/让出执行，再尝试，不把正常帧压力直接断言掉。
- `ReleaseLazyPageForEviction / CancelLazyEviction / FinishLazyEviction` 共用原 GPLM 淘汰握手。`ClearUndoCache` 仅在无并发访问的清缓存场景使用，同样经过权限释放及 WAL 等待，不直接丢帧。
- `PageUsesWal` 明确包含主表和 undo，`PageLsn` 从 undo 页头读 LSN。脏 undo 的推送、交接、淘汰继续等对应 writer WAL，不能落入索引/FSM 的跳过等待或直接刷整页分支。

计算端不写 undo 最终页文件。缓存未命中且没有计算节点持有效页时，原 Lazy 流程携 GPLM 给出的页 LSN 调用存储 `GetUndoPage`。这与普通 `GetPageWithLsn` 一样要等回放，但 undo 用完整 PageKey 定位独立文件，不能按 RmPageHdr 解码。

## 4. WAL、回放和等待

`MakeUndoWal` 当前只提供 enqueue：在原 log_mtx 中分配该节点 LLSN，并通过 `AddMetadataLog` 入原 WAL 队列。Init/Append/Seal 的 redo 先入队，再应用到持 X 权限的 undo 缓冲页。`Manager::Append` 返回 Ref 后，DTX 才安装主行并入队主行 redo。提交、回滚、页面转移和淘汰分别沿现有路径等所需 WAL 前缀；不为每条 undo 单独 fsync。

当前 WAL 为每计算节点一个文件 `LOG_FILE_NAME_<node>`。计算端按时间、记录数及显式 flush 目标组成批次；存储端 `LogManager::AppendAndSync` 对每个非空 LogWrite 批次写入该 writer 文件并做一次 fdatasync，随后公布 durable 字节边界。**当前没有存储端 Leader 汇集多个 Follower LogWrite 请求、再合用一次 fdatasync 的组提交实现。** 多条日志共用一个计算端批次仍然存在；统计字段 `group_commit_logwrite_sum` 不能作为 Leader/Follower 合并存在的证据。

存储端只有一个回放线程，轮询各 writer 的 durable 前缀，每个流内按文件顺序同步 Dispatch；跨文件之间没有统一到达顺序。普通主页面仍保留 `waiting_` 页前驱队列，迁移后同一页的前驱可能来自另一 writer。不能因为 P3 单节点不需要乱序历史队列，就删掉这项已有多流机制。

undo 的 `Store::Submit` 维护每个 PageKey 的前驱队列，完整写页后推进该页 applied LSN 并唤醒读者。文件为 `UNDO_<epoch hex>_<writer>.pages`，第 0 页是空间头，数据页用本地 number。Store 没有另建常驻数据页缓冲池。

### 主行引用和历史读取的区别

MVC2 的主行 UPDATE 复用原有 flags 字节，最低位仍表示附带旧行，另两位标记本次是安装新版本（Install）还是保留已有历史引用（Retain）。Lazy 覆盖、2PC 安装使用 Install。回滚恢复旧行、解锁和交接使用 Retain。存储回放 Install 时核对 writer、同流 LSN 先后、undo 页所属完整事务身份及记录的表、RID 和 key。Retain 走主行页前驱队列，可携带另一 writer 生成的历史引用。旧格式的 0/1 字节仍可读，对能识别为同流的历史引用执行顺序检查。

`GetUndoPage` 的等待仍保留：**WAL 已 durable，但回放线程尚未执行到目标页**时，读请求通过 `Store::ReadPage(PageKey, required_lsn)` 等待。回放到目标后再检查页身份/校验并返回；文件尚未创建、文件洞、页未回放都不能当作历史终点。`WaitForReadWaits` 和 `WaitUntilRead` 是供测试观察明确事件的接口，不是另一套业务等待或重试协议。

例如 U 的 Append 为同一 writer 的 LSN=10，主行更新为 LSN=20。存储回放 20 时，10 已完成；但在回放线程执行 10 前，计算端完全可能先发来取 U 的 RPC，这个 RPC 必须等。页转移前等 WAL 则保证转出的内容有持久化依据，不能用日志生成顺序替代。

新格式要求计算端和存储端使用支持 UPDATE flags 的版本；旧存储端会把非零 flags 解释为附带旧行。

### 重复应用与原型范围

- 普通主页面 redo 已去掉 `CheckSameRedo` 和序列化整条日志做相等比较的辅助函数。已回放的 `lsn <= page_lsn` 仍跳过；等待队列同一 prev_lsn 再插入会 CHECK。LogWrite RPC 设置 `max_retry=0`，当前不处理断线后自动重发同批日志的容错流程。
- undo Store/Redo 仍保留页级幂等检查和前驱队列：重复 Init/Append/Seal 核对已有内容，排队的同前驱记录比较字节，矛盾则失败。不能把删除 CheckSameRedo 描述成删除了全部重复检查。
- 启动时扫描完整 WAL 前缀并截掉不完整尾的机制仍在。它和页级重放不等于完整事务恢复；本轮不新增断线重试、崩溃决议、协调点恢复或 HA 验证。
- `undo_store_.Sync()` 由 `LogReplay::Drain` 在排空时调用，对派生 undo 页文件同步；不是每条 Append 的 fsync，也不是提交的 WAL 刷新入口。单元测试还有显式 Store::Sync 调用。

孤立 undo 只保存旧值，不自动更改主行、决定事务提交或推断业务更新已经发生。

页号按 writer 交错映射到正的 `page_id_t`。软容量限制控制新写事务准入。页号空间将尽时，2PC 在取行写锁前为每个不同 RID 预留最多一页，Prepare 和单写安装可使用该预留。Lazy 追加若需要另开一页且页号已用尽，保留原事务链尾并走正常 Abort。结束事务时归还未使用的预留，已分配的页号持续保留。

## 5. 当前文件、测试入口与限制

| 文件 | 当前职责 |
|---|---|
| `core/undo/format.{h,cc}` | 身份和页号映射、完整逻辑行/记录编解码、4 KiB 页格式与校验 |
| `core/undo/manager.{h,cc}` | 逻辑段、逐页分配、Append/Finish、行链读取、事务逆序读取；调用 PageAccess |
| `core/undo/config.h` | 独立帧数和锁表初始容量配置 |
| `core/undo/redo.{h,cc}` | Init/Append/Seal 共流日志、回放应用及页级幂等检查 |
| `core/undo/store.{h,cc}` | 存储文件、前驱回放、目标页就绪等待和排空同步 |
| `core/mvcc/row.{h,cc}` | 真实主行与逻辑 RowImage 转换，安装时保留现有锁字段 |
| `compute_server/server.{h,cc}`、`lazyrelease_server.cc` | 原 Lazy 页协议、BufferPool、取页/迁移/淘汰与 WAL 适配 |

P4 在 AliYun1 重新运行了 `undo_storage_test`、`undo_storage_ndebug_test`、`lazy_mvcc_test`/`run_lazy_mvcc.py` 单节点，以及新增两节点 cluster test/小负载。两节点测试观测到 undo 远端取页、权限转移、main/undo 淘汰后历史读和 Abort。旧版“五种不完整尾”“两 writer 独立目录”等结果仅保留为历史记录，不是当前测试清单。

P4 已不再把这些列为完全未验证：两节点跨节点历史读、重新取页回滚、main/undo 淘汰、锁表越界（`undo_lock_table_initial_capacity=4`）均已实际运行。未验证的是跨物理机、3+ 节点、封口页多读者专项、P6 复用和故障恢复。保留 fail-fast 启动设置。

P4 已完成两节点 Lazy 发布/creator 状态/历史迁移；P5 接 2PC/mix，P6 做 GC/页槽复用/历史磁盘配额。SQL、通用增删、主键变化、范围扫描和性能收益仍无新增结论。
