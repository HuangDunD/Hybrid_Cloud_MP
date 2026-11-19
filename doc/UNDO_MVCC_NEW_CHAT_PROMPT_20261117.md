# 新窗口提示词：接续当前 P4

更新：2026-11-18。原 P0 + P1 起步模板已改为当前接续状态，避免重复实现或直接跳到 P5。本文件只是可复制的上下文，不会创建新聊天或自动授权验证；具体工作范围以用户当次指令为准。

```text
请在 /Users/noctis/Documents/项目/Hybrid_Cloud_MP 中接续 Undo / MVCC。

当前范围止于 P4：两节点 Lazy 的真实 MVCC、提交发布、跨节点 creator 状态和主行/undo 页迁移。P0/P1 协议基础、P2 undo 存储、P3 单节点 Lazy 和 P4 多节点 Lazy 已有源码，并已在 AliYun1 重新构建运行。先核对源码和进度顶部，不能把历史“P3 已通过”套用到其他模式，也不要从 P0 重写或自动扩展 P5。

本轮先按我当前要求进行代码 review/修改或文档同步。在我明确安排验证前，不运行构建、测试、SSH 或部署；本地代码完成并允许验证后，再按下述远端流程执行。不要为了补记录虚报测试，也不要为阶段内部常规选择反复请求确认。

先检查 Git 状态、适用 AGENTS.md 与实际源码。设计基线和最近核对的 HEAD 为 09fed8e，实现是未提交改动，用户可能继续编辑或暂存；不要回退、覆盖或重新暂存用户改动。

阅读顺序：
1. doc/UNDO_MVCC_DESIGN_20261117.md：D1—D9 已确认，不重新选型。
2. doc/UNDO_MVCC_IMPLEMENTATION_PLAN_20261117.md：当前状态说明、P3 与退出条件。
3. doc/SCN_MVCC_PROTOCOL.md：SCN、安全快照与共享字段。
4. doc/UNDO_STORAGE_PROTOCOL.md、doc/LAZY_MVCC_P3.md：当前页协议、格式和真实事务接入。
5. doc/UNDO_MVCC_IMPLEMENTATION_PROGRESS.md：先读顶部当前状态，再按需看历史命令/结果。Oracle 调研只在需要背景时读。

当前代码事实：
- 业务 Lazy 由 SYSTEM_MODE==1 && WORKLOAD_MODE!=4 判定，已删除 lazy_mvcc 开关，统一 ExecuteLazy/CommitLazy/AbortLazy。不能再保留另一条不使用 undo 的旧 Lazy Abort。eager/2PC/mix/SQL 的旧路径仍保留，尚未接通各自 undo。
- P4 已允许同一 epoch 内的多计算节点；固定协调点为节点 0，成员集合显式传给 LazyRuntime。WAL 开启且 mvcc_epoch 必须为合法 16 字节。仓库默认配置仍是空 epoch/三节点地址，不能直接运行；集成脚本生成新 epoch、新数据、独立端口配置。P4 验证只覆盖 2 个 loopback 计算进程。
- Begin 先登记固定 snapshot，再建立逻辑 undo 段；snapshot_scn 来自本地 safe_scn，不每事务向中心单独领号。tx_id/start_ts 是身份，不用于可见性排序。
- 自身版本优先，初始化版本单独定义；其他版本必须创建者 COMMITTED 且 commit_scn<=snapshot。可见墓碑立即返回不存在。当前版本不可见则沿真实 undo 链读，未知状态或断链明确失败。
- CommitCoordinator 统一编号，全员 Stage 后再 Advance；成功与交锁须等全员 Advance ACK。P4 LazyRuntime 持有全员成员集合：协调点本地 `SubmitCommit`，其他节点走 `Commit` RPC；发布线程在收齐 ACK 前不完成。creator 状态先查本地 Stage 终态，未知查 home，仍未知显式失败。
- 回调在 ExecuteLazy 安装，旧私有副本变化在 CommitLazy 补写，二者都经 InstallMvcc：保留行写锁、释放主页面保护，先 Append 完整旧行，再重取主页重检并安装新值/creator/deleted/Ref。每次实际覆盖独立 undo，无变化不造版本。
- 写权限后检查 SI 冲突；快照后的其他已提交版本导致正常 Abort/retry。AbortLazy 逆序用 previous_txn/sequence 重新取页恢复已安装修改，生成 redo、等 WAL 后再结束状态和交锁；不恢复旧锁字段，未安装私有副本直接撤销。
- TPC-C valid=0 是版本化墓碑，保留 bitmap/RID/BLink 入口。DataItem 当前 168 字节，RowFields 96 字节，MVC2/RMV2；旧数据要重建，不能仅改 sizeof 断言数字或静默兼容。
- undo 不再有独立 DirectoryService/CacheService/RpcPageAccess/LocalAccess，也没有自建 buffer 包装。Manager 的 PageAccess 由 ComputeServer 实现，复用原 LPLM/GPLM、Lazy S/X、Pending/PushPage、BufferPool/LRU 和 BufferReleaseUnlock 淘汰握手。
- 每节点一个 Manager 的 next_page_ 共用于所有事务。Begin 不分物理页，首次写和页满时各申请一张 4 KiB 页。UNDO_TABLE_ID=30000，映射页号=(number-1)*100+writer+1；节点 0/1/2 的前两页为 1/101、2/102、3/103，不是一次分配连续 100 页。
- undo_buffer_pool_size_pages 默认 10000，是每节点独立帧预算。undo_lock_table_initial_capacity 默认 10000，是锁表初始页号范围，范围外通过 entries_ 按需建立；允许 0，无 sparse_ 开关，不能混同两个容量。
- PageUsesWal 显式包含 undo，脏页转移/淘汰仍等 WAL。存储 GetUndoPage 使用完整 PageKey 和 required_lsn 等待回放；计算端不新增脏 undo 文件写入路径。
- undo redo 和主行 redo 共用实际写节点 WAL，存储每节点一个日志文件，流内按文件顺序同步回放。P3 的主行 CheckHistory 检查同 writer 的 undo 已完成，已删 HistoryReady/history_waiting_；普通主页面跨流前驱队列仍在。P4 Lazy 仍同 writer；跨 writer 的旧链头/回滚依赖留给 P5 2PC/P6 恢复，不能只删断言解决。
- CheckSameRedo 已删除，LogWrite 禁自动重试；undo Store 的页级重复校验仍保留。计算端仍批量刷新，存储端每个非空 LogWrite 批次一次 fdatasync，没有 Leader/Follower 多请求合并。undo_store_.Sync 在 Drain 排空时同步派生文件，不是逐条 undo fsync。
- P4 已用两节点真实数据验证主行/undo 跨节点读取、权限交接、main/undo 淘汰、存储回放等待和 Abort；仍有 2 节点 loopback 限制。当前只追加，页/记录代次固定为 1，没有 GC、页复用、历史磁盘配额、完整事务终态恢复或旧 epoch 接管。原型只验证正常正确性，保留必要 redo 机制，不扩展断线重试、故障恢复工程或 HA 验收。

编码要求：延续 assert/CHECK 风格，断言身份、固定快照、合法状态、RID/代次、页权限、记录完整性及 WAL/引用顺序。内部不可能状态直接失败，不写 catch-all、坏链跳过、缺历史返回最新值或默认成功。正常锁/SI 冲突、页面等待和资源压力走明确正常路径。必要副作用先执行再断言。保留 common/fatal_checks.h 的启动设置。

用户允许验证后：
- 先完成本地修改，再核对实际 SSH 别名（此前为 AliYun1，区分大小写）、远端 /data/workspace/ 下的目录和已有修改。此前原项目为 /data/workspace/Hybrid_Cloud_MP，不盲目覆盖用户改动；独立副本不代替要求的原 Git 工作区同步。
- 用独立数据、epoch、端口与构建目录。根 CMake 可能生成 protobuf，检查生成物。实际运行相关测试二进制，tests/CMakeLists.txt 无 add_test，不能只跑 ctest。
- 重新运行 P1/P2/P4 组件和相关锁/2PC 回归；用 `run_lazy_mvcc.py` 跑单节点三负载，用 `run_lazy_mvcc_cluster.py` 跑两节点确定性场景，用 `run_lazy_mvcc_multinode_workload.py` 跑两节点小负载。重点验证全员 Stage/Advance、held ACK/WAL、跨节点 creator 状态、主行/undo 迁移、淘汰后历史读/回滚、并发追加；补锁表数组范围和越界条目检查。
- 原独立目录/缓存夹具已删除，旧“五种不完整尾”等记录不是当前用例清单。只报告实际执行的命令、退出状态和结果。

收尾按本次授权更新交接记录，区分源码已接通和运行已通过。P4 完成后，P5 从 2PC/mix 的 Context/Prepare/Commit、参与者统一发布和远端读可见性继续；P5 后直接进入 P7，P6 GC 延期为可选优化。不创建新聊天，不要求别的窗口依赖本聊天隐藏上下文，不把整个 MVCC 任务标成已完成。
```
