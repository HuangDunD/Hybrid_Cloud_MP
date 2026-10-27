# 持久化 Prepare 提交优化：实现与验证记录

日期：2026-10-23。

更新：2026-10-25，开启开关时已进一步改为存储汇总持久化 YES 并直接通知协调者，详见 [存储通知实现记录](CORNUS_STORAGE_NOTIFY_IMPLEMENTATION_NOTES.md)。下文保留基础版本的实施与验证记录；当前版本不再等待全部 Prepare RPC 返回才发起 Commit。

本次实现的是节点、RPC 和存储正常工作时的提交路径优化，不是完整 Cornus 容错协议。

## 配置开关

`config/compute_node_config.json` 的 `local_compute_node.cornus_prepare_commit` 控制多写参与者的提交协议：

```json
"cornus_prepare_commit": 1
```

- `1`（默认，包括省略字段）：启用本文的持久化 Prepare 优化。
- `0`：恢复原多写 2PC 流程，即仅事务 ID 的 Prepare → Prepare 结束日志及刷盘等待 → coordinator backup 日志及刷盘等待 → 携带最终值的 LEGACY Commit 及其结束日志刷盘等待。
- 仅接受整数 `0`、`1`，启动时读取；修改后重启计算节点，不支持热切换。新路径仍要求 `generate_log=1`，关闭优化不额外增加该要求。
- mode 2 和 mode 4 的 2PC 分支共用此开关。只读、单写、mix 的 lazy 分支不受其影响。AsyncCommit2pc 保持关闭，两种提交协议都同步等待 Commit RPC。
- 协调者选择事务协议，参与者按 Prepare 版本和 Commit kind 执行，不用本节点配置覆盖已收到的事务协议。所有节点使用相同值便于对比；即使协调者配置不同也不会误将 Prepared Commit 当作普通 Commit。
- 两种设置都保留退出日志排空屏障，因为本节点可能参与其他协调者发起的优化事务。关闭时仅恢复原事务提交协议，不撤销退出排空修补和准备响应的真实汇总检查。

## 实现范围

- 开关开启时，入口仍是 `DTX::Tx2PCCommit()` 的 `write_participants.size() > 1` 分支，仅由 mode 2 或 mode 4 已选择的 2PC 分支进入。
- `UniqueRWIndices()` 固定最终更新值，按页面所属参与者分组，并校验分组与完整写参与者集合一致。只支持已有记录 UPDATE；不扩展插入、删除、SQL 协议。
- Prepare 包含协议版本、协调者节点、进程启动时生成的 128 位随机 incarnation、本地事务 ID、start/commit 时间戳、排序去重的完整写参与者集合、本参与者身份及写集。每个写项包含表名、表 ID、page/slot、key、操作类型和独立拥有的业务 value 字节。
- 参与者检查写项归属、长度、tuple 身份和排他锁持有者；保存 PREPARING 上下文，持久化完整准备记录后才置 PREPARED 并返回 YES。上下文使用复合身份索引；相同身份的冲突请求不能覆盖已有准备数据。
- 协调者检查全部 Prepare RPC 的传输结果和 YES，不再写 backup 决议日志。缺少 YES 时明确终止进程，不把不确定状态转成普通 Abort；未实现失败恢复或重试。
- Commit 请求显式区分 LEGACY/PREPARED。后者只使用匹配的 PREPARED 上下文，沿用原页面更新、UPDATE 日志副本、锁交接和 FinishTx 流程，结束日志入队后返回，不等待该结束日志刷盘。日志复制完成后释放上下文。
- 协调者并行发出 Prepared Commit，Join 全部 RPC 并检查完成回复，之后才提交收尾和累计成功。
- 只读、单写本地、单写远端的事务分支保留；特别是单写远端的 LEGACY Commit 仍等待结束日志。AsyncCommit2pc 未开启；lazy 分流、阈值、页面迁移/推送/淘汰等待未修改。

## 日志与退出

`LogType::PREPARED` 追加到原枚举末尾。完整、通过校验的该类型记录就是本参与者 YES。格式使用原日志头、LE32 payload 长度、protobuf PrepareRequest、LE64 FNV-1a 校验值；校验覆盖日志头、长度和 payload。完整逻辑记录最大为现有回放缓冲的 40 KiB，超限拒绝，不做分片。

准备日志的 LSN 分配与入队在同一日志队列锁内完成，不更新页面或页面 LSN。回放校验记录后仅消费元数据，仍执行公共 offset 收尾，不更新业务页、bitmap、索引、页面 LSN 或全局提交状态。原始准备字节保留在 WAL 中；回放推进不代表可以清除投票记录。

日志关闭时新协议拒绝运行。包含准备记录的刷盘 RPC 若报错，采用 fail-fast，避免将传输错误当成 YES；没有新增 fdatasync 错误处理或重试框架。

实际 benchmark 退出路径原先在全节点完成屏障之后直接停存储，没有全节点日志 drain。现在 mode 2/4 使用“所有协调者完成 → 各节点 LogFlush → 所有节点 drain 完成 → 停存储”的顺序。新增的 flush 互斥覆盖正在执行的刷盘批次，防止 drain 只看到空队列却漏掉在途批次。

## 与方案描述的实现差异

1. 新增 `CommitPrepared_2pc()` 专用发送函数，而不是复用旧 `Commit_2pc(..., true)` 的参数表；它无异步开关，仍明确 Join 全部调用。旧函数继续服务单写远端路径。
2. 正常退出需要修补现有屏障，因此额外修改 `remote_server/server.cc`：完成消息的负节点数表示还需第二轮 drain 屏障。这是退出顺序修补，不是新增事务提交阶段。后续使用时 compute、storage、remote 三种二进制都需统一更新；本次未部署。
3. protobuf 类型由独立 `twopc_protocol` 构建目标提供，供 compute 核心和 storage 回放共享。生成文件由项目 protoc 生成，遵循仓库规则不跟踪。
4. 方案引用的 `doc/build_env.md` 在当前工作区不存在；当前机器依赖足以完成项目构建，没有安装新依赖。

## 已执行验证

- `cmake -S . -B build`：成功；使用本机 Protobuf 3.12.4。
- `cmake --build build -j 6`：全部目标构建成功，包括 compute_server、storage_pool、remote_node、客户端和测试程序。
- `build/tests/prepared_2pc_test`：通过。另覆盖开关缺省/0/1、非法配置拒绝、旧 Prepare 与新 Prepare 的线协议区分以及普通 Commit 的默认 LEGACY 标记。原测试覆盖准备记录往返编码、截断/校验/容量边界、重复身份冲突、时间戳匹配和上下文生命周期；使用临时本地文件运行实际在线回放，确认 PREPARED 不修改页面且推进进度，后续 UPDATE/LOCK/BATCHEND 正常应用、页面 LSN 链正确、原始投票字节仍保留。
- `build/tests/lock_wait_test`：80 passed，0 failed。沙箱内首次因本机监听受限未能启动 brpc 服务；获得权限后在本机完整通过。
- `git diff --check`：通过。
- 实施前工作区只有未跟踪的原实施文档；没有覆盖该文档，没有回滚其他内容，没有修改已有实验结果。

## 未验证和未实现

未运行开关 0/1 下真实多计算节点的 mode 2/mix 事务端到端测试，也未实测多进程退出屏障；相关路径完成编译和代码检查。未部署远程集群、未运行性能实验或故障注入。

随机 incarnation 的唯一性依赖高质量随机数及进程内事务 ID 不重复的假设，没有持久化 epoch 服务。没有 LogOnce/CAS、投票仲裁、崩溃恢复、重复应用判定、重启后锁恢复或安全日志回收。准备日志为后续恢复保留证据，不代表本版已支持恢复。提交阶段仍可能因原页面机制间接等待日志；后台 UPDATE/结束日志仍需最终落盘。
