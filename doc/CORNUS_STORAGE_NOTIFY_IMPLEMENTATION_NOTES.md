# 存储端汇总持久化 Prepare 并通知协调者

日期：2026-10-25。

本次是在持久化 Prepare 正常路径上的进一步优化。未实现 LogOnce、故障恢复、失败重试或多存储服务间的投票汇总。

## 开关与适用范围

沿用 `local_compute_node.cornus_prepare_commit`，没有新增配置项：

- `1`（默认）：多写参与者的纯 2PC，以及 mix 已选择的多写 2PC 分支，使用存储端通知触发 Commit。
- `0`：继续执行原事务 ID Prepare、coordinator backup 决议日志及普通 Commit 的刷盘等待。
- 只读、单写本地、单写远端、lazy 分支不变。AsyncCommit2pc 保持关闭。

新请求使用 Prepare 协议版本 2。版本 1 的准备日志仍可解析、回放，但不会触发新通知；版本 0 的旧 Prepare 仍按原路径处理。上线时须统一更新 compute/storage 二进制；本次没有部署任何节点。

## 正常流程

1. 协调者固定完整最终写集和写参与者集合，以复合事务身份注册通知等待项，再并行发送 Prepare。注册先于发送，避免快速返回的通知丢失。
2. 参与者执行原有校验、准备上下文保存、完整准备日志入队和持久化等待，不修改业务页面或页面 LSN。
3. 存储 `LogWrite` 在 `write_batch_log_to_disk()` 返回、以及编译启用时既有复制流程完成后，检查该批次日志并登记版本 2 的完整 PREPARED 记录。此处不能使用 replay 已可读作为持久化依据。
4. 存储按 `(coordinator_node, coordinator_incarnation, local_tx_id)` 建立临时投票项，保存一致的 start_ts、commit_ts、完整参与者集合和每个参与者的准备 payload。重复参与者不重复计数；同身份下冲突的元信息或写集明确报错。
5. 收齐精确的写参与者集合后，存储通过独立异步 RPC 向协调者发送 `AllPrepared`。地址来自已有 compute 节点配置，不使用日志中任意指定的网络地址。发送时不持有投票 map 或刷盘锁；LogWrite 不等待通知 RPC 完成。
6. 协调者的通知 handler 只校验身份、时间戳、参与者集合并唤醒等待项，不在 handler 中执行 Commit。收到通知后即可开始并行 Prepared Commit，不再以全部 Prepare RPC Join 为前提。
7. 协调者仍同步 Join 所有 Commit RPC，并检查每个完成回复，之后才返回事务成功。

完整持久化准备记录仍是 YES 的证据；AllPrepared 只是传递这个事实，没有新增 coordinator 或 storage 决议日志。

## 提前到达的 Commit 与资源生命周期

存储通知可能早于参与者收到 LogWrite 响应。因此 Commit 可以看到匹配的 PREPARING 上下文。版本 2 的 Commit 在该事务自己的条件变量上等待 MarkPrepared，再进入 APPLYING。等待期间不持有全局上下文 map 锁、页面锁或行锁管理器锁。没有绕过持久化检查，也没有直接把 PREPARING 当成 PREPARED。

原 Prepare RPC 仍会回复，但不在协调者进入 Commit 的等待链路上。每个异步调用独立拥有 request、response 和 controller，直至回调结束；没有引用事务栈上的对象。正常 benchmark 退出时，在已有完成/日志排空屏障之前 drain 这些回调。

存储收到通知 RPC 的成功回复后释放投票项，并按身份记录最多 4096 条已完成元信息：只有与已完成记录逐字节一致的重复投票才幂等忽略（重放/重复日志），元信息不一致说明 `local_tx_id` 在同一 incarnation 内被复用，存储直接 fail-fast，不再把新事务的通知静默吞掉。身份 `(coordinator_node, coordinator_incarnation, local_tx_id)` 必须按事务唯一，workload 侧每个事务分配一个新事务 id（YCSB 原先整个 coroutine 复用一个 id，已修正）；协调者仍按原语义接受迟到/重复通知，不知道的身份不会重建事务。重放、失败后未收齐的投票和状态重建仍属于未来恢复工作。

RPC 错误采用现有正常路径的 fail-fast 风格，不触发普通 Abort 或重试；fdatasync 返回失败时也明确停止，避免发布错误的持久化水位。

## 已完成验证

- 本地全项目 CMake 构建。
- 新增 `prepare_notification_test`，使用本机真实 brpc 服务、生产 StoragePoolImpl / LogManager / fdatasync、生产存储通知器、协调者发送/等待代码及参与者准备上下文；业务页面更新在该测试中由模拟参与者表示。
  - 两个原 Prepare 回复被刻意阻塞时，存储通知仍能触发并完成 Commit。
  - Commit 早于本地 MarkPrepared 到达时会等待，不会被误拒绝或提前应用。
  - 最后一个 Commit 回复未到时，协调者不会完成事务。
  - 退出 drain 会等待仍在途的 Prepare 回调。
  - 协调者自身作为 writer 时也纳入完整集合。
  - 重复/迟到 YES 不冒充其他参与者，正常通知确认后释放投票项。
  - 相同 local_tx_id、不同 incarnation 的投票不会混合。
  - 版本 1 准备记录不触发版本 2 通知。
- 既有 `prepared_2pc_test`：配置、编码边界、上下文生命周期和实际在线回放校验。
- 工作区现有 `lock_wait_test`：110 passed，0 failed；保留用户新增的两阶段锁交接实现和用例。
- `git diff --check`；对本轮开始时的快照核对，锁等待实现、锁交接 proto、锁测试及配置文件均未被本轮改写。

测试仅使用本机临时数据，没有远程部署、故障注入、长实验或改写已有实验结果。运行通知测试时使用 `/tmp` 下的二进制副本，使存储服务的测试统计输出也位于临时目录。

## 验证范围和性能解释

尚未运行真实多进程 YCSB/SmallBank 的 mode 2/mix 端到端事务，也未实测远程集群退出屏障、启用复制的部署或性能收益。当前设计要求一个事务所有写参与者的准备日志汇聚到同一存储服务，不能直接推广为多存储分片协议。

本次缩短的是通知提交判据的关键路径，不承诺减少一个完整 RPC 往返或减少总消息数。Prepare 原回复仍存在，还新增一次聚合通知；提前到达的 Commit 仍可能等待本地准备状态、页面获取或原有页面日志机制。后台日志仍需落盘，完整 Cornus 容错能力仍未实现。
