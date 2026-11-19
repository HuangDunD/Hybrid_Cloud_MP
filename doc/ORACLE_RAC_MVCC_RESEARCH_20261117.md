# Oracle RAC 的 SCN、快照和 undo：对本项目的启示

调研日期：2026-11-17。仅采用 Oracle 官方资料；现代提交传播主要参考 2026 年 9 月 Exadata 白皮书，早期 Lamport 说明明确标注版本。

最新确认：D1—D9 的上层选择均已确认，包括 SCN 固定快照与提交同步、保留 Lazy/2PC 的共享行写入时机差异，以及沿用现有负载操作范围。具体并发分配和发布协议尚需细化，尚未实现代码。本文件保留调研依据与性能比较，最终设计状态以 [主设计文档](/Users/noctis/Documents/项目/Hybrid_Cloud_MP/doc/UNDO_MVCC_DESIGN_20261117.md) 为准。

## 1. 需要修正的结论

“现有号段不能直接比较先后”不等于“必须每笔事务向中心单独取号”。前者仍是本项目的具体问题，后者只是一个容易建立正确性的候选方案，之前表述得过于绝对。

Oracle9i RAC 官方资料明确说明 SCN 可以在各实例并行产生，并通过实例间消息传播；这是早期实现说明，不能据此把当年的参数默认值当成现代 RAC 默认值。[Oracle9i RAC：SCN Processing](https://docs.oracle.com/cd/B10500_01/rac.920/a96597/coord.htm)

现代 Oracle 的公开资料确认存在提交时的 SCN 同步协议，见第 3 节。因此更值得研究的是“哪些边界必须协调、哪些消息可以合并”，而不是把讨论限制为“一次取 1 个还是 100 个”。

## 2. 分清事务身份、快照边界与提交顺序

Oracle 的事务 XID 由 undo 段号、槽号和序列号组成；它与 SCN 是不同概念。提交时，undo 事务表记录提交状态和提交 SCN。[Oracle：Transactions](https://docs.oracle.com/en/database/oracle/oracle-database/26/cncpt/transactions.html)

| 概念 | 本项目可以采用的职责划分，属于设计建议 |
|---|---|
| tx_id | 标识版本属于哪笔事务；只保证唯一时可以批量分配，不比较其大小判断先后 |
| snapshot_scn | 本事务固定的读取边界，Begin 时取得，整个事务复用 |
| commit_scn | 事务提交在全局逻辑顺序中的位置；与实际提交状态联合使用 |
| WAL LSN | 日志与页面回放进度，不能拿来代替事务快照边界 |

Oracle 默认 READ COMMITTED 使用语句级快照；只读或 SERIALIZABLE 事务使用事务级边界。ITL 指向 undo 事务表，帮助判断提交状态。[Oracle：Data Concurrency and Consistency](https://docs.oracle.com/en/database/oracle/oracle-database/26/cncpt/data-concurrency-and-consistency.html)

本项目已经确认事务级 SI，因此保留 Begin 时的快照，不改成 Oracle 默认的语句级快照，也不因为 Oracle 使用 SERIALIZABLE 这个名称就扩大本项目隔离保证。

本项目现已选择 SCN 路线：自身更新可见；其他事务的候选版本必须已全局提交，且 commit_scn <= snapshot_scn。不可见则继续沿 undo 找旧版本。snapshot_scn 是包含端点且受协议保护的快照边界，不能机械地把现有 begin_ts 换个名字。

这条路线不以“每次 Begin 保存一份全局活跃事务清单”为必需前提，但仍需可靠的事务状态及正确的 SCN 协议。不能简单删掉活跃集合，同时继续使用当前预领号段和提前填入的 commit_ts。

## 3. RAC 怎样同步 SCN

2026 年白皮书描述的 Broadcast-on-Commit：提交前确保所有实例的 SCN 至少达到提交 SCN；传统方式由 LGWR 发给远端 LMS、等待确认，并结合 redo I/O 完成提交。21c 的 Exadata 优化改用 RDMA 原子操作。[Oracle，2026-09 白皮书，第 8 页](https://www.oracle.com/a/ocom/docs/database/rac-cache-fusion-performance-optimizations-on-exadata.pdf)

Oracle 的产品说明还明确表示 SCN 消息与 redo 写入并行发起，返回提交前检查两者完成情况。[Oracle：Broadcast-on-Commit over RDMA](https://www.oracle.com/database/technologies/exadata/software/broadcastoncommit/)

由这些资料可以得出的工程结论是：全局顺序可以通过实例协同维护；提交仍有协调成本，可以与日志 I/O 重叠。不能得出“各节点随便本地取号、完全不用协调”，也不能得出“每笔事务 Begin 必须访问中心”。

公开概念资料没有给出全部并发路径的内部算法。尤其是同时取快照和并发提交、已分配但未完成的提交、不同实例看到不同 SCN 时如何收敛，不能仅凭这段描述还原为一个 `max()+1` 加广播的完整 SI 实现。本文不声称已证明这样的简化协议正确。

## 4. 这是否证明当前预领号段可直接保留

不能。当前的反例仍然成立：B 以 450 开始后，A 才启动，但继续消耗先前领取的号段，以 206 提交；A 既不在 B 开始时的活跃集合中，又满足 `206 < 450`。唯一性没有解决版本顺序。

我们可以继续批量分配只用于身份的 tx_id；若想批量分配可见性时间戳，则必须额外解决未用号段、已取号未完成的事务，以及快照推进之间的关系。RAC 的 SCN 协同不能作为跳过这些条件的依据。

本项目已经选择的 SCN 方案必须满足以下约束：

1. 同一事务的所有节点使用同一个快照，后续读不能把快照提升为新值。
2. 一个快照建立后，不能再把原本不属于其历史的事务发布成这个快照可见的提交。
3. 在新事务开始前已经完成全局提交的修改，应包含在新快照中，不能靠一个任意滞后的本地计数器代替这个保证。
4. 已分配提交编号不等于已提交。若使用发布水位，水位推进必须正确处理尚未完成的提交，而不是直接取收到的最大值。
5. 2PC/mix 要整笔事务一起进入可见状态；SCN 同步不代替 Prepare、决议与各参与者完成写入的条件。

以上是本项目需要满足的设计条件，不是 Oracle 内部算法的逐步复述。

## 5. undo 和跨实例历史读

RAC 为实例分配 undo tablespace，正常情况下由对应实例写入，其他实例可以读取其中 undo。[Oracle RAC：Automatic Undo Management](https://docs.oracle.com/en/database/oracle/oracle-database/26/adrac/rac_undo_tblspc.html)

Oracle undo segment 可服务多个活跃事务，不能等同于我们已确认的“一事务一个逻辑段”。本项目保留 D3 的管理方式，无需为模仿 Oracle 改动它。[Oracle：Undo Segments and Transactions](https://docs.oracle.com/en/database/oracle/oracle-database/26/cncpt/logical-storage-structures.html)

Oracle 区分当前块和一致性读副本（CR clone），通过 undo 构造历史副本；RAC 能跨实例传递这种副本。[Oracle：Database Buffer Cache](https://docs.oracle.com/en/database/oracle/oracle-database/21/cncpt/memory-architecture.html)，[Oracle：跨实例一致性读](https://docs.oracle.com/en/database/oracle/oracle-database/26/cncpt/data-concurrency-and-consistency.html)

所以我们文档中的“读 undo 导致整页 S/X 来回交接”，只是直接复用本项目现有页协议的代价，不是 undo MVCC 必然如此。未来可以研究让持有者返回已构造的历史行/历史页，或已发布的不可变 undo 内容。该扩展必须定义副本适用快照和回收规则，不能把任意旧页当成正确历史；本轮尚未选定它。

## 6. redo 的共性与持久化路径的差异

Oracle redo 保护普通数据块、undo 块和 undo 事务表的变化；因此“undo 本身也需要 redo”有直接官方依据。[Oracle：Managing the Redo Log](https://docs.oracle.com/en/database/oracle/oracle-database/21/admin/managing-the-redo-log.html)

Oracle 同时有 DBW 写脏缓冲块的正常路径。[Oracle：Buffer Writes](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/memory-architecture.html)

本项目仍按用户确认的架构，由存储端回放 redo 写普通数据页和 undo 页文件。借鉴其 undo/redo 关系，不意味着把本项目改成 Oracle 的数据文件写入路径。高可用与完整事务故障恢复仍不在本轮范围。

## 7. 当前确认状态与后续设计

- D2 已确认使用 SCN 快照、事务状态与提交传播；Begin 固定 snapshot_scn，写提交满足所需日志、全事务写入与集群同步条件后返回成功。
- 广播请求可并发异步发出，提交需要等待所需确认；旧事务快照不随节点进度变化。不把“发出通知”当作“完成同步”。
- 普通身份 tx_id 与 SCN 分离；仅用于唯一身份的批量分配可保留。集中 Begin 取快照方案仅保留作研究对照，不再与活跃集合方案并列要求用户选型。
- D1—D9 均已确认：D5 保留 Lazy 执行更新时覆盖共享行、2PC 在 Commit 覆盖的差异，并在相应覆盖前准备 undo；D9 覆盖现有负载实际写操作及删除标记，不新增通用数据库操作。重复更新首版每次实际覆盖保存完整 undo、不合并。后续技术细化见主文档第 14 节。

SCN 如何分配、并发提交如何排序、广播 ACK 与可用快照边界如何衔接，还需形成满足第 4 节约束的完整协议。方向确认不等于算法已完成。这些是下一阶段的设计细化，不进入本轮代码实现。

## 8. 提交广播是否同步，以及与 Begin 取快照的性能比较

本节保留选型时的性能分析。A 为 Begin 向协调服务取得有效快照、写事务提交时向服务发布结果；B 为 Begin 使用按协议维护的本地快照边界、写事务提交时完成集群 SCN 同步。当前设计沿 B 的方向继续细化。两者都可以使用 SCN，且都必须满足第 4 节的一致性要求，不能把原始计数器的读写直接当成完整协议。

### 8.1 广播并发发出，提交等待完成

正常等待提交的语义是：SCN 广播与 redo I/O 可以并行发起，但提交不能在所需广播确认尚未完成时就返回成功。Oracle 的流程说明明确列出 RAC 等待 broadcast ack 后再通知前台。[Oracle：Redo Transport Troubleshooting and Tuning](https://docs.oracle.com/en/database/oracle/oracle-database/26/haovw/redo-transport-troubleshooting-and-tuning.html)

本项目当前设计按异步并发发送 RPC、提交等待所需确认的方向细化，不串行访问每个节点，也不发出就忘。通知接收方推进协议规定的 SCN 状态，不等于让该节点立刻把所有相关数据页回放落盘；数据页仍按既有取页与 LSN 依赖处理。纯异步传播若要保留相同快照语义，就必须把必要等待转移到 Begin/读路径或引入其他一致性机制，不能无条件使用落后的本地边界。

只看可重叠的两个环节，设 WAL 持久化等待为 W、这一轮广播的最慢确认耗时为 B，理想完成时间约为 `max(W, B)`，相对只等 WAL 的额外等待约为 `max(0, B-W)`。这是上界条件合适时的简化开销模型，不包含 SCN 分配、Prepare、数据安装和其他依赖，也不是本项目测量结果。

例如假设 W=1 ms、B=0.2 ms，广播可被 WAL 等待覆盖；若 B=2 ms，就可能多等约 1 ms。如果本项目的正确协议要求某些步骤先后执行，那部分时间不能强行算作重叠。

### 8.2 两种方案的主要代价

| 比较项 | Begin 请求协调服务 | 提交广播、本地快照 |
|---|---|---|
| 只读事务 | 通常承担一次 Begin 请求；结束还有历史保留管理 | 无需为取快照专门请求中心；历史保留管理仍需进行 |
| 写事务 | Begin 请求，以及提交编号/结果发布的协调，不能只计算 Begin 一次 | 提交需向所有需要同步的实例传播并等确认 |
| 延迟风险 | 中心的往返延迟、排队和 CPU 热点 | 广播最慢接收者、网络及远端调度的尾延迟 |
| 扩容风险 | 集中的请求处理能力成为瓶颈，可讨论批处理 | 每轮接收者数随节点数增长，全局写负载高时通信量大 |
| 可重叠与批处理 | Begin 请求可以合并；提交协调能否重叠取决于协议 | SCN 同步可尝试与 WAL 重叠，并按正确协议合并提交批次 |
| 较有利的场景 | 写事务比例高、节点较多，且协调服务容量足够 | 只读事务比例高、节点少、网络快且延迟稳定 |

以下只用于比较通信规模：不合并批次、忽略 ACK、GC、SCN 分配及其他事务消息，假设每个事务 Begin 请求一次、每个写事务向中心发布一次。每秒 Q 个事务，写事务比例 f，N 个计算节点：中心约承接 `Q + Qf` 次请求；广播约有 `Qf(N-1)` 次远端投递。两者不是相同的 CPU 成本，不能直接据此推导吞吐胜负。

例如 Q=1,000、N=4、90% 事务只读，广播约 300 次远端投递，中心约 1,100 次请求；若 N=16 且全是写事务，广播约 15,000 次远端投递，中心约 2,000 次请求。协议批处理会改变这些数字。应统计“包含写操作的事务比例”，而不是只统计单条操作中有多少是读。

广播批处理也不是每次随意取最大的 SCN。合并时必须明确哪些提交已满足条件、快照能推进到哪里，并处理未完成项。Oracle 已有 group commit 的公开依据，但不能据此假设本项目的逐事务广播已经自动获得同样的合并收益。[Oracle：SQL Processing for Application Developers](https://docs.oracle.com/en/database/oracle/oracle-database/21/adfns/sql-processing-for-application-developers.html)

### 8.3 对本项目的判断

当前配置列出 3 个计算节点，`use_rdma=false`，已有 WAL 批次刷新。配置中的 3 ms 刷新周期及成本模型参数不是实测的 WAL/RPC 延迟。[当前配置](/Users/noctis/Documents/项目/Hybrid_Cloud_MP/config/compute_node_config.json:17)

当前用户已选择 SCN 路线及提交同步方向，设计应尽量让通信与 WAL 等待重叠。少量节点、低延迟稳定网络有利于这一选择；集中快照服务可保留作性能对照。若将来扩展到较多节点、高延迟网络或很高的写事务比例，再评估是否调整协调方式。这里是通信与等待结构分析，尚无本项目实测性能结论。

比较至少覆盖只读、读多写少、写密集三类负载，并记录 Begin 延迟、提交 p50/p99、最慢广播确认、实际 WAL 等待、每秒 SCN 消息数和协调服务排队。固定事务语义与日志持久性，才能进行公平比较；本轮未运行性能实验。
