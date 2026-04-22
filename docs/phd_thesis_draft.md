# 面向共享存储多主数据库的负载亲和动态重分区与迁移机制研究

> 博士学位论文初稿 / 顶级期刊长文雏形
> 当前版本：基于 `Hybrid_Cloud_MP / WookongDB MP` 代码与本地 Stage 4 实验结果整理
> 写作原则：只写已经由代码或实验支持的结论；尚未完成的顶刊级实验明确标注为“待补充”。

## 摘要

云原生数据库正在从单主或静态分片架构转向多主共享存储架构，以同时获得计算弹性、存储解耦和较高资源利用率。然而，多主共享存储系统把数据页所有权转移推入事务执行关键路径：当事务访问的热点数据跨计算节点分布时，系统需要频繁执行远程页面锁定、日志等待、页面推送以及所有权更新，导致尾延迟升高和吞吐下降。现有系统通常依赖静态数据布局、页级所有权缓存或 Lazy Release 等机制降低远程访问频率，但它们对动态负载相关性和热点账户/键之间的事务亲和关系响应不足。

本文研究一种面向多主共享存储数据库的负载亲和动态重分区机制。核心思想是：在事务执行过程中轻量采样同一事务内共同访问的数据项，在线构建数据项亲和图；周期性对图进行分布式重分区，将具有强事务共现关系的数据项重新指派到同一计算节点；后台迁移线程按限速策略执行数据项迁移，以减少后续事务的跨节点页面所有权转移。本文在 WookongDB MP 原型系统中实现了完整链路，包括事务采样、亲和图聚合、跨节点边交换、ParMETIS 侧车重分区、无锁读路径 AssignmentTable、后台迁移、实验汇总与诊断工具。

基于当前本地双计算节点实验，本文得到两个阶段性结论。第一，在开启日志的 SmallBank 亲和负载下，动态亲和机制能够显著降低远程访问比例并提升吞吐：10k 每 worker 实验中，吞吐由 165.61 tx/s 提升到 313.20 tx/s，提升 89.12%，远程访问比例由 25.69% 降到 18.14%；2k 每 worker 实验中，吞吐提升约 48.73%。第二，在关闭日志且事务推进速度显著提高的场景中，当前迁移限速与后台调度策略尚未充分跟上负载变化，100k 与 2M 事务实验未观察到收益，吞吐下降 5.24% 到 10.30%。这表明：动态亲和重分区的收益取决于“重分区/迁移收敛速度、迁移干扰、日志瓶颈、事务推进速度”之间的平衡。本文进一步揭示了日志路径的主要瓶颈：存储层 `fdatasync` 平均耗时约 4.7 到 4.9 ms，且当前组提交效果有限。

本文贡献包括：（1）提出面向共享存储多主数据库的事务亲和图抽象与在线采样机制；（2）设计并实现一条从事务采样到后台迁移的动态重分区流水线；（3）建立实验汇总口径，区分 per-node、cluster、wall-clock 与日志/no-log 场景；（4）系统化分析日志路径、迁移限速、图划分周期和事务推进速度对吞吐的影响。本文当前版本仍需补充分布式物理集群、多负载、多节点规模扩展、对比系统与恢复一致性实验，方可支撑顶级期刊投稿。

**关键词**：云原生数据库；共享存储；多主事务处理；动态重分区；亲和图；数据迁移；Lazy Release；所有权转移

## Abstract

Multi-primary shared-storage databases decouple compute and storage while allowing multiple compute nodes to serve transactions concurrently. However, they also move page ownership transfers into the transaction critical path. When hot records accessed by one transaction are scattered across compute nodes, the system repeatedly performs remote locking, log waiting, page push, and ownership metadata updates, which reduces throughput and increases latency.

This dissertation draft studies workload-affinity-driven dynamic repartitioning for multi-primary shared-storage databases. The key idea is to sample co-accessed records inside transactions, build an online affinity graph, periodically repartition the graph, and migrate strongly related records toward the same compute node. We implement the end-to-end pipeline in WookongDB MP, including transaction sampling, graph aggregation, distributed edge shuffling, ParMETIS-based repartitioning, lock-free assignment-table lookup, rate-limited background migration, and experiment aggregation.

Preliminary local two-compute-node experiments show that, with logging enabled, affinity-driven repartitioning substantially improves SmallBank throughput. In a 10k-attempts-per-worker run, throughput improves from 165.61 tx/s to 313.20 tx/s, while the remote access ratio drops from 25.69% to 18.14%. In contrast, no-log long-running experiments serve as sensitivity studies: when the transaction stream becomes much faster, the current migration policy does not converge fast enough and may introduce additional ownership-transfer overhead. These results suggest that the benefit of affinity repartitioning depends critically on the interaction among migration speed, migration overhead, logging bottlenecks, and workload dynamics.

## 第 1 章 绪论

### 1.1 研究背景

云数据库的体系结构正在经历从“计算存储紧耦合”到“计算存储分离”的演进。共享存储架构通过将数据持久化、日志写入和页面读取下沉到统一存储层，使计算节点可以按需扩缩容，并减少数据副本管理成本。与此同时，多主架构允许多个计算节点同时处理事务，避免单主节点成为吞吐瓶颈。

但多主共享存储架构带来了新的核心矛盾：数据在存储层共享，而事务执行发生在多个计算节点；为了保证并发控制与数据一致性，系统必须维护页面或记录的所有权状态。当一个计算节点访问不属于本节点的热点页面时，系统需要执行远程所有权转移。若负载中存在强相关访问模式，例如 SmallBank 中经常同时访问的一组账户分布在不同节点，远程页面获取会成为常态，吞吐受到显著影响。

WookongDB MP 当前支持 SQL 模式和负载模式，负载模式包括 SmallBank、YCSB 和 TPCC，并实现 Eager 与 Lazy 页面获取/释放策略。在 Lazy 模式下，系统通过 Lazy Release 和日志回放机制维护页面正确性。本文工作在该基础上进一步引入亲和感知的动态重分区机制。

### 1.2 问题定义

给定一个多主共享存储数据库集群，设计算节点集合为 `N`，数据项集合为 `D`。每个数据项 `d in D` 在任意时刻被 AssignmentTable 指派给一个目标计算节点 `owner(d) in N`。事务 `T` 访问一组数据项 `A(T)`。如果 `A(T)` 中的数据项分散在多个节点，事务执行过程中会触发更多远程页面获取和所有权转移。

本文目标是在不阻塞事务主路径的前提下，在线学习事务访问亲和关系，并动态调整 `owner(d)`，使强相关数据项尽可能被分配到同一计算节点，同时控制迁移开销与负载均衡误差。

形式化地，系统需要最小化：

```text
Cost = alpha * RemoteAccessCost
     + beta  * MigrationCost
     + gamma * LoadImbalanceCost
     + delta * RepartitionOverhead
```

其中：

- `RemoteAccessCost` 表示事务访问跨节点数据导致的远程锁、页面推送和日志等待开销；
- `MigrationCost` 表示动态迁移数据项引入的页面写入、索引更新和日志成本；
- `LoadImbalanceCost` 表示过度聚集热点数据导致的节点负载不均；
- `RepartitionOverhead` 表示采样、聚合、分区和分发 AssignmentTable 的后台开销。

### 1.3 研究挑战

1. **在线性**：系统必须在事务运行过程中持续采样和重分区，不能依赖离线分析。
2. **低侵入性**：采样和路由不能显著增加事务主路径开销。
3. **一致性**：迁移过程中需要保持 BLink 索引、页面数据、日志和所有权状态的一致。
4. **收敛速度**：迁移速度过慢会导致 AssignmentTable 长期落后于事务热点；迁移速度过快又会与前台事务竞争锁、缓存和存储 RPC 资源。
5. **实验口径**：多节点系统吞吐统计必须区分 per-worker、per-node、cluster aggregate 和 wall-clock 口径，否则容易误判性能。

### 1.4 本文贡献

本文当前实现与实验支撑以下贡献：

1. 提出事务亲和图模型，将同一事务内共同访问的数据项转换为带权边，用于刻画负载相关性。
2. 设计在线亲和流水线：SampleRing、Aggregator、EdgeShuffler、Partitioner、AssignmentTable 和 MigrationWorker。
3. 实现基于 ParMETIS 的动态重分区侧车进程，避免把复杂图划分库直接嵌入事务系统主进程。
4. 实现限速后台迁移机制，通过 `migration_tick_ms` 与 `migration_batch` 控制迁移吞吐。
5. 完成实验汇总口径修正，输出 per-node 与 cluster 结果，显式记录日志开关、attempted_num 作用域、节点数、远程访问比例和迁移统计。
6. 通过本地实验发现：log-on 场景 affinity 收益显著，no-log 高吞吐场景当前实现尚未充分收敛，说明迁移调度与日志策略需要联合优化。

## 第 2 章 相关工作

> 本章需要在正式论文中补充完整引用。当前仅列写作方向，避免在初稿中伪造不准确引用。

### 2.1 云原生共享存储数据库

共享存储数据库通过统一存储层提供数据持久化能力，计算节点按需访问数据页。典型研究问题包括缓存一致性、页面所有权、日志提交、恢复协议和弹性扩缩容。本文继承 WookongDB MP / Chimera 的多主共享存储思想，关注在动态负载相关性下如何降低所有权转移。

### 2.2 多主并发控制与页面所有权

多主系统需要在多个计算节点之间协调读写锁和页面有效性。Eager 策略会更积极地释放或同步页面所有权；Lazy Release 策略则延迟释放以提高缓存命中和减少无谓同步。Lazy 策略在局部性较强时有效，但当事务访问关系发生变化或热点跨节点分布时，仍会产生大量远程获取。

### 2.3 数据库动态重分区

传统动态重分区通常面向 shared-nothing 分片数据库，以减少分布式事务或平衡负载为目标。本文场景不同：底层数据位于共享存储，重分区不是改变存储分片，而是改变数据项在计算节点侧的亲和归属和访问局部性。因此，迁移机制必须和页面锁、日志、索引以及 Lazy Release 协议协同。

### 2.4 图划分与事务亲和

事务访问数据项之间天然构成超图或图结构。为降低实现复杂度，本文采用事务内读写集合的 pairwise cross-product 构建普通带权图，并使用 ParMETIS 进行分布式重分区。该设计牺牲部分超图表达能力，但便于在线执行和工程集成。

## 第 3 章 系统背景与基线架构

### 3.1 WookongDB MP 概述

WookongDB MP 支持两类运行模式：

- SQL 模式：支持基础建表、插入、删除、更新、查询、join 和显式事务；
- 负载模式：支持 SmallBank、YCSB 和 TPCC 标准负载。

事务并发控制采用 2PL。负载模式可运行在 Eager 或 Lazy 页面获取策略下。本文实验主要使用 SmallBank 亲和负载和 Lazy 模式。

### 3.2 Lazy Release 页面获取流程

在 Lazy 模式下，计算节点访问页面时先检查本地 Lazy Local Page Lock。如果本地已有对应远程所有权，则直接访问本地缓冲池；否则通过远程页表服务执行 `LRPSLock` 或 `LRPXLock`。获取远程锁后，系统可能需要：

1. 从存储层按 LSN 读取页面；
2. 等待当前最新持有者 PushPage；
3. 接收 LockSuccess 通知；
4. 在本地缓冲池安装页面；
5. 完成事务操作并在释放时根据 pending 状态决定是否推送或释放远程所有权。

该路径的主要成本包括远程 RPC、页面推送、日志等待和所有权元数据更新。

### 3.3 日志路径

当前系统在开启日志时会为更新和提交生成日志，并通过存储层刷盘。Stage 4 实验表明，log-on 场景中存储层 `fdatasync` 是主要瓶颈之一：10k log-on 实验中，baseline 平均 `fdatasync` 时间约 4.93 ms，affinity 约 4.75 ms；平均每次 `fdatasync` 合并的 logwrite 数约 1.03，说明当前组提交效果较弱。

## 第 4 章 亲和感知动态重分区设计

### 4.1 总体架构

本文系统由六个后台或辅助模块构成：

```text
Transaction Path
    -> SampleRing
    -> Aggregator
    -> EdgeShuffler
    -> ParMETIS Sidecar
    -> AssignmentTable
    -> MigrationWorker
```

事务成功提交后，系统记录其访问的数据项集合。Aggregator 周期性消费采样，生成本地亲和图。EdgeShuffler 在节点间交换边，Partitioner 调用 ParMETIS 侧车进程得到新的节点分配。AssignmentTable 以 RCU 风格发布新快照，MigrationWorker 根据快照限速迁移数据项。

### 4.2 事务采样与亲和图构建

事务 `T` 访问的数据项集合为 `A(T)`。对 `A(T)` 中的数据项两两组合生成边：

```text
for each transaction T:
  for each pair (u, v) in A(T):
    edge_weight[u, v] += 1
```

当前实现采用 `SampleRing` 降低主路径开销。采样只在事务成功提交后进入亲和图，避免 aborted transaction 的访问模式污染分区结果。

### 4.3 图聚合与衰减

Aggregator 每 `aggregator_tick_ms` 消费采样，并在 `partition_cycle_ms` 周期边界发布图快照。为适应负载漂移，边权引入 decay：

```text
w_t(e) = decay * w_{t-1}(e) + new_samples_t(e)
```

当前配置中 `edge_decay_factor=0.5`，`aggregator_tick_ms=50`，`partition_cycle_ms=1000`。

### 4.4 分布式边交换

由于每个计算节点只能观察本地事务访问，系统需要跨节点合并亲和图信息。EdgeShuffler 通过 AffinityService 在节点间交换图边和顶点访问信息，并通过 barrier 保证分区 epoch 的一致性。

### 4.5 ParMETIS 侧车重分区

Partitioner 将亲和图编码为 ParMETIS 输入，包括：

- `vtxdist`：分布式顶点范围；
- `xadj/adjncy`：CSR 图结构；
- `adjwgt`：边权；
- `vwgt`：顶点负载权重；
- `vsize`：迁移成本近似；
- `prev_part`：上一次分区结果。

系统使用侧车进程而不是直接链接 ParMETIS，以隔离 MPI 运行时复杂性，并降低主进程的工程耦合度。侧车通过 Unix Domain Socket 与计算节点通信。

### 4.6 AssignmentTable

AssignmentTable 维护 `tuple_id -> node_id` 的映射。读路径使用 `std::shared_ptr` 快照和 atomic load/store，避免阻塞事务。写路径通过 Merge 发布新快照，并使用 TTL 控制冷数据项增长。

该机制有两个作用：

1. 热路径可快速判断数据项应由哪个节点服务；
2. MigrationWorker 可扫描快照，找出目标节点不是本节点的数据项并生成迁移计划。

### 4.7 后台迁移

MigrationWorker 每 `migration_tick_ms` 执行一次：

```text
while not stopped:
  sleep(migration_tick_ms)
  PlannerSweep(AssignmentTable)
  plans = Drain(migration_batch)
  for plan in plans:
    if MigrateOne(plan):
      migrations_done++
    else:
      migrations_failed++
```

当前配置中 `migration_tick_ms=200`。`migration_batch=50` 时理论上每个 tick 最多执行 50 个计划，约 250 migrations/s；实现中 PlannerSweep 可提前规划更多候选，但 Drain 控制每轮实际迁移数量。

`MigrateOne` 的策略是复制 tuple bytes 到目标节点新页面，更新 BLink 索引，并删除源 slot。日志用于保证恢复时两侧状态一致。当前实现说明中明确指出 BLink 索引恢复语义仍需更严格验证，因此正式论文需要单独补充恢复一致性实验。

### 4.8 数据项标识与亲和建模

本文以数据项而不是页面作为重分区基本单位。原因是共享存储系统中的页面通常包含多个账户或记录，而事务亲和关系往往发生在记录层。例如 SmallBank 中两个账户频繁被同一事务同时访问，如果直接以页面为单位迁移，可能把大量无关冷记录一并移动，增加迁移成本和负载扰动。

实现中，系统将业务键编码为统一的 `tuple_id`。对于 SmallBank，`tuple_id` 对应账户 id；对于 YCSB，可以直接使用 key；对于 TPCC，需要将 warehouse、district、customer、order 等复合键映射到全局唯一 id。亲和图中的顶点即为 `tuple_id`，边表示两个数据项在事务中共同出现。

亲和图采用加权无向图：

```text
G_t = (V_t, E_t, W_t)
V_t: 在时间窗口 t 中被访问过的数据项
E_t: 同一事务中共同访问过的数据项对
W_t(u, v): u 和 v 的共现强度
```

对于访问集合大小为 `k` 的事务，最多产生 `k * (k - 1) / 2` 条边。为了限制主路径开销，本文当前实现对每个事务采样的数据项数量设置上界，并在后台线程中完成真正的边聚合。

### 4.9 事务主路径采样

亲和采样发生在事务成功提交之后。这样做有两个目的：

1. 避免 aborted transaction 的临时访问污染亲和图；
2. 将采样点放在事务状态已经确定之后，降低与并发控制逻辑的耦合。

事务执行过程中，系统已经维护 read-only set 和 read-write set。提交成功后，采样模块读取本事务实际访问的数据项集合，并将其写入每个 worker 的 SampleRing。SampleRing 是单生产者或低竞争写入的数据结构，Aggregator 后台线程周期性 drain。事务主路径只做轻量 push，不执行图划分或跨节点 RPC。

采样伪代码如下：

```text
RecordTxn(T):
  if affinity disabled:
    return
  items = CollectAccessedTupleIds(T.read_set, T.write_set)
  if items.size < 2:
    return
  items = DeduplicateAndCap(items)
  local_sample_ring.push(items)
```

其中 `DeduplicateAndCap` 的作用是避免同一事务内重复访问同一键导致边权膨胀，并限制单个大事务对 SampleRing 和图聚合的瞬时冲击。

### 4.10 图聚合、衰减与剪枝

Aggregator 以固定 tick 周期从所有 worker 的 SampleRing 中取出样本，将事务访问集合转换为边，并累加到 LocalGraph。对每个样本 `S = {v1, v2, ..., vk}`，Aggregator 对所有 `i < j` 执行：

```text
edge_weight[vi, vj] += 1
node_access[vi] += 1
node_access[vj] += 1
```

图聚合还维护每个顶点的本节点访问次数，用于分区时构造顶点权重 `vwgt`。若某个数据项在本节点访问频繁，则该顶点迁移到其他节点需要付出更高机会成本；反之，若该数据项经常与远端节点的数据项共现，则重分区可能将其重新指派给远端节点。

为了适应热点变化，系统在每个分区周期对旧边权执行衰减：

```text
w_new(e) = decay_factor * w_old(e) + w_observed(e)
```

当前配置中 `decay_factor=0.5`。这使系统对近期访问更敏感，同时保留一部分历史信息，避免短时波动导致数据项来回迁移。正式实验中需要系统评估 `decay_factor` 的影响，例如 `0.0, 0.25, 0.5, 0.75, 0.9`。

### 4.11 分布式图划分协议

分布式重分区需要把多个计算节点观察到的图信息合并成 ParMETIS 可处理的输入。本文采用三阶段协议：

1. **边交换**：每个节点根据顶点或边的哈希归属，将本地边发送给负责节点，使同一条边的权重能被合并。
2. **顶点 inventory 交换**：节点交换当前持有或观察到的顶点集合，用于构造全局 `vtxdist` 和本地顶点编号。
3. **Assignment 分发**：分区完成后，各节点将自己负责的 `tuple_id -> node_id` 映射切片分发给其他节点，最终合并进本地 AssignmentTable。

ParMETIS 输入包含以下数组：

| 字段 | 含义 | 本文用途 |
|---|---|---|
| `vtxdist` | 各 rank 顶点范围 | 定义分布式图切分 |
| `xadj/adjncy` | CSR 邻接表 | 表示亲和边 |
| `adjwgt` | 边权 | 表示事务共现强度 |
| `vwgt` | 顶点权重 | 表示访问热度和负载 |
| `vsize` | 迁移成本 | 近似迁移代价 |
| `prev_part` | 上一轮分区 | 支持 adaptive repartition |

分区目标可概括为：

```text
minimize edgecut(G)
subject to load_balance(partition) <= ubvec
and migration_delta(prev_part, new_part) is controlled by repartition objective
```

其中 edgecut 表示被切到不同节点的亲和边权之和。edgecut 越低，说明强相关数据项越倾向于被分配到同一计算节点。

### 4.12 AssignmentTable 发布与热路径查找

AssignmentTable 使用 RCU 风格快照发布。Partitioner 每轮生成新的 assignment delta 后，与当前快照合并并原子发布。读者通过 atomic load 获取 shared pointer，查找过程不加锁。

查找逻辑为：

```text
Lookup(tuple_id, fallback_owner):
  snapshot = atomic_load(current_assignment)
  if tuple_id in snapshot:
    return snapshot[tuple_id].node_id
  return fallback_owner
```

这里 `fallback_owner` 是系统原有的页面或哈希归属逻辑。这样设计保证了冷数据项或尚未进入亲和图的数据项仍按原机制访问，避免 AssignmentTable 不完整时影响正确性。

为控制长期运行时的内存增长，AssignmentTable 合并时使用 TTL：

```text
keep entry if current_epoch - last_seen_epoch <= ttl_epochs
```

当前配置中 `assignment_ttl_epochs=30`。后续实验需要评估 TTL 对内存占用、迁移频率和吞吐的影响。

### 4.13 迁移计划生成与执行协议

MigrationWorker 以 AssignmentTable 为输入，扫描本节点当前可见的数据项。如果某个数据项的目标节点不是本节点，则生成迁移计划：

```text
Plan = (tuple_id, src_node, dst_node, table_id, rid)
```

计划进入 MigrationQueue 后，由迁移线程按 `migration_batch` 限速执行。一次迁移的关键步骤如下：

1. 根据 `tuple_id` 通过 BLink 定位源记录 `Rid`；
2. 获取源页面 X 锁，保证迁移期间记录不会被前台事务并发修改；
3. 在目标节点或目标页面上分配可用 slot；
4. 复制 DataItem 元数据和 value bytes；
5. 更新 BLink 索引，使后续查找指向新位置；
6. 标记源 slot 删除或无效；
7. 写入必要的 insert/delete/update 日志；
8. 释放页面锁，并更新迁移统计。

迁移必须满足三个不变量：

```text
I1: 对任意 tuple_id，BLink 至多指向一个有效版本；
I2: 迁移完成后，AssignmentTable 的目标节点与物理位置逐步收敛；
I3: 前台事务只能看到迁移前或迁移后的完整记录，不能看到半迁移状态。
```

由于迁移与前台事务共享锁、缓冲池和存储 RPC，迁移线程必须限速。本文使用两个参数控制迁移强度：

- `migration_tick_ms`：迁移调度周期；
- `migration_batch`：每个周期最多执行的迁移数。

后续实验会系统扫描 `migration_batch` 和 `migration_tick_ms`，寻找吞吐收益与迁移干扰之间的平衡点。

## 第 5 章 实验方法与评价体系

本章定义本文的实验问题、实验变量、评价指标和汇总口径。由于后续还将继续补充更多实验，本章采用可扩展实验框架：先固定当前已完成的本地双节点实验，再给出后续多机、多负载和消融实验的统一方法。

### 5.1 研究问题

本文实验围绕以下问题展开：

| 编号 | 问题 | 评价方式 |
|---|---|---|
| RQ1 | 亲和重分区是否能降低远程页面访问比例？ | 比较 `from_remote_ratio`、`lock_ratio` |
| RQ2 | 远程访问降低是否能转化为吞吐提升？ | 比较 cluster throughput 与 wall-clock throughput |
| RQ3 | 日志开启与关闭时，亲和机制收益是否不同？ | 分别运行 log-on 与 no-log 实验 |
| RQ4 | 迁移速度如何影响收敛和前台干扰？ | 扫描 `migration_batch` 与 `migration_tick_ms` |
| RQ5 | 图划分参数如何影响 edgecut 和迁移量？ | 扫描 `partition_cycle_ms`、`decay_factor`、`ubvec` |
| RQ6 | 该方法是否能推广到不同负载和节点规模？ | 扩展到 YCSB、TPCC 和多机多节点 |

当前已完成实验主要回答 RQ1、RQ2 和 RQ3 的初步版本；RQ4 到 RQ6 将在后续实验中继续补齐。

### 5.2 实验系统配置

当前实验为本地双计算节点环境：

| 项目 | 当前设置 |
|---|---|
| compute nodes | 2 个逻辑计算节点，均运行在 `127.0.0.1` |
| storage node | 1 个本地存储服务 |
| metadata/page-table service | 1 个本地服务 |
| threads per machine | 2 |
| coroutine per thread | 1 |
| page mode | Lazy |
| partition cycle | 1000 ms |
| aggregator tick | 50 ms |
| migration tick | 200 ms |
| timeseries tick | 1000 ms |
| ParMETIS sidecar | 每个计算节点对应 1 个 sidecar rank |

后续多机实验将补充：

| 项目 | 待记录内容 |
|---|---|
| CPU | 型号、核心数、NUMA 拓扑 |
| Memory | 总容量、频率 |
| Storage | SSD/NVMe 型号、文件系统、同步写配置 |
| Network | 带宽、RTT、是否跨机架 |
| Software | OS、compiler、brpc、MPI、ParMETIS 版本 |

### 5.3 Workload 配置

当前主负载为 `smallbank_aff`。配置如下：

| 参数 | 值 |
|---|---:|
| `num_accounts` | 500000 |
| `num_hot_accounts` | 100000 |
| `use_zipfian` | 1 |
| `zipf_theta` | 0.92 |
| `affinity_txn_ratio` | 0.98 |
| `affinity_graph_mode` | `interleaved_hub` |
| `affinity_group_count` | 64 |
| `affinity_group_hubs` | 2 |
| `affinity_hub_weight` | 0.9 |
| `friend_degree_min` | 4 |
| `friend_degree_max` | 6 |
| `freq_amalgamate` | 40 |
| `freq_send_payment` | 40 |
| other tx frequency | each 5 |

该负载用于模拟具有明确事务共现结构的热点账户访问。后续会加入三类扩展：

1. **YCSB 参数扫描**：调整 read/write ratio、Zipf theta 和 key range，评估方法对简单键值负载的适应性。
2. **TPCC 事务混合**：覆盖 NewOrder、Payment、Delivery、OrderStatus 和 StockLevel，评估复杂多表事务。
3. **热点漂移负载**：周期性改变热点集合或 affinity group，评估衰减因子和 TTL 对动态负载的响应速度。

### 5.4 实验变量

实验变量分为系统变量、亲和变量和负载变量。

| 类型 | 变量 | 说明 |
|---|---|---|
| 系统变量 | `generate_log` | 是否生成并刷写日志 |
| 系统变量 | `threads` | 每个计算节点 worker 数 |
| 系统变量 | `coroutine_num` | 每 worker coroutine 数 |
| 系统变量 | `log_flush_interval_ms` | 日志刷盘周期 |
| 系统变量 | `log_flush_batch_trigger` | 组提交触发阈值 |
| 亲和变量 | `partition_cycle_ms` | 图划分周期 |
| 亲和变量 | `aggregator_tick_ms` | 采样聚合周期 |
| 亲和变量 | `migration_tick_ms` | 迁移调度周期 |
| 亲和变量 | `migration_batch` | 每轮迁移上限 |
| 亲和变量 | `edge_decay_factor` | 边权衰减因子 |
| 亲和变量 | `assignment_ttl_epochs` | AssignmentTable TTL |
| 亲和变量 | `ubvec` | ParMETIS 负载均衡容忍度 |
| 负载变量 | `attempted_num` | 每 worker 尝试事务数 |
| 负载变量 | `zipf_theta` | 热点倾斜程度 |
| 负载变量 | `affinity_txn_ratio` | 亲和事务比例 |
| 负载变量 | tx mix | 不同事务类型占比 |

### 5.5 Baseline 与对照组

当前实验包含两个主要对照：

| 组别 | 设置 | 目的 |
|---|---|---|
| Baseline | Lazy 模式，关闭 affinity | 衡量原始 Lazy Release 性能 |
| Affinity | Lazy 模式，开启 affinity 全流水线 | 衡量亲和重分区收益 |

后续需要补充以下消融对照：

| 组别 | 设置 | 目的 |
|---|---|---|
| Sampling-only | 只采样不分区 | 测量采样主路径开销 |
| Partition-only | 分区但不迁移 | 区分图划分收益和迁移收益 |
| Static assignment | 固定亲和分配 | 与动态调整对比 |
| No decay | `edge_decay_factor=1` 或关闭衰减 | 验证衰减对热点漂移的价值 |
| No TTL | AssignmentTable 不裁剪 | 测量长跑内存增长 |
| Migration sweep | 扫描 migration batch/tick | 找到收敛与干扰平衡点 |

### 5.6 指标定义

本文报告以下指标：

| 指标 | 定义 | 用途 |
|---|---|---|
| `throughput` | 各节点吞吐聚合 | 主性能指标 |
| `wall-clock throughput` | cluster commit 数 / max wall time | 校验聚合吞吐 |
| `from_remote_ratio` | remote fetch / total fetch | 衡量远程访问比例 |
| `from_local_ratio` | local fetch / total fetch | 衡量本地访问比例 |
| `from_storage_ratio` | storage fetch / total fetch | 衡量存储读取比例 |
| `lock_ratio` | remote lock / total fetch | 衡量所有权转移频率 |
| `ownership_transfer_time_total` | 远程所有权转移总耗时 | 分析远程访问成本 |
| `tx_exe_time` | 事务执行阶段总耗时 | 分析主路径成本 |
| `tx_fetch_exe_time` | 页面获取阶段总耗时 | 分析页面路径成本 |
| `migrations_planned` | 规划迁移数量 | 衡量分区变化强度 |
| `migrations_done` | 完成迁移数量 | 衡量迁移收敛 |
| `migrations_failed` | 未完成迁移数量 | 衡量迁移有效性 |
| `edgecut` | 跨分区边权 | 衡量图划分质量 |
| `fdatasync_count` | 存储刷盘次数 | 分析日志路径 |
| `avg_fdatasync_time_ms` | 平均刷盘耗时 | 分析日志瓶颈 |

### 5.7 汇总口径

实验脚本已修复 per-node 与 cluster 结果混淆问题。当前 Stage 4 输出中显式包含：

- `aggregation_scope=cluster`
- `aggregation_node_count=2`
- `attempted_num_scope=per_worker`
- `expected_cluster_attempted_num`

cluster attempted 数计算为：

```text
expected_cluster_attempted_num = attempted_num_per_worker
                               * compute_node_count
                               * threads_per_node
```

例如 `attempted_num=10000`、2 节点、每节点 2 线程时，cluster 尝试事务数为 40000。

吞吐使用两种口径同时报告：

```text
reported_throughput = sum(node_i_reported_throughput)
wall_clock_throughput = sum(committed_txn_i) / max(node_i_elapsed_time)
```

当各节点结束时间接近时，两者应基本一致。若差异明显，说明节点负载不均、启动/退出时间不一致或统计口径存在偏差，需要单独分析。

### 5.8 当前实验矩阵与后续扩展

当前已完成实验：

| 实验 | log | attempted scope | migration setting | 目的 |
|---|---|---:|---|---|
| SmallBank log-on 2k | on | per worker | batch 50 | 短运行收益验证 |
| SmallBank log-on 10k | on | per worker | batch 50 | 主要收益实验 |
| SmallBank no-log 2k | off | per worker | batch 50 | 无日志短运行敏感性 |
| SmallBank no-log 100k | off | per worker | batch 10 | 高吞吐迁移收敛观察 |
| SmallBank no-log 2M | off | cluster total | batch 10 | 长运行收敛观察 |

后续实验计划：

| 实验方向 | 变量 | 目标 |
|---|---|---|
| 多节点扩展 | 2/4/8 compute nodes | 验证规模化收益 |
| 迁移参数扫描 | batch, tick | 找到最佳迁移强度 |
| 分区参数扫描 | cycle, decay, ubvec | 验证图划分质量 |
| 日志参数扫描 | flush interval, batch trigger | 区分日志瓶颈和亲和收益 |
| YCSB | theta, read/write ratio | 验证简单 KV 负载 |
| TPCC | warehouse, tx mix | 验证复杂事务负载 |
| 热点漂移 | hot set period | 验证动态适应能力 |

## 第 6 章 实验结果

### 6.1 开启日志：10k 每 worker

这是当前收益最明显的一组实验，用于回答 RQ1 和 RQ2：亲和重分区是否能降低远程访问比例，并转化为吞吐提升。

| 指标 | Baseline | Affinity | 变化 |
|---|---:|---:|---:|
| Throughput | 165.614 tx/s | 313.202 tx/s | +89.12% |
| Wall-clock throughput | 165.363 tx/s | 311.929 tx/s | +88.63% |
| Total time | 241.795 s | 128.228 s | -46.97% |
| Remote ratio | 0.25688 | 0.18143 | -29.37% relative |
| Lock ratio | 0.32679 | 0.23875 | -26.94% relative |
| Ownership avg | 6.879 ms | 1.041 ms | -84.87% |
| Migrations planned | 0 | 23801 | - |
| Migrations done | 0 | 23801 | 100% |
| Migrations failed | 0 | 0 | - |

日志路径：

| 指标 | Baseline | Affinity |
|---|---:|---:|
| fdatasync count | 42965 | 25830 |
| avg fdatasync time | 4.928 ms | 4.748 ms |
| avg logwrite / fdatasync | 1.035 | 1.030 |

该实验表明，亲和重分区降低了远程页面访问比例和所有权转移耗时，从而在 log-on 场景中显著提升吞吐。虽然日志刷盘仍是主瓶颈，但 affinity 减少了远程等待与页面推送放大的事务执行时间。

### 6.2 开启日志：2k 每 worker

| 指标 | Baseline | Affinity | 变化 |
|---|---:|---:|---:|
| Throughput | 181.896 tx/s | 270.528 tx/s | +48.73% |
| Remote ratio | 0.22215 | 0.16439 | -26.00% relative |
| Migrations planned | 0 | 5133 | - |
| Migrations done | 0 | 5082 | 99.01% |

2k 实验中 affinity 仍然有效，但提升幅度低于 10k。原因可能是实验运行时间较短，亲和图采样、分区和迁移尚未充分进入稳态。

### 6.3 关闭日志：2k 每 worker

| 指标 | Baseline | Affinity | 变化 |
|---|---:|---:|---:|
| Throughput | 4987.83 tx/s | 5413.02 tx/s | +8.52% |
| Remote ratio | 0.16544 | 0.16311 | 轻微下降 |
| Partition runs | 0 | 2 | - |
| Migrations done | 0 | 71 | - |

no-log 2k 场景运行时间太短，只有少量 partition 和 migration，不能充分评价 affinity 机制。

### 6.4 关闭日志：100k 每 worker，batch=10

| 指标 | Baseline | Affinity | 变化 |
|---|---:|---:|---:|
| Throughput | 9509.99 tx/s | 9011.26 tx/s | -5.24% |
| Remote ratio | 0.21043 | 0.21196 | 略升 |
| Migrations planned | 0 | 10549 | - |
| Migrations done | 0 | 2689 | 25.49% |

该结果显示，在 no-log 高吞吐场景下，事务推进速度显著快于迁移收敛速度。batch=10 较稳定，但迁移完成比例不足，AssignmentTable 与物理数据位置之间存在较长滞后，导致远程比例未下降，后台维护反而引入额外开销。

### 6.5 关闭日志：2M cluster attempts，batch=10

| 指标 | Baseline | Affinity | 变化 |
|---|---:|---:|---:|
| Throughput | 10229.33 tx/s | 9175.51 tx/s | -10.30% |
| Wall-clock throughput | 10171.61 tx/s | 9118.83 tx/s | -10.35% |
| Remote ratio | 0.21087 | 0.21169 | 略升 |
| Migrations planned | 0 | 42957 | - |
| Migrations done | 0 | 10767 | 25.06% |
| Migration phase | - | 217.029 s | - |

长跑 no-log 实验进一步验证：当前 batch=10 迁移策略并未在高吞吐场景中形成有效 locality 改善。affinity graph 和 partitioner 正常运行，但迁移速度相对事务速度不足，且额外 notify/push/ownership 开销抵消了潜在收益。

### 6.6 当前结果小结

当前实验呈现出清晰的分层现象：

| 场景 | 观察 | 解释 |
|---|---|---|
| log-on, 2k/10k | affinity 显著提升吞吐 | 远程访问和 ownership transfer 成本被有效降低 |
| no-log, 2k | affinity 有轻微收益 | 运行时间短，分区和迁移轮次较少 |
| no-log, 100k/2M | affinity 暂未体现收益 | 事务推进快，当前迁移策略收敛不足 |

这说明本文方法的核心机制已经在 log-on 场景下生效，但要在 no-log 高吞吐场景下获得稳定收益，需要继续优化迁移计划生成、迁移限速、分区周期和后台线程资源隔离。后续实验将围绕这些参数展开系统扫描。

### 6.7 后续实验扩展位

正式论文将继续补充以下结果表：

| 编号 | 实验 | 需要回答的问题 |
|---|---|---|
| E1 | `migration_batch` sweep | 迁移强度如何影响吞吐和收敛 |
| E2 | `partition_cycle_ms` sweep | 更频繁或更稀疏分区是否更优 |
| E3 | `edge_decay_factor` sweep | 热点变化时边权衰减如何影响质量 |
| E4 | log flush 参数 sweep | 日志组提交优化后 affinity 收益是否变化 |
| E5 | YCSB Zipf sweep | 不同热点倾斜程度下是否仍有效 |
| E6 | TPCC warehouse sweep | 多表复杂事务下亲和图是否能捕获局部性 |
| E7 | 多节点扩展 | 4/8 节点下 edgecut、迁移量和吞吐如何变化 |

## 第 7 章 讨论

### 7.1 为什么 log-on 场景提升明显

log-on 场景中事务执行本身受日志刷盘限制，远程所有权转移会进一步放大等待时间。affinity 通过迁移强相关数据项，降低远程访问比例和 ownership transfer 平均耗时，因此即使日志仍然是瓶颈，也能显著缩短事务执行和提交前等待时间。

在 10k log-on 实验中：

- remote ratio 从 25.69% 降至 18.14%；
- ownership transfer avg 从 6.879 ms 降至 1.041 ms；
- 吞吐提升 89.12%。

### 7.2 为什么 no-log 长跑没有提升

no-log 场景消除了日志刷盘瓶颈，事务推进速度提高到约 9k 到 10k tx/s。此时 affinity 后台链路必须更快地完成采样、分区、AssignmentTable 发布和数据迁移，否则前台事务看到的仍是旧布局或半收敛布局。

当前 no-log 长跑中，batch=10 的迁移完成比例约 25%。这意味着大量计划没有在实验结束前完成，重分区收益无法兑现，反而引入额外后台开销。

### 7.3 迁移批量的意义

`migration_batch` 控制每个 `migration_tick_ms` 周期内最多执行多少迁移计划。它不是事务 batch，也不是日志 batch。

在当前配置中：

```text
migration_tick_ms = 200
migration_batch = 50
theoretical cap ≈ 250 migrations/s
```

batch 太小会导致收敛慢，AssignmentTable 已经更新但物理数据位置尚未完成迁移；batch 太大则会增加后台迁移对前台事务的锁竞争、缓冲池竞争和存储 RPC 竞争。因此，`migration_batch` 是亲和重分区系统中的核心调优参数，后续需要和 `migration_tick_ms`、`partition_cycle_ms` 一起扫描。

### 7.4 日志系统瓶颈

当前 log-on 实验中，平均 `fdatasync` 时间约 4.7 到 4.9 ms，且 `avg_logwrite_per_fdatasync≈1.03`。这说明组提交几乎没有把多个日志写有效合并。未来需要优化：

- 增大 log flush batch trigger；
- 调整 flush interval；
- 区分 commit log 与 update log flush 策略；
- 在不破坏一致性的前提下合并更多事务日志；
- 评估异步提交或 group commit 对吞吐和延迟的影响。

## 第 8 章 研究边界与后续实验

### 8.1 当前限制

1. 当前实验主要来自单机本地双节点，不足以证明真实集群网络环境下的结论。
2. 当前主要负载是 SmallBank affinity，YCSB 和 TPCC 结果尚未系统呈现。
3. no-log 高吞吐场景下，当前迁移参数尚未充分收敛，需要系统扫描迁移与分区参数。
4. BLink 迁移恢复语义尚需更严格论证。
5. 当前 group commit 效果弱，日志瓶颈可能掩盖或放大 affinity 效果。

### 8.2 面向顶级期刊投稿的后续实验

1. **真实多机实验**：至少 2、4、8 个计算节点，独立存储节点，报告网络 RTT、磁盘型号和 CPU。
2. **多负载实验**：SmallBank、YCSB、TPCC，覆盖只读/读写混合、Zipf 参数变化、热点漂移。
3. **消融实验**：
   - no affinity；
   - only sampling；
   - only partition no migration；
   - migration batch 变化；
   - decay factor 变化；
   - TTL 变化；
   - ParMETIS adaptive vs static partition。
4. **长时间实验**：长时间运行 30 min / 1 h，观察内存、AssignmentTable size、迁移完成率和吞吐稳定性。
5. **恢复一致性实验**：验证迁移过程中日志、索引和页面状态在恢复流程中的一致性。
6. **对比实验**：与静态哈希、页面 owner baseline、Chimera 原始策略或其他动态分片策略对比。
7. **开销拆解**：采样开销、聚合开销、分区耗时、迁移耗时、RPC 数、日志刷盘耗时。

## 第 9 章 结论

本文围绕多主共享存储数据库中的远程所有权转移问题，设计并实现了一套负载亲和动态重分区机制。该机制通过在线事务采样构建亲和图，利用分布式图划分生成新的数据项归属，并通过限速后台迁移逐步改善数据局部性。

当前实验表明，在开启日志的 SmallBank 亲和负载中，该机制可以显著降低远程访问比例并提升吞吐，10k 每 worker 实验达到 89.12% 的吞吐提升。然而，在关闭日志的高吞吐场景中，当前迁移限速和后台调度参数尚未支撑快速收敛，长跑实验未观察到收益。这一结果揭示了一个关键事实：动态亲和机制不是单纯的图划分问题，而是事务速度、迁移速度、日志刷盘、页面所有权协议和恢复语义之间的系统性协同问题。

未来工作将聚焦三点：第一，系统扫描迁移批量、迁移周期、分区周期和边权衰减参数；第二，优化日志组提交和迁移调度，使 no-log 与 log-on 场景都能形成可解释的收益曲线；第三，在真实多机环境和更多负载上完成顶刊级系统评估。

## 顶刊投稿版摘要草案

Multi-primary shared-storage databases are attractive for cloud-native transaction processing, but they suffer from frequent page ownership transfers when correlated hot records are accessed across compute nodes. This paper presents an affinity-driven dynamic repartitioning mechanism that learns transaction co-access patterns online, repartitions a distributed affinity graph, and migrates records in the background to reduce future ownership transfers. We implement the design in WookongDB MP with an end-to-end pipeline consisting of transaction sampling, graph aggregation, distributed edge shuffling, ParMETIS-based repartitioning, lock-free assignment-table publication, and rate-limited migration. Preliminary two-node experiments on a SmallBank affinity workload show that, with logging enabled, our approach improves throughput by up to 89.12% and reduces the remote access ratio from 25.69% to 18.14%. We also identify a key systems tradeoff: when logging is disabled and transaction throughput increases by an order of magnitude, migration convergence becomes the bottleneck and may offset affinity benefits. These results suggest that workload-affinity repartitioning is promising but must be co-designed with logging, migration scheduling, and ownership-transfer protocols.

## 论文标题备选

1. 面向共享存储多主数据库的负载亲和动态重分区机制研究
2. Affinity-Driven Dynamic Repartitioning for Multi-Primary Shared-Storage Databases
3. Reducing Ownership Transfers in Multi-Primary Shared-Storage Databases via Online Affinity Repartitioning
4. Workload-Aware Record Migration for Cloud-Native Shared-Storage Transaction Processing

## 投稿目标建议

数据库方向顶级期刊/会议式期刊可考虑：

- PVLDB：系统实现与实验要求高，适合该工作，但需要真实集群和完整对比。
- ACM TODS：更偏长期完整论文，需要更强理论化问题定义和系统分析。
- IEEE TKDE：可接受系统设计加实验，但仍需要多负载、多规模实验。

当前版本定位为系统论文长文初稿；随着后续实验补齐，可逐步整理为顶级期刊投稿版本。

## 需要补齐的数据表

| 实验 | 当前状态 | 顶刊所需状态 |
|---|---|---|
| SmallBank log-on | 已有 2k、10k 本地双节点 | 扩展到多机、多节点、多参数 |
| SmallBank no-log | 已有 2k、100k、2M | 补充迁移参数扫描 |
| YCSB | 待补 | Zipf theta sweep、read/write sweep |
| TPCC | 待补 | NewOrder/Payment 混合、warehouse sweep |
| group commit | 初步诊断 | 系统调参和优化实验 |
| recovery consistency | 待补 | 明确迁移恢复协议和一致性验证 |
| baseline comparison | 部分已有 | 与 Chimera/静态 owner/无迁移对比 |
