# 面向共享存储多主数据库的在线亲和图划分与受控迁移

**面向 NDBC/SIGMOD 风格的论文初稿 v0**

English title: **Online Affinity Graph Partitioning and Controlled Migration for Shared-Storage Multi-Primary Databases**

> **Draft status**: 这是 v0。文中以 `**TODO(...)**` 标记的位置是阻碍顶会投稿的硬缺口：缺 baseline、缺形式化论证、缺 motivating measurement、缺多 workload/scaling 验证。当前评估只刻画了系统行为，**尚未证明本方法相对 baseline 的价值**。完整缺口清单见 §6.7。在补齐 §6.7 列出的实验之前，本稿不适合作为最终投稿稿。

## 摘要

共享存储多主数据库通过允许多个计算节点访问同一份持久化数据获得弹性，但在偏斜 OLTP 负载下，跨节点 ownership transfer、远程 fetch 与缓存一致性协议会主导事务延迟。已有工作把这一问题约化为事务访问图的 min-cut 划分（Schism, E-Store, Clay 等）。本文实证发现，在共享存储多主架构下仅优化图割并不能预测吞吐：在我们的 32-worker 短时对比中，最低 edgecut 的 MP-Router 模式同时给出最低 TPS。我们提出 **placement × routing 二维视角**，并构造一套以 **AssignmentTable** 为核心的在线流水线：事务采样 → 在线亲和图 → ParMETIS 候选分区 → AssignmentTable 发布 → 受控后台迁移 → 路由反馈。AssignmentTable 把"理论归属"与"物理布局"解耦，使分区结果不再直接冲击运行时；受控迁移通过批次、churn 门控和失败 backoff 把图收益逐步翻译为本地访问。在 WookongDB MP/Hybrid_Cloud_MP 原型 4 compute + 1 service 的 SmallBank affinity workload 上，对齐复测完成 9700 万事务、吞吐 30,549.89 TPS、cluster local 84.16%、weighted cut ratio 2.58%。**注意：当前评估缺少与 affinity-disabled、静态分片、Schism/Clay 复现的 baseline 对比，因此结果仅刻画原型性能而非证明方法相对最先进工作的优势**。我们同时指出 cluster local 高不等价于负载均衡：当前 node0 路由占比 7.89%、local 70.53%；node3 路由占比 43.07%、local 88.33%。

## Abstract

Shared-storage multi-primary databases enable multiple compute nodes to access the same persistent data, but under skewed OLTP workloads, cross-node ownership transfers, remote fetches, and cache-coherence protocols dominate transaction latency. Prior work reduces this to a min-cut partitioning problem over a transaction co-access graph (Schism, E-Store, Clay). We empirically observe that minimizing edge cut alone fails to predict throughput in this setting: in our short-window comparison, the MP-Router mode with the lowest edgecut also achieves the lowest TPS. We argue affinity optimization is a **two-dimensional problem (placement × routing)** in this setting and propose an online pipeline centered on an **AssignmentTable** abstraction: transaction sampling → online affinity graph → ParMETIS candidate partitioning → AssignmentTable publish → controlled background migration → routing feedback. AssignmentTable decouples logical assignment from physical layout; controlled migration translates graph quality into runtime locality through batching, churn gating, and failure backoff. On a 4-compute-node WookongDB MP prototype running a SmallBank affinity workload, our aligned re-test completes 97M transactions at 30,549.89 TPS with 84.16% cluster locality and a weighted cut ratio of 2.58%. **Caveat: this evaluation lacks baselines against affinity-disabled, static partitioning, and Schism/Clay reproductions; the numbers characterize the prototype rather than demonstrate superiority over prior art**. We also report that high cluster locality masks per-node skew: node0 receives 7.89% of routed transactions with 70.53% locality, while node3 receives 43.07% with 88.33%.

## 关键词

共享存储数据库；多主数据库；事务亲和性；在线图划分；ParMETIS；数据迁移；MP-Router；OLTP

## 1. 引言

共享存储多主数据库（Amazon Aurora、Microsoft Socrates、Alibaba PolarDB、WookongDB MP 等）通过让多个计算节点访问同一份持久化数据获得弹性。但当 OLTP 工作负载具有访问偏斜时，跨节点 ownership transfer 和远程 fetch 会主导事务延迟。

> **TODO(measurement)**: 给出本原型在 SmallBank without affinity 下的 cross-node fetch 占比与对 TPS 的影响。当前 §1 缺 motivating measurement——本应在此放一张 figure，展示 "no-affinity baseline 中 X% 事务因跨节点 fetch 而停顿"。该数据需要在 §6.7 实验补全后回填。

一个自然的想法是利用事务的共访问模式做亲和性放置：把经常一起出现的 tuple 放在同一节点。已有工作把这归约为图的 min-cut 问题：将事务建图、对图做平衡 min-cut 划分（Schism[1], E-Store[4], Clay[5]）。

但在共享存储多主架构下，**仅最小化图割并不能预测吞吐**。在我们 32-worker 短时对比中（§6.4 表 4）：

- MP-Router **mode 23** 取得最低 edgecut（6,557），但同时取得最低 TPS（18,781，cluster local 56.99%）。
- mode 13 与 mode 24 edgecut 是 mode 23 的 2-4 倍（24,373 / 13,212），TPS 反而提高 36-39%（25,517 / 26,046）。

这违反"图割越低、locality 越高、TPS 越高"的直觉。我们认为根源是：**亲和性优化在共享存储多主架构下是二维问题——placement（数据归属何处）和 routing（事务被路由到何处）独立影响 locality**。已有 min-cut 方法只优化 placement 一维。一旦 routing 层与 placement 不协同，placement 的图收益就无法转化为吞吐。

本文沿这个观察设计方法：

- 用 **AssignmentTable** 作为 placement 层的显式抽象，与 routing 解耦。Placement 决策被 AssignmentTable 表达，但不立即变为物理迁移。
- 用**受控后台迁移**把 AssignmentTable 更新逐步落地，通过批次、churn 门控、失败 backoff 限制后台对前台的冲击。
- 用**异步反馈链分析**判断改动来自 placement 还是 routing，避免错误归因（§7.1）。

**贡献**：

1. **二维视角与经验证据**：将亲和性优化形式化为 placement × routing 二维问题，并通过 §6.4 短时 mode 对比给出 edgecut 不预测 TPS 的实证。
2. **AssignmentTable 抽象**：定义 placement 与物理布局解耦的中间层，给出其不变量（§3.2）与与 ParMETIS、受控迁移的集成方法。
3. **跨实验图质量度量**：提出 `weighted_cut_ratio = edgecut / total_edge_weight` 作为跨实验图质量对比的归一化指标，避免被图规模差异误导。
4. **原型实证**：在 4 compute + 1 service WookongDB MP 上完成 1 小时级运行，给出 mode 13 + 在线亲和优化的当前性能（30.55k TPS、84.16% cluster local）与当前瓶颈（路由偏斜、迁移失败）。

> **TODO(scope)**: 贡献 4 当前是"系统能跑到这个数"，**不是**"我们的方法比 X、Y、Z 都好"。在补齐 §6.7 列出的 baseline 与 ablation 实验之前，本文不能对 prior art 做优越性声明。完成 §6.7 后，贡献 4 应改写为"相对 no-affinity 提升 X%、相对 static hash 提升 Y%、相对 Schism reproduction 提升 Z%"的对比性陈述。

## 2. 背景与动机

### 2.1 共享存储多主数据库

共享存储多主数据库将持久化数据放在共享存储层，将事务执行放在多个计算节点。多个计算节点可以同时访问同一个逻辑数据库，并在运行时通过锁、元数据服务或 ownership 协议协调访问。这种架构避免了传统 shared-nothing 中固定分片带来的数据重分布成本，但使本地缓存和 page ownership 管理更加复杂。

当事务只访问本节点已拥有的数据时，系统可以走本地路径；当事务需要访问其他节点拥有的 page/tuple 时，就会触发远程 fetch、ownership 协调或 cache fusion[7]。对于偏斜 OLTP 负载，少量热门 tuple 反复出现在事务中，跨节点访问被放大。

> **TODO(citation)**: §2.1 引用 Aurora[SIGMOD17]、Socrates[SIGMOD19]、PolarDB[VLDB18]/Cao 等具体论文。当前只引用了 Oracle Cache Fusion，覆盖不全。

### 2.2 工作负载特性

OLTP 工作负载通常具有：

- **访问偏斜**：少量热门 key 出现在大量事务中（如 SmallBank 的 amalgamate 经常涉及若干热门账户）。
- **共访问稳定**：在窗口尺度上，"经常一起出现"的 tuple 对在事务之间稳定反复。
- **运行时演化**：业务热点不会永远不变。

> **TODO(measurement)**: 给出本实验 SmallBank workload 下的 (i) 实际 Zipfian 分布拟合；(ii) 单事务访问集大小分布；(iii) 共访问 tuple 对的复现窗口长度分布。这三组分布是论证"亲和性可被利用"的基础证据，当前完全缺失。

这三个特性共同决定：(i) 存在可被利用的亲和性结构，(ii) 静态分片难以持续贴合，(iii) 离线优化无法响应在线变化。本文方法因此设计为以事务窗口为单位的在线流水线，而非离线划分。

### 2.3 为什么静态策略不足

静态分片或静态路由面临三个问题：

第一，**负载随时间变化**。Zipfian 热点会迁移，固定哈希或范围分片无法持续贴合实际共访问关系。

第二，**路由模式独立影响 locality**。即使底层数据布局相同，不同 MP-Router mode 也会改变事务落点。这是 §6.4 实证的现象，也是本文 placement × routing 二维视角的基础。

第三，**数据布局优化不能只看理论最优**。在线系统中，迁移与前台事务竞争资源。一个在图上更优的分区，若导致大量迁移失败或 page ownership 频繁变化，可能降低 TPS。

## 3. 方法

### 3.1 问题陈述

记 `T` 为 tuple 集合，`N` 为 compute 节点集合，`W = ⟨τ_1, τ_2, …⟩` 为按时间排序的事务序列，每个事务 `τ` 关联访问集 `A(τ) ⊆ T`。记 `O_t: T → N` 为 `t` 时刻的物理 ownership 映射；`R: Transaction → N` 为路由策略（本文视为外部固定函数，由 MP-Router 决定）。

事务 `τ` 在时刻 `t` 的本地访问比例：

```text
local(τ, t) = |{x ∈ A(τ) : O_t(x) = R(τ)}| / |A(τ)|
```

我们的目标是基于 `t` 之前观测到的 W 历史选择 ownership 序列 `{O_t}`，最大化：

```text
∑_{τ ∈ W} local(τ, t_τ)
```

约束：

- **迁移预算**：`|{x : O_t(x) ≠ O_{t-Δ}(x)}| ≤ B` 在每个调度窗口 Δ 内。
- **负载均衡**：节点间数据量与流量比例不超过 `ubvec`。
- **可观测性**：`O_t` 只能基于 `t` 之前的 `W` 决定（在线决策）。

这是一个有预算约束的在线 placement 序列决策问题。静态方法在 `R` 已知且 `W` 已观察完毕时离线求解；本方法在 `W` 在线流入、`R` 不可改变的前提下求近似解。

> **TODO(formal)**: 上述目标函数与 ParMETIS 的"min weighted cut subject to balance"在何种约束下等价？需要：(a) 论证两者一致；或 (b) 给出近似比；或 (c) 明确两者是 proxy 关系并给出 proxy gap 实测（§6.4 已部分提供经验证据，但缺乏 formal bound）。

### 3.2 AssignmentTable：placement 抽象

记 `A: T → N ∪ {⊥}` 为 AssignmentTable，其中 `A(x) = n` 表示 tuple `x` 建议归属节点 `n`，`A(x) = ⊥` 表示未分配。AssignmentTable 满足以下不变量：

- **I1 (atomicity)**：`A` 在每个 `partition_cycle` 末端整体替换或保持不变；不存在部分更新的中间状态可被前台或后台读到。
- **I2 (decoupled read)**：前台事务读取 `O` 与后台 worker 读取 `A` 互不阻塞。`A.read(x)` 返回 `n` 不代表 `O(x) = n`。
- **I3 (eventual convergence)**：在迁移预算 `B` 充足、partition 决策稳定（churn 受限）的窗口内，`O` 在该窗口结束时与 `A` 在非热点 tuple 上一致。

> **TODO(formal)**: I3 当前是 informal claim。需要：(a) 形式化"churn 受限"（用 `max_changed_vertices_ratio` 阈值）；(b) 给出"非热点 tuple"的定义（用 backoff 失败次数 < k）；(c) 给出在这些约束下的进展性证明或反例。

为诊断 placement 决策质量与物理布局推进进度，定义四个访问覆盖比例：

- `best_access_ratio`：理论最优 assignment 能覆盖的访问比例（由观察到的 W 计算）。
- `parmetis_access_ratio`：直接采用 ParMETIS 输出能覆盖的比例。
- `assigned_access_ratio`：当前已发布 AssignmentTable 能覆盖的比例。
- `current_access_ratio`：当前物理 ownership `O` 实际覆盖的比例。

四者关系：`current ≤ assigned ≤ parmetis ≤ best`。各 ratio 之间的 gap 分别诊断"物理迁移进度"、"churn 门控保守度"、"分区算法到最优的距离"。具体数值见 §6（**TODO(measurement)**: 当前 §6 未单独报告四个 ratio，需要补充该表）。

### 3.3 在线亲和图

事务执行后，采样器记录 `A(τ)`，将同事务中的 tuple 对 `(t_i, t_j)` 累加为图边。基本指标：

- `n_vertices`：图顶点数。
- `n_edges`：图边数。
- `total_edge_weight`：边权之和。
- `edgecut`：在当前 ownership 下被切开的边权之和。

由于 `edgecut` 随图规模与采样量变化，本文使用：

```text
weighted_cut_ratio = edgecut / total_edge_weight
```

作为跨实验图质量的归一化对比指标。

> **TODO(formal)**: pairwise 边构造在事务访问集大小 k 较大时复杂度为 O(k²)。本文采样阶段是否做了子采样？子采样下 weighted_cut_ratio 是否仍然是无偏估计？需要补充实现细节与误差分析。

### 3.4 ParMETIS 候选分区

系统周期性把当前 affinity graph 发送给 ParMETIS sidecar[6]。ParMETIS 输出候选分区 `P: T → N`，但不直接修改 `O`：

```text
if churn(P, A_prev) ≤ max_changed_vertices_ratio:
  A ← P                          # 整体接受
else:
  A ← partially_accept(P, A_prev) # 只接受稳定且高收益的顶点变更
```

两个关键参数：`repart_itr` 控制 ParMETIS adaptive repartitioning 的迁移倾向，`ubvec` 控制负载不均衡容忍度。这些参数会显著影响分区稳定性、迁移规模和最终 TPS（§6.5 实证）。

> **TODO(scope)**: `partially_accept` 的具体实现（哪些顶点被保留、哪些被替换）当前未在论文中描述。需要补充算法或写明引用代码位置。

### 3.5 受控后台迁移

后台迁移器从 `A` 与 `O` 的 diff 中生成迁移任务：

```text
candidates ← {x ∈ T : A(x) ≠ O(x)}
candidates ← filter_failed_hot_tuples(candidates)  # backoff
candidates ← prioritize_by_expected_gain(candidates)
batch ← take_budget(candidates, B)
```

策略门控：

- **批次限制**：限制每轮迁移数量，避免后台瞬时冲击前台。
- **源页/目标合并**：同源 page、同目标节点的任务合并，减少分散迁移。
- **失败 backoff**：连续失败的 tuple 做指数退避，避免热点失败任务反复占满预算。
- **收益排序**：按 expected_gain 排序，优先迁移对 local ratio 边际收益高的任务。

> **TODO(formal)**: `expected_gain` 当前是 heuristic。需要：(a) 形式化定义（如"未来 W' 窗口内若 O(x)=A(x) 则 local 提升量的估计"）；(b) 与 ParMETIS 内部目标的一致性论证；(c) 说明 heuristic 的最差情况偏差。

### 3.6 路由反馈链

迁移成功后，MP-Router 通过 LOOKUP 或运行时 page id changes 逐步感知 ownership 变化。期望的反馈是：`from_local_ratio` ↑、`from_remote_ratio` ↓、`cache fusion ratio` ↓、fetch latency ↓、TPS ↑。但这些指标**异步演化**，不一定同步变化，因此分析必须区分各层贡献（§7.1）。

### 3.7 算法

```text
Algorithm 1: Online affinity optimization loop

Input:  transaction stream W, ownership O, assignment A, budget B
Output: continuously updated O converging to A

while system is running:
  S ← collect committed transaction accesses          # §3.3
  G ← update_affinity_graph(G, S)

  if partition_cycle_elapsed:                          # §3.4
    P ← ParMETIS_Repartition(G, ubvec, repart_itr)
    if churn(P, A) ≤ max_changed_vertices_ratio:
      A ← publish_assignment(P)                        # I1
    else:
      A ← partially_accept(P, A)

  candidates ← diff(A, O)                              # §3.5
  candidates ← filter_failed_hot_tuples(candidates)
  candidates ← prioritize_by_expected_gain(candidates)
  batch ← take_budget(candidates, B)

  for task in batch:
    result ← migrate_tuple_or_page(task)
    update_ownership_and_backoff(result)
```

复杂度（每个调度 tick）：图更新 `O(|S|·k²)`（`k` 为单事务访问集大小），ParMETIS 调用近似 `O(|V| log |V|)`，迁移调度 `O(|candidates|)`，迁移执行 `O(B)`。

> **TODO(formal)**: 给出 `O` 收敛到 `A` 的时间界（在 I3 成立时）；以及 churn 门控下整体系统的稳定性论证。

## 4. 实现

### 4.1 原型系统

基于 WookongDB MP/Hybrid_Cloud_MP（开源原型）实现。系统包含 storage、remote metadata、compute 三层。启动顺序：

1. service host 启动 `storage_pool` 与 `remote_node`。
2. 4 个 compute host 启动 `compute_server interactive smallbank_aff <node_id>`，端口 `9115 + node_id`。
3. 本地 MP-Router 启动 `run --workload smallbank --system-mode <mode>`，通过 LOOKUP 初始化 key→page/node 映射后发起 SmallBank 事务。

### 4.2 指标采集

MP-Router 侧：总事务数、elapsed、throughput、exec latency、fetch latency、route distribution、page operations、page id changes、cache fusion。

Hybrid affinity 侧：local/remote/storage fetch ratio、per-node fetch ratio、edgecut、total_edge_weight、migration planned/done/failed/backlog、assignment access ratios。

由于 MP-Router 跑完事务后可能进入 0 TPS 循环，实验结束后用 MP-Router elapsed window 截断 affinity timeseries 得到 `summary_recovered_window.txt`，避免 post-run 后台线程继续写 CSV 污染统计。

## 5. 实验设置

### 5.1 集群拓扑与启动命令

```text
compute hosts: 10.10.2.31, 10.10.2.32, 10.10.2.33, 10.10.2.34
service host:  10.10.2.38
workload:      smallbank_aff
accounts:      500,000
hot accounts:  100,000
MP-Router workers: 32
WAL:           disabled
data loading:  random_generate
```

所有 MP-Router 多节点实验统一使用 `tests/scripts/multinode_mprouter_smoke.py`。主要复测命令：

```bash
python3 tests/scripts/multinode_mprouter_smoke.py \
  --mprouter-system-mode 13 --worker-threads 32 --try-count 760000 \
  --batch-size 1000 --mprouter-affinity-txn-ratio 1.0 \
  --random-generate --disable-wal \
  --edge-decay-factor 0.90 --max-changed-vertices-ratio 0.55 \
  --repart-itr 1000 --ubvec 1.20 --timeout 5400 --force-clean \
  --result-dir result/multinode_mprouter_mode13_29k_retest_aligned
```

### 5.2 参数对齐原则

为避免错误归因，每轮复测至少记录：mode、worker threads、try count、batch size、affinity transaction ratio、WAL、random_generate、edge decay、max changed vertices ratio、repart_itr、ubvec、migration batch、migration tick、partition cycle、MP-Router binary 来源、git status。§6.5 给出一次失败复测的实证。

### 5.3 评价指标

- **吞吐**：MP-Router total TPS 与 warmup 后 TPS。
- **本地性**：cluster local、last300 local、per-node local。
- **图质量**：edgecut、total_edge_weight、weighted_cut_ratio。
- **迁移效率**：planned、done、failed、backlog。
- **路由均衡**：每节点 routed transaction ratio。
- **ownership churn**：page id changes、cache fusion ratio。

## 6. 实验评估

### 6.0 评估范围与局限

**本评估目前能说明的**：

1. 流水线端到端可运行（1 小时、9700 万事务）。
2. 同一 mode 下参数对齐可复现。
3. 不同 MP-Router mode 给出非单调 (TPS, local, edgecut) 组合，支持 §1 的 placement × routing 二维论点（§6.4）。
4. 高 cluster local 在路由不均衡时掩盖 per-node 差异（§6.2）。

**本评估目前不能说明的**：

| 缺口 | 影响 |
|---|---|
| 无 affinity-disabled baseline | 无法量化"affinity 机制本身"的收益 |
| 无静态分片 baseline（hash / range） | 无法对比简单方案 |
| 无 Schism / Clay 复现 | 无法对比 prior art |
| 无 ablation（各 gate 单独拆掉） | 无法证明每个机制都必要 |
| 单一 workload（仅 SmallBank） | 无法证明跨 workload 泛化 |
| 单一规模（4 compute） | 无法证明 scaling |
| 无长跑（>1h） | 无法证明稳定性 |
| 无开销分解 | 无法证明 affinity 机制开销 < 收益 |

完整缺口清单与所需实验见 §6.7。**在补齐这些之前，本节的数值仅是原型行为刻画，不是相对最先进方法的优越性证明**。

### 6.1 1 小时参考实验与对齐复测

表 1 比较 2026-05-14 04:33 参考轮和 22:47 参数对齐复测。两轮均 mode 13、32 workers、WAL disabled、random_generate、batch size 1000、affinity transaction ratio 1.0、`repart_itr=1000`。主要差异是对齐复测使用 `edge_decay_factor=0.90` 并保留了完整 recovered window 汇总。

**表 1：mode 13 1 小时级实验结果**

| 指标 | 04:33 参考轮 | 22:47 对齐复测 |
|---|---:|---:|
| Total txns | 97,276,000 | 97,276,000 |
| Elapsed ms | 3,255,366 | 3,184,169 |
| TPS | 29,881.74 | 30,549.89 |
| TPS improvement | — | +2.24% |
| Cluster local | 80.99% | 84.16% |
| Last300 local | 83.92% | 86.18% |
| Edgecut | 41,116 | 29,863 |
| Total edge weight | 1,079,847 | 1,158,338 |
| Weighted cut ratio | 3.81% | 2.58% |
| Cut-ratio reduction | — | 32.29% |
| Migrations planned | 437,406 | 1,104,714 |
| Migrations done | 232,904 | 258,376 |
| Migrations failed | 203,102 | 845,138 |
| Page id changes | 236,554 | 297,196 |

对齐复测达到 30.55k TPS，同时 local ratio 和 weighted cut ratio 均优于参考轮。但 migration failed 大幅增加（约 4 倍）：更好的图割和更高 local 并非免费获得，后台迁移仍存在显著无效尝试。

### 6.2 本地性与负载均衡：cluster local 高 ≠ 均衡

对齐复测 per-node：

**表 2：对齐复测的 per-node locality 与路由分布**

| 节点 | Routed txn ratio | Local ratio |
|---|---:|---:|
| node0 | 7.89% | 70.53% |
| node1 | 18.40% | 79.32% |
| node2 | 30.64% | 84.19% |
| node3 | 43.07% | 88.33% |

仅看 cluster local = 84.16% 容易得出"已高度本地化"的结论。但 node3 承担的流量是 node0 的 5.46 倍，高流量节点的高 local 在加权平均中占主导，掩盖了 node0 只有 70.53% 的事实。

本文因此把 locality 分为两个层次：

- **Cluster locality**：全局流量加权后的本地访问比例。
- **Per-node locality + route balance**：是否均衡，低流量节点是否被平均值掩盖。

### 6.3 与 mode 24 的 1 小时运行对比

> **TODO(scope)**: 这个对比 edge_decay_factor 不同（mode 24 用 0.77，mode 13 用 0.90），严格说不是 ablation。要么补一组同 decay 的 mode 24 实验，要么把本节降级为"非控制变量观察"并明确标注。

**表 3：mode 13 与 mode 24 的 1 小时级观测**

| 指标 | mode 24 | mode 13 对齐复测 |
|---|---:|---:|
| Total txns | 92,156,000 | 97,276,000 |
| TPS | 26,661.92 | 30,549.89 |
| Cluster local | 73.89% | 84.16% |
| Page id changes | 241,774 | 297,196 |
| Migrations planned | 482,914 | 1,104,714 |
| Migrations done | 241,597 | 258,376 |
| Migrations failed | 239,917 | 845,138 |

mode 13 在 TPS 和 cluster local 上领先，但伴随更高 planned/failed migrations 和更多 page id changes。

### 6.4 短时 mode 对比：edgecut 不预测 TPS（核心实证）

这是支撑 §1 二维视角论点的核心实证。

**表 4：短时 32-worker mode 对比**

| Mode | TPS | Cluster local | Edgecut | Page id changes |
|---|---:|---:|---:|---:|
| 13 | 25,516.55 | 71.95% | 24,373 | 254 |
| 23 | 18,781.43 | 56.99% | 6,557 | 460 |
| 24 | 26,045.94 | 72.11% | 13,212 | 170 |

mode 23 的 edgecut 最低（6,557），但 cluster local（56.99%）和 TPS（18,781）也最差。edgecut 与 TPS 在此处明显非单调：mode 24 edgecut 是 mode 23 的 2 倍，TPS 反而高出 39%。这表明在 routing 层差异显著时，placement 层的图割收益无法主导吞吐。

> **TODO(experiment)**: 短时实验持续时间过短（page id changes 量级仅 100s）。需要补充：(a) 每个 mode 跑满 1 小时以排除"短时未收敛"假说；(b) 同 mode 下系统性扫描 ubvec/repart_itr 以分离 placement 与 routing 贡献；(c) 在固定 placement 下变化 routing mode 的因果实验，直接验证 placement × routing 解耦。

### 6.5 参数未对齐复测：参数对齐的必要性实证

非对齐复测的结果：

```text
TPS                = 27,813.25
cluster local      = 83.51%
last300 local      = 85.30%
weighted_cut_ratio = 3.42%
```

cluster local 看似不差，但参数未对齐：

- `batch_size=200`（参考轮 1000）。
- `mprouter_affinity_txn_ratio=0.5`（参考轮 1.0）。
- `repart_itr=5000`（参考轮 1000）。

cluster local 高并不自动推导出 TPS 高：路由供给节奏、事务类型比例、分区迁移倾向都会改变运行时行为。论文实验必须以 `experiment_args.json` 为准做参数核对。

### 6.6 Cache fusion 与 page id changes

对齐复测后段 cache fusion ratio 下降到约 0.05%-0.10%，说明后段 ownership churn 已缓和。但 page id changes 达 297,196，高于参考轮 236,554：即使最终 cache fusion ratio 较低，迁移过程仍产生可观 page ownership 变化。

### 6.7 投稿前必须补齐的实验

下表是把本稿升到 SIGMOD/PVLDB/NDBC 投稿级所必需的实验矩阵。每行都是当前缺失的。

**表 5：缺失实验矩阵**

| # | 实验 | 论证什么 | 优先级 | 状态 |
|---|---|---|---|---|
| E1 | TPS / locality with affinity disabled（同 workload、同集群） | affinity 机制本身的净收益（无此则贡献 4 无意义） | P0 | TODO |
| E2 | 静态 hash 分片 baseline | 简单方案的下界 | P0 | TODO |
| E3 | 静态 range 分片 baseline | 范围分片下界 | P1 | TODO |
| E4 | Schism[1] 复现（offline workload-driven） | 对比经典 workload-driven 方法 | P0 | TODO |
| E5 | Clay[5] 复现（adaptive） | 对比 SOTA 自适应分区 | P0 | TODO |
| E6 | E-Store[4] 复现（fine-grained elastic） | 对比 hot tuple 细粒度方案 | P1 | TODO |
| E7 | Ablation: 关闭 churn gate | 证明 churn 门控必要 | P0 | TODO |
| E8 | Ablation: 关闭 failure backoff | 证明 backoff 必要 | P0 | TODO |
| E9 | Ablation: 关闭 partial accept | 证明部分接受必要 | P0 | TODO |
| E10 | Ablation: 关闭 AssignmentTable（直接迁移） | 证明 placement / 物理解耦必要 | P0 | TODO |
| E11 | YCSB at θ ∈ {0.5, 0.8, 0.99}、不同 read/write 比例 | 跨 workload 泛化 | P0 | TODO |
| E12 | TPCC | 跨 schema 复杂度泛化 | P1 | TODO |
| E13 | Scaling 2 / 4 / 8 / 16 compute nodes | 扩展性 | P0 | TODO |
| E14 | 长跑 4h、12h | 稳定性（避免"短时巧合"） | P1 | TODO |
| E15 | 开销分解（采样、建图、ParMETIS、迁移 CPU/RAM） | 证明 affinity 开销 < 收益 | P1 | TODO |
| E16 | 路由偏斜消融（固定 placement，扫不同 routing mode） | 直接验证 placement × routing 解耦 | P0 | TODO |
| E17 | Assignment access ratio 四个指标的完整 timeseries | 诊断 §3.2 中 best/parmetis/assigned/current 之间 gap | P1 | TODO |
| E18 | Motivating no-affinity 微基准（§1 figure 用） | 引言 hook | P0 | TODO |

**保守估计**：完成所有 P0 实验需 ≥ 4-6 周（含 baseline 复现的工程成本）。Schism / Clay 复现工作量最大。

> **TODO(scope)**: 是否真的要完整复现 Schism / Clay？SIGMOD/PVLDB 通常要求至少有 fair comparison；NDBC 可能放宽。需要先定投稿目标会议再确定 E4/E5 是否做。

## 7. 讨论

### 7.1 指标之间的关系：为什么 edgecut、local 和 TPS 不线性相关

§6.4 的实证表明 edgecut、local、TPS 不存在简单线性关系。原因：

1. **edgecut 是图指标**：衡量共同访问强度是否跨分区，不反映迁移是否完成。
2. **local 是运行时指标**：取决于事务实际路由、page ownership、MP-Router page map、fetch 路径。
3. **TPS 是系统指标**：还包含路由线程、队列、锁等待、migration worker、cache fusion、日志路径。
4. **cluster local 是加权平均**：高流量节点主导集群平均值，掩盖低流量节点（§6.2）。

建议分析顺序：

```text
route distribution
  → per-node local
  → weighted_cut_ratio
  → migration done / failed / backlog
  → page id changes / cache fusion
  → TPS / latency
```

每一层先排除自己的解释，再走到下一层。

### 7.2 当前方法的瓶颈

基于 §6 现有数据：

- **负载不均衡**：node3 / node0 路由比 5.46:1，对长期稳定性不利。
- **per-node local 不均衡**：node0 仅 70.53%，与 node3 88.33% 相差 ~18 个百分点。
- **迁移失败过高**：对齐复测 planned 1.1M、done 258k、failed 845k；失败远高于成功，预算消耗在热点或 stale 任务。

> **TODO(scope)**: 这三个瓶颈在 §6.7 完成之前都只是"我们这套系统的瓶颈"，而非"this class of methods 的瓶颈"。补齐 baseline 后需重新判断哪些是本方法独有、哪些是 shared-storage multi-primary 通病。

### 7.3 可行优化方向

1. **收益门控迁移**：迁移前估算 expected_gain，只接受高于阈值的任务。
2. **部分接受 assignment**：ParMETIS 结果变化过大时只接受稳定且高收益的顶点变更。
3. **负载感知路由反馈**：在不修改 MP-Router 源码的约束下，通过 Hybrid 侧指标暴露或配置策略，使过度偏斜的节点不继续吸收流量。
4. **失败迁移抑制**：连续失败 tuple/page 做指数 backoff，并统计失败原因分类（热点锁冲突 / page 状态变化 / stale plan）。

## 8. 相关工作

> **TODO(scope)**: 本节当前的"vs 本文"对比是基于本文 design 的 talking point，**不是基于实证对比**。在 E4/E5 完成前，"我们更好"的暗示不能成立。本节应改写为"我们关注的问题与 X 不同"而非"我们比 X 更优"。当前文字已朝这个方向调整，但 E4/E5 之后需进一步重写。

### 8.1 工作负载感知分区

Shared-nothing OLTP 系统长期使用工作负载感知分区减少分布式事务。Schism[1] 把事务访问关系建图并用最小割划分；Horticulture[2] 通过偏斜感知规划布局；E-Store[4] 关注热点 tuple 的细粒度弹性迁移；Clay[5] 在多种 schema 上做细粒度自适应分区。**这些工作共同假设 shard 拥有数据的强归属：分片决定即物理拥有**。本文目标场景是共享存储多主，数据并非只能由单一节点持久拥有；page ownership、cache fusion 和路由器 page map 把"图划分质量"与"性能收益"隔开一层异步反馈链，因此需要 AssignmentTable 这样的解耦层。

> **TODO(citation)**: 补充 Squall[SIGMOD'15] 在线迁移、Rocksteady[SIGMOD'18] 内存数据库快速迁移的引用与对比。

### 8.2 在线图划分与 ParMETIS

ParMETIS[6] 支持并行图划分与 adaptive repartitioning。直接把 ParMETIS 输出当迁移指令的缺陷：(i) 无视迁移成本；(ii) 在热点 tuple 上振荡；(iii) 与前台事务锁冲突。本文把 ParMETIS 作为候选 assignment 生成器而非迁移命令，通过 churn 门控、失败 backoff、批次限制把"图上最优"翻译为"运行时可承受"。

### 8.3 共享存储与缓存一致性

Oracle RAC Cache Fusion[7] 与 Aurora、Socrates、PolarDB 等共享存储系统通过缓存一致性、page ownership 或 redo/undo 协议协调多节点访问。**这类系统通常假设访问模式难以预测，依赖 ownership 协议被动应对**。本文在该层之上加入主动的访问感知层：把 cache fusion ratio 和 page id changes 作为运行时 churn 指标，用 affinity graph 主动减少跨节点共访问，让 ownership 协议处理残余。

> **TODO(citation)**: Aurora[SIGMOD'17]、Socrates[SIGMOD'19]、PolarDB[VLDB'18] 具体 paper 引用。

### 8.4 自适应路由与确定性执行

Calvin[3] 通过事务确定性执行简化分布式事务，把分配集中在 sequencer 层；H-Store / VoltDB 系列依赖单线程分区执行最小化协调。**这两类系统假定有清晰的 partition primary**。本文不修改 MP-Router，把它当固定外部 driver，研究"路由策略不变时 placement 能拿到多少 locality"。§6.4 显示路由策略本身显著影响 locality；未来工作需将 placement 与 routing 联合优化，而非只优化其中一端。

## 9. 结论

本文针对共享存储多主数据库的在线亲和性问题，提出 **placement × routing 二维视角**，并构造以 **AssignmentTable** 为核心的解耦流水线：事务采样 → 在线亲和图 → ParMETIS 候选分区 → AssignmentTable → 受控后台迁移 → 路由反馈。AssignmentTable 把分区决策与物理迁移解耦，受控迁移把图收益逐步翻译为运行时 locality。

在 WookongDB MP 4 compute + 1 service SmallBank affinity workload 1 小时级运行中，对齐复测达到 30,549.89 TPS、cluster local 84.16%、weighted cut ratio 2.58%；相对此前同配置参考轮，吞吐 +2.24%，cluster local +3.17 pp，weighted cut ratio 相对降低 32.29%。短时 mode 对比（§6.4）经验上支持二维视角：最低 edgecut 的模式同时给出最低 TPS。

**当前评估有显著缺口**（§6.0 / §6.7）：缺 affinity-disabled、静态分片、Schism / Clay baseline；缺 ablation、多 workload、scaling、长跑实验。本稿在补齐这些实验前不构成对 prior art 的优越性声明。我们将这些缺口作为下一阶段的明确工作项推进。

## 参考文献草案

> **TODO(citation)**: 以下每条都需要确认期刊/会议、卷期/页码、DOI。当前条目仅为占位。

[1] Carlo Curino, Evan P. C. Jones, Yang Zhang, Sam Madden. **Schism: a Workload-Driven Approach to Database Replication and Partitioning**. PVLDB, 2010.

[2] Andrew Pavlo, Carlo Curino, Stanley B. Zdonik. **Skew-Aware Automatic Database Partitioning in Shared-Nothing, Parallel OLTP Systems**. SIGMOD, 2012.

[3] Alexander Thomson, Thaddeus Diamond, Shu-Chun Weng, Kun Ren, Philip Shao, Daniel J. Abadi. **Calvin: Fast Distributed Transactions for Partitioned Database Systems**. SIGMOD, 2012.

[4] Rebecca Taft, et al. **E-Store: Fine-Grained Elastic Partitioning for Distributed Transaction Processing Systems**. PVLDB, 2014.

[5] Marco Serafini, et al. **Clay: Fine-Grained Adaptive Partitioning for General Database Schemas**. PVLDB, 2016.

[6] George Karypis, Vipin Kumar. **ParMETIS: Parallel Graph Partitioning and Sparse Matrix Ordering Library**.

[7] Oracle. **Cache Fusion and the Global Cache Service**. Oracle RAC documentation.

> **TODO(citation)**: 补充 Aurora（Verbitski et al. SIGMOD'17）、Socrates（Antonopoulos et al. SIGMOD'19）、PolarDB（Cao et al. VLDB'18）、Squall（Elmore et al. SIGMOD'15）、Rocksteady（Kulkarni et al. SIGMOD'18）。

## 附录 A：主要实验路径

```text
最佳对齐复测:
result/multinode_mprouter_mode13_29k_retest_aligned/20260514_224737/default/summary_recovered_window.txt

1 小时参考轮:
result/multinode_mprouter_mode13_migpriority_1h/20260514_043322/default/summary_recovered_window.txt

mode 24 一小时:
result/multinode_mprouter_mode24_1h/20260513_163330/default/summary.txt

短时 mode 对比:
result/multinode_mprouter_mode_compare_32w/mode13/20260513_162157/default/summary.txt
result/multinode_mprouter_mode_compare_32w/mode23/20260513_162405/default/summary.txt
result/multinode_mprouter_mode_compare_32w/mode24/20260513_162612/default/summary.txt
```

## 附录 B：术语表

- **Affinity graph**：根据事务共同访问关系构造的带权图。
- **Edgecut**：被分区切开的边权总和。
- **Total edge weight**：图中所有边权总和。
- **Weighted cut ratio**：`edgecut / total_edge_weight`。
- **AssignmentTable**：记录 tuple 建议归属节点的中间层（§3.2）。
- **placement × routing**：本文提出的二维视角；placement 由 AssignmentTable + 迁移决定，routing 由 MP-Router 决定。
- **Migration planned / done / failed**：计划 / 成功 / 失败的迁移任务数。
- **Page id changes**：MP-Router 感知到的 page id 或 ownership 变化次数。
- **Cache fusion ratio**：跨节点 page ownership 变化 / page operations。
- **Cluster local**：全局流量加权后的本地 fetch 比例。
- **Per-node local**：单个 compute 节点的本地 fetch 比例。

## 附录 C：TODO 索引

为便于追踪，把全文 TODO 按类型集中：

**TODO(experiment)** —— 需要新跑实验：§6.7 表 5（E1-E18 共 18 项）；§6.4 的延长 / 因果实验。

**TODO(measurement)** —— 需要测量并填回正文：§1 motivating measurement；§2.2 workload 分布刻画；§3.2 四个 assignment access ratio 的 timeseries 报告。

**TODO(formal)** —— 需要形式化论证：§3.1 目标函数与 min-cut 等价性；§3.2 不变量 I3 的证明；§3.3 子采样下 `weighted_cut_ratio` 无偏性；§3.5 `expected_gain` 形式化；§3.7 收敛时间界。

**TODO(citation)** —— 需要补充或核实引用：§2.1 共享存储系统；§8.1 Squall / Rocksteady；§8.3 Aurora / Socrates / PolarDB；参考文献部分页码与 DOI。

**TODO(scope)** —— 需要决策：贡献 4 改写时机（§1）；`partially_accept` 算法描述位置（§3.4）；§6.3 是否补同 decay 对比或降级标注；§6.7 是否完整复现 Schism / Clay（取决于投稿目标）；§7.2 瓶颈表述（本方法独有 vs 通病）；§8 整体口径在 E4/E5 之后是否重写。
