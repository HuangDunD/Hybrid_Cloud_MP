# MP-Router Affinity 实验方法与实践原理

本文记录 `Hybrid_Cloud_MP` 中使用 MP-Router 驱动多节点亲和性实验的方法、指标解释和实践原则。目的是把已经验证过的实验流程固定下来，避免后续因参数不一致、采样窗口不一致或指标误读导致结论漂移。

## 1. 实验目标与约束

### 1.1 实验目标

当前 mode 13 + affinity 实验关注三个问题：

1. 4 compute + 1 service 的多节点环境下，MP-Router mode 13 能否稳定达到 29k 以上 TPS。
2. Affinity 机制能否提升本地访问比例、降低跨节点访问比例。
3. 图划分质量、迁移压力、路由偏斜之间的关系是什么。

### 1.2 实验约束

- 测试脚本固定使用 `tests/scripts/multinode_mprouter_smoke.py`，不混用其他 harness。
- 不修改 MP-Router 相关代码；MP-Router 仅作为外部 workload/router driver 使用。
- 重点观察 mode 13 下的 SmallBank affinity workload。
- 复测必须保证全部关键参数对齐（详见 §5.2），不能只对齐 `system-mode` 和 worker 数。

### 1.3 核心判断原则

`cluster local`、`edgecut`、`TPS` 三者不是线性正相关。判断实验质量必须同时看：route distribution、per-node local、weighted_cut_ratio、migration failed、page_id_changes、cache_fusion。详见 §4.7。

## 2. 系统链路

整体拓扑：

```text
service host
  └─ storage_pool
  └─ remote_node
4 × compute host
  └─ compute_server interactive smallbank_aff <node_id>   # 端口 9115 + node_id
1 × MP-Router host
  └─ run --workload smallbank --system-mode 13 ...
```

流程：

1. `multinode_mprouter_smoke.py` 部署并启动 Hybrid 集群。
2. service host 启动 `storage_pool` 和 `remote_node`。
3. 4 个 compute host 启动 interactive compute server，对外暴露 `9115 + node_id` 端口，供 MP-Router 通过 LOOKUP / SmallBank 协议访问。
4. MP-Router binary 先通过 Hybrid LOOKUP 初始化 key → page/node 映射，再按 mode 13 路由策略推送事务。
5. Compute 节点在事务执行过程中持续产生 affinity 采样、图更新、ParMETIS 分区结果和迁移任务。

MP-Router 启动示例：

```bash
/root/mingtai/MP-Router/build/serve/test/run \
  --workload smallbank \
  --system-mode 13 \
  --access-pattern 1 \
  --zipfian-theta 0.8 \
  --account-count 500000 \
  --worker-threads 32 \
  --try-count 760000 \
  --affinity-txn-ratio 1.0 \
  --batch-size 1000 \
  --num-bucket 4
```

## 3. 方法原理：Affinity 闭环流水线

Affinity 机制是一个五段闭环流水线：

```text
事务采样 → 在线建图 → ParMETIS 分区 → AssignmentTable 发布
       ↑                                       ↓
       └──── 路由 / page map 反馈 ←── 后台迁移
```

### 3.1 事务采样

事务执行后，系统记录该事务访问过的 tuple/page 信息。一个事务访问多个 tuple 时，这些 tuple 之间形成共同访问关系。采样本身不直接触发迁移，而是把运行时访问关系转化为 affinity graph 的输入。

### 3.2 在线建图

Affinity aggregator 周期性消费采样，构造一张共访问图：

- tuple 是顶点。
- 同一事务中共同访问的 tuple 对构成边。
- 边权代表共同访问强度。

核心字段：

- `n_vertices`：图顶点数。
- `n_edges`：图边数。
- `total_edge_weight`：所有边权总和。
- `edgecut`：被当前分配切开的边权总和。
- `weighted_cut_ratio = edgecut / total_edge_weight`。

`edgecut` 单位是被切开的边权（不是边条数），反映多少共同访问强度跨了节点。单独看 `edgecut` 不够，必须结合 `total_edge_weight` 看 `weighted_cut_ratio`（详见 §4.3）。

### 3.3 图划分与 AssignmentTable

系统周期性调用 ParMETIS sidecar 对当前 affinity graph 做重分区。分区结果不会直接搬动 tuple，而是先发布到 **AssignmentTable**：表示某个 tuple 更适合由哪个 compute 节点拥有。

AssignmentTable 是"应该在哪里"的状态，不是"已经在哪里"的状态。前台事务和后台迁移都读取这个表，但它的发布不等价于物理迁移已经完成。

不同层次的 assignment 指标分别衡量这种"应在 vs 已在"的差距：

- `assignment_best_access_ratio`：理论上最优选择能覆盖多少访问。
- `assignment_current_access_ratio`：当前已知布局覆盖多少访问。
- `assignment_parmetis_access_ratio`：直接采用 ParMETIS 分区结果覆盖多少访问。
- `assignment_assigned_access_ratio`：当前被发布或接受的 assignment 覆盖多少访问。

### 3.4 后台迁移

Migration planner 扫描 AssignmentTable，找到目标节点与当前节点不一致的 tuple，生成迁移任务；Migration worker 在后台执行迁移。

迁移并非越多越好，它会带来：

- 页面锁竞争。
- ownership transfer。
- page id changes。
- MP-Router 侧 page map 更新。
- 对前台事务的短期干扰。

因此当前方法用多种门控降低迁移抖动：

- 限制每轮迁移批次。
- 按源 page 和目标节点合并迁移任务。
- 限制高 churn 分区结果对 assignment 的冲击。
- 对迁移失败的热点 tuple 做 backoff，避免失败任务反复占满迁移预算。

### 3.5 从迁移到路由的反馈

迁移成功后，tuple/page 归属逐步向 assignment 靠拢。MP-Router 通过 LOOKUP 和运行中的 page id 变化感知 page ownership 变化。当事务访问的 tuple 集中在同一目标节点：

- `from_local_ratio` 上升。
- `from_remote_ratio` 下降。
- cache fusion 比例下降。
- fetch-to-complete latency 下降。
- TPS 上升。

但这个链路是异步的、有延迟的：

- 图划分好不代表迁移已完成。
- 迁移完成不代表 TPS 一定升。
- local 升高也可能来自路由流量偏斜，而非真实改善。

因此分析必须按流水线逐层定位，不能跳过中间环节直接由 edgecut 推到 TPS。具体定位顺序见 §4.7。

## 4. 指标体系

### 4.1 TPS

主指标来自 MP-Router `result.txt`：

- `mprouter_throughput_tps`
- `mprouter_throughput_after_warmup_tps`
- `mprouter_total_txns`
- `mprouter_elapsed_ms`

不要用实时 `[Routed TPS]` 或 `[Exec TPS]` 单点下结论：实时窗口波动较大。最终结论以总事务数和总 elapsed time 为准。

### 4.2 local / remote / storage ratio

Affinity timeseries 中：

- `from_local_ratio`：fetch 命中本地计算节点的比例。
- `from_remote_ratio`：需要远端计算节点的比例。
- `from_storage_ratio`：需要到 storage 的比例。

当前实验 storage ratio 很低，主要矛盾是 local 与 remote 之间的转换。

**cluster local 高不代表每个节点都好**。当路由流量偏向某个节点，cluster local 会被高流量节点主导，掩盖低流量节点的差表现。per-node 拆解和路由分布的对照见 §6.2.1 / §4.6。

### 4.3 edgecut 与 weighted_cut_ratio

`edgecut` 是跨分区边权总和，单位是权重，不是边数。更推荐看：

```text
weighted_cut_ratio = edgecut / total_edge_weight
```

原因：

- `edgecut` 会随图规模和采样量变化。
- `total_edge_weight` 变大时，同样的 edgecut 代表更小的相对割。
- 不同轮实验只有在 total_edge_weight 接近时，edgecut 才直接可比。

三轮实验对照（数据见 §6.1）：参数对齐复测的 edgecut 高于未对齐复测，但因为 total_edge_weight 更高，最终 weighted_cut_ratio 反而更低，图质量更好。

### 4.4 migration 指标

关键字段：

- `affinity_migrations_planned_window`
- `affinity_migrations_done_window`
- `affinity_migrations_failed_window`
- `affinity_migration_backlog_window`

含义：

- `planned` 高 → assignment 与当前布局差异大，系统想移动的 tuple 多。
- `done` → 实际成功迁移的 tuple 数。
- `failed` → 多数来自热点 tuple 被前台事务锁住、source page 状态变化、stale plan。
- `backlog = planned - done`，表示尚未完成的迁移压力。

`failed` 过高说明迁移预算被热点失败任务消耗。这种情况下继续加大迁移 batch 不一定有效，可能放大前台干扰；应该检查失败 backoff、迁移优先级、每源页/目标节点限额、stale plan 比例。

### 4.5 page id changes 与 cache fusion

MP-Router 日志：

- `Page ID changes`：MP-Router 感知到 page id / ownership 变化的次数。
- `Cache Fusions`：模拟或记录的跨节点 ownership/page 变化次数。
- `Cache Fusions ratio`：相对 page operations 的比例。

这类指标反映路由层看到的页面归属 churn，**不是越高越好**：

- 短期迁移会带来 page id changes。
- 长期 affinity 收敛时，cache fusion ratio 应下降。

参数对齐复测后段 cache fusion ratio 下降到约 0.05%-0.10%，说明后段 page ownership churn 已经缓和。

### 4.6 路由分布

Route distribution 解释为什么 cluster local 与 per-node local 不一致。三轮实验的 routed txn ratio：

| 实验 | node0 | node1 | node2 | node3 |
|---|---:|---:|---:|---:|
| 04:33 参考轮 | 40.54% | 19.67% | 19.73% | 20.06% |
| 参数未对齐复测 | 7.38% | 18.45% | 30.47% | 43.70% |
| 参数对齐复测 | 7.89% | 18.40% | 30.64% | 43.07% |

mode 13 在后两轮明显偏向 node3。这种偏斜可以提高 cluster local，但会让 node3 队列长期满、node0 local 偏低。后续优化不能只追求 cluster local，还要看负载均衡（详见 §6.2.2）。

### 4.7 指标之间的关系：为什么 local、edgecut、TPS 不线性正相关

常见误区：edgecut 越低 → local 越高 → TPS 越高。实际不成立，原因有四类：

1. **图指标 ≠ 运行时指标**。
   图划分产出的是"应该在哪里"，local 取决于 tuple/page 是否已迁移完成、MP-Router 是否已感知映射、事务是否真的打到目标节点。

2. **edgecut 是图上权重，TPS 是系统路径开销**。
   TPS 还受路由线程、队列、热点节点、迁移失败、page id changes、cache fusion、日志等待、锁等待影响。

3. **cluster local 被流量分布加权**。
   若 node3 承担 43% 流量且 local 很高，cluster local 自然高；但 node0 local 可能很低。cluster local 高不一定均衡。

4. **迁移本身制造短期 churn**。
   更激进的迁移可能降低 future edgecut，但短期会增加 page id changes、cache fusion、锁竞争，TPS 反而下降。

因此分析顺序应该是：

```text
route distribution
  → per-node local
  → weighted_cut_ratio
  → migration done / failed / backlog
  → page id changes / cache fusion
  → TPS / latency
```

每一层先排除自己的解释，再走到下一层。

## 5. 复测方法

### 5.1 推荐复测命令

复测 2026-05-14 04:33 那组 29k TPS / 80% local 实验的对齐命令：

```bash
python3 tests/scripts/multinode_mprouter_smoke.py \
  --mprouter-system-mode 13 \
  --worker-threads 32 \
  --try-count 760000 \
  --batch-size 1000 \
  --mprouter-affinity-txn-ratio 1.0 \
  --random-generate \
  --disable-wal \
  --edge-decay-factor 0.90 \
  --max-changed-vertices-ratio 0.55 \
  --repart-itr 1000 \
  --ubvec 1.20 \
  --timeout 5400 \
  --force-clean \
  --result-dir result/multinode_mprouter_mode13_29k_retest_aligned
```

关键参数的设计意图：

- `--worker-threads 32`：MP-Router worker 数。不要回到 192 workers；当前结论基于 32 workers。
- `--batch-size 1000`：参考轮使用的 batch size。错误复测使用 200，导致吞吐和调度行为不可比。
- `--mprouter-affinity-txn-ratio 1.0`：参考轮使用 100% affinity transaction ratio。错误复测使用 0.5，改变了访问图和路由压力。
- `--repart-itr 1000`：ParMETIS AdaptiveRepart `itr`。错误复测使用 5000，改变了迁移和分区行为。
- `--random-generate`：storage 侧随机生成数据，避免加载路径成为变量。
- `--disable-wal`：关闭 WAL，排除日志刷盘干扰。
- `--edge-decay-factor 0.90`：让 affinity graph 保留较长历史，降低短窗口噪声。
- `--max-changed-vertices-ratio 0.55`：限制高 churn 分区结果对 assignment 的冲击。

### 5.2 关键参数对齐清单

复测前必须保存并对照 `experiment_args.json`。至少确认以下字段一致：

```text
mprouter_system_mode
worker_threads
try_count
batch_size
mprouter_affinity_txn_ratio
disable_wal
random_generate
edge_decay_factor
max_changed_vertices_ratio
repart_itr
ubvec
migration_batch
migration_tick_ms
partition_cycle_ms
rebuild
```

上一轮 27.8k 的错误复测说明：**只对齐 mode 和 worker 数是不够的**。`batch_size`、`affinity_txn_ratio`、`repart_itr` 这些参数足以改变实验结论。

### 5.3 复测实践原则

- **固定脚本**：所有 MP-Router 多节点实验使用 `tests/scripts/multinode_mprouter_smoke.py`。不混用其他 harness，否则启动方式、采集逻辑、配置注入和结果目录都会不同。
- **不用单点实时 TPS 下结论**：实时 `[Routed TPS]`、`[Exec TPS]` 只能观察阶段性状态。最终结论必须来自 `mprouter_throughput_tps`、`mprouter_elapsed_ms`、`mprouter_total_txns` 和 window 截断后的 affinity 指标。
- **一次只改一个变量**：调参时依次只改一个 affinity 参数（如 `repart_itr`）、一个迁移参数（如 `migration_batch`）、一个路由参数（如 `batch_size`），不要同时改 batch、affinity ratio、repart itr 和迁移策略，否则无法归因。
- **不把 MP-Router 源码作为调参对象**：需要优化时优先在 Hybrid affinity、配置参数和实验方法上做。MP-Router binary 作为固定外部 driver。

### 5.4 Window summary 截断

MP-Router 跑满事务后可能进入 0 TPS 循环不自然退出。处理方式：

1. 确认日志或 `result.txt` 已经出现：

   ```text
   Total transactions executed
   Elapsed time
   Throughput
   Throughput (after warmup)
   ```

2. 再终止已完成的 MP-Router 进程组，让脚本继续收集 compute timeseries。
3. 对 affinity timeseries 按 MP-Router `elapsed_ms` 截断，生成 `summary_recovered_window.txt`。

这样可以避免 post-run 后台 affinity 线程继续写 CSV，污染 final local、edgecut、migration 统计。

### 5.5 分析模板

每轮实验结束后建议按此模板记录：

```text
result_dir=
command=
git_status=
mprouter_binary_source=
total_txns=
elapsed_ms=
tps=
after_warmup_tps=
cluster_local=
last300_local=
per_node_local=
route_distribution=
edgecut=
total_edge_weight=
weighted_cut_ratio=
migrations_planned=
migrations_done=
migrations_failed=
migration_backlog=
page_id_changes=
cache_fusion_observation=
conclusion=
```

`git_status` 和 `mprouter_binary_source` 很重要：即使不改 MP-Router 代码，`--rebuild` 或 dirty worktree 也可能改变复测环境。

## 6. 实验结果

### 6.1 三轮实验对照

| 实验 | TPS | cluster local | last300 local | edgecut | total_edge_weight | weighted_cut_ratio | mig done | mig failed | page_id_changes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 04:33 参考轮 | 29881.74 | 80.99% | 83.92% | 41116 | 1079847 | 3.81% | 232904 | 203102 | 236554 |
| 参数未对齐复测 | 27813.25 | 83.51% | 85.30% | 26426 | 771840 | 3.42% | — | — | — |
| 参数对齐复测 | 30549.89 | 84.16% | 86.18% | 29863 | 1158338 | 2.58% | 258376 | 845138 | 297196 |

结果路径：

- 参考轮：`result/multinode_mprouter_mode13_migpriority_1h/20260514_043322/default/summary_recovered_window.txt`
- 未对齐复测：`result/multinode_mprouter_mode13_29k_retest/20260514_212959/default/summary_recovered_window.txt`
- 对齐复测：`result/multinode_mprouter_mode13_29k_retest_aligned/20260514_224737/default/summary_recovered_window.txt`

参数未对齐复测的差异：

- `batch_size=200`（参考轮 1000）。
- `mprouter_affinity_txn_ratio=0.5`（参考轮 1.0）。
- `repart_itr=5000`（参考轮 1000）。

结论：

- 29k TPS 已经复现，并提升到 30.55k。
- cluster local 比参考轮高约 3.17 个百分点。
- weighted_cut_ratio 明显低于参考轮，图质量更好。
- 但 migration failed 大幅上升（约 4 倍），是当前最显眼的待优化点。

### 6.2 已识别的优化方向

#### 6.2.1 per-node local 不均衡

参数对齐复测的 per-node local：

```text
node0 local = 70.53%
node1 local = 79.32%
node2 local = 84.19%
node3 local = 88.33%
```

cluster local 84.16% 掩盖了 node0 的 70.53%。若目标是"每个节点 local 都到 85%"，当前还没达到。

#### 6.2.2 路由流量偏斜

mode 13 把大量流量推到 node3（参数对齐复测 43.07%，参考轮 20.06%；完整三轮对照见 §4.6）。

这种偏斜有助于 cluster local，但会让 node3 队列长期满、降低系统均衡性。

#### 6.2.3 migration failed 过高

参数对齐复测：

```text
migrations planned = 1104714
migrations done    =  258376
migrations failed  =  845138
```

失败数远高于成功数。后续优化方向：

- 热点 tuple 失败 backoff 是否足够。
- planner 是否优先选择更易成功、收益更高的迁移。
- 每源 page / 目标节点限额是否过激进或过保守。
- stale plan 比例是否过高。
- 迁移成功对 local 的边际收益是否值得当前 churn。

#### 6.2.4 page id changes 仍高于参考轮

参考轮 `page_id_changes=236554`，对齐复测 `page_id_changes=297196`。迁移或 ownership 变化更多。虽然 weighted_cut_ratio 更好、TPS 更高，但若继续优化稳定性，应降低无收益 page changes。

## 7. 当前结论

当前方法的核心不是简单"降低 edgecut"，而是建立一个闭环：

```text
事务访问采样
  → 在线亲和图
  → ParMETIS 分区
  → AssignmentTable 发布
  → 受控后台迁移
  → MP-Router 路由与 page map 更新
  → local / remote / cache fusion / TPS 反馈
```

最可靠的复测方式：

1. 固定 `multinode_mprouter_smoke.py`。
2. 固定参考参数（§5.2）。
3. 使用 MP-Router elapsed window 截断 affinity metrics（§5.4）。
4. 同时看 TPS、cluster local、per-node local、weighted_cut_ratio、migration failed、page id changes、route distribution。

基于 2026-05-14 的对齐复测，mode 13 + affinity 方法已能复现并超过 29k TPS，cluster local 达约 84.16%、last300 local 达约 86.18%。但若目标是"每个节点 local 都达到 85% 以上"，当前仍未完成；短板集中在 node0 local 低、路由分布偏向 node3、migration failed 过高（详见 §6.2）。
