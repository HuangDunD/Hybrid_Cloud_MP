# TCP / eRDMA 对照实验（2026-11-05，10000 与 20000 IOPS）

2026-11-06 原开发/存储实例释放后，已重建四机环境并通过 RDMA 短测；新地址、环境修复与验证范围见文末“环境重建”。下面的性能表仍对应原实验环境。

状态：10000 与 20000 IOPS 各 32 次运行全部完成，共 64 次。前半部分保留 10000 IOPS 分析；后半部分为 20000 IOPS 复测和跨档位比较。3800 IOPS 数据不混入统计。

20000 IOPS 复测的主要发现：

- RDMA 在所有模式的执行阶段都节省时间：YCSB Lazy 约 144 μs/attempt，其余 YCSB 模式约 29–36 μs，SmallBank 约 29–37 μs。
- 提高 IOPS 后，SmallBank 吞吐整体有所改善；YCSB 未显示一致改善，单次刷盘时延仍有波动。
- 同一 20000 IOPS 档位下，YCSB Lazy 的 RDMA TPS 增益为 7.6%，普通 2PC 约 0%，Cornus 两版约 2.9%；不能把扩大 IOPS 配额等同于普遍放大 RDMA 收益。
- SmallBank Lazy 每节点日志流的同步 RPC 占用约 97%–98%，每批约一个写事务；即使设备平均 IOPS 尚低于新配额，日志流完成速率仍可能限制吞吐。
- 两次短测尚不能隔离所有云盘时延波动，配对观测和完整阶段表见文档后半部分。


## 环境、版本与对照方法

- 四台 ecs.g9i.2xlarge，Ubuntu 22.04，8 vCPU / 约 32 GiB 内存。
- 三个计算节点管理 IP `.125/.126/.127`，实验 ERI IP `.131/.130/.128`；存储和 remote_node 在管理 IP `.124`、ERI IP `.129`。
- TCP 和 RDMA 均使用 eth1 的同一组 ERI 地址、同版 RDMA-enabled bRPC 1.9.0、同一组项目二进制。RDMA 使用 Compat、GID index 1、max SGE 4、按需建立 QP。
- `ConfigureRpcTransport` 已从 `common/config.cc` 移到 `common/rpc_transport.cc`，三种服务显式调用。Linux 实际编译、链接通过，开发副本与四节点运行副本的部署哈希一致。
- 10000 IOPS 这轮实验的存储云盘档位为 10000，由用户确认；没有通过云控制台 API 独立核实配置。系统采样显示多个 case 的写 IOPS 接近 10000。
- YCSB、SmallBank × Lazy、2PC、Cornus、Cornus-opt × TCP、RDMA × 两次重复，共 32 次。每节点 2 个工作线程，每次 30 秒，θ=0.6、local_ratio=0.5、write_txn_ratio=0.8、NO_WAIT，日志开启，后台日志周期 3 ms、触发阈值 16 条。
- 固定种子打散模式顺序；同一负载/模式的 TCP 与 RDMA 相邻执行，第二次重复反转传输顺序与模式遍历顺序。每次重启进程，复用已有的数据准备流程；测量期间不查询 bRPC 调试连接接口。

原始目录：开发机 `/data/workspace/Hybrid_Cloud_MP/result/transport_study_20261105_iops10000`；本地同名 `result` 目录保存副本。

旧目录 `result/transport_study_20261105` 在 IOPS 调整时仍有运行，已停止并保留 `STOPPED_IOPS_CHANGE.json`。其中部分 case 可能跨越配置变更，因此不作为本表的配对基线。

## 吞吐与阶段时间

TPS 为三个计算节点 Commit TPS 之和。下表是两次运行各自指标的算术均值；执行和提交时间分母为所有完成的事务尝试，包含回滚，单位 ms/attempt，不是只统计成功事务的条件均值。

| 负载 | 模式 | TCP TPS | RDMA TPS | TPS 变化 | 执行 ms：TCP → RDMA | 提交 ms：TCP → RDMA |
|---|---|---|---|---|---|---|
| ycsb | Lazy | 2632 | 2768 | +5.1% | 0.863 → 0.725 | 1.366 → 1.394 |
| ycsb | 2PC | 2080 | 2164 | +4.1% | 0.407 → 0.377 | 2.382 → 2.313 |
| ycsb | Cornus | 2667 | 2700 | +1.2% | 0.403 → 0.369 | 1.774 → 1.785 |
| ycsb | Cornus-opt | 2586 | 2702 | +4.5% | 0.406 → 0.372 | 1.841 → 1.782 |
| smallbank | Lazy | 5580 | 5570 | -0.2% | 0.199 → 0.161 | 0.871 → 0.910 |
| smallbank | 2PC | 4740 | 4851 | +2.3% | 0.132 → 0.101 | 1.128 → 1.130 |
| smallbank | Cornus | 5942 | 6135 | +3.3% | 0.128 → 0.098 | 0.876 → 0.875 |
| smallbank | Cornus-opt | 5929 | 6158 | +3.9% | 0.128 → 0.098 | 0.878 → 0.871 |

![吞吐对照](../result/transport_study_20261105_iops10000/throughput.png)

![执行和提交阶段](../result/transport_study_20261105_iops10000/stage_times.png)

图中的点与误差线展示两次实测及其范围，不是置信区间。两次相邻配对的 TPS 变化分别为：

| 负载 | 模式 | 重复 1 TPS 变化 | 重复 2 TPS 变化 |
|---|---|---|---|
| ycsb | Lazy | +4.98% | +5.31% |
| ycsb | 2PC | +6.50% | +1.69% |
| ycsb | Cornus | +1.01% | +1.48% |
| ycsb | Cornus-opt | +4.03% | +4.99% |
| smallbank | Lazy | -0.07% | -0.28% |
| smallbank | 2PC | +2.29% | +2.36% |
| smallbank | Cornus | +3.58% | +2.93% |
| smallbank | Cornus-opt | +3.56% | +4.19% |

## 可以得出的结论

### 1. RDMA 更明显地改善执行阶段，收益大小取决于执行中的通信次数

YCSB Lazy 的平均所有权转移调用数约为 TCP 5.68、RDMA 5.70 次/attempt；单次平均耗时从 138.4 μs 降到 114.9 μs。执行阶段累计节省约 138 μs/attempt。

YCSB Cornus 的远程 fetch 约 3.24 次/attempt，单次平均耗时从 86.3 μs 降到 78.6 μs，执行阶段节省约 34 μs/attempt。这两类调用承担的工作不同，不能把单次延迟直接当成相同 RPC 的速度比较；它们能解释为什么这组负载下 Lazy 的绝对收益更大。所有权转移计时可能跨阶段，不与 execute 相加。

引入 RDMA 后，YCSB Lazy 与 Cornus 变体处于接近的吞吐范围；这组结果不支持“优化 2PC 必然压过所有权转移”。也不能凭两次短测把几个百分点的差异推广到其他冲突率或并发度。

### 2. 执行提速不保证 TPS 同比例增长，存储等待可能吸收收益

SmallBank Lazy 的执行均值从 0.199 ms 降到 0.161 ms，但提交从 0.871 ms 增到 0.910 ms，TPS 基本不变。两轮设备写 IOPS 都接近 10000，这与存储限制掩盖网络收益的解释一致；尚未进行相同负载下的多档 IOPS 对照，不能仅凭这组相关性完全确定原因。

YCSB 各模式的设备写 IOPS 约 8200–9800，并非所有 case 都持续达到 10000；日志同步延迟与 IOPS 上限需要分别看待。

即使 TPS 不变，SmallBank Lazy 计算节点平均 CPU busy 仍从 5.54% 降到 4.36%。这里百分比以整台 8 vCPU 为分母，包含主机上其他工作，不是单核占用率，也不是仅应用 CPU 的剖析结果。

### 3. 普通 2PC 的额外持久决议等待仍然清楚可见

以下仅取 SmallBank 多写参与者的成功提交，按该路径事务数加权合并两次运行，单位 ms/path transaction：

| SmallBank 多写路径 | 传输 | 提交 ms | prepare ms | decision_log ms | final_commit ms |
|---|---|---|---|---|---|
| 2PC | TCP | 2.283 | 1.212 | 0.946 | 0.122 |
| 2PC | RDMA | 2.280 | 1.222 | 0.967 | 0.089 |
| Cornus | TCP | 1.310 | 1.204 | 0.000 | 0.104 |
| Cornus | RDMA | 1.297 | 1.207 | 0.000 | 0.087 |

Cornus 消除了单独的 decision_log 等待，收益显著大于本配置下 TCP 换 RDMA 的收益。RDMA 缩短 final_commit 等通信时间，但不能消除前面的持久化屏障。提交总时间还包括未单列的本地工作，表中子项不保证精确加和到总计。

Cornus-opt 在这组配置下没有显示出相对普通 Cornus 的稳定吞吐优势，暂时不能把存储直接通知视为已证实的性能提升。

### 4. 日志条数、WAL 字节、fdatasync 次数与云盘 IOPS 是四种量

| SmallBank 模式 | 传输 | WAL B/提交 | fdatasync 次/提交 | 记录/日志批 | fdatasync ms/次 | 设备写 IOPS |
|---|---|---|---|---|---|---|
| Lazy | TCP | 431 | 0.798 | 4.20 | 0.516 | 10000 |
| Lazy | RDMA | 431 | 0.799 | 4.19 | 0.503 | 9999 |
| Cornus | TCP | 538 | 0.731 | 5.70 | 0.484 | 10138 |
| Cornus | RDMA | 538 | 0.710 | 5.87 | 0.472 | 10230 |

SmallBank Cornus 每次提交的 WAL 字节更多，却通过更大的日志批次产生更少的 fdatasync/commit。这解释了为什么“参与者多、逻辑日志多”不必表现为更高的物理刷盘次数。fdatasync 平均耗时也依赖各模式的批次、并发与队列状态，不能当成与模式无关的固定常数。

只读/单本地写/单远程写/多写路径的占比和耗时均保留在 `metrics.json`。Lazy 的单写表示提交时只有一个日志生产节点，其中仍可能包含执行期间的跨节点页面转移；不能直接与 Cornus 单本地写样本视作相同事务群。

## 核验、原始数据与限制

- 共 3,924,755 次完成尝试、3,912,398 次成功提交；32 次运行、96 份节点结果全部通过核验。
- 所有运行使用同一源码指纹 `cb2a43e8a4814fe51968c747568bea56478065ca3efac1bd2dbb95b638f9ae36`。由于本轮含未提交改动，不能只用 Git HEAD 判断版本；以 `deployment_audit.json`、源码指纹和有效配置共同确定。
- 事务计数、四种提交路径计数/总时间、五种日志等待计数/总时间均核对一致。
- 全部 RDMA case 的四节点硬件收发字节计数有增长；TCP case 对应 RDMA 字节计数没有增长，未观察到采样指标中的带宽/包速率限流丢包。
- `baseline_plan.json`、`driver.py` 记录编排；每个 case 保存 `config.json`、`metadata.json`、`node*/effective_config.json`、`result.txt`、`stats.log`、TPS 时间序列、存储计时与系统采样。`metrics.json`、`summary.json` 为分析结果，`final_validation.json` 为核验记录。
- 全窗口 TPS 和阶段时间用于主表。另保留去掉开头约 6 秒、末尾约 2 秒的 TPS，以及对应区间 CPU/IOPS 作为稳定性检查；阶段时间没有按这个稳态区间重新计算。节点测量窗口各自计时，不是严格同步的全局窗口。
- 每种组合只有两次 30 秒运行，结论局限于每节点 2 线程、θ=0.6、本实现和云盘配置。未推断 p99，也未测试高并发、热点、高远程比例下的表现。
- `generate_log=0` 会改变普通 2PC 的提交路径，不能作为只移除刷盘成本的干净对照；本轮始终开启日志。参与者日志等待与协调者关键路径存在重叠，不能相加。

上述 10000 IOPS 实验之后，按相同配置完成了下面的 20000 IOPS 复测。

## 20000 IOPS 复测与跨档位比较

本轮仍聚焦 TCP/eRDMA 对 Lazy、普通 2PC 与 Cornus 变体的影响，没有引入新的组批、限流或日志调度策略。

- 用户确认将存储 IOPS 档位提高至 20000。没有通过云控制台 API 独立读取配额；实际 I/O 数据见下表。
- 同一份 32 次运行计划、每节点 2 线程、每次 30 秒、两个重复；负载、冲突参数、日志配置和模式顺序均与 10000 IOPS 相同。
- 四节点运行副本的源码、二进制在开跑前和结束后核对。两档位的 32 份运行配置及 96 份节点有效配置逐一相等；核验记录为 `cross_study_validation.json`。
- 新结果目录：开发服务器 `/data/workspace/Hybrid_Cloud_MP/result/transport_study_20261105_iops20000`，本地保存同名 `result` 副本。

### Commit TPS：扩容收益与 RDMA 收益分别比较

每格 TPS 是两次运行的平均集群 Commit TPS；RDMA 增益为同一档位下 `RDMA TPS / TCP TPS - 1`。跨档位实验在不同时间段进行，存储时延波动仍可能影响差值。

| 负载 | 模式 | 1万：TCP / RDMA TPS | 1万：RDMA增益 | 2万：TCP / RDMA TPS | 2万：RDMA增益 |
|---|---|---|---|---|---|
| ycsb | Lazy | 2632 / 2768 | +5.1% | 2494 / 2684 | +7.6% |
| ycsb | 2PC | 2080 / 2164 | +4.1% | 2085 / 2085 | +0.0% |
| ycsb | Cornus | 2667 / 2700 | +1.2% | 2553 / 2627 | +2.9% |
| ycsb | Cornus-opt | 2586 / 2702 | +4.5% | 2544 / 2618 | +2.9% |
| smallbank | Lazy | 5580 / 5570 | -0.2% | 6259 / 6363 | +1.7% |
| smallbank | 2PC | 4740 / 4851 | +2.3% | 5049 / 5182 | +2.6% |
| smallbank | Cornus | 5942 / 6135 | +3.3% | 6361 / 6539 | +2.8% |
| smallbank | Cornus-opt | 5929 / 6158 | +3.9% | 6055 / 6425 | +6.1% |

![不同IOPS档位下的RDMA收益](../result/transport_study_20261105_iops20000/rdma_gain_by_iops.png)

![20000 IOPS吞吐](../result/transport_study_20261105_iops20000/throughput.png)

### 20000 IOPS 的执行、提交和刷盘时间

执行和提交时间分母是所有完成的事务尝试，包含回滚；fdatasync 分母是实际同步调用次数；设备 IOPS 为系统采样稳态区间的值。

| 负载 | 模式 | 执行 ms：TCP → RDMA | 提交 ms：TCP → RDMA | fdatasync ms/次：TCP → RDMA | 设备写 IOPS：TCP / RDMA |
|---|---|---|---|---|---|
| ycsb | Lazy | 0.869 → 0.725 | 1.486 → 1.461 | 0.671 → 0.664 | 7597 / 8061 |
| ycsb | 2PC | 0.405 → 0.376 | 2.380 → 2.417 | 0.680 → 0.706 | 8901 / 9138 |
| ycsb | Cornus | 0.407 → 0.370 | 1.873 → 1.844 | 0.755 → 0.742 | 8849 / 8548 |
| ycsb | Cornus-opt | 0.409 → 0.375 | 1.882 → 1.848 | 0.756 → 0.747 | 8739 / 8496 |
| smallbank | Lazy | 0.196 → 0.159 | 0.756 → 0.778 | 0.436 → 0.432 | 11635 / 11862 |
| smallbank | 2PC | 0.129 → 0.100 | 1.053 → 1.053 | 0.450 → 0.454 | 11035 / 11012 |
| smallbank | Cornus | 0.127 → 0.097 | 0.810 → 0.815 | 0.446 → 0.454 | 11071 / 11073 |
| smallbank | Cornus-opt | 0.128 → 0.097 | 0.857 → 0.831 | 0.466 → 0.446 | 10529 / 10779 |

![20000 IOPS阶段时间](../result/transport_study_20261105_iops20000/stage_times.png)

两次相邻 TCP/RDMA 配对的增益分别如下。图中点和误差线仅表示两个观测及其范围，不是置信区间。

| 负载 | 模式 | 2万重复1 RDMA增益 | 2万重复2 RDMA增益 |
|---|---|---|---|
| ycsb | Lazy | +8.92% | +6.37% |
| ycsb | 2PC | +1.28% | -1.22% |
| ycsb | Cornus | +3.45% | +2.31% |
| ycsb | Cornus-opt | +1.87% | +3.97% |
| smallbank | Lazy | -0.17% | +3.51% |
| smallbank | 2PC | +3.42% | +1.83% |
| smallbank | Cornus | +3.01% | +2.58% |
| smallbank | Cornus-opt | +4.62% | +7.65% |

### SmallBank Lazy：磁盘配额与单日志流吞吐是两种限制

扩容后 SmallBank Lazy 的 TCP TPS 变化为 +12.2%，RDMA 为 +14.2%。实际写 IOPS 超过原来的约 10000，但仍显著低于用户报告的 20000 档位；RDMA 相对 TCP 的 TPS 变化仍为 +1.7%。因此，原来的 IOPS 档位限制了吞吐，但不是 RDMA 吞吐收益不明显的唯一解释。

代码中，`compute_server/server.h` 的 `LogFlush()` 在同一个节点内持有 `log_flush_mtx_` 完成同步 `LogWrite` RPC，并在成功后推进持久化 LSN；benchmark 每节点一个后台日志线程。`core/storage/log_manager.cc` 对每个节点的日志流执行 append + fdatasync。

结合原始计数可以估算日志线程用于存储 RPC 的占用：`所有节点日志 RPC 累计秒数 / (3节点 × 30秒)`。这是现有计时推导的平均占用，包含 RPC 与持久化等待，不是 CPU 利用率或磁盘 busy；也不是专门采集的请求时间线。

| IOPS档位 | 传输 | 日志RPC ms/批 | 日志RPC占用估算 | 写提交数/日志批 | 执行 ms/attempt | 提交 ms/attempt |
|---|---|---|---|---|---|---|
| 10000 | TCP | 0.654 | 97.0% | 1.001 | 0.199 | 0.871 |
| 10000 | RDMA | 0.663 | 98.3% | 1.000 | 0.161 | 0.910 |
| 20000 | TCP | 0.581 | 96.9% | 1.001 | 0.196 | 0.756 |
| 20000 | RDMA | 0.580 | 98.2% | 1.000 | 0.159 | 0.778 |

这些观测支持：当前 SmallBank Lazy 的日志线程接近持续忙碌，而且每批仅覆盖约一个写事务。worker 通过 RDMA 更早完成执行后，可能更早进入前一批日志尚未结束的等待区间，执行时间的下降未必提高日志流的完成速率。这是对当前实现与低并发配置的解释，不是所有权转移协议的普遍性能上限；最终归因仍可用请求时间线进一步验证。

本轮没有为提高吞吐而修改日志策略，保留了与 10000 IOPS 的可比性。

### 结论的边界与后续验证

- 提高 IOPS 后，RDMA 在执行阶段是否节省时间，要与提交阶段和吞吐分别判断；不能预设扩容一定会放大 RDMA 的相对收益。
- YCSB Lazy 在 20000 IOPS 下执行均值为 TCP 0.869 ms、RDMA 0.725 ms，提交均值为 1.486/1.461 ms。各模式的完整数据、CPU 与提交路径分解均在 `iops_comparison.json` 和 `metrics.json` 中。
- 未观察到的云端内部限流和其他资源限制，不能仅由主机平均写 IOPS 排除。跨档位不是交替随机化实验；YCSB 等未接近旧配额的 case，更需留意 fdatasync 波动。
- 当前结果仅覆盖每节点 2 线程、θ=0.6。若继续验证 RDMA，下一项应保持 20000 IOPS 和现有日志实现，单独提高 worker 并发，观察通信比例、日志组批和吞吐随并发变化；不同时改日志策略。
- 20000 IOPS 共 4,087,170 次完成尝试、4,075,548 次成功提交，96 份节点结果核验通过；RDMA 四节点硬件收发计数有增长，TCP 对照未出现 RDMA 流量，采样指标中未观察到带宽/包速率限流丢包。


## 单次所有权获取与 remote fetch 的 RDMA 收益

以下均值按三个计算节点、两次重复的累计操作时间除以累计调用次数计算，不是各节点均值的简单平均。单位为 μs/次；计数包含返回事务冲突的 remote fetch 请求。数据来自各档位的 `metrics.json`，可由节点 `result.txt` 中原始计数和累计时间复核。

计时范围：

- Lazy：`compute_server/lazyrelease_server.cc` 的 `ownership_transfer_count` / 累计时间，统计需要获取全局权限的路径，从本地锁操作之后开始，到权限及页面就绪结束。包含共享/排他权限、升级、可能的远端释放、页面推送或存储读取，不保证每次都实际更换页面所有者。
- 2PC / Cornus：`compute_server/twopc_fetch_server.cc` 的 `Get_2pc_Remote_page`，从构造请求前到 RPC 返回并完成结果解析、记录拷贝后结束。服务端 `GetDataItem` 读取本地页，写请求还检查/获取行锁并生成锁日志；当前成功响应传回整个 `PAGE_SIZE` 页面，调用端再抽取目标记录。计时不包含后续事务提交。
- 因此两者都是当前实现的操作耗时，不能当作纯网络 RTT，也不能将两种调用视为相同工作量。这里比较各自从 TCP 换到 RDMA 后的变化。

### 20000 IOPS

| 负载 | 操作 | TCP μs/次 | RDMA μs/次 | 节省 μs/次 | 降幅 |
|---|---|---:|---:|---:|---:|
| ycsb | Lazy 所有权获取 | 139.4 | 115.7 | 23.7 | 17.0% |
| ycsb | 2PC remote fetch | 85.2 | 78.9 | 6.3 | 7.4% |
| ycsb | Cornus remote fetch | 87.1 | 78.7 | 8.4 | 9.6% |
| ycsb | Cornus-opt remote fetch | 86.8 | 79.3 | 7.5 | 8.7% |
| smallbank | Lazy 所有权获取 | 138.1 | 110.6 | 27.5 | 19.9% |
| smallbank | 2PC remote fetch | 100.9 | 77.0 | 24.0 | 23.7% |
| smallbank | Cornus remote fetch | 103.2 | 76.4 | 26.7 | 25.9% |
| smallbank | Cornus-opt remote fetch | 102.7 | 76.5 | 26.2 | 25.5% |

### 10000 IOPS

| 负载 | 操作 | TCP μs/次 | RDMA μs/次 | 节省 μs/次 | 降幅 |
|---|---|---:|---:|---:|---:|
| ycsb | Lazy 所有权获取 | 138.4 | 114.9 | 23.5 | 17.0% |
| ycsb | 2PC remote fetch | 86.0 | 79.0 | 6.9 | 8.1% |
| ycsb | Cornus remote fetch | 86.3 | 78.6 | 7.7 | 8.9% |
| ycsb | Cornus-opt remote fetch | 86.0 | 78.9 | 7.1 | 8.3% |
| smallbank | Lazy 所有权获取 | 138.8 | 111.4 | 27.4 | 19.7% |
| smallbank | 2PC remote fetch | 102.1 | 77.1 | 25.0 | 24.5% |
| smallbank | Cornus remote fetch | 103.0 | 76.8 | 26.2 | 25.5% |
| smallbank | Cornus-opt remote fetch | 102.7 | 76.4 | 26.3 | 25.6% |

### 对执行阶段的影响

在 20000 IOPS 下，YCSB 每次事务尝试约有 5.7 次 Lazy 所有权获取，普通 2PC 约有 3.2 次 remote fetch。将累计操作时间按事务尝试数归一化，Lazy 从 792.8 降到 656.2 μs/attempt，节省 136.5 μs；普通 2PC 从 274.6 降到 254.6 μs/attempt，节省 20.0 μs。对应完整执行阶段分别节省 144.2 和 29.1 μs/attempt。Cornus 两版 remote fetch 单次节省约 7.5–8.4 μs，累计节省约 24–27 μs/attempt。

SmallBank 单次绝对收益更接近：Lazy 节省 27.5 μs，2PC/Cornus 各版节省 24–27 μs。普通 2PC 的相对降幅比 Lazy 大，主要因为原始单次耗时更短，不能概括成 RDMA 总是更有利于所有权转移。

两个 IOPS 档位的单次调用数据接近，说明本轮测得的执行路径收益在这两个档位下较稳定。当前 YCSB 的 Lazy 执行劣势缩小得更明显，但仍未追平普通 2PC；执行阶段省时也不等于吞吐同比提高，后者还受到提交和日志流完成速率影响。以上仅为当前低并发、固定负载下的均值，不推断尾延迟或所有配置下的性能。

## 环境重建（2026-11-06）

原开发/存储实例已释放且无磁盘或快照，保留下来的原计算节点改作开发、storage_pool 和 remote_node。已提交代码恢复到 `1cbf967`；旧开发目录和旧运行目录分别保留为 `/data/workspace/Hybrid_Cloud_MP-preserved-20261106`、`/usr/local/exper/Hybrid_Cloud_MP-preserved-20261106`。历史实验结果仍在本地，新的 YCSB/SmallBank 数据由基准程序重建。

| 角色 | 管理地址 | RDMA 地址/网卡 |
|---|---|---|
| 开发、storage_pool、remote_node | Ali-hybrid；内网 172.16.219.127 | 172.16.219.128 / eth1 |
| compute 0 | 172.16.219.130 | 同管理地址 / eth0 |
| compute 1 | 172.16.219.133 | 同管理地址 / eth0 |
| compute 2 | 172.16.219.134 | 同管理地址 / eth0 |

本轮修复和验证：

- 新节点的主网卡已具备 eRDMA，但镜像中的驱动处于 Standard 模式，bRPC 为旧版。已启用并持久化 `compat_mode=Y`，同步保留节点上已验证的 RDMA bRPC；四机 `libbrpc.so` SHA256 一致，为 `6346bba474bff0a4c5e575426e3ffd34b568ec8aac42178bfe8e55c3eeef1f41`。旧库和头文件保留在各新节点 `/root/hcmp-env-backup-20261106`。
- eRDMA 使用 GID index 1、max SGE 4、按需建立 QP；兼容模式与 bRPC OOB 建链的关系见[阿里云官方说明](https://help.aliyun.com/zh/ecs/user-guide/deploy-brpc-applications-with-high-network-performance-based-on-erdma)。
- 开发节点原有 `/etc/netplan/60-hcmp-erdma.yaml` 只配置了旧节点的辅助网卡路由，导致 `.133/.134` 访问 `.128` 超时，而 `.130` 正常。已把计算节点的 `/32` 路由更新为 `.130/.133/.134` 经 eth1，验证返回路径正确。
- 已配置开发机到三台计算节点以及自身 `.128` 的免密。原实验脚本可直接使用该免密，不需将新密码写入仓库。
- 三台计算节点分别对开发节点的实际时间戳服务完成 32 次 RDMA RPC。随后使用原 `test_multi_node.py` 完成部署和编译，并运行 YCSB、SmallBank × Lazy、2PC、Cornus、Cornus-opt 共 8 组短测，每节点 2 worker、每组 10 秒、日志开启。
- 24 份节点结果完整，共 89,712 次事务尝试、89,470 次提交；事务计数与计时覆盖核对通过，`stats.log` 无一致性告警。四机 RDMA 硬件收发字节数均增长；快照同时包含启停过程，记录到 `hw_rx_disable_drop_cnt` 增长，不能把本次验证表述为所有设备丢弃计数为零。结束后无残留压测、存储或 remote 进程。

开发目录为 `/data/workspace/Hybrid_Cloud_MP`，运行目录为 `/usr/local/exper/Hybrid_Cloud_MP`。在开发目录运行 `python3 test_multi_node.py` 可启动当前脚本配置的实验；本次短测通过 `--run-seconds 10 --result-dir result/rdma_short_20261106` 指定时间与结果目录。

恢复及设备证据在 `result/environment_restore_20261106`，短测结果在 `result/rdma_short_20261106`，本地与开发机均保存副本。此次只验证部署、通信和正常事务路径；新存储盘的配置未与之前 20000 IOPS 档位对齐，不能据此比较新旧性能，也未覆盖故障恢复正确性。

下一轮目标：先审查并复用已有 `lazy_2RTT_count`、`lazy_3RTT_count`、所有权获取计时、prepare 回调及在线提交成本采样，校准计数范围和开关，再补同一事务的首个/最后一个持久化 YES 时间差。旧样本中 RTT 分类计数之和与所有权获取次数不一致，不能直接计算每次获取的平均网络轮数。本轮没有改动这些统计或协议实现，也未开始长时间成本边界探索。

### 通用开发镜像模板：172.16.219.130

2026-11-06 在 `.130` 补齐 `python3-paramiko`、`sshpass`、pip 和 venv，保留现有编译工具链、RDMA bRPC 与持久化 Compat 配置。`/data/workspace/Hybrid_Cloud_MP` 已恢复为 `1cbf967` 加当前集群地址和本文的工作区改动，旧开发目录保留为 `/data/workspace/Hybrid_Cloud_MP-preserved-20261106`；仓库 Git 作者配置与本地一致。该开发目录已独立编译 `compute_server`、`storage_pool`、`remote_node`，实验入口的计划生成也通过验证。

用户指定的 `hcy@DESKTOP-IPQ3DNT` 公钥已加入 `/root/.ssh/authorized_keys`，指纹为 `SHA256:kIpV/jV/omliZXbMAIFeF2itnJAzH5lKANJkeLo9TKU`。核对了公钥格式、文件权限和 sshd 公钥认证配置；未持有对应私钥，因此未声称完成该密钥的客户端登录测试。没有把开发节点的出站 SSH 私钥复制到模板。

使用独立临时数据目录验证本机存储、remote 服务与 32 次 RDMA 时间戳 RPC，全部成功；临时数据和验证进程已清理。证据在 `.130` 的 `result/image_template_20261106`，也备份到本地及现开发机。

可将 `.130` 制作为计算/开发/存储通用软件环境镜像。克隆实例仍需在云侧选择支持 eRDMA 的规格并开启主网卡 ERI，设置登录凭据，更新脚本顶部的集群地址并配置节点间认证；若继续使用 `.128` 作为存储，还需更新它到新计算节点的辅助网卡路由。模板准备验证不等同于已经创建并验证新镜像；本轮未创建镜像、停止实例或启动长期实验。
