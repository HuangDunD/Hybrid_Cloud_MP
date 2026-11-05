# 实验结果与 stats.log

运行入口是 `python3 test_multi_node.py`。日常修改脚本前部的“实验配置”区后直接运行；
类和函数集中在配置区之后。续跑时读取已保存的计划参数。`summary_human.txt` 的 key=value 格式、
字段与旧含义保持不变；新增的 `stats.log` 直接读取各节点原始数据，不沿用旧摘要的平均规则。

集群 IP 也在脚本前部配置：`compute_server_hostnames` 按顺序对应计算节点 0、1、2，
`remote_server_host` 是同机部署的 `storage_pool` 和 `remote_node` 地址。
当前默认计算节点为 `172.16.219.121`、`.122`、`.123`，服务节点为 Ali-hybrid 的
`172.16.219.124`。修改地址时直接编辑脚本顶部的这两个变量。
脚本将这些地址同时用于 SSH、部署、计划配置快照以及下发的 `compute_node_config.json`，
无需再手动修改 JSON 中的 IP。其他配置仍从 JSON 读取，源码目录的 JSON 不被改写。
直接启动 C++ 程序时仍使用它所在目录的 JSON。更换集群地址后应创建新计划。

当前脚本默认是新集群短测：YCSB、SmallBank 各跑 Lazy 和三种 2PC，每节点 2 个线程、
每次 20 秒，本地访问比例 0.5、写比例 0.8、Zipf theta 0.60、NO_WAIT。
共 2 个 case、8 次运行，测量时间合计约 160 秒，另加部署、编译和初始化时间。
保持 `generate_log=1`、`parallel_page_fetch=0`，
确认节点正常结束、计时覆盖完整，并比较 `stats.log` 的 execute、commit、abort、other 和 total。
直接在脚本顶部 `modes` 选择协议，不再手动切换同名 2pc 结果背后的开关。
短测用于验证环境和统计，不据此作性能结论。
修改脚本参数后直接运行会创建新计划；续跑旧计划仍使用旧计划的参数。

| 模式 / 结果目录 | Prepare 完成条件 | 协调者决议日志 | 最终 Commit |
| --- | --- | --- | --- |
| `lazy` | 单节点提交 | 不适用 | 本地提交等待 |
| `2pc` | 完整写集持久化后，等待参与者 YES | 等待 | 等待全部 Prepared Commit RPC |
| `2pc-cornus` | 同上，等待参与者 YES | 省去 | 同上 |
| `2pc-cornus-opt` | 存储汇总持久化 YES，通知协调者 | 省去 | 同上 |

三种 2PC 都沿用新基线完整 Prepare 写集，多写参与者的最终 Commit 不再单独等待结束日志。
单写参与者快速路径保持不变。`2pc-cornus` 是本次补齐的中间正常路径，
用于分离“省去决议日志”与“存储通知”两项收益；尚未实现完整 Cornus 的 LogOnce、故障恢复和接管。
脚本下发 `local_compute_node.two_pc_variant`，并同步旧 `cornus_prepare_commit` 字段。
直接启动旧配置时仍兼容原开关（0=2pc，1=2pc-cornus-opt）；显式模式优先。
`stats.log`、`comparison.csv` 使用独立模式名；comparison 另列 execute、commit、Prepare、Backup 和最终 Commit 耗时。

首次部署会先创建目标父目录，再将源码复制到明确的 `Hybrid_Cloud_MP` 目录；
通过 `cmake -S ... -B ...` 创建并配置构建目录，不要求镜像预先带有 build 目录。
计算节点只编译 `compute_server`，服务节点编译 `storage_pool` 和 `remote_node`。

## Case plan 与目录

参考 `run_fastlog_iteration.sh`，先将完整实验计划写入文件，再按 case 顺序执行。
每个 case 表示一组负载参数，负载名统一使用 ycsb/smallbank/tpcc 小写，
下面按 lazy/2pc/mix 等模式分别存结果。已有计划继续使用其保存的目录名。
同一个 case/mode 只执行一次，不创建负载分类、round、repeat 或 legacy 目录。

```text
result/benchmark_results_<运行时间>/
  case_plan.tsv                  # 带列名的完整计划；编号从 001 开始
  case_plan.md                   # 同一份计划的可读表格
  case_status.tsv                # 每个 case/mode 的执行状态
  config.json                    # 基础配置、源码指纹、计划指纹（去除凭据）
  comparison.csv                 # 每个已完整结束的 case/mode 一行
  case_001-ycsb-t10-theta0.95-lr0.5-wr0.9-mix_thres0.2-topn100-lock1/
    lazy/
      metadata.json
      stats.log
      summary_human.txt
      summary_matrix.txt
      node0/result.txt
      node0/tps.log               # 运行时每 2 秒追加带时间戳的 TPS 日志
      node0/compute.out           # 计算节点完整标准输出/错误
      node0/effective_config.json
      node1/...
      node2/...
      storage/storage_timing_stats.txt
    2pc/...
    mix/...
  case_002-ycsb-t10-theta0.95-lr0.5-wr0.5-mix_thres0.2-topn100-lock1/...
  case_013-smallbank-t10-theta0.95-lr0.5-wr0.9-mix_thres0.2-topn100-lock1/...
```

模式来自生成计划时的 `modes`，只创建实际执行的模式目录；上图展示可用的组织形式。
完整参数包括节点数和每线程尝试数保存在 case_plan.tsv，不依赖解析目录名。
目录名中的 `mix_thres0.2` 表示混合模式的选择阈值，`theta0.95` 表示负载的 Zipf 参数。
运行时长和尝试数保留在计划与报表中，不写入目录名。
summary_human.txt 继续使用原来的 key=value 字段结构（其中 round=0 仅为字段兼容），
stats.log 和 comparison.csv 使用 case_id。旧 final_human/final_matrix 的跨参数平均不再生成。
历史结果不移动，tools/benchmark/report.py 仍能读取历史原始结果。
旧版绘图和专项比较脚本已移除，新计划的比较数据统一使用 comparison.csv。

## 按数量或时间运行与 TPS

SmallBank 的 SendPayment 在余额不足时不修改余额，仍走正常提交并返回实际提交结果；
长事务中只要有一笔余额不足，整笔长事务都不修改余额。取数和加锁流程保持不变，
业务余额不足不再触发回滚，执行或提交失败仍按实际结果统计。

原有三个负载循环只支持每线程固定尝试数，原 TPS 输出由 TxExe 触发，
且最终 throughput 是各线程 TPS 之和。现在 YCSB、SmallBank、TPCC 均支持：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| run_mode | count | count 按每线程尝试数；time 按测量时长 |
| run_seconds | 60 | time 模式的测量秒数；该模式忽略尝试数上限 |

这些配置可在 test_multi_node.py 中设置，也可用下面的命令行参数覆盖，
生成计划时固定下来。远端对应 local_compute_node 中的 benchmark_run_mode、
benchmark_run_seconds。TPS 输出间隔固定为 2 秒，无需配置。
JSON 中显式保存默认值，手动启动计算程序时直接使用它们；通过 test_multi_node.py 运行时，
以脚本配置生成的 case plan 为准，启动各节点前将该 case 的值写入远端 JSON。
因此日常从脚本跑实验只需改脚本前部配置，不必同时改两处。

参考 MP-Router 的独立周期采样与窗口计数方式，计算节点等本地所有 worker 初始化完毕，
再统一开始计时。控制线程每隔约 2 秒采样一次，即使工作线程全在等锁也会输出 0 TPS。
日志采用 MP-Router 的逐行风格，以节点本地日期时间开头（精确到毫秒），
显示节点编号、提交 TPS、完成 TPS、累计提交/回滚数，不打印采样间隔或阶段标签。
TPS 仍使用实际经过的单调时钟时长作分母；末尾另输出不足一个周期的尾段。
committed_tps 为成功提交增量/经过秒数；completed_tps 包括回滚。
控制脚本将采样实时写入结果目录中的 nodeN/tps.log 并刷新文件，不在终端打印节点 TPS。
结束后采集节点完整 tps.log 与 compute.out，最终 TPS 汇总写入 stats.log。

内置负载每个工作线程顺序执行事务，完成当前事务后才发起下一笔，没有预生成的大批事务队列。
time 到期后停止发起新事务，在途事务继续收尾。正式汇总 committed_tps 为
**截止前完成的提交数 / 配置的测量秒数**，截止后的提交只进入 final_commits，
不回填窗口 TPS。stats.log 的 node_throughput 显示测量时长、收尾时长、窗口计数、
最终计数；throughput_summary 的 sum_node_window_tps 为各节点窗口 TPS 之和。
节点之间没有新增同步起跑协议，因此它不是同一集群墙钟窗口的精确吞吐。
跨截止点的采样段可能包含两侧的事务，
最终汇总单独使用按完成时刻判断的窗口计数，不对实时采样行求平均。

每个工作线程完成当前事务后检查时间，到时便退出负载循环；主线程等所有 worker 自然结束。
不设置额外的收尾超时，也不因运行时间到期强制终止计算进程。
run_seconds 不包含建库、部署、初始化、事务收尾和结束握手。本次没有加入 warmup 阶段。

事务 breakdown 仍覆盖所有完整结束的尝试，包含收尾阶段，不能直接拿窗口提交数作为它的分母。
time 模式的旧延迟百分位仅保留每线程最先完成的最多 100000 个成功事务样本，
并非整个时间窗口的无偏样本；事务计数和新 breakdown 累计值没有此上限。

## 时间口径

新编译的计算节点使用单调时钟：每次从 TxBegin 入口开始，到提交/回滚完成结束。
主表分母是**已完整测量的事务尝试数（成功 + 回滚）**，不是 commit log 数，
不是 TxExe 调用数，也不是只除以成功提交数。coverage_pct 检查测量数与负载尝试数是否一致。

- begin：清理前次事务、初始化、获取开始时间戳等。
- execute：所有 TxExe 调用之和；同一事务多次调用也只算一次事务。
- commit：TxCommit 内耗时，包括该函数内部提交失败后的收尾。
- abort：显式 TxAbortWorkLoad 内耗时，与 execute/commit 计时不重叠。
- other：端到端减去上述四项，包括 workload 在 DTX 调用外的 key 生成、业务计算和计时开销。
- total：端到端平均耗时；五项相加应等于 total。

execution_path_timing 对 eager/lazy/2pc/single/mix_lazy/mix_2pc 分别统计，
**各行除以本路径的完整尝试数**，包含执行期回滚。outcome_latency 则按成功/回滚各自的数目计算。
路径在事务收尾时按实际选择确定；mix_2pc 不等于一定执行多参与者 Prepare。
每条路径也单独列出 other_ms，避免把未落在 DTX 方法里的业务时间遗漏。

time_details 在同一张表内以树形缩进展示三个根节点，不新增表，summary_human.txt 保持原格式：

- transaction_stages：协调者 execute / commit 时间。只读为 0 个实际写参与者；单写分 local / remote；多写为超过 1 个实际写参与者。Lazy 的写事务实际在 home 提交，因此属于 single_writer/local，不能按之前接触过的页面所属节点数误归为多写。
- execute 按最终进入的提交路径分组，另列 before_commit_abort，容纳未进入提交阶段就回滚的尝试。commit 只统计进入 TxCommit 的事务，包含该函数内失败收尾；不把执行期回滚填成某条提交路径。
- multi_writer 的 commit 再分 prepare、decision_log、final_commit、other。前三项使用单调时钟测量互不重叠的实际代码区间；other 为总提交时间减去这三项，包含提交时间戳获取和其余协调工作。Cornus 的 decision_log 为零。
- log_waits：所有节点同步 wait_log_flush 调用的累计等待，细分 commit_log / prepare_log / decision_log / page_log / abort_log。page_log 来自纯同步页面日志等待，不使用混有页面接收/锁等待的旧 wait_log_flush_push_page_time。所有 LSN 等待调用都必须显式指定分类；异步推页调度器挂起时间不包含在该组。
- overlapping_diagnostics：保留原来的取页、锁等待、所有权转移等诊断量。这些行可能互相重叠，不能与事务阶段或日志等待相加。

列为 slice、count、total_s、avg_ms、meaning。事务组 count 为对应路径事务数；Prepare 等子阶段同除以多写事务数；日志组 count 为实际同步等待调用次数（包括目标 LSN 已持久化而快速返回的调用）；诊断组仍按 issued attempts 归一化。零次数时 avg_ms 为 N/A。
父子关系要求 total_s 对账，路径的 avg_ms 需按次数加权；多写提交的子阶段因分母相同，avg_ms 才能直接相加。日志等待有独立的原始总次数和总纳秒计数，与各分类合计校验；参与者可并行等待，因此不能把这一组当成协调者的串行提交时间。
旧结果缺少这些分类耗时和调用次数时保留原 time_details 展示，不从旧的 single_txn_count（包含只读）推算新分类。原始 result.txt 字段继续保留，新增 stats_commit_* 和 stats_log_wait_* 字段。

旧二进制仅有累计阶段计时：可计算每次 issued attempt 的阶段时间；端到端、other、
按执行路径的时间均为 N/A。不可从现有几项凑出 total。
旧 throughput 是各线程吞吐之和，报表明确标注 legacy_sum_worker_tps；
total_time_seconds 是各工作线程运行时长的平均值，不是整个实验墙钟。

## 汇总规则

计数/累计耗时相加；均值用累计耗时除以对应次数；回滚率用总回滚/总尝试；
最大值取最大值。各节点百分位不能平均，故保留节点 P50/P99，集群百分位为 N/A。
缺失字段/存储统计/旧二进制未采集的字段显示 N/A；缺失计算节点时集群数值也显示 N/A。
日志构造次数不代表真正落盘次数，尤其 generate_log=0 时。

config.json 是源配置快照；nodeN/effective_config.json 是命令行覆盖后的远端实际配置。
旧结果中的 run.json 仍可读取和续跑。
源码指纹不证明部署二进制一致；只有重新构建的新二进制才输出 stats_schema_version 与新计时。
本次不改变日志、事务、锁或持久化协议。

## 生成、执行与续跑

```bash
# 只保存计划，不连接远端、不编译、不压测（可省略 --result-dir 使用时间戳目录）
python3 test_multi_node.py --plan-only --result-dir result/my_plan

# 生成每个模式测量 60 秒、每 2 秒输出 TPS 的计划
python3 test_multi_node.py --plan-only --run-mode time --run-seconds 60 \
  --result-dir result/time_60s

# 执行或续跑这份按时间运行的计划
python3 test_multi_node.py --resume-result-dir result/time_60s

# 查看 case_plan.md 后，执行这份计划；中断后再次使用同一命令即可续跑
python3 test_multi_node.py --resume-result-dir result/my_plan

# 从 case 007 开始，仍跳过已经完整结束的模式
python3 test_multi_node.py --resume-result-dir result/my_plan --resume-from-case 7

# 仅打印计划，不创建任何结果文件
python3 test_multi_node.py --dry-run

# 不带参数：生成新的时间戳计划，然后开始执行
python3 test_multi_node.py

# 重新生成报表，支持新旧目录；不改写旧摘要
python3 -m tools.benchmark.report result/my_plan
```

续跑直接读取保存的 case_plan.tsv：即使修改了脚本顶部的负载、线程数、模式或参数扫描列表，
原计划的编号、参数和模式也保持不变。基础配置或数据库源码改变时会拒绝混用结果；
需还原配置/源码，或创建新计划。计划文件也有指纹校验，不应在执行中修改。

case_status.tsv 中的 pending/running/complete/failed/interrupted 分别表示未开始、运行中、
已完成、失败和用户中断；完成标记存在但结果丢失/不完整时显示 incomplete。
完成判断需要匹配的元数据、非空摘要/stats.log/节点 tps.log，以及所有节点完整的最终计数。
按数量模式要求达到计划尝试数；按时间模式校验测量秒数，不要求达到固定事务数。
程序被杀或机器掉电留下 running，也会在续跑时重跑该模式；已经完成的其他模式保留。
未完成模式重新运行时，清理该模式的旧采集文件，避免混入上次的半份结果。
关键计划、状态和完成元数据通过临时文件、fsync、原子替换保存；同一计划禁止并发执行。
`.run.lock` 是同一结果目录的进程互斥锁文件，防止两个脚本同时运行同一计划。
进程退出时系统释放锁；空文件可以保留，文件存在不代表实验正在运行。

RESUME_FROM 环境变量仍是 --resume-result-dir 的别名。
需要主动重跑已完成项时可用 --rerun-completed（或 SKIP_COMPLETED=0）。
AUTO_BOOTSTRAP=0 沿用原脚本的含义：使用已部署的程序，跳过部署和编译；
默认 AUTO_BOOTSTRAP=1。计划预览以及全部完成后的续跑都不会部署或连接集群。
使用已部署程序时，先重新编译本轮修改，再执行实验。
续跑不允许用命令行覆盖已保存的停止策略，应创建新计划。
旧目录没有 case_plan.tsv，只支持生成报表，不自动转换成可续跑的计划。
存储统计在每个模式启动前清除旧文件，结束后收集；缺失时报告 N/A。

## Python 文件职责

本次将独立的 benchmark_plan.py 合回 test_multi_node.py：计划、续跑、配置、远端启动与采集
都由同一个运行入口负责。报表实现放在 tools/benchmark/report.py，由主脚本自动调用；
也可通过上述模块命令独立生成报告。
旧版绘图和专项比较入口已删除，不建立 legacy/archive 代码目录。
独立交互压测客户端 interactive_bench_loadgen.py 属于另一条运行流程。

## 验证

此类修改以服务器上的真实编译和运行验证为主，不因本地缺少运行环境增加模拟测试文件。
在服务器的独立目录中完整构建，确认空闲后运行少量真实 case，核对：

- 按数量模式能达到计划尝试数，按时间模式能在截止后停止发起新事务。
- 每个计算节点约每 2 秒输出 TPS，最终窗口计数和原始结果一致。
- 到期后当前事务自然结束，收尾事务单独计数，不计入窗口 TPS。
- 中断后可以使用原计划续跑，已经完整结束的模式被跳过。

语法检查或计划预览不能代替真实负载验证。

2026-10-22 已在真实集群验证提交 99837f2：31 上完整构建 compute_server、storage_pool、
remote_node，部署到 31/32/33 三个计算节点与 34 存储/元信息节点。
采用 YCSB lazy、每节点 2 个线程、generate_log=0、lock_mode=0：

- 时间模式：各节点测量 10 秒，约每 2 秒输出 TPS，全部自然结束并返回 0。
  窗口内提交 9749 笔，收尾另完成 6 笔；节点窗口 TPS 之和为 974.9。
  总尝试 10587 次，breakdown 覆盖率 100%。
- 数量模式：每线程 100 次，各节点均完成 200 次尝试，全部正常结束。
- 再次续跑已完成的时间计划，直接跳过，没有重启集群。

服务器结果位于 /root/hcy/Hybrid_Cloud_MP/result/timed_smoke_20261022 和
count_smoke_20261022。以上为功能短测，不代表正式性能结论；本次未验证其他模式、负载、
开启日志、等待锁策略或中途故障恢复。

### 2026-11-04 阿里云首次部署与短测

Ali-hybrid（172.16.219.124）运行控制脚本、storage_pool 和 remote_node；
172.16.219.121/122/123 为三个计算节点。开发目录为 /data/workspace/Hybrid_Cloud_MP，
部署目录为 /usr/local/exper/Hybrid_Cloud_MP。

首次部署失败的原因是 scp 仅指定父目录，父目录不存在时源码落在 /usr/local/exper 本身，
构建命令却进入 /usr/local/exper/Hybrid_Cloud_MP/build。脚本已修复为先创建父目录、
复制到完整项目路径，再用 cmake -S/-B 创建构建目录。构建输出先读取再等待退出，失败时保留输出尾部。
开发节点已安装 python3-paramiko 和 sshpass；四台机器原有 BRPC 和编译工具可用。

已通过脚本完成清理、复制、四节点编译、启动、采集全过程。结果保存在 Ali-hybrid：
/data/workspace/Hybrid_Cloud_MP/result/benchmark_results_20261104_132649。
YCSB、SmallBank 各运行 Lazy 和新基线 2PC，每节点 2 线程、20 秒，日志开启，
cornus_prepare_commit=0。四次运行全部 complete，12 份节点结果齐全，计时覆盖率均为 100%。
短测只验证环境、路径和统计，不作为正式性能结论；当前未验证优化版 2PC。

### 2026-11-04 四模式时间分解探索（半小时）

本次在 Ali-hybrid 控制的 3 个计算节点上实跑。每节点 8 vCPU / 32 GB；统一 Debug / -O0 构建，日志开启，NO_WAIT，parallel_page_fetch=0，每次测量 20 秒，无预热。基础刷日志间隔 3 ms，批量触发 16、通知阈值 4；存储 group commit 默认等待 150 us。

模式定义：2pc 是已经前置完整 Prepare 写集、取消多写参与者 Commit 结束日志等待的新基线；2pc-cornus 使用相同 Prepare，但取消协调者 Backup 决议日志；2pc-cornus-opt 进一步使用存储 AllPrepared 通知。两种 Cornus 都是正常路径原型，没有补齐 LogOnce、故障恢复或接管。单写参与者路径不变。

负载参数 local_ratio=0.5 是近似本地事务比例，并非每条记录有一半本地访问。YCSB 的 wr0.8 对应每事务 2 读 8 写；SmallBank 对应约 80% 写事务。不要混淆两种负载的写比例含义。

时间均为 ms/完整尝试（成功+回滚），按节点累计时间/累计次数计算；总时间包含 begin、execute、commit、abort、other。高冲突时这些值不是仅成功事务的延迟。TPS 是节点测量窗口 TPS 之和。

| 负载 / 每节点线程 / θ | Lazy E / C | 2PC E / C | Cornus E / C | Cornus-opt E / C |
| --- | ---: | ---: | ---: | ---: |
| ycsb / 2 / 0.6 | 0.823 / 3.799 | 0.397 / 4.714 | 0.370 / 3.138 | 0.382 / 3.194 |
| ycsb / 10 / 0.6 | 1.530 / 3.143 | 0.413 / 5.209 | 0.436 / 5.711 | 0.431 / 6.054 |
| ycsb / 10 / 0.95 | 7.134 / 2.329 | 0.299 / 2.176 | 0.324 / 1.832 | 0.328 / 2.168 |
| smallbank / 2 / 0.6 | 0.218 / 3.122 | 0.159 / 3.676 | 0.146 / 2.812 | 0.146 / 2.837 |
| smallbank / 10 / 0.6 | 0.236 / 2.472 | 0.129 / 3.418 | 0.131 / 2.588 | 0.130 / 2.657 |
| smallbank / 10 / 0.95 | 1.755 / 3.552 | 0.125 / 3.335 | 0.127 / 2.530 | 0.127 / 2.654 |

| 负载 / 每节点线程 / θ | Lazy TPS | 2PC TPS | Cornus TPS | Cornus-opt TPS |
| --- | ---: | ---: | ---: | ---: |
| ycsb / 2 / 0.6 | 1283.05 | 1145.60 | 1661.75 | 1639.70 |
| ycsb / 10 / 0.6 | 5908.85 | 4777.20 | 4393.50 | 4168.05 |
| ycsb / 10 / 0.95 | 1077.30 | 2837.25 | 3728.15 | 3519.25 |
| smallbank / 2 / 0.6 | 1792.15 | 1561.15 | 2023.15 | 2005.95 |
| smallbank / 10 / 0.6 | 11024.05 | 8429.90 | 10990.80 | 10724.30 |
| smallbank / 10 / 0.95 | 5461.35 | 8307.70 | 10888.20 | 10418.75 |

初步结论：

- 低并发下，取消协调者 Backup 已足以让 Cornus 超过 Lazy。YCSB 基线 Backup 约 1.59 ms/尝试，取消后提交从约 4.71 降到 3.14 ms；不要把这一收益全部归因于存储通知。
- 10 线程强热点 θ=0.95 下，Lazy 执行耗时明显增加：YCSB 约 7.13 ms，2PC 约 0.30 ms；两者回滚率都约 58%，不能只用回滚次数解释差距。SmallBank 也出现类似执行阶段差额。
- 10 线程、中等热点下，Lazy 在两轮 YCSB 中都领先；SmallBank 的 Lazy 与普通 Cornus 接近持平。这组探索存在不同的优势区域，不能由低并发短测推出“优化 2PC 总能替代 Lazy”。
- 主矩阵六组条件均未看到存储通知版超过普通 Cornus。当前毫秒级持久化/组批等待大于可节省的通知转发成本；最终 Commit 仍可能等待参与者本地准备状态。该解释是待进一步细分验证的机制推断。
- 反向顺序复测中，普通 Cornus 的中等热点 YCSB 从约 4394 升至 5653 TPS，每次 fdatasync 合并的 LogWrite 数从 1.61 升至 2.39；Lazy 与基线 2PC 基本稳定。排序变化同时也是重复运行，不能单独归因为运行顺序，但它证明单次结果不足以确定固定的提交成本。

最后两组核验（YCSB，10 线程，θ=0.6）：

| 实验 | 模式 | TPS | Execute ms | Commit ms | Prepare ms | Backup ms | 最终 Commit ms | LogWrite / fdatasync | fdatasync ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3ms 反向顺序 | 2pc-cornus-opt | 4142.100 | 0.433 | 6.127 | 2.787 | 0.000 | 0.144 | 1.500 | 1.525 |
| 3ms 反向顺序 | 2pc-cornus | 5653.050 | 0.439 | 4.378 | 1.998 | 0.000 | 0.090 | 2.388 | 1.604 |
| 3ms 反向顺序 | 2pc | 4723.100 | 0.411 | 5.277 | 1.566 | 1.795 | 0.095 | 2.990 | 1.578 |
| 3ms 反向顺序 | lazy | 5909.650 | 1.539 | 3.139 | 0.000 | 0.000 | 0.000 | 2.994 | 1.540 |
| 1ms 刷日志间隔 | lazy | 5943.850 | 1.535 | 3.111 | 0.000 | 0.000 | 0.000 | 2.995 | 1.527 |
| 1ms 刷日志间隔 | 2pc | 4686.450 | 0.373 | 4.291 | 1.125 | 1.290 | 0.066 | 2.988 | 1.567 |
| 1ms 刷日志间隔 | 2pc-cornus | 4923.850 | 0.432 | 5.072 | 2.346 | 0.000 | 0.085 | 1.916 | 1.552 |
| 1ms 刷日志间隔 | 2pc-cornus-opt | 4147.050 | 0.432 | 6.097 | 2.773 | 0.000 | 0.140 | 1.501 | 1.520 |

1 ms 核验没有改变“Lazy 在该 YCSB 条件下领先、存储通知未显示收益”的结论。
普通 Cornus 为 4923.85 TPS，处于两次 3 ms 运行的 4393.50 与 5653.10 之间，
每次刷盘合并 LogWrite 数也处于中间（1.92）；这不能证明仅调短刷新间隔就能解决波动。
基线 2PC 的 commit/尝试从约 5.21 降到 4.29 ms，但回滚率从 6.36% 升至 18.30%，
TPS 没有提高（4777.20 → 4686.45）。阶段平均值必须与回滚率、成功吞吐共同解释。
敏感性组完成后，开发目录的原始 JSON 已原样恢复，基础刷日志间隔仍为 3 ms。

服务器结果目录（均位于 /data/workspace/Hybrid_Cloud_MP/result）：

- `protocol_compare_20261104_t10`
- `protocol_compare_20261104_t10_flush1`
- `protocol_compare_20261104_t10_reverse`
- `protocol_compare_20261104_t2`

当前记录 32 次已完成运行。原始统计摘要及图：result/protocol_exploration_20261104/metrics.json、phase_breakdown.png / .svg。源码修改记录 protocol_changes.patch。

这些是有明确协议边界的探索性实验，不是统计显著性或完整容错结论。下一步应固定构建配置、加入预热并重复少量代表点，优先解释组批/排队如何改变 Prepare 和单写事务日志等待，再评价存储通知增益。

### 2026-11-04 AutoPL 同配置复测

用户将存储节点云盘从 PL0 改为 AutoPL，并报告预配置 IOPS 为 6200。本次在 15:25:40–15:40:53（UTC+8）复跑上一轮主矩阵，共 24 次，72 份计算节点结果全部完整，计时覆盖率均为 100%。云盘规格来自用户报告，本次未通过云控制台 API 独立核验。

保持三计算节点、原二进制（Debug / -O0）、20 秒运行、无预热、日志开启、NO_WAIT、local_ratio=0.5、wr0.8、刷日志间隔 3 ms、批量触发 16、通知阈值 4 和存储 group commit 150 us。2 线程测试 θ=0.6；10 线程测试 θ=0.6/0.95；YCSB、SmallBank 均按 lazy → 2pc → 2pc-cornus → 2pc-cornus-opt 顺序运行。未复跑上一轮反序组与 1 ms 刷日志敏感性组。

运行前后基础 JSON 均与上一轮一致；各模式三个节点的关键生效参数与旧记录逐项一致。使用现有二进制，不重新部署或编译，运行前后二进制哈希一致。清理 ._* 后源码目录指纹与旧记录不同；协议三个 C++ 文件的 Git patch SHA256 与上一轮保存的 protocol_changes.patch 完全一致，且其余受版本管理的核心代码无修改。详见 environment.json。

时间口径：Execute / Commit 是累计阶段时间除以完整尝试数（含回滚），单位 ms；fdatasync 是存储侧实际同步调用平均时间，单位 ms；TPS 是三个节点测量窗口成功 TPS 之和。箭头表示 PL0 → AutoPL；每个条件各一次短测，不作统计显著性判断。

| 负载 / 线程 / θ | 模式 | Execute ms | Commit ms | fdatasync ms | Commit TPS | 回滚率 % |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| ycsb / 2 / 0.6 | lazy | 0.823 → 0.759 | 3.799 → 2.119 | 1.419 → 0.560 | 1283.0 → 2053.8 | 0.26 → 0.25 |
| ycsb / 2 / 0.6 | 2pc | 0.397 → 0.359 | 4.714 → 2.484 | 1.411 → 0.597 | 1145.6 → 2049.0 | 1.17 → 1.26 |
| ycsb / 2 / 0.6 | 2pc-cornus | 0.370 → 0.355 | 3.138 → 1.681 | 1.435 → 0.649 | 1661.8 → 2862.4 | 1.11 → 0.80 |
| ycsb / 2 / 0.6 | 2pc-cornus-opt | 0.382 → 0.360 | 3.194 → 1.703 | 1.443 → 0.633 | 1639.7 → 2825.2 | 0.76 → 0.79 |
| smallbank / 2 / 0.6 | lazy | 0.218 → 0.199 | 3.122 → 1.864 | 1.273 → 0.454 | 1792.1 → 2897.8 | 0.00 → 0.00 |
| smallbank / 2 / 0.6 | 2pc | 0.159 → 0.135 | 3.676 → 2.142 | 1.322 → 0.441 | 1561.1 → 2627.2 | 0.01 → 0.01 |
| smallbank / 2 / 0.6 | 2pc-cornus | 0.146 → 0.125 | 2.812 → 1.556 | 1.324 → 0.451 | 2023.2 → 3556.8 | 0.00 → 0.01 |
| smallbank / 2 / 0.6 | 2pc-cornus-opt | 0.146 → 0.125 | 2.837 → 1.590 | 1.330 → 0.467 | 2006.0 → 3484.4 | 0.01 → 0.01 |
| ycsb / 10 / 0.6 | lazy | 1.530 → 1.255 | 3.143 → 1.895 | 1.539 → 0.772 | 5908.9 → 8827.3 | 4.16 → 3.75 |
| ycsb / 10 / 0.6 | 2pc | 0.413 → 0.378 | 5.209 → 2.822 | 1.557 → 0.803 | 4777.2 → 7506.8 | 6.36 → 13.25 |
| ycsb / 10 / 0.6 | 2pc-cornus | 0.436 → 0.449 | 5.711 → 2.767 | 1.510 → 0.781 | 4393.5 → 8428.1 | 5.12 → 4.88 |
| ycsb / 10 / 0.6 | 2pc-cornus-opt | 0.431 → 0.406 | 6.054 → 3.371 | 1.511 → 0.766 | 4168.1 → 7152.5 | 5.11 → 5.10 |
| ycsb / 10 / 0.95 | lazy | 7.134 → 3.924 | 2.329 → 1.194 | 1.403 → 0.587 | 1077.3 → 2041.1 | 57.75 → 57.36 |
| ycsb / 10 / 0.95 | 2pc | 0.299 → 0.289 | 2.176 → 1.260 | 1.521 → 0.757 | 2837.2 → 4665.1 | 58.49 → 58.71 |
| ycsb / 10 / 0.95 | 2pc-cornus | 0.324 → 0.316 | 1.832 → 1.030 | 1.604 → 0.784 | 3728.2 → 6113.2 | 50.91 → 51.40 |
| ycsb / 10 / 0.95 | 2pc-cornus-opt | 0.328 → 0.320 | 2.168 → 1.206 | 1.546 → 0.801 | 3519.2 → 5930.6 | 48.23 → 48.48 |
| smallbank / 10 / 0.6 | lazy | 0.236 → 0.217 | 2.472 → 1.057 | 1.386 → 0.495 | 11024.0 → 23343.4 | 0.01 → 0.00 |
| smallbank / 10 / 0.6 | 2pc | 0.129 → 0.117 | 3.418 → 1.558 | 1.387 → 0.490 | 8429.9 → 17811.5 | 0.04 → 0.04 |
| smallbank / 10 / 0.6 | 2pc-cornus | 0.131 → 0.121 | 2.588 → 1.219 | 1.389 → 0.516 | 10990.8 → 22244.7 | 0.03 → 0.03 |
| smallbank / 10 / 0.6 | 2pc-cornus-opt | 0.130 → 0.122 | 2.657 → 1.304 | 1.391 → 0.531 | 10724.3 → 20898.9 | 0.02 → 0.03 |
| smallbank / 10 / 0.95 | lazy | 1.755 → 0.930 | 3.552 → 1.581 | 1.389 → 0.467 | 5461.3 → 11510.3 | 1.88 → 1.87 |
| smallbank / 10 / 0.95 | 2pc | 0.125 → 0.113 | 3.335 → 1.527 | 1.396 → 0.497 | 8307.7 → 17470.0 | 2.48 → 2.58 |
| smallbank / 10 / 0.95 | 2pc-cornus | 0.127 → 0.118 | 2.530 → 1.216 | 1.388 → 0.531 | 10888.2 → 21641.7 | 1.85 → 1.89 |
| smallbank / 10 / 0.95 | 2pc-cornus-opt | 0.127 → 0.118 | 2.654 → 1.274 | 1.424 → 0.529 | 10418.8 → 20792.7 | 1.77 → 1.71 |

结果解释：

- 所有 24 个点平均 fdatasync 延迟下降 48.2%–66.7%，提交时间/尝试下降 39.7%–57.2%，成功 TPS 上升 49.4%–111.7%。这支持持久化路径是上一轮的重要性能因素，但不能仅据此断言旧盘具体是 IOPS 还是带宽限流。
- 多数 2PC 执行阶段变化较小；强热点 Lazy 执行阶段明显改善。YCSB θ=0.95、10 线程 Execute 7.134 → 3.924 ms，SmallBank 1.755 → 0.930 ms。名为 wait_log_flush_push_page_time 的页面交接混合计时分别从 1.910 → 0.964、0.423 → 0.191 ms/尝试；后续代码审查发现它还包含接收页面及远端锁等待，不能作为纯 WAL 等待证据。这些量也不能与主阶段相加。
- YCSB θ=0.6、10 线程，2PC Prepare 1.542 → 0.755 ms，Backup 1.775 → 0.912 ms；执行 0.413 → 0.378 ms。持久化优化主要落在提交路径。该点回滚率也从 6.36% 升至 13.25%，因此阶段均值下降包含尝试组成变化；累计 worker 时间/成功提交仍由 6.278 降到 3.994 ms，成功 TPS 4777.2 → 7506.8。
- 升级后仍有不同优势区域：2 线程两负载普通 Cornus 最快；10 线程 θ=0.6 两负载 Lazy 最快；10 线程 θ=0.95 两负载普通 Cornus 最快。SmallBank 中等热点旧盘 Lazy/Cornus 基本持平，新盘本轮 Lazy 领先约 4.9%，单次结果不足以判断这种小差异是否稳定。
- 六组条件中，存储通知版仍均未超过普通 Cornus。YCSB θ=0.6、10 线程，普通 Cornus 每次 fdatasync 合并的 LogWrite 由 1.606 变成 2.028，通知版约 1.500 → 1.507；两者本轮 fdatasync 分别为 0.781/0.766 ms，但 Prepare 为 1.288/1.575 ms、Commit 为 2.767/3.371 ms。盘速不能单独解释通知版的剩余差距，需继续分析日志批次形成、排队和通知后的提交等待。普通 Cornus 之前反序复测已有波动，因此其本次涨幅不能全部归为磁盘变化。

存储节点监控：iostat 每 2 秒采样，设备为 /dev/nvme0n1（项目位于其 ext4 根分区）。每个模式按三节点 TPS 时间戳的共同内部窗口筛选，排除初始化/收集区间，取得 8 个完整采样区间。各模式平均写吞吐约 22.7–149.7 MiB/s，块设备写请求约 3183–5036 次/s，平均队列长度约 0.48–0.94。%util 约 98%–100%；这表示该采样窗口持续有 I/O 活动，不能单独证明 NVMe/云盘已达到吞吐上限。块设备请求次数、fdatasync 次数、LogWrite RPC 数与云盘规格的 IOPS 不是同一口径。旧轮没有同口径 iostat，因此本轮无法给出 IOPS/带宽限流的前后归因。每模式的窗口与均值/最大值保存在 comparison.json。

下一步优先固定 AutoPL 环境，在 YCSB 10 线程 θ=0.6 上重复普通 Cornus 与通知版，分析 compute 端批次形成时间、storage RPC/落盘等待和 AllPrepared 后至最终 Commit 的等待。先解释合批差异，再决定是否修改协议；不要继续用单次吞吐差直接判断通知机制优劣。两种 Cornus 仍仅验证正常路径，故障恢复边界同上。

结果保存在本地及 Ali-hybrid 的对应 result/ 目录：

- protocol_compare_autopl_20261104_t2：8 次低并发原始统计。
- protocol_compare_autopl_20261104_t10：16 次高并发原始统计。
- autopl_comparison_20261104：metrics.json、comparison.json、environment.json、iostat.txt、两张对比图及 SVG。

![执行与提交时间对比](../result/autopl_comparison_20261104/phase_comparison.png)

![日志落盘延迟对比](../result/autopl_comparison_20261104/fdatasync_comparison.png)

### 2026-11-04 日志刷新语义审查（只审查，未改运行代码）

检查范围：Lazy/2PC 单写节点结束日志、2PC Prepare/Backup、多写节点 Prepared Commit、计算侧日志线程、存储 group commit，以及页面交接的刷新请求。对照本地 PostgreSQL 16.2 的 XLogFlush 与 PostgreSQL 16 官方文档。此次未运行新实验、未改 C++ 或配置。

发现与证据：

1. **所有模式共用刷新实现，但同步需求没有独立的立即推进语义。** TxCommitOver → wait_log_flush、Prepare handler → wait_log_flush、Backup → wait_log_flush 都进入 compute_server/server.h 的相同刷新队列和 LogFlush。wait_log_flush 调用 NotifyLogFlush 后睡在 persist_lsn_cond；NotifyLogFlush 只有累计通知达到阈值才 signal。实验配置阈值为 4（代码默认值为 3），定时周期为 3 ms，队列达到 16 条也贡献通知。当待同步日志已经入队而通知未达阈值时，事务仍可能等待后台定时器。一次通知并不必然使刷新线程运行；不同日志条数、请求到达顺序和参与者并发会产生不同等待。定位：server.h:1652、2033、2171。

2. **Lazy 的异步推页路径还有另一种等待方式。** compute_server/server.cc 的 PushPageToOther 在 push_page_with_scheduler 开启时，把 wait_lsn 挂到推页调度器，未在该入队点向日志刷新线程请求推进；调度器靠 LogFlush 完成后 markPushReady 才放行。直接推页分支则调用 wait_log_flush(Page*)。因此统一刷新逻辑时，应覆盖“异步任务等待 WAL”的调用方，不能只改阻塞式提交等待。定位：server.cc:619–659、core/fiber/scheduler.cc:190。

3. **存储层也主动延迟，且 urgent 字段没有形成有效通路。** LogWrite 始终进入 append_and_group_sync；leader 默认 usleep(150 us) 后才 fdatasync。这才更接近有意的 group commit delay，默认值来自 log_manager.h:47，也可由 LOG_GROUP_COMMIT_WAIT_US 改写。计算端 LogFlush 固定 set_urgent(0)，存储 LogWrite handler 不读取该字段。不能只把 urgent 改为 1 就认为实现了立即同步。

4. **普通日志 RPC 失败仍可能被确认持久化。** server.h:1944–1962 对含 PREPARED 的失败批次 fail-fast；普通批次仅记录错误，随后仍按 max_lsn 更新 persist_lsn 并通知等待者。storage_ok 目前只保护在线成本采样，没有保护持久化水位。该差异违反统一的持久化确认语义；未做失败注入，不能说本轮实验触发过它。

5. **先分配 LSN 后入队，会使 max_lsn 水位越过尚未入队的记录。** Lazy/单写路径 AddUpdateLog(generate_next=true) 在 log_mtx 下预留结束日志 LSN，但 TxCommitOver 稍后才把该记录入队；Backup/Abort 也有先 generate_next_llsn_with_lock 再 AddToLog 的调用。一个可行的交错是：A 预留结束记录 L，B 入队 L+1，后台刷出 B 并把 persist_lsn 置为 L+1，A 入队结束记录并等待 L 时直接通过。该结论来自锁和调用顺序审查，未做并发交错实跑复现。已有 AddMetadataLog 把分配与入队置于同一 log_mtx 临界区，可作为统一修正的基础；不能把“批次最大 LSN”无条件等同于“所有先前已分配记录均持久化”。定位：server.cc:953–963、dtx_exe.cc:689–707、dtx_log.cc:27–45、server.h:1957–1962/2005。

6. **诊断计时名称与内容不一致。** wait_log_flush_push_page_time 除 wait_log_flush(Page*) 外，还累计 lazyrelease_server.cc:102、116、251、277 的 TryGetPushData / TryRemoteLockSuccess 等接收页面及远端锁等待。此前 1.910 → 0.964 ms 等数值是混合的页面交接等待诊断量，不能解释为纯 WAL 等待。主 Execute/Commit 计时和存储 fdatasync 计时不因此失效。应在分析中纠正口径，后续只补必要的纯日志队列/持久化分段计时，避免继续用该混合指标归因。

PostgreSQL 对照：

- 同步提交调用 XLogFlush(target LSN)。它先检查已持久化位置；未满足则等待 WAL 写锁或由自己推进写入与同步；取得锁后复查，可能由前一个执行者顺带完成。写出前还等待目标范围内的日志插入完成。它不依赖后台 WAL writer 周期。
- wal_writer_delay 是后台 WAL writer 的调度周期；commit_delay 是刷盘前主动合批的可选延迟，默认 0，设置为正时还受 commit_siblings 等条件约束。两个参数不能混用。
- commit_delay=0 仍可以自然 group commit：一次 I/O 期间到达的其他请求可以被后续批次覆盖，或已经被同一次刷新覆盖，不必为每个事务单独执行一次 fsync。

来源：[PG 16 WAL 参数](https://www.postgresql.org/docs/16/runtime-config-wal.html)、[PG 16 group commit 说明](https://www.postgresql.org/docs/16/wal-configuration.html)、[官方 XLogFlush 源码](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/access/transam/xlog.c)。本地源码：/Users/hcy/D/postgresql-16.2-fastlog/src/backend/access/transam/xlog.c:2555。

建议修改边界（尚未实施）：

- 保留一个公共日志队列和后台刷新线程，后台周期只处理没有紧急持久化需求的日志。
- Commit、Prepare、Backup、Abort 及等待 WAL 的页面交接统一登记请求的目标 LSN，并立即唤醒刷新线程；这类请求绕过通知计数/批量阈值。
- 同步请求应作为受锁保护的持久状态保留，不能只 signal 一下；当前 I/O 未覆盖目标时，完成后应立即处理下一批，不能再次等 3 ms。已覆盖目标则复用结果返回；保持合批而不是每事务独立发一个 RPC。
- 先统一 LSN 分配与入队、成功持久化确认的规则。RPC 失败时不得推进持久化水位或把任务标为 ready。
- 无人为延迟的对照应把存储 group commit delay 设为 0；自然合批保留。不要把后台 interval 简单设为 0，以免空闲忙轮询，也不要将通知阈值调成 1 当作完整语义修复。
- 修复后先在服务器复测 YCSB θ=0.6 的 2/10 线程代表点。记录纯入队至发出、LogWrite RPC、存储 fdatasync 和端到端同步等待；有时间时再做短故障/并发交错验证。无需为此新增一批本地模拟测试文件。

审查结论：现有代码已经支持“相同协议外的刷新调度差异可能污染提交成本比较”的判断；尚不能量化这些问题解释了 Lazy/Cornus 差额的多少，也不能预先保证统一刷新后 Lazy 必然更快。


### 2026-11-04 统一日志持久化服务（实现与服务器验证）

本节接续上述审查。没有引入 ELOG/FastLog 的提前日志生成或执行—持久化重叠；只复用目标 LSN 请求、专用刷新线程、自然合批和成功后发布持久化水位的设计。各协议仍在原有位置生成日志、等待自己的证据。

实现边界：

- `wait_log_flush(target)` 和异步推页等待均调用 `RequestLogFlush(target)`。请求以最大目标 LSN 保存在 `persist_lsn_mtx` 下，通知刷新线程后仍保留；正在进行的 RPC 未覆盖它时，下一轮立即继续刷新。已覆盖的请求直接复用持久化水位。
- 3 ms 后台周期和 16 条批量触发继续用于后台工作，同步请求绕过这两个条件。删除 `log_flush_notify_threshold`，不再累计若干次通知才唤醒。没有新增配置项。
- 更新日志与元数据日志均在日志互斥锁内完成 LSN 分配和入队。Lazy/单写节点结束日志不再提前预留 LSN；Prepare、Backup、Abort 和参与者结束记录使用同一个 `AddMetadataLog`。日志可以因页面 LSN 推进而跳号，但不会用较大水位越过一条尚未入队的本节点记录。
- 所有 `LogWrite` RPC 成功后才推进 `persist_lsn`、唤醒等待者和放行推页；普通日志与 Prepare 不再有不同的错误确认行为。错误时终止进程，不把错误伪装为持久化成功。
- 存储端沿用同一个 append/group-sync 服务，处理部分写入和 EINTR，完整写入后才登记序号；`fdatasync` 成功后才发布 durable 序号。默认人为合批等待由 150 us 改为 0，仍保留自然合批及已有环境变量。删除无效 `urgent` 字段，保留其 protobuf 字段号。
- 关闭日志时不提交持久化请求，也不让异步推页等待无法推进的 WAL 水位。该模式仅用于已有的 Lazy/基础 2PC 无日志比较；Cornus 仍要求日志开启。

协议区别没有消除：Lazy 等待自己的结束记录；基础 2PC 保留 Prepare/Backup；Cornus 跳过 Backup；通知版仍走存储持久投票通知。Prepared Commit 的参与者结束日志仍沿用原有不等待方式，没有人为追加一个持久化阶段。共享的服务承诺是“等待的目标已真实持久化”，并不要求四种模式每事务产生同样条数、同样字节数或同样轮数的日志。

真实服务器验证：Ali-hybrid (`172.16.219.124`) 为 storage_pool + remote_node，`.121/.122/.123` 为计算节点；沿用 AutoPL、Debug 构建、日志开启、NO_WAIT 和每节点单协程配置。

- 四节点真实环境编译通过。常规矩阵 24/24 次运行完成：YCSB/SmallBank，2 线程 θ=0.6，以及 10 线程 θ=0.6/0.95，各自四模式、每节点 20 秒。三节点统计覆盖率均为 100%。
- 每次运行后扫描存储端实际 LOG_FILE，跳过 16 字节文件头，校验记录边界、节点编号和每节点严格递增的 LSN。24 次合计 38,662,725 条日志全部通过；这是实际并发运行的检查，不等于对所有线程交错的形式证明。
- 将后台周期临时改为 5000 ms、批量触发设为 100,000,000，四模式各跑 8 秒，均正常完成且实际日志顺序检查通过。用于验证主动请求能绕过定时/批量条件，配置随后恢复。
- 上述 24+4 次源代码指纹为 `409965873da8558723d00586a29d9a0a641df075b8f1ef65870a4e957daf9396`。之后补了关闭日志时异步推页的两处 `IsLogEnabled()` 判断，并根据故障实测将错误分支改为显式终止。正常日志开启路径的调度与持久化顺序不变；最终源码指纹与补充验证单独记录。

低并发结果（每节点 2 线程、θ=0.6、20 秒；执行/提交均为累计阶段时间除以事务尝试数）：

| 负载 | 模式 | Execute ms | Commit ms | fdatasync ms/次 | Commit TPS |
| --- | --- | ---: | ---: | ---: | ---: |
| YCSB | Lazy | 0.797 | 1.550 | 0.499 | 2500.9 |
| YCSB | 2PC | 0.372 | 2.558 | 0.495 | 1971.2 |
| YCSB | Cornus | 0.366 | 1.976 | 0.542 | 2487.1 |
| YCSB | Cornus-opt | 0.367 | 1.988 | 0.542 | 2464.2 |
| SmallBank | Lazy | 0.190 | 1.045 | 0.384 | 4831.9 |
| SmallBank | 2PC | 0.131 | 1.328 | 0.380 | 4095.9 |
| SmallBank | Cornus | 0.126 | 1.002 | 0.378 | 5290.4 |
| SmallBank | Cornus-opt | 0.127 | 1.002 | 0.375 | 5284.4 |

与上一轮同盘配置相比，YCSB Lazy Commit 为 2.119 → 1.550 ms，Cornus 为 1.681 → 1.976 ms。本轮 YCSB 中 Lazy 执行较慢、提交较快；SmallBank 中 Lazy 与 Cornus 提交接近。结果支持刷新服务会影响协议成本比较，但修改同时包括唤醒、LSN/错误确认修正和去除存储人为等待，且只有一次矩阵运行，不能拆分其独立贡献或据此断言协议必然排名。fdatasync 是每次同步调用的均值，与每事务提交时间口径不同，不应直接相减推导网络/协议时间。

原始结果目录为 `result/protocol_compare_unified_20261104_t2`、`result/protocol_compare_unified_20261104_t10`；主动请求验证为 `result/unified_log_request_only_20261104`。汇总在 `result/unified_log_validation_20261104/metrics.json`，每模式包含 `durable_order_audit.json`。

该修改没有补齐 Cornus 的 LogOnce、崩溃恢复和接管协议；故障处理目标是拒绝错误持久化确认，不能解释为故障后继续提交或完整恢复能力。RAFT 关闭的真实测试也不能替代复制/共识路径验证。

故障验证中间发现：第一次注入第 500 次 WAL `fdatasync` 返回 EIO，存储打印了 `Check failed ... WAL fdatasync failed`，但计算节点仍正常退出，计划被报告完成。该真实结果说明本环境不能用 `CHECK` 日志本身保证停止执行。于是三个持久化错误分支（WAL write、fdatasync、计算节点 LogWrite RPC）改为明确记录错误并 `std::abort()`，确保不能继续发布水位。原始失败验证保留在服务器 `result/unified_log_fault_20261104_lazy_sync_check_only`；它是发现问题的证据，不计入通过测试。没有为此增加生产配置开关或仓库模拟测试文件。

最终验证完成：

- 两处关闭日志推页判断补齐后，Lazy/基础 2PC 的无日志短测 2/2 通过（`unified_log_disabled_20261104`）；四模式的 5 秒后台周期复测 4/4 通过（`unified_log_final_20261104`）。
- 显式错误终止修正后的四模式正常短测 4/4 通过（`unified_log_verified_20261104`），三节点统计覆盖率均为 100%。
- 四模式分别注入第 500 次 WAL fdatasync 返回 EIO，另对 Lazy 注入第 500 次 WAL write 返回 EIO。5/5 均观察到存储错误、计算节点 `LogFlush failed` 和运行失败；错误不再被报告成成功。注入只作用于本次 storage_pool 进程的 LOG_FILE，没有修改生产故障配置。原始 case 状态保留 failed，`fault_verdicts.json` 标记这是预期失败通过；不把 failed 状态伪改为 complete。
- 故障验证只说明失败批次不能沿原路径被确认持久化；已经完成持久化的事务仍可能在故障后短时间内收尾。这不是“故障发生后任何事务都不能完成”的测试，也不是崩溃恢复测试。
- 最终源码指纹为 `879927a372f236d6eeddb0c3e41eeabdedcc0c27c1266b8b1d347148510adf1e`。本地、开发目录及四部署目录的 9 个本次 C++/proto 文件哈希一致；三个计算节点二进制一致。`final_deployment.json` 保存具体哈希。所有测试进程已结束，节点配置恢复 generate_log=1、后台周期 3 ms、批量触发 16 和原运行时间配置。

完整验证证据在 `result/unified_log_validation_20261104`：`metrics.json` 为 24 次常规矩阵；`request_and_final_metrics.json` 为主动请求及最终正常短测；`fault_verdicts.json` 为五项预期失败验证；`final_deployment.json` 为最终部署核验。原始统计与故障记录均同步到本地 result 对应目录。以上没有新增仓库测试脚本，也未提交或推送 Git。


### 2026-11-04 每节点独立 WAL 与跨文件回放

本节接续上一节统一刷新服务，进一步将共享单文件改为共享存储上的每节点独立文件。共享可访问不要求共同 append 同一个文件。

设计参考 [PolarDB-MP 论文 §4.5（LLSN 与恢复）](https://zhangyingqiang.com/paper/PolarDB_Multi_Primary.pdf)：同一页面的日志必须遵守生成顺序，不同页面不要求全局总序。本实现沿用项目已有的节点 LLSN 和页面 prev_lsn，采用按页面前驱链调度；没有照搬论文的 LLSN_bound 分块归并算法。

当前实现：

- 计算侧仍按原位置产生日志、分配 LLSN、合批并请求持久化，各协议等待条件不变。没有引入提前日志生成。
- 存储按记录头中的生产节点路由到 `LOG_FILE_0`、`LOG_FILE_1`、`LOG_FILE_2`。一次 RPC 只能包含同一生产节点的完整记录。文件使用独立 fd 和互斥锁，各自 append → fdatasync → 回复成功；创建文件时同步目录项。
- 不再跨节点合并 fdatasync，不再维护共享文件的 written_seq/durable_seq 或人为合批等待。`LOG_GROUP_COMMIT_WAIT_US` 已移除。计算侧 3 ms 后台周期、16 条触发和目标 LSN 主动唤醒保留。原报告中的 LogWrite/fdatasync 合批比例列保留，当前应为 1；这不等于一次同步调用只产生一次设备 I/O。
- 只有成功 fdatasync 后，存储才向回放器公布该节点文件的可读范围。回放器逐流读取完整记录，以 `(表名, 页号)` 为页面标识：`prev_lsn == page.LLSN` 时应用；前驱尚未应用时排队；前驱到达后继续唤醒同页链条；已被页面 LLSN 覆盖的记录按幂等 redo 跳过。不同表相同页号不能共用等待队列。
- UPDATE、LOCK、INSERT、DELETE 共用该排序入口；Prepare/事务结束等元数据保留在自己的 WAL 中，不作为页面修改应用。修正 DELETE 回放后的页面记录数。
- 启动时扫描所有节点文件的完整记录前缀，从头重放，删除原先“启动时直接认为文件末尾之前已回放”的逻辑。只截去不完整的最后一条记录；前驱缺失或链条矛盾不能报告恢复成功。存储接受客户端请求前先完成已有日志的页面回放。
- 正常关闭前，计算侧完成最后日志刷新和屏障；存储等待所有已持久化记录回放完成并同步修改过的数据文件，再输出统计和回复关闭成功。缺失前驱或关闭失败时实验失败。
- 当前不保存可跨崩溃信任的回放 checkpoint，因此重启会重新扫描保留的 WAL，依靠页面 LLSN 幂等处理。旧实现每应用一条记录就两次写入共享 WAL 文件头的行为也已取消；性能变化包含这一部分，不能全部归因于文件拆分。还没有加入日志切段、回收和 checkpoint 优化。新文件是纯日志记录，不再包含旧单文件的 16 字节回放进度头；测试脚本启动新 case 时清理旧单文件和本轮节点文件，历史 result 目录不动。

验证方法：扩展已有 `tests/test_cases/prepared_2pc_test.cc`，在服务器真实存储代码上验证跨文件乱序、不同表相同页号隔离、UPDATE/LOCK/DELETE/INSERT 链、跨读取缓冲区边界、从初始数据完整重放、重复重启、部分尾记录及缺失前驱的失败处理；原 `prepare_notification_test` 同时验证持久投票通知路径。没有增加一批本机 mock 测试文件。

真实负载的每次实验结束后另用独立解析器扫描三个 WAL：检查文件归属及每节点 LLSN 递增；按页面 LLSN 排列后检查 prev_lsn 连续性；检查落地页面的最终 LLSN/pre_LLSN；根据每个元组最后的 UPDATE 与 LOCK 日志重建预期字节并与数据文件逐字节核对。还核对存储已持久化字节、已读取字节、日志记录数、已应用记录数及 pending=0。该解析器只用于服务器实验验证，放在 /tmp，不作为新的仓库运行入口。

源码指纹：`2c1a2c9809448971063fd90d62cf2429e1c9022b4a8c2bc883336112ebc121f0`。单文件对照为上一节 `protocol_compare_unified_20261104_t2/t10`。本轮四节点构建及上述原生测试通过；8 次短测及 24 次同配置比较矩阵的实际核验记录保存在各模式 `replay_audit.json`。性能表与后续恢复验证见本节下文。

本轮 SSH 初次启动曾被开发节点公网的持续失败登录占用未认证连接影响，两个启动尝试失败。之后临时使用仅监听 127.0.0.1 的 SSH 管理端口供该机脚本连接自身，集群数据 IP、RPC 路径和负载参数未变；实验结束后移除。此措施不代表消除了公网后台活动对机器的所有影响，因此性能仍按单轮比较解释。

这里验证的是多日志流的物理 redo 与页面依赖正确性，包括已有数据上的幂等重放；不把它描述为完整的事务恢复。Cornus 决议恢复、未提交事务 undo、撕裂页面修复等原有未实现部分不由文件拆分补齐。现有 SQL NEWPAGE 路径原本即禁用，本轮未将其改成可用功能。

同配置矩阵结果（均为 20 秒、3 个计算节点、写比例 0.8、本地比例 0.5；执行/提交为每次事务尝试的阶段均值，TPS 为三节点提交 TPS 之和）：

| 负载 | 线程/θ | 模式 | Execute ms 前→后 | Commit ms 前→后 | fdatasync ms/次 前→后 | TPS 前→后 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| ycsb | 2/0.6 | Lazy | 0.797→0.790 | 1.550→1.402 | 0.499→0.644 | 2501→2674 |
| ycsb | 2/0.6 | 2PC | 0.372→0.380 | 2.558→2.330 | 0.495→0.670 | 1971→2150 |
| ycsb | 2/0.6 | Cornus | 0.366→0.368 | 1.976→1.864 | 0.542→0.746 | 2487→2605 |
| ycsb | 2/0.6 | Cornus-opt | 0.367→0.374 | 1.988→1.882 | 0.542→0.770 | 2464→2578 |
| smallbank | 2/0.6 | Lazy | 0.190→0.190 | 1.045→0.842 | 0.384→0.490 | 4832→5781 |
| smallbank | 2/0.6 | 2PC | 0.131→0.128 | 1.328→1.095 | 0.380→0.465 | 4096→4880 |
| smallbank | 2/0.6 | Cornus | 0.126→0.127 | 1.002→0.847 | 0.378→0.502 | 5290→6126 |
| smallbank | 2/0.6 | Cornus-opt | 0.127→0.125 | 1.002→0.853 | 0.375→0.460 | 5284→6097 |
| ycsb | 10/0.6 | Lazy | 1.244→1.225 | 2.062→1.823 | 0.617→0.855 | 8450→9168 |
| ycsb | 10/0.6 | 2PC | 0.407→0.407 | 3.272→2.940 | 0.653→0.832 | 7249→7961 |
| ycsb | 10/0.6 | Cornus | 0.989→0.421 | 2.510→2.287 | 0.658→0.857 | 7781→9848 |
| ycsb | 10/0.6 | Cornus-opt | 0.925→0.427 | 2.627→2.397 | 0.677→0.828 | 7674→9456 |
| ycsb | 10/0.95 | Lazy | 3.677→3.323 | 1.195→1.101 | 0.496→0.672 | 2139→2328 |
| ycsb | 10/0.95 | 2PC | 0.286→0.283 | 1.297→1.125 | 0.611→0.815 | 4226→4660 |
| ycsb | 10/0.95 | Cornus | 0.302→0.304 | 1.091→0.963 | 0.640→0.823 | 5362→5872 |
| ycsb | 10/0.95 | Cornus-opt | 0.302→0.309 | 1.192→1.006 | 0.683→0.805 | 5182→5826 |
| smallbank | 10/0.6 | Lazy | 0.210→0.212 | 1.276→1.042 | 0.460→0.619 | 20041→23702 |
| smallbank | 10/0.6 | 2PC | 0.115→0.115 | 1.735→1.473 | 0.458→0.564 | 16132→18787 |
| smallbank | 10/0.6 | Cornus | 0.116→0.115 | 1.309→1.199 | 0.460→0.609 | 20920→22690 |
| smallbank | 10/0.6 | Cornus-opt | 0.116→0.114 | 1.320→1.232 | 0.453→0.614 | 20756→22138 |
| smallbank | 10/0.95 | Lazy | 0.760→0.631 | 1.506→1.211 | 0.420→0.504 | 12797→15735 |
| smallbank | 10/0.95 | 2PC | 0.111→0.111 | 1.602→1.411 | 0.439→0.561 | 16699→18801 |
| smallbank | 10/0.95 | Cornus | 0.112→0.112 | 1.242→1.156 | 0.445→0.599 | 21299→22695 |
| smallbank | 10/0.95 | Cornus-opt | 0.111→0.111 | 1.292→1.189 | 0.453→0.630 | 20581→22188 |

24 组的平均提交时间均下降，提交 TPS 均提高；统计覆盖率均为 100%。低并发 YCSB 仍表现为 Lazy 执行较慢、提交较快；低并发 SmallBank 的 Lazy/Cornus 提交现在接近（0.842/0.847 ms）。10 线程 θ=0.6 的 SmallBank 中 Lazy TPS 本轮超过 Cornus，但 θ=0.95 时仍落后；不能从一个点推广协议优劣。

单次 fdatasync 平均耗时不一定下降：独立文件允许不同节点并发同步，去掉了共享文件的排队/合批关系，且回放不再每条记录两次改写 WAL 头。因而不能用 fdatasync 均值直接解释每事务提交均值，也不能把全部收益归为文件拆分。YCSB 10 线程 θ=0.6 的 Cornus 执行时间变化较大，单轮对照无法拆分其原因。完整值与配置一致性核对见 `result/per_node_wal_validation_20261104/metrics.json` 和 `comparison.json`。

恢复入口补充：检测到现存的 `LOG_FILE_<node>` 时保留已有数据文件，跳过 benchmark 数据生成。原 `LoadData` 的页数检查可能因配置或计数差异重新生成文件，不能让该逻辑在 redo 前覆盖恢复基线。没有 WAL 的新 case 仍按原流程准备数据。此修复只改变保留 WAL 的启动分支，24 次性能矩阵使用的正常启动及运行路径未变。

完整恢复验证使用 `per_node_wal_restore_final_20261104`：YCSB、SmallBank × 四模式各跑 6 秒。计算节点启动前复制数据文件；运行结束后把三个 WAL 放到副本的相同相对目录结构中，用实际 `storage_pool` 启动恢复。8/8 的全部业务数据文件 SHA-256 与原运行结束文件一致，之后每组再由实际 LogReplay 重启两次，哈希不变，读取/回放记录数相等且 pending=0。这里验证从运行前副本恢复，而不只是对已经回放完成的数据重放一遍。

首次恢复验证因临时快照目录扁平化而失败：真实日志保存的表名为 `../storage_server/...`，验证目录缺少该布局，打开表返回 ENOENT。保留原失败记录 `per_node_wal_restore_20261104` 和 `post_layout_failure.json`；随后修正临时目录并完整重跑，不将原失败计作通过。验证用辅助程序和脚本留在服务器 `/tmp`，没有新增仓库运行入口或本机 mock 测试。

最终验证与部署核验：

- 8 次短测、24 次同配置矩阵、8 次恢复负载全部成功。逐流独立核对共 47,580,113 条日志记录、4,225,649 次元组最终字节检查，所有运行的页面链、最终页面 LLSN、持久化/读取字节及回放完成状态一致。
- 8 次实际 storage_pool 从运行前副本恢复，以及 16 次随后重复回放均通过整文件哈希核对。现有原生 prepared_2pc_test 与 prepare_notification_test 通过；缺失前驱子进程预期 SIGABRT，不是测试意外崩溃。
- 四模式分别注入 WAL fdatasync 失败，加上 Lazy 的 WAL write 失败，5/5 均观察到注入、计算节点拒绝失败 RPC 和 runner 失败。对应 case 状态保留 failed，预期失败结论见 fault_verdicts.json。
- 最终源码指纹 `95b75dc204e40c0db3bc0434e7eefc323e261514f7d8b08c12547d674968cbb6`，性能矩阵指纹 `2c1a2c9809448971063fd90d62cf2429e1c9022b4a8c2bc883336112ebc121f0`；两者之间仅补了上述存储启动恢复分支。开发目录和四部署目录的八个本轮 C++ 文件哈希一致，三个计算节点二进制一致。最终恢复负载已用新入口实跑。
- 节点配置恢复日志开启、3 ms 后台周期、16 条批量触发及开发目录原运行时间设置。所有压测、存储、远程服务进程已结束。iostat 仅覆盖性能矩阵后半段，不能用它进行完整的逐组磁盘归因。

证据汇总位于 `result/per_node_wal_validation_20261104`：completion_audit.json、deployment_audit.json、replay_tests.log、metrics.json、comparison.json、fault_verdicts.json；各 case 另有 replay_audit.json，恢复组另有 restore_audit.json 与实际启动日志。没有提交或推送 Git。


### 2026-11-05 time_details 父子结构与提交路径计时

本轮仅修改统计与报告：在原 time_details 中展示事务路径、日志等待及重叠诊断，不新增统计表、不修改提交协议和刷盘调度、不改变 summary_human.txt。原始兼容计数 single_txn_count 仍包含只读；新路径计时使用独立 stats_commit_* 字段，不重解释旧数据。

三个真实计算节点构建成功，开发目录和四部署目录的七个代码文件哈希一致，三个计算二进制哈希一致。YCSB / SmallBank × Lazy / 2PC / Cornus / Cornus-opt，2 线程、θ=0.6、8 秒，共 8 次运行、24 份节点结果通过逐节点核验：

- Commit 累计纳秒等于只读、本地单写、远端单写、多写的总和；多写各顺序子阶段不超过多写总时间，差值作为 other。
- 执行时间按进入提交的路径归集，剩余执行时间归入 before_commit_abort；本轮观察到 845 次未进入提交的回滚。
- 四类提交路径均有实际样本：只读 38,483、本地单写 135,935、远端单写 35,426、多写 64,684。2PC 的新路径计数与原有单/多写计数交叉核对一致；Lazy 不产生远端单写或多写提交样本。
- 五类日志等待均有实际调用：Commit 171,361、Prepare 160,114、决议日志 18,746、同步页面日志 67,592、Abort 1,167。各分类累计次数和纳秒数与独立总计精确一致。
- Cornus 两种模式的决议日志子阶段为零。旧的真实结果仍可按原报告口径生成，不伪造缺失的新分类。

结果位于 result/time_details_validation_20261105，各模式有 stats.log 和 time_details_audit.json；validation.json、deployment_audit.json 保存完整核验及源码/二进制证据。短测用于统计正确性验证，不据此判断性能变化。显示值保留三位小数，精确对账使用原始纳秒值。运行时配置已恢复，未新增本地 mock 测试文件。
## eRDMA 接入（2026-11-05）

本轮只替换 bRPC 传输，保留事务协议、页面所有权和日志刷盘语义。运行入口仍是 `test_multi_node.py`，顶部 `use_rdma` 控制所有进程；脚本写入各节点 `compute_node_config.json` 的顶层 `use_rdma`。原 storage JSON 的独立开关已删除，避免计算端与存储端选用不同传输。结果 `config.json`、每模式 `metadata.json` 和 `stats.log` 保存传输选择。

| 角色 | 管理 IP（eth0） | 实验 IP（eth1 / ERI） |
| --- | --- | --- |
| storage_pool + remote_node / 开发机 | 172.16.219.124 | 172.16.219.129 |
| compute 0 | 172.16.219.125 | 172.16.219.131 |
| compute 1 | 172.16.219.126 | 172.16.219.130 |
| compute 2 | 172.16.219.127 | 172.16.219.128 |

四台均为 Ubuntu 22.04、ecs.g9i.2xlarge。新增 ERI 后，在 `/etc/netplan/60-hcmp-erdma.yaml` 为 eth1 启用 DHCP，并为另外三个实验 IP 配置走 eth1 的 /32 路由。辅助网卡不接管默认路由和 DNS。TCP 与 RDMA 对照均使用相同实验 IP；SSH 也可经这些私网地址访问，原 Ali-hybrid 管理入口继续可用。

运行条件：

- eRDMA 驱动 `compat_mode=Y`，配置保存在 `/etc/modprobe.d/hcmp-erdma.conf` 并更新 Ubuntu initramfs。当前四台均有 `erdma_0`、PORT_ACTIVE，GID index 1 为各自 ERI 的 IPv4 地址。
- root 的 memlock 限制设置在 `/etc/security/limits.d/90-hcmp-rdma.conf`。实例间需允许应用端口及 eRDMA Compat 建链端口；本轮没有修改云安全组。
- bRPC 1.9.0 以 `WITH_RDMA=ON` 构建，源码和构建目录为 `/opt/src/brpc-1.9.0`，独立安装目录 `/opt/brpc-1.9.0-rdma`，同时安装到四台 `/usr/local` 供原一键构建使用。旧安装备份 `/opt/hcmp-brpc-before-rdma.tar.gz`。
- 保留原环境 `bthread/condition_variable.h` 的 `wait(lock, predicate)` 模板兼容接口。官方 1.9.0 原版没有这个重载，重装依赖时不能漏掉；没有修改项目锁算法。
- 统一初始化位于 `common/rpc_transport.cc`，设置 `rdma_gid_index=1`、`rdma_max_sge=4`、`rdma_prepared_qp_cnt=0`。使用现有 flag 限制 SGE，无需修改 bRPC 的 GetRdmaMaxSge 源码。若以后换网卡或增加多个 RDMA 设备，应重新核对 GID/设备选择。

覆盖路径：计算节点间 RPC、日志和页面存储 RPC、索引预取、remote_node RPC，以及 Cornus 存储通知。启动元信息和 barrier 的原始 TCP socket 保留；它们不是事务数据 RPC。bRPC 本身也用 TCP 交换 RDMA 建链参数，因此不能用“存在 TCP 连接”判断 RDMA 未生效。

验证证据统一放在开发机 `result/erdma_validation_20261105`。三个计算节点到存储的官方 bRPC 1 KiB 回显测试均成功，双方日志记录 `Handshake ends (use rdma)`。配置里的 `rpc_transport=rdma` 只表示选用 RDMA，仍需检查实际连接是否 ESTABLISHED，不能忽略 bRPC 自动回退 TCP 的可能。

参考：[阿里云 bRPC/eRDMA 接入](https://help.aliyun.com/zh/ecs/user-guide/deploy-brpc-applications-with-high-network-performance-based-on-erdma)、[ERI 配置](https://help.aliyun.com/zh/ecs/user-guide/elastic-rdma-network-interface-card-eri)、[已有网卡属性修改限制](https://help.aliyun.com/zh/ecs/developer-reference/api-ecs-2014-05-26-modifynetworkinterfaceattribute)。已有网卡切换通信模式的 API 当前文档仍标注邀测；本轮采用新增辅助 ERI，不依赖该功能。

本轮验证完成：TCP、RDMA 各 8/8 组（YCSB / SmallBank × Lazy / 2PC / Cornus / Cornus-opt，每节点 2 线程、每组 5 秒）。48 份节点输出、147,159 次事务尝试完成核对：事务尝试/提交/回滚计数一致，四提交路径耗时之和等于总 commit 时间，五类同步日志等待的次数和纳秒值分别等于全局总计。`validation.json` 保存结论和逐组结果。

实际数据库连接采样覆盖四种模式、三个计算服务、storage_pool 和 remote_node：852 次业务 socket 观测均为 `rdma_state=ON`、`handshake_state=ESTABLISHED`，各端均有非零发送 WR 计数。原始证据为 `project_rdma_connections.jsonl`。初始采集因 bRPC 对 Python User-Agent 返回 HTML，没有解析出 socket ID；切换为纯文本后获得上述观测，先前 HTML 快照也保留。这里的 852 是采样次数，不是独立连接数。

脚本默认 `use_rdma = True`；切回 TCP 只需改为 `False`，运行命令仍是 `python3 test_multi_node.py`。本轮短测指定 `--run-seconds 5`，未改默认运行时长；没有新增仓库测试脚本。两种传输使用同一组 ERI IP、同一版支持 RDMA 的 bRPC 和同一组项目二进制。短测只用于功能与统计验证，RDMA 轮还采集了调试接口，不能用其吞吐或时延直接下性能结论。正式对照应关闭连接采样并延长测量。
