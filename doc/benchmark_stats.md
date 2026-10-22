# 实验结果与 stats.log

运行入口是 `python3 test_multi_node.py`。日常修改脚本前部的“实验配置”区后直接运行；
类和函数集中在配置区之后。续跑时读取已保存的计划参数。`summary_human.txt` 的 key=value 格式、
字段与旧含义保持不变；新增的 `stats.log` 直接读取各节点原始数据，不沿用旧摘要的平均规则。

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
  run.json                       # 基础配置、源码指纹、计划指纹（去除凭据）
  comparison.csv                 # 每个已完整结束的 case/mode 一行
  case_001-ycsb-t10-theta0.95-lr0.5-wr0.9-skew0.2-topn100-lock1-seconds60/
    lazy/
      metadata.json
      stats.log
      summary_human.txt
      summary_matrix.txt
      node0/result.txt
      node0/tps.log               # 运行时每 2 秒追加 TPS，CSV 格式
      node0/compute.out           # 计算节点完整标准输出/错误
      node0/effective_config.json
      node1/...
      node2/...
      storage/storage_timing_stats.txt
    2pc/...
    mix/...
  case_002-ycsb-t10-theta0.95-lr0.5-wr0.5-skew0.2-topn100-lock1-seconds60/...
  case_013-smallbank-t10-theta0.95-lr0.5-wr0.9-skew0.2-topn100-lock1-seconds60/...
```

模式来自生成计划时的 `modes`，只创建实际执行的模式目录；上图展示可用的组织形式。
完整参数包括节点数和每线程尝试数保存在 case_plan.tsv，不依赖解析目录名。
按时间运行的目录以 `-seconds60` 等结尾；按数量运行则以 `-tx10000` 等结尾。
summary_human.txt 继续使用原来的 key=value 字段结构（其中 round=0 仅为字段兼容），
stats.log 和 comparison.csv 使用 case_id。旧 final_human/final_matrix 的跨参数平均不再生成。
历史结果不移动，tools/benchmark/report.py 仍能读取历史原始结果。
旧版绘图和专项比较脚本已移除，新计划的比较数据统一使用 comparison.csv。

## 按数量或时间运行与 TPS

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
每段使用实际经过的单调时钟时长作分母；末尾另输出不足一个周期的尾段。
committed_tps 为成功提交增量/经过秒数；completed_tps 包括回滚。
控制脚本将采样实时写入结果目录中的 nodeN/tps.log 并刷新文件，不在终端打印节点 TPS。
结束后采集节点完整 tps.log 与 compute.out，最终 TPS 汇总写入 stats.log。

内置负载每个工作线程顺序执行事务，完成当前事务后才发起下一笔，没有预生成的大批事务队列。
time 到期后停止发起新事务，在途事务继续收尾。正式汇总 committed_tps 为
**截止前完成的提交数 / 配置的测量秒数**，截止后的提交只进入 final_commits，
不回填窗口 TPS。stats.log 的 node_throughput 显示测量时长、收尾时长、窗口计数、
最终计数；throughput_summary 的 sum_node_window_tps 为各节点窗口 TPS 之和。
节点之间没有新增同步起跑协议，因此它不是同一集群墙钟窗口的精确吞吐。
实时采样 phase 表示该段结束时处于 measure 还是 drain；跨截止点的采样段可能包含两侧的事务，
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

time_details 的取页、锁等待、Prepare/Backup/Commit、日志等待是主阶段内部的诊断视角，
彼此可能嵌套，还可能包括参与者/后台线程工作。禁止把这些行加到主 breakdown，
或将它们当成同一条串行关键路径。所有字段明确使用秒、毫秒、纳秒或次数。

旧二进制仅有累计阶段计时：可计算每次 issued attempt 的阶段时间；端到端、other、
按执行路径的时间均为 N/A。不可从现有几项凑出 total。
旧 throughput 是各线程吞吐之和，报表明确标注 legacy_sum_worker_tps；
total_time_seconds 是各工作线程运行时长的平均值，不是整个实验墙钟。

## 汇总规则

计数/累计耗时相加；均值用累计耗时除以对应次数；回滚率用总回滚/总尝试；
最大值取最大值。各节点百分位不能平均，故保留节点 P50/P99，集群百分位为 N/A。
缺失字段/存储统计/旧二进制未采集的字段显示 N/A；缺失计算节点时集群数值也显示 N/A。
日志构造次数不代表真正落盘次数，尤其 generate_log=0 时。

run.json 是源配置快照；nodeN/effective_config.json 是命令行覆盖后的远端实际配置。
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
