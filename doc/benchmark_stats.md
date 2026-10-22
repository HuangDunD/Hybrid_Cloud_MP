# 实验结果与 stats.log

运行入口仍是 `python3 test_multi_node.py`。`summary_human.txt` 的 key=value 格式、
字段与旧含义保持不变；新增的 `stats.log` 直接读取各节点原始数据，不沿用旧摘要的平均规则。

## 目录

```text
result/<运行时间>/
  run.json                       # 本轮输入配置、源码指纹，已去除凭据
  comparison.csv                 # 每个组合/模式/重复一行，不混合不同参数取平均
  ycsb/
    case_lr0.5_theta_0.95_wr_0.9_skew_0.2_topn_100_lock1/
      lazy/
        repeat_01/               # 第一次重复；repeat_02 是第二次
          metadata.json
          stats.log
          summary_human.txt
          summary_matrix.txt
          node0/result.txt
          node0/effective_config.json
          node1/...
          node2/...
          storage/storage_timing_stats.txt
      2pc/repeat_01/...
      mix/repeat_01/...
  smallbank/...
  legacy/round_00/ycsb_lazy/<原参数名>/  # 指向上面同一份结果的相对符号链接
```

模式目录只为实际配置的 modes 创建。旧 `round_00` 是从零编号的第一次重复，
`ycsb_lazy` 是负载与模式拼在一起的名字。现在将这两个维度分开。
旧绘图脚本可将 `--result-dir` 指向 `legacy/round_00`，历史目录无需移动。
旧绘图脚本仍不区分同一参数下的多个 lock_mode，使用它时应只扫描一种锁模式；
新 stats.log / comparison.csv 保留该维度，可用于多种锁模式的比较。
旧 final_human/final_matrix 汇总仅为兼容保留；它们跨参数取平均，不用于新分析。

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

## 重生成与续跑

```bash
# 只增加 stats.log / comparison.csv，不改写旧摘要；兼容新旧目录
python3 benchmark_report.py result/<运行时间>
# 预览到另一个目录，原结果零写入
python3 benchmark_report.py result/<运行时间> --output-dir /tmp/hcmp-preview
# 显式续跑，按配置/源码指纹以及重复编号识别完成项
RESUME_FROM=result/<运行时间> python3 test_multi_node.py
```

默认启动新一轮，不再因为另一轮同名参数的摘要存在就跳过。不同线程数、日志设置、
源码、重复编号不能互相当作完成记录。旧结果缺少配置指纹，只支持读取和生成报告，
不自动作为新实验的续跑凭据。SKIP_COMPLETED=0 可在显式续跑目录中重新执行全部组合。
存储统计在本组合启动前清除旧文件，结束后收集；缺失时报告 N/A。

## 验证

```bash
python3 -m unittest discover -s tests/scripts -p 'test_benchmark_*.py' -v
c++ -std=c++17 -I. tests/test_cases/benchmark_stats_test.cc -o /tmp/hcmp-stats-test
/tmp/hcmp-stats-test
```

脚本测试替换所有远端操作，覆盖三种模式、两次重复、旧目录链接、断点续跑及失败重跑。
它们不部署程序、不启动远端实验。真实的新计时结果仍需重新编译后运行负载验证。
