# WookongDB MP 
<img src="doc/wookongdb.png" alt="alt text" width="600" />

本项目支持 **SQL 模式** 和 **负载模式**。SQL 在 Lazy 模式下使用事务级快照隔离。显式事务在 `BEGIN` 时固定快照，自动提交语句各自取得快照。普通 `SELECT` 通过 undo 读取可见行，写者按 SI 规则检查冲突。见[实施与验收记录](doc/UNDO_MVCC_IMPLEMENTATION_PROGRESS.md)。

- **SQL 模式**：支持基础 SQL 操作
- **负载模式**：支持 SmallBank、YCSB、TPC-C，以及原生 DTX 版本的 TATP 和 sysbench OLTP。

在 **lazy** 模式下，采用 **日志回放** 机制来确保页面的正确性,
**SQL** 模式目前仅支持 **lazy** 模式运行，其它模式暂不支持

> **注意**：启动时，需确保存储层、元信息层和计算层的启动模式保持一致（例如均为 `sql` 或均为 `ycsb`）。

---

## 多节点实验入口

自动压测统一从根目录的 `test_multi_node.py` 启动，负责生成 case plan、执行和中断续跑。
结果包含 `stats.log`、`comparison.csv` 和各节点原始日志，配置与统计口径见
[实验说明](doc/benchmark_stats.md)。

```bash
# 只生成计划，确认参数后再执行
python3 test_multi_node.py --plan-only --run-mode time --run-seconds 60 --result-dir result/my_plan
python3 test_multi_node.py --resume-result-dir result/my_plan

# 仅重新生成已有结果的报表
python3 -m tools.benchmark.report result/my_plan
```

报表实现位于 `tools/benchmark/report.py`，压测时自动调用。
旧版绘图与专项结果比较脚本已移除；下面的组件启动步骤用于手动运行。

## 启动步骤

请按照以下顺序启动各组件：

### 1. 存储节点 (Storage Node)

```bash
cd ./build/storage_server
./storage_pool [mode]
```

- **参数说明**：
  - `mode`: 运行模式，可选值为 `sql`, `ycsb`, `smallbank`, `tpcc`, `tatp`, `sysbench`。

- **示例**：
  ```bash
  ./storage_pool sql
  ```

### 2. 元信息节点 (Metadata Node)

```bash
cd ./build/remote_server
./remote_node [mode]
```

- **示例**：
  ```bash
  ./remote_node sql
  ```

### 3. 计算节点 (Compute Node)

#### 方式 A：以 SQL 模式启动

在每个计算节点执行：

```bash
cd ./build/compute_server
./compute_server [node_id] [db_name]
```

- **参数说明**：
  1. `node_id`: 节点 ID（从 0 开始）。
  2. `db_name`: 启动的数据库名称(需确保各个节点打开的数据库一致)

- **示例**：
  ```bash
  ./compute_server 0 test_db    (节点 0)
  ./compute_server 1 test_db    (节点 1)
  ```

> SQL 模式的计算层启动后，每个节点会对外暴露一个端口，需使用客户端连接到指定节点输入 SQL 交互
> ```bash
> # 在另一个终端执行
> cd ./build/WookongDB_client
> ./WookongDB_client -h [ip] -p [port]
> # 示例：连接到节点 0
> ./WookongDB_client -h 127.0.0.1 -p 9095   (默认端口从 9095 开始，根据节点号依次递增)
> ```

> **SQL 模式的 YCSB 表初始化**：节点 0 首次启动时会检查 `ycsb_user_table`；不存在时自动创建并插入 1000 行，已存在则跳过。表结构为 `id`（主键）、`magic`、`file_0`…`file_9`（`char(90)`；SQL 行加上 `DataItem` 头受 `RM_MAX_RECORD_SIZE=1080` 限制）。初始化在节点 0 的 SQL 监听器启动后由后台客户端执行，看到日志 `[SQL] initialized ycsb_user_table with 1000 rows` 后再查询；其他节点通过建表通知获得同一张表。已有表不会重复插入。

#### 方式 B：以负载模式启动

在每个计算节点执行：

```bash
cd ./build/compute_server
./compute_server [workload] [mode] [thread_num] [write_txn_ratio] [local_txn_ratio] [machine_id]
```

- **参数说明**：
  1. `workload`: 负载类型，支持 `smallbank`, `ycsb`, `tpcc`, `tatp`, `sysbench`。
  2. `mode`: 页面获取模式，支持 `eager`, `lazy`。
  3. `thread_num`: 本节点的执行线程数量。
  4. `write_txn_ratio`: 写事务比例 (范围 0-1)。
  5. `local_txn_ratio`: 本地事务比例 (范围 0-1)。
  6. `machine_id`: 当前节点 ID（从 0 开始）。

- **示例**：
  ```bash
  # 启动 smallbank 负载，Lazy 模式，3 个线程，80% 只读，80% 本地事务，机器 ID 为 0
  ./compute_server smallbank lazy 3 0.8 0.8 0
  其它节点启动同上
  ```

---

## 配置文件说明

配置文件位于 `{Hybrid_Cloud_MP/config}` 目录下。

### TATP 和 sysbench OLTP

使用 `python3 test_multi_node.py --workloads tatp sysbench` 运行两个新负载，加上 `--dry-run` 可以先查看实验矩阵。也可在脚本顶部调整默认的 `bench_names`。两个负载的 `write_txn_ratio` 都是每次尝试选择含写事务的概率。每笔事务以 `local_txn_ratio` 的概率从当前计算节点的固定归属数据选键，其余尝试允许随机选节点。全本地分支覆盖所有访问，包括 sysbench 的整个范围和索引页。

- **TATP** 使用七种业务事务。写比例取 `0.2` 时，三种读类型的权重为 35、10、35，四种写类型为 2、14、2、2。四张表按订户共置，每个订户在每张表内占一条物理记录。按类型和起始时段编码的预置槽支持呼叫转移的插入、删除、重复和缺失判断；号码映射利用号码与订户 ID 的一一对应关系。`config/tatp_config.json` 控制订户数和键分布。手动运行可选择 `key_distribution=uniform` 或 `nurand`，多节点参数扫描使用 `system` 及其 Zipf 或热点配置。
- **sysbench OLTP** 使用单表原生读事务和读写事务模板。每笔执行 10 次点读和 4 类范围读（投影、求和、排序、去重排序），读写事务还更新 `k`、更新 `c`、按原 ID 替换一行。主表按 ID 顺序装载，每个数据页有一个与之共置的有序二级索引叶，覆盖该页的 `(k,id)`。主表和索引叶在同一事务内更新。替换通过已有记录更新完成。该变体测量 DTX 路径，省去了 SQL 协议、解析和物理删插的开销。`config/sysbench_config.json` 中的 `table_size` 和 `range_size` 分别控制行数和查询范围，默认范围为 100 行。
- **数据规模** 需要让每个计算节点至少拥有一个分区的数据；使用 sysbench 本地事务时，每个节点还需要至少一个容得下完整查询范围的连续 ID 区间。可增大数据集或减小 `partition_size_per_table`。
- **结果** 每个节点的 `result.txt` 包含逐类型的尝试和提交数。`oltp_all_local_attempts` 统计所有目标数据都归发起节点的尝试。`oltp_zero_writer_attempts`、`oltp_single_writer_attempts` 和 `oltp_multi_writer_attempts` 按计划写入的节点数计数。`oltp_business_misses` 是 TATP 因缺行、条件未命中或重复插入而回滚的尝试，已包含在总回滚数中。这些计数均排除预热。

在 AliYun1 编译 `storage_pool`、`remote_node` 和 `compute_server` 后，可以运行三节点正确性验收。结果目录使用尚未创建的路径。

```bash
python3 tests/integration/run_native_oltp.py --result-dir result_oltp/check_01
```

验收通过 `test_multi_node.py` 执行 18 组实验。每组结束时，在临时存储目录回收之前检查 WAL 回放后的数据，并将校验结果写入该组的 `data_verification.txt`。正确性测试使用 32 MB 的每流 WAL 预分配。

### 常见配置修改
- **修改缓冲池大小**：
  编辑 `config/compute_node_config.json`，设置 `"buffer_pool_size": "128MB"` 或 `"1GB"`。这是每个计算节点统一的页面缓存容量，由所有表的数据、索引及 FSM 页面共享；单位按 1024 换算，支持小数。
- **修改分区大小**：
  编辑 `config/compute_node_config.json`，修改 `"partition_size_per_table"` (默认为 100)。

### 修改计算节点数量 (静态配置)
本项目目前**仅支持静态配置**计算节点数量，不支持在运行时动态添加或删除节点。
如果需要变更节点数量（例如从 2 个节点变更为 N 个），必须在**启动集群前**修改以下配置文件：

1. **`compute_node_config.json`**
   - 修改 `"local_compute_node"` -> `"machine_num"` 为 `N`。
   - 在 `"remote_compute_nodes"` -> `"remote_compute_node_ips"` 列表中添加新节点 IP。
   - 在 `"remote_compute_nodes"` -> `"remote_compute_node_port"` 列表中添加新节点端口。

2. **`storage_node_config.json`**
   - 在 `"remote_compute_nodes"` -> `"compute_node_ips"` 列表中添加新节点 IP。
   - 在 `"remote_compute_nodes"` -> `"compute_node_ports"` 列表中添加新节点端口。

3. **`remote_server_config.json`**
   - 在 `"remote_compute_nodes"` -> `"compute_node_ips"` 列表中添加新节点 IP。
   - 在 `"remote_compute_nodes"` -> `"compute_node_ports"` 列表中添加新节点端口。

> **重要**：所有节点上的配置项须保持一致，部分参数，例如 machine_num , thread_num 等，由节点启动时自动配置，无需修改

---

## 支持的 SQL 语法
- **创建表**: `CREATE TABLE table_name (col1 type1, col2 type2, ...);`
  - 支持类型: `INT`, `FLOAT`, `CHAR(n)`
  - 支持主键: `PRIMARY KEY (col)`
  - 示例 1 ：`create table student(id int , age int , name char(50))`
  - 示例 2 ：`create table student(id int , age int , name char(50) , primary key(id))`
- **删除表**: `DROP TABLE table_name;`
- **显示表**: `SHOW TABLES;`
- **查看表结构**: `DESC table_name;`

- **插入**: `INSERT INTO table_name VALUES (val1, val2, ...);`
- **删除**: `DELETE FROM table_name where ...;`
- **更新**: `UPDATE table_name SET col=val where ...;`
- **查询**: `SELECT * FROM table_name where ...;`
- **join**: `SELECT * FROM table1 , table2 where table.col1 = table2.col2; (目前仅支持 2 表 Join 查询)`

- **事务**: `begin , commit , abort , rollback`
---

## 致谢与引用 (Acknowledgements)

本项目基于 **Chimera** 实现。如果您使用了本项目代码，请引用以下论文和仓库：

- **Chimera: Mitigating Ownership Transfers in Multi-Primary Shared-Storage Cloud-Native Databases**  
  *Proceedings of the VLDB Endowment (PVLDB), 2025*  
  GitHub: [https://github.com/HuangDunD/Chimera](https://github.com/HuangDunD/Chimera)
