
# Hybrid_Cloud_MP 故障恢复流程详细文档

## 一、正常故障恢复流程（按阶段详述）

### 概述

当系统中某个计算节点（以下称"故障节点"）崩溃时，所有存活的计算节点会**并行**执行 Instance Recovery（IR）流程。整个恢复过程分为 **故障检测 → Phase 1 → Phase 2 → Phase 3 → Phase 4** 五个阶段。

---

### 阶段 0：故障检测与通知

**执行者**：HeartbeatMonitor（运行在 Remote Server 上）

**流程**：

1. HeartbeatMonitor 每隔 **1500ms** 向所有计算节点发送心跳 RPC（超时 1000ms）
2. 某个计算节点连续 **3 次**心跳失败后，被宣告为 DEAD
3. HeartbeatMonitor 并发通知所有**存活计算节点**（`NotifyNodeFailure` RPC）
4. HeartbeatMonitor 并发通知所有**存储节点**（`NotifyNodeFailure` RPC）
5. 存活计算节点收到通知后，调用 `MarkNodeFailed(failed_node_id)` 进入恢复流程

**关键时间点**：
- 从节点真正崩溃到被检测到：最长 `1500ms × 3 = 4.5s`
- 通知传播延迟：约 2ms（RPC 超时 2000ms）

---

### 阶段 1：GPLM 锁清理与页面重分布（Phase 1）

**执行者**：每个存活计算节点独立执行  
**目标**：清理故障节点在全局页面锁管理器（GPLM）中的所有锁状态，并接管其管理的页面

**详细步骤**：

#### 1a. 清理故障节点持有的锁（CleanFailedNodeAndSetIRLock）

遍历本节点管理的所有 GPLM 页面，对每个页面执行 `CleanFailedNodeNoBlock`：

| 故障节点锁状态 | 处理方式 | 是否设 IR 锁 | 原因 |
|---|---|---|---|
| 持有 X 锁 | 移除 holder，lock 重置为 0 | ✅ 设置 IR 锁 | 最新数据可能只在故障节点内存中，需要日志恢复 |
| 持有 S 锁 | 移除 holder，lock-- | ❌ 不设 IR 锁 | 其他 S 锁 holder 仍有有效数据副本 |
| 不是 holder | 无需处理 lock | ❌ 不设 IR 锁 | 故障节点未持有该页面 |

对所有页面统一执行：
- 清空 `request_queue`（存活节点的请求将通过 LPLM wakeup 重试）
- 重置 `s_request_num = 0`、`x_request_num = 0`
- 重置 `src_node_id = INVALID_NODE_ID`
- 重置 `is_pending = false`
- 清理 `GlobalValidInfo` 中故障节点的有效性标记

#### 1b. 接管故障节点管理的页面（页面重分布）

故障节点原来管理的 GPLM 页面需要被存活节点接管：

- 分配算法：`new_owner = surviving[page_id % surviving_count]`
- 本节点接管的页面：
  - 重置锁状态（`Reset()`）
  - 设置 IR 锁（`SetIRLock()`）
  - 标记为 storage-only（`MarkOnluInStorage()`）

#### 1c. 设置 Phase 2 Barrier 期望值

- 调用 `SetIRScanExpected(surviving_count)`
- 初始化 `ir_scan_complete_count_ = 0`、`phase2_complete_ = false`

**Phase 1 完成标志**：日志输出 `Phase 1 complete: X-lock cleaned N pages (IR locked), S-lock cleaned M pages (no IR lock), redistributed K pages to surviving nodes`

---

### 阶段 2：LPLM 扫描与状态汇报（Phase 2）

**执行者**：每个存活计算节点独立执行  
**目标**：扫描本地 LPLM，向新的 GPLM 管理者汇报自己持有的页面状态，重建全局锁视图

**详细步骤**：

#### 2a. 日志刷新（P8 修复）

- 在扫描前先调用 `LogFlush()`，将本节点所有待刷日志发送到存储层
- 确保后续 Phase 4 分析时，存储层拥有完整的日志数据

#### 2b. 遍历 LPLM 汇报页面状态

对每个原来由故障节点管理的页面：
1. 检查本节点 LPLM 中是否持有远程锁（`HasOwnerOrGranting()`）
2. 如果持有，确定新的 GPLM 管理者（`get_recovery_node_id`）
3. 发送 `ReportPageStatus` RPC，包含：
   - `reporter_node_id`：汇报节点 ID
   - `lock_mode`：0=NONE, 1=SHARED, 2=EXCLUSIVE
   - `has_valid_copy`：是否持有有效页面副本

#### 2c. GPLM 侧处理汇报

收到 `ReportPageStatus` 后：
1. 调用 `RecoverAddHolder(reporter, exclusive)` 恢复 holder 列表
2. 如果页面仍有 IR 锁且有存活节点持有有效副本 → **释放 IR 锁**
3. 更新 `GlobalValidInfo`

#### 2d. 发送扫描完成通知

- 向所有存活节点发送 `IRScanComplete` RPC
- 带 **3 次重试**（每次间隔 500ms），防止通知丢失

#### 2e. Barrier 同步

- 每个节点等待收到所有存活节点的 `IRScanComplete`
- 带 **30 秒全局超时**（P6 修复），超时后强制继续
- 所有通知到齐后，调用 `CollectRemainingIRLockedPages` 收集仍持有 IR 锁的页面

**Phase 2 完成标志**：日志输出 `Phase 2 complete: N pages still have IR locks, pending Phase 3 analysis`

---

### 阶段 3：唤醒阻塞线程（Phase 3 - Wakeup）

**执行者**：每个存活计算节点独立执行  
**目标**：中断所有可能阻塞在故障节点相关操作上的线程

**详细步骤**：

#### 3a. 递增恢复纪元

```cpp
recovery_epoch.fetch_add(1);
```

所有 in-flight 事务在执行结束时会检查 epoch 是否变化，若变化则主动 abort。

#### 3b. 唤醒所有 LPLM 等待线程

遍历所有 LPLM 的每个页面锁，调用 `SetRecoveryAbort()`：

- 若 `is_pending = true`：清除 `is_pending`，保留 `remote_mode`（让事务正常释放锁）
- 若 `is_granting = true`：清除 `is_granting`，重置 `lock = 0`、`remote_mode = NONE`
- 设置 `recovery_abort = true`
- 调用 `cv.notify_all()` 唤醒所有等待的线程

**Phase 3 完成标志**：日志输出 `Phase 3: woke up all LPLM waiters`

---

### 阶段 4：日志分析与数据恢复（Phase 4 - Storage Recovery）

**执行者**：每个存活计算节点独立执行  
**目标**：对仍持有 IR 锁的页面，通过存储层日志回放恢复数据正确性

**详细步骤**：

#### 4a. 等待 Phase 2 Barrier 完成

调用 `WaitPhase2AndGetRemainingIRPages()` 获取剩余 IR 锁页面列表（含 table_id、page_id、gplm_lsn）。

#### 4b. 再次日志刷新

调用 `LogFlush()` 确保本节点所有日志已到达存储层。

#### 4c. 分批发送存储层分析请求

- 每批最多 500 个页面
- 发送 `AnalyzeRecoveryPages` RPC 到存储层（30s 超时）

#### 4d. 存储层执行 Redo

对每个页面：
1. 等待该页面上的日志批次完成（最多等 5s）
2. 读取磁盘页面，获取 `disk_lsn`
3. 若 `gplm_lsn == 0` 或 `disk_lsn < gplm_lsn`：
   - 调用 `RedoForPage` 执行定向日志回放
   - 扫描日志文件，收集目标页面的日志条目
   - 按 LSN 排序，逐条应用（UPDATE/INSERT/DELETE）
   - 将回放后的页面写回磁盘
4. 若 `disk_lsn >= gplm_lsn`：页面已是最新，无需回放

#### 4e. 存储层执行 Undo

调用 `UndoForFailedNode(failed_node_id)`：
1. 正向扫描日志，构建事务状态表（有 `BatchEndLogRecord` → 已提交）
2. 反向扫描日志，对故障节点的未提交事务执行 Undo
3. Undo 操作：UPDATE 恢复 old_value，INSERT 清除 bitmap，DELETE 恢复 bitmap

#### 4f. 释放 IR 锁

对每个处理完的页面调用 `ReleaseIRLockForPage`：
- 标记为 storage-only
- 清除 IR 锁

#### 4g. RPC 失败降级

若 `AnalyzeRecoveryPages` RPC 失败：
- 直接释放 IR 锁
- 标记为 storage-only
- 后续访问时从存储层重新拉取

**Phase 4 完成标志**：日志输出 `Phase 3 complete: N pages released directly, M pages recovered via log replay. All IR locks cleared.`

---

## 二、从页面视角看故障恢复

### 页面分类与恢复时机

根据故障发生时页面的锁状态和所有权关系，可以将页面分为以下几类：

---

### 类型 A：故障节点持有 X 锁的页面

**特征**：故障节点是该页面的唯一写者，最新数据可能只存在于故障节点的内存中。

**恢复过程**：

| 阶段 | 操作 | 页面状态 |
|------|------|----------|
| Phase 1 | `CleanFailedNodeNoBlock` 移除 holder，设置 **IR 锁** | 🔒 IR 锁锁定，不可访问 |
| Phase 2 | 无存活节点汇报持有此页面 → IR 锁保留 | 🔒 IR 锁锁定，不可访问 |
| Phase 4 | 存储层执行 `RedoForPage` 日志回放 + `UndoForFailedNode` | 🔒 IR 锁锁定，不可访问 |
| Phase 4 完成 | `ReleaseIRLockForPage`，标记 storage-only | ✅ 可正常访问（从存储层获取） |

**恢复时机**：Phase 4 完成后（整个恢复流程结束）  
**数据来源**：存储层日志回放后的页面数据  
**可能的数据状态**：
- 已提交事务的修改 → 通过 Redo 恢复
- 未提交事务的修改 → 通过 Undo 撤销

---

### 类型 B：故障节点持有 S 锁的页面（其他节点也持有 S 锁）

**特征**：故障节点是该页面的读者之一，其他存活节点也持有该页面的 S 锁副本。

**恢复过程**：

| 阶段 | 操作 | 页面状态 |
|------|------|----------|
| Phase 1 | `CleanFailedNodeNoBlock` 移除 holder，lock--，**不设 IR 锁** | ✅ 可正常访问 |
| Phase 1 | 清空 request_queue，重置 is_pending | ✅ 可正常访问 |

**恢复时机**：Phase 1 完成后即可正常访问  
**数据来源**：其他存活节点的本地缓存（仍持有有效 S 锁副本）  
**说明**：由于 S 锁页面的数据在所有 holder 节点上是一致的（只读），故障节点的崩溃不影响数据正确性，只需清理锁状态即可。

---

### 类型 C：故障节点持有 S 锁的页面（故障节点是唯一 holder）

**特征**：故障节点是该页面唯一的 S 锁持有者。

**恢复过程**：

| 阶段 | 操作 | 页面状态 |
|------|------|----------|
| Phase 1 | `CleanFailedNodeNoBlock` 移除 holder，lock 变为 0，**不设 IR 锁** | ✅ 可正常访问 |
| Phase 1 | 清理 GlobalValidInfo 中故障节点的有效性 | ✅ 可正常访问 |

**恢复时机**：Phase 1 完成后即可正常访问  
**数据来源**：存储层（因为 S 锁页面不会被修改，存储层数据就是最新的）  
**说明**：S 锁意味着只读，故障节点没有修改过该页面，存储层的数据就是正确的。新的访问请求会从存储层获取。

---

### 类型 D：故障节点管理的 GPLM 页面（被存活节点持有锁）

**特征**：页面的 GPLM 管理者是故障节点，但实际持有锁的是存活节点。

**恢复过程**：

| 阶段 | 操作 | 页面状态 |
|------|------|----------|
| Phase 1 | 存活节点接管该页面的 GPLM 管理权，设置 IR 锁 | 🔒 IR 锁锁定 |
| Phase 2 | 存活节点通过 `ReportPageStatus` 汇报自己持有的锁 | 🔒 → ✅ IR 锁释放 |
| Phase 2 | `RecoverAddHolder` 恢复 holder 列表，释放 IR 锁 | ✅ 可正常访问 |

**恢复时机**：Phase 2 中收到汇报后即释放 IR 锁  
**数据来源**：存活节点本地缓存（它们仍持有有效数据）  
**说明**：这类页面的数据在存活节点上是完整的，只是 GPLM 管理权需要转移。一旦存活节点汇报了自己的锁状态，IR 锁就会被释放。

---

### 类型 E：故障节点管理的 GPLM 页面（无人持有锁）

**特征**：页面的 GPLM 管理者是故障节点，且当前没有任何节点持有该页面的锁。

**恢复过程**：

| 阶段 | 操作 | 页面状态 |
|------|------|----------|
| Phase 1 | 存活节点接管该页面的 GPLM 管理权，设置 IR 锁 | 🔒 IR 锁锁定 |
| Phase 2 | 无存活节点汇报持有此页面 → IR 锁保留 | 🔒 IR 锁锁定 |
| Phase 4 | 存储层分析：`disk_lsn >= gplm_lsn`（页面已是最新） | 🔒 IR 锁锁定 |
| Phase 4 完成 | `ReleaseIRLockForPage`，标记 storage-only | ✅ 可正常访问 |

**恢复时机**：Phase 4 完成后  
**数据来源**：存储层（页面数据已是最新）  
**说明**：虽然这类页面实际上不需要日志回放，但由于无法在 Phase 2 确认其状态，需要等到 Phase 4 由存储层确认后才能释放 IR 锁。

---

### 类型 F：与故障节点无关的页面

**特征**：页面的 GPLM 管理者不是故障节点，故障节点也不持有该页面的锁。

**恢复过程**：

| 阶段 | 操作 | 页面状态 |
|------|------|----------|
| Phase 1 | `CleanFailedNodeNoBlock` 返回 holder_type=0，无操作 | ✅ 始终可正常访问 |

**恢复时机**：无需恢复，始终可用  
**说明**：这类页面完全不受故障影响。但由于 Phase 1 中清空了 request_queue，如果有存活节点正在排队等待该页面的锁，其请求会被丢弃，需要通过 LPLM wakeup 重试。

---

### 页面恢复时间线总结

```
时间 ──────────────────────────────────────────────────────────────────────────►

故障发生    Phase 1完成     Phase 2完成      Phase 3完成      Phase 4完成
   │            │               │                │                │
   │            │               │                │                │
   │  类型B/C/F │               │                │                │
   │  ─────────►│ 可访问        │                │                │
   │            │               │                │                │
   │            │  类型D        │                │                │
   │            │  ────────────►│ 可访问         │                │
   │            │               │                │                │
   │            │               │    类型A/E     │                │
   │            │               │    ───────────────────────────►│ 可访问
   │            │               │                │                │
   ▼            ▼               ▼                ▼                ▼
```

---

## 三、从事务视角看故障恢复

### 事务分类

根据故障发生时事务的执行状态，可以将事务分为以下几类：

---

### 类型 1：故障节点上已提交的事务

**状态**：事务已写入 `BatchEndLogRecord`，日志已（或应已）发送到存储层。

**恢复行为**：

| 条件 | 结果 |
|------|------|
| 日志已刷到存储层 | Phase 4 Redo 回放恢复所有修改，数据不丢失 ✅ |
| 日志未刷到存储层（节点崩溃前未来得及刷新） | 修改丢失 ❌（P3 问题：当前通过事务提交前强制 LogFlush 缓解） |

**说明**：正常情况下，事务提交路径中会确保日志在返回客户端前已发送到存储层。如果日志确实到达了存储层，Phase 4 的 Redo 会完整恢复这些修改。

---

### 类型 2：故障节点上未提交的事务

**状态**：事务已写入部分 redo 日志，但没有 `BatchEndLogRecord`。

**恢复行为**：

1. Phase 4 中 Redo 可能会先回放这些日志（因为无法区分已提交和未提交）
2. Redo 完成后，`UndoForFailedNode` 扫描日志发现无 `BatchEndLogRecord`
3. 反向扫描，对该事务的所有修改执行 Undo（恢复 old_value）
4. 最终效果：**未提交事务的所有修改被完全撤销** ✅

---

### 类型 3：存活节点上正在执行的事务（访问了故障节点管理的页面）

**状态**：事务正在存活节点上执行，且已经获取了由故障节点管理的页面的远程锁。

**恢复行为**：

```
事务执行流程：
TxExe 开始 → 记录 tx_start_epoch
    │
    ├── 读/写操作（可能阻塞在 LPLM cv.wait 等待 PushPage）
    │       │
    │       └── Phase 3: SetRecoveryAbort() 唤醒
    │               │
    │               └── TryGetPushData 返回 false → 从存储层获取数据
    │
    ├── 执行完成后检查 recovery_epoch
    │       │
    │       └── epoch 变化 → 事务 abort
    │
    └── TxAbortWorkLoad → 释放所有已获取的锁
```

**具体场景**：

| 场景 | 事务行为 |
|------|----------|
| 事务正在等待 PushPage（cv.wait） | Phase 3 `SetRecoveryAbort` 唤醒 → `TryGetPushData` 返回 false → 从存储层获取数据 → 继续执行 → 最终因 epoch 变化而 abort |
| 事务正在等待远程加锁（is_granting） | Phase 3 `SetRecoveryAbort` 清除 is_granting → 线程被唤醒 → 检测到 recovery_abort → 重试加锁（路由到新管理者）→ 最终因 epoch 变化而 abort |
| 事务正在等待 Pending 释放（is_pending） | Phase 3 `SetRecoveryAbort` 清除 is_pending → 线程被唤醒 → 继续执行 → 最终因 epoch 变化而 abort |
| 事务已完成所有操作，准备提交 | 检查 epoch 变化 → abort |

**最终结果**：所有在恢复期间执行的事务都会被 abort，需要由上层重试。

---

### 类型 4：存活节点上正在执行的事务（仅访问非故障节点管理的页面）

**状态**：事务正在存活节点上执行，且只访问了由存活节点管理的页面。

**恢复行为**：

| 场景 | 事务行为 |
|------|----------|
| 事务在 Phase 3 之前完成 | 正常提交 ✅（epoch 未变化） |
| 事务在 Phase 3 之后检查 epoch | epoch 已变化 → abort ❌ |

**说明**：即使事务没有访问故障节点的页面，只要在 Phase 3 `recovery_epoch` 递增之后才检查 epoch，就会被 abort。这是一种保守策略，确保不会有事务读到不一致的数据。

---

### 类型 5：恢复完成后新发起的事务

**状态**：在所有 Phase 完成后才开始执行的事务。

**恢复行为**：

- 记录的 `tx_start_epoch` 是最新的 epoch 值
- 所有 IR 锁已释放，页面可正常访问
- 页面路由通过 `get_recovery_node_id` 自动路由到新的管理者
- 正常执行，不受恢复影响 ✅

---

### 类型 6：存活节点上正在执行的事务（遇到 IR 锁页面）

**状态**：事务在恢复期间尝试访问一个被 IR 锁锁定的页面。

**恢复行为**：

```
加锁请求 → GPLM 检查 IsIRLockedNoBlock()
    │
    └── 返回 ir_locked = true
            │
            └── 客户端收到 ir_locked 响应
                    │
                    └── usleep(1000) → 1ms 后重试
                            │
                            └── 循环重试直到 IR 锁释放
                                    │
                                    └── 最终因 epoch 变化而 abort
```

**说明**：当事务尝试访问 IR 锁页面时，GPLM 会立即返回 `ir_locked = true`，客户端会以 1ms 间隔轮询重试。但由于 Phase 3 已经递增了 epoch，即使最终获得了锁，事务也会在执行结束时因 epoch 检查而 abort。

---

### 事务恢复时间线总结

```
时间 ──────────────────────────────────────────────────────────────────────────►

故障发生    Phase 1完成     Phase 2完成      Phase 3完成      Phase 4完成
   │            │               │                │                │
   │            │               │                │                │
   │ 类型3/4事务│               │                │                │
   │ 可能仍在执行│              │   epoch递增     │                │
   │ ──────────────────────────────────────────►│ abort          │
   │            │               │                │                │
   │            │               │                │  类型5事务      │
   │            │               │                │  ─────────────►│ 正常执行
   │            │               │                │                │
   │ 类型6事务  │               │                │                │
   │ IR锁轮询  │               │                │                │
   │ ──────────────────────────────────────────►│ abort          │
   │            │               │                │                │
   ▼            ▼               ▼                ▼                ▼
```

---

## 四、关键机制说明

### 4.1 IR 锁机制

IR 锁（Instance Recovery Lock）是恢复期间的核心保护机制：

- **设置时机**：Phase 1 中对故障节点持有 X 锁的页面 + 故障节点管理的被接管页面
- **释放时机**：
  - Phase 2 中存活节点汇报持有该页面 → 立即释放
  - Phase 4 中存储层分析完成 → 释放
  - RPC 失败降级 → 直接释放（标记 storage-only）
- **对新请求的影响**：返回 `ir_locked = true`，客户端 1ms 轮询重试

### 4.2 Recovery Epoch 机制

- 每次恢复递增 `recovery_epoch`（原子操作）
- 事务开始时记录 `tx_start_epoch`
- 事务结束时比较：若不一致则 abort
- **目的**：确保恢复期间不会有事务基于不一致的锁状态提交

### 4.3 页面路由机制

- 正常情况：`get_node_id_by_page_id(table_id, page_id)` 确定 GPLM 管理者
- 故障后：`get_recovery_node_id(table_id, page_id)` 自动路由到存活节点
  - 若原管理者存活 → 返回原管理者
  - 若原管理者故障 → `surviving[page_id % surviving_count]`

### 4.4 降级策略

| 场景 | 降级行为 |
|------|----------|
| `AnalyzeRecoveryPages` RPC 失败 | 直接释放 IR 锁，标记 storage-only，后续从存储层获取 |
| Phase 2 Barrier 超时（30s） | 强制收集当前状态，继续 Phase 4 |
| `IRScanComplete` 发送失败（3次重试后） | 记录错误，可能影响 Barrier |
| `TryGetPushData` 被恢复中断 | 从存储层获取数据 |
