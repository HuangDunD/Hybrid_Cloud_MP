
# Hybrid_Cloud_MP 单节点故障恢复正确性修复计划

## 1. 问题总览

当前故障恢复流程（Instance Recovery）在**锁状态清理和重建**方面已基本完成，但在**数据正确性**方面存在以下关键缺陷：

| 编号 | 问题 | 严重程度 | 影响 |
|------|------|----------|------|
| P1 | 存储层 `AnalyzeRecoveryPages` 未实现日志回放 | 🔴 致命 | 故障节点独占修改的页面数据丢失 |
| P2 | 故障节点未提交事务的 Undo 缺失 | 🔴 致命 | 脏数据永久保留 |
| P3 | 故障节点崩溃时未刷日志丢失 | 🔴 致命 | 最近修改无法恢复 |
| P4 | `gplm_lsn` 记录不可靠 | 🔴 严重 | 无法正确判断页面是否需要回放 |
| P5 | Phase 2 扫描遗漏 `is_granting` 状态的页面 | 🟠 中等 | GPLM 状态恢复不完整 |
| P6 | Phase 2 Barrier 同步无容错 | 🟠 中等 | 恢复流程可能永久阻塞 |
| P7 | `SetRecoveryAbort` 清理 `is_pending` 导致锁泄漏 | 🟠 中等 | GPLM/LPLM 状态不一致 |
| P8 | 多节点独立执行 Phase 4 缺乏协调 | 🟠 中等 | 日志不完整、重复处理 |
| P9 | `UndoForFailedNode` 不处理存活节点未提交事务的 lock 残留 | 🔴 致命 | 存活节点 abort 事务的行级锁永久残留 |
| P10 | `UndoForFailedNode` 被多个计算节点重复调用 | 🟠 中等 | 重复 Undo 可能与并发日志 replay 产生竞争 |

本文档针对上述影响**正确性**的问题（P1-P10）提出修复方案。

---

## 2. 修复方案

---

### 2.1 【P1】实现存储层日志回放（Redo）

#### 2.1.1 问题分析

当前 `AnalyzeRecoveryPages`（`storage_rpc.cc:480`）只是读取磁盘页面并比较 LSN，当发现 `disk_lsn < gplm_lsn` 时仅打印 WARNING，**没有执行任何回放操作**。

#### 2.1.2 修复方案

**核心思路**：在存储层实现针对特定页面的日志定向回放（Targeted Redo）。

**实现步骤**：

1. **扩展 `LogReplay` 接口**：新增 `RedoForPage(table_name, page_no, target_lsn)` 方法
   - 从日志文件中扫描与目标页面相关的日志记录
   - 按 LSN 顺序应用所有 `disk_lsn < log_lsn <= target_lsn` 的日志
   - 返回回放后的页面数据

2. **修改 `AnalyzeRecoveryPages` 逻辑**：
   ```
   对每个请求页面:
     读取磁盘页面，获取 disk_lsn
     if disk_lsn >= gplm_lsn:
       status = 0  // 页面已是最新
     else:
       回放后的数据 = LogReplay::RedoForPage(table_name, page_no, gplm_lsn)
       if 回放成功:
         将回放后数据写回磁盘
         status = 1, page_data = 回放后数据
       else:
         status = 0  // 无法回放，标记为 storage-only（降级处理）
   ```

3. **日志索引加速**：为避免全量扫描日志文件，新增按 `(table_name, page_no)` 的日志索引
   - 在 `LogReplay::apply_single_log()` 中维护一个 `page_log_index_`：`map<PageId, vector<{offset, lsn}>>`
   - `RedoForPage` 时通过索引快速定位相关日志

**涉及文件**：
- `core/storage/logreplay.h` — 新增 `RedoForPage` 接口和页面日志索引
- `core/storage/logreplay.cc` — 实现定向回放逻辑
- `core/storage/storage_rpc.cc` — 修改 `AnalyzeRecoveryPages` 调用回放

**验证标准**：
- 故障节点持有 X 锁的页面，在恢复后磁盘数据与 `gplm_lsn` 一致
- 回放后的页面通过 CRC 校验

---

### 2.2 【P2】实现未提交事务的 Undo

#### 2.2.1 问题分析

`LogReplay::restore()` 是空函数。故障节点上未提交的事务（已写 redo 日志但无 `BatchEndLogRecord`/`CommitLogRecord`）的修改会通过 redo 回放到磁盘，但不会被撤销。

#### 2.2.2 修复方案

**核心思路**：在 Phase 4 的 Redo 完成后，执行 Undo 阶段，撤销所有未提交事务的修改。

**实现步骤**：

1. **事务状态判定**：
   - 扫描日志文件，收集所有事务的状态：
     - 有 `BatchEndLogRecord` → 已提交
     - 无 `BatchEndLogRecord` → 未提交（需要 Undo）
   - 只关注故障节点（`log_node_id_ == failed_node_id`）的事务

2. **Undo 日志设计**：
   - 当前 `UpdateLogRecord` 已包含 `old_value_` 和 `new_value_`
   - Undo 操作：将 `new_value_` 替换回 `old_value_`
   - 对 `InsertLogRecord`：清除 bitmap 中对应 slot
   - 对 `DeleteLogRecord`：恢复 bitmap 中对应 slot

3. **实现 `LogReplay::UndoForFailedNode(node_id_t failed_node_id)`**：
   ```
   Step 1: 扫描日志，构建事务状态表
     committed_txns = {txn_id | 存在 BatchEndLogRecord}
     
   Step 2: 反向扫描日志，对未提交事务执行 Undo
     for each log_record in reverse order:
       if log_record.node_id == failed_node_id
          && log_record.txn_id NOT IN committed_txns:
         apply_undo_log(log_record)
   ```

4. **集成到恢复流程**：
   - 在 `AnalyzeRecoveryPages` 中，先执行 Redo，再执行 Undo
   - 或者将 Undo 作为独立的 RPC 调用（`UndoFailedNodeTransactions`）

**涉及文件**：
- `core/storage/logreplay.h` — 新增 `UndoForFailedNode` 接口
- `core/storage/logreplay.cc` — 实现 Undo 逻辑（填充 `restore()` 函数）
- `core/storage/storage_rpc.cc` — 在 Redo 后调用 Undo
- `core/storage/storage_service.proto` — 可选：新增 Undo RPC

**验证标准**：
- 故障节点上未提交事务的修改在恢复后被完全撤销
- 已提交事务的修改保持不变

---

### 2.3 【P3】保证故障节点日志不丢失

#### 2.3.1 问题分析

故障节点的 `log_records` 队列中可能有未发送到存储层的日志（后台线程每 10-100ms 刷一次）。节点崩溃时这些日志永久丢失，导致 Redo 无法恢复最近的修改。

#### 2.3.2 修复方案

**方案 A（推荐）：事务提交前强制日志持久化**

**核心思路**：修改事务提交路径，确保事务的所有日志在返回客户端前已发送到存储层。

**实现步骤**：

1. **修改 `TxCommit` 路径**：在事务提交时，调用 `LogFlush()` 或等待 `persist_lsn >= tx_max_lsn`
   ```cpp
   bool DTX::TxCommit(coro_yield_t& yield) {
     // ... 现有逻辑 ...
     AddLogToTxn();  // 生成 commit log
     
     // 新增：等待日志持久化
     compute_server->WaitLogPersist(max_lsn);
     
     // 释放锁、返回
   }
   ```

2. **实现 `WaitLogPersist(LLSN target_lsn)`**：
   ```cpp
   void ComputeServer::WaitLogPersist(LLSN target_lsn) {
     // 如果当前 persist_lsn 已经覆盖，直接返回
     if (GetPersistedLSN() >= target_lsn) return;
     
     // 触发一次立即刷新
     LogFlush();
     
     // 等待确认
     std::unique_lock<std::mutex> lk(persist_lsn_mtx);
     persist_lsn_cond.wait(lk, [&]{ return persist_lsn >= target_lsn; });
   }
   ```

3. **性能优化**：使用 Group Commit 机制
   - 多个事务的日志合并为一次 RPC 发送
   - 后台线程检测到有等待者时立即触发刷新（而非等待 10ms 间隔）
   - 引入 `urgent` 标志，当有事务等待时设置 `urgent=1`

**方案 B（备选）：存活节点代为刷新**

如果方案 A 对性能影响过大，可以采用：
- 存活节点在 Phase 4 之前，各自执行 `LogFlush()` 确保自己的日志已发送
- 但这**无法恢复故障节点本地未发送的日志**，只能保证存活节点的日志完整

**方案 C（补充）：日志双写/复制**

- 每个计算节点的日志同时写入本地和远程存储（或另一个计算节点）
- 故障时从副本恢复日志
- 实现复杂度高，作为长期优化方案

**推荐**：短期采用方案 A + B 组合，长期考虑方案 C。

**涉及文件**：
- `compute_server/server.h` — 新增 `WaitLogPersist`，修改 `LogFlush` 支持 urgent
- `core/dtx/dtx_exe.cc` — 修改 `TxCommit` 路径
- `compute_server/worker/handler.cc` — 修改后台刷新线程逻辑

**验证标准**：
- 事务提交返回客户端时，其所有日志已在存储层持久化
- 故障节点崩溃后，存储层拥有该节点所有已提交事务的完整日志

---

### 2.4 【P4】修复 `gplm_lsn` 的维护机制

#### 2.4.1 问题分析

`LR_GlobalPageLock::lsn_id` 用于记录页面最后已知 LSN，但：
1. `Reset()` 中 `lsn_id` 未被显式处理（保留旧值或为 0）
2. `CleanFailedNodeNoBlock()` 不更新 `lsn_id`
3. 正常加锁/解锁路径中 `lsn_id` 的更新时机不明确

#### 2.4.2 修复方案

**核心思路**：在每次页面所有权转移时（解锁/释放），更新 GPLM 中的 `lsn_id`。

**实现步骤**：

1. **在解锁 RPC 中携带 LSN**：
   - 修改 `LRPAnyUnLock` 请求，新增 `lsn` 字段（已存在于 `BufferReleaseUnlockRequest` 中）
   - 计算节点释放锁时，将页面当前的 LLSN 发送给 GPLM

2. **在 GPLM 解锁处理中更新 `lsn_id`**：
   ```cpp
   void LRPAnyUnLock_handler(...) {
     // ... 现有解锁逻辑 ...
     LLSN page_lsn = request->lsn();
     if (page_lsn > gl->getLsnIDNoBlock()) {
       gl->setLsnIDNoBlock(page_lsn);
     }
   }
   ```

3. **在 `CleanFailedNodeNoBlock` 中保留 `lsn_id`**：
   - 不重置 `lsn_id`（当前实现已经没有重置，确认即可）
   
4. **在 `Reset()` 中保留 `lsn_id`**（用于接管场景）：
   ```cpp
   void Reset(){
     lock = 0;
     hold_lock_nodes.clear();
     request_queue.clear();
     s_request_num = 0;
     x_request_num = 0;
     is_pending = false;
     src_node_id = INVALID_NODE_ID;
     ir_locked = false;
     // 注意：不重置 lsn_id，保留最后已知值
   }
   ```

5. **在 Pending 推送时更新 LSN**：
   - 当持有者节点推送页面给新持有者时，在 `NotifyPushPage` 的响应中携带页面 LSN
   - GPLM 在 `TransferControl` 时更新 `lsn_id`

**涉及文件**：
- `core/GPLM/global_LR_page_lock.h` — 确保 `Reset()` 不清除 `lsn_id`，新增 LSN 更新逻辑
- `core/remote_page_table/remote_page_table.proto` — `PAnyUnLockRequest` 新增 `lsn` 字段
- `core/remote_page_table/remote_page_table_rpc.h` — 解锁处理中更新 `lsn_id`
- `compute_server/lazyrelease_server.cc` — 解锁时携带页面 LSN

**验证标准**：
- 任意时刻 `gplm_lsn` >= 该页面在存储层磁盘上的 LSN
- Phase 4 发送给存储层的 `gplm_lsn` 能正确反映页面最后修改的 LSN

---

### 2.5 【P5】修复 Phase 2 扫描遗漏 `is_granting` 页面

#### 2.5.1 问题分析

`HasOwner()` 只检查 `remote_mode == SHARED || EXCLUSIVE`，但 `is_granting` 状态下 `remote_mode` 可能仍为 NONE（正在向远程申请但未收到响应）。这些页面不会被 Phase 2 汇报。

#### 2.5.2 修复方案

**核心思路**：扩展 Phase 2 扫描条件，包含 `is_granting` 状态的页面。

**实现步骤**：

1. **新增 `HasOwnerOrGranting()` 方法**：
   ```cpp
   bool HasOwnerOrGranting() {
     std::lock_guard<std::mutex> l(mutex);
     return (remote_mode == LockMode::SHARED || 
             remote_mode == LockMode::EXCLUSIVE || 
             is_granting);
   }
   ```

2. **修改 Phase 2 扫描逻辑**：
   ```cpp
   // RunIRRecoveryScan 中
   if (!lr->HasOwnerOrGranting()) continue;
   
   // 获取锁模式时区分 granting 状态
   int lock_mode = 0;
   if (lr->IsUpgrading()) {
     lock_mode = 1;  // SHARED upgrading
   } else if (is_granting && remote_mode == NONE) {
     // 正在申请但未确认，按请求的锁类型汇报
     lock_mode = (lr->getLock() == EXCLUSIVE_LOCKED) ? 2 : 1;
   } else if (lr->HasOwner()) {
     lock_mode = lr->getLock() == EXCLUSIVE_LOCKED ? 2 : 1;
   }
   ```

3. **GPLM 侧处理**：`RecoverAddHolder` 已经能处理重复添加（有去重逻辑），所以即使 granting 状态的页面实际上未被 GPLM 授予锁，汇报也不会导致错误。

**涉及文件**：
- `core/LPLM/local_LR_page_lock.h` — 新增 `HasOwnerOrGranting()`
- `compute_server/server.h` — 修改 `RunIRRecoveryScan` 使用新方法

**验证标准**：
- 所有处于 granting 状态的页面在恢复后 GPLM 状态正确
- 不会因为重复汇报导致 GPLM 状态异常

---

### 2.6 【P6】修复 Phase 2 Barrier 同步的容错性

#### 2.6.1 问题分析

`IRScanComplete` RPC 失败时只打 WARNING，不重试也不降低 `ir_scan_expected_`。如果某个存活节点的通知丢失，`WaitPhase2AndGetRemainingIRPages()` 会永久阻塞（虽然有 2s 超时唤醒，但 `phase2_complete_` 永远不为 true）。

#### 2.6.2 修复方案

**核心思路**：为 Barrier 同步增加超时机制和重试逻辑。

**实现步骤**：

1. **`IRScanComplete` 发送增加重试**：
   ```cpp
   for (int i = 0; i < ComputeNodeCount; i++) {
     if (i == (int)failed_node_id || IsNodeFailed(i)) continue;
     
     int retry = 0;
     bool success = false;
     while (retry < 3 && !success) {
       brpc::Controller cntl;
       cntl.set_timeout_ms(2000);
       // ... 发送 RPC ...
       if (!cntl.Failed()) {
         success = true;
       } else {
         retry++;
         usleep(500000);  // 500ms backoff
       }
     }
     if (!success) {
       LOG(ERROR) << "Failed to send IRScanComplete to node " << i << " after 3 retries";
       // 降级：通知本地 barrier 减少期望值
       page_table_service_impl_->DecrementIRScanExpected();
     }
   }
   ```

2. **新增 `DecrementIRScanExpected()` 方法**：
   ```cpp
   void DecrementIRScanExpected() {
     std::lock_guard<std::mutex> lk(ir_scan_mutex_);
     ir_scan_expected_--;
     if (ir_scan_complete_count_ >= ir_scan_expected_) {
       CollectRemainingIRLockedPages(/*failed_node*/);
     }
   }
   ```

3. **`WaitPhase2AndGetRemainingIRPages` 增加全局超时**：
   ```cpp
   std::vector<IRLockedPageInfo> WaitPhase2AndGetRemainingIRPages() {
     std::unique_lock<std::mutex> lk(ir_scan_mutex_);
     auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
     while (!phase2_complete_) {
       if (ir_scan_cv_.wait_until(lk, deadline) == std::cv_status::timeout) {
         LOG(ERROR) << "[IR Recovery] Phase 2 barrier timeout! Forcing completion.";
         CollectRemainingIRLockedPages(/*failed_node*/);
         break;
       }
     }
     return remaining_ir_pages_;
   }
   ```

**涉及文件**：
- `core/remote_page_table/remote_page_table_rpc.h` — 新增 `DecrementIRScanExpected`，修改 `WaitPhase2`
- `compute_server/server.h` — `RunIRRecoveryScan` 中增加重试逻辑

**验证标准**：
- 即使某个存活节点的 `IRScanComplete` 通知丢失，恢复流程仍能在 30s 内完成
- 不会出现永久阻塞

---

### 2.7 【P7】修复 `SetRecoveryAbort` 导致的锁状态不一致

#### 2.7.1 问题分析

`SetRecoveryAbort()` 中：
```cpp
if (is_pending) {
    is_pending = false;
    if (!is_granting) {
        remote_mode = LockMode::NONE;  // 问题：GPLM 可能仍认为本节点是 holder
    }
}
```

将 `remote_mode` 设为 NONE 后，本节点认为不再持有远程锁，但 GPLM 中可能仍记录着本节点是 holder（Phase 1 的 `CleanFailedNodeNoBlock` 只清理了**故障节点**的 holder 状态，不会清理存活节点的）。

#### 2.7.2 修复方案

**核心思路**：`SetRecoveryAbort` 不应该修改 `remote_mode`，而是让重试逻辑来处理状态同步。

**实现步骤**：

1. **修改 `SetRecoveryAbort()`**：
   ```cpp
   void SetRecoveryAbort() {
     std::lock_guard<std::mutex> lk(mutex);
     // 清理 is_pending 防止 busy-wait
     if (is_pending) {
       is_pending = false;
       // 不修改 remote_mode！让重试逻辑通过 GPLM 来确认真实状态
       // 如果 is_granting=true，说明正在申请锁，recovery abort 后重试会重新发 RPC
       // 如果 is_granting=false，说明已持有锁，保留 remote_mode 让后续正常释放
     }
     // 清理 is_granting 状态，让等待线程能够重新发起加锁请求
     if (is_granting) {
       is_granting = false;
       // 重置本地锁计数（granting 期间 lock 已被设置）
       lock = 0;
       remote_mode = LockMode::NONE;
     }
     recovery_abort.store(true);
     cv.notify_all();
   }
   ```

2. **关键区分**：
   - `is_granting=true`：正在申请远程锁，可以安全重置所有状态（因为 GPLM 侧的 `CleanFailedNodeNoBlock` 已清空了请求队列）
   - `is_granting=false, is_pending=true`：已持有远程锁但被要求释放，**保留 `remote_mode`**，让事务正常完成后释放

3. **事务侧处理**：被唤醒的事务检测到 `recovery_abort` 后会 abort，abort 过程中会正常释放远程锁（通过 `UnlockAny`），此时 GPLM 和 LPLM 状态自然同步。

**涉及文件**：
- `core/LPLM/local_LR_page_lock.h` — 修改 `SetRecoveryAbort()` 逻辑

**验证标准**：
- 恢复后所有存活节点的 LPLM `remote_mode` 与 GPLM `hold_lock_nodes` 一致
- 不存在 GPLM 认为节点持有锁但 LPLM 认为不持有的情况

---

### 2.8 【P8】Phase 4 日志刷新协调

#### 2.8.1 问题分析

Phase 4 中每个节点只调用自己的 `LogFlush()`，但 IR 锁页面可能被**其他存活节点**修改过（在故障发生前），这些节点的日志可能还未刷新到存储层。

#### 2.8.2 修复方案

**核心思路**：在 Phase 4 开始前，确保**所有存活节点**的日志都已刷新到存储层。

**实现步骤**：

1. **在 Phase 3（唤醒线程）之后、Phase 4 之前，插入全局日志刷新步骤**：
   ```cpp
   // Phase 3.5: 全局日志刷新
   // 所有存活节点刷新自己的日志到存储层
   LogFlush();  // 本节点刷新
   
   // 通知其他存活节点也刷新（通过新 RPC 或复用现有机制）
   for (int i = 0; i < ComputeNodeCount; i++) {
     if (i == my_id || IsNodeFailed(i)) continue;
     // 发送 FlushLog RPC
     RequestRemoteLogFlush(i);
   }
   ```

2. **新增 `FlushLog` RPC**（或复用 `IRScanComplete` 的时机）：
   - 更简单的方案：将 `LogFlush()` 放在 `RunIRRecoveryScan` 的**开头**
   - 这样当所有节点的 `IRScanComplete` 到达时，可以保证所有节点的日志已刷新

3. **修改 `RunIRRecoveryScan`**：
   ```cpp
   void RunIRRecoveryScan(node_id_t failed_node_id, node_id_t my_id) {
     // 首先刷新本节点日志，确保存储层有完整数据
     LogFlush();
     
     // ... 原有扫描逻辑 ...
   }
   ```

**涉及文件**：
- `compute_server/server.h` — 在 `RunIRRecoveryScan` 开头添加 `LogFlush()`

**验证标准**：
- Phase 4 向存储层发送分析请求时，所有存活节点的日志已在存储层
- 存储层能看到所有已提交事务的完整日志

---

## 3. 实施优先级与依赖关系

```
┌─────────────────────────────────────────────────────────┐
│                    实施路线图                              │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  第一阶段（基础保障）：                                    │
│  ┌──────┐    ┌──────┐    ┌──────┐                      │
│  │ P3   │───▶│ P4   │───▶│ P8   │                      │
│  │日志不│    │LSN维 │    │日志刷│                      │
│  │丢失  │    │护    │    │新协调│                      │
│  └──────┘    └──────┘    └──────┘                      │
│       │                       │                         │
│       ▼                       ▼                         │
│  第二阶段（核心恢复）：                                    │
│  ┌──────┐    ┌──────┐                                  │
│  │ P1   │───▶│ P2   │                                  │
│  │Redo  │    │Undo  │                                  │
│  │实现  │    │实现  │                                  │
│  └──────┘    └──────┘                                  │
│                                                         │
│  第三阶段（健壮性）：                                      │
│  ┌──────┐    ┌──────┐    ┌──────┐                      │
│  │ P5   │    │ P6   │    │ P7   │                      │
│  │扫描  │    │Barrier│    │锁状态│                      │
│  │修复  │    │容错  │    │修复  │                      │
│  └──────┘    └──────┘    └──────┘                      │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 依赖关系说明：

| 阶段 | 任务 | 前置依赖 | 预估工作量 |
|------|------|----------|-----------|
| 第一阶段 | P3: 日志持久化保证 | 无 | 2-3 天 |
| 第一阶段 | P4: LSN 维护 | 无 | 1-2 天 |
| 第一阶段 | P8: 日志刷新协调 | P3 | 0.5 天 |
| 第二阶段 | P1: Redo 实现 | P3, P4 | 3-5 天 |
| 第二阶段 | P2: Undo 实现 | P1 | 3-4 天 |
| 第三阶段 | P5: 扫描修复 | 无 | 0.5 天 |
| 第三阶段 | P6: Barrier 容错 | 无 | 1 天 |
| 第三阶段 | P7: 锁状态修复 | 无 | 1 天 |

---

## 4. 测试方案

### 4.1 单元测试

| 测试项 | 验证内容 |
|--------|----------|
| `TestRedoForPage` | 构造日志序列，验证定向回放后页面数据正确 |
| `TestUndoUncommitted` | 构造未提交事务日志，验证 Undo 后页面恢复原状 |
| `TestLSNTracking` | 验证加锁/解锁/推送过程中 `gplm_lsn` 单调递增 |
| `TestBarrierTimeout` | 模拟 RPC 失败，验证 Barrier 超时后恢复继续 |

### 4.2 集成测试

| 测试场景 | 步骤 | 预期结果 |
|----------|------|----------|
| X 锁页面恢复 | Node0 持有 X 锁修改页面 → Node0 崩溃 → 恢复 | 页面数据通过 Redo 恢复到最新 |
| 未提交事务回滚 | Node0 修改页面但未提交 → Node0 崩溃 → 恢复 | 页面数据通过 Undo 恢复到修改前 |
| 日志不丢失 | Node0 提交事务 → 立即崩溃 → 恢复 | 已提交事务的修改不丢失 |
| Barrier 容错 | 恢复过程中 Node1 的 IRScanComplete 丢失 | 30s 超时后恢复继续完成 |
| 并发事务恢复 | 多个事务并发执行中 Node0 崩溃 | 已提交事务保留，未提交事务回滚 |

### 4.3 压力测试

- 在高并发负载下（YCSB/TPC-C）随机 kill 一个节点
- 验证恢复后数据一致性（通过全表扫描对比）
- 验证恢复后系统能继续正常处理事务

---

## 5. 风险与降级策略

| 风险 | 降级策略 |
|------|----------|
| Redo 回放失败（日志损坏） | 标记页面为 storage-only，从存储层最后一致快照读取 |
| Undo 回放失败 | 记录失败页面，人工介入修复 |
| 日志持久化影响性能 | 提供配置开关，允许用户选择性能优先（接受少量数据丢失风险） |
| Phase 4 超时 | 释放所有 IR 锁，标记为 storage-only，后续访问从存储层获取 |

---

## 6. 附录：关键数据结构变更

### 6.1 `LogReplay` 新增接口

```cpp
class LogReplay {
public:
    // 针对特定页面执行 Redo 回放
    // 返回回放后的页面数据，如果无需回放返回空
    std::optional<std::string> RedoForPage(
        const std::string& table_name, 
        page_id_t page_no, 
        LLSN target_lsn);
    
    // 撤销故障节点所有未提交事务的修改
    int UndoForFailedNode(node_id_t failed_node_id);
    
private:
    // 页面日志索引：加速定向回放
    struct LogIndexEntry {
        uint64_t file_offset;
        LLSN lsn;
        LogType type;
    };
    std::unordered_map<PageId, std::vector<LogIndexEntry>> page_log_index_;
    std::mutex page_log_index_mutex_;
};
```

---

### 2.9 【P9】存活节点未提交事务的 lock 残留

#### 2.9.1 问题分析

**现象**：故障恢复完成后，数据一致性验证发现部分记录的 `DataItem.lock` 字段仍为 `0xFF00000000000000`（`EXCLUSIVE_LOCKED`），这些记录既包含故障节点分区的页面，也包含存活节点分区的页面。

**根本原因**：

当前 `UndoForFailedNode` 只扫描 `log_node_id == failed_node_id` 的日志记录，即只 undo 故障节点产生的未提交事务。但存活节点在恢复期间也有事务被 abort（因为 `recovery_epoch` 变化），这些事务的 lock 清理存在以下竞争条件：

```
时间线：
  t1: 存活节点事务 T 执行 TxExe，设置 lock=EXCLUSIVE_LOCKED，生成 UpdateLog_A（有 old_record）
  t2: UpdateLog_A 通过 LogFlush 发送到存储节点，replay 到磁盘（磁盘上 lock=EXCLUSIVE_LOCKED）
  t3: 故障发生，恢复开始
  t4: 事务 T 检测到 recovery_epoch 变化，调用 TxAbortWorkLoad
  t5: TxAbortWorkLoad 中 FetchXPage 等待 IR 锁释放（阻塞）
  t6: AnalyzeRecoveryPages 被调用，UndoForFailedNode 执行
      - 扫描到 UpdateLog_A，但 node_id 是存活节点，跳过
      - Undo 完成，磁盘上 lock 仍为 EXCLUSIVE_LOCKED
  t7: 恢复完成，IR 锁释放
  t8: TxAbortWorkLoad 获取页面，清理 lock=0，生成 UpdateLog_B
  t9: UpdateLog_B 通过 LogFlush 发送到存储节点，replay 到磁盘（磁盘上 lock=0）
```

正常情况下 t9 会修复问题。但如果以下任一条件成立，lock 就会永久残留：

1. **LogFlush 在 t8-t9 之间失败**（如存储节点 RPC 超时）
2. **存活节点在 t9 之前被 kill**（测试脚本使用 `pkill` 强制终止）
3. **TxAbortWorkLoad 中 FetchXPage 失败**（如 RPC 连接断开）
4. **UpdateLog_B 的 replay 因 LSN 冲突被跳过**（prev_lsn 不匹配）

此外，`UndoForFailedNode` 被两个存活计算节点各调用一次（因为两个节点各发送了一次 `AnalyzeRecoveryPages` 请求），存在重复执行和竞争的问题。

#### 2.9.2 修复方案

**核心思路**：`UndoForFailedNode` 应该 undo **所有节点**的未提交事务（即没有对应 `BATCHEND` 日志的事务），而不仅仅是故障节点的。

**修改文件**：`core/storage/logreplay.cc` — `UndoForFailedNode` 函数

**实现步骤**：

```cpp
int LogReplay::UndoForFailedNode(node_id_t failed_node_id) {
    // 第一步：扫描日志文件，构建所有节点的事务状态表
    // ...
    
    // 修改：不再只关注故障节点的日志，而是关注所有节点
    // if (log_node_id == failed_node_id) {  // 删除此条件
    if (type == LogType::BATCHEND) {
        committed_txns.insert(log_txn_id);
    } else if (type == LogType::UPDATE || type == LogType::INSERT || type == LogType::DELETE) {
        undo_candidates.push_back({scan_offset + inner_offset, log_size, log_lsn, log_txn_id});
    }
    // }
    
    // 第二步：反向扫描，对所有未提交事务执行 Undo
    // （逻辑不变，只是范围扩大了）
}
```

**注意事项**：
- 扩大 Undo 范围后，已提交事务的 commit UpdateLog（lock=0）也会出现在 `undo_candidates` 中，但由于其 `txn_id` 在 `committed_txns` 中，会被正确跳过
- 存活节点的 `TxAbortWorkLoad` 生成的 UpdateLog 没有 `old_record`（`HasUndoPayload()` 返回 false），所以不会被 undo——这是正确的，因为 abort 的 UpdateLog 本身就是在清理 lock

**验证标准**：
- 恢复完成后，所有数据页面的 `DataItem.lock` 字段为 0
- 不存在 `EXCLUSIVE_LOCKED` 残留

---

### 2.10 【P10】`UndoForFailedNode` 被重复调用

#### 2.10.1 问题分析

当前每个存活计算节点都会独立发送 `AnalyzeRecoveryPages` RPC 请求，存储节点对每个请求都执行一次 `UndoForFailedNode`。这导致：

1. **重复执行**：同一个 Undo 操作被执行多次（虽然幂等，但浪费资源）
2. **竞争条件**：两个线程同时执行 Undo，可能与并发的日志 replay（来自存活节点的 LogFlush）产生竞争
3. **不一致风险**：第一次 Undo 恢复了 lock=0，但在第二次 Undo 执行前，存活节点的 LogFlush 可能又把 lock=EXCLUSIVE_LOCKED 写回了磁盘

#### 2.10.2 修复方案

**方案 A（推荐）**：只让一个计算节点（coordinator）执行 `AnalyzeRecoveryPages`，其他节点等待结果。

**方案 B**：在存储节点侧加锁，确保 `UndoForFailedNode` 只执行一次：

```cpp
void StoragePoolImpl::AnalyzeRecoveryPages(...) {
    // ...
    
    // 使用 once_flag 确保 Undo 只执行一次
    static std::once_flag undo_flag;
    int undo_count = 0;
    std::call_once(undo_flag, [&]() {
        undo_count = log_replay->UndoForFailedNode(failed_node_id);
    });
    
    // ...
}
```

**方案 C**：在 Undo 执行前，先暂停存活节点的 LogFlush（通过 RPC 通知），确保 Undo 期间没有新日志被 replay。

**涉及文件**：
- `core/storage/storage_rpc.cc` — `AnalyzeRecoveryPages` 中添加去重逻辑
- `compute_server/server.h` — 恢复期间暂停 LogFlush

---

### 6.2 Proto 变更

```protobuf
// PAnyUnLockRequest 新增 LSN 字段
message PAnyUnLockRequest {
    PageID page_id = 1;
    sint32 node_id = 2;
    uint64 lsn = 3;        // 新增：页面当前 LSN
};
```

### 6.3 `LRLocalPageLock` 新增接口

```cpp
class LRLocalPageLock {
public:
    // Phase 2 扫描用：包含 granting 状态
    bool HasOwnerOrGranting() {
        std::lock_guard<std::mutex> l(mutex);
        return (remote_mode == LockMode::SHARED || 
                remote_mode == LockMode::EXCLUSIVE || 
                is_granting);
    }
};
```
