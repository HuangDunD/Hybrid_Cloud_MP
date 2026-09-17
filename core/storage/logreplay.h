#pragma once

#include <unistd.h>
#include <array>
#include <atomic>
#include <list>
#include <set>
#include <thread>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <assert.h>
#include <butil/logging.h>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "sm_manager.h"
#include "common.h"
#include "log_record.h"
#include "log_format.h"
#include "undo_area.h"
#include "disk_manager.h"
#include "base/data_item.h"
#include "util/bitmap.h"

class LogBuffer {
public:
    LogBuffer() { 
        offset_ = 0; 
        memset(buffer_, 0, sizeof(buffer_));
    }

    char buffer_[LOG_REPLAY_BUFFER_SIZE+1];
    uint64_t offset_;    // 写入log的offset
};

class LogReplay{
public:
    LogReplay(DiskManager* disk_manager, SmManager *sm , const std::string m , std::unordered_map<table_id_t, std::string> table_name_map = {});

private:
    // legacy 模式（v1 单文件 LOG_FILE）初始化，保持原有行为：
    // 从文件头恢复 persist_off_ 断点，重启续放（不再跳过重放）
    void InitLegacyStorage();
public:

    ~LogReplay(){
        replay_stop = true;
        if (replay_thread_.joinable()) {
            replay_thread_.join();
        }
        // if (checkpoint_thread_.joinable()) {
        //     checkpoint_thread_.join();
        // }
        if (log_replay_fd_ >= 0) {
            close(log_replay_fd_);   // legacy 模式
        }
        if (log_write_head_fd_ >= 0) {
            close(log_write_head_fd_);
        }
        // v2 模式：关闭段文件 fd 缓存
        for (auto& kv : v2_seg_fd_cache_) {
            ::close(kv.second);
        }
        v2_seg_fd_cache_.clear();
    };

    uint64_t  read_log(char *log_data, int size, uint64_t offset);
    // P1 续：sync_to_disk=false（replay 线程默认）经 write-back 页缓存延迟
    // 落盘；sync_to_disk=true（测试等直接调用方）保持"apply 返回后磁盘
    // 已更新"的同步契约，立即写盘并从缓存移除
    void apply_sigle_log(LogRecord* log_record, uint64_t curr_offset, bool sync_to_disk = false);
    void apply_sigle_log(const std::shared_ptr<LogRecord>& log_record, int curr_offset, bool allow_enqueue);    void apply_undo_log(const LogRecord* log_record);
    // 对一条 WAL 记录字节流执行 undo 应用（UndoForFailedNode 的 undo 区路径与
    // WAL 全扫 fallback 路径共用）。返回是否实际执行了 undo 操作
    bool ApplyUndoWalRecord(const char* rec, uint32_t len);
    void add_max_replay_off_(int off) {
        std::lock_guard<std::mutex> latch(latch1_);
        max_replay_off_ += off;
    }
    void replayFun();
    void checkpointFun();
    void restore();

    // ==================== 重放暂停/恢复 ====================
    // open_db 重建 blink/FSM 文件期间必须暂停重放线程，否则重放持有的
    // fd/页面缓存会指向被销毁的文件
    void ResumeReplay() { replay_pause_mtx_.unlock(); }

    // P1 续：暂停时先落盘 replay 页缓存（与 replay 互斥的直写路径
    // ——Undo/定向Redo/open_db 重建——要求磁盘上已是已应用状态）。
    // flush 内部 fail-closed 不抛异常：在锁持有期间抛异常会使调用方
    // RAII guard 构造未完成而泄漏 pause 锁（死锁）。
    void PauseReplay() {
        replay_pause_mtx_.lock();
        try {
            FlushReplayPages();
        } catch (...) {
            // FlushReplayPages 契约上不抛；防御性兜底保证锁语义完整
            LOG(ERROR) << "[LogReplay] PauseReplay: unexpected flush exception, cache state uncertain";
        }
    }

    // 等待重放线程追平到调用时刻的日志尾（persist_off_ >= max_replay_off_）。
    // Undo 前必须调用：undo 基于"日志已物化"假设，若 undo 快于重放，
    // 未提交日志随后会被 replay 再次物化，导致已撤销的脏数据复活
    bool WaitReplayCaughtUp(int timeout_ms = 30000);
    // 只读快照当前重放边界：{日志尾地址(含), 已应用地址(含)}。
    // 仅用于计量/展示，不构成任何安全判定（安全判定用 WaitReplayCaughtUp/ValidationCut）
    std::pair<uint64_t, uint64_t> ReplayBoundaries() {
        uint64_t tail, applied;
        { std::lock_guard<std::mutex> lock(latch1_); tail = max_replay_off_; }
        { std::lock_guard<std::mutex> lock(latch2_); applied = persist_off_; }
        return {tail, applied};
    }
    // 落盘全部 replay 缓存脏页（安全点：WaitReplayCaughtUp 成功后、
    // PauseReplay 内部）。与 replay 线程通过 replay_cache_mtx_ 互斥。
    // 返回 false 表示有页落盘失败被丢弃（fail-closed，调用方不得把
    // 本次追平当作有效）。不抛异常。
    bool FlushReplayPages() {
        std::lock_guard<std::mutex> lock(replay_cache_mtx_);
        return FlushReplayPagesLocked();
    }
    std::pair<uint64_t, uint64_t> ValidationCut(int timeout_ms = 1800000) {
        if (!WaitReplayCaughtUp(timeout_ms)) throw std::runtime_error("replay drain deadline");
        std::lock_guard<std::recursive_mutex> pause(replay_pause_mtx_);
        uint64_t tail, applied;
        { std::lock_guard<std::mutex> lock(latch1_); tail = max_replay_off_; }
        { std::lock_guard<std::mutex> lock(latch2_); applied = persist_off_; }
        if (applied != tail) throw std::runtime_error("replay cut is not stable");
        if (undo_area_ && undo_area_->ActiveTxnCount() != 0)
            throw std::runtime_error("unresolved Undo transactions at validation cut");
        if (sm_manager) sm_manager->getBufferPoolMgr()->flush_all_pages();
        disk_manager_->SyncOpenFiles();
        if (v2_mode_) {
            std::lock_guard<std::mutex> lock(manifest_mtx_);
            manifest_.replayed_addr = applied + 1;
            manifest_.checkpoint_addr = applied + 1;
            PersistManifest();
        }
        return {tail, applied};
    }
    // Profiling only: one pending-WAL scan, never used as a safety certificate.
    void ObserveRecoveryBacklog(const char* reason);

    // ==================== 日志格式 v2（段式 + manifest，设计文档 §3） ====================
    bool IsV2Mode() const { return v2_mode_; }
    // ---- 写侧（LogManager 调用，append_mtx_ 内） ----
    uint64_t ManifestActiveSeg() const { return manifest_.active_seg_id; }
    uint64_t ManifestWriteEndAddr() const { return v2_write_end_addr_; }
    uint64_t ManifestEpoch() const { return manifest_.epoch; }
    void SetMaxReplayOff(uint64_t addr);      // 精确设置日志尾（v2 块边界推进）
    void OnSegmentRolled(uint64_t new_seg_id); // 段滚动时更新 manifest.active_seg
    // ---- manifest 持久化 ----
    void PersistManifest();                   // 双写 manifest（epoch++）

    // ==================== 故障恢复接口 ====================
    /**
     * @brief 针对特定页面执行 Redo 回放
     * 
     * 扫描日志文件，找到与目标页面相关的日志记录，按 LSN 顺序应用所有
     * disk_lsn < log_lsn <= target_lsn 的日志。
     * 
     * @param table_name 表名
     * @param page_no 页面号
     * @param disk_lsn 磁盘上当前页面的 LSN
     * @param target_lsn 目标 LSN（GPLM 中记录的最后已知 LSN）
     * @param out_page_data 输出：回放后的完整页面数据（PAGE_SIZE 字节）
     * @return true 回放成功，out_page_data 包含最新数据
     * @return false 无需回放或回放失败
     */
    bool RedoForPage(const std::string& table_name, page_id_t page_no,
                     LLSN disk_lsn, LLSN target_lsn, char* out_page_data);

    /**
     * @brief 撤销故障节点所有未提交事务的修改
     *
     * 扫描日志文件，找到故障节点的所有事务，判断哪些已提交（有 BATCHEND 记录），
     * 对未提交事务反向执行 Undo 操作。
     *
     * P0 正确性修复：alive_node_ids 中的存活节点前缀事务跳过 Undo——存活节点
     * 的事务终局（BATCHEND/ABORTEND+补偿）由节点自己负责并随后到达；若不
     * 过滤，Undo 扫描窗口内"数据日志已到、终局未到"的存活节点在途事务会被
     * 误撤销（含已提交待确认的事务，导致已提交数据丢失）。
     *
     * @param failed_node_id 故障节点 ID
     * @param alive_node_ids 存活计算节点集合（空集合=不过滤，保持旧行为，仅
     *                       供旧测试兼容；生产恢复路径必须传完整存活列表）
     * @return int 撤销操作数；-1 表示前置条件或日志读取失败，不能发布
     */
    int UndoForFailedNode(node_id_t failed_node_id,
                          const std::set<node_id_t>& alive_node_ids = {});

    // 批量定向 Redo 的请求/结果
    struct RecoveryRedoRequest {
        std::string table_name;
        page_id_t page_no;
        LLSN disk_lsn;     // 磁盘上当前页面的 LSN
        LLSN target_lsn;   // GPLM 中记录的最后已知 LSN（0 表示丢失，回放到日志末尾）
    };

    struct RecoveryRedoResult {
        bool success = false;
        bool scan_complete = false;
        bool no_matching_redo = false;
        std::string page_data;   // success 时有效（PAGE_SIZE 字节）
        LLSN recovered_lsn = 0;  // success 时为回放后页面头的 LSN
    };

    /**
     * @brief 对一批页面执行定向 Redo 回放（性能优化）
     * 
     * RedoForPage 每页都要全量扫描日志文件，恢复耗时为 O(页面数 x 日志大小)。
     * 本接口只扫描一次日志文件，将各页面相关的日志记录分桶收集后逐页回放，
     * 复杂度降为 O(日志大小 + 各页回放开销)。
     * 结果顺序与 requests 一一对应。
     */
    std::vector<RecoveryRedoResult> RedoForPages(const std::vector<RecoveryRedoRequest>& requests);

    batch_id_t get_persist_batch_id() { 
        std::lock_guard<std::mutex> latch(latch2_);
        return persist_batch_id_; 
    }
    void set_table_name_map(std::unordered_map<table_id_t, std::string> table_name_map) {
        std::lock_guard<std::mutex> guard(table_fd_mutex_);
        table_name_map_ = std::move(table_name_map);
        table_fd_cache_.clear();
    }
    void pushLogintoHashTable(std::string s);
    bool overwriteFixedLine(const std::string& filename, int lineNumber, const std::string& newContent, int lineLength );
    const std::string& GetLogFilePath() const { return log_file_path_; }
    DiskManager* GetDiskManager() const { return disk_manager_; }
private:
    // 定向 Redo：单条日志的位置信息（RedoForPage / RedoForPages 共用）
    struct RedoEntry {
        LLSN lsn;
        uint64_t offset;
        uint32_t size;
    };

    /**
     * @brief 将按 LSN 排序的日志条目依次应用到页面数据上（RedoForPage / RedoForPages 共用）
     * @return 实际应用的日志条数
     */
    int ApplyRedoEntriesToPage(int fd, const RmFileHdr& file_hdr,
                               const std::vector<RedoEntry>& entries,
                               char* page_data /* PAGE_SIZE, in/out */);

    int ResolveTableFd(table_id_t table_id, const char* table_name_ptr, size_t table_name_size);
    std::string ResolveTableName(table_id_t table_id, const char* table_name_ptr, size_t table_name_size) const;

    // ==================== BLink / FSM 日志重放 ====================
    // BLINKINSERT/BLINKDELETE 是逻辑日志：在存储侧 blink 树上重做操作。
    // FSMUPDATE 重放 = ApplyFsmUpdate（匿名命名空间中的文件级实现）。
    // redo 与 UndoForFailedNode 的 undo 可能并发访问同一棵树，通过
    // 树句柄自带的 op_mutex 互斥
    void ApplyBLinkInsert(table_id_t blink_table_id, const std::string &table_name,
                          itemkey_t key, const Rid &rid);
    void ApplyBLinkDelete(table_id_t blink_table_id, const std::string &table_name,
                          itemkey_t key, const Rid &rid);
    void ApplyFSMUpdate(table_id_t fsm_table_id, const std::string &table_name,
                        uint32_t page_id, uint32_t free_space);
    class Semaphore {
    public:
        void Release(size_t n = 1) {
            std::lock_guard<std::mutex> guard(mutex_);
            count_ += n;
            for (size_t i = 0; i < n; ++i) {
                cv_.notify_one();
            }
        }

        void Acquire() {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&] { return count_ > 0; });
            --count_;
        }

    private:
        std::mutex mutex_;
        std::condition_variable cv_;
        size_t count_ = 0;
    };

    struct WaitingLog {
        std::shared_ptr<LogRecord> log;
        int curr_offset = 0;
        LLSN prev_llsn = 0;
    };

    struct PageWaitQueue {
        std::mutex mutex;
        std::deque<WaitingLog> queue;
        Semaphore semaphore;
    };

    void EnqueueWaitingLog(page_id_t page_id, const std::shared_ptr<LogRecord>& log, int curr_offset);
    void WakeWaitingLogs(page_id_t page_id, LLSN page_llsn);
    PageWaitQueue* FindOrCreateWaitQueue(page_id_t page_id);

    // ==================== P1 续：replay 目标页写合并缓存 ====================
    // 动机：r1c-natural-load-p2-001 实测 replay ~1.9MB/s——每条 heap 数据日志
    // 3 次同步小 IO（page0 读 + 目标页读 + 目标页写），数百万条日志下成为
    // 追平瓶颈（快照 pre-catchup 超时的根源之一）。缓存目标页（write-back
    // LRU，默认 8192 页 = 32MB），同页连续日志合并为一次最终写盘。
    // 键为表名哈希而非 fd：fd 会被 close_file/destroy_file 关闭并复用，
    // 以 fd 为键会在文件重建后写错目标（open_db 重建 blink/FSM 即此场景）。
    //
    // ==================== P0 正确性修复（replay cache fail-closed 契约）====================
    // 缺陷 1（丢页无持续错误）：flush/驱逐丢脏页（文件打不开等）原先只计数
    //   +LOG(ERROR)，无持续错误状态；周期 flush 又忽略返回值，丢失的脏页
    //   不会被重放重做（顺序 replay 已越过该日志），后续 WaitReplayCaughtUp
    //   可能"追平"成功——静默数据丢失。
    // 缺陷 2（直写后缓存残留）：flush 只清 dirty 标志不清缓存内容；Undo/
    //   定向 Redo 直写磁盘后，replay 线程新日志在残留的旧缓存内容上应用，
    //   已撤销数据可能复活并覆盖恢复结果。
    // 修复：
    //   a) replay_cache_poisoned_：任何丢页路径置位；WaitReplayCaughtUp 检查
    //      到 poisoned 一律返回 false（fail-closed：宁可恢复失败保持隔离，
    //      不把可能丢页的状态当"追平"）。仅进程重启可清除。
    //   b) InvalidateAllReplayPages()：Undo/Phase4 Redo 等直写磁盘的恢复
    //      路径结束后调用（在 replay 暂停窗口内，此时缓存已全部 flush 且
    //      clean，移除无数据损失）；若发现仍有 dirty 页（前置 flush 契约
    //      被破坏）则置 poisoned。open_db 重建文件后同样调用（消除表名
    //      哈希跨文件代际的残留风险）。
    struct ReplayCacheKey {
        uint64_t path_hash;
        page_id_t page_no;
        bool operator==(const ReplayCacheKey& o) const {
            return path_hash == o.path_hash && page_no == o.page_no;
        }
    };
    struct ReplayCacheKeyHash {
        size_t operator()(const ReplayCacheKey& k) const {
            return k.path_hash ^ (static_cast<uint64_t>(k.page_no) << 32);
        }
    };
    struct ReplayPageEntry {
        std::array<char, PAGE_SIZE> data;
        bool dirty = false;
        std::string table_name;  // flush/驱逐时重新解析 fd
    };
    std::unordered_map<ReplayCacheKey, std::unique_ptr<ReplayPageEntry>, ReplayCacheKeyHash> replay_pages_;
    std::list<ReplayCacheKey> replay_lru_;
    std::unordered_map<ReplayCacheKey, std::list<ReplayCacheKey>::iterator, ReplayCacheKeyHash> replay_lru_pos_;
    mutable std::mutex replay_cache_mtx_;
    size_t replay_page_capacity_ = 8192;
    uint64_t replay_page_flush_writes_ = 0;   // 计量：合并后实际写盘次数
    uint64_t replay_page_flush_batches_ = 0;  // 计量：flush 调用次数
    uint64_t replay_page_flush_drops_ = 0;    // 计量：flush/驱逐失败丢弃页数
    uint64_t applied_since_flush_ = 0;         // 距上次周期落盘的 apply 条数
    // 以下 helper 均要求调用者已持有 replay_cache_mtx_
    char* AcquireReplayPageLocked(int fd, const std::string& table_name, page_id_t page_no);
    void MarkReplayPageDirtyLocked(int fd, const std::string& table_name, page_id_t page_no);
    void EvictReplayPagesLocked();
    // 落盘全部脏页；返回是否有失败（丢弃页）。不抛异常。
    bool FlushReplayPagesLocked();
    // 直接调用方的同步落盘（写盘+移出缓存）；失败抛 runtime_error
    void SyncReplayPageToDiskLocked(const std::string& table_name, page_id_t page_no);
    // 缓存整体失效（要求调用者已持有 replay_cache_mtx_）：移除全部缓存页。
    // 前置契约：调用点处于 PauseReplay 窗口且已 FlushReplayPages 成功
    // （此时缓存全部 clean，移除无数据损失）。发现 dirty 页即置 poisoned。
    void InvalidateAllReplayPagesLocked();

public:
    // 公共失效入口（自身加锁）：Undo/定向 Redo 直写磁盘的恢复路径结束后、
    // open_db 重建文件后调用。返回是否发现 dirty 页（true=已置 poisoned）。
    bool InvalidateAllReplayPages() {
        std::lock_guard<std::mutex> lock(replay_cache_mtx_);
        return InvalidateAllReplayPagesLocked();
    }
    // 诊断：缓存是否已 poison（丢页未恢复）
    bool ReplayCachePoisoned() const { return replay_cache_poisoned_.load(std::memory_order_acquire); }

private:
    // 丢页持续错误状态：置位后所有 WaitReplayCaughtUp/ValidationCut 失败，
    // 恢复保持隔离（fail-closed）。仅进程重启可清除。
    std::atomic<bool> replay_cache_poisoned_{false};


    int log_replay_fd_ = -1;        // 重放log文件fd（legacy 模式），从头开始顺序读
    int log_write_head_fd_ = -1;    // 写文件头fd（legacy 模式）

    std::mutex latch1_;             // 用于保护max_replay_off_这一共享变量
    uint64_t max_replay_off_;         // log文件中最后一个字节的偏移量

    std::mutex latch2_;              // 用于保护persist_batch_id_和persist_off_两个共享变量
    batch_id_t persist_batch_id_;   // 已经可持久化的batch的id
    uint64_t persist_off_;            // 已经可持久化的batch的最后一个字节的偏移量

    DiskManager* disk_manager_;
    LogBuffer buffer_;

    std::unordered_map<table_id_t, std::string> table_name_map_;
    std::unordered_map<table_id_t, int> table_fd_cache_;
    std::mutex table_fd_mutex_;

    std::unordered_map<page_id_t, PageWaitQueue> page_wait_queues_;
    std::mutex page_wait_mutex_;

    std::atomic<bool> replay_stop{false};
    std::thread replay_thread_;
    std::thread checkpoint_thread_;//检查点进程，负责将wal应用到磁盘，替换replay_thread_
    std::condition_variable cv_; // 条件变量

    // 重放暂停锁：replayFun 每轮循环开头短暂持锁，PauseReplay 拿住该锁
    // 即可阻塞重放线程（open_db 重建 blink/FSM 文件、Phase 4 恢复期间使用）。
    // 使用递归锁以支持嵌套暂停（如 AnalyzeRecoveryPages 外层暂停 +
    // UndoForFailedNode 内部暂停）
    std::recursive_mutex replay_pause_mtx_;

    // ==================== v2 状态 ====================
    bool v2_mode_ = false;
    logfmt::ManifestRecord manifest_;
    std::mutex manifest_mtx_;          // manifest_ 与 v2_write_end_addr_ 的互斥
    uint64_t v2_write_end_addr_ = 0;   // 日志写位置（逻辑偏移，重启时由段文件尾部校验恢复）
    std::unordered_map<uint64_t, int> v2_seg_fd_cache_;  // 段文件 fd 缓存（读侧）
    std::mutex v2_seg_fd_mtx_;

    // ==================== 独立 undo 区（docs/UNDO_DESIGN.md） ====================
    // undo 信息脱离 WAL 独立存放于 undo/ 目录，按事务串链；replay 线程 apply
    // 前 TrackLog，BATCHEND 重放时 CommitTxn 回收。UndoForFailedNode 优先走
    // 事务表+undo 链（免全扫 WAL），不可用时回退原两遍全扫路径。
    std::unique_ptr<UndoArea> undo_area_;

    void InitV2Storage();              // v2 初始化/加载（构造时调用）
    void WriteManifestTo(const std::string& path);  // 单份写（PersistManifest 调两次）
    ssize_t PreadSegment(uint64_t seg_id, char* buf, size_t size, uint64_t seg_off);  // 段读（fd 缓存）
    uint64_t RecoverWriteEndAddr(uint64_t seg_id);  // 启动时从段尾向前校验恢复写位置
    void replayFunV2();                // v2 重放循环（LogStreamReader 驱动）

public:
    // ==================== 统一日志扫描层（设计文档 §4.3） ====================
    // 全量扫描所有日志：v2 用 LogStreamReader 按块校验迭代，legacy 用原
    // 字节流分块扫描。回调 cb(完整记录指针, 记录长度, 记录逻辑偏移)，
    // 返回 false 停止扫描。供 RedoForPage(s)/UndoForFailedNode 复用，
    // 消除三份几乎相同的扫描代码
    bool ScanAllLogs(uint64_t end_addr, const std::function<bool(const char*, uint32_t, uint64_t)>& cb);

    // 按逻辑偏移读单条完整记录（定向 Redo/Undo 用）
    bool ReadLogRecordAt(uint64_t addr, char* out, uint32_t cap, uint32_t& out_len);

private:

    int num_records_per_page_;
    int bitmap_size_;

    std::string log_file_path_;

    // SQL
    SmManager *sm_manager;
    std::string mode;   //sql , ycsb , tpcc , smallbank

public:
    //std::unordered_map<int,int>checkpoint;//记录每个页面id对应的持久化的llsn最大值
    std::unordered_map<PageId, std::pair<std::mutex, int>> pageid_batch_count_;// 记录每个pageid上的log batch的数量
    std::mutex latch3_;             // 用于保护pageid_batch_count_这一共享变量
    std::unordered_map<int, std::vector<LLSN>> llsnrecord;
    // Print llsnrecord map (one line per key)
    void print_llsnrecord();
};