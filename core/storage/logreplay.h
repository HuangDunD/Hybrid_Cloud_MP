#pragma once

#include <unistd.h>
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
    void apply_sigle_log(LogRecord* log_record, uint64_t curr_offset);
    void apply_sigle_log(const std::shared_ptr<LogRecord>& log_record, int curr_offset, bool allow_enqueue);
    void apply_undo_log(const LogRecord* log_record);
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
    void PauseReplay() { replay_pause_mtx_.lock(); }
    void ResumeReplay() { replay_pause_mtx_.unlock(); }

    // 等待重放线程追平到调用时刻的日志尾（persist_off_ >= max_replay_off_）。
    // Undo 前必须调用：undo 基于"日志已物化"假设，若 undo 快于重放，
    // 未提交日志随后会被 replay 再次物化，导致已撤销的脏数据复活
    bool WaitReplayCaughtUp(int timeout_ms = 30000);
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

    /**
     * @brief 撤销故障节点所有未提交事务的修改
     * 
     * 扫描日志文件，找到故障节点的所有事务，判断哪些已提交（有 BATCHEND 记录），
     * 对未提交事务反向执行 Undo 操作。
     * 
     * @param failed_node_id 故障节点 ID
     * @return int 撤销操作数；-1 表示前置条件或日志读取失败，不能发布
     */
    int UndoForFailedNode(node_id_t failed_node_id);
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