#pragma once

#include <string>

#include "common.h"
#include "storage/disk_manager.h"
#include "storage/log_format.h"
#include "logreplay.h"
#include <mutex>

class LogManager {
public:
    LogManager(DiskManager* disk_manager, LogReplay* log_replay, std::string log_file_name = LOG_FILE_NAME);
    ~LogManager();

    // lsn_t add_log_to_buffer(std::string log_record);
    void write_batch_log_to_disk(std::string batch_log);
    void write_batch_log_to_disk(char* batch_log, size_t size);
    void write_raft_log_to_disk(std::string batch_log);

    int log_file_fd_ = -1;           // legacy 模式的日志文件 fd（v2 模式不使用）
    DiskManager* disk_manager_;
    LogReplay* log_replay_;

    // 追加互斥锁：lseek(SEEK_END)+write 不是原子操作，多个计算节点并发
    // LogWrite（brpc 服务端多线程处理）时会互相覆盖对方的日志数据。
    // 三个写入口共用同一把锁，保证日志追加的原子性与顺序性
    std::mutex append_mtx_;

private:
    // ==================== v2 段式写入（设计文档 §3.1/§3.2/§4.2） ====================
    // AppendBatchV2 将 v1 记录流按记录边界切成 32KB 自描述块写入当前段，
    // 段满滚动；每写完一块推进 max_replay_off_（逻辑偏移）
    void AppendBatchV2(const char* data, size_t len);
    void WriteBlockV2(const char* payload, uint32_t payload_len, uint16_t rec_cnt);
    void RollSegmentV2();
    void OpenSegmentV2(uint64_t seg_id);

    bool v2_mode_ = false;

    // 写入侧状态（仅 append_mtx_ 内访问）
    uint64_t cur_seg_id_ = 1;          // 当前写入段
    int cur_seg_fd_ = -1;
    uint64_t cur_seg_write_off_ = logfmt::SEG_HEADER_SIZE;  // 段内物理写偏移
    uint32_t cur_block_seq_ = 0;       // 段内块序号
};
