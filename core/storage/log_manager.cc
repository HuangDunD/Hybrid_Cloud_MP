#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#include "log_manager.h"

LogManager::LogManager(DiskManager* disk_manager, LogReplay* log_replay, std::string log_file_name)
        :disk_manager_(disk_manager), log_replay_(log_replay) {
    v2_mode_ = log_replay_ != nullptr && log_replay_->IsV2Mode();
    if (v2_mode_) {
        // v2 模式：从 manifest 恢复写入状态（active 段 + 写位置）
        cur_seg_id_ = log_replay_->ManifestActiveSeg();
        uint64_t end_addr = log_replay_->ManifestWriteEndAddr();
        cur_seg_write_off_ = logfmt::AddrSegOff(end_addr);
        if (cur_seg_write_off_ < logfmt::SEG_HEADER_SIZE) {
            cur_seg_write_off_ = logfmt::SEG_HEADER_SIZE;
        }
        // 块序号 = 写位置对应的块序号（段内块连续编号）
        cur_block_seq_ = (uint32_t)((cur_seg_write_off_ - logfmt::SEG_HEADER_SIZE) / logfmt::BLOCK_SIZE);
        cur_seg_fd_ = -1;  // 首次写入时懒打开
        return;
    }

    // legacy 模式：沿用原 LOG_FILE 单文件
    if(log_file_name != LOG_FILE_NAME){
        if(disk_manager_->is_file(log_file_name)) {
            disk_manager_->destroy_file(log_file_name);
        }
        disk_manager_->create_file(log_file_name);
    }
    log_file_fd_ = disk_manager_->open_file(log_file_name);
}

LogManager::~LogManager() {
    if (cur_seg_fd_ >= 0) {
        ::close(cur_seg_fd_);
    }
}

void LogManager::write_batch_log_to_disk(std::string batch_log) {
    write_batch_log_to_disk(const_cast<char*>(batch_log.data()), batch_log.length());
}

void LogManager::write_raft_log_to_disk(std::string batch_log){
    // raft 日志与数据日志共用同一日志流
    write_batch_log_to_disk(batch_log);
}

void LogManager::write_batch_log_to_disk(char* batch_log, size_t size) {
    // C1 修复：追加全程持锁。lseek(SEEK_END)+write 不是原子的，
    // 多计算节点并发 LogWrite 时会互相覆盖对方的日志
    std::lock_guard<std::mutex> append_lk(append_mtx_);

    if (v2_mode_) {
        AppendBatchV2(batch_log, size);
        return;
    }

    // ---- legacy 写入路径 ----
    if (log_file_fd_ == -1) {
        log_file_fd_ = disk_manager_->open_file(LOG_FILE_NAME);
    }

    lseek(log_file_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_file_fd_, batch_log, size);
    assert(bytes_write == (ssize_t)size);

    log_replay_->add_max_replay_off_(bytes_write);
}

// ==================== v2 写入实现 ====================

void LogManager::AppendBatchV2(const char* data, size_t len) {
    // 按 v1 记录边界把字节流切成 ≤ BLOCK_PAYLOAD_MAX 的段，逐块写入。
    // 记录不跨块：切块必须落在记录边界上
    size_t pos = 0;
    while (pos < len) {
        size_t chunk = 0;
        uint16_t rec_cnt = 0;
        while (pos + chunk < len) {
            if (pos + chunk + OFFSET_LOG_TOT_LEN + sizeof(uint32_t) > len) {
                break;  // 剩余不足一条记录头（不应发生：RPC 批次为完整记录流）
            }
            uint32_t rec_len = *reinterpret_cast<const uint32_t*>(
                data + pos + chunk + OFFSET_LOG_TOT_LEN);
            if (rec_len == 0 || rec_len > logfmt::BLOCK_PAYLOAD_MAX) {
                break;
            }
            if (chunk + rec_len > logfmt::BLOCK_PAYLOAD_MAX) {
                break;  // 本块已满，落盘
            }
            if (pos + chunk + rec_len > len) {
                break;  // 批次尾部不完整记录（不应发生），防御性截断
            }
            chunk += rec_len;
            rec_cnt++;
        }
        if (chunk == 0) {
            // 防御：批次损坏或单条记录超块载荷（本系统记录 ~1KB 级，不可能）
            LOG(ERROR) << "[LogManagerV2] AppendBatch: cannot slice records at pos="
                       << pos << " len=" << len;
            assert(false);
            break;
        }
        WriteBlockV2(data + pos, (uint32_t)chunk, rec_cnt);
        pos += chunk;
    }
}

void LogManager::WriteBlockV2(const char* payload, uint32_t payload_len, uint16_t rec_cnt) {
    if (cur_seg_fd_ < 0) {
        OpenSegmentV2(cur_seg_id_);
    }
    // 段剩余空间不足一整块则滚动
    if (cur_seg_write_off_ + logfmt::BLOCK_SIZE > logfmt::SEG_SIZE) {
        RollSegmentV2();
    }

    char block[logfmt::BLOCK_SIZE];
    memset(block, 0, sizeof(block));

    logfmt::BlockHeader hdr;
    hdr.block_seq = cur_block_seq_++;
    hdr.record_cnt = rec_cnt;
    hdr.flags = 0;
    hdr.payload_len = payload_len;
    hdr.crc = logfmt::BlockHeader::ComputeCrc(hdr.block_seq, rec_cnt, 0, payload, payload_len);
    hdr.Serialize(block);
    memcpy(block + logfmt::BLOCK_HEADER_SIZE, payload, payload_len);

    // 单块一次 pwrite 落盘；块内自校验使读侧可识别半写块（视为日志尾）
    ssize_t n = pwrite(cur_seg_fd_, block, logfmt::BLOCK_SIZE, (off_t)cur_seg_write_off_);
    assert(n == (ssize_t)logfmt::BLOCK_SIZE);

    cur_seg_write_off_ += logfmt::BLOCK_SIZE;
    // 推进日志尾逻辑偏移（块占用完整 32KB 逻辑空间，含填充）
    log_replay_->SetMaxReplayOff(logfmt::MakeAddr(cur_seg_id_, cur_seg_write_off_) - 1);
}

void LogManager::RollSegmentV2() {
    if (cur_seg_fd_ >= 0) {
        if (::fdatasync(cur_seg_fd_) != 0) throw std::runtime_error("WAL segment sync failed");
        ::close(cur_seg_fd_);
        cur_seg_fd_ = -1;
    }
    cur_seg_id_++;
    cur_seg_write_off_ = logfmt::SEG_HEADER_SIZE;
    cur_block_seq_ = 0;
    OpenSegmentV2(cur_seg_id_);
    // 通知 manifest：活跃段已切换
    log_replay_->OnSegmentRolled(cur_seg_id_);
}

void LogManager::OpenSegmentV2(uint64_t seg_id) {
    std::string path = logfmt::SegFileName(seg_id);
    bool need_init = !disk_manager_->is_file(path);

    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    assert(fd >= 0);
    cur_seg_fd_ = fd;

    if (need_init) {
        // 写段头
        char hdr_buf[logfmt::SEG_HEADER_SIZE];
        memset(hdr_buf, 0, sizeof(hdr_buf));
        logfmt::SegHeader sh;
        sh.magic = logfmt::SEG_MAGIC;
        sh.version = logfmt::LOG_FORMAT_VERSION;
        sh.seg_id = seg_id;
        sh.create_epoch = log_replay_->ManifestEpoch();
        sh.Serialize(hdr_buf);
        ssize_t n = pwrite(fd, hdr_buf, sizeof(hdr_buf), 0);
        assert(n == (ssize_t)sizeof(hdr_buf));
    }
}
