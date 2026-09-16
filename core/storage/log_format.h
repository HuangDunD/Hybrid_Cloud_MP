#pragma once

// ============================================================================
// 日志存储结构 v2：段式文件 + 自描述块 + CRC32C + 双写 manifest
//
// 设计文档：docs/LOG_SYSTEM_DESIGN.md §3.1/§3.2
//
// 布局：
//   log_v2/
//     manifest.0 / manifest.1     元数据双写（epoch 大者生效，CRC 校验）
//     seg_<seg_id:016x>.log       64MB 定长段，写满滚动
//
//   SegFile  = [SegHeader 4KB] + Block[0..N]
//   Block    = [BlockHeader 16B] + payload_len 字节 v1 记录流 + 填充至 32KB
//              记录不跨块、块不跨段；每块自校验、可独立解析
//
// 关键抽象：逻辑偏移（LogAddr）把多段拼接成逻辑大文件，
//   addr = seg_id * SEG_SIZE + seg 内物理偏移
// 调用方（replayFun / RedoForPages / UndoForFailedNode）继续使用线性偏移，
// 无需感知段与块的存在
// ============================================================================

#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

#include <butil/crc32c.h>

#include "common.h"
#include "storage/log_record.h"   // OFFSET_LOG_TOT_LEN（v1 记录长度字段偏移）

namespace logfmt {

// ---------------- 常量 ----------------
constexpr const char* LOG_V2_DIR = "log_v2";
constexpr const char* MANIFEST_0 = "log_v2/manifest.0";
constexpr const char* MANIFEST_1 = "log_v2/manifest.1";

constexpr uint64_t SEG_SIZE = 64ull * 1024 * 1024;   // 段大小 64MB
constexpr uint32_t SEG_HEADER_SIZE = 4096;           // 段头 4KB
constexpr uint32_t BLOCK_SIZE = 32 * 1024;           // 块大小 32KB
constexpr uint32_t BLOCK_HEADER_SIZE = 16;           // 块头 16B
constexpr uint32_t BLOCK_PAYLOAD_MAX = BLOCK_SIZE - BLOCK_HEADER_SIZE;

constexpr uint32_t MANIFEST_MAGIC = 0x4D414E49;      // "MANI"
constexpr uint32_t SEG_MAGIC = 0x4C534547;           // "LSEG"
constexpr uint32_t LOG_FORMAT_VERSION = 2;

constexpr uint16_t BLOCK_FLAG_PAD = 0x1;             // 填充块（段尾剩余空间）

// ---------------- 逻辑偏移（LogAddr） ----------------
// addr = seg_id * SEG_SIZE + seg 内物理偏移（seg 内偏移含 4KB 段头）
inline uint64_t MakeAddr(uint64_t seg_id, uint64_t seg_off) {
    return seg_id * SEG_SIZE + seg_off;
}
inline uint64_t AddrSegId(uint64_t addr) { return addr / SEG_SIZE; }
inline uint64_t AddrSegOff(uint64_t addr) { return addr % SEG_SIZE; }
// 段内第一个可用块的起始逻辑偏移
inline uint64_t SegFirstBlockAddr(uint64_t seg_id) {
    return MakeAddr(seg_id, SEG_HEADER_SIZE);
}

inline std::string SegFileName(uint64_t seg_id) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%s/seg_%016lx.log", LOG_V2_DIR, (unsigned long)seg_id);
    return std::string(buf);
}

// ---------------- 段头 ----------------
struct SegHeader {
    uint32_t magic;
    uint32_t version;
    uint64_t seg_id;
    uint64_t create_epoch;

    void Serialize(char* dest) const {
        memcpy(dest + 0, &magic, 4);
        memcpy(dest + 4, &version, 4);
        memcpy(dest + 8, &seg_id, 8);
        memcpy(dest + 16, &create_epoch, 8);
    }
    bool Deserialize(const char* src) {
        memcpy(&magic, src + 0, 4);
        memcpy(&version, src + 4, 4);
        memcpy(&seg_id, src + 8, 8);
        memcpy(&create_epoch, src + 16, 8);
        return magic == SEG_MAGIC && version == LOG_FORMAT_VERSION;
    }
};

// ---------------- 块头 ----------------
// [crc32c(4) | block_seq(4) | record_cnt(2) | flags(2) | payload_len(4)]
struct BlockHeader {
    uint32_t crc;          // Mask(crc32c(block_seq .. 块尾)，即覆盖本字段之后全部内容)
    uint32_t block_seq;    // 段内块序号
    uint16_t record_cnt;
    uint16_t flags;
    uint32_t payload_len;  // 块内记录区实际字节数（<= BLOCK_PAYLOAD_MAX）

    static uint32_t ComputeCrc(uint32_t block_seq, uint16_t record_cnt, uint16_t flags,
                               const char* payload, uint32_t payload_len) {
        uint32_t c = butil::crc32c::Extend(0, reinterpret_cast<const char*>(&block_seq), 4);
        c = butil::crc32c::Extend(c, reinterpret_cast<const char*>(&record_cnt), 2);
        c = butil::crc32c::Extend(c, reinterpret_cast<const char*>(&flags), 2);
        c = butil::crc32c::Extend(c, payload, payload_len);
        return butil::crc32c::Mask(c);
    }

    void Serialize(char* dest) const {
        memcpy(dest + 0, &crc, 4);
        memcpy(dest + 4, &block_seq, 4);
        memcpy(dest + 8, &record_cnt, 2);
        memcpy(dest + 10, &flags, 2);
        memcpy(dest + 12, &payload_len, 4);
    }
    void Deserialize(const char* src) {
        memcpy(&crc, src + 0, 4);
        memcpy(&block_seq, src + 4, 4);
        memcpy(&record_cnt, src + 8, 2);
        memcpy(&flags, src + 10, 2);
        memcpy(&payload_len, src + 12, 4);
    }
    // 校验：长度合法 + CRC 匹配（payload 指针指向块头之后）
    bool Verify(const char* payload) const {
        if (payload_len > BLOCK_PAYLOAD_MAX) return false;
        uint32_t expect = ComputeCrc(block_seq, record_cnt, flags, payload, payload_len);
        return expect == crc;
    }
};

// ---------------- manifest ----------------
// 定长 96B 写入；crc 覆盖前 40 字节（magic..checkpoint_addr）
struct ManifestRecord {
    uint32_t magic = MANIFEST_MAGIC;
    uint32_t version = LOG_FORMAT_VERSION;
    uint64_t epoch = 0;              // 单调递增，双写取大者
    uint64_t active_seg_id = 0;      // 当前写入段
    uint64_t replayed_addr = 0;      // 重放进度（下一条待重放记录的逻辑偏移）
    uint64_t checkpoint_addr = 0;    // 最近 checkpoint 点（段回收留给阶段三）
    uint32_t crc = 0;

    static constexpr size_t kBodyLen = 4 + 4 + 8 + 8 + 8 + 8;  // 不含 crc 的正文长度

    uint32_t ComputeCrc() const {
        char buf[kBodyLen];
        memcpy(buf + 0, &magic, 4);
        memcpy(buf + 4, &version, 4);
        memcpy(buf + 8, &epoch, 8);
        memcpy(buf + 16, &active_seg_id, 8);
        memcpy(buf + 24, &replayed_addr, 8);
        memcpy(buf + 32, &checkpoint_addr, 8);
        return butil::crc32c::Mask(butil::crc32c::Value(buf, kBodyLen));
    }

    void Serialize(char* dest) const {
        memcpy(dest + 0, &magic, 4);
        memcpy(dest + 4, &version, 4);
        memcpy(dest + 8, &epoch, 8);
        memcpy(dest + 16, &active_seg_id, 8);
        memcpy(dest + 24, &replayed_addr, 8);
        memcpy(dest + 32, &checkpoint_addr, 8);
        uint32_t c = ComputeCrc();
        memcpy(dest + 40, &c, 4);
    }
    bool Deserialize(const char* src) {
        memcpy(&magic, src + 0, 4);
        memcpy(&version, src + 4, 4);
        memcpy(&epoch, src + 8, 8);
        memcpy(&active_seg_id, src + 16, 8);
        memcpy(&replayed_addr, src + 24, 8);
        memcpy(&checkpoint_addr, src + 32, 8);
        memcpy(&crc, src + 40, 4);
        return magic == MANIFEST_MAGIC && version == LOG_FORMAT_VERSION && crc == ComputeCrc();
    }
};

// ============================================================================
// LogStreamReader：跨段跨块的 v1 记录流迭代器
//
// - Next()：从当前位置顺序取下一条完整记录（零拷贝，指向内部块缓冲）
// - ReadRecordAt()：按逻辑偏移随机读一条完整记录（定向 Redo/Undo 用）
// - 块 CRC 校验失败 / 段不存在 / 到达 end_addr 均视为日志尾（安全截断）
//
// 记录不跨块、块不跨段（由写入侧 LogManager 保证）
// ============================================================================
class LogStreamReader {
public:
    // 段文件读取回调：pread 语义，返回实际读取字节数（<0 失败）
    using PreadFn = std::function<ssize_t(uint64_t seg_id, char* buf, size_t size, uint64_t seg_off)>;

    explicit LogStreamReader(PreadFn fn) : pread_fn_(std::move(fn)) {}

    // 定位到逻辑偏移。addr 应为某条记录的起点；若落在块头/段头区域
    // 则自动对齐到该块 payload 起点。定位只计算位置，首次 Next() 时才读块
    void Seek(uint64_t addr) {
        cur_block_addr_ = BlockAddrOf(addr);
        uint64_t payload_start = cur_block_addr_ + BLOCK_HEADER_SIZE;
        if (addr <= payload_start) {
            pos_in_payload_ = 0;
        } else {
            pos_in_payload_ = static_cast<uint32_t>(addr - payload_start);
        }
        block_loaded_ = false;
    }

    // 当前迭代位置（下一条记录的逻辑偏移；用于重放进度持久化）
    uint64_t CurAddr() const {
        return cur_block_addr_ + BLOCK_HEADER_SIZE + pos_in_payload_;
    }

    // Only a fully consumed, CRC-verified block may advance over its padding.
    bool AtEnd(uint64_t end_addr) const { return cur_block_addr_ >= end_addr; }
    uint64_t NextReplayAddr() const {
        if (block_loaded_ && pos_in_payload_ == payload_len_) {
            uint64_t next = cur_block_addr_ + BLOCK_SIZE;
            return AddrSegOff(next) + BLOCK_SIZE > SEG_SIZE
                ? SegFirstBlockAddr(AddrSegId(cur_block_addr_) + 1) : next;
        }
        return CurAddr();
    }

    // 顺序取下一条完整记录。
    // end_addr 为日志尾的排他边界（max_replay_off_ + 1）。
    // 返回 true：*out/*out_len/*out_addr 有效（out 指向内部缓冲，下次调用失效）。
    // 返回 false：到达日志尾，或遇到坏块/半写块（安全截断）。
    bool Next(const char*& out, uint32_t& out_len, uint64_t& out_addr, uint64_t end_addr) {
        while (true) {
            if (!block_loaded_) {
                if (!LoadBlock(cur_block_addr_, end_addr)) {
                    return false;
                }
                block_loaded_ = true;
            }
            if (pos_in_payload_ == payload_len_) {
                if (!AdvanceBlock(end_addr)) return false;
                continue;
            }
            if (pos_in_payload_ + LOG_HEADER_SIZE > payload_len_) return false;
            uint32_t rec_len = *reinterpret_cast<const uint32_t*>(
                block_ + BLOCK_HEADER_SIZE + pos_in_payload_ + OFFSET_LOG_TOT_LEN);
            if (rec_len < LOG_HEADER_SIZE || rec_len > BLOCK_PAYLOAD_MAX ||
                pos_in_payload_ + rec_len > payload_len_) {
                // 半写/损坏记录：安全截断为日志尾
                return false;
            }
            out = block_ + BLOCK_HEADER_SIZE + pos_in_payload_;
            out_len = rec_len;
            out_addr = cur_block_addr_ + BLOCK_HEADER_SIZE + pos_in_payload_;
            pos_in_payload_ += rec_len;
            return true;
        }
    }

    // 随机读一条完整记录（addr 为记录起点逻辑偏移）
    bool ReadRecordAt(uint64_t addr, char* out, uint32_t out_cap, uint32_t& out_len) {
        if (addr == 0) return false;
        uint64_t seg_id = AddrSegId(addr);
        uint64_t seg_off = AddrSegOff(addr);
        // 先读记录头拿总长
        char hdr[OFFSET_LOG_TOT_LEN + sizeof(uint32_t)];
        ssize_t n = pread_fn_(seg_id, hdr, sizeof(hdr), seg_off);
        if (n != (ssize_t)sizeof(hdr)) return false;
        uint32_t rec_len = *reinterpret_cast<const uint32_t*>(hdr + OFFSET_LOG_TOT_LEN);
        if (rec_len == 0 || rec_len > out_cap || rec_len > BLOCK_PAYLOAD_MAX) return false;
        n = pread_fn_(seg_id, out, rec_len, seg_off);
        if (n != (ssize_t)rec_len) return false;
        out_len = rec_len;
        return true;
    }

private:
    // addr 所在块的起始逻辑偏移
    static uint64_t BlockAddrOf(uint64_t addr) {
        uint64_t seg_id = AddrSegId(addr);
        uint64_t seg_off = AddrSegOff(addr);
        if (seg_off < SEG_HEADER_SIZE) {
            return SegFirstBlockAddr(seg_id);
        }
        uint64_t block_idx = (seg_off - SEG_HEADER_SIZE) / BLOCK_SIZE;
        return MakeAddr(seg_id, SEG_HEADER_SIZE + block_idx * BLOCK_SIZE);
    }

    // 推进到下一块（含跨段滚动）；超出日志尾则返回 false
    bool AdvanceBlock(uint64_t end_addr) {
        uint64_t seg_id = AddrSegId(cur_block_addr_);
        uint64_t seg_off = AddrSegOff(cur_block_addr_);
        uint64_t next_off = seg_off + BLOCK_SIZE;
        if (next_off + BLOCK_SIZE > SEG_SIZE) {
            // 滚动到下一段首块
            cur_block_addr_ = SegFirstBlockAddr(seg_id + 1);
        } else {
            cur_block_addr_ = MakeAddr(seg_id, next_off);
        }
        pos_in_payload_ = 0;
        block_loaded_ = false;
        return cur_block_addr_ + BLOCK_HEADER_SIZE < end_addr;
    }

    // 加载并校验 block_addr 处的块
    bool LoadBlock(uint64_t block_addr, uint64_t end_addr) {
        if (block_addr + BLOCK_HEADER_SIZE >= end_addr) {
            return false;
        }
        uint64_t seg_id = AddrSegId(block_addr);
        uint64_t seg_off = AddrSegOff(block_addr);
        ssize_t n = pread_fn_(seg_id, block_, BLOCK_SIZE, seg_off);
        if (n < (ssize_t)BLOCK_HEADER_SIZE) {
            return false;  // 段不存在/读到尾
        }
        BlockHeader hdr;
        hdr.Deserialize(block_);
        if (hdr.flags & BLOCK_FLAG_PAD) {
            return false;  // 填充块（段尾），由 AdvanceBlock 跨段
        }
        if (n < (ssize_t)(BLOCK_HEADER_SIZE + hdr.payload_len)) {
            return false;  // 半写块
        }
        if (hdr.payload_len > BLOCK_PAYLOAD_MAX) {
            return false;
        }
        if (!hdr.Verify(block_ + BLOCK_HEADER_SIZE)) {
            return false;  // CRC 校验失败：坏块安全截断
        }
        payload_len_ = hdr.payload_len;
        return true;
    }

private:
    PreadFn pread_fn_;
    char block_[BLOCK_SIZE]{};
    uint64_t cur_block_addr_ = 0;    // 当前块起始逻辑偏移
    uint32_t pos_in_payload_ = 0;    // 块内 payload 区解析位置
    uint32_t payload_len_ = 0;       // 当前块记录区长度
    bool block_loaded_ = false;
};

}  // namespace logfmt
