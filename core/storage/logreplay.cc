#include <algorithm>
#include <assert.h>
#include <atomic>
#include <ctime>
#include <iomanip>
#include <limits>
#include <map>
#include <vector>
#include <unordered_set>

#include "logreplay.h"
#include "core/recovery/observation.h"
#include "fsm_tree/s_fsm_tree.h"
#include "util/bitmap.h"

namespace {

uint8_t FsmSpaceToCategory(uint32_t free_space) {
    if (free_space == 0) return static_cast<uint8_t>(S_SpaceCategory::NO_SPACE);
    if (free_space >= PAGE_SIZE * 9 / 10) return static_cast<uint8_t>(S_SpaceCategory::EMPTY);
    if (free_space >= PAGE_SIZE * 2 / 3) return static_cast<uint8_t>(S_SpaceCategory::ALMOST_EMPTY);
    if (free_space >= PAGE_SIZE / 3) return static_cast<uint8_t>(S_SpaceCategory::HALF_FULL);
    if (free_space >= PAGE_SIZE / 10) return static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL);
    return static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL);
}

uint32_t GetParentIndex(uint32_t child) { return (child - 1) / 2; }
uint32_t GetLeftChild(uint32_t parent) { return 2 * parent + 1; }
uint32_t GetRightChild(uint32_t parent) { return 2 * parent + 2; }
bool IsLeafNode(const S_FSMPageData& page_data, uint32_t index) { return index >= page_data.header.leaf_start; }

void EnsureChildVector(S_FSMPageData& page_data) {
    if (page_data.header.page_type != S_FSMPageType::INTERNAL_PAGE) {
        page_data.child_page_ids.clear();
        return;
    }
    if (!page_data.child_page_ids.empty()) {
        return;
    }
    page_data.child_page_ids.reserve(page_data.header.child_count);
    for (uint32_t i = 0; i < page_data.header.child_count; ++i) {
        page_data.child_page_ids.push_back(page_data.header.first_child_page + i);
    }
}

uint32_t FindChildIndex(const S_FSMPageData& parent_page, uint32_t child_page_id) {
    if (parent_page.header.page_type != S_FSMPageType::INTERNAL_PAGE) {
        return std::numeric_limits<uint32_t>::max();
    }
    for (uint32_t i = 0; i < parent_page.child_page_ids.size(); ++i) {
        if (parent_page.child_page_ids[i] == child_page_id) {
            return i;
        }
    }
    if (parent_page.child_page_ids.empty() && parent_page.header.child_count > 0) {
        if (child_page_id >= parent_page.header.first_child_page &&
            child_page_id < parent_page.header.first_child_page + parent_page.header.child_count) {
            return child_page_id - parent_page.header.first_child_page;
        }
    }
    return std::numeric_limits<uint32_t>::max();
}

bool DeserializeFSMPage(S_FSMPageData& page_data, const char* buffer, uint32_t size) {
    if (size < sizeof(S_FSMPageHeader)) {
        return false;
    }

    S_FSMPageHeader header;
    std::memcpy(&header, buffer, sizeof(S_FSMPageHeader));

    if (header.magic_number != 0x46535047) {
        return false;
    }

    page_data.header = header;

    const size_t payload_bytes = size - sizeof(S_FSMPageHeader);
    const size_t child_bytes_needed = static_cast<size_t>(header.child_count) * sizeof(uint32_t);
    bool has_child_blob = (header.page_type == S_FSMPageType::INTERNAL_PAGE) &&
                         (payload_bytes >= header.node_count + child_bytes_needed);

    size_t node_bytes_limit = has_child_blob ? (payload_bytes - child_bytes_needed) : payload_bytes;
    const char* node_data = buffer + sizeof(S_FSMPageHeader);
    page_data.nodes.resize(std::min(static_cast<size_t>(header.node_count), node_bytes_limit));

    for (size_t i = 0; i < page_data.nodes.size(); ++i) {
        page_data.nodes[i] = S_FSMNode(static_cast<S_SpaceCategory>(static_cast<uint8_t>(node_data[i])));
    }

    const size_t node_bytes = page_data.nodes.size();
    size_t remaining = payload_bytes > node_bytes ? payload_bytes - node_bytes : 0;
    page_data.child_page_ids.clear();
    if (header.page_type == S_FSMPageType::INTERNAL_PAGE && header.child_count > 0) {
        if (has_child_blob && remaining >= child_bytes_needed) {
            page_data.child_page_ids.resize(header.child_count);
            const char* child_ptr = node_data + node_bytes;
            std::memcpy(page_data.child_page_ids.data(), child_ptr, child_bytes_needed);
        } else {
            page_data.child_page_ids.resize(header.child_count);
            for (uint32_t i = 0; i < header.child_count; ++i) {
                page_data.child_page_ids[i] = header.first_child_page + i;
            }
        }
    }

    page_data.is_loaded = true;
    page_data.is_dirty = false;
    return true;
}

bool SerializeFSMPage(S_FSMPageData& page_data, char* buffer, uint32_t size) {
    if (size < PAGE_SIZE) {
        return false;
    }

    if (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE) {
        EnsureChildVector(page_data);
        page_data.header.child_count = static_cast<uint32_t>(page_data.child_page_ids.size());
        page_data.header.first_child_page = page_data.child_page_ids.empty() ? 0 : page_data.child_page_ids.front();
    } else {
        page_data.child_page_ids.clear();
        page_data.header.child_count = 0;
        page_data.header.first_child_page = 0;
    }

    size_t child_bytes = (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE)
                             ? static_cast<size_t>(page_data.header.child_count) * sizeof(uint32_t)
                             : 0;
    size_t node_region_capacity = (size > sizeof(S_FSMPageHeader) + child_bytes)
                                      ? size - sizeof(S_FSMPageHeader) - child_bytes
                                      : 0;
    if (page_data.nodes.size() > node_region_capacity) {
        return false;
    }

    page_data.header.magic_number = 0x46535047;
    page_data.header.timestamp = static_cast<uint64_t>(time(nullptr));
    std::memcpy(buffer, &page_data.header, sizeof(S_FSMPageHeader));

    char* node_data = buffer + sizeof(S_FSMPageHeader);
    size_t node_bytes = std::min(page_data.nodes.size(), node_region_capacity);
    for (size_t i = 0; i < node_bytes; ++i) {
        node_data[i] = static_cast<char>(page_data.nodes[i].get_value());
        page_data.nodes[i].clear_dirty();
    }

    if (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE && page_data.header.child_count > 0) {
        char* child_ptr = node_data + node_bytes;
        std::memcpy(child_ptr, page_data.child_page_ids.data(), child_bytes);
    }

    return true;
}

bool LoadFSMPage(DiskManager* disk_manager, int fd, uint32_t page_id, S_FSMPageData& out) {
    char buffer[PAGE_SIZE];
    disk_manager->read_page(fd, page_id, buffer, PAGE_SIZE);
    return DeserializeFSMPage(out, buffer, PAGE_SIZE);
}

bool StoreFSMPage(DiskManager* disk_manager, int fd, uint32_t page_id, S_FSMPageData& page) {
    char buffer[PAGE_SIZE] = {0};
    if (!SerializeFSMPage(page, buffer, PAGE_SIZE)) {
        return false;
    }
    disk_manager->write_page(fd, page_id, buffer, PAGE_SIZE);
    return true;
}

bool RecomputeWithinPage(S_FSMPageData& page, uint32_t node_index) {
    uint8_t original_root = page.nodes.empty() ? 0 : page.nodes[0].get_value();
    while (node_index > 0) {
        uint32_t parent_index = GetParentIndex(node_index);
        uint32_t left_child = GetLeftChild(parent_index);
        uint32_t right_child = GetRightChild(parent_index);
        uint8_t left_value = (left_child < page.header.node_count) ? page.nodes[left_child].get_value() : 0;
        uint8_t right_value = (right_child < page.header.node_count) ? page.nodes[right_child].get_value() : 0;
        uint8_t new_parent_value = std::max(left_value, right_value);
        if (new_parent_value == page.nodes[parent_index].get_value()) {
            break;
        }
        page.nodes[parent_index].set_value(new_parent_value);
        node_index = parent_index;
    }
    return !page.nodes.empty() && page.nodes[0].get_value() != original_root;
}

using FsmPathEntry = std::pair<uint32_t, S_FSMPageData>;

bool BuildPathToLeaf(DiskManager* disk_manager, int fd, uint32_t page_id, uint32_t heap_page_id, std::vector<FsmPathEntry>& path) {
    S_FSMPageData page;
    if (!LoadFSMPage(disk_manager, fd, page_id, page)) {
        return false;
    }
    path.emplace_back(page_id, std::move(page));
    auto& current = path.back().second;
    if (current.header.page_type == S_FSMPageType::LEAF_PAGE) {
        bool manage = heap_page_id >= current.header.first_heap_page &&
                      heap_page_id < current.header.first_heap_page + current.header.heap_pages_count;
        if (manage) {
            return true;
        }
        path.pop_back();
        return false;
    }

    EnsureChildVector(current);
    for (uint32_t child_page_id : current.child_page_ids) {
        if (BuildPathToLeaf(disk_manager, fd, child_page_id, heap_page_id, path)) {
            return true;
        }
    }
    path.pop_back();
    return false;
}

bool ApplyFsmUpdate(DiskManager* disk_manager, int fd, uint32_t heap_page_id, uint32_t free_space) {
    char meta_buffer[PAGE_SIZE];
    disk_manager->read_page(fd, S_FSM_META_PAGE_ID, meta_buffer, PAGE_SIZE);
    S_FSMMetaData meta{};
    std::memcpy(&meta, meta_buffer, sizeof(S_FSMMetaData));
    if (meta.magic_number != 0x46534D54) {
        return false;
    }

    std::vector<FsmPathEntry> path;
    if (!BuildPathToLeaf(disk_manager, fd, meta.root_page_id, heap_page_id, path)) {
        return false;
    }

    auto& leaf_entry = path.back();
    auto& leaf_page = leaf_entry.second;
    uint32_t heap_offset = heap_page_id - leaf_page.header.first_heap_page;
    uint32_t leaf_index = leaf_page.header.leaf_start + heap_offset;
    if (leaf_index >= leaf_page.header.node_count) {
        return false;
    }

    uint8_t new_category = FsmSpaceToCategory(free_space);
    uint8_t old_category = leaf_page.nodes[leaf_index].get_value();
    if (new_category == old_category) {
        return true;
    }

    leaf_page.nodes[leaf_index].set_value(new_category);
    bool leaf_root_changed = RecomputeWithinPage(leaf_page, leaf_index);
    if (!StoreFSMPage(disk_manager, fd, leaf_entry.first, leaf_page)) {
        return false;
    }

    uint8_t child_root_value = leaf_page.nodes.empty() ? 0 : leaf_page.nodes[0].get_value();
    bool propagate = leaf_root_changed;
    for (int i = static_cast<int>(path.size()) - 2; i >= 0 && propagate; --i) {
        auto& parent_entry = path[static_cast<size_t>(i)];
        auto& parent_page = parent_entry.second;
        EnsureChildVector(parent_page);
        uint32_t child_offset = FindChildIndex(parent_page, path[static_cast<size_t>(i + 1)].first);
        if (child_offset == std::numeric_limits<uint32_t>::max()) {
            break;
        }
        uint32_t parent_node_index = parent_page.header.leaf_start + child_offset;
        if (parent_node_index >= parent_page.header.node_count) {
            break;
        }
        if (parent_page.nodes[parent_node_index].get_value() == child_root_value) {
            propagate = false;
            continue;
        }
        parent_page.nodes[parent_node_index].set_value(child_root_value);
        bool parent_root_changed = RecomputeWithinPage(parent_page, parent_node_index);
        if (!StoreFSMPage(disk_manager, fd, parent_entry.first, parent_page)) {
            return false;
        }
        child_root_value = parent_page.nodes.empty() ? 0 : parent_page.nodes[0].get_value();
        propagate = parent_root_changed;
    }
    return true;
}

}  // namespace

// ============================================================================
// 构造与初始化（v2 段式 / legacy 单文件 双模式，设计文档 §7 迁移策略）
// ============================================================================

LogReplay::LogReplay(DiskManager* disk_manager, SmManager *sm, const std::string m,
                     std::unordered_map<table_id_t, std::string> table_name_map)
    : disk_manager_(disk_manager), table_name_map_(std::move(table_name_map)), sm_manager(sm) {
    mode = m;
    char path[1024];
    getcwd(path, sizeof(path));
    log_file_path_ = std::string(path) + "/" + LOG_FILE_NAME;

    // ---- 模式检测 ----
    // 1. 存在 v2 manifest → v2
    // 2. 否则若旧 LOG_FILE 存在且非空 → legacy（旧系统升级，断点续放，
    //    不做在线切换；清空数据目录后即走 v2）
    // 3. 全新部署 → v2
    bool has_manifest = disk_manager_->is_file(logfmt::MANIFEST_0) ||
                        disk_manager_->is_file(logfmt::MANIFEST_1);
    if (has_manifest) {
        v2_mode_ = true;
    } else if (disk_manager_->is_file(log_file_path_) &&
               disk_manager_->get_file_size(log_file_path_) > 0) {
        v2_mode_ = false;
    } else {
        v2_mode_ = true;
    }

    if (v2_mode_) {
        InitV2Storage();
        LOG(INFO) << "[LogReplay] v2 segmented log storage initialized, replayed_addr="
                  << manifest_.replayed_addr << " write_end=" << v2_write_end_addr_;
    } else {
        InitLegacyStorage();
        LOG(INFO) << "[LogReplay] legacy single-file log storage, persist_off_=" << persist_off_;
    }

    // 独立 undo 区（与 WAL 格式无关，v1/v2 均启用；初始化失败自动禁用，
    // UndoForFailedNode 回退 WAL 全扫路径）。必须在 replay 线程启动前完成
    // 崩溃重建，保证 TrackLog/CommitTxn 挂载点就绪
    undo_area_ = std::make_unique<UndoArea>();

    replay_thread_ = std::thread(v2_mode_ ? &LogReplay::replayFunV2 : &LogReplay::replayFun, this);

    num_records_per_page_ = (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (sizeof(DataItem) + sizeof(itemkey_t)) * BITMAP_WIDTH);
    bitmap_size_ = (num_records_per_page_ + BITMAP_WIDTH - 1) / BITMAP_WIDTH;
}

void LogReplay::InitLegacyStorage() {
    if(!disk_manager_->is_file(log_file_path_)) {
        disk_manager_->create_file(log_file_path_);
        log_replay_fd_ = open(log_file_path_.c_str(), O_RDWR);
        log_write_head_fd_ = open(log_file_path_.c_str(), O_RDWR);

        persist_batch_id_ = 0;
        persist_off_ = sizeof(batch_id_t) + sizeof(size_t) - 1;

        write(log_write_head_fd_, &persist_batch_id_, sizeof(batch_id_t));
        write(log_write_head_fd_, &persist_off_, sizeof(size_t));
    }
    else {
        log_replay_fd_ = open(log_file_path_.c_str(), O_RDWR);
        log_write_head_fd_ = open(log_file_path_.c_str(), O_RDWR);

        off_t offset = lseek(log_replay_fd_, 0, SEEK_SET);
        if (offset == -1) {
            std::cerr << "Failed to seek log file." << std::endl;
            assert(0);
        }
        ssize_t bytes_read = read(log_replay_fd_, &persist_batch_id_, sizeof(batch_id_t));
        if(bytes_read != sizeof(batch_id_t)){
            std::cerr << "Failed to read persist_batch_id_." << std::endl;
            assert(0);
        }
        bytes_read = read(log_replay_fd_, &persist_off_, sizeof(size_t));
        persist_off_ -= 1;
        if(bytes_read != sizeof(size_t)){
            std::cerr << "Failed to read persist_off_." << std::endl;
            assert(0);
        }
    }

    max_replay_off_ = disk_manager_->get_file_size(log_file_path_) - 1;
    // 保留文件头中持久化的重放进度（断点续放），不再跳过重放；
    // 已应用日志的重复重放由 apply_sigle_log 的页面 LLSN 检查幂等跳过
}

// ==================== v2 存储初始化与 manifest 管理 ====================

void LogReplay::InitV2Storage() {
    // 确保 log_v2 目录存在
    if (!disk_manager_->is_dir(logfmt::LOG_V2_DIR)) {
        disk_manager_->create_dir(logfmt::LOG_V2_DIR);
    }

    // 读 manifest 双份，CRC 通过且 epoch 大者生效（单份撕裂可回退）
    logfmt::ManifestRecord m0, m1;
    bool ok0 = false, ok1 = false;
    {
        char buf[64];
        int fd = ::open(logfmt::MANIFEST_0, O_RDONLY);
        if (fd >= 0) {
            ok0 = (::pread(fd, buf, sizeof(buf), 0) == (ssize_t)sizeof(buf)) && m0.Deserialize(buf);
            ::close(fd);
        }
        fd = ::open(logfmt::MANIFEST_1, O_RDONLY);
        if (fd >= 0) {
            ok1 = (::pread(fd, buf, sizeof(buf), 0) == (ssize_t)sizeof(buf)) && m1.Deserialize(buf);
            ::close(fd);
        }
    }

    if (ok0 || ok1) {
        manifest_ = (ok0 && (!ok1 || m0.epoch >= m1.epoch)) ? m0 : m1;
    } else {
        // 全新初始化：第一段从 seg_id=1 开始
        manifest_.epoch = 0;
        manifest_.active_seg_id = 1;
        manifest_.replayed_addr = logfmt::SegFirstBlockAddr(1);
        manifest_.checkpoint_addr = manifest_.replayed_addr;
        // 创建首段并写段头
        std::string seg_path = logfmt::SegFileName(1);
        int fd = ::open(seg_path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        assert(fd >= 0);
        char hdr_buf[logfmt::SEG_HEADER_SIZE];
        memset(hdr_buf, 0, sizeof(hdr_buf));
        logfmt::SegHeader sh;
        sh.magic = logfmt::SEG_MAGIC;
        sh.version = logfmt::LOG_FORMAT_VERSION;
        sh.seg_id = 1;
        sh.create_epoch = 1;
        sh.Serialize(hdr_buf);
        ssize_t n = ::pwrite(fd, hdr_buf, sizeof(hdr_buf), 0);
        assert(n == (ssize_t)sizeof(hdr_buf));
        ::close(fd);
        PersistManifest();
    }

    // 恢复写位置：从 active 段文件尾向前做块 CRC 校验，
    // 半写/损坏的尾部块作废（索引即缓存：写位置可从段内容恢复）
    v2_write_end_addr_ = RecoverWriteEndAddr(manifest_.active_seg_id);
    max_replay_off_ = v2_write_end_addr_ - 1;
    // persist_off_ 语义保持"已重放最后字节偏移"（WaitReplayCaughtUp 依赖）
    persist_off_ = manifest_.replayed_addr > 0 ? manifest_.replayed_addr - 1 : 0;
    persist_batch_id_ = 0;
}

uint64_t LogReplay::RecoverWriteEndAddr(uint64_t seg_id) {
    std::string path = logfmt::SegFileName(seg_id);
    if (!disk_manager_->is_file(path)) {
        return logfmt::SegFirstBlockAddr(seg_id);
    }
    struct stat st;
    if (::stat(path.c_str(), &st) != 0 || (uint64_t)st.st_size <= logfmt::SEG_HEADER_SIZE) {
        return logfmt::SegFirstBlockAddr(seg_id);
    }
    uint64_t blk_cnt = ((uint64_t)st.st_size - logfmt::SEG_HEADER_SIZE) / logfmt::BLOCK_SIZE;
    if (blk_cnt == 0) {
        return logfmt::SegFirstBlockAddr(seg_id);
    }

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return logfmt::SegFirstBlockAddr(seg_id);
    }
    // 从前向后逐块校验：写位置 = 从头连续有效前缀的边界。
    // 读侧 LogStreamReader 遇到坏块即截断，因此 write_end 不能超过第一个
    // 坏块——否则坏块之后的数据形成永远读不到的"洞"，新写入接在洞后
    // 也无法被重放。正常写中断只会损坏最后一块，此处语义与其一致；
    // 中间坏块（介质损坏）时保守截断，丢弃坏块后未重放的数据
    uint64_t result = logfmt::SegFirstBlockAddr(seg_id);
    std::vector<char> blk_buf(logfmt::BLOCK_SIZE);
    for (uint64_t i = 0; i < blk_cnt; i++) {
        uint64_t off = logfmt::SEG_HEADER_SIZE + i * logfmt::BLOCK_SIZE;
        ssize_t n = ::pread(fd, blk_buf.data(), logfmt::BLOCK_SIZE, (off_t)off);
        if (n < (ssize_t)logfmt::BLOCK_HEADER_SIZE) {
            break;
        }
        logfmt::BlockHeader hdr;
        hdr.Deserialize(blk_buf.data());
        if ((hdr.flags & logfmt::BLOCK_FLAG_PAD) != 0) {
            break;  // 填充块（段尾标记）
        }
        if (hdr.payload_len > logfmt::BLOCK_PAYLOAD_MAX ||
            n < (ssize_t)(logfmt::BLOCK_HEADER_SIZE + hdr.payload_len) ||
            !hdr.Verify(blk_buf.data() + logfmt::BLOCK_HEADER_SIZE)) {
            LOG(WARNING) << "[LogReplayV2] seg " << seg_id << " block " << i
                         << " invalid (corrupted/torn), truncate write position";
            break;  // 坏块/半写块：截断
        }
        result = logfmt::MakeAddr(seg_id, off + logfmt::BLOCK_SIZE);
    }
    ::close(fd);
    return result;
}

void LogReplay::WriteManifestTo(const std::string& path) {
    char buf[64];
    memset(buf, 0, sizeof(buf));
    manifest_.Serialize(buf);
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) throw std::runtime_error("cannot open replay manifest: " + path);
    const ssize_t n = ::pwrite(fd, buf, sizeof(buf), 0);
    if (n != static_cast<ssize_t>(sizeof(buf))) {
        ::close(fd);
        throw std::runtime_error("incomplete replay manifest write: " + path);
    }
    const int rc = ::fsync(fd);
    ::close(fd);
    if (rc != 0) throw std::runtime_error("replay manifest fsync failed: " + path);
}

void LogReplay::PersistManifest() {
    // 双写 + epoch 递增；读侧取 epoch 大者，单份撕裂可回退（I6）
    manifest_.epoch++;
    WriteManifestTo((manifest_.epoch % 2 == 0) ? logfmt::MANIFEST_0 : logfmt::MANIFEST_1);
}

void LogReplay::SetMaxReplayOff(uint64_t addr) {
    {
        std::lock_guard<std::mutex> l(latch1_);
        if (addr > max_replay_off_) {
            max_replay_off_ = addr;
        }
    }
    // 同步写位置的内存镜像（ManifestWriteEndAddr 供 LogManager 恢复写入）
    std::lock_guard<std::mutex> lk(manifest_mtx_);
    if (addr + 1 > v2_write_end_addr_) {
        v2_write_end_addr_ = addr + 1;
    }
}

void LogReplay::OnSegmentRolled(uint64_t new_seg_id) {
    std::lock_guard<std::mutex> lk(manifest_mtx_);
    manifest_.active_seg_id = new_seg_id;
    PersistManifest();
}

ssize_t LogReplay::PreadSegment(uint64_t seg_id, char* buf, size_t size, uint64_t seg_off) {
    int fd = -1;
    {
        std::lock_guard<std::mutex> lk(v2_seg_fd_mtx_);
        auto it = v2_seg_fd_cache_.find(seg_id);
        if (it == v2_seg_fd_cache_.end()) {
            fd = ::open(logfmt::SegFileName(seg_id).c_str(), O_RDONLY);
            if (fd < 0) {
                return -1;
            }
            v2_seg_fd_cache_[seg_id] = fd;
        } else {
            fd = it->second;
        }
    }
    return ::pread(fd, buf, size, (off_t)seg_off);
}

// ==================== v2 重放循环（LogStreamReader 驱动） ====================

void LogReplay::replayFunV2() {
    logfmt::LogStreamReader reader([this](uint64_t seg, char* buf, size_t size, uint64_t off) {
        return PreadSegment(seg, buf, size, off);
    });
    {
        std::lock_guard<std::mutex> lk(manifest_mtx_);
        reader.Seek(manifest_.replayed_addr);
    }

    while (!replay_stop) {
        std::unique_lock<std::recursive_mutex> pause_lk(replay_pause_mtx_);

        uint64_t end_addr;
        {
            std::lock_guard<std::mutex> l(latch1_);
            end_addr = max_replay_off_ + 1;
        }

        const char* rec = nullptr;
        uint32_t rec_len = 0;
        uint64_t rec_addr = 0;
        if (!reader.Next(rec, rec_len, rec_addr, end_addr)) {
            pause_lk.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        // 按类型构造记录（与 replayFun 相同分派）
        LogType type = *reinterpret_cast<const LogType*>(rec + OFFSET_LOG_TYPE);
        LogRecord* record = nullptr;
        switch (type) {
            case LogType::BEGIN:       record = new BeginLogRecord(); break;
            case LogType::ABORT:       record = new AbortLogRecord(); break;
            case LogType::COMMIT:      record = new CommitLogRecord(); break;
            case LogType::INSERT:      record = new InsertLogRecord(); break;
            case LogType::UPDATE:      record = new UpdateLogRecord(); break;
            case LogType::DELETE:      record = new DeleteLogRecord(); break;
            case LogType::NEWPAGE:     record = new NewPageLogRecord(); break;
            case LogType::FSMUPDATE:   record = new FSMUpdateLogRecord(); break;
            case LogType::BLINKINSERT: record = new BLinkInsertLogRecord(); break;
            case LogType::BLINKDELETE: record = new BLinkDeleteLogRecord(); break;
            case LogType::BATCHEND:    record = new BatchEndLogRecord(); break;
            case LogType::ABORTEND:    record = new AbortLogRecord(); break;
            default:
                LOG(ERROR) << "[ReplayV2] unknown log type " << (int)type << " at " << rec_addr;
                assert(0);
                break;
        }
        record->deserialize(rec);
        // v2 模式：apply 内不逐条写 manifest（由下方批量持久化）
        apply_sigle_log(record, rec_addr);
        delete record;
        {
            std::lock_guard<std::mutex> progress_lock(latch2_);
            persist_off_ = std::min(reader.NextReplayAddr(), end_addr) - 1;
        }
        // P1 续：周期性落盘缓存脏页。write-back 页缓存若长期不驱逐，
        // GetPageWithLsn 的磁盘 LSN 等待会挂起到下一个安全点；小数据集
        // 下缓存远未满、永不驱逐。每 2048 条 apply 落盘一次，等待窗口
        // 有界（<1-2s），同页连续日志仍合并（128 键批次内同页日志 ~10 条）。
        if (++applied_since_flush_ >= 2048) {
            applied_since_flush_ = 0;
            FlushReplayPages();
        }

        // persist_off_ is the in-memory applied frontier. ValidationCut publishes
        // the durable frontier only after data-page flush and fdatasync.
    }
}

// ==================== 统一日志扫描层 ====================

bool LogReplay::ScanAllLogs(uint64_t end_addr, const std::function<bool(const char*, uint32_t, uint64_t)>& cb) {
    recovery_observation::Span scan_span("wal_analysis");
    if (v2_mode_) {
        logfmt::LogStreamReader reader([this](uint64_t seg, char* buf, size_t size, uint64_t off) {
            return PreadSegment(seg, buf, size, off);
        });
        reader.Seek(logfmt::SegFirstBlockAddr(1));
        const char* rec = nullptr;
        uint32_t len = 0;
        uint64_t addr = 0;
        while (reader.Next(rec, len, addr, end_addr)) {
            if (!cb(rec, len, addr)) return false;
        }
        return reader.AtEnd(end_addr);
    }

    uint64_t file_size = std::min(end_addr, disk_manager_->get_file_size(log_file_path_));
    uint64_t scan_offset = sizeof(batch_id_t) + sizeof(size_t);
    const size_t SCAN_BUFFER_SIZE = 1024 * 1024;
    std::vector<char> scan_buffer(SCAN_BUFFER_SIZE);
    while (scan_offset < file_size) {
        size_t read_size = std::min((size_t)(file_size - scan_offset), SCAN_BUFFER_SIZE);
        uint64_t bytes_read = read_log(scan_buffer.data(), read_size, scan_offset);
        if (bytes_read == (uint64_t)-1 || bytes_read == 0) return false;
        size_t inner_offset = 0;
        while (inner_offset + LOG_HEADER_SIZE <= bytes_read) {
            uint32_t log_size;
            memcpy(&log_size, scan_buffer.data() + inner_offset + OFFSET_LOG_TOT_LEN, sizeof(log_size));
            if (log_size < LOG_HEADER_SIZE) return false;
            if (inner_offset + log_size > bytes_read) break;
            if (!cb(scan_buffer.data() + inner_offset, log_size, scan_offset + inner_offset)) return false;
            inner_offset += log_size;
        }
        if (inner_offset == 0) return false;
        scan_offset += inner_offset;
    }
    return scan_offset == end_addr;
}

bool LogReplay::ReadLogRecordAt(uint64_t addr, char* out, uint32_t cap, uint32_t& out_len) {
    if (v2_mode_) {
        logfmt::LogStreamReader reader([this](uint64_t seg, char* buf, size_t size, uint64_t off) {
            return PreadSegment(seg, buf, size, off);
        });
        return reader.ReadRecordAt(addr, out, cap, out_len);
    }
    // legacy：直接按文件偏移读（先读头拿总长）
    char hdr[OFFSET_LOG_TOT_LEN + sizeof(uint32_t)];
    uint64_t n = read_log(hdr, sizeof(hdr), addr);
    if (n == (uint64_t)-1 || n < sizeof(hdr)) return false;
    uint32_t rec_len = *reinterpret_cast<const uint32_t*>(hdr + OFFSET_LOG_TOT_LEN);
    if (rec_len == 0 || rec_len > cap) return false;
    n = read_log(out, rec_len, addr);
    if (n == (uint64_t)-1 || n < rec_len) return false;
    out_len = rec_len;
    return true;
}

std::string LogReplay::ResolveTableName(table_id_t table_id, const char* table_name_ptr, size_t table_name_size) const {
    auto it = table_name_map_.find(table_id);
    if (it != table_name_map_.end()) {
        return it->second;
    }
    if (table_name_ptr != nullptr && table_name_size > 0) {
        return std::string(table_name_ptr, table_name_ptr + table_name_size);
    }
    LOG(FATAL) << "Cannot resolve table name for table_id " << table_id;
    return {};
}

int LogReplay::ResolveTableFd(table_id_t table_id, const char* table_name_ptr, size_t table_name_size) {
    const std::string table_name = ResolveTableName(table_id, table_name_ptr, table_name_size);
    std::lock_guard<std::mutex> guard(table_fd_mutex_);
    auto it = table_fd_cache_.find(table_id);
    if (it != table_fd_cache_.end()) {
        return it->second;
    }
    int fd = disk_manager_->open_file(table_name);
    table_fd_cache_[table_id] = fd;
    return fd;
}

// ==================== BLink / FSM 日志重放实现 ====================

void LogReplay::ApplyBLinkInsert(table_id_t blink_table_id, const std::string &table_name,
                                 itemkey_t key, const Rid &rid) {
    if (sm_manager == nullptr) throw std::runtime_error("BLink replay has no storage manager");
    if (!disk_manager_->is_file(table_name))
        throw std::runtime_error("BLink replay index missing: " + table_name);
    S_BLinkIndexHandle *handle = sm_manager->GetOrCreateBLinkHandle(table_name);
    if (handle == nullptr || !handle->valid())
        throw std::runtime_error("BLink replay index invalid: " + table_name);
    std::lock_guard<std::mutex> lk(handle->get_op_mutex());
    Rid current;
    if (handle->search(&key, current) && !(current == rid))
        throw std::runtime_error("BLink replay INSERT conflicts with another RID");
    if (handle->insert_entry(&key, rid) == INVALID_PAGE_ID)
        throw std::runtime_error("BLink replay INSERT failed");
    // 第 13 层观测：blink 回放实际生效证据（首 8 条 + 每 100 条）
    {
        static std::atomic<int> bl_replay_seen_{0};
        int n = ++bl_replay_seen_;
        if (n <= 8 || n % 100 == 0)
            LOG(INFO) << "[LogReplay] BLink INSERT applied #" << n << " table=" << table_name
                      << " key=" << key;
    }
}

void LogReplay::ApplyBLinkDelete(table_id_t blink_table_id, const std::string &table_name,
                                 itemkey_t key, const Rid &rid) {
    if (sm_manager == nullptr) throw std::runtime_error("BLink replay has no storage manager");
    if (!disk_manager_->is_file(table_name))
        throw std::runtime_error("BLink replay index missing: " + table_name);
    S_BLinkIndexHandle *handle = sm_manager->GetOrCreateBLinkHandle(table_name);
    if (handle == nullptr || !handle->valid())
        throw std::runtime_error("BLink replay index invalid: " + table_name);
    std::lock_guard<std::mutex> lk(handle->get_op_mutex());
    Rid current;
    if (handle->search(&key, current) && !(current == rid))
        throw std::runtime_error("BLink replay DELETE targets another RID");
    handle->remove_entry(&key);
}

void LogReplay::ApplyFSMUpdate(table_id_t fsm_table_id, const std::string &table_name,
                               uint32_t page_id, uint32_t free_space) {
    if (!disk_manager_->is_file(table_name))
        throw std::runtime_error("FSM replay file missing: " + table_name);
    int fd = ResolveTableFd(fsm_table_id, table_name.c_str(), table_name.size());
    if (fd < 0) throw std::runtime_error("FSM replay file unavailable: " + table_name);
    ApplyFsmUpdate(disk_manager_, fd, page_id, free_space);
}

bool LogReplay::overwriteFixedLine(const std::string& filename, int lineNumber, const std::string& newContent, int lineLength ) {
    // 检查新内容长度是否匹配固定行长度（注意：newContent不应包含换行符）
    // 假设 lineLength 已经包含了换行符占用的字节
    if (newContent.length() != lineLength - 1) { // 例如，lineLength=19(18字符+1个'\n')，则newContent长度应为18
        std::cerr << "Error: New content length must be " << (lineLength - 1) << " characters." << std::endl;
        return false;
    }

    // 打开文件用于读写（二进制模式可以避免一些换行符的自动转换问题）
    std::fstream file(filename, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file '" << filename << "'." << std::endl;
        return false;
    }

    // 获取文件总长度并计算总行数
    file.seekg(0, std::ios::end);
    std::streampos fileSize = file.tellg();
    int totalLines = fileSize / lineLength;

    // 检查行号是否有效
    if (lineNumber < 1 || lineNumber > totalLines) {
        std::cerr << "Error: Line number " << lineNumber << " is out of range. File has " << totalLines << " lines." << std::endl;
        file.close();
        return false;
    }

    // 计算目标行的起始偏移量（字节位置）
    // 注意：行号从1开始，但偏移量从0开始
    std::streampos targetPos = static_cast<std::streampos>(lineNumber - 1) * lineLength;

    // 定位到目标行开始位置
    file.seekp(targetPos, std::ios::beg);
    
    // 写入新的行内容（注意：这里不自动添加换行符，因为newContent应已保证长度正确，且文件中原有换行符保持不变）
    file << newContent; // 写入正好 (lineLength - 1) 个字符，覆盖旧数据

    // 检查写入是否成功
    if (file.fail()) {
        //std::cerr << "Error: Failed to write to file." << std::endl;
        file.close();
        return false;
    }

    file.close();
    //std::cout << "Line " << lineNumber << " updated successfully using direct overwrite." << std::endl;
    return true;
}
// ==================== P1 续：replay 目标页写合并缓存 ====================
// 实现说明见 logreplay.h 成员区注释。所有函数要求调用者持有
// replay_cache_mtx_。缓存键 = (表名哈希, 页号)，flush/驱逐时按表名
// 重新解析 fd（DiskManager 路径->fd 缓存），规避 fd 关闭/复用风险。
static uint64_t ReplayTableHash(const std::string& name) {
    return static_cast<uint64_t>(std::hash<std::string>{}(name));
}

char* LogReplay::AcquireReplayPageLocked(int fd, const std::string& table_name, page_id_t page_no) {
    const ReplayCacheKey key{ReplayTableHash(table_name), page_no};
    auto it = replay_pages_.find(key);
    if (it == replay_pages_.end()) {
        auto entry = std::make_unique<ReplayPageEntry>();
        disk_manager_->read_page(fd, page_no, entry->data.data(), PAGE_SIZE);
        entry->table_name = table_name;
        it = replay_pages_.emplace(key, std::move(entry)).first;
    }
    // LRU touch：移到链头
    auto pos = replay_lru_pos_.find(key);
    if (pos != replay_lru_pos_.end()) {
        replay_lru_.erase(pos->second);
        replay_lru_pos_.erase(pos);
    }
    replay_lru_.push_front(key);
    replay_lru_pos_[key] = replay_lru_.begin();
    return it->second->data.data();
}

void LogReplay::MarkReplayPageDirtyLocked(int fd, const std::string& table_name, page_id_t page_no) {
    (void)fd;
    const ReplayCacheKey key{ReplayTableHash(table_name), page_no};
    auto it = replay_pages_.find(key);
    if (it != replay_pages_.end()) it->second->dirty = true;
    EvictReplayPagesLocked();
}

void LogReplay::EvictReplayPagesLocked() {
    while (replay_pages_.size() > replay_page_capacity_ && !replay_lru_.empty()) {
        const ReplayCacheKey victim = replay_lru_.back();
        auto it = replay_pages_.find(victim);
        if (it != replay_pages_.end()) {
            if (it->second->dirty) {
                int fd = disk_manager_->open_file(it->second->table_name);
                if (fd < 0) {
                    // P0 修复：丢脏页不再只计数——顺序 replay 已越过该日志，
                    // 丢弃即静默数据丢失。置 poisoned 使所有后续追平判定
                    // fail-closed（宁可恢复失败保持隔离）。
                    replay_cache_poisoned_.store(true, std::memory_order_release);
                    ++replay_page_flush_drops_;
                    LOG(ERROR) << "[LogReplay] replay cache drop (cannot open) table="
                               << it->second->table_name << " page=" << victim.page_no
                               << " — CACHE POISONED, all catch-up checks will fail";
                } else {
                    disk_manager_->write_page(fd, victim.page_no,
                                              it->second->data.data(), PAGE_SIZE);
                    ++replay_page_flush_writes_;
                }
            }
            replay_pages_.erase(it);
        }
        replay_lru_pos_.erase(victim);
        replay_lru_.pop_back();
    }
}

bool LogReplay::FlushReplayPagesLocked() {
    ++replay_page_flush_batches_;
    bool all_ok = true;
    for (auto it = replay_pages_.begin(); it != replay_pages_.end();) {
        if (!it->second->dirty) {
            ++it;
            continue;
        }
        int fd = disk_manager_->open_file(it->second->table_name);
        if (fd < 0) {
            // P0 修复：丢脏页置 poisoned（与驱逐路径一致）；周期 flush
            // 忽略返回值也不能把丢页状态静默当成功
            replay_cache_poisoned_.store(true, std::memory_order_release);
            ++replay_page_flush_drops_;
            LOG(ERROR) << "[LogReplay] replay flush drop (cannot open) table="
                       << it->second->table_name << " page=" << it->first.page_no
                       << " — CACHE POISONED, all catch-up checks will fail";
            all_ok = false;
            it = replay_pages_.erase(it);  // 丢弃，交由幂等重放恢复
            continue;
        }
        disk_manager_->write_page(fd, it->first.page_no,
                                  it->second->data.data(), PAGE_SIZE);
        ++replay_page_flush_writes_;
        it->second->dirty = false;
        ++it;
    }
    return all_ok;
}

// P0 修复：缓存整体失效。前置契约：调用点处于 PauseReplay 窗口且已成功
// FlushReplayPages（缓存全 clean，移除无数据损失）。发现 dirty 页说明
// 契约被破坏——置 poisoned 并丢弃（fail-closed）。返回是否发现 dirty 页。
bool LogReplay::InvalidateAllReplayPagesLocked() {
    bool had_dirty = false;
    for (const auto& kv : replay_pages_) {
        if (kv.second->dirty) { had_dirty = true; break; }
    }
    if (had_dirty) {
        replay_cache_poisoned_.store(true, std::memory_order_release);
        LOG(ERROR) << "[LogReplay] InvalidateAllReplayPages: dirty pages present "
                      "outside flush contract — CACHE POISONED";
    }
    replay_pages_.clear();
    replay_lru_.clear();
    replay_lru_pos_.clear();
    return had_dirty;
}

// P1 续：直接调用方（测试）的同步落盘——写盘并从缓存移除，保持
// "apply 返回后磁盘已更新"契约。要求调用者已持有 replay_cache_mtx_。
// 写失败时从缓存移除该页并向上抛（与无缓存实现的 write_page 异常语义
// 一致；apply_sigle_log 的直接调用不在 pause 锁内，异常可安全传播）
void LogReplay::SyncReplayPageToDiskLocked(const std::string& table_name, page_id_t page_no) {
    const ReplayCacheKey key{ReplayTableHash(table_name), page_no};
    auto it = replay_pages_.find(key);
    if (it == replay_pages_.end()) return;
    auto pos = replay_lru_pos_.find(key);
    if (pos != replay_lru_pos_.end()) {
        replay_lru_.erase(pos->second);
        replay_lru_pos_.erase(pos);
    }
    int fd = disk_manager_->open_file(table_name);
    if (fd < 0) {
        // P0 修复：同步落盘失败丢页同样 poison（fail-closed）
        replay_cache_poisoned_.store(true, std::memory_order_release);
        replay_pages_.erase(it);
        throw std::runtime_error("replay sync target table unavailable: " + table_name);
    }
    disk_manager_->write_page(fd, page_no, it->second->data.data(), PAGE_SIZE);
    ++replay_page_flush_writes_;
    replay_pages_.erase(it);
}

void LogReplay::apply_sigle_log(LogRecord* log, uint64_t curr_offset, bool sync_to_disk) {
    recovery_observation::Span apply_span(
        (log->log_type_ == LogType::BLINKINSERT || log->log_type_ == LogType::BLINKDELETE)
            ? "background_index_apply" : "background_other_apply");
    // 独立 undo 区挂载点（仅 replay 线程调用本函数，定向 Redo 走
    // ApplyRedoEntriesToPage 不经此处，不会重复记录）：
    // - 数据日志：apply 前把 undo 载荷（整条 WAL 记录）追加到 undo 区并挂链；
    //   放在 apply 前使 undo 区内容与"日志已到达"语义一致（apply 幂等跳过的
    //   记录其 undo 信息同样入区，与 WAL 全扫语义等价）
    // - BATCHEND：事务提交，回收其 undo 链空间
    if (undo_area_ != nullptr && undo_area_->IsEnabled()) {
        if (log->log_type_ == LogType::BATCHEND) {
            undo_area_->CommitTxn(log->log_node_id_, log->log_tid_);
        } else if (log->log_type_ == LogType::ABORTEND) {
            undo_area_->AbortTxn(log->log_node_id_, log->log_tid_);
        } else {
            undo_area_->TrackLog(log);
        }
    }
    switch(log->log_type_) {
        case LogType::INSERT: {
            InsertLogRecord* insert_log = dynamic_cast<InsertLogRecord*>(log);

            std::string table_name(insert_log->table_name_, insert_log->table_name_ + insert_log->table_name_size_);
            if (mode == "SQL" && !sm_manager->db.is_table(table_name)){
                return;
            }
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0)
                throw std::runtime_error("heap replay file unavailable: " + table_name);

            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
            if (insert_log->slot_no_ < 0 || insert_log->slot_no_ >= file_hdr.num_records_per_page_) {
               assert(false);
            }

            std::lock_guard<std::mutex> replay_cache_lk(replay_cache_mtx_);
            char* buffer = AcquireReplayPageLocked(fd, table_name, insert_log->page_no_);

            auto* page_hdr = reinterpret_cast<RmPageHdr*>(buffer);
            const LLSN log_llsn = static_cast<LLSN>(insert_log->lsn_);
            if (page_hdr->LLSN_ >= log_llsn) break;
            if (log->prev_lsn_ != page_hdr->LLSN_)
                throw std::runtime_error("INSERT replay predecessor missing: " + table_name +
                    " page=" + std::to_string(insert_log->page_no_) + " have=" + std::to_string(page_hdr->LLSN_) +
                    " expected=" + std::to_string(log->prev_lsn_) + " next=" + std::to_string(log_llsn));

            char* bitmap = buffer + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            const int slot_no = insert_log->slot_no_;
            if (!Bitmap::is_set(bitmap, slot_no)) {
                Bitmap::set(bitmap, slot_no);
                page_hdr->num_records_++;
            }else {
                // BitMap 一定是 false
                assert(false);
            }

            // 要改三个地方，data_item + value + itemkey
            char *slots = bitmap + file_hdr.bitmap_size_;
            char* tuple = slots + slot_no * (file_hdr.record_size_ + sizeof(itemkey_t));

            // std::cout << "FileHdr Record Size = " << file_hdr.record_size_ << " log record size = " << insert_log->insert_value_.value_size_ << "\n";

            itemkey_t* item_key = reinterpret_cast<itemkey_t*>(tuple);
            *item_key = insert_log->insert_value_.key_;


            memcpy(tuple + sizeof(itemkey_t) , insert_log->insert_value_.value_ , insert_log->insert_value_.value_size_);

            int id = *reinterpret_cast<int*>(insert_log->insert_value_.value_ + sizeof(DataItem));
            int age = *reinterpret_cast<int*>(insert_log->insert_value_.value_ + sizeof(DataItem) + sizeof(int));
            // std::cout << "id = " << id << " age = " << age << "\n";

            page_hdr->pre_LLSN_ = page_hdr->LLSN_;
            page_hdr->LLSN_ = log_llsn;

            // 写回到磁盘里（P1 续：默认经 replay 页缓存合并延迟落盘；
            // 直接调用方（sync_to_disk）立即写盘并移出缓存，保持同步契约）
            if (sync_to_disk) SyncReplayPageToDiskLocked(table_name, insert_log->page_no_);
            else MarkReplayPageDirtyLocked(fd, table_name, insert_log->page_no_);
        } break;
        case LogType::DELETE: {
            DeleteLogRecord* delete_log = dynamic_cast<DeleteLogRecord*>(log);
            const std::string table_name(delete_log->table_name_, delete_log->table_name_size_);

            if (mode == "SQL" && !sm_manager->db.is_table(table_name)){
                return;
            }
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0){
                throw InternalError("DELETE replay cannot open table: " + table_name);
            }

            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
            if (delete_log->slot_no_ < 0 || delete_log->slot_no_ >= file_hdr.num_records_per_page_) {
                assert(false);
            }

            std::lock_guard<std::mutex> replay_cache_lk(replay_cache_mtx_);
            char* buffer = AcquireReplayPageLocked(fd, table_name, delete_log->page_no_);

            auto* page_hdr = reinterpret_cast<RmPageHdr*>(buffer);
            char* bitmap = buffer + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            const LLSN log_llsn = static_cast<LLSN>(delete_log->lsn_);

            if (page_hdr->LLSN_ >= log_llsn) break;
            if (log->prev_lsn_ != page_hdr->LLSN_)
                throw std::runtime_error("DELETE replay predecessor missing: " + table_name +
                    " page=" + std::to_string(delete_log->page_no_) + " have=" + std::to_string(page_hdr->LLSN_) +
                    " expected=" + std::to_string(log->prev_lsn_) + " next=" + std::to_string(log_llsn));

            // TODO：DeleteLog 的逻辑需要重新考虑下，这里先不搞了
            assert(Bitmap::is_set(bitmap , delete_log->slot_no_));
            Bitmap::reset(bitmap, delete_log->slot_no_);
            if (page_hdr->num_records_ > 0) --page_hdr->num_records_;

            page_hdr->pre_LLSN_ = page_hdr->LLSN_;
            page_hdr->LLSN_ = log_llsn;
            
            char *slots = bitmap + file_hdr.bitmap_size_;
            char* tuple = slots + delete_log->slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
            DataItem *data_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(data_item->valid == 1);
            assert(data_item->lock == EXCLUSIVE_LOCKED);
            data_item->valid = 0;
            data_item->lock = UNLOCKED;
            data_item->user_insert = 0;

            // 写回到存储（P1 续：默认缓存合并；直接调用方立即写盘）
            if (sync_to_disk) SyncReplayPageToDiskLocked(table_name, delete_log->page_no_);
            else MarkReplayPageDirtyLocked(fd, table_name, delete_log->page_no_);

        } break;
        case LogType::UPDATE: {
            // std::cout << "进入UPDATE重做"<<std::endl;
            UpdateLogRecord* update_log = dynamic_cast<UpdateLogRecord*>(log);
            std::string table_name(update_log->table_name_, update_log->table_name_ + update_log->table_name_size_);
            if (mode == "SQL" && !sm_manager->db.is_table(table_name)){
                return;
            }
            
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0)
                throw std::runtime_error("heap replay file unavailable: " + table_name);

            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);


            std::lock_guard<std::mutex> replay_cache_lk(replay_cache_mtx_);
            char* buffer = AcquireReplayPageLocked(fd, table_name, update_log->rid_.page_no_);

            RmPageHdr* page_hdr = reinterpret_cast<RmPageHdr*>(buffer);
            const LLSN log_llsn = static_cast<LLSN>(update_log->lsn_);

            // LOG(INFO) << "Apply Update Log , table_name = " << 
            //     table_name << " page_id = " << update_log->rid_.page_no_ << " slot_no = " << update_log->rid_.slot_no_
            //     << " page lsn = " << page_hdr->LLSN_ << " log lsn = " << log_llsn << " log prev_lsn = " << log->prev_lsn_;

            if (page_hdr->LLSN_ >= log_llsn) break;
            if (log->prev_lsn_ != page_hdr->LLSN_)
                throw std::runtime_error("UPDATE replay predecessor missing: " + table_name +
                    " page=" + std::to_string(update_log->rid_.page_no_) + " have=" + std::to_string(page_hdr->LLSN_) +
                    " expected=" + std::to_string(log->prev_lsn_) + " next=" + std::to_string(log_llsn));

            page_hdr->pre_LLSN_ = page_hdr->LLSN_;
            page_hdr->LLSN_ = log_llsn;

            char* bitmap = buffer + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            char *slots = bitmap + file_hdr.bitmap_size_;
            char* tuple = slots + update_log->rid_.slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
            itemkey_t *item_key = reinterpret_cast<itemkey_t*>(tuple);
            *item_key = update_log->new_value_.key_;
            memcpy(tuple + sizeof(item_key) , update_log->new_value_.value_ , update_log->new_value_.value_size_);

            // 写回到存储（P1 续：默认缓存合并；直接调用方立即写盘）
            if (sync_to_disk) SyncReplayPageToDiskLocked(table_name, update_log->rid_.page_no_);
            else MarkReplayPageDirtyLocked(fd, table_name, update_log->rid_.page_no_);
        } break;
        case LogType::NEWPAGE: {
            // 这里有点问题，需要拿到 tab_name，现在反正没用这个，先不管了
            assert(false);
            NewPageLogRecord* new_page_log = dynamic_cast<NewPageLogRecord*>(log);
            int fd = ResolveTableFd(new_page_log->table_id_, nullptr, 0);
            if (fd < 0) break;

            bool is_file = true;
           
            // 预读现有文件头信息，维护 free list 和 num_pages
            int file_num_pages = disk_manager_->get_fd2pageno(fd);
            int old_first_free = RM_NO_PAGE;
            if (is_file) {
                // 读取旧的 first_free_page_no，作为新链尾的 next
                char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
                disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
                old_first_free = *reinterpret_cast<int*>(page0_buf + OFFSET_FILE_HDR + OFFSET_FIRST_FREE_PAGE_NO);
            }

            page_id_t chain_head = RM_NO_PAGE;
            page_id_t prev_page = RM_NO_PAGE;

            for (int i = 0; i < new_page_log->request_pages_; ++i) {
                page_id_t page_no = disk_manager_->allocate_page(fd);

                char init_value[PAGE_SIZE];
                memset(init_value, 0, PAGE_SIZE);
                disk_manager_->write_page(fd, page_no, (const char*)&init_value, PAGE_SIZE);

                if (chain_head == RM_NO_PAGE) {
                    chain_head = page_no;
                }
                if (is_file) {
                    int next_free = RM_NO_PAGE;
                    disk_manager_->update_value(fd, page_no, OFFSET_NEXT_FREE_PAGE_NO,
                                                reinterpret_cast<char*>(&next_free), sizeof(int));
                    if (prev_page != RM_NO_PAGE) {
                        // 串联上一页
                        disk_manager_->update_value(fd, prev_page, OFFSET_NEXT_FREE_PAGE_NO,
                                                    reinterpret_cast<char*>(&page_no), sizeof(int));
                    }
                    prev_page = page_no;
                }
            }

            if (is_file && chain_head != RM_NO_PAGE) {
                // 将尾部 next 指向旧的 free head
                disk_manager_->update_value(fd, prev_page, OFFSET_NEXT_FREE_PAGE_NO,
                                            reinterpret_cast<char*>(&old_first_free), sizeof(int));

                // 更新文件头 first_free 指向新链头
                disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FILE_HDR + OFFSET_FIRST_FREE_PAGE_NO,
                                            reinterpret_cast<char*>(&chain_head), sizeof(int));

                // 更新 num_pages: 基于原 num_pages 加上申请数量
                int new_num_pages = file_num_pages + new_page_log->request_pages_;
                disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FILE_HDR + OFFSET_NUM_PAGES,
                                            reinterpret_cast<char*>(&new_num_pages), sizeof(int));
            }
        } break;
        case LogType::FSMUPDATE: {
            auto fsm_log = dynamic_cast<FSMUpdateLogRecord*>(log);
            if (fsm_log == nullptr) {
                break;
            }
            std::string fsm_table = ResolveTableName(fsm_log->table_id_, fsm_log->table_name_, fsm_log->table_name_size_);
            ApplyFSMUpdate(fsm_log->table_id_, fsm_table, fsm_log->page_id_, fsm_log->free_space_);
        } break;
        case LogType::BLINKINSERT: {
            auto bl_log = dynamic_cast<BLinkInsertLogRecord*>(log);
            if (bl_log == nullptr) {
                break;
            }
            std::string bl_table = ResolveTableName(bl_log->table_id_, bl_log->table_name_, bl_log->table_name_size_);
            ApplyBLinkInsert(bl_log->table_id_, bl_table, bl_log->key_, bl_log->rid_);
        } break;
        case LogType::BLINKDELETE: {
            auto bl_log = dynamic_cast<BLinkDeleteLogRecord*>(log);
            if (bl_log == nullptr) {
                break;
            }
            std::string bl_table = ResolveTableName(bl_log->table_id_, bl_log->table_name_, bl_log->table_name_size_);
            ApplyBLinkDelete(bl_log->table_id_, bl_table, bl_log->key_, bl_log->rid_);
        } break;
        case LogType::BATCHEND: {
            BatchEndLogRecord* batch_end_log = dynamic_cast<BatchEndLogRecord*>(log);

            std::unique_lock<std::mutex> latch(latch2_);
            persist_batch_id_ = batch_end_log->log_batch_id_;
            latch.unlock();
            //print_llsnrecord();
            //LOG(INFO) << "Update persist_batch_id, new persist_batch_id: " << persist_batch_id_;
        } break;
        default:
        break;
    }

    {
        std::unique_lock<std::mutex> latch(latch2_);
        persist_off_ = curr_offset + log->log_tot_len_ - 1;
        latch.unlock();
    }

    if (!v2_mode_) {
        // legacy：逐条写日志文件头（每条 2 次小写）
        lseek(log_write_head_fd_, 0, SEEK_SET);
        ssize_t result = write(log_write_head_fd_, &persist_batch_id_, sizeof(batch_id_t));
        if (result == -1) {
            LOG(FATAL) << "Fail to write persist_batch_id into log_file";
        }
        result = write(log_write_head_fd_, &persist_off_, sizeof(size_t));
        if (result == -1) {
            LOG(FATAL) << "Fail to write persist_off into log_file";
        }
    }
    // v2：重放进度由 replayFunV2 按批持久化到 manifest（每 64 条一次），
    // 崩溃后多重放的部分由页版本比较与逻辑日志幂等吸收
}

void LogReplay::apply_undo_log(const LogRecord* log_record) {
    if (log_record == nullptr) {
        return;
    }

    switch (log_record->log_type_) {
        case LogType::UPDATE: {
            const UpdateLogRecord* update_log = dynamic_cast<const UpdateLogRecord*>(log_record);
            if (update_log == nullptr || !update_log->HasUndoPayload()) {
                return;
            }
            std::string table_name(update_log->table_name_, update_log->table_name_ + update_log->table_name_size_);
            int fd = disk_manager_->open_file(table_name);
            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
            const RmRecord& undo_image = update_log->old_value();
            const int slot_base = sizeof(RmPageHdr) + file_hdr.bitmap_size_ +
                                   update_log->rid_.slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
            const int value_offset = slot_base + static_cast<int>(sizeof(itemkey_t));
            disk_manager_->update_value(fd,
                                        update_log->rid_.page_no_,
                                        value_offset,
                                        undo_image.value_,
                                        undo_image.value_size_ * sizeof(char));
            break;
        }
        case LogType::DELETE: {
            const DeleteLogRecord* delete_log = dynamic_cast<const DeleteLogRecord*>(log_record);
            if (delete_log == nullptr || !delete_log->has_undo_meta_) {
                return;
            }
            int fd = ResolveTableFd(delete_log->table_id_, delete_log->table_name_, delete_log->table_name_size_);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FILE_HDR + OFFSET_FIRST_FREE_PAGE_NO, (char*)(&delete_log->undo_first_free_page_no_), sizeof(int));
            disk_manager_->update_value(fd, delete_log->page_no_, OFFSET_PAGE_HDR, (char*)&delete_log->undo_page_hdr_, sizeof(RmPageHdr));
            disk_manager_->update_value(fd, delete_log->page_no_, delete_log->bucket_offset_, const_cast<char*>(&delete_log->undo_bucket_value_), sizeof(char));
            break;
        }
        case LogType::NEWPAGE: {
            // No explicit undo for page allocation requests; replay decides actual allocation.
            return;
        }
        default:
            break;
    }
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
uint64_t LogReplay::read_log(char *log_data, int size, uint64_t offset) {
    // read log file from the previous end
    assert (log_replay_fd_ != -1);
    uint64_t file_size = disk_manager_->get_file_size(log_file_path_);
    if (offset > file_size) {
        return -1;
    }

    if (file_size - offset < size){
        size = file_size - offset;
    }
    // size = std::min(size, file_size - offset);
    if(size == 0) return 0;
    // 使用 pread 替代 lseek+read，保证多线程并发读取时的线程安全
    uint64_t bytes_read = pread(log_replay_fd_, log_data, size, offset);
    assert(bytes_read == (uint64_t)size);
    return bytes_read;
}

void LogReplay::replayFun(){
    // offset 指向下一个要读的起始位置
    uint64_t offset = persist_off_ + 1;
    uint64_t read_bytes;
    while (!replay_stop) {
        std::unique_lock<std::recursive_mutex> pause_lk(replay_pause_mtx_);
        uint64_t end_addr;
        {
            std::lock_guard<std::mutex> lock(latch1_);
            end_addr = max_replay_off_ + 1;
        }
        size_t read_size = offset < end_addr
            ? std::min<uint64_t>(end_addr - offset, LOG_REPLAY_BUFFER_SIZE) : 0;
        if (read_size == 0) {
            pause_lk.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        // LOG(INFO) << "Begin apply log, apply size is " << read_size << ", max_replay_off_: " << max_replay_off_ << ", offset: " << offset;
        // offset为要读取数据的起始位置，persist_off_为已经读取的字节的结尾位置，所以需要+1
        // offset ++;
        read_bytes = read_log(buffer_.buffer_, read_size, offset);
        // LOG(INFO) << "read bytes: " << read_bytes;
        buffer_.offset_ = read_bytes - 1;
        size_t inner_offset = 0;
        // int replay_batch_id;
        while (inner_offset <= buffer_.offset_ ) {
            // buffer.offset_存储了buffer中数据的最大长度，判断在buffer存储的数据内能否读到下一条日志的总长度数据
            if (inner_offset + OFFSET_LOG_TOT_LEN + sizeof(uint32_t) > (unsigned long)buffer_.offset_) {
                // LOG(INFO) << "the next log record's tot_len cannot be read, inner_offset: " << inner_offset << ", buffer_offset: " << buffer_.offset_;
                break;
            }
            // 获取日志记录长度
            uint32_t size = *reinterpret_cast<const uint32_t *>(buffer_.buffer_ + inner_offset + OFFSET_LOG_TOT_LEN);
            // 如果剩余数据不是一条完整的日志记录，则不再进行读取
            if (size == 0 || size + inner_offset > (uint64_t)buffer_.offset_ + 1) {
            //  LOG(INFO) << "The remain data does not contain a complete log record, the next log record's size is: " << size << ", inner_offset: " << inner_offset << ", buffer_offset: " << buffer_.offset_;
                usleep(1000);
                break;
            }    
            // LOG(INFO) << "the next log record's size is: " << size;       
            LogRecord *record;
            LogType type = *reinterpret_cast<const LogType *>(buffer_.buffer_ + inner_offset + OFFSET_LOG_TYPE);
            switch (type) {
                case LogType::BEGIN:
                    record = new BeginLogRecord();
                    break;
                case LogType::ABORT:
                    record = new AbortLogRecord();
                    break;
                case LogType::COMMIT:
                    record = new CommitLogRecord();
                    break;
                case LogType::INSERT:
                    record = new InsertLogRecord();
                    break;
                case LogType::UPDATE:
                    record = new UpdateLogRecord();
                    break;
                case LogType::DELETE:
                    record = new DeleteLogRecord();
                    break;
                case LogType::NEWPAGE:
                    record = new NewPageLogRecord();
                    break;
                case LogType::FSMUPDATE:
                    record = new FSMUpdateLogRecord();
                    break;
                case LogType::BLINKINSERT:
                    record = new BLinkInsertLogRecord();
                    break;
                case LogType::BLINKDELETE:
                    record = new BLinkDeleteLogRecord();
                    break;
                case LogType::BATCHEND:
                    record = new BatchEndLogRecord();
                    break;
                case LogType::ABORTEND:
                    record = new AbortLogRecord();
                    break;
                default:
                    assert(0);
                    break;
            }
            record->deserialize(buffer_.buffer_ + inner_offset);
            // redo the log if necessary
            apply_sigle_log(record, offset + inner_offset);
            // replay_batch_id = record->log_batch_id_;
            delete record;
            inner_offset += size;
            // P1 续：与 replayFunV2 相同的周期性缓存落盘（有界等待窗口）
            if (++applied_since_flush_ >= 2048) {
                applied_since_flush_ = 0;
                FlushReplayPages();
            }
        }
        offset += inner_offset;
    }
}

void LogReplay::checkpointFun(){
    /*
    将缓冲池中合适的脏页刷入磁盘，更新LLSN集合
    有一个问题是对页面先修改的日志A可能比后修改的日志B后写入日志文件中，这样在重做时就会出现先重做B再重做A的情况
    一种解决办法是让A的事务写完日志再释放页面所有权，但是太卡性能了
    
    */
    // offset 指向下一个要读的起始位置
   

}

void LogReplay::restore() {
   //有一个问题是什么时候使用这个函数

   //第一阶段，判断哪些事务是做完的，可以通过查崩溃节点事务的endlog来判断
   
   //第二三阶段，应用redo和undolog

   //redo:原来redo的逻辑
   //undo:同理

   //第四阶段，同步与清理锁表之类数据结构
   //待思考咋写

}

bool LogReplay::RedoForPage(const std::string& table_name, page_id_t page_no,
                            LLSN disk_lsn, LLSN target_lsn, char* out_page_data) {
    // 当 gplm_lsn 丢失（=0）时，回放范围从当前磁盘位置到所有日志末尾
    if (target_lsn == 0) {
        target_lsn = UINT64_MAX;
    }
    if (disk_lsn >= target_lsn) {
        return false;  // 页面已是最新，无需回放
    }

    // 打开目标表文件
    int fd = disk_manager_->open_file(table_name);
    if (fd < 0) {
        LOG(WARNING) << "[RedoForPage] Cannot open file for table " << table_name;
        return false;
    }

    // 读取当前磁盘页面作为基础
    char page_data[PAGE_SIZE];
    try {
        disk_manager_->read_page(fd, page_no, page_data, PAGE_SIZE);
    } catch (const std::exception& e) {
        LOG(WARNING) << "[RedoForPage] Failed to read page " << page_no << ": " << e.what();
        return false;
    }

    // 扫描日志，收集与目标页面相关的日志记录（统一扫描层 LogStreamReader）
    uint64_t end_addr;
    {
        std::lock_guard<std::mutex> l(latch1_);
        end_addr = max_replay_off_ + 1;
    }

    std::vector<RedoEntry> redo_entries;
    ScanAllLogs(end_addr, [&](const char* rec, uint32_t log_size, uint64_t addr) -> bool {
        LogType type = *reinterpret_cast<const LogType*>(rec + OFFSET_LOG_TYPE);
        LLSN log_lsn = *reinterpret_cast<const LLSN*>(rec + OFFSET_LSN);

        // 只关注 disk_lsn < log_lsn <= target_lsn 范围内的日志
        if (log_lsn <= disk_lsn || log_lsn > target_lsn) {
            return true;
        }
        bool is_target_page = false;

        if (type == LogType::UPDATE) {
            UpdateLogRecord update_log;
            update_log.deserialize(rec);
            std::string log_table(update_log.table_name_,
                                 update_log.table_name_ + update_log.table_name_size_);
            if (log_table == table_name && update_log.rid_.page_no_ == (int)page_no) {
                is_target_page = true;
            }
        } else if (type == LogType::INSERT) {
            InsertLogRecord insert_log;
            insert_log.deserialize(rec);
            std::string log_table(insert_log.table_name_,
                                 insert_log.table_name_ + insert_log.table_name_size_);
            if (log_table == table_name && insert_log.page_no_ == (int)page_no) {
                is_target_page = true;
            }
        } else if (type == LogType::DELETE) {
            DeleteLogRecord delete_log;
            delete_log.deserialize(rec);
            std::string log_table(delete_log.table_name_,
                                 delete_log.table_name_ + delete_log.table_name_size_);
            if (log_table == table_name && delete_log.page_no_ == (int)page_no) {
                is_target_page = true;
            }
        }

        if (is_target_page) {
            redo_entries.push_back({log_lsn, addr, log_size});
        }
        return true;
    });

    if (redo_entries.empty()) {
        LOG(INFO) << "[RedoForPage] No redo entries found for table=" << table_name
                  << " page=" << page_no << " (disk_lsn=" << disk_lsn
                  << ", target_lsn=" << target_lsn << ")";
        return false;
    }

    // 按 LSN 排序
    std::sort(redo_entries.begin(), redo_entries.end(),
              [](const RedoEntry& a, const RedoEntry& b) { return a.lsn < b.lsn; });

    LOG(INFO) << "[RedoForPage] Replaying " << redo_entries.size() << " log entries for table="
              << table_name << " page=" << page_no
              << " (disk_lsn=" << disk_lsn << " -> target_lsn=" << target_lsn << ")";

    // 读取文件头信息（用于计算 slot 偏移）
    RmFileHdr file_hdr{};
    char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
    disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
    file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);

    // 按 LSN 顺序应用每条日志
    int applied_count = ApplyRedoEntriesToPage(fd, file_hdr, redo_entries, page_data);

    if (applied_count > 0) {
        // 将回放后的页面写回磁盘
        disk_manager_->write_page(fd, page_no, page_data, PAGE_SIZE);
        // 输出回放后的数据
        memcpy(out_page_data, page_data, PAGE_SIZE);
        LOG(INFO) << "[RedoForPage] Successfully applied " << applied_count
                  << " redo entries for table=" << table_name << " page=" << page_no
                  << ", new LSN=" << reinterpret_cast<RmPageHdr*>(page_data)->LLSN_;
        return true;
    }

    return false;
}

std::vector<LogReplay::RecoveryRedoResult>
LogReplay::RedoForPages(const std::vector<RecoveryRedoRequest>& requests) {
    std::vector<RecoveryRedoResult> results(requests.size());
    if (requests.empty()) return results;

    // 收集每个待恢复页面的上下文：key = (table_name, page_no)
    struct PageCtx {
        size_t req_index;          // 对应 requests/results 的下标
        LLSN disk_lsn;
        LLSN target_lsn;           // 已归一化（0 -> UINT64_MAX）
        bool need_redo;            // disk_lsn < target_lsn 才需要回放
        std::vector<RedoEntry> entries;
    };
    std::map<std::pair<std::string, page_id_t>, PageCtx> ctx_map;

    for (size_t i = 0; i < requests.size(); i++) {
        const auto& req = requests[i];
        LLSN target = (req.target_lsn == 0) ? UINT64_MAX : req.target_lsn;
        PageCtx ctx;
        ctx.req_index = i;
        ctx.disk_lsn = req.disk_lsn;
        ctx.target_lsn = target;
        ctx.need_redo = req.disk_lsn < target;
        ctx_map[{req.table_name, req.page_no}] = std::move(ctx);
    }

    // 单次扫描日志（统一扫描层 LogStreamReader），为所有页面分桶收集相关日志
    uint64_t end_addr;
    {
        std::lock_guard<std::mutex> l(latch1_);
        end_addr = max_replay_off_ + 1;
    }
    bool scan_complete = ScanAllLogs(end_addr, [&](const char* rec, uint32_t log_size, uint64_t addr) -> bool {
        LogType type = *reinterpret_cast<const LogType*>(rec + OFFSET_LOG_TYPE);
        LLSN log_lsn = *reinterpret_cast<const LLSN*>(rec + OFFSET_LSN);

        // 只处理数据日志（UPDATE/INSERT/DELETE），解析出 (table, page) 后查分桶
        if (type != LogType::UPDATE && type != LogType::INSERT && type != LogType::DELETE) {
            return true;
        }

        std::string log_table;
        page_id_t log_page_no = 0;

        if (type == LogType::UPDATE) {
            UpdateLogRecord update_log;
            update_log.deserialize(rec);
            log_table.assign(update_log.table_name_,
                             update_log.table_name_ + update_log.table_name_size_);
            log_page_no = update_log.rid_.page_no_;
        } else if (type == LogType::INSERT) {
            InsertLogRecord insert_log;
            insert_log.deserialize(rec);
            log_table.assign(insert_log.table_name_,
                             insert_log.table_name_ + insert_log.table_name_size_);
            log_page_no = insert_log.page_no_;
        } else {
            DeleteLogRecord delete_log;
            delete_log.deserialize(rec);
            log_table.assign(delete_log.table_name_,
                             delete_log.table_name_ + delete_log.table_name_size_);
            log_page_no = delete_log.page_no_;
        }

        auto it = ctx_map.find({log_table, log_page_no});
        if (it != ctx_map.end()) {
            PageCtx& ctx = it->second;
            // 与 RedoForPage 相同的过滤条件：disk_lsn < log_lsn <= target_lsn
            if (ctx.need_redo && log_lsn > ctx.disk_lsn && log_lsn <= ctx.target_lsn) {
                ctx.entries.push_back({log_lsn, addr, log_size});
            }
        }
        return true;
    });

    if (!scan_complete) return results;
    recovery_observation::Span redo_span("targeted_heap_redo");
    // 逐页回放
    for (auto& kv : ctx_map) {
        const auto& key = kv.first;
        PageCtx& ctx = kv.second;
        auto& res = results[ctx.req_index];

        res.scan_complete = true;
        if (!ctx.need_redo || ctx.entries.empty()) {
            res.no_matching_redo = true;
            continue;
        }

        // 按 LSN 排序后应用
        std::sort(ctx.entries.begin(), ctx.entries.end(),
                  [](const RedoEntry& a, const RedoEntry& b) { return a.lsn < b.lsn; });

        int fd = disk_manager_->open_file(key.first);
        if (fd < 0) {
            LOG(WARNING) << "[RedoForPages] Cannot open file for table " << key.first;
            continue;
        }

        char page_data[PAGE_SIZE];
        try {
            disk_manager_->read_page(fd, key.second, page_data, PAGE_SIZE);
        } catch (const std::exception& e) {
            LOG(WARNING) << "[RedoForPages] Failed to read page " << key.second << ": " << e.what();
            continue;
        }

        RmFileHdr file_hdr{};
        char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
        disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
        file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);

        int applied = ApplyRedoEntriesToPage(fd, file_hdr, ctx.entries, page_data);
        if (applied == static_cast<int>(ctx.entries.size()) && applied > 0 &&
            (ctx.target_lsn == UINT64_MAX || reinterpret_cast<RmPageHdr*>(page_data)->LLSN_ >= ctx.target_lsn)) {
            // R2c C2 版本守卫（32.2"新提交不会被旧恢复覆盖"的页级防线）：
            // 写回前校验盘上页头 LLSN 仍等于读基线 disk_lsn。若读-应用-写回
            // 窗口内该页被并发前进（在线域/replay 写入），此处重放像已过时，
            // 直接写回等于用旧基线覆盖新提交——放弃写回并显式失败，
            // 该页保持 IR 隔离由调用方重试（fail-closed）。
            char recheck[PAGE_SIZE];
            disk_manager_->read_page(fd, key.second, recheck, PAGE_SIZE);
            LLSN disk_now = reinterpret_cast<RmPageHdr*>(recheck)->LLSN_;
            if (disk_now != ctx.disk_lsn) {
                LOG(WARNING) << "[RedoForPages] write-back version guard rejected: table="
                             << key.first << " page=" << key.second
                             << " baseline_lsn=" << ctx.disk_lsn
                             << " disk_lsn_now=" << disk_now
                             << " — page advanced concurrently, redo abandoned";
                continue;
            }
            disk_manager_->write_page(fd, key.second, page_data, PAGE_SIZE);
            res.success = true;
            res.page_data.assign(page_data, PAGE_SIZE);
            res.recovered_lsn = reinterpret_cast<RmPageHdr*>(page_data)->LLSN_;
            LOG(INFO) << "[RedoForPages] Applied " << applied << " redo entries for table="
                      << key.first << " page=" << key.second
                      << " (disk_lsn=" << ctx.disk_lsn
                      << " -> target_lsn=" << ctx.target_lsn << ")";
        }
    }

    return results;
}

int LogReplay::ApplyRedoEntriesToPage(int fd, const RmFileHdr& file_hdr,
                                      const std::vector<RedoEntry>& entries,
                                      char* page_data) {
    int applied_count = 0;
    for (const auto& entry : entries) {
        std::vector<char> log_buf(entry.size);
        // 统一按逻辑偏移读单条记录（v2 段式 / legacy 单文件）
        uint32_t rec_len = 0;
        if (!ReadLogRecordAt(entry.offset, log_buf.data(), entry.size, rec_len) || rec_len < entry.size) {
            continue;
        }

        LogType type = *reinterpret_cast<const LogType*>(log_buf.data() + OFFSET_LOG_TYPE);
        RmPageHdr* page_hdr = reinterpret_cast<RmPageHdr*>(page_data);

        switch (type) {
            case LogType::UPDATE: {
                UpdateLogRecord update_log;
                update_log.deserialize(log_buf.data());
                // 跳过 LSN 检查（强制回放），直接应用
                page_hdr->pre_LLSN_ = page_hdr->LLSN_;
                page_hdr->LLSN_ = static_cast<LLSN>(update_log.lsn_);

                char* bitmap = page_data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
                char* slots = bitmap + file_hdr.bitmap_size_;
                char* tuple = slots + update_log.rid_.slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
                itemkey_t* item_key = reinterpret_cast<itemkey_t*>(tuple);
                *item_key = update_log.new_value_.key_;
                memcpy(tuple + sizeof(itemkey_t), update_log.new_value_.value_,
                       update_log.new_value_.value_size_);
                applied_count++;
                break;
            }
            case LogType::INSERT: {
                InsertLogRecord insert_log;
                insert_log.deserialize(log_buf.data());
                page_hdr->pre_LLSN_ = page_hdr->LLSN_;
                page_hdr->LLSN_ = static_cast<LLSN>(insert_log.lsn_);

                char* bitmap = page_data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
                if (!Bitmap::is_set(bitmap, insert_log.slot_no_)) {
                    Bitmap::set(bitmap, insert_log.slot_no_);
                    page_hdr->num_records_++;
                }
                char* slots = bitmap + file_hdr.bitmap_size_;
                char* tuple = slots + insert_log.slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
                itemkey_t* item_key = reinterpret_cast<itemkey_t*>(tuple);
                *item_key = insert_log.insert_value_.key_;
                memcpy(tuple + sizeof(itemkey_t), insert_log.insert_value_.value_,
                       insert_log.insert_value_.value_size_);
                applied_count++;
                break;
            }
            case LogType::DELETE: {
                DeleteLogRecord delete_log;
                delete_log.deserialize(log_buf.data());
                page_hdr->pre_LLSN_ = page_hdr->LLSN_;
                page_hdr->LLSN_ = static_cast<LLSN>(delete_log.lsn_);

                char* bitmap = page_data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
                if (Bitmap::is_set(bitmap, delete_log.slot_no_)) {
                    Bitmap::reset(bitmap, delete_log.slot_no_);
                    if (page_hdr->num_records_ > 0) page_hdr->num_records_--;
                }
                applied_count++;
                break;
            }
            default:
                break;
        }
    }
    return applied_count;
}

void LogReplay::ObserveRecoveryBacklog(const char* reason) {
    if (!recovery_observation::Enabled()) return;
    recovery_observation::Span span("backlog_identification");
    std::lock_guard<std::recursive_mutex> pause(replay_pause_mtx_);
    uint64_t start, end;
    { std::lock_guard<std::mutex> lock(latch2_); start = persist_off_ + 1; }
    { std::lock_guard<std::mutex> lock(latch1_); end = max_replay_off_ + 1; }
    uint64_t counts[3]{}, bytes[3]{}, io_bytes = 0;
    const uint64_t cap = recovery_observation::EnvUInt("HCM_TRACE_WAL_SCAN_BYTES", 8 * 1024 * 1024, 1024ULL * 1024 * 1024);
    auto count = [&](const char* rec, uint32_t len) {
        LogType type;
        memcpy(&type, rec + OFFSET_LOG_TYPE, sizeof(type));
        int kind = (type == LogType::BLINKINSERT || type == LogType::BLINKDELETE) ? 1
            : (type == LogType::INSERT || type == LogType::UPDATE || type == LogType::DELETE) ? 0 : 2;
        ++counts[kind]; bytes[kind] += len;
    };
    bool complete = start >= end;
    if (!complete && v2_mode_) {
        logfmt::LogStreamReader reader([&](uint64_t seg, char* buf, size_t size, uint64_t off) -> ssize_t {
            if (io_bytes + size > cap) return -1;
            ssize_t n = PreadSegment(seg, buf, size, off);
            if (n > 0) io_bytes += n;
            return n;
        });
        reader.Seek(start);
        const char* rec; uint32_t len; uint64_t addr;
        while (reader.Next(rec, len, addr, end)) count(rec, len);
        complete = reader.AtEnd(end);
    } else if (!complete) {
        uint64_t addr = start;
        while (addr < end && io_bytes < cap) {
            char header[LOG_HEADER_SIZE];
            if (addr + sizeof(header) > end || pread(log_replay_fd_, header, sizeof(header), addr) != sizeof(header)) break;
            io_bytes += sizeof(header);
            uint32_t len; memcpy(&len, header + OFFSET_LOG_TOT_LEN, sizeof(len));
            if (len < sizeof(header) || addr + len > end) break;
            count(header, len);
            addr += len;
        }
        complete = addr == end;
    }
    recovery_observation::Emit("backlog_bounds", -1, -1, start, end, io_bytes, reason);
    recovery_observation::Emit("backlog_heap", -1, -1, counts[0], bytes[0], complete, reason);
    recovery_observation::Emit("backlog_index", -1, -1, counts[1], bytes[1], complete, reason);
    recovery_observation::Emit("backlog_control", -1, -1, counts[2], bytes[2], complete, reason);
}

// 等待重放追平（Undo 前置条件）
bool LogReplay::WaitReplayCaughtUp(int timeout_ms) {
    recovery_observation::Span span("replay_barrier");
    // P0 修复：缓存 poison（丢脏页未恢复）时一律失败——不能把可能
    // 丢页的状态当作"已追平"用于任何安全判定
    if (ReplayCachePoisoned()) {
        LOG(ERROR) << "[LogReplay] WaitReplayCaughtUp: replay cache POISONED "
                      "(dirty pages dropped), refusing catch-up certificate";
        span.Stop(0);
        return false;
    }
    uint64_t target;
    {
        std::lock_guard<std::mutex> l(latch1_);
        target = max_replay_off_;
    }
    int waited_ms = 0;
    while (waited_ms < timeout_ms) {
        {
            std::lock_guard<std::mutex> l(latch2_);
            if (persist_off_ >= target) {
                // P0 修复（成功前复查 poison）：等待期间 replay 线程的驱逐/
                // flush 失败路径可能已把缓存置 poisoned——若不复查，会把
                // 可能丢页的状态当"已追平"返回 true。失败语义与入口检查
                // 一致（fail-closed）。
                if (ReplayCachePoisoned()) {
                    LOG(ERROR) << "[LogReplay] WaitReplayCaughtUp: replay cache POISONED "
                                  "during wait, refusing catch-up certificate";
                    span.Stop(0);
                    return false;
                }
                // P1 续："追平"的对外语义 = 已应用且已落盘。replay 页缓存
                // （write-back）中的脏页必须先落盘，WaitReplayCaughtUp 的
                // true 才能作为 GetPageWithLsn/ValidationCut/快照取页的
                // 前置保证，与无缓存实现完全一致。落盘失败 fail-closed：
                // 返回 false（未追平），绝不把失败静默当作成功。
                if (!FlushReplayPages()) {
                    LOG(ERROR) << "[LogReplay] WaitReplayCaughtUp: flush failed, treating as not caught up";
                    span.Stop(0);
                    return false;
                }
                span.Stop(1);
                return true;
            }
        }
        usleep(1000);
        waited_ms++;
    }
    LOG(ERROR) << "[Undo] WaitReplayCaughtUp timeout after " << timeout_ms << "ms";
    return false;
}

int LogReplay::UndoForFailedNode(node_id_t failed_node_id,
                                 const std::set<node_id_t>& alive_node_ids) {
    recovery_observation::Span undo_span("undo_all_active");
    // P0 修复：存活节点前缀事务跳过 Undo——存活节点的事务终局
    // （BATCHEND/ABORTEND+补偿）由节点自己负责并随后到达；不过滤时，
    // 扫描窗口内"数据日志已到、终局未到"的在途事务（含已提交待确认）
    // 会被误撤销。tx 前缀约定 (tx >> 48) == node_id + 1；前缀 0 视为
    // 无法归属（历史格式），保守 undo。
    const auto txn_belongs_to_alive = [&alive_node_ids](tx_id_t txn_id) -> bool {
        const uint64_t prefix = txn_id >> 48;
        if (prefix == 0) return false;
        return alive_node_ids.count(static_cast<node_id_t>(prefix - 1)) > 0;
    };
    // C4 修复（第一步，顺序）：先等待重放线程追平日志尾。
    // undo 基于"日志已物化"假设——若 undo 快于重放，未提交事务的日志
    // 随后会被 replay 再次物化，导致已撤销的脏数据复活
    if (!WaitReplayCaughtUp()) {
        LOG(ERROR) << "[UndoForFailedNode] replay not caught up, abort undo for safety";
        return -1;
    }

    // C4 修复（第二步，互斥）：Undo（RPC 线程）用 update_value/write_page
    // 直写数据文件，与 replayFun（后台线程）的 write_page 并发会产生
    // 页面级撕裂。Undo 期间暂停重放线程，结束后恢复（RAII 保证异常路径）
    // 注：独立 undo 区路径下停顿窗口为 O(未提交日志数)；fallback 的
    // 两遍全扫路径才随日志总量增长
    struct ReplayPauseGuard {
        LogReplay* lr;
        explicit ReplayPauseGuard(LogReplay* l) : lr(l) { lr->PauseReplay(); }
        ~ReplayPauseGuard() { lr->ResumeReplay(); }
    } pause_guard(this);

    // 优先走独立 undo 区：事务表 + undo 链直接定位未提交事务的全部 undo
    // 记录（O(未提交日志数)），替代下方两遍全扫 WAL（O(全量日志)）。
    // undo 区不可用（初始化失败/写失败）时回退原路径，功能不缺失
    if (undo_area_ != nullptr && undo_area_->IsEnabled()) {
        // P0 修复（Undo 虚报成功）：回调返回 1=已应用 / 0=幂等无操作 /
        // -1=硬失败（缺前镜像/文件不可用）。硬失败或 undo 区链/记录损坏
        // （UndoAllActiveTxns 返回 -1）时不得发布完成——UndoForFailedNode
        // 返回 -1，调用方 AnalyzeRecoveryPages 置 RPC 失败，恢复保持隔离。
        int hard_failures = 0;
        int undone = undo_area_->UndoAllActiveTxns(failed_node_id,
            [this, &hard_failures](const char* wal, uint32_t len) -> int {
                bool hard = false;
                const bool applied = ApplyUndoWalRecord(wal, len, &hard);
                if (hard) hard_failures++;
                return applied ? 1 : (hard ? -1 : 0);
            },
            alive_node_ids);
        if (undone >= 0 && hard_failures == 0) {
            LOG(INFO) << "[UndoForFailedNode] undo via undo area (failed node "
                      << failed_node_id << "): " << undone << " operations undone";
            // P0 修复：undo 直写磁盘，replay 缓存残留的旧内容会在 resume 后
            // 被新日志应用复活——整体失效（pause 窗口内缓存已 flush clean）
            InvalidateAllReplayPages();
            return undone;
        }
        if (hard_failures > 0) {
            LOG(ERROR) << "[UndoForFailedNode] " << hard_failures
                       << " undo records NOT reversible (missing before-image / file unavailable)"
                          " — recovery must stay isolated";
            // 已部分应用的 undo 仍需失效缓存（pause 窗口内），防止 resume
            // 后旧缓存内容复活覆盖直写结果
            InvalidateAllReplayPages();
            return -1;
        }
        LOG(WARNING) << "[UndoForFailedNode] undo area unavailable, "
                        "fallback to WAL full scan";
    }

    // 第一步：扫描日志，构建事务状态表（统一扫描层 LogStreamReader）
    uint64_t end_addr;
    {
        std::lock_guard<std::mutex> l(latch1_);
        end_addr = max_replay_off_ + 1;
    }

    // 收集故障节点的事务状态
    std::unordered_set<tx_id_t> committed_txns;  // 已提交的事务
    struct UndoLogEntry {
        uint64_t offset;
        uint32_t size;
        LLSN lsn;
        tx_id_t txn_id;
    };
    std::vector<UndoLogEntry> undo_candidates;  // 可能需要 undo 的日志

    bool undo_scan_complete = ScanAllLogs(end_addr, [&](const char* rec, uint32_t log_size, uint64_t addr) -> bool {
        LogType type = *reinterpret_cast<const LogType*>(rec + OFFSET_LOG_TYPE);
        tx_id_t log_txn_id = *reinterpret_cast<const tx_id_t*>(rec + OFFSET_LOG_TID);
        LLSN log_lsn = *reinterpret_cast<const LLSN*>(rec + OFFSET_LSN);

        // 关注所有节点的日志（不仅故障节点），因为存活节点在恢复期间
        // abort 的事务也可能在存储层留下 lock=EXCLUSIVE_LOCKED 的脏数据。
        // P0 修复：存活节点前缀事务跳过（终局由其自身负责，见函数头注释）
        if (txn_belongs_to_alive(log_txn_id)) return true;
        if (type == LogType::BATCHEND || type == LogType::ABORTEND) {
            // BatchEnd/ABORTEND 分别表示提交终局和补偿完成终局；二者都不能再按未决事务 Undo
            committed_txns.insert(log_txn_id);
        } else if (type == LogType::UPDATE || type == LogType::INSERT || type == LogType::DELETE ||
                   type == LogType::BLINKINSERT || type == LogType::BLINKDELETE ||
                   type == LogType::FSMUPDATE) {
            undo_candidates.push_back({addr, log_size, log_lsn, log_txn_id});
        }
        return true;
    });

    if (!undo_scan_complete) return -1;
    // 第二步：反向扫描，对未提交事务执行 Undo
    int undo_count = 0;
    int fallback_hard_failures = 0;
    // 从后往前遍历：undo_candidates 按日志文件偏移升序收集，
    // 反向即"日志流降序"（同一页面/事务内的补偿顺序由此保证）
    for (int i = (int)undo_candidates.size() - 1; i >= 0; i--) {
        const auto& entry = undo_candidates[i];
        // 如果事务已提交，跳过
        if (committed_txns.count(entry.txn_id) > 0) continue;

        // 读取日志记录并执行 undo（统一按逻辑偏移读单条记录）
        std::vector<char> log_buf(entry.size);
        uint32_t rec_len = 0;
        if (!ReadLogRecordAt(entry.offset, log_buf.data(), entry.size, rec_len) || rec_len < entry.size) {
            return -1;
        }
        bool hard = false;
        if (ApplyUndoWalRecord(log_buf.data(), rec_len, &hard)) {
            undo_count++;
        } else if (hard) {
            // P0 修复（Undo 虚报成功）：缺前镜像/文件不可用不是可重试的
            // 正常事件——计数并最终失败，不发布 undo 完成证书
            fallback_hard_failures++;
            LOG(ERROR) << "[UndoForFailedNode] WAL fallback: record NOT reversible "
                          "(missing before-image / file unavailable) at offset "
                       << entry.offset << " txn " << entry.txn_id;
        }
    }
    if (fallback_hard_failures > 0) {
        LOG(ERROR) << "[UndoForFailedNode] " << fallback_hard_failures
                   << " records not reversible — recovery must stay isolated";
        InvalidateAllReplayPages();
        return -1;
    }

    LOG(INFO) << "[UndoForFailedNode] Completed undo (triggered by failed node " << failed_node_id
              << "): " << undo_count << " operations undone (all nodes), "
              << committed_txns.size() << " committed transactions preserved";
    // P0 修复：undo 直写磁盘，整体失效 replay 缓存防旧内容复活
    InvalidateAllReplayPages();
    return undo_count;
}

// 对一条 WAL 记录字节流执行 undo 应用。undo 区路径与 WAL 全扫 fallback
// 路径共用本函数，保证两条路径 undo 语义完全一致。所有 undo 操作幂等
// （UPDATE/DELETE/FSM 写回旧值；INSERT 查 bitmap 后清；blink 操作互逆幂等），
// 重复应用安全（崩溃后重 undo / undo 区重复记录场景）。
// 返回 false 且 *hard_failed=true 表示不可撤销的失败（缺前镜像/文件不可用），
// 调用方必须使恢复保持隔离；返回 false 且 *hard_failed=false 表示幂等
// 无操作（已撤销/键不匹配/FSM 启发式自愈），不构成失败。
bool LogReplay::ApplyUndoWalRecord(const char* rec, uint32_t len, bool* hard_failed) {
    if (hard_failed != nullptr) *hard_failed = false;
    if (rec == nullptr || len < LOG_HEADER_SIZE) {
        // 记录头不可读：无法判定内容，按硬失败处理（fail-closed）
        if (hard_failed != nullptr) *hard_failed = true;
        return false;
    }
    LogType type = *reinterpret_cast<const LogType*>(rec + OFFSET_LOG_TYPE);

    switch (type) {
        case LogType::UPDATE: {
            UpdateLogRecord update_log;
            update_log.deserialize(rec);
            if (update_log.HasUndoPayload()) {
                apply_undo_log(&update_log);
                return true;
            }
            // UPDATE 无前镜像：无法恢复旧值，硬失败
            if (hard_failed != nullptr) *hard_failed = true;
            return false;
        }
        case LogType::INSERT: {
            // Undo insert = 清除 bitmap 中对应 slot
            InsertLogRecord insert_log;
            insert_log.deserialize(rec);
            std::string tbl_name(insert_log.table_name_,
                                 insert_log.table_name_ + insert_log.table_name_size_);
            int fd = disk_manager_->open_file(tbl_name);
            if (fd < 0) {
                // 目标表文件不可用：undo 无法执行，硬失败
                if (hard_failed != nullptr) *hard_failed = true;
                return false;
            }

            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);

            char buffer[PAGE_SIZE];
            disk_manager_->read_page(fd, insert_log.page_no_, buffer, PAGE_SIZE);
            auto* page_hdr = reinterpret_cast<RmPageHdr*>(buffer);
            char* bitmap = buffer + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;

            if (insert_log.slot_no_ < 0 || insert_log.slot_no_ >= file_hdr.num_records_per_page_ ||
                file_hdr.record_size_ < static_cast<int>(sizeof(DataItem)) ||
                sizeof(RmPageHdr) + file_hdr.bitmap_size_ + static_cast<uint64_t>(file_hdr.num_records_per_page_) *
                    (sizeof(itemkey_t) + file_hdr.record_size_) > PAGE_SIZE)
                throw std::runtime_error("invalid INSERT Undo RID/layout");
            char* tuple = bitmap + file_hdr.bitmap_size_ + insert_log.slot_no_ * (sizeof(itemkey_t) + file_hdr.record_size_);
            itemkey_t current_key;
            memcpy(&current_key, tuple, sizeof(current_key));
            if (current_key != insert_log.insert_value_.key_) return false;
            if (Bitmap::is_set(bitmap, insert_log.slot_no_)) {
                auto* item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
                item->valid = 0;
                item->lock = UNLOCKED;
                item->user_insert = 0;
                Bitmap::reset(bitmap, insert_log.slot_no_);
                if (page_hdr->num_records_ <= 0) throw std::runtime_error("INSERT Undo invalid heap count");
                page_hdr->num_records_--;
                disk_manager_->write_page(fd, insert_log.page_no_, buffer, PAGE_SIZE);
                return true;
            }
            return false;
        }
        case LogType::DELETE: {
            DeleteLogRecord delete_log;
            delete_log.deserialize(rec);
            if (delete_log.has_undo_meta_) {
                apply_undo_log(&delete_log);
                return true;
            }
            // DELETE 无前镜像：无法恢复被删记录，硬失败
            if (hard_failed != nullptr) *hard_failed = true;
            return false;
        }
        case LogType::BLINKINSERT: {
            // undo 插入 = 从存储侧 blink 树删除该 key（幂等）
            BLinkInsertLogRecord bl_log;
            bl_log.deserialize(rec);
            std::string bl_table = ResolveTableName(bl_log.table_id_, bl_log.table_name_, bl_log.table_name_size_);
            if (!sm_manager) throw std::runtime_error("BLink Undo has no storage manager");
            auto* handle = sm_manager->GetOrCreateBLinkHandle(bl_table);
            if (!handle || !handle->valid()) throw std::runtime_error("BLink Undo index unavailable");
            std::lock_guard<std::mutex> lock(handle->get_op_mutex());
            Rid current;
            if (!handle->search(&bl_log.key_, current) || !(current == bl_log.rid_)) return false;
            handle->remove_entry(&bl_log.key_);
            return true;
        }
        case LogType::BLINKDELETE: {
            // undo 删除 = 用日志记录的 (key, rid) 重新插入（幂等）
            BLinkDeleteLogRecord bl_log;
            bl_log.deserialize(rec);
            std::string bl_table = ResolveTableName(bl_log.table_id_, bl_log.table_name_, bl_log.table_name_size_);
            if (!sm_manager) throw std::runtime_error("BLink Undo has no storage manager");
            auto* handle = sm_manager->GetOrCreateBLinkHandle(bl_table);
            if (!handle || !handle->valid()) throw std::runtime_error("BLink Undo index unavailable");
            std::lock_guard<std::mutex> lock(handle->get_op_mutex());
            Rid current;
            if (handle->search(&bl_log.key_, current)) return false;
            if (handle->insert_entry(&bl_log.key_, bl_log.rid_) == INVALID_PAGE_ID)
                throw std::runtime_error("BLink Undo INSERT failed");
            return true;
        }
        case LogType::FSMUPDATE: {
            // undo FSM 更新 = 恢复旧空间值（类别级近似；无旧值时跳过，
            // FSM 属启发式数据，误差由后续更新自愈）
            FSMUpdateLogRecord fsm_log;
            fsm_log.deserialize(rec);
            if (fsm_log.HasUndoMeta()) {
                std::string fsm_table = ResolveTableName(fsm_log.table_id_, fsm_log.table_name_, fsm_log.table_name_size_);
                ApplyFSMUpdate(fsm_log.table_id_, fsm_table, fsm_log.page_id_, fsm_log.old_free_space_);
                return true;
            }
            return false;
        }
        default:
            return false;
    }
}
void LogReplay::print_llsnrecord() {
    if (llsnrecord.empty()) {
        std::cout << "llsnrecord is empty\n";
        return;
    }
   for (auto it = llsnrecord.begin(); it != llsnrecord.end(); ++it) {
        std::cout << "Key: " << it->first << " -> Values: ";
        // 遍历当前键对应的 vector
        for (auto vec_it = it->second.begin(); vec_it != it->second.end(); ++vec_it) {
            std::cout << *vec_it << " ";
        }
        std::cout << std::endl;
    }
}