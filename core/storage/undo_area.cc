#include "undo_area.h"

#include <algorithm>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <butil/crc32c.h>
#include <butil/logging.h>

UndoArea::UndoArea(std::string dir, uint64_t seg_size)
    : dir_(std::move(dir)), seg_size_(seg_size) {
    Init();
}

UndoArea::~UndoArea() {
    if (cur_seg_fd_ >= 0) ::close(cur_seg_fd_);
    if (txn_table_fd_ >= 0) ::close(txn_table_fd_);
    for (auto& kv : read_fds_) ::close(kv.second);
    read_fds_.clear();
}

void UndoArea::Init() {
    // 目录创建（与 log_v2 同级的相对路径约定）
    if (::mkdir(dir_.c_str(), 0755) != 0 && errno != EEXIST) {
        LOG(ERROR) << "[UndoArea] mkdir " << dir_ << " failed: " << strerror(errno)
                   << ", undo area disabled (fallback to WAL full-scan undo)";
        return;
    }
    const std::string txn_table_path = dir_ + "/txn_table.log";
    txn_table_fd_ = ::open(txn_table_path.c_str(), O_RDWR | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR);
    if (txn_table_fd_ < 0) {
        LOG(ERROR) << "[UndoArea] open txn_table failed, undo area disabled";
        return;
    }

    // 崩溃重建（索引即缓存：全部结构从盘内容重建，不构成新故障点）
    std::vector<uint64_t> segs = ListSegments();
    std::unordered_map<uint64_t, uint64_t> seg_valid_end;
    for (uint64_t seg_id : segs) {
        seg_valid_end[seg_id] = ScanSegment(seg_id);
    }
    ReplayTxnTable();
    RebuildSegRefsAndCleanup(segs);

    // 写位置：最大段仍有活跃引用则在其有效数据尾续写（尾部半写已被扫描
    // 截断，续写恰好覆盖坏尾）；无活跃引用的段已被上方清理删除，从下一
    // 段号开新段，保证 undo 地址全局单调
    if (segs.empty() || seg_refs_.count(segs.back()) == 0) {
        cur_seg_id_ = segs.empty() ? 1 : segs.back() + 1;
        cur_seg_off_ = 0;
    } else {
        cur_seg_id_ = segs.back();
        cur_seg_off_ = seg_valid_end[cur_seg_id_];
    }
    MaybeCompactTxnTableLocked();
    enabled_ = true;
    LOG(INFO) << "[UndoArea] initialized: dir=" << dir_ << " active_txns=" << chains_.size()
              << " cur_seg=" << cur_seg_id_ << " off=" << cur_seg_off_;
}

std::vector<uint64_t> UndoArea::ListSegments() {
    std::vector<uint64_t> segs;
    DIR* d = ::opendir(dir_.c_str());
    if (d == nullptr) return segs;
    struct dirent* ent;
    while ((ent = ::readdir(d)) != nullptr) {
        uint64_t seg_id = 0;
        if (sscanf(ent->d_name, "useg_%lx.log", &seg_id) == 1 && seg_id > 0) {
            segs.push_back(seg_id);
        }
    }
    ::closedir(d);
    std::sort(segs.begin(), segs.end());
    return segs;
}

// 扫描单个 undo 段重建事务链。记录自描述（内嵌 WAL 记录头含 node_id/tid），
// 扫描序 = 写入序，逐条覆盖 chain.last_addr 即得链尾。返回有效数据尾偏移
// （尾部半写/坏记录截断——与 WAL 段 RecoverWriteEndAddr 同语义）。
uint64_t UndoArea::ScanSegment(uint64_t seg_id) {
    const std::string path = undofmt::UndoSegFileName(dir_, seg_id);
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return 0;
    struct stat st;
    if (::fstat(fd, &st) != 0) { ::close(fd); return 0; }
    const uint64_t file_size = static_cast<uint64_t>(st.st_size);

    uint64_t off = 0;
    while (off + undofmt::UNDO_REC_HEADER_SIZE <= file_size) {
        char hdr[undofmt::UNDO_REC_HEADER_SIZE];
        if (::pread(fd, hdr, sizeof(hdr), (off_t)off) != (ssize_t)sizeof(hdr)) break;
        uint32_t magic, rec_len;
        memcpy(&magic, hdr, 4);
        memcpy(&rec_len, hdr + 4, 4);
        if (magic != undofmt::UNDO_REC_MAGIC || rec_len < undofmt::UNDO_REC_HEADER_SIZE ||
            off + rec_len > file_size) {
            break;  // 半写/损坏：截断
        }
        std::vector<char> rec(rec_len);
        memcpy(rec.data(), hdr, sizeof(hdr));
        if (::pread(fd, rec.data() + sizeof(hdr), rec_len - sizeof(hdr), (off_t)(off + sizeof(hdr)))
            != (ssize_t)(rec_len - sizeof(hdr))) {
            break;
        }
        uint32_t expect_crc;
        memcpy(&expect_crc, hdr + 8, 4);
        uint32_t actual_crc = butil::crc32c::Mask(
            butil::crc32c::Value(rec.data() + 12, rec_len - 12));
        if (expect_crc != actual_crc) {
            LOG(WARNING) << "[UndoArea] seg " << seg_id << " off " << off
                         << " crc mismatch, truncate";
            break;
        }
        // 内嵌 WAL 记录头取事务归属
        const char* wal = rec.data() + undofmt::UNDO_REC_HEADER_SIZE;
        TxnKey key;
        memcpy(&key.tid, wal + OFFSET_LOG_TID, sizeof(tx_id_t));
        memcpy(&key.node_id, wal + OFFSET_LOG_NODE_ID, sizeof(node_id_t));
        uint64_t prev_addr;
        memcpy(&prev_addr, hdr + 12, 8);

        auto& chain = chains_[key];
        if (chain.count == 0) { chain.node_id = key.node_id; chain.tid = key.tid; }
        chain.last_addr = undofmt::MakeUndoAddr(seg_id, off);
        chain.seg_ids.insert(seg_id);
        chain.count++;
        (void)prev_addr;  // 链关系留在记录内，遍历时按需读取，无需全量载入内存
        off += rec_len;
    }
    ::close(fd);
    return off;
}

void UndoArea::ReplayTxnTable() {
    // 重放状态变迁：已提交/已回滚事务从活跃表剔除（其 undo 残留随引用计数回收）
    const std::string path = dir_ + "/txn_table.log";
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return;
    struct stat st;
    if (::fstat(fd, &st) != 0) { ::close(fd); return; }
    const uint64_t file_size = static_cast<uint64_t>(st.st_size);

    uint64_t off = 0;
    while (off + undofmt::TXN_REC_SIZE <= file_size) {
        char buf[undofmt::TXN_REC_SIZE];
        if (::pread(fd, buf, sizeof(buf), (off_t)off) != (ssize_t)sizeof(buf)) break;
        uint32_t rec_len, crc;
        memcpy(&rec_len, buf, 4);
        memcpy(&crc, buf + 4, 4);
        if (rec_len != undofmt::TXN_REC_SIZE) break;  // 半写截断
        uint32_t actual = butil::crc32c::Mask(butil::crc32c::Value(buf + 8, sizeof(buf) - 8));
        if (crc != actual) {
            LOG(WARNING) << "[UndoArea] txn_table crc mismatch at " << off << ", truncate";
            break;
        }
        TxnKey key;
        memcpy(&key.node_id, buf + 8, 4);
        memcpy(&key.tid, buf + 12, 8);
        uint8_t status = buf[20];
        if (status == undofmt::TXN_STATUS_COMMITTED || status == undofmt::TXN_STATUS_UNDONE) {
            chains_.erase(key);
        }
        off += undofmt::TXN_REC_SIZE;
    }
    ::close(fd);
}

void UndoArea::RebuildSegRefsAndCleanup(const std::vector<uint64_t>& segs) {
    seg_refs_.clear();
    for (auto& kv : chains_) {
        for (uint64_t seg : kv.second.seg_ids) seg_refs_[seg]++;
    }
    // 零引用段（全部记录属已提交/已回滚事务的残留）整段回收
    for (uint64_t seg : segs) {
        if (seg_refs_.count(seg) == 0) {
            ::unlink(undofmt::UndoSegFileName(dir_, seg).c_str());
        }
    }
}

// ---------------- 写入路径（replay 线程） ----------------

void UndoArea::TrackLog(const LogRecord* rec) {
    if (!enabled_ || rec == nullptr) return;
    switch (rec->log_type_) {
        case LogType::UPDATE:
        case LogType::INSERT:
        case LogType::DELETE:
        case LogType::BLINKINSERT:
        case LogType::BLINKDELETE:
        case LogType::FSMUPDATE:
            break;
        default:
            return;  // 控制记录/NEWPAGE 无需 undo
    }
    if (rec->log_tid_ == INVALID_TXN_ID) return;

    // undo 记录 = 链头 + 完整内嵌 WAL 记录（undo 信息 WAL 记录自带，
    // 内嵌整体避免逐类型抽取字段，undo 应用侧直接 deserialize 复用现有逻辑）
    std::vector<char> wal(rec->log_tot_len_);
    rec->serialize(wal.data());

    std::lock_guard<std::mutex> lk(mtx_);
    TxnKey key{rec->log_node_id_, rec->log_tid_};
    auto& chain = chains_[key];
    if (chain.count == 0) { chain.node_id = key.node_id; chain.tid = key.tid; }
    uint64_t addr = AppendRecordLocked(wal.data(), rec->log_tot_len_, chain.last_addr);
    if (addr == 0) return;  // 写失败已禁用
    chain.last_addr = addr;
    chain.count++;
    uint64_t seg = undofmt::UndoAddrSegId(addr);
    if (chain.seg_ids.insert(seg).second) {
        seg_refs_[seg]++;
    }
}

void UndoArea::CommitTxn(node_id_t node_id, tx_id_t tid) {
    if (!enabled_) return;
    std::lock_guard<std::mutex> lk(mtx_);
    TxnKey key{node_id, tid};
    auto it = chains_.find(key);
    if (it == chains_.end()) return;  // 无数据日志的事务（只读/纯控制）
    // 顺序保证：先持久化提交状态，再释放空间——崩溃后状态可查，不会把
    // 已提交事务误 undo（process 崩溃模型下 write 进 page cache 即可，
    // 与系统整体 durability 一致）
    AppendTxnStatusLocked(node_id, tid, undofmt::TXN_STATUS_COMMITTED);
    ReleaseChainLocked(it->second);
    chains_.erase(it);
    MaybeCompactTxnTableLocked();
}

void UndoArea::AbortTxn(node_id_t node_id, tx_id_t tid) {
    if (!enabled_) throw std::runtime_error("Undo area disabled at abort terminal");
    std::lock_guard<std::mutex> lock(mtx_);
    const TxnKey key{node_id, tid};
    const auto it = chains_.find(key);
    AppendTxnStatusLocked(node_id, tid, undofmt::TXN_STATUS_UNDONE);
    if (!enabled_) throw std::runtime_error("Undo abort terminal write failed");
    if (it != chains_.end()) {
        ReleaseChainLocked(it->second);
        chains_.erase(it);
    }
    MaybeCompactTxnTableLocked();
}

uint64_t UndoArea::AppendRecordLocked(const char* wal_rec, uint32_t wal_len, uint64_t prev_addr) {
    uint32_t rec_len = undofmt::UNDO_REC_HEADER_SIZE + wal_len;
    if (rec_len > seg_size_) {
        LOG(ERROR) << "[UndoArea] record too large: " << rec_len;
        enabled_ = false;
        return 0;
    }
    if (cur_seg_off_ + rec_len > seg_size_) {
        // 段写满滚动；旧段引用计数在 CommitTxn/Undo 归零时删除
        if (cur_seg_fd_ >= 0) { ::close(cur_seg_fd_); cur_seg_fd_ = -1; }
        cur_seg_id_++;
        cur_seg_off_ = 0;
    }
    if (cur_seg_fd_ < 0) OpenCurSegLocked();
    if (cur_seg_fd_ < 0) { enabled_ = false; return 0; }

    std::vector<char> buf(rec_len);
    uint32_t magic = undofmt::UNDO_REC_MAGIC;
    memcpy(buf.data(), &magic, 4);
    memcpy(buf.data() + 4, &rec_len, 4);
    memcpy(buf.data() + 12, &prev_addr, 8);
    memcpy(buf.data() + undofmt::UNDO_REC_HEADER_SIZE, wal_rec, wal_len);
    uint32_t crc = butil::crc32c::Mask(
        butil::crc32c::Value(buf.data() + 12, rec_len - 12));
    memcpy(buf.data() + 8, &crc, 4);

    ssize_t n = ::pwrite(cur_seg_fd_, buf.data(), rec_len, (off_t)cur_seg_off_);
    if (n != (ssize_t)rec_len) {
        LOG(ERROR) << "[UndoArea] append failed: " << n << "/" << rec_len
                   << ", undo area disabled";
        enabled_ = false;
        return 0;
    }
    uint64_t addr = undofmt::MakeUndoAddr(cur_seg_id_, cur_seg_off_);
    cur_seg_off_ += rec_len;
    return addr;
}

void UndoArea::OpenCurSegLocked() {
    const std::string path = undofmt::UndoSegFileName(dir_, cur_seg_id_);
    // 段首写（off=0）时 O_TRUNC 清掉可能存在的同名历史文件（崩溃前同段号
    // 残留）；续写（off>0，Init 续接场景）保持原内容
    int flags = O_RDWR | O_CREAT;
    if (cur_seg_off_ == 0) flags |= O_TRUNC;
    cur_seg_fd_ = ::open(path.c_str(), flags, S_IRUSR | S_IWUSR);
    if (cur_seg_fd_ < 0) {
        LOG(ERROR) << "[UndoArea] open seg " << path << " failed: " << strerror(errno);
    }
}

void UndoArea::AppendTxnStatusLocked(node_id_t node_id, tx_id_t tid, uint8_t status) {
    char buf[undofmt::TXN_REC_SIZE];
    uint32_t rec_len = undofmt::TXN_REC_SIZE;
    memcpy(buf, &rec_len, 4);
    memcpy(buf + 8, &node_id, 4);
    memcpy(buf + 12, &tid, 8);
    buf[20] = (char)status;
    uint32_t crc = butil::crc32c::Mask(butil::crc32c::Value(buf + 8, sizeof(buf) - 8));
    memcpy(buf + 4, &crc, 4);
    ssize_t n = ::write(txn_table_fd_, buf, sizeof(buf));  // O_APPEND
    if (n != (ssize_t)sizeof(buf)) {
        LOG(ERROR) << "[UndoArea] txn_table append failed, undo area disabled";
        enabled_ = false;
    }
}

void UndoArea::ReleaseChainLocked(const TxnUndoChain& chain) {
    for (uint64_t seg : chain.seg_ids) {
        auto it = seg_refs_.find(seg);
        if (it == seg_refs_.end()) continue;
        if (--it->second == 0) {
            seg_refs_.erase(it);
            if (seg == cur_seg_id_) {
                // 当前写入段也不再被引用：关闭删除并滚动到下一段。
                // 必须滚动而非原位复用——否则新记录地址小于历史地址，
                // 破坏"undo 地址单调 = 日志流序"的全局降序 undo 语义；
                // 且保证盘上不留无归属残留记录，崩溃重建不会误复活
                // 已终结事务（txn_table 压缩的前提）
                if (cur_seg_fd_ >= 0) { ::close(cur_seg_fd_); cur_seg_fd_ = -1; }
                DeleteSegLocked(seg);
                cur_seg_id_++;
                cur_seg_off_ = 0;
            } else {
                DeleteSegLocked(seg);
            }
        }
    }
}

void UndoArea::DeleteSegLocked(uint64_t seg_id) {
    auto it = read_fds_.find(seg_id);
    if (it != read_fds_.end()) { ::close(it->second); read_fds_.erase(it); }
    ::unlink(undofmt::UndoSegFileName(dir_, seg_id).c_str());
}

void UndoArea::MaybeCompactTxnTableLocked() {
    // 无活跃事务 ⇒ 全部状态记录已消费完毕，截断防无限增长
    if (chains_.empty() && txn_table_fd_ >= 0) {
        ::ftruncate(txn_table_fd_, 0);
        ::lseek(txn_table_fd_, 0, SEEK_END);  // O_APPEND 下保险
    }
}

// ---------------- 崩溃恢复 undo ----------------

int UndoArea::UndoAllActiveTxns(
        node_id_t failed_node_id,
        const std::function<int(const char* wal_rec, uint32_t len)>& apply_cb,
        const std::set<node_id_t>& alive_node_ids) {
    if (!enabled_) return -1;
    std::lock_guard<std::mutex> lk(mtx_);

    // P0 修复：存活节点事务跳过 undo（终局由其自身负责）；其链保留，
    // 待其终局到达时正常回收；若该节点随后故障，下一轮 undo（alive
    // 列表不再含它）仍可沿链撤销——因此下方绝不能对全表 clear
    const auto is_alive = [&alive_node_ids](node_id_t node_id) {
        return alive_node_ids.count(node_id) > 0;
    };
    std::vector<TxnKey> dead_keys;
    dead_keys.reserve(chains_.size());

    // 1. 沿各事务链收集全部 undo 记录地址（只读 20B 链头拿 prev_addr）。
    //    undo 区地址随写入单调递增（单写者 replay 线程）⇒ 地址序 = 日志流序
    // P0 修复（虚报成功）：链断裂不再 break 后继续标记 UNDONE——记为
    // 失败，本次不发布任何事务终局（fail-closed）
    bool chains_ok = true;
    std::vector<uint64_t> addrs;
    addrs.reserve(1024);
    for (auto& kv : chains_) {
        if (is_alive(kv.first.node_id)) continue;
        dead_keys.push_back(kv.first);
        uint64_t addr = kv.second.last_addr;
        while (addr != 0) {
            addrs.push_back(addr);
            uint64_t prev = 0;
            if (!ReadPrevAddrLocked(addr, &prev)) {
                LOG(ERROR) << "[UndoArea] walk chain of txn " << kv.second.tid
                           << " broken at " << addr << " — undo FAILED, no txn finalized";
                chains_ok = false;
                break;
            }
            addr = prev;
        }
    }
    if (!chains_ok) {
        return -1;
    }

    // 2. 全局降序：与原实现"按日志流降序撤销"语义一致（同事务/同页补偿
    //    顺序保证；跨事务因 2PL 不共享脏页，顺序无关）
    std::sort(addrs.begin(), addrs.end(), std::greater<uint64_t>());

    // 3. 逐条读完整记录并应用 undo。全部应用完成前不写状态/不删段——
    //    此期间崩溃可由下次恢复完整重来（undo 应用幂等）。
    //    P0 修复（虚报成功）：读取失败或回调 -1（硬失败）同样整体失败，
    //    不标记 UNDONE。
    int undone = 0;
    bool apply_ok = true;
    std::vector<char> wal;
    for (uint64_t addr : addrs) {
        uint32_t wal_len = 0;
        if (!ReadWalRecordLocked(addr, wal, &wal_len)) {
            LOG(ERROR) << "[UndoArea] read undo record failed at " << addr
                       << " — undo FAILED, no txn finalized";
            apply_ok = false;
            break;
        }
        const int r = apply_cb(wal.data(), wal_len);
        if (r < 0) {
            // 硬失败（缺前镜像/文件不可用）：由调用方记录详细日志
            apply_ok = false;
            break;
        }
        if (r > 0) undone++;
    }
    if (!apply_ok) {
        return -1;
    }

    // 4. 逐事务：先持久化 UNDONE 状态再回收空间（崩溃安全顺序，
    //    部分完成时剩余事务下次恢复幂等重 undo）。只作用于本次撤销的
    //    死事务链；存活节点链原样保留
    for (const auto& key : dead_keys) {
        AppendTxnStatusLocked(key.node_id, key.tid, undofmt::TXN_STATUS_UNDONE);
    }
    for (const auto& key : dead_keys) {
        ReleaseChainLocked(chains_.at(key));
    }
    LOG(INFO) << "[UndoArea] undo for failed node " << failed_node_id << ": "
              << undone << " records undone across " << dead_keys.size()
              << " dead-node txns; " << (chains_.size() - dead_keys.size())
              << " alive-node txns retained";
    for (const auto& key : dead_keys) {
        chains_.erase(key);
    }
    MaybeCompactTxnTableLocked();
    return undone;
}

// ---------------- 记录读取 ----------------

int UndoArea::GetReadFdLocked(uint64_t seg_id) {
    auto it = read_fds_.find(seg_id);
    if (it != read_fds_.end()) return it->second;
    int fd = ::open(undofmt::UndoSegFileName(dir_, seg_id).c_str(), O_RDONLY);
    if (fd < 0) return -1;
    read_fds_[seg_id] = fd;
    return fd;
}

bool UndoArea::ReadPrevAddrLocked(uint64_t addr, uint64_t* prev_addr) {
    int fd = GetReadFdLocked(undofmt::UndoAddrSegId(addr));
    if (fd < 0) return false;
    char hdr[undofmt::UNDO_REC_HEADER_SIZE];
    if (::pread(fd, hdr, sizeof(hdr), (off_t)undofmt::UndoAddrSegOff(addr))
        != (ssize_t)sizeof(hdr)) {
        return false;
    }
    uint32_t magic;
    memcpy(&magic, hdr, 4);
    if (magic != undofmt::UNDO_REC_MAGIC) return false;
    memcpy(prev_addr, hdr + 12, 8);
    return true;
}

bool UndoArea::ReadWalRecordLocked(uint64_t addr, std::vector<char>& out, uint32_t* wal_len) {
    int fd = GetReadFdLocked(undofmt::UndoAddrSegId(addr));
    if (fd < 0) return false;
    const uint64_t off = undofmt::UndoAddrSegOff(addr);
    char hdr[undofmt::UNDO_REC_HEADER_SIZE];
    if (::pread(fd, hdr, sizeof(hdr), (off_t)off) != (ssize_t)sizeof(hdr)) return false;
    uint32_t magic, rec_len;
    memcpy(&magic, hdr, 4);
    memcpy(&rec_len, hdr + 4, 4);
    if (magic != undofmt::UNDO_REC_MAGIC || rec_len < undofmt::UNDO_REC_HEADER_SIZE ||
        rec_len > seg_size_) {
        return false;
    }
    out.resize(rec_len);
    memcpy(out.data(), hdr, sizeof(hdr));
    if (::pread(fd, out.data() + sizeof(hdr), rec_len - sizeof(hdr),
                (off_t)(off + sizeof(hdr))) != (ssize_t)(rec_len - sizeof(hdr))) {
        return false;
    }
    uint32_t expect_crc;
    memcpy(&expect_crc, hdr + 8, 4);
    uint32_t actual_crc = butil::crc32c::Mask(
        butil::crc32c::Value(out.data() + 12, rec_len - 12));
    if (expect_crc != actual_crc) return false;
    *wal_len = rec_len - undofmt::UNDO_REC_HEADER_SIZE;
    // out 调整为纯 WAL 记录字节流
    memmove(out.data(), out.data() + undofmt::UNDO_REC_HEADER_SIZE, *wal_len);
    out.resize(*wal_len);
    return true;
}

// ---------------- 测试/观测 ----------------

size_t UndoArea::ActiveTxnCount() {
    std::lock_guard<std::mutex> lk(mtx_);
    return chains_.size();
}

bool UndoArea::HasChain(node_id_t node_id, tx_id_t tid) {
    std::lock_guard<std::mutex> lk(mtx_);
    return chains_.count(TxnKey{node_id, tid}) > 0;
}

uint64_t UndoArea::ChainLastAddr(node_id_t node_id, tx_id_t tid) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = chains_.find(TxnKey{node_id, tid});
    return it == chains_.end() ? 0 : it->second.last_addr;
}

uint64_t UndoArea::CurSegId() {
    std::lock_guard<std::mutex> lk(mtx_);
    return cur_seg_id_;
}

uint64_t UndoArea::TxnTableSize() {
    struct stat st;
    if (::fstat(txn_table_fd_, &st) != 0) return 0;
    return static_cast<uint64_t>(st.st_size);
}
