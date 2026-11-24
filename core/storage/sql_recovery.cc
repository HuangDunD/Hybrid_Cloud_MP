#include "storage/sql_recovery.h"
#include "mvcc/row.h"
#include <algorithm>
#include <fcntl.h>
#include <fstream>
#include <filesystem>
#include <unistd.h>

void SqlRecovery::ObserveLock(uint64_t timestamp, uint32_t home) {
    const auto id = timestamp >> 8;
    CHECK(id && (timestamp & 255) == home);
    auto& writer = writers_[home];
    writer.holders.insert(id);
    writer.max_id = std::max(writer.max_id, id);
}

void SqlRecovery::Observe(const LogRecord& log) {
    CHECK(log.log_node_id_ >= 0);
    const auto home = uint32_t(log.log_node_id_);
    auto& writer = writers_[home];
    writer.max_lsn = std::max(writer.max_lsn, uint64_t(log.lsn_));
    const uint64_t tid = log.log_tid_;
    if (tid && tid != uint64_t(INVALID_TXN_ID)) writer.max_id = std::max(writer.max_id, tid);
    const Id id{home, tid};
    if (log.log_type_ == LogType::PREPARED)
        LOG(FATAL) << "prepared transaction in single-writer SQL database";
    if (log.log_type_ == LogType::UNDO_PAGE) {
        const auto& redo = static_cast<const undo::Redo&>(log);
        if (epoch_.empty()) epoch_ = redo.segment.epoch;
        CHECK(redo.segment == (mvcc::TxnId{epoch_, home, tid}) && !ended_.count(id));
        writer.max_page = std::max(writer.max_page, redo.page.number);
        auto& changes = changes_[id];
        if (redo.operation == undo::Operation::Initialize)
            CHECK(changes.pages.insert(redo.page.number).second);
        else if (redo.operation == undo::Operation::Seal)
            CHECK(changes.sealed.insert(redo.page.number).second);
    } else if (log.log_type_ == LogType::LOCK) {
        const auto& lock = static_cast<const LockLogRecord&>(log);
        if (lock.lock_type_ == EXCLUSIVE_LOCKED) ObserveLock(lock.timeStamp_, lock.txn_home_node_);
    } else if (log.log_type_ == LogType::INSERT) {
        const auto& insert = static_cast<const InsertLogRecord&>(log);
        CHECK(insert.insert_value_.value_size_ >= sizeof(DataItem));
        const auto& item = *reinterpret_cast<const DataItem*>(insert.insert_value_.value_);
        if (item.lock == EXCLUSIVE_LOCKED) ObserveLock(item.timeStamp, item.txn_home_node);
    } else if (log.log_type_ == LogType::UPDATE) {
        // Only logical version installs/rollbacks before this writer's end
        // participate. Later deferred physical lock cleanup is not a decision.
        const auto it = changes_.find(id);
        if (it == changes_.end() || ended_.count(id)) return;
        const auto& update = static_cast<const UpdateLogRecord&>(log);
        CHECK(update.new_value_.value_size_ >= sizeof(DataItem));
        const auto& item = *reinterpret_cast<const DataItem*>(update.new_value_.value_);
        const Slot slot{item.table_id, update.rid_.page_no_, update.rid_.slot_no_};
        const bool owns = !item.mvcc.initial && item.mvcc.creator_local == tid &&
                          item.mvcc.creator_home == home;
        if (owns) {
            CHECK(std::string(item.mvcc.creator_epoch.data(), 16) == epoch_);
            it->second.rows[slot] = true;
        } else if (it->second.rows.count(slot)) it->second.rows[slot] = false;
    } else if (log.log_type_ == LogType::MVCC_COMMIT || log.log_type_ == LogType::COMMIT || log.log_type_ == LogType::ABORT ||
               log.log_type_ == LogType::BATCHEND) {
        CHECK(tid && tid < (UINT64_MAX >> 8) && ended_.insert(id).second);
        const auto it = changes_.find(id);
        const bool stamped = log.log_type_ == LogType::MVCC_COMMIT;
        CHECK(!stamped || it != changes_.end()) << "commit timestamp without a writer version";
        if (it == changes_.end()) return;  // locking without a version install
        const auto& c = it->second;
        const size_t owned = std::count_if(c.rows.begin(), c.rows.end(),
                                            [](const auto& row) { return row.second; });
        CHECK(c.sealed == c.pages && (owned == 0 || owned == c.rows.size()));
        CHECK(log.log_type_ != LogType::ABORT || owned == 0);
        CHECK((!stamped && log.log_type_ != LogType::COMMIT) || owned == c.rows.size());
        CHECK(!stamped || owned > 0);
        if (owned) {
            const auto ts = stamped ? static_cast<const MvccCommitLogRecord&>(log).commit_ts : 0;
            CHECK(writer.committed.emplace(tid, ts).second);
            if (stamped) max_commit_ts_ = std::max(max_commit_ts_, ts);
            else ++legacy_count_;
        }
        // Incomplete transactions remain in changes_ for crash undo.
        changes_.erase(it);
    }
}

void SqlRecovery::BeginBoot(const storage_service::OpendbRequest& req, unsigned members) {
    CHECK(boot_.empty() && req.cluster_boot().size() == 16 && members > 0 && members <= 256);
    members_ = members;
    for (const auto& [home, writer] : writers_)
        CHECK(home < members && writer.max_id < (UINT64_MAX >> 8) && writer.max_lsn < UINT64_MAX);
    // Install a boot fence before admitting any SQL process. A storage-only
    // reopen has no cluster_boot and never rolls back a live transaction.
    const std::string name = "sql_mvcc.boot";
    storage_service::SqlBoot prior;
    if (std::filesystem::exists(name)) {
        std::ifstream in(name, std::ios::binary);
        CHECK(in && prior.ParseFromIstream(&in) && prior.database_epoch().size() == 16 &&
              prior.cluster_boot().size() == 16 && prior.members() == members);
        CHECK(prior.cluster_boot() != req.cluster_boot()) << "SQL compute recovery requires a full cluster restart";
        CHECK(epoch_.empty() || epoch_ == prior.database_epoch()) << "database and WAL epoch differ";
        epoch_ = prior.database_epoch();
    }
    if (epoch_.empty()) epoch_ = req.configured_epoch().empty() ? req.cluster_boot() : req.configured_epoch();
    undo::Validate(mvcc::TxnId{epoch_, 0, 1});
    CHECK(req.configured_epoch().empty() || req.configured_epoch() == epoch_);
    boot_ = req.cluster_boot();
    storage_service::SqlBoot boot;
    boot.set_cluster_boot(boot_);
    boot.set_database_epoch(epoch_);
    boot.set_members(members);
    const auto tmp = name + ".tmp";
    const int fd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    CHECK(fd >= 0 && boot.SerializeToFileDescriptor(fd) && fsync(fd) == 0 && close(fd) == 0);
    CHECK(rename(tmp.c_str(), name.c_str()) == 0);
    const int dir = open(".", O_DIRECTORY | O_RDONLY);
    CHECK(dir >= 0 && fsync(dir) == 0 && close(dir) == 0);
}

bool SqlRecovery::Join(const storage_service::OpendbRequest& req, storage_service::SqlRecovery* out) {
    if (req.cluster_boot() != boot_ || req.node_id() < 0 || unsigned(req.node_id()) >= members_ ||
        !req.startup_id() || (!req.configured_epoch().empty() && req.configured_epoch() != epoch_))
        return false;
    const auto joined = joined_.emplace(req.node_id(), req.startup_id());
    if (joined.first->second != req.startup_id()) return false;
    if (joined.second && joined_.size() == 1) {
        // The cold heap scan has finished. Subsequent joins only need the
        // committed IDs and the writer allocation high-water marks.
        changes_.clear();
        ended_.clear();
        for (auto& entry : writers_) entry.second.holders.clear();
    }
    out->set_epoch(epoch_);
    out->set_max_commit_ts(std::max(legacy_count_, max_commit_ts_));
    const auto& local = writers_[req.node_id()];
    out->set_local_id(local.max_id);
    out->set_undo_pages(local.max_page);
    out->set_writer_lsn(local.max_lsn);
    for (const auto& [home, writer] : writers_) {
        if (writer.committed.empty()) continue;
        auto* list = out->add_writers();
        list->set_home(home);
        storage_service::SqlRecovery::Writer::Range* run = nullptr;
        for (const auto& [tid, ts] : writer.committed) {
            const bool consecutive = run && tid == run->last() + 1 &&
                (ts ? run->first_commit_ts() && ts == run->first_commit_ts() + (tid - run->first())
                     : !run->first_commit_ts());
            if (!consecutive) {
                run = list->add_committed();
                run->set_first(tid);
                run->set_first_commit_ts(ts);
            }
            run->set_last(tid);
        }
    }
    return true;
}

bool SqlRecovery::RecoverRow(DataItem& item, const undo::RowId& id, undo::Store& store) const {
    CHECK(item.table_id == int(id.table) && item.mvcc.format == mvcc::RowFields{}.format &&
          item.mvcc.reserved == 0 && item.mvcc.initial <= 1 && item.valid <= 1);
    bool dirty = false;
    std::set<std::pair<undo::PageKey, uint32_t>> visited;
    while (!item.mvcc.initial) {
        const mvcc::TxnId creator{std::string(item.mvcc.creator_epoch.data(), 16),
                                  item.mvcc.creator_home, item.mvcc.creator_local};
        CHECK(creator.epoch == epoch_ && creator.home < members_ && creator.local);
        auto ref = undo::DecodeRef({item.mvcc.history.data(), item.mvcc.history.size()});
        CHECK(!ref.empty() && ref.page.epoch == epoch_ && ref.page.writer == creator.home);
        const std::string_view value{reinterpret_cast<const char*>(&item) + sizeof(item), size_t(item.value_size)};
        CHECK(store.Ready(ref, id, creator, value)) << "missing SQL version undo";
        if (writers_.at(creator.home).committed.count(creator.local)) break;
        const Id owner{creator.home, creator.local};
        CHECK(changes_.count(owner) && !ended_.count(owner)) << "unproven SQL version";
        CHECK(visited.emplace(ref.page, ref.slot).second);
        const undo::RowImage row{id, {creator, false, !item.valid}, ref, std::string(value)};
        const auto bytes = store.ReadPage(ref.page, ref.lsn);
        mvcc::InstallRow(item, undo::Read(bytes, ref, id).RestorePrevious(row, ref));
        dirty = true;
    }
    if (item.mvcc.initial) {
        CHECK(item.mvcc.creator_epoch == (std::array<char, 16>{}) &&
              !item.mvcc.creator_home && !item.mvcc.creator_local &&
              undo::DecodeRef({item.mvcc.history.data(), item.mvcc.history.size()}).empty());
    }
    if (item.lock == EXCLUSIVE_LOCKED) {
        const auto owner = writers_.find(item.txn_home_node);
        CHECK(owner != writers_.end() && (item.timeStamp & 255) == item.txn_home_node &&
              owner->second.holders.count(item.timeStamp >> 8)) << "unproven recovered lock";
        item.lock = UNLOCKED;
        dirty = true;
    }
    return dirty;
}
