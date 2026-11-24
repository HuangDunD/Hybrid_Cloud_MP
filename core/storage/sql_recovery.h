#pragma once

#include "storage/storage_service.pb.h"
#include "undo/store.h"

struct DataItem;

// Built once from the durable SQL WAL, before the first post-restart snapshot.
// SQL has one physical writer per transaction. Home and local id identify its
// stream; the undo record binds that stream to the database epoch.
class SqlRecovery {
public:
    void Observe(const LogRecord& record);
    void BeginBoot(const storage_service::OpendbRequest& request, unsigned members);
    bool Join(const storage_service::OpendbRequest& request, storage_service::SqlRecovery* out);
    bool RecoverRow(DataItem& item, const undo::RowId& id, undo::Store& undo) const;

private:
    using Id = std::pair<uint32_t, uint64_t>;
    using Slot = std::tuple<uint32_t, uint32_t, uint32_t>;
    struct Writer {
        uint64_t max_id = 0, max_lsn = 0;
        uint32_t max_page = 0;
        std::set<uint64_t> holders;
        std::map<uint64_t, uint64_t> committed; // local id -> commit timestamp (0 for legacy WAL)
    };
    struct Changes {
        std::map<Slot, bool> rows;  // still owned at the end of a pre-upgrade transaction
        std::set<uint32_t> pages, sealed;
    };
    void ObserveLock(uint64_t timestamp, uint32_t home);
    std::map<uint32_t, Writer> writers_;
    std::map<Id, Changes> changes_;
    std::set<Id> ended_;
    std::map<uint32_t, uint64_t> joined_;
    std::string epoch_, boot_;
    uint64_t legacy_count_ = 0, max_commit_ts_ = 0;
    unsigned members_ = 0;
};
