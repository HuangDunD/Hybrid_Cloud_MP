#pragma once
#include <functional>
#include <memory>
#include <map>
#include <mutex>
#include <atomic>
#include <optional>
#include <set>
#include <tuple>
#include <bthread/mutex.h>
#include "base/page.h"
#include "undo/redo.h"

/*
    这个主要是管理 undo 页面的分配和获取的
    每个节点各自管理间隔 100 的页面
    比如节点 0:0-100 ， 300-400 ， 600-700
    节点1:101-200 。。
    这样处理之后，多个节点就不会被分到相同的号了，减少节点之间通信的开销
*/

namespace undo {
struct Wal {
    std::function<uint64_t(std::unique_ptr<LogRecord>)> enqueue;
};
class PageAccess {
public:
    virtual ~PageAccess() = default;
    // Implemented by ComputeServer with rpc_lazy_fetch/release_*_page.
    // The returned frame remains protected until ReleaseUndoPage.
    virtual Page* FetchUndoPage(const PageKey&, bool exclusive, bool create = false) = 0;
    virtual void ReleaseUndoPage(const PageKey&, bool exclusive) = 0;
    virtual void ClearUndoCache() = 0;
};
// One instance per compute node/epoch; its buffer is shared by all subsegments.
// It stores history, not transaction visibility or main-row write permissions.
class Manager {
public:
    Manager(std::string epoch, uint32_t node, Wal wal, PageAccess& access,
            uint64_t max_pages = 0, uint32_t recovered_pages = 0);
    void Begin(const mvcc::Context& context);  // logical only, including read-only transactions
    Ref Append(const mvcc::TxnId& txn, const RowImage& old,
               std::optional<std::string_view> successor = std::nullopt);
    void Finish(const mvcc::TxnId& txn);  // seals the local subsegment; does not commit/abort
    Record Read(const Ref& ref, const RowId& expected);
    Record ReadRollback(const Ref& ref, const mvcc::TxnId& txn, uint64_t sequence);
    void ClearCache();
    Ref Tail(const mvcc::TxnId& txn);
    uint64_t AllocatedPages() const { return next_page_.load(); }
    // Admission is checked before a transaction's first write. Existing
    // segments with an undo page may keep appending (in-flight resources are
    // preserved); a new segment is refused once the configured page budget is
    // exhausted. It never overwrites or reuses an old page.
    using WriteSlot = std::tuple<table_id_t, page_id_t, int>;
    // A 2PC writer reserves one possible page for each distinct row before
    // Prepare. Lazy may still abort normally if a later append needs another page.
    bool AdmitNewWrite(const mvcc::TxnId& txn, std::optional<WriteSlot> slot = std::nullopt);
    void SetCapacityForTest(uint64_t pages) { max_pages_ = pages; }
    uint64_t CapacityRejects() const { return capacity_rejects_.load(); }
    uint64_t AdmittedReservations() const { return admitted_without_page_.load(); }
    uint64_t AppendCount() const { return append_count_.load(); }
    uint64_t AppendNs() const { return append_ns_.load(); }
    uint64_t ReadCount() const { return read_count_.load(); }
    uint64_t ReadNs() const { return read_ns_.load(); }
    uint64_t SealCount() const { return seal_count_.load(); }
    uint64_t SealNs() const { return seal_ns_.load(); }
    void ResetBenchmarkStats() {
        capacity_rejects_ = 0;
        append_count_ = 0;
        append_ns_ = 0;
        read_count_ = 0;
        read_ns_ = 0;
        seal_count_ = 0;
        seal_ns_ = 0;
    }

private:
    friend struct ManagerAddressLimitTest;
    struct Segment {
        explicit Segment(mvcc::Context c) : context(std::move(c)) {}
        bthread::Mutex mutex;
        const mvcc::Context context;
        std::optional<PageKey> page;
        uint64_t sequence = 0;
        Ref tail;
        bool finished = false;
        bool admitted = false;
        uint64_t page_credits = 0;
        std::set<WriteSlot> write_slots;
    };
    std::shared_ptr<Segment> Get(const mvcc::TxnId& txn);
    PageBytes LoadRef(const Ref& ref);
    uint64_t Apply(Redo redo, PageBytes& bytes, Page* frame, bool exists);
    const std::string epoch_;
    const uint32_t node_;
    const uint64_t addressable_pages_;
    Wal wal_;
    PageAccess& access_;
    std::atomic<uint64_t> next_page_{0};
    std::atomic<uint64_t> max_pages_{0};
    std::atomic<uint64_t> admitted_without_page_{0};
    std::atomic<uint64_t> capacity_rejects_{0};
    std::atomic<uint64_t> append_count_{0}, append_ns_{0};
    std::atomic<uint64_t> read_count_{0}, read_ns_{0};
    std::atomic<uint64_t> seal_count_{0}, seal_ns_{0};
    std::mutex capacity_mutex_;
    uint64_t reserved_pages_ = 0;  // protected by capacity_mutex_
    std::mutex mutex_;
    std::map<mvcc::TxnId, std::shared_ptr<Segment>> segments_;
};
}  // namespace undo
