#include "undo/manager.h"
#include "dtx/benchmark_stats.h"
#include <butil/logging.h>
#include <limits>
namespace undo {
namespace {
struct OpTimer {
    uint64_t start;
    std::atomic<uint64_t>& ns;
    std::atomic<uint64_t>& count;
    ~OpTimer() {
        ns.fetch_add(benchmark_stats::NowNs() - start, std::memory_order_relaxed);
        count.fetch_add(1, std::memory_order_relaxed);
    }
};
}  // namespace
Manager::Manager(std::string epoch, uint32_t node, Wal wal, PageAccess& access,
                 uint64_t max_pages, uint32_t recovered_pages)
    : epoch_(std::move(epoch)), node_(node),
      addressable_pages_((uint64_t(std::numeric_limits<page_id_t>::max()) - node - 1) /
                         MAX_REMOTE_NODE_NUM + 1),
      wal_(std::move(wal)), access_(access),
      next_page_(recovered_pages), max_pages_(max_pages) {
    CHECK(recovered_pages <= addressable_pages_);
    Validate(mvcc::TxnId{epoch_, node_, 1});
    CHECK(wal_.enqueue);
}
bool Manager::AdmitNewWrite(const mvcc::TxnId& txn, std::optional<WriteSlot> slot) {
    auto segment = Get(txn);
    std::lock_guard<bthread::Mutex> lock(segment->mutex);
    CHECK(!segment->finished);
    if (slot && segment->write_slots.count(*slot)) return true;
    const bool first = !segment->page && !segment->admitted;
    if (!slot && !first) return true;
    std::lock_guard<std::mutex> capacity(capacity_mutex_);
    const auto budget = max_pages_.load();
    if ((first && budget && next_page_.load() + admitted_without_page_.load() >= budget) ||
        next_page_.load() + reserved_pages_ >= addressable_pages_) {
        ++capacity_rejects_;
        return false;
    }
    if (slot) segment->write_slots.insert(*slot);
    if (first) {
        segment->admitted = true;
        ++admitted_without_page_;
    }
    ++segment->page_credits;
    ++reserved_pages_;
    return true;
}
void Manager::Begin(const mvcc::Context& context) {
    Validate(context.id);
    CHECK(context.id.epoch == epoch_);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = segments_.find(context.id);
    if (it != segments_.end()) {
        CHECK(it->second->context.snapshot == context.snapshot);
        return;
    }
    segments_.emplace(context.id, std::make_shared<Segment>(context));
}
std::shared_ptr<Manager::Segment> Manager::Get(const mvcc::TxnId& txn) {
    Validate(txn);
    CHECK(txn.epoch == epoch_);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = segments_.find(txn);
    CHECK(it != segments_.end()) << "undo Begin required";
    return it->second;
}
uint64_t Manager::Apply(Redo redo, PageBytes& bytes, Page* frame, bool exists) {
    CHECK(redo.page.writer == node_ && redo.page.epoch == epoch_);
    CHECK(frame->get_page_id().table_id == UNDO_TABLE_ID &&
          frame->get_page_id().page_no == PageNumber(redo.page));
    const uint64_t lsn = wal_.enqueue(std::make_unique<Redo>(redo));
    CHECK(lsn > redo.prev_lsn_);
    redo.lsn_ = lsn;
    redo.Apply(bytes, exists);
    memcpy(frame->get_data(), bytes.data(), PAGE_SIZE);
    frame->set_dirty(true);  // normal Lazy eviction/push waits for this page's WAL
    return lsn;
}
Ref Manager::Append(const mvcc::TxnId& txn, const RowImage& old,
                    std::optional<std::string_view> successor) {
    OpTimer timer{benchmark_stats::NowNs(), append_ns_, append_count_};
    auto segment = Get(txn);
    std::lock_guard<bthread::Mutex> lock(segment->mutex);
    CHECK(!segment->finished);
    const auto encoded = EncodeRow(old);
    (void)encoded;
    CHECK(old.version.initial || old.version.creator.epoch == epoch_);
    CHECK(old.previous.empty() || old.previous.page.epoch == epoch_);
    Record record{segment->sequence + 1, segment->tail, old};
    if (successor) record.CompactFor(*successor);
    for (;;) {
        const bool create = !segment->page;
        if (create) {
            std::lock_guard<std::mutex> capacity(capacity_mutex_);
            if (segment->page_credits) {
                --segment->page_credits;
                --reserved_pages_;
            } else if (next_page_.load() + reserved_pages_ >= addressable_pages_) {
                ++capacity_rejects_;
                return {};  // no new main-row reference or transaction tail was installed
            }
            if (segment->admitted) {
                segment->admitted = false;
                --admitted_without_page_;
            }
            const auto number = ++next_page_;
            CHECK(number > 0 && number <= UINT32_MAX);
            segment->page = PageKey{PageKind::Undo, epoch_, node_, uint32_t(number), 1};
            (void)PageNumber(*segment->page);  // enforce the shared page-ID boundary
        }
        const auto key = *segment->page;
        Page* frame = access_.FetchUndoPage(key, true, create);
        PageBytes bytes{};
        if (create)
            Apply(Redo(Operation::Initialize, key, txn, 0), bytes, frame, false);
        else
            memcpy(bytes.data(), frame->get_data(), PAGE_SIZE);
        auto h = Inspect(bytes);
        CHECK(h.key == key && h.segment == txn && !h.seal_lsn);
        if (!Fits(bytes, record)) {
            Apply(Redo(Operation::Seal, key, txn, h.lsn), bytes, frame, true);
            access_.ReleaseUndoPage(key, true);
            segment->page.reset();
            continue;
        }
        const auto lsn = Apply(Redo(Operation::Append, key, txn, h.lsn, h.count, record), bytes, frame, true);
        access_.ReleaseUndoPage(key, true);
        segment->sequence = record.sequence;
        segment->tail = {key, h.count, 1, lsn};
        return segment->tail;
    }
}
void Manager::Finish(const mvcc::TxnId& txn) {
    OpTimer timer{benchmark_stats::NowNs(), seal_ns_, seal_count_};
    auto segment = Get(txn);
    std::lock_guard<bthread::Mutex> lock(segment->mutex);
    if (segment->finished) return;
    if (segment->page_credits || segment->admitted) {
        std::lock_guard<std::mutex> capacity(capacity_mutex_);
        CHECK(reserved_pages_ >= segment->page_credits);
        reserved_pages_ -= segment->page_credits;
        segment->page_credits = 0;
        if (segment->admitted) {
            segment->admitted = false;
            --admitted_without_page_;
        }
    }
    segment->write_slots.clear();
    if (segment->page) {
        const auto key = *segment->page;
        Page* frame = access_.FetchUndoPage(key, true);
        PageBytes bytes;
        memcpy(bytes.data(), frame->get_data(), PAGE_SIZE);
        const auto h = Inspect(bytes);
        CHECK(h.key == key && h.segment == txn);
        Apply(Redo(Operation::Seal, key, txn, h.lsn), bytes, frame, true);
        access_.ReleaseUndoPage(key, true);
    }
    segment->finished = true;
}
PageBytes Manager::LoadRef(const Ref& ref) {
    Validate(ref);
    CHECK(!ref.empty() && ref.page.epoch == epoch_);
    Page* frame = access_.FetchUndoPage(ref.page, false);
    PageBytes bytes;
    memcpy(bytes.data(), frame->get_data(), PAGE_SIZE);
    const auto header = Inspect(bytes);
    CHECK(header.key == ref.page && header.lsn >= ref.lsn);
    access_.ReleaseUndoPage(ref.page, false);
    return bytes;
}
Record Manager::Read(const Ref& ref, const RowId& expected) {
    OpTimer timer{benchmark_stats::NowNs(), read_ns_, read_count_};
    return undo::Read(LoadRef(ref), ref, expected);
}
Record Manager::ReadRollback(const Ref& ref, const mvcc::TxnId& txn, uint64_t sequence) {
    OpTimer timer{benchmark_stats::NowNs(), read_ns_, read_count_};
    CHECK(ref.page.writer == node_);
    auto bytes = LoadRef(ref);
    CHECK(Inspect(bytes).segment == txn);
    auto record = ReadRecord(bytes, ref);
    CHECK(record.sequence == sequence);
    return record;
}
void Manager::ClearCache() {
    access_.ClearUndoCache();
}
Ref Manager::Tail(const mvcc::TxnId& txn) {
    auto segment = Get(txn);
    std::lock_guard<bthread::Mutex> lock(segment->mutex);
    return segment->tail;
}
}  // namespace undo
