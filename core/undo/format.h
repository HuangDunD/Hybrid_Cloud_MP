#pragma once
#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include "mvcc/protocol.h"
#include "common.h"

namespace undo {
constexpr uint32_t kFormat = 1;
enum class PageKind : uint32_t { Undo = 1 };
struct PageKey {
    PageKind kind = PageKind::Undo;
    std::string epoch;
    uint32_t writer = 0;
    uint32_t number = 0;
    uint64_t generation = 1;
    auto Key() const { return std::tie(kind, epoch, writer, number, generation); }
    bool operator==(const PageKey& x) const { return Key() == x.Key(); }
    bool operator<(const PageKey& x) const { return Key() < x.Key(); }
};
struct Ref {
    PageKey page;
    uint32_t slot = 0;
    uint32_t generation = 1;
    uint64_t lsn = 0;  // target page's WAL stream is (page.epoch, page.writer)
    bool empty() const { return lsn == 0; }
    bool operator==(const Ref& x) const {
        return page == x.page && slot == x.slot && generation == x.generation && lsn == x.lsn;
    }
};
struct RowId {
    uint32_t table = 0;
    uint32_t page = 0;
    uint32_t slot = 0;
    uint64_t key = 0;
    bool operator==(const RowId& x) const {
        return table == x.table && page == x.page && slot == x.slot && key == x.key;
    }
};
// Explicit, portable logical row format. No DataItem, locks, queues or pointers.
// P3 translates this envelope to/from the fixed MVC2 main-row fields.
struct RowImage {
    RowId id;
    mvcc::Version version;
    Ref previous;
    std::string value;
};
struct Record {
    uint64_t sequence = 0;  // increasing in this transaction's writer subsegment
    Ref previous_txn;
    RowImage old;
};
constexpr size_t kKeyBytes = 40;
constexpr size_t kRefBytes = kKeyBytes + 4 + 4 + 8;
constexpr size_t kRowFixedBytes = 4 + 20 + 2 + 28 + kRefBytes + 4;
constexpr size_t kRecordFixedBytes = 8 + kRefBytes + kRowFixedBytes;
constexpr size_t kPageHeaderBytes = 128;
constexpr size_t kEntryBytes = 32;
constexpr size_t kChecksumBytes = 8;
constexpr size_t kMaxValueBytes =
    PAGE_SIZE - kPageHeaderBytes - kChecksumBytes - kEntryBytes - kRecordFixedBytes;
using PageBytes = std::array<char, PAGE_SIZE>;

void Validate(const PageKey& key);
// Non-fatal structural check used by durable PREPARED validation.
bool ValidRefBytes(std::string_view bytes);
void Validate(const Ref& ref);
void Validate(const mvcc::TxnId& id);
std::string EncodeRef(const Ref& ref);
Ref DecodeRef(std::string_view bytes);
std::string EncodeKey(const PageKey& key);
PageKey DecodeKey(std::string_view bytes);
std::string EncodeTxn(const mvcc::TxnId& id);
mvcc::TxnId DecodeTxn(std::string_view bytes);
std::string EncodeRow(const RowImage& row);
RowImage DecodeRow(std::string_view bytes);
std::string EncodeRecord(const Record& record);
Record DecodeRecord(std::string_view bytes);
std::string SpaceFile(const PageKey& key);
// Stable address in UNDO_TABLE_ID. No slot/page reuse in this format epoch.
page_id_t PageNumber(const PageKey& key);
PageKey PageIdentity(const std::string& epoch, page_id_t number);
uint64_t Checksum(std::string_view bytes);

struct Header {
    PageKey key;
    mvcc::TxnId segment;
    uint64_t init_lsn = 0;
    uint64_t lsn = 0;
    uint64_t seal_lsn = 0;
    uint64_t seal_previous = 0;
    uint32_t used = kPageHeaderBytes;
    uint32_t count = 0;
};
Header Inspect(const PageBytes& bytes);
PageBytes Initialize(const PageKey& key, const mvcc::TxnId& segment, uint64_t lsn);
bool Fits(const PageBytes& page, const Record& record);
// Apply only while holding X permission. Replay uses these same bounded codecs.
void Append(PageBytes& page, uint32_t slot, const Record& record, uint64_t previous, uint64_t lsn);
void Seal(PageBytes& page, uint64_t previous, uint64_t lsn);
Record ReadRecord(const PageBytes& bytes, const Ref& ref);
Record Read(const PageBytes& bytes, const Ref& ref, const RowId& expected);
void CheckAppendReplay(const PageBytes& page, uint32_t slot, const Record& record, uint64_t previous,
                       uint64_t lsn);
}  // namespace undo
