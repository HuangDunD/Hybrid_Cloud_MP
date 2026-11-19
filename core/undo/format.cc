#include "undo/format.h"
#include <algorithm>
#include <limits>
#include <butil/logging.h>

namespace undo {
namespace {
constexpr uint32_t kPageMagic = 0x554e4431;
constexpr uint32_t kEntryComplete = 0x554e444f;
class Encoder {
public:
    std::string bytes;
    void U(uint64_t value, size_t n) {
        for (size_t i = 0; i < n; ++i) bytes.push_back(char(value >> (8 * i)));
    }
    void Raw(std::string_view value) { bytes.append(value.data(), value.size()); }
};
class Decoder {
public:
    explicit Decoder(std::string_view bytes) : bytes_(bytes) {}
    uint64_t U(size_t n) {
        CHECK(n <= 8 && n <= bytes_.size() - pos_);
        uint64_t value = 0;
        for (size_t i = 0; i < n; ++i) value |= uint64_t(uint8_t(bytes_[pos_++])) << (8 * i);
        return value;
    }
    std::string_view Raw(size_t n) {
        CHECK(n <= bytes_.size() - pos_);
        auto r = bytes_.substr(pos_, n);
        pos_ += n;
        return r;
    }
    void End() { CHECK(pos_ == bytes_.size()) << "undo trailing/truncated fields"; }

private:
    std::string_view bytes_;
    size_t pos_ = 0;
};
void PutRef(Encoder& e, const Ref& ref) {
    Validate(ref);
    if (ref.empty()) {
        e.Raw(std::string(kRefBytes, '\0'));
        return;
    }
    e.Raw(EncodeKey(ref.page));
    e.U(ref.slot, 4);
    e.U(ref.generation, 4);
    e.U(ref.lsn, 8);
}
Ref GetRef(Decoder& d) {
    const auto raw = d.Raw(kRefBytes);
    if (raw == std::string(kRefBytes, '\0')) return {};
    Decoder fields(raw);
    Ref r;
    r.page = DecodeKey(fields.Raw(kKeyBytes));
    r.slot = fields.U(4);
    r.generation = fields.U(4);
    r.lsn = fields.U(8);
    fields.End();
    CHECK(!r.empty());
    Validate(r);
    return r;
}
void SetHeader(PageBytes& bytes, const Header& h) {
    Encoder e;
    e.U(kPageMagic, 4);
    e.U(kFormat, 4);
    e.Raw(EncodeKey(h.key));
    e.Raw(EncodeTxn(h.segment));
    e.U(h.init_lsn, 8);
    e.U(h.lsn, 8);
    e.U(h.seal_lsn, 8);
    e.U(h.seal_previous, 8);
    e.U(h.used, 4);
    e.U(h.count, 4);
    CHECK(e.bytes.size() <= kPageHeaderBytes);
    e.bytes.resize(kPageHeaderBytes, '\0');
    std::copy(e.bytes.begin(), e.bytes.end(), bytes.begin());
    Encoder checksum;
    checksum.U(Checksum({bytes.data(), PAGE_SIZE - kChecksumBytes}), 8);
    std::copy(checksum.bytes.begin(), checksum.bytes.end(), bytes.end() - kChecksumBytes);
}
struct Entry {
    uint64_t lsn, previous;
    std::string_view record;
};
Entry EntryAt(const PageBytes& bytes, const Header& h, uint32_t slot) {
    CHECK(slot < h.count);
    size_t pos = kPageHeaderBytes;
    Entry result{};
    uint64_t last_lsn = h.init_lsn;
    for (uint32_t i = 0; i < h.count; ++i) {
        CHECK(pos <= h.used && kEntryBytes <= h.used - pos);
        Decoder d({bytes.data() + pos, kEntryBytes});
        const auto length = d.U(4);
        CHECK(d.U(4) == kEntryComplete) << "incomplete undo record";
        CHECK(d.U(4) == 1) << "unsupported record generation";
        CHECK(d.U(4) == i);
        const auto lsn = d.U(8), previous = d.U(8);
        CHECK(length >= kRecordFixedBytes && length <= h.used - pos - kEntryBytes);
        CHECK(lsn > previous && previous == last_lsn && lsn <= h.lsn);
        last_lsn = lsn;
        if (i == slot) result = {lsn, previous, {bytes.data() + pos + kEntryBytes, size_t(length)}};
        pos += kEntryBytes + length;
    }
    CHECK(h.seal_lsn ? h.seal_previous == last_lsn : h.lsn == last_lsn);
    CHECK(pos == h.used) << "undo used boundary differs from entries";
    return result;
}
}  // namespace
uint64_t Checksum(std::string_view bytes) {
    uint64_t h = 14695981039346656037ULL;
    for (uint8_t b : bytes) {
        h ^= b;
        h *= 1099511628211ULL;
    }
    return h;
}
void Validate(const mvcc::TxnId& id) {
    CHECK(id.epoch.size() == 16 && id.epoch != std::string(16, '\0') && id.local > 0 &&
          id.home < MAX_REMOTE_NODE_NUM);
}
void Validate(const PageKey& k) {
    CHECK(k.kind == PageKind::Undo && k.epoch.size() == 16 && k.epoch != std::string(16, '\0'))
        << "not an undo page";
    CHECK(k.writer < MAX_REMOTE_NODE_NUM && k.number > 0 &&
          k.number < uint32_t(std::numeric_limits<int32_t>::max()));
    CHECK(k.generation == 1) << "P2 never reuses pages; generation must be one";
}
void Validate(const Ref& r) {
    if (r.empty()) {
        CHECK(r == Ref{}) << "partially null undo reference";
        return;
    }
    Validate(r.page);
    CHECK(r.generation == 1 && r.slot < (PAGE_SIZE - kPageHeaderBytes) / (kEntryBytes + kRecordFixedBytes));
}
bool ValidRefBytes(std::string_view bytes) {
    if (bytes.size() != kRefBytes) return false;
    if (bytes == std::string(kRefBytes, '\0')) return true;
    const auto raw = bytes.substr(0, kKeyBytes);
    // kind=Undo, 16 non-NUL epoch bytes, writer<100, number>1, generation=1.
    uint32_t kind, writer, number, reserved;
    uint64_t generation;
    memcpy(&kind, raw.data(), 4);
    std::string epoch(raw.data() + 4, 16);
    memcpy(&writer, raw.data() + 20, 4);
    memcpy(&number, raw.data() + 24, 4);
    memcpy(&generation, raw.data() + 28, 8);
    memcpy(&reserved, raw.data() + 36, 4);
    uint32_t slot, ref_generation;
    uint64_t lsn;
    memcpy(&slot, bytes.data() + kKeyBytes, 4);
    memcpy(&ref_generation, bytes.data() + kKeyBytes + 4, 4);
    memcpy(&lsn, bytes.data() + kKeyBytes + 8, 8);
    return kind == uint32_t(PageKind::Undo) && epoch != std::string(16, '\0') &&
           writer < MAX_REMOTE_NODE_NUM && number > 0 &&
           number < uint32_t(std::numeric_limits<int32_t>::max()) && generation == 1 &&
           reserved == 0 && ref_generation == 1 && lsn > 0 &&
           slot < (PAGE_SIZE - kPageHeaderBytes) / (kEntryBytes + kRecordFixedBytes);
}
std::string EncodeRef(const Ref& ref) { Encoder e; PutRef(e,ref); return e.bytes; }
Ref DecodeRef(std::string_view bytes) { CHECK(bytes.size()==kRefBytes); Decoder d(bytes); auto ref=GetRef(d); d.End(); return ref; }
std::string EncodeKey(const PageKey& k) {
    Validate(k);
    Encoder e;
    e.U(uint32_t(k.kind), 4);
    e.Raw(k.epoch);
    e.U(k.writer, 4);
    e.U(k.number, 4);
    e.U(k.generation, 8);
    e.U(0, 4);
    CHECK(e.bytes.size() == kKeyBytes);
    return e.bytes;
}
PageKey DecodeKey(std::string_view bytes) {
    CHECK(bytes.size() == kKeyBytes);
    Decoder d(bytes);
    PageKey k;
    k.kind = PageKind(d.U(4));
    k.epoch = std::string(d.Raw(16));
    k.writer = d.U(4);
    k.number = d.U(4);
    k.generation = d.U(8);
    CHECK(d.U(4) == 0);
    d.End();
    Validate(k);
    return k;
}
std::string EncodeTxn(const mvcc::TxnId& id) {
    Validate(id);
    Encoder e;
    e.Raw(id.epoch);
    e.U(id.home, 4);
    e.U(id.local, 8);
    return e.bytes;
}
mvcc::TxnId DecodeTxn(std::string_view bytes) {
    CHECK(bytes.size() == 28);
    Decoder d(bytes);
    mvcc::TxnId id;
    id.epoch = std::string(d.Raw(16));
    id.home = d.U(4);
    id.local = d.U(8);
    d.End();
    Validate(id);
    return id;
}
std::string EncodeRow(const RowImage& r) {
    CHECK(r.id.table < 10000 && r.id.page > 0 && r.id.page < uint32_t(std::numeric_limits<int32_t>::max()) &&
          r.id.slot < PAGE_SIZE && r.value.size() <= kMaxValueBytes);
    Encoder e;
    e.U(kFormat, 4);
    e.U(r.id.table, 4);
    e.U(r.id.page, 4);
    e.U(r.id.slot, 4);
    e.U(r.id.key, 8);
    e.U(r.version.initial, 1);
    e.U(r.version.deleted, 1);
    if (r.version.initial) {
        CHECK(r.version.creator == mvcc::TxnId{});
        e.Raw(std::string(28, '\0'));
    } else
        e.Raw(EncodeTxn(r.version.creator));
    PutRef(e, r.previous);
    e.U(r.value.size(), 4);
    e.Raw(r.value);
    CHECK(e.bytes.size() == kRowFixedBytes + r.value.size());
    return e.bytes;
}
RowImage DecodeRow(std::string_view bytes) {
    CHECK(bytes.size() >= kRowFixedBytes);
    Decoder d(bytes);
    RowImage r;
    CHECK(d.U(4) == kFormat);
    r.id.table = d.U(4);
    r.id.page = d.U(4);
    r.id.slot = d.U(4);
    r.id.key = d.U(8);
    auto initial = d.U(1), deleted = d.U(1);
    CHECK(initial <= 1 && deleted <= 1);
    r.version.initial = initial;
    r.version.deleted = deleted;
    auto creator = d.Raw(28);
    if (initial)
        CHECK(creator == std::string(28, '\0'));
    else
        r.version.creator = DecodeTxn(creator);
    r.previous = GetRef(d);
    auto size = d.U(4);
    CHECK(size <= kMaxValueBytes);
    r.value = std::string(d.Raw(size));
    d.End();
    const auto encoded = EncodeRow(r);
    CHECK(encoded == bytes);
    return r;
}
std::string EncodeRecord(const Record& r) {
    CHECK(r.sequence > 0 && ((r.sequence == 1) == r.previous_txn.empty()));
    Encoder e;
    e.U(r.sequence, 8);
    PutRef(e, r.previous_txn);
    e.Raw(EncodeRow(r.old));
    return e.bytes;
}
Record DecodeRecord(std::string_view bytes) {
    CHECK(bytes.size() >= kRecordFixedBytes);
    Decoder d(bytes);
    Record r;
    r.sequence = d.U(8);
    r.previous_txn = GetRef(d);
    r.old = DecodeRow(d.Raw(bytes.size() - 8 - kRefBytes));
    d.End();
    CHECK(r.sequence > 0 && ((r.sequence == 1) == r.previous_txn.empty()));
    return r;
}
page_id_t PageNumber(const PageKey& key) {
    Validate(key);
    CHECK(key.generation == 1) << "undo page reuse is not implemented";
    const uint64_t number = (uint64_t(key.number) - 1) * MAX_REMOTE_NODE_NUM + key.writer + 1;
    CHECK(number <= uint64_t(std::numeric_limits<page_id_t>::max()));
    return static_cast<page_id_t>(number);
}
PageKey PageIdentity(const std::string& epoch, page_id_t number) {
    CHECK(number > 0);
    PageKey key{PageKind::Undo, epoch, uint32_t(number - 1) % MAX_REMOTE_NODE_NUM,
                uint32_t(number - 1) / MAX_REMOTE_NODE_NUM + 1, 1};
    Validate(key);
    return key;
}
std::string SpaceFile(const PageKey& k) {
    Validate(k);
    const char* digits = "0123456789abcdef";
    std::string hex;
    for (uint8_t byte : k.epoch) {
        hex += digits[byte >> 4];
        hex += digits[byte & 15];
    }
    return "UNDO_" + hex + "_" + std::to_string(k.writer) + ".pages";
}
Header Inspect(const PageBytes& bytes) {
    Decoder checksum({bytes.data() + PAGE_SIZE - kChecksumBytes, kChecksumBytes});
    CHECK(checksum.U(8) == Checksum({bytes.data(), PAGE_SIZE - kChecksumBytes}))
        << "undo page checksum mismatch";
    Decoder d({bytes.data(), kPageHeaderBytes});
    Header h;
    CHECK(d.U(4) == kPageMagic && d.U(4) == kFormat);
    h.key = DecodeKey(d.Raw(kKeyBytes));
    h.segment = DecodeTxn(d.Raw(28));
    CHECK(h.key.epoch == h.segment.epoch);
    h.init_lsn = d.U(8);
    h.lsn = d.U(8);
    h.seal_lsn = d.U(8);
    h.seal_previous = d.U(8);
    h.used = d.U(4);
    h.count = d.U(4);
    CHECK(h.init_lsn > 0 && h.lsn >= h.init_lsn && h.used >= kPageHeaderBytes &&
          h.used <= PAGE_SIZE - kChecksumBytes);
    CHECK(h.count <= (PAGE_SIZE - kPageHeaderBytes) / (kEntryBytes + kRecordFixedBytes));
    CHECK(h.seal_lsn ? (h.seal_lsn == h.lsn && h.seal_previous >= h.init_lsn && h.seal_previous < h.seal_lsn)
                     : h.seal_previous == 0);
    if (h.count)
        EntryAt(bytes, h, 0);
    else
        CHECK(h.used == kPageHeaderBytes);
    return h;
}
PageBytes Initialize(const PageKey& k, const mvcc::TxnId& segment, uint64_t lsn) {
    Validate(k);
    Validate(segment);
    CHECK(k.epoch == segment.epoch && lsn > 0);
    PageBytes bytes{};
    Header h{k, segment, lsn, lsn, 0, 0, uint32_t(kPageHeaderBytes), 0};
    SetHeader(bytes, h);
    return bytes;
}
bool Fits(const PageBytes& bytes, const Record& record) {
    auto h = Inspect(bytes);
    auto encoded = EncodeRecord(record);
    return !h.seal_lsn && encoded.size() + kEntryBytes <= PAGE_SIZE - kChecksumBytes - h.used;
}
void Append(PageBytes& bytes, uint32_t slot, const Record& record, uint64_t previous, uint64_t lsn) {
    auto h = Inspect(bytes);
    auto encoded = EncodeRecord(record);
    CHECK(!h.seal_lsn && slot == h.count && h.lsn == previous && lsn > previous);
    CHECK(encoded.size() + kEntryBytes <= PAGE_SIZE - kChecksumBytes - h.used);
    CHECK(record.old.version.initial || record.old.version.creator.epoch == h.key.epoch);
    CHECK(record.old.previous.empty() || record.old.previous.page.epoch == h.key.epoch);
    if (h.count) {
        const auto previous_entry = EntryAt(bytes, h, h.count - 1);
        const Ref expected{h.key, h.count - 1, 1, previous_entry.lsn};
        CHECK(record.previous_txn == expected &&
              record.sequence == DecodeRecord(previous_entry.record).sequence + 1);
    }
    if (!record.previous_txn.empty())
        CHECK(record.previous_txn.page.epoch == h.key.epoch &&
              record.previous_txn.page.writer == h.key.writer && record.previous_txn.lsn < lsn);
    Encoder entry;
    entry.U(encoded.size(), 4);
    entry.U(kEntryComplete, 4);
    entry.U(1, 4);
    entry.U(slot, 4);
    entry.U(lsn, 8);
    entry.U(previous, 8);
    entry.Raw(encoded);
    std::copy(entry.bytes.begin(), entry.bytes.end(), bytes.begin() + h.used);
    h.used += entry.bytes.size();
    ++h.count;
    h.lsn = lsn;
    SetHeader(bytes, h);
}
void Seal(PageBytes& bytes, uint64_t previous, uint64_t lsn) {
    auto h = Inspect(bytes);
    CHECK(!h.seal_lsn && h.lsn == previous && lsn > previous);
    h.seal_previous = previous;
    h.seal_lsn = lsn;
    h.lsn = lsn;
    SetHeader(bytes, h);
}
Record ReadRecord(const PageBytes& bytes, const Ref& ref) {
    Validate(ref);
    CHECK(!ref.empty());
    auto h = Inspect(bytes);
    CHECK(h.key == ref.page && h.lsn >= ref.lsn);
    auto entry = EntryAt(bytes, h, ref.slot);
    CHECK(entry.lsn == ref.lsn);
    return DecodeRecord(entry.record);
}
Record Read(const PageBytes& bytes, const Ref& ref, const RowId& expected) {
    auto record = ReadRecord(bytes, ref);
    CHECK(record.old.id == expected) << "undo RID/key mismatch";
    return record;
}
void CheckAppendReplay(const PageBytes& bytes, uint32_t slot, const Record& r, uint64_t previous,
                       uint64_t lsn) {
    auto h = Inspect(bytes);
    auto entry = EntryAt(bytes, h, slot);
    const auto encoded = EncodeRecord(r);
    CHECK(entry.lsn == lsn && entry.previous == previous && entry.record == encoded)
        << "contradictory duplicate undo redo";
}
}  // namespace undo
