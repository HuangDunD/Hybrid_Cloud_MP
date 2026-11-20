#include "undo/format.h"
#include <algorithm>
#include <cstring>
#include <limits>
#include <butil/logging.h>
#include <butil/crc32c.h>

namespace undo {
namespace {
constexpr uint64_t kFnvPrime = 1099511628211ULL;
constexpr uint32_t kPageMagic = 0x554e4431;
constexpr uint32_t kDeltaFormat = 2;
constexpr size_t kDeltaHeaderBytes = 4 * sizeof(uint32_t);
void ValidateDelta(const Record& r) {
    const auto& d = *r.delta;
    CHECK(d.length <= kMaxValueBytes && d.offset <= d.length &&
          r.old.value.size() <= d.length - d.offset &&
          r.old.value.size() + kDeltaHeaderBytes < d.length);
}
constexpr uint32_t kEntryComplete = 0x554e444f;
// These widths belong to the persisted format. Comparing a fixed zero image
// avoids allocating a temporary string for each null reference or epoch.
template<size_t N> constexpr std::array<char, N> kZeros{};
template<size_t N> constexpr std::string_view ZeroBytes() {
    return {kZeros<N>.data(), N};
}
class Encoder {
public:
    std::string bytes;
    void U(uint64_t value, size_t n) {
        char field[sizeof(value)];
        CHECK(n <= sizeof(field));
        for (size_t i = 0; i < n; ++i) field[i] = char(value >> (8 * i));
        bytes.append(field, n);
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
        e.Raw(ZeroBytes<kRefBytes>());
        return;
    }
    e.Raw(EncodeKey(ref.page));
    e.U(ref.slot, 4);
    e.U(ref.generation, 4);
    e.U(ref.lsn, 8);
}
Ref GetRef(Decoder& d) {
    const auto raw = d.Raw(kRefBytes);
    if (raw == ZeroBytes<kRefBytes>()) return {};
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
// Replay and row validation often inspect the same page image in succession.
uint64_t PageChecksum(const PageBytes& bytes) {
    struct LastImage {
        std::array<char, PAGE_SIZE - kChecksumBytes> payload;
        uint64_t checksum;
        bool valid;
    };
    static thread_local LastImage last{};
    if (last.valid && std::memcmp(last.payload.data(), bytes.data(), last.payload.size()) == 0)
        return last.checksum;
    // A zero byte in FNV-1a only multiplies by the prime modulo 2^64.
    // Verify the actual suffix, including bytes outside the declared used region.
    static constexpr std::array<char, 64> zeros{};
    static constexpr auto suffix_factor = [] {
        std::array<uint64_t, (PAGE_SIZE - kChecksumBytes) / zeros.size() + 1> powers{};
        uint64_t block = 1;
        for (size_t i = 0; i < zeros.size(); ++i) block *= kFnvPrime;
        powers[0] = 1;
        for (size_t i = 1; i < powers.size(); ++i) powers[i] = powers[i - 1] * block;
        return powers;
    }();
    size_t end = last.payload.size();
    while (end >= zeros.size() &&
           std::memcmp(bytes.data() + end - zeros.size(), zeros.data(), zeros.size()) == 0)
        end -= zeros.size();
    last.checksum = Checksum({bytes.data(), end}) * suffix_factor[(last.payload.size() - end) / zeros.size()];
    std::memcpy(last.payload.data(), bytes.data(), last.payload.size());
    last.valid = true;
    return last.checksum;
}
void SetHeader(PageBytes& bytes, const Header& h) {
    Encoder e;
    e.bytes.reserve(kPageHeaderBytes);
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
    checksum.U(PageChecksum(bytes), 8);
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
        h *= kFnvPrime;
    }
    return h;
}
void Validate(const mvcc::TxnId& id) {
    CHECK(id.epoch.size() == 16 && id.epoch != ZeroBytes<16>() && id.local > 0 &&
          id.home < MAX_REMOTE_NODE_NUM);
}
void Validate(const PageKey& k) {
    CHECK(k.kind == PageKind::Undo && k.epoch.size() == 16 && k.epoch != ZeroBytes<16>())
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
    if (bytes == ZeroBytes<kRefBytes>()) return true;
    const auto raw = bytes.substr(0, kKeyBytes);
    // kind=Undo, 16 non-NUL epoch bytes, writer<100, number>1, generation=1.
    uint32_t kind, writer, number, reserved;
    uint64_t generation;
    memcpy(&kind, raw.data(), 4);
    const auto epoch = raw.substr(4, 16);
    memcpy(&writer, raw.data() + 20, 4);
    memcpy(&number, raw.data() + 24, 4);
    memcpy(&generation, raw.data() + 28, 8);
    memcpy(&reserved, raw.data() + 36, 4);
    uint32_t slot, ref_generation;
    uint64_t lsn;
    memcpy(&slot, bytes.data() + kKeyBytes, 4);
    memcpy(&ref_generation, bytes.data() + kKeyBytes + 4, 4);
    memcpy(&lsn, bytes.data() + kKeyBytes + 8, 8);
    return kind == uint32_t(PageKind::Undo) && epoch != ZeroBytes<16>() &&
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
    e.bytes.reserve(kKeyBytes);
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
    e.bytes.reserve(28);
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
    e.bytes.reserve(kRowFixedBytes + r.value.size());
    e.U(kFormat, 4);
    e.U(r.id.table, 4);
    e.U(r.id.page, 4);
    e.U(r.id.slot, 4);
    e.U(r.id.key, 8);
    e.U(r.version.initial, 1);
    e.U(r.version.deleted, 1);
    if (r.version.initial) {
        CHECK(r.version.creator == mvcc::TxnId{});
        e.Raw(ZeroBytes<28>());
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
        CHECK(creator == ZeroBytes<28>());
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
void Record::CompactFor(std::string_view successor) {
    CHECK(!delta);
    const auto length = old.value.size();
    if (length != successor.size() || length <= kDeltaHeaderBytes) return;
    CHECK(length <= kMaxValueBytes);
    size_t first = 0, end = length;
    while (first < end && old.value[first] == successor[first]) ++first;
    while (end > first && old.value[end - 1] == successor[end - 1]) --end;
    if (end - first + kDeltaHeaderBytes >= length) return;
    delta = ValueDelta{uint32_t(length), uint32_t(first),
                       butil::crc32c::Value(successor.data(), length)};
    old.value = old.value.substr(first, end - first);
}
void Record::CheckSuccessor(std::string_view successor) const {
    if (delta) {
        CHECK(successor.size() == delta->length &&
              butil::crc32c::Value(successor.data(), successor.size()) == delta->successor_crc)
            << "undo delta successor value mismatch";
    }
}
RowImage Record::RestorePrevious(const RowImage& successor, const Ref& ref) const {
    CHECK(!ref.empty() && successor.previous == ref && old.id == successor.id);
    if (!delta) return old;
    CheckSuccessor(successor.value);
    RowImage restored = old;
    restored.value = successor.value;
    restored.value.replace(delta->offset, old.value.size(), old.value);
    return restored;
}
std::string EncodeRecord(const Record& r) {
    CHECK(r.sequence > 0 && ((r.sequence == 1) == r.previous_txn.empty()));
    Encoder e;
    CHECK(r.old.value.size() <= kMaxValueBytes);
    e.bytes.reserve(kRecordFixedBytes + r.old.value.size() + (r.delta ? kDeltaHeaderBytes : 0));
    e.U(r.sequence, 8);
    PutRef(e, r.previous_txn);
    if (r.delta) {
        ValidateDelta(r);
        e.U(kDeltaFormat, 4);
        e.U(r.delta->length, 4);
        e.U(r.delta->offset, 4);
        e.U(r.delta->successor_crc, 4);
    }
    e.Raw(EncodeRow(r.old));
    return e.bytes;
}
Record DecodeRecord(std::string_view bytes) {
    CHECK(bytes.size() >= kRecordFixedBytes);
    Decoder d(bytes);
    Record r;
    r.sequence = d.U(8);
    r.previous_txn = GetRef(d);
    auto payload = d.Raw(bytes.size() - 8 - kRefBytes);
    Decoder header(payload);
    if (header.U(4) == kDeltaFormat) {
        r.delta = ValueDelta{uint32_t(header.U(4)), uint32_t(header.U(4)), uint32_t(header.U(4))};
        payload.remove_prefix(kDeltaHeaderBytes);
    }
    r.old = DecodeRow(payload);
    if (r.delta) ValidateDelta(r);
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
    hex.reserve(k.epoch.size() * 2);
    for (uint8_t byte : k.epoch) {
        hex += digits[byte >> 4];
        hex += digits[byte & 15];
    }
    return "UNDO_" + hex + "_" + std::to_string(k.writer) + ".pages";
}
Header Inspect(const PageBytes& bytes) {
    Decoder checksum({bytes.data() + PAGE_SIZE - kChecksumBytes, kChecksumBytes});
    CHECK(checksum.U(8) == PageChecksum(bytes))
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
