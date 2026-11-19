#include "undo/redo.h"
#include <butil/logging.h>

namespace undo {
namespace {
void Put(char* p, uint64_t v, size_t n) {
    for (size_t i = 0; i < n; ++i) p[i] = char(v >> (8 * i));
}
uint64_t Get(const char* p, size_t n) {
    uint64_t v = 0;
    for (size_t i = 0; i < n; ++i) v |= uint64_t(uint8_t(p[i])) << (8 * i);
    return v;
}
constexpr size_t kFixed = 4 + 4 + kKeyBytes + 28 + 4 + 4;
}  // namespace
Redo::Redo() {
    log_type_ = LogType::UNDO_PAGE;
    log_tot_len_ = LOG_HEADER_SIZE + kFixed + 8;
}
Redo::Redo(Operation op, PageKey key, mvcc::TxnId txn, uint64_t previous, uint32_t index, Record r) : Redo() {
    operation = op;
    page = std::move(key);
    segment = std::move(txn);
    slot = index;
    record = std::move(r);
    Validate(page);
    Validate(segment);
    CHECK(page.epoch == segment.epoch);
    prev_lsn_ = previous;
    log_tid_ = segment.local;
    log_node_id_ = page.writer;
    log_tot_len_ = LOG_HEADER_SIZE + Payload().size() + 8;
    CHECK(log_tot_len_ <= LOG_REPLAY_BUFFER_SIZE);
}
std::string Redo::Payload() const {
    std::string value = operation == Operation::Append ? EncodeRecord(record) : std::string();
    CHECK(operation == Operation::Initialize || operation == Operation::Append ||
          operation == Operation::Seal);
    std::string out(kFixed, '\0');
    size_t pos = 0;
    Put(&out[pos], kFormat, 4);
    pos += 4;
    Put(&out[pos], uint32_t(operation), 4);
    pos += 4;
    out.replace(pos, kKeyBytes, EncodeKey(page));
    pos += kKeyBytes;
    out.replace(pos, 28, EncodeTxn(segment));
    pos += 28;
    Put(&out[pos], slot, 4);
    pos += 4;
    Put(&out[pos], value.size(), 4);
    out += value;
    return out;
}
void Redo::serialize(char* dest) const {
    CHECK(log_type_ == LogType::UNDO_PAGE && lsn_ > prev_lsn_ && log_node_id_ == int(page.writer) &&
          log_tid_ == segment.local);
    const auto payload = Payload();
    CHECK(log_tot_len_ == LOG_HEADER_SIZE + payload.size() + 8);
    LogRecord::serialize(dest);
    memcpy(dest + LOG_HEADER_SIZE, payload.data(), payload.size());
    Put(dest + log_tot_len_ - 8, Checksum({dest, log_tot_len_ - 8}), 8);
}
void Redo::Decode(const char* src, size_t size) {
    CHECK(size >= LOG_HEADER_SIZE + kFixed + 8 && size <= LOG_REPLAY_BUFFER_SIZE);
    uint32_t length;
    memcpy(&length, src + OFFSET_LOG_TOT_LEN, 4);
    CHECK(length == size);
    CHECK(Get(src + size - 8, 8) == Checksum({src, size - 8})) << "undo redo checksum";
    // Copy the native legacy envelope without unaligned reinterpret_cast loads.
    memcpy(&log_type_, src + OFFSET_LOG_TYPE, sizeof(log_type_));
    memcpy(&lsn_, src + OFFSET_LSN, sizeof(lsn_));
    memcpy(&log_batch_id_, src + OFFSET_BATCH_ID, sizeof(log_batch_id_));
    memcpy(&log_tid_, src + OFFSET_LOG_TID, sizeof(log_tid_));
    memcpy(&log_node_id_, src + OFFSET_LOG_NODE_ID, sizeof(log_node_id_));
    memcpy(&prev_lsn_, src + OFFSET_PREV_LSN, sizeof(prev_lsn_));
    log_tot_len_ = length;
    size_t pos = LOG_HEADER_SIZE;
    CHECK(Get(src + pos, 4) == kFormat);
    pos += 4;
    operation = Operation(Get(src + pos, 4));
    pos += 4;
    page = DecodeKey({src + pos, kKeyBytes});
    pos += kKeyBytes;
    segment = DecodeTxn({src + pos, 28});
    pos += 28;
    slot = Get(src + pos, 4);
    pos += 4;
    const auto bytes = Get(src + pos, 4);
    pos += 4;
    CHECK(bytes == size - pos - 8);
    CHECK(log_type_ == LogType::UNDO_PAGE && lsn_ > prev_lsn_ && log_node_id_ == int(page.writer) &&
          log_tid_ == segment.local && page.epoch == segment.epoch);
    if (operation == Operation::Append)
        record = DecodeRecord({src + pos, size_t(bytes)});
    else {
        CHECK((operation == Operation::Initialize || operation == Operation::Seal) && bytes == 0 &&
              slot == 0);
    }
    if (operation == Operation::Initialize)
        CHECK(prev_lsn_ == 0);
    else
        CHECK(prev_lsn_ > 0);
}
void Redo::deserialize(const char* src) {
    uint32_t size;
    memcpy(&size, src + OFFSET_LOG_TOT_LEN, 4);
    Decode(src, size);
}
void Redo::Apply(PageBytes& bytes, bool exists) const {
    CHECK(lsn_ > prev_lsn_ && log_node_id_ == int(page.writer) && log_tid_ == segment.local);
    if (!exists) {
        CHECK(operation == Operation::Initialize && prev_lsn_ == 0);
        bytes = Initialize(page, segment, lsn_);
        return;
    }
    const auto h = Inspect(bytes);
    CHECK(h.key == page && h.segment == segment);
    if (h.lsn >= lsn_) {
        switch (operation) {
            case Operation::Initialize:
                CHECK(h.init_lsn == lsn_ && prev_lsn_ == 0);
                break;
            case Operation::Append:
                CheckAppendReplay(bytes, slot, record, prev_lsn_, lsn_);
                break;
            case Operation::Seal:
                CHECK(h.seal_lsn == lsn_ && h.seal_previous == prev_lsn_);
                break;
            default:
                LOG(FATAL) << "unknown undo redo operation";
        }
        return;
    }
    CHECK(h.lsn == prev_lsn_) << "undo page predecessor not replayed";
    switch (operation) {
        case Operation::Append:
            Append(bytes, slot, record, prev_lsn_, lsn_);
            break;
        case Operation::Seal:
            Seal(bytes, prev_lsn_, lsn_);
            break;
        default:
            LOG(FATAL) << "undo page reinitialized";
    }
}
}  // namespace undo
