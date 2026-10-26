#pragma once

#include <stdexcept>
#include "storage/log_record.h"
#include "compute_node/prepared_txn.h"

// Existing native log header, followed by LE32 payload length, protobuf
// PrepareRequest and LE64 FNV-1a checksum over header + length + payload.
// A complete validated record represents the participant's YES, not COMMIT.
class PreparedLogRecord : public LogRecord {
public:
    static constexpr size_t kEnvelopeSize = LOG_HEADER_SIZE + 4 + 8;
    PreparedLogRecord() {
        log_type_ = LogType::PREPARED;
        log_batch_id_ = INVALID_BATCH_ID;
        prev_lsn_ = INVALID_LSN;
        log_tot_len_ = kEnvelopeSize;
    }
    explicit PreparedLogRecord(const twopc_service::PrepareRequest& r) : PreparedLogRecord() {
        if (!prepared_2pc::ValidRequest(r) || !Fits(r))
            throw std::invalid_argument("invalid or oversized prepared write set");
        payload_ = r.SerializeAsString();
        log_tid_ = r.transaction_id();
        log_node_id_ = r.participant_node();
        log_tot_len_ += payload_.size();
    }
    static bool Fits(const twopc_service::PrepareRequest& r) {
        return r.ByteSizeLong() <= LOG_REPLAY_BUFFER_SIZE - kEnvelopeSize;
    }
    const std::string& payload() const { return payload_; }
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        PutLE(dest + LOG_HEADER_SIZE, payload_.size(), 4);
        memcpy(dest + LOG_HEADER_SIZE + 4, payload_.data(), payload_.size());
        PutLE(dest + log_tot_len_ - 8, Checksum(dest, log_tot_len_ - 8), 8);
    }
    // Bounded decoder for replay and format checks; never reads an incomplete record.
    bool Decode(const char* src, size_t available) {
        if (available < kEnvelopeSize) return false;
        uint32_t length;
        memcpy(&length, src + OFFSET_LOG_TOT_LEN, sizeof(length));
        if (length < kEnvelopeSize || length > LOG_REPLAY_BUFFER_SIZE || length > available) return false;
        const uint64_t payload_size = GetLE(src + LOG_HEADER_SIZE, 4);
        if (payload_size != length - kEnvelopeSize ||
            GetLE(src + length - 8, 8) != Checksum(src, length - 8)) return false;
        twopc_service::PrepareRequest request;
        if (!request.ParseFromArray(src + LOG_HEADER_SIZE + 4, payload_size) ||
            !prepared_2pc::ValidRequest(request)) return false;
        LogRecord::deserialize(src);
        if (log_type_ != LogType::PREPARED || log_tid_ != request.transaction_id() ||
            log_node_id_ != request.participant_node() || prev_lsn_ != INVALID_LSN) return false;
        payload_.assign(src + LOG_HEADER_SIZE + 4, payload_size);
        return true;
    }
    void deserialize(const char* src) override {
        uint32_t length;
        memcpy(&length, src + OFFSET_LOG_TOT_LEN, sizeof(length));
        if (!Decode(src, length)) throw std::runtime_error("invalid PREPARED log record");
    }
private:
    static void PutLE(char* p, uint64_t value, size_t n) {
        for (size_t i = 0; i < n; ++i) p[i] = static_cast<char>(value >> (8 * i));
    }
    static uint64_t GetLE(const char* p, size_t n) {
        uint64_t value = 0;
        for (size_t i = 0; i < n; ++i) value |= uint64_t(static_cast<unsigned char>(p[i])) << (8 * i);
        return value;
    }
    static uint64_t Checksum(const char* p, size_t n) {
        uint64_t hash = 14695981039346656037ULL;
        for (size_t i = 0; i < n; ++i) { hash ^= static_cast<unsigned char>(p[i]); hash *= 1099511628211ULL; }
        return hash;
    }
    std::string payload_;
};
