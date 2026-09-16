#pragma once

#include <string>

#include "common.h"
#include "base/page.h"
#include "record/record.h"

const int OFFSET_BATCH_ID = 0;
// the offset of log_type_ in log header
const int OFFSET_LOG_TYPE = sizeof(batch_id_t);
// the offset of lsn_ in log header
const int OFFSET_LSN = OFFSET_LOG_TYPE + sizeof(int);
// the offset of log_tot_len_ in log header
const int OFFSET_LOG_TOT_LEN = OFFSET_LSN + sizeof(LLSN);
// the offset of log_tid_ in log header
const int OFFSET_LOG_TID = OFFSET_LOG_TOT_LEN + sizeof(uint32_t);
// the offset of log_node_id_ in log header
const int OFFSET_LOG_NODE_ID = OFFSET_LOG_TID + sizeof(tx_id_t);
// the offset of prev_lsn_ in log header
const int OFFSET_PREV_LSN = OFFSET_LOG_NODE_ID + sizeof(node_id_t);
// offset of log data
const int OFFSET_LOG_DATA = OFFSET_PREV_LSN + sizeof(LLSN);
// sizeof log_header
const int LOG_HEADER_SIZE = OFFSET_LOG_DATA;

/* the type of redo log */
enum LogType: int {
    UPDATE = 0,
    INSERT,
    DELETE,
    BEGIN,
    COMMIT,
    ABORT,
    NEWPAGE,
    FSMUPDATE,
    BATCHEND,
    BLINKINSERT,   // BLink 树插入索引项（逻辑日志，重放端在存储侧 blink 树上幂等重做）
    BLINKDELETE    // BLink 树删除索引项（逻辑日志，携带被删的 Rid 供 undo 重插）
};

/* used for debug, convert LogType into string */
static std::string LogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT",
    "NEWPAGE",
    "FSMUPDATE",
    "BATCHEND",
    "BLINKINSERT",
    "BLINKDELETE"
};

class LogRecord {
public:
    batch_id_t log_batch_id_{}; // the batch id
    LogType log_type_; // log type
    LLSN lsn_{}; // log sequence number,现为llsn
    uint32_t log_tot_len_{}; // the length of whole log record
    tx_id_t log_tid_{}; // the transaction id
    node_id_t log_node_id_{}; // the node id
    // no need for undo temporarily，reservered
    LLSN prev_lsn_{}; // 修改的页面的前一个lsn，用于构成日志链条

    virtual ~LogRecord() = default; // 虚析构函数

    // serialize log record into dest(char*)
    virtual void serialize(char *dest) const {
        memcpy(dest + OFFSET_BATCH_ID, &log_batch_id_, sizeof(batch_id_t));
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(LLSN));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(tx_id_t));
        memcpy(dest + OFFSET_LOG_NODE_ID, &log_node_id_, sizeof(node_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(LLSN));
    }

    // deserialize src(char*) into log record
    virtual void deserialize(const char *src) {
        log_batch_id_ = *reinterpret_cast<const batch_id_t *>(src + OFFSET_BATCH_ID);
        log_type_ = *reinterpret_cast<const LogType *>(src + OFFSET_LOG_TYPE);
        lsn_ = *reinterpret_cast<const LLSN *>(src + OFFSET_LSN);
        log_tot_len_ = *reinterpret_cast<const uint32_t *>(src + OFFSET_LOG_TOT_LEN);
        log_tid_ = *reinterpret_cast<const tx_id_t *>(src + OFFSET_LOG_TID);
        log_node_id_ = *reinterpret_cast<const node_id_t *>(src + OFFSET_LOG_NODE_ID);
        prev_lsn_ = *reinterpret_cast<const LLSN *>(src + OFFSET_PREV_LSN);
    }
    // used for debug
    virtual void format_print() {
        printf("Print Log Record:\n");
        printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
        printf("lsn: %lu\n", lsn_);
        printf("log_tot_len: %d\n", log_tot_len_);
        printf("log_tid: %lu\n", log_tid_);
        printf("log_node_id: %d\n", log_node_id_);
        printf("prev_lsn: %lu\n", prev_lsn_);
    }
};

class BeginLogRecord : public LogRecord {
public:
    BeginLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::BEGIN;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
    }

    BeginLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id) : BeginLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
    }

    void format_print() override {
        LogRecord::format_print();
    }
};

class CommitLogRecord : public LogRecord {
public:
    CommitLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::COMMIT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
    }

    CommitLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id) : CommitLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
    }

    void format_print() override {
        LogRecord::format_print();
    }
};

class AbortLogRecord : public LogRecord {
public:
    AbortLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::ABORT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
    }

    AbortLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id) : AbortLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
    }

    void format_print() override {
        LogRecord::format_print();
    }
};

class InsertLogRecord : public LogRecord {
public:
    InsertLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::INSERT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
        table_name_size_ = 0;
    }

    InsertLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, RmRecord &insert_value, int page_no,
                    int slot_no, std::string table_name)
        : InsertLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
        insert_value_ = insert_value;
        page_no_ = page_no;
        slot_no_ = slot_no;
        log_tot_len_ += sizeof(itemkey_t);
        log_tot_len_ += sizeof(size_t);
        log_tot_len_ += insert_value_.value_size_;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += sizeof(int);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;
    }

    ~InsertLogRecord() override {
        delete[] table_name_;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        insert_value_.Serialize(dest + offset, offset);
        memcpy(dest + offset, &page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &slot_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
        offset += table_name_size_;
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        insert_value_.Deserialize(src + OFFSET_LOG_DATA, offset);
        page_no_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        slot_no_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
        offset += table_name_size_;
    }

    void format_print() override {
        printf("insert record\n");
        LogRecord::format_print();
        printf("insert_value: %s\n", insert_value_.value_);
        printf("insert rid: %d, %d\n", page_no_, slot_no_);
        printf("table name: %s\n", table_name_);
    }

    RmRecord insert_value_; // the value of inserted record
    int page_no_{}; // page number
    int slot_no_{}; // slot number
    char *table_name_; // table name
    size_t table_name_size_; // the size of the table name
};

class DeleteLogRecord : public LogRecord {
public:
    DeleteLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::DELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_id_ = INVALID_TABLE_ID;
        table_name_ = nullptr;
        table_name_size_ = 0;
        page_no_ = -1;
        slot_no_ = -1;
        has_delete_meta_ = false;
        has_undo_meta_ = false;
    }

    DeleteLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, table_id_t table_id,
                    const std::string &table_name, int page_no, int slot_no)
        : DeleteLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
        table_id_ = table_id;
        log_tot_len_ += sizeof(table_id_t);
        table_name_size_ = table_name.length();
        log_tot_len_ += sizeof(size_t);
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, table_name.c_str(), table_name_size_);
            log_tot_len_ += table_name_size_;
        }
        page_no_ = page_no;
        log_tot_len_ += sizeof(int);
        slot_no_ = slot_no;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += sizeof(uint8_t); // delete-meta flag
        log_tot_len_ += sizeof(uint8_t); // undo-meta flag
    }

    DeleteLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, const std::string &table_name, int page_no,
                    int slot_no)
        : DeleteLogRecord(batch_id, node_id, txn_id, INVALID_TABLE_ID, table_name, page_no, slot_no) {
    }

    void set_meta(int bucket_offset, char bucket_value, const RmPageHdr &page_hdr, int first_free_page_no,
                  char undo_bucket_value, const RmPageHdr &undo_page_hdr, int undo_first_free_page_no) {
        has_delete_meta_ = true;
        bucket_offset_ = bucket_offset;
        log_tot_len_ += sizeof(int);
        bucket_value_ = bucket_value;
        log_tot_len_ += sizeof(char);
        page_hdr_ = page_hdr;
        log_tot_len_ += sizeof(RmPageHdr);
        first_free_page_no_ = first_free_page_no;
        log_tot_len_ += sizeof(int);

        undo_bucket_value_ = undo_bucket_value;
        undo_page_hdr_ = undo_page_hdr;
        undo_first_free_page_no_ = undo_first_free_page_no;
        has_undo_meta_ = true;
        log_tot_len_ += sizeof(char);
        log_tot_len_ += sizeof(RmPageHdr);
        log_tot_len_ += sizeof(int);
    }

    // Backward-compatible helper when undo meta is unavailable
    void set_meta(int bucket_offset, char bucket_value, const RmPageHdr &page_hdr, int first_free_page_no) {
        set_meta(bucket_offset, bucket_value, page_hdr, first_free_page_no, bucket_value, page_hdr, first_free_page_no);
    }

    ~DeleteLogRecord() override {
        delete[] table_name_;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &table_id_, sizeof(table_id_t));
        offset += sizeof(table_id_t);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            memcpy(dest + offset, table_name_, table_name_size_);
            offset += table_name_size_;
        }
        memcpy(dest + offset, &page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &slot_no_, sizeof(int));
        offset += sizeof(int);

        uint8_t has_delete_flag = static_cast<uint8_t>(has_delete_meta_ ? 1 : 0);
        memcpy(dest + offset, &has_delete_flag, sizeof(uint8_t));
        offset += sizeof(uint8_t);
        if (has_delete_meta_) {
            memcpy(dest + offset, &bucket_offset_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, &bucket_value_, sizeof(char));
            offset += sizeof(char);
            memcpy(dest + offset, &page_hdr_, sizeof(RmPageHdr));
            offset += sizeof(RmPageHdr);
            memcpy(dest + offset, &first_free_page_no_, sizeof(int));
            offset += sizeof(int);
        }

        uint8_t has_undo_flag = static_cast<uint8_t>(has_undo_meta_ ? 1 : 0);
        memcpy(dest + offset, &has_undo_flag, sizeof(uint8_t));
        offset += sizeof(uint8_t);
        if (has_undo_meta_) {
            memcpy(dest + offset, &undo_bucket_value_, sizeof(char));
            offset += sizeof(char);
            memcpy(dest + offset, &undo_page_hdr_, sizeof(RmPageHdr));
            offset += sizeof(RmPageHdr);
            memcpy(dest + offset, &undo_first_free_page_no_, sizeof(int));
        }
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        table_id_ = *reinterpret_cast<const table_id_t *>(src + offset);
        offset += sizeof(table_id_t);
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, src + offset, table_name_size_);
            offset += table_name_size_;
        }
        page_no_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        slot_no_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);

        uint8_t has_delete_flag = *reinterpret_cast<const uint8_t *>(src + offset);
        offset += sizeof(uint8_t);
        has_delete_meta_ = (has_delete_flag != 0);
        if (has_delete_meta_) {
            bucket_offset_ = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
            bucket_value_ = *reinterpret_cast<const char *>(src + offset);
            offset += sizeof(char);
            page_hdr_ = *reinterpret_cast<const RmPageHdr *>(src + offset);
            offset += sizeof(RmPageHdr);
            first_free_page_no_ = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
        }

        uint8_t has_undo_flag = *reinterpret_cast<const uint8_t *>(src + offset);
        offset += sizeof(uint8_t);
        has_undo_meta_ = (has_undo_flag != 0);
        if (has_undo_meta_) {
            undo_bucket_value_ = *reinterpret_cast<const char *>(src + offset);
            offset += sizeof(char);
            undo_page_hdr_ = *reinterpret_cast<const RmPageHdr *>(src + offset);
            offset += sizeof(RmPageHdr);
            undo_first_free_page_no_ = *reinterpret_cast<const int *>(src + offset);
        }
    }

    void format_print() override {
        LogRecord::format_print();
        printf("table id: %d\n", table_id_);
        if (table_name_ != nullptr) {
            printf("table name: %s\n", table_name_);
        }
        printf("delete rid: page=%d slot=%d\n", page_no_, slot_no_);
        if (has_delete_meta_) {
            printf("has delete meta\n");
        }
        if (has_undo_meta_) {
            printf("has undo meta\n");
        }
    }

    // RmRecord delete_value_;
    // Rid rid_;
    table_id_t table_id_;
    char *table_name_;
    size_t table_name_size_;
    int page_no_; // page_no of deleted record
    int slot_no_; // slot_no of deleted record
    bool has_delete_meta_;
    int bucket_offset_{}; // the location in bitmap that has to be modified
    char bucket_value_{}; // modified value in bitmap
    RmPageHdr page_hdr_{}; // modified value of page_hdr
    int first_free_page_no_{}; // modified value of file_hdr.first_free_page_no

    bool has_undo_meta_;
    char undo_bucket_value_{};
    RmPageHdr undo_page_hdr_{};
    int undo_first_free_page_no_{};
};

class FSMUpdateLogRecord : public LogRecord {
public:
    // old_free_space 无效值标记（旧格式日志或未采集到旧值时使用，
    // 此时 Undo 无法精确恢复 FSM 旧状态，只能跳过——FSM 空间信息
    // 属启发式数据，误差可由后续更新自愈）
    static constexpr uint32_t kNoOldFreeSpace = 0xFFFFFFFF;

    FSMUpdateLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::FSMUPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_id_ = INVALID_TABLE_ID;
        page_id_ = 0;
        free_space_ = 0;
        old_free_space_ = kNoOldFreeSpace;
        table_name_ = nullptr;
        table_name_size_ = 0;
        log_tot_len_ += sizeof(table_id_t);
        log_tot_len_ += sizeof(uint32_t);
        log_tot_len_ += sizeof(uint32_t);
        log_tot_len_ += sizeof(uint32_t); // old_free_space_（undo 依据）
        log_tot_len_ += sizeof(size_t);
    }

    FSMUpdateLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id,
                       table_id_t table_id, const std::string &table_name,
                       uint32_t page_id, uint32_t free_space,
                       uint32_t old_free_space = kNoOldFreeSpace)
        : FSMUpdateLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
        table_id_ = table_id;
        page_id_ = page_id;
        free_space_ = free_space;
        old_free_space_ = old_free_space;
        table_name_size_ = table_name.length();
        log_tot_len_ += table_name_size_;
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, table_name.c_str(), table_name_size_);
        }
    }

    ~FSMUpdateLogRecord() override {
        delete[] table_name_;
    }

    bool HasUndoMeta() const { return old_free_space_ != kNoOldFreeSpace; }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &table_id_, sizeof(table_id_t));
        offset += sizeof(table_id_t);
        memcpy(dest + offset, &page_id_, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy(dest + offset, &free_space_, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy(dest + offset, &old_free_space_, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            memcpy(dest + offset, table_name_, table_name_size_);
        }
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        table_id_ = *reinterpret_cast<const table_id_t *>(src + offset);
        offset += sizeof(table_id_t);
        page_id_ = *reinterpret_cast<const uint32_t *>(src + offset);
        offset += sizeof(uint32_t);
        free_space_ = *reinterpret_cast<const uint32_t *>(src + offset);
        offset += sizeof(uint32_t);
        // old_free_space_ 为后续追加字段。用"总长与 name_size 字段自洽性"
        // 区分新旧格式：新格式下 name_size 位于 old_free_space_ 之后，
        // 且 header+tid+pid+free+old+name_size+name == log_tot_len_ 恰好成立；
        // 旧格式无 old_free_space_，该校验不成立则按旧格式解析。
        // 如此旧日志（如有）也能正确恢复，old_free_space_ 为无效值（跳过 undo）
        const int off_after_free = offset;
        bool new_format = false;
        if (off_after_free + (int)sizeof(uint32_t) + (int)sizeof(size_t) <= (int)log_tot_len_) {
            size_t name_size_as_new =
                *reinterpret_cast<const size_t *>(src + off_after_free + sizeof(uint32_t));
            if (off_after_free + (int)sizeof(uint32_t) + (int)sizeof(size_t) + (int)name_size_as_new
                == (int)log_tot_len_) {
                new_format = true;
            }
        }
        if (new_format) {
            old_free_space_ = *reinterpret_cast<const uint32_t *>(src + off_after_free);
            offset = off_after_free + (int)sizeof(uint32_t);
        } else {
            old_free_space_ = kNoOldFreeSpace;
            offset = off_after_free;
        }
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, src + offset, table_name_size_);
        }
    }

    void format_print() override {
        LogRecord::format_print();
        printf("fsm update table_id: %d page_id: %u free: %u old_free: %u\n",
               table_id_, page_id_, free_space_, old_free_space_);
    }

    table_id_t table_id_;
    uint32_t page_id_;
    uint32_t free_space_;
    uint32_t old_free_space_;  // 更新前的空间估计值（类别还原），供 Undo 恢复
    char *table_name_;
    size_t table_name_size_;
};

class UpdateLogRecord : public LogRecord {
public:
    UpdateLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::UPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
        table_id_ = -1;
        has_old_value_ = false;
        log_tot_len_ += sizeof(uint8_t); // store undo-flag regardless of payload
    }

    UpdateLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, RmRecord &new_value, const Rid &rid,
                    std::string table_name, const RmRecord *old_value = nullptr)
        : UpdateLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
        // old_value_ = old_value;
        new_value_ = new_value;
        log_tot_len_ += sizeof(itemkey_t);
        log_tot_len_ += sizeof(size_t);
        log_tot_len_ += new_value_.value_size_;
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        log_tot_len_ += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += table_name_size_;
        if (old_value != nullptr) {
            AttachUndoPayload(*old_value);
        }
    }

    ~UpdateLogRecord() override {
        delete[] table_name_;
    }

    void AttachUndoPayload(const RmRecord &old_value) {
        if (has_old_value_) {
            return;
        }
        old_value_ = old_value;
        has_old_value_ = true;
        log_tot_len_ += sizeof(itemkey_t);
        log_tot_len_ += sizeof(size_t);
        log_tot_len_ += old_value_.value_size_;
    }

    bool HasUndoPayload() const { return has_old_value_; }

    const RmRecord &old_value() const { return old_value_; }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        // memcpy(dest + offset, &old_value_.value_size_, sizeof(int));
        // offset += sizeof(int);
        // memcpy(dest + offset, old_value_.value_, old_value_.value_size_);
        // offset += old_value_.value_size_;
        new_value_.Serialize(dest + offset, offset);
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
        offset += table_name_size_;
        uint8_t has_old = static_cast<uint8_t>(has_old_value_ ? 1 : 0);
        memcpy(dest + offset, &has_old, sizeof(uint8_t));
        offset += sizeof(uint8_t);
        if (has_old_value_) {
            old_value_.Serialize(dest + offset, offset);
        }
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        // printf("finish deserialize log header\n");
        // old_value_.Deserialize(src + OFFSET_LOG_DATA);
        // printf("finish deserialze old value\n");
        int offset = OFFSET_LOG_DATA;
        new_value_.Deserialize(src + offset, offset);
        // printf("finish deserialze new value\n");
        rid_ = *reinterpret_cast<const Rid *>(src + offset);
        // printf("finish deserialze rid\n");
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
        offset += table_name_size_;
        uint8_t has_old = *reinterpret_cast<const uint8_t *>(src + offset);
        offset += sizeof(uint8_t);
        has_old_value_ = (has_old != 0);
        if (has_old_value_) {
            old_value_.Deserialize(src + offset, offset);
        }
    }

    void format_print() override {
        LogRecord::format_print();
        // printf("old_value: %s\n", old_value_.value_);
        printf("new_value: %s\n", new_value_.value_);
        printf("update rid: %d, %d\n", rid_.page_no_, rid_.slot_no_);
        printf("table name: %s\n", table_name_);
    }

    // RmRecord old_value_;
    RmRecord new_value_;
    RmRecord old_value_;
    Rid rid_{};
    char *table_name_;
    size_t table_name_size_{};
    table_id_t table_id_;
    bool has_old_value_;
};

class NewPageLogRecord : public LogRecord {
public:
    NewPageLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::NEWPAGE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_id_ = INVALID_TABLE_ID;
        table_name_ = nullptr;
        request_pages_ = 0;
    }

    NewPageLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, table_id_t table_id,
                     const std::string &table_name, int request_pages) : NewPageLogRecord() {
        log_batch_id_ = batch_id;
        log_node_id_ = node_id;
        log_tid_ = txn_id;
        table_id_ = table_id;
        log_tot_len_ += sizeof(table_id_t);
        request_pages_ = request_pages;
        log_tot_len_ += sizeof(int);
        table_name_size_ = table_name.length();
        log_tot_len_ += sizeof(size_t);
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, table_name.c_str(), table_name_size_);
            log_tot_len_ += table_name_size_;
        }
    }

    NewPageLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, const std::string &table_name,
                     int request_pages)
        : NewPageLogRecord(batch_id, node_id, txn_id, INVALID_TABLE_ID, table_name, request_pages) {
    }

    ~NewPageLogRecord() override {
        delete[] table_name_;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &request_pages_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &table_id_, sizeof(table_id_t));
        offset += sizeof(table_id_t);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            memcpy(dest + offset, table_name_, table_name_size_);
            offset += table_name_size_;
        }
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        request_pages_ = *reinterpret_cast<const int *>(src + offset);
        offset += sizeof(int);
        table_id_ = *reinterpret_cast<const table_id_t *>(src + offset);
        offset += sizeof(table_id_t);
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, src + offset, table_name_size_);
            offset += table_name_size_;
        }
    }

    table_id_t table_id_;
    char *table_name_;
    size_t table_name_size_;
    int request_pages_;
};

class BatchEndLogRecord : public LogRecord {
public:
    BatchEndLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::BATCHEND;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
    }

    BatchEndLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id) : BatchEndLogRecord() {
        log_batch_id_ = batch_id;
        log_node_id_ = node_id;
        log_tid_ = txn_id;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
    }

    void format_print() override {
        LogRecord::format_print();
    }
};

// BLink 树插入索引项的逻辑日志。
// 重放端在存储侧 blink 树上重新执行 insert_entry（幂等：key 已存在则跳过）；
// Undo 时执行 remove_entry 撤销插入。
// 注意：blink 页面没有 RmPageHdr/LLSN 字段，因此该类日志不参与页面
// LLSN 链（prev_lsn_ 恒为 INVALID_LSN），仅依赖日志文件内的全序保证
// 同一 key 操作的重放顺序。
class BLinkInsertLogRecord : public LogRecord {
public:
    BLinkInsertLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::BLINKINSERT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_id_ = INVALID_TABLE_ID;
        key_ = 0;
        rid_ = {.page_no_ = INVALID_PAGE_ID, .slot_no_ = -1};
        table_name_ = nullptr;
        table_name_size_ = 0;
        log_tot_len_ += sizeof(table_id_t);
        log_tot_len_ += sizeof(itemkey_t);
        log_tot_len_ += sizeof(Rid);
        log_tot_len_ += sizeof(size_t);
    }

    BLinkInsertLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id,
                         table_id_t table_id, const std::string &table_name,
                         itemkey_t key, const Rid &rid)
        : BLinkInsertLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
        table_id_ = table_id;
        key_ = key;
        rid_ = rid;
        table_name_size_ = table_name.length();
        log_tot_len_ += table_name_size_;
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, table_name.c_str(), table_name_size_);
        }
    }

    ~BLinkInsertLogRecord() override {
        delete[] table_name_;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &table_id_, sizeof(table_id_t));
        offset += sizeof(table_id_t);
        memcpy(dest + offset, &key_, sizeof(itemkey_t));
        offset += sizeof(itemkey_t);
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            memcpy(dest + offset, table_name_, table_name_size_);
        }
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        table_id_ = *reinterpret_cast<const table_id_t *>(src + offset);
        offset += sizeof(table_id_t);
        key_ = *reinterpret_cast<const itemkey_t *>(src + offset);
        offset += sizeof(itemkey_t);
        rid_ = *reinterpret_cast<const Rid *>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, src + offset, table_name_size_);
        }
    }

    void format_print() override {
        LogRecord::format_print();
        printf("blink insert table_id: %d key: %lu rid: (%d,%d)\n",
               table_id_, (unsigned long)key_, rid_.page_no_, rid_.slot_no_);
    }

    table_id_t table_id_;      // blink 树表 ID（heap table_id + 10000）
    itemkey_t key_;            // 插入的主键
    Rid rid_;                  // 插入的 Rid
    char *table_name_;         // blink 索引文件名（如 xxx_bl）
    size_t table_name_size_;
};

// BLink 树删除索引项的逻辑日志。
// 携带被删除项的 (key, rid)：重放端执行 remove_entry（幂等）；
// Undo 时用 rid 重新 insert_entry 恢复。
class BLinkDeleteLogRecord : public LogRecord {
public:
    BLinkDeleteLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::BLINKDELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_id_ = INVALID_TABLE_ID;
        key_ = 0;
        rid_ = {.page_no_ = INVALID_PAGE_ID, .slot_no_ = -1};
        table_name_ = nullptr;
        table_name_size_ = 0;
        log_tot_len_ += sizeof(table_id_t);
        log_tot_len_ += sizeof(itemkey_t);
        log_tot_len_ += sizeof(Rid);
        log_tot_len_ += sizeof(size_t);
    }

    BLinkDeleteLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id,
                         table_id_t table_id, const std::string &table_name,
                         itemkey_t key, const Rid &rid)
        : BLinkDeleteLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
        table_id_ = table_id;
        key_ = key;
        rid_ = rid;
        table_name_size_ = table_name.length();
        log_tot_len_ += table_name_size_;
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, table_name.c_str(), table_name_size_);
        }
    }

    ~BLinkDeleteLogRecord() override {
        delete[] table_name_;
    }

    void serialize(char *dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &table_id_, sizeof(table_id_t));
        offset += sizeof(table_id_t);
        memcpy(dest + offset, &key_, sizeof(itemkey_t));
        offset += sizeof(itemkey_t);
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            memcpy(dest + offset, table_name_, table_name_size_);
        }
    }

    void deserialize(const char *src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        table_id_ = *reinterpret_cast<const table_id_t *>(src + offset);
        offset += sizeof(table_id_t);
        key_ = *reinterpret_cast<const itemkey_t *>(src + offset);
        offset += sizeof(itemkey_t);
        rid_ = *reinterpret_cast<const Rid *>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);
        if (table_name_size_ > 0) {
            table_name_ = new char[table_name_size_];
            memcpy(table_name_, src + offset, table_name_size_);
        }
    }

    void format_print() override {
        LogRecord::format_print();
        printf("blink delete table_id: %d key: %lu rid: (%d,%d)\n",
               table_id_, (unsigned long)key_, rid_.page_no_, rid_.slot_no_);
    }

    table_id_t table_id_;      // blink 树表 ID（heap table_id + 10000）
    itemkey_t key_;            // 被删除的主键
    Rid rid_;                  // 被删除项的 Rid（undo 重插依据）
    char *table_name_;         // blink 索引文件名（如 xxx_bl）
    size_t table_name_size_;
};