// Read-only, fail-closed validation of quiescent BLink/heap/FSM snapshots.
// The parser uses production ABI definitions, but not production traversal code.
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <type_traits>
#include <unistd.h>
#include <vector>
#include <openssl/sha.h>

#include "base/data_item.h"
#include "index/bp_tree/blink/blink_defs.h"
#include "record/record.h"
#include "fsm/fsm_tree.h"
#include "storage/fsm_tree/s_fsm_tree.h"
#include "storage/log_format.h"
#include "util/bitmap.h"

namespace {
constexpr int kOrder = (PAGE_SIZE - sizeof(BLNodeHdr)) / (sizeof(itemkey_t) + sizeof(Rid)) - 1;
constexpr size_t kKeysOffset = sizeof(BLNodeHdr);
constexpr size_t kRidsOffset = kKeysOffset + (kOrder + 1) * sizeof(itemkey_t);
constexpr const char* kDigestFormat = "key-le64,size-le64,version-le64,sha256(value);sorted-key;v2";
static_assert(kOrder > 0 && kRidsOffset + (kOrder + 1) * sizeof(Rid) <= PAGE_SIZE, "BLink arrays exceed page");
static_assert(std::is_standard_layout<DataItem>::value, "DataItem offsets require standard layout");
static_assert(sizeof(itemkey_t) == sizeof(uint64_t), "canonical digest key width");
static_assert(sizeof(FSMMetaData) == sizeof(S_FSMMetaData), "compute/storage FSM metadata ABI differs");
static_assert(sizeof(FSMPageHeader) == sizeof(S_FSMPageHeader), "compute/storage FSM header ABI differs");
#define CHECK_FSM_OFFSET(T, field) static_assert(offsetof(T, field) == offsetof(S_##T, field), "compute/storage FSM field ABI differs")
CHECK_FSM_OFFSET(FSMMetaData, magic_number);
CHECK_FSM_OFFSET(FSMMetaData, version);
CHECK_FSM_OFFSET(FSMMetaData, total_heap_pages);
CHECK_FSM_OFFSET(FSMMetaData, total_fsm_pages);
CHECK_FSM_OFFSET(FSMMetaData, tree_height);
CHECK_FSM_OFFSET(FSMMetaData, root_page_id);
CHECK_FSM_OFFSET(FSMMetaData, next_fsm_page_id);
CHECK_FSM_OFFSET(FSMMetaData, leaves_per_page);
CHECK_FSM_OFFSET(FSMMetaData, children_per_page);
CHECK_FSM_OFFSET(FSMMetaData, table_id);
CHECK_FSM_OFFSET(FSMPageHeader, magic_number);
CHECK_FSM_OFFSET(FSMPageHeader, page_id);
CHECK_FSM_OFFSET(FSMPageHeader, page_type);
CHECK_FSM_OFFSET(FSMPageHeader, parent_page);
CHECK_FSM_OFFSET(FSMPageHeader, level);
CHECK_FSM_OFFSET(FSMPageHeader, first_heap_page);
CHECK_FSM_OFFSET(FSMPageHeader, heap_pages_count);
CHECK_FSM_OFFSET(FSMPageHeader, first_child_page);
CHECK_FSM_OFFSET(FSMPageHeader, child_count);
CHECK_FSM_OFFSET(FSMPageHeader, node_count);
CHECK_FSM_OFFSET(FSMPageHeader, leaf_start);
#undef CHECK_FSM_OFFSET

using PageBytes = std::array<char, PAGE_SIZE>;
using Digest = std::array<unsigned char, SHA256_DIGEST_LENGTH>;

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

template <typename T> T read_at(const char* bytes, size_t offset, size_t length = PAGE_SIZE) {
    static_assert(std::is_trivially_copyable<T>::value, "raw reads require trivial types");
    require(offset <= length && sizeof(T) <= length - offset, "field exceeds buffer bounds");
    T value;
    std::memcpy(&value, bytes + offset, sizeof(T));
    return value;
}

std::string json_string(const std::string& text) {
    std::ostringstream out;
    out << '"';
    for (unsigned char c : text) {
        if (c == '"' || c == '\\') out << '\\' << char(c);
        else if (c < 0x20) out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << unsigned(c) << std::dec;
        else out << char(c);
    }
    out << '"';
    return out.str();
}

std::string hex_bytes(const unsigned char* bytes, size_t length) {
    static const char digits[] = "0123456789abcdef";
    std::string result(length * 2, '0');
    for (size_t i = 0; i < length; ++i) {
        result[2 * i] = digits[bytes[i] >> 4];
        result[2 * i + 1] = digits[bytes[i] & 15];
    }
    return result;
}

Digest value_digest(const char* value, size_t length) {
    Digest result{};
    require(SHA256(reinterpret_cast<const unsigned char*>(value), length, result.data()) != nullptr, "SHA256 failed");
    return result;
}

std::array<unsigned char, 8> le64(uint64_t value) {
    std::array<unsigned char, 8> bytes{};
    for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = static_cast<unsigned char>(value >> (8 * i));
    return bytes;
}

class PageFile {
public:
    explicit PageFile(const std::string& path) : path_(path) {
        fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        require(fd_ >= 0, "cannot open " + path + ": " + std::strerror(errno));
        if (::fstat(fd_, &initial_) != 0 || !S_ISREG(initial_.st_mode) || initial_.st_size < 0 ||
            initial_.st_size % PAGE_SIZE != 0 || initial_.st_size / PAGE_SIZE > std::numeric_limits<page_id_t>::max()) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("invalid file type/length/page count: " + path);
        }
    }
    ~PageFile() { if (fd_ >= 0) ::close(fd_); }
    PageFile(const PageFile&) = delete;
    PageFile& operator=(const PageFile&) = delete;
    int64_t bytes() const { return initial_.st_size; }
    int64_t pages() const { return bytes() / PAGE_SIZE; }
    PageBytes read(int64_t page) const {
        require(page >= 0 && page < pages(), "page id outside " + path_ + ": " + std::to_string(page));
        PageBytes bytes{};
        size_t done = 0;
        while (done < bytes.size()) {
            ssize_t n = ::pread(fd_, bytes.data() + done, bytes.size() - done, off_t(page) * PAGE_SIZE + done);
            if (n < 0 && errno == EINTR) continue;
            require(n > 0, "short/error page read: " + path_ + " page " + std::to_string(page));
            done += static_cast<size_t>(n);
        }
        return bytes;
    }
    void unchanged() const {
        struct stat current{};
        require(::fstat(fd_, &current) == 0 && current.st_size == initial_.st_size &&
                current.st_mtim.tv_sec == initial_.st_mtim.tv_sec && current.st_mtim.tv_nsec == initial_.st_mtim.tv_nsec &&
                current.st_ctim.tv_sec == initial_.st_ctim.tv_sec && current.st_ctim.tv_nsec == initial_.st_ctim.tv_nsec,
                "file changed during validation (quiescent snapshot required): " + path_);
    }
private:
    int fd_ = -1;
    std::string path_;
    struct stat initial_{};
};

struct ModelRange { uint64_t begin, end, version; };
struct Options {
    std::string db_dir, table, model_run_id;
    bool json_only = false, dump_content = false, request_model = false;
    std::vector<ModelRange> ranges;
};

uint64_t decimal(const std::string& input) {
    require(!input.empty() && input.find_first_not_of("0123456789") == std::string::npos, "invalid unsigned integer: " + input);
    size_t consumed = 0;
    uint64_t result = std::stoull(input, &consumed);
    require(consumed == input.size(), "invalid unsigned integer: " + input);
    return result;
}

Options parse_options(int argc, char** argv) {
    require(argc >= 3, "usage: tree_stats <db_dir> <table> [--json-only] [--model-range begin:end:version] [--model-run-id ID] [--dump-content]; tree_stats --abi-json");
    Options options;
    options.db_dir = argv[1];
    options.table = argv[2];
    require(!options.table.empty() && options.table.find('/') == std::string::npos && options.table != "." && options.table != "..", "invalid table name");
    for (int i = 3; i < argc; ++i) {
        std::string argument = argv[i];
        if (argument == "--json-only") options.json_only = true;
        else if (argument == "--dump-content") options.dump_content = true;
        else if (argument == "--model-run-id") {
            require(++i < argc, "--model-run-id requires an ID");
            require(!options.request_model, "duplicate --model-run-id");
            options.request_model = true;
            options.model_run_id = argv[i];
        } else if (argument == "--model-range") {
            require(++i < argc, "--model-range requires begin:end:version (half-open)");
            std::string range = argv[i];
            size_t first = range.find(':'), second = first == std::string::npos ? first : range.find(':', first + 1);
            require(first != std::string::npos && second != std::string::npos && range.find(':', second + 1) == std::string::npos,
                    "invalid model range: " + range);
            ModelRange model{decimal(range.substr(0, first)), decimal(range.substr(first + 1, second - first - 1)), decimal(range.substr(second + 1))};
            require(model.begin < model.end && model.version > 0, "model range must be nonempty and version positive");
            options.ranges.push_back(model);
        } else throw std::runtime_error("unknown argument: " + argument);
    }
    require(!options.request_model || !options.ranges.empty(), "--model-run-id requires --model-range");
    std::sort(options.ranges.begin(), options.ranges.end(), [](const ModelRange& a, const ModelRange& b) { return a.begin < b.begin; });
    for (size_t i = 1; i < options.ranges.size(); ++i)
        require(options.ranges[i - 1].end <= options.ranges[i].begin, "overlapping model ranges");
    return options;
}

std::string model_value(uint64_t key, uint64_t version, const Options& options) {
    std::string result(4, '\0');
    result[0] = 125;
    for (unsigned field = 0; field < 10; ++field) {
        std::ostringstream prefix;
        prefix << (options.request_model ? 'v' : 'L') << std::setfill('0') << std::setw(6) << version
               << (options.request_model ? "|k" : "|K") << std::setw(options.request_model ? 6 : 9) << key
               << (options.request_model ? "|f" : "|F") << field << '|';
        std::string body = prefix.str();
        if (options.request_model) {
            std::string seed_input = options.model_run_id + "|" + std::to_string(key) + "|" + std::to_string(version) + "|" + std::to_string(field);
            Digest seed = value_digest(seed_input.data(), seed_input.size());
            while (body.size() < 100) {
                body.append(reinterpret_cast<const char*>(seed.data()), seed.size());
                seed = value_digest(reinterpret_cast<const char*>(seed.data()), seed.size());
            }
        } else {
            // Reduce before multiplication to match Python's unbounded integers.
            unsigned fill = ((key % 251) * 131 + field * 17 + version % 251) % 251 + 1;
            body.append(100, static_cast<char>(fill));
        }
        result.append(body, 0, 100);
    }
    return result;
}

struct Stats {
    int height = 0;
    int64_t total_keys = 0;
    std::vector<int64_t> level_pages, level_keys;
    int64_t leaf_min_occ = -1, leaf_max_occ = -1, leaf_pages = 0, internal_pages = 0;
    int64_t heap_pages = 0, heap_records = 0, heap_bitmap_set = 0, heap_lock_residual = 0;
    int64_t idx_only_keys = 0, heap_only_keys = 0, rid_mismatch = 0, dup_keys_idx = 0, dup_keys_heap = 0;
    int64_t fsm_file_bytes = 0, heap_file_bytes = 0, bl_file_bytes = 0;
    uint64_t fsm_active_pages = 0, fsm_retired_pages = 0, fsm_conservative_pages = 0;
    uint64_t model_expected_records = 0, model_checked_records = 0, full_value_bytes_verified = 0;
    std::string content_sha256;
};

struct IndexEntry {
    itemkey_t key;
    Rid rid;
    bool seen = false;
    uint32_t value_size = 0;
    Digest value_hash{};
    uint64_t version = 0;
};
struct Node {
    BLNodeHdr header{};
    itemkey_t lower = 0, upper = 0;
    bool bounded = false;
};

BLNodeHdr node_header(const PageBytes& page, page_id_t pid) {
    for (size_t offset : {offsetof(BLNodeHdr, is_leaf), offsetof(BLNodeHdr, is_root), offsetof(BLNodeHdr, has_high_key)})
        require(read_at<uint8_t>(page.data(), offset) <= 1, "noncanonical BLink boolean on page " + std::to_string(pid));
    return read_at<BLNodeHdr>(page.data(), 0);
}

std::vector<IndexEntry> check_index(const PageFile& file, Stats& stats) {
    require(file.pages() >= BL_INIT_PAGE_NUM, "BLink file shorter than initial page count");
    PageBytes head = file.read(BL_HEAD_PAGE_ID);
    BLFileHdr header{read_at<page_id_t>(head.data(), offsetof(BLFileHdr, root_page_id)),
                     read_at<page_id_t>(head.data(), offsetof(BLFileHdr, first_leaf)),
                     read_at<page_id_t>(head.data(), offsetof(BLFileHdr, last_leaf))};
    auto ordinary = [&](page_id_t pid) { return pid >= BL_INIT_ROOT_PAGE_ID && pid < file.pages(); };
    require(ordinary(header.root_page_id) && ordinary(header.first_leaf) && ordinary(header.last_leaf), "invalid BLink file header page ids");
    PageBytes sentinel_page = file.read(BP_LEAF_HEADER_PAGE_ID);
    BLNodeHdr sentinel = node_header(sentinel_page, BP_LEAF_HEADER_PAGE_ID);
    require(sentinel.is_leaf && !sentinel.is_root && !sentinel.has_high_key && sentinel.num_key == 0 &&
            sentinel.right_sibling == INVALID_PAGE_ID && sentinel.next_leaf == header.first_leaf && sentinel.prev_leaf == header.last_leaf,
            "invalid leaf sentinel/page-0 endpoints");

    struct Work { page_id_t pid; int level; itemkey_t lower, upper; bool bounded; };
    std::vector<Work> stack{{header.root_page_id, 1, NEG_KEY, 0, false}};
    std::map<page_id_t, Node> nodes;
    std::vector<std::vector<page_id_t>> levels;
    std::vector<IndexEntry> entries;
    int leaf_depth = -1;
    while (!stack.empty()) {
        Work work = stack.back();
        stack.pop_back();
        require(ordinary(work.pid), "invalid child page id: " + std::to_string(work.pid));
        require(nodes.find(work.pid) == nodes.end(), "cycle/duplicate reachable BLink page: " + std::to_string(work.pid));
        require(work.level <= file.pages() - 2, "impossible BLink depth");
        PageBytes page = file.read(work.pid);
        BLNodeHdr nh = node_header(page, work.pid);
        std::string where = " on BLink page " + std::to_string(work.pid);
        require(nh.is_root == (work.pid == header.root_page_id), "root flag/header mismatch" + where);
        require(nh.num_key >= 0 && nh.num_key <= kOrder, "illegal key count" + where);
        require(nh.is_leaf || nh.num_key > 0, "empty internal page" + where);
        require(nh.has_high_key == work.bounded && (!work.bounded || nh.high_key == work.upper), "high-key/parent interval mismatch" + where);
        require(nh.right_sibling == INVALID_PAGE_ID || ordinary(nh.right_sibling), "invalid right sibling" + where);
        nodes.emplace(work.pid, Node{nh, work.lower, work.upper, work.bounded});
        if (levels.size() < static_cast<size_t>(work.level)) {
            levels.resize(work.level);
            stats.level_pages.resize(work.level);
            stats.level_keys.resize(work.level);
        }
        levels[work.level - 1].push_back(work.pid);
        ++stats.level_pages[work.level - 1];
        stats.level_keys[work.level - 1] += nh.num_key;
        stats.height = static_cast<int>(levels.size());
        std::vector<itemkey_t> keys;
        std::vector<Rid> rids;
        keys.reserve(nh.num_key);
        rids.reserve(nh.num_key);
        for (int i = 0; i < nh.num_key; ++i) {
            itemkey_t key = read_at<itemkey_t>(page.data(), kKeysOffset + size_t(i) * sizeof(itemkey_t));
            Rid rid = read_at<Rid>(page.data(), kRidsOffset + size_t(i) * sizeof(Rid));
            if (i > 0 && key == keys.back()) ++stats.dup_keys_idx;
            require(i == 0 || key > keys.back(), "nonascending/duplicate keys" + where);
            if (!nh.is_leaf && i == 0) require(key == NEG_KEY, "internal first key is not NEG_KEY" + where);
            else require(key >= work.lower && (!work.bounded || key < work.upper) && (nh.is_leaf || key > work.lower),
                         "key outside recursive parent interval" + where);
            keys.push_back(key);
            rids.push_back(rid);
        }
        if (nh.is_leaf) {
            require(leaf_depth == -1 || leaf_depth == work.level, "leaf depths differ");
            leaf_depth = work.level;
            ++stats.leaf_pages;
            if (stats.leaf_min_occ == -1 || nh.num_key < stats.leaf_min_occ) stats.leaf_min_occ = nh.num_key;
            stats.leaf_max_occ = std::max<int64_t>(stats.leaf_max_occ, nh.num_key);
            for (int i = 0; i < nh.num_key; ++i) {
                if (!entries.empty() && entries.back().key == keys[i]) ++stats.dup_keys_idx;
                require(entries.empty() || entries.back().key < keys[i], "duplicate/overlapping leaf key ranges");
                entries.push_back(IndexEntry{keys[i], rids[i]});
                ++stats.total_keys;
            }
        } else {
            ++stats.internal_pages;
            require(nh.prev_leaf == INVALID_PAGE_ID && nh.next_leaf == INVALID_PAGE_ID, "internal page has leaf links" + where);
            for (int i = nh.num_key - 1; i >= 0; --i) {
                require(ordinary(rids[i].page_no_) && rids[i].slot_no_ == -1, "invalid internal child RID" + where);
                bool bounded = i + 1 < nh.num_key || work.bounded;
                itemkey_t upper = i + 1 < nh.num_key ? keys[i + 1] : work.upper;
                stack.push_back(Work{rids[i].page_no_, work.level + 1, i == 0 ? work.lower : keys[i], upper, bounded});
            }
        }
    }
    require(nodes.size() == static_cast<size_t>(file.pages() - BL_INIT_ROOT_PAGE_ID), "allocated ordinary BLink pages are unreachable");
    require(leaf_depth > 0 && !levels.back().empty(), "no leaf level");
    for (const auto& level : levels) {
        for (size_t i = 0; i < level.size(); ++i) {
            const Node& node = nodes.at(level[i]);
            page_id_t expected_right = i + 1 < level.size() ? level[i + 1] : INVALID_PAGE_ID;
            require(node.header.right_sibling == expected_right, "wrong same-level right sibling on page " + std::to_string(level[i]));
            require(node.header.has_high_key == (i + 1 < level.size()), "wrong rightmost high-key flag");
            if (i + 1 < level.size()) require(node.header.high_key == nodes.at(level[i + 1]).lower, "high-key/right-sibling boundary mismatch");
        }
    }
    const auto& leaves = levels.back();
    require(leaves.front() == header.first_leaf && leaves.back() == header.last_leaf, "file-header leaf endpoints disagree with tree order");
    for (size_t i = 0; i < leaves.size(); ++i) {
        const BLNodeHdr& leaf = nodes.at(leaves[i]).header;
        require(leaf.is_leaf && leaf.prev_leaf == (i == 0 ? BP_LEAF_HEADER_PAGE_ID : leaves[i - 1]) &&
                leaf.next_leaf == (i + 1 == leaves.size() ? BP_LEAF_HEADER_PAGE_ID : leaves[i + 1]),
                "leaf forward/backward chain differs from exact tree order on page " + std::to_string(leaves[i]));
    }
    return entries;
}

struct HeapInfo {
    RmFileHdr header{};
    std::vector<uint32_t> free_bytes;
    table_id_t table_id = INVALID_TABLE_ID;
};

HeapInfo check_heap(const PageFile& file, std::vector<IndexEntry>& entries, const Options& options, Stats& stats) {
    require(file.pages() >= 1, "missing heap header");
    PageBytes page = file.read(RM_FILE_HDR_PAGE);
    RmPageHdr page_header = read_at<RmPageHdr>(page.data(), OFFSET_PAGE_HDR);
    require(page_header.num_records_ == 0 && page_header.next_free_page_no_ == RM_NO_PAGE, "invalid heap page-0 header");
    HeapInfo heap;
    heap.header = read_at<RmFileHdr>(page.data(), sizeof(RmPageHdr));
    const RmFileHdr& header = heap.header;
    require(header.num_pages_ == file.pages(), "heap header num_pages differs from physical file");
    require(header.record_size_ >= static_cast<int>(sizeof(DataItem)) && header.record_size_ <= RM_MAX_RECORD_SIZE,
            "invalid heap record size");
    const size_t slot_size = size_t(header.record_size_) + sizeof(itemkey_t);
    require(header.num_records_per_page_ > 0 && header.num_records_per_page_ <= PAGE_SIZE / slot_size,
            "invalid heap slots per page");
    const int expected_slots = (BITMAP_WIDTH * (PAGE_SIZE - 1 - int(sizeof(RmFileHdr))) + 1) / (1 + slot_size * BITMAP_WIDTH);
    require(header.num_records_per_page_ == expected_slots && header.bitmap_size_ == (expected_slots + BITMAP_WIDTH - 1) / BITMAP_WIDTH,
            "heap slot/bitmap geometry differs from production allocation formula");
    require(sizeof(RmPageHdr) + size_t(header.bitmap_size_) + size_t(expected_slots) * slot_size <= PAGE_SIZE,
            "heap slots overflow page");
    auto valid_free_link = [&](int pid) { return pid == RM_NO_PAGE || (pid >= RM_FIRST_RECORD_PAGE && pid < header.num_pages_); };
    require(valid_free_link(header.first_free_page_no_), "heap first_free_page_no outside allocated range");
    heap.free_bytes.resize(file.pages(), 0);
    stats.heap_pages = file.pages() - RM_FIRST_RECORD_PAGE;
    for (auto& entry : entries)
        require(entry.rid.page_no_ >= RM_FIRST_RECORD_PAGE && entry.rid.page_no_ < header.num_pages_ &&
                entry.rid.slot_no_ >= 0 && entry.rid.slot_no_ < header.num_records_per_page_, "index RID outside heap allocation/slot bounds");

    for (const ModelRange& range : options.ranges) {
        require(range.end - range.begin <= std::numeric_limits<uint64_t>::max() - stats.model_expected_records, "model record count overflow");
        stats.model_expected_records += range.end - range.begin;
    }
    if (!options.ranges.empty())
        require(stats.model_expected_records == entries.size(), "model expected key count differs from index");

    for (int64_t pid = RM_FIRST_RECORD_PAGE; pid < file.pages(); ++pid) {
        page = file.read(pid);
        RmPageHdr ph = read_at<RmPageHdr>(page.data(), OFFSET_PAGE_HDR);
        std::string where = " on heap page " + std::to_string(pid);
        require(ph.num_records_ >= 0 && ph.num_records_ <= expected_slots, "invalid heap num_records" + where);
        require(valid_free_link(ph.next_free_page_no_), "heap next_free_page_no outside allocation" + where);
        const char* bitmap = page.data() + sizeof(RmPageHdr);
        const char* slots = bitmap + header.bitmap_size_;
        int bitmap_count = 0;
        for (int slot = 0; slot < expected_slots; ++slot) {
            bool occupied = Bitmap::is_set(bitmap, slot);
            const char* tuple = slots + size_t(slot) * slot_size;
            const char* data_item = tuple + sizeof(itemkey_t);
            uint8_t valid = read_at<uint8_t>(data_item, offsetof(DataItem, valid), header.record_size_);
            uint8_t deletion = read_at<uint8_t>(data_item, offsetof(DataItem, user_insert), header.record_size_);
            lock_t lock = read_at<lock_t>(data_item, offsetof(DataItem, lock), header.record_size_);
            require(valid <= 1 && (valid == 1) == occupied, "bitmap/valid mismatch at slot " + std::to_string(slot) + where);
            if (lock != UNLOCKED) ++stats.heap_lock_residual;
            require(lock == UNLOCKED, "residual tuple lock at slot " + std::to_string(slot) + where);
            require(deletion == 0, "residual tuple deletion marker at slot " + std::to_string(slot) + where);
            if (!occupied) continue;
            ++bitmap_count;
            ++stats.heap_bitmap_set;
            ++stats.heap_records;
            itemkey_t key = read_at<itemkey_t>(tuple, 0, slot_size);
            table_id_t tid = read_at<table_id_t>(data_item, offsetof(DataItem, table_id), header.record_size_);
            require(tid >= 0, "invalid heap DataItem table_id" + where);
            if (heap.table_id == INVALID_TABLE_ID) heap.table_id = tid;
            require(tid == heap.table_id, "mixed table IDs in heap");
            int value_size = read_at<int>(data_item, offsetof(DataItem, value_size), header.record_size_);
            require(value_size >= 0 && size_t(value_size) == size_t(header.record_size_) - sizeof(DataItem), "heap DataItem value_size differs from fixed record size" + where);
            const char* value = data_item + sizeof(DataItem);
            auto found = std::lower_bound(entries.begin(), entries.end(), key, [](const IndexEntry& entry, itemkey_t target) { return entry.key < target; });
            if (found == entries.end() || found->key != key) ++stats.heap_only_keys;
            require(found != entries.end() && found->key == key, "heap key absent from index: " + std::to_string(key));
            if (found->seen) ++stats.dup_keys_heap;
            require(!found->seen, "duplicate heap key: " + std::to_string(key));
            if (found->rid != Rid{static_cast<page_id_t>(pid), slot}) ++stats.rid_mismatch;
            require(found->rid == Rid{static_cast<page_id_t>(pid), slot}, "key-to-RID mismatch for key " + std::to_string(key));
            found->seen = true;
            found->value_size = static_cast<uint32_t>(value_size);
            found->value_hash = value_digest(value, value_size);
            found->version = read_at<uint64_t>(data_item, offsetof(DataItem, version), header.record_size_);
            if (!options.ranges.empty()) {
                auto range = std::upper_bound(options.ranges.begin(), options.ranges.end(), key, [](uint64_t target, const ModelRange& model) { return target < model.begin; });
                require(range != options.ranges.begin(), "heap key outside expected model: " + std::to_string(key));
                --range;
                require(key < range->end, "heap key outside expected model: " + std::to_string(key));
                std::string expected = model_value(key, range->version, options);
                require(expected.size() == size_t(value_size) && std::memcmp(expected.data(), value, expected.size()) == 0,
                        "complete model value mismatch for key " + std::to_string(key));
                ++stats.model_checked_records;
                stats.full_value_bytes_verified += value_size;
            }
        }
        for (int bit = expected_slots; bit < header.bitmap_size_ * BITMAP_WIDTH; ++bit)
            require(!Bitmap::is_set(bitmap, bit), "nonzero bitmap padding bit" + where);
        require(bitmap_count == ph.num_records_, "heap num_records differs from bitmap/valid count" + where);
        heap.free_bytes[pid] = (expected_slots - bitmap_count) * slot_size;
    }
    for (const IndexEntry& entry : entries) if (!entry.seen) ++stats.idx_only_keys;
    require(stats.idx_only_keys == 0, "index contains keys missing from heap");
    require(options.ranges.empty() || stats.model_checked_records == stats.model_expected_records, "model coverage is incomplete");
    return heap;
}

uint8_t space_category(uint32_t bytes) {
    // Same thresholds as SecFSM/S_SecFSM::space_to_category (not byte equality).
    if (bytes == 0) return static_cast<uint8_t>(S_SpaceCategory::NO_SPACE);
    if (bytes >= PAGE_SIZE * 9 / 10) return static_cast<uint8_t>(S_SpaceCategory::EMPTY);
    if (bytes >= PAGE_SIZE * 2 / 3) return static_cast<uint8_t>(S_SpaceCategory::ALMOST_EMPTY);
    if (bytes >= PAGE_SIZE / 3) return static_cast<uint8_t>(S_SpaceCategory::HALF_FULL);
    return static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL);
}

bool valid_category(uint8_t category) {
    return category == static_cast<uint8_t>(S_SpaceCategory::NO_SPACE) || category == static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL) ||
           category == static_cast<uint8_t>(S_SpaceCategory::HALF_FULL) || category == static_cast<uint8_t>(S_SpaceCategory::ALMOST_EMPTY) ||
           category == static_cast<uint8_t>(S_SpaceCategory::EMPTY);
}

void check_fsm(const PageFile& file, const HeapInfo& heap, Stats& stats) {
    require(file.pages() >= S_FSM_ROOT_PAGE_ID + 1, "FSM file shorter than initial pages");
    PageBytes meta_page = file.read(S_FSM_META_PAGE_ID);
    S_FSMMetaData meta = read_at<S_FSMMetaData>(meta_page.data(), 0);
    require(meta.magic_number == 0x46534D54 && meta.version == 1, "invalid FSM metadata magic/version");
    require(meta.total_heap_pages >= heap.free_bytes.size() && meta.total_heap_pages <= uint32_t(std::numeric_limits<page_id_t>::max()),
            "FSM capacity does not cover heap allocation or exceeds page-id range");
    require(meta.leaves_per_page == S_LEAVES_PER_PAGE && meta.children_per_page == S_CHILDREN_PER_PAGE, "invalid FSM fanout metadata");
    require(meta.tree_height > 0 && meta.total_fsm_pages > 0 && meta.tree_height <= meta.total_fsm_pages, "invalid FSM tree size/height");
    require(uint64_t(meta.next_fsm_page_id) + 1 == uint64_t(file.pages()) && meta.next_fsm_page_id >= S_FSM_ROOT_PAGE_ID &&
            meta.total_fsm_pages <= meta.next_fsm_page_id - S_FSM_ROOT_PAGE_ID + 1, "FSM allocation metadata exceeds file");
    // Storage extend() rebuilds a new contiguous generation without reclaiming the old one.
    uint32_t first_active = meta.next_fsm_page_id - meta.total_fsm_pages + 1;
    require(meta.root_page_id >= first_active && meta.root_page_id <= meta.next_fsm_page_id, "FSM root outside active allocation generation");
    require(meta.table_id >= 20000 && (heap.table_id == INVALID_TABLE_ID || int64_t(heap.table_id) + 20000 == meta.table_id), "FSM table_id does not match heap");
    stats.fsm_retired_pages = first_active - S_FSM_ROOT_PAGE_ID;
    PageBytes header_page = file.read(0);
    size_t cached_offset = 0;
    if (read_at<int>(header_page.data(), 0) == RM_NO_PAGE) {
        RmPageHdr prefix = read_at<RmPageHdr>(header_page.data(), 0);
        require(prefix.num_records_ == 0, "invalid FSM page-0 RmPageHdr");
        cached_offset = sizeof(RmPageHdr);
    }
    RmFileHdr cached = read_at<RmFileHdr>(header_page.data(), cached_offset);
    require(cached.record_size_ == heap.header.record_size_ && cached.num_records_per_page_ == heap.header.num_records_per_page_ &&
            cached.bitmap_size_ == heap.header.bitmap_size_ && cached.num_pages_ >= 1 && cached.num_pages_ <= heap.header.num_pages_ &&
            (cached.first_free_page_no_ == RM_NO_PAGE || (cached.first_free_page_no_ >= RM_FIRST_RECORD_PAGE && cached.first_free_page_no_ < heap.header.num_pages_)),
            "FSM page-0 cached heap geometry invalid");

    struct Work { uint32_t pid, parent, level; int expected_root; };
    std::vector<Work> stack{{meta.root_page_id, 0, meta.tree_height - 1, -1}};
    std::set<uint32_t> visited;
    uint64_t next_heap_page = 0;
    while (!stack.empty()) {
        Work work = stack.back();
        stack.pop_back();
        require(work.pid >= first_active && work.pid <= meta.next_fsm_page_id, "FSM child outside active allocation generation");
        require(visited.insert(work.pid).second, "FSM cycle/duplicate reachable page");
        PageBytes page = file.read(work.pid);
        S_FSMPageHeader header = read_at<S_FSMPageHeader>(page.data(), 0);
        std::string where = " on FSM page " + std::to_string(work.pid);
        bool leaf = header.page_type == S_FSMPageType::LEAF_PAGE;
        require(header.magic_number == 0x46535047 && header.page_id == work.pid, "FSM page identity mismatch" + where);
        require(leaf || header.page_type == S_FSMPageType::INTERNAL_PAGE, "invalid FSM page type" + where);
        require(header.parent_page == work.parent && header.level == work.level && leaf == (work.level == 0), "FSM parent/level/type mismatch" + where);
        uint32_t count = leaf ? header.heap_pages_count : header.child_count;
        require(count > 0 && count <= (leaf ? meta.leaves_per_page : meta.children_per_page), "invalid FSM child/heap count" + where);
        uint32_t capacity = 1;
        while (capacity < count) capacity <<= 1;
        require(header.leaf_start == capacity - 1 && header.node_count == header.leaf_start + count, "invalid FSM binary-tree geometry" + where);
        size_t child_bytes = leaf ? 0 : size_t(count) * sizeof(uint32_t);
        require(sizeof(S_FSMPageHeader) + uint64_t(header.node_count) + child_bytes <= PAGE_SIZE, "FSM arrays exceed page" + where);
        const auto* values = reinterpret_cast<const unsigned char*>(page.data() + sizeof(S_FSMPageHeader));
        for (uint32_t i = 0; i < header.node_count; ++i) require(valid_category(values[i]), "invalid FSM category" + where);
        for (uint32_t i = 0; i < header.leaf_start; ++i) {
            uint32_t left = 2 * i + 1, right = left + 1;
            uint8_t expected = std::max(left < header.node_count ? values[left] : uint8_t(0), right < header.node_count ? values[right] : uint8_t(0));
            require(values[i] == expected, "FSM in-page max aggregate mismatch" + where);
        }
        require(work.expected_root < 0 || values[0] == work.expected_root, "FSM cross-page aggregate mismatch" + where);
        if (leaf) {
            require(header.child_count == 0 && header.first_child_page == 0 && header.first_heap_page == next_heap_page &&
                    uint64_t(header.first_heap_page) + count <= meta.total_heap_pages, "FSM heap ranges overlap/gap/out of allocation" + where);
            for (uint32_t i = 0; i < count; ++i) {
                uint32_t heap_page = header.first_heap_page + i;
                uint8_t stored = values[header.leaf_start + i];
                uint8_t actual = heap_page < heap.free_bytes.size() ? space_category(heap.free_bytes[heap_page])
                                                                  : static_cast<uint8_t>(S_SpaceCategory::NO_SPACE);
                require(stored <= actual, "FSM overestimates heap capacity at heap page " + std::to_string(heap_page));
                if (stored < actual) ++stats.fsm_conservative_pages;
            }
            next_heap_page += count;
        } else {
            require(header.first_heap_page == 0 && header.heap_pages_count == 0, "FSM internal page has heap interval" + where);
            size_t children_offset = sizeof(S_FSMPageHeader) + header.node_count;
            require(header.first_child_page == read_at<uint32_t>(page.data(), children_offset), "FSM first_child/blob mismatch" + where);
            for (uint32_t i = count; i > 0; --i) {
                uint32_t child = read_at<uint32_t>(page.data(), children_offset + size_t(i - 1) * sizeof(uint32_t));
                stack.push_back(Work{child, work.pid, work.level - 1, values[header.leaf_start + i - 1]});
            }
        }
    }
    stats.fsm_active_pages = visited.size();
    require(visited.size() == meta.total_fsm_pages, "unreachable allocated active FSM pages");
    require(next_heap_page == meta.total_heap_pages, "FSM does not cover all allocated heap pages");
}

std::string content_digest(const std::vector<IndexEntry>& entries) {
    SHA256_CTX context;
    require(SHA256_Init(&context) == 1, "SHA256 init failed");
    for (const auto& entry : entries) {
        auto key = le64(entry.key), size = le64(entry.value_size), version = le64(entry.version);
        require(SHA256_Update(&context, key.data(), key.size()) == 1 && SHA256_Update(&context, size.data(), size.size()) == 1 &&
                SHA256_Update(&context, version.data(), version.size()) == 1 &&
                SHA256_Update(&context, entry.value_hash.data(), entry.value_hash.size()) == 1, "SHA256 update failed");
    }
    Digest result{};
    require(SHA256_Final(result.data(), &context) == 1, "SHA256 final failed");
    return hex_bytes(result.data(), result.size());
}

void dump_content(const PageFile& file, const HeapInfo& heap, const std::vector<IndexEntry>& entries) {
    const size_t slot_size = size_t(heap.header.record_size_) + sizeof(itemkey_t);
    PageBytes page{};
    page_id_t last_page = INVALID_PAGE_ID;
    std::cout << "CONTENT_BEGIN\n";
    for (const IndexEntry& entry : entries) {
        if (last_page != entry.rid.page_no_) { page = file.read(entry.rid.page_no_); last_page = entry.rid.page_no_; }
        size_t offset = sizeof(RmPageHdr) + heap.header.bitmap_size_ + size_t(entry.rid.slot_no_) * slot_size + sizeof(itemkey_t) + sizeof(DataItem);
        require(offset <= PAGE_SIZE && entry.value_size <= PAGE_SIZE - offset, "content export bounds error");
        const char* value = page.data() + offset;
        require(value_digest(value, entry.value_size) == entry.value_hash &&
                read_at<uint64_t>(value - sizeof(DataItem), offsetof(DataItem, version), sizeof(DataItem)) == entry.version,
                "heap value/version changed during export");
        std::cout << "{\"key\":" << entry.key << ",\"version\":" << entry.version << ",\"value_hex\":\""
                  << hex_bytes(reinterpret_cast<const unsigned char*>(value), entry.value_size) << "\"}\n";
    }
    std::cout << "CONTENT_END\n";
    require(bool(std::cout), "content output failed");
}

void abi_json() {
    std::cout << '{';
    bool first = true;
    auto field = [&](const std::string& name, uint64_t value) { std::cout << (first ? "" : ",") << json_string(name) << ':' << value; first = false; };
#define ABI_SIZE(T) field("sizeof_" #T, sizeof(T))
#define ABI_OFFSET(T, member) field(#T "." #member, offsetof(T, member))
    field("PAGE_SIZE", PAGE_SIZE); field("order", kOrder); field("BL_KEYS_OFFSET", kKeysOffset); field("BL_RIDS_OFFSET", kRidsOffset);
    field("BP_LEAF_HEADER_PAGE_ID", BP_LEAF_HEADER_PAGE_ID); field("BL_HEAD_PAGE_ID", BL_HEAD_PAGE_ID);
    field("BL_INIT_ROOT_PAGE_ID", BL_INIT_ROOT_PAGE_ID); field("BITMAP_WIDTH", BITMAP_WIDTH);
    ABI_SIZE(BLFileHdr); ABI_SIZE(BLNodeHdr); ABI_SIZE(Rid); ABI_SIZE(RmFileHdr); ABI_SIZE(RmPageHdr); ABI_SIZE(DataItem);
    ABI_SIZE(itemkey_t); ABI_SIZE(table_id_t); ABI_SIZE(page_id_t); ABI_SIZE(size_t); ABI_SIZE(tx_id_t); ABI_SIZE(batch_id_t); ABI_SIZE(LLSN);
    ABI_SIZE(FSMMetaData); ABI_SIZE(FSMPageHeader);
    ABI_OFFSET(BLFileHdr, root_page_id); ABI_OFFSET(BLFileHdr, first_leaf); ABI_OFFSET(BLFileHdr, last_leaf);
    ABI_OFFSET(BLNodeHdr, prev_leaf); ABI_OFFSET(BLNodeHdr, next_leaf); ABI_OFFSET(BLNodeHdr, right_sibling); ABI_OFFSET(BLNodeHdr, num_key);
    ABI_OFFSET(BLNodeHdr, high_key); ABI_OFFSET(BLNodeHdr, is_leaf); ABI_OFFSET(BLNodeHdr, has_high_key); ABI_OFFSET(BLNodeHdr, is_root);
    ABI_OFFSET(Rid, page_no_); ABI_OFFSET(Rid, slot_no_);
    ABI_OFFSET(RmFileHdr, record_size_); ABI_OFFSET(RmFileHdr, num_pages_); ABI_OFFSET(RmFileHdr, num_records_per_page_);
    ABI_OFFSET(RmFileHdr, first_free_page_no_); ABI_OFFSET(RmFileHdr, bitmap_size_);
    ABI_OFFSET(RmPageHdr, next_free_page_no_); ABI_OFFSET(RmPageHdr, num_records_); ABI_OFFSET(RmPageHdr, LLSN_); ABI_OFFSET(RmPageHdr, pre_LLSN_);
    ABI_OFFSET(DataItem, table_id); ABI_OFFSET(DataItem, lock); ABI_OFFSET(DataItem, value); ABI_OFFSET(DataItem, value_size);
    ABI_OFFSET(DataItem, version); ABI_OFFSET(DataItem, prev_lsn); ABI_OFFSET(DataItem, valid); ABI_OFFSET(DataItem, user_insert);
    ABI_OFFSET(FSMMetaData, magic_number); ABI_OFFSET(FSMMetaData, version); ABI_OFFSET(FSMMetaData, total_heap_pages); ABI_OFFSET(FSMMetaData, total_fsm_pages);
    ABI_OFFSET(FSMMetaData, tree_height); ABI_OFFSET(FSMMetaData, root_page_id); ABI_OFFSET(FSMMetaData, next_fsm_page_id);
    ABI_OFFSET(FSMMetaData, leaves_per_page); ABI_OFFSET(FSMMetaData, children_per_page); ABI_OFFSET(FSMMetaData, table_id);
    ABI_OFFSET(FSMPageHeader, magic_number); ABI_OFFSET(FSMPageHeader, page_id); ABI_OFFSET(FSMPageHeader, page_type);
    ABI_OFFSET(FSMPageHeader, parent_page); ABI_OFFSET(FSMPageHeader, level); ABI_OFFSET(FSMPageHeader, first_heap_page);
    ABI_OFFSET(FSMPageHeader, heap_pages_count); ABI_OFFSET(FSMPageHeader, first_child_page); ABI_OFFSET(FSMPageHeader, child_count);
    ABI_OFFSET(FSMPageHeader, node_count); ABI_OFFSET(FSMPageHeader, leaf_start);
    field("FSM_META_PAGE_ID", FSM_META_PAGE_ID); field("FSM_ROOT_PAGE_ID", FSM_ROOT_PAGE_ID);
    field("LEAVES_PER_PAGE", LEAVES_PER_PAGE); field("CHILDREN_PER_PAGE", CHILDREN_PER_PAGE);
    field("LOG_HEADER_SIZE", LOG_HEADER_SIZE); field("OFFSET_BATCH_ID", OFFSET_BATCH_ID); field("OFFSET_LOG_TYPE", OFFSET_LOG_TYPE);
    field("OFFSET_LSN", OFFSET_LSN); field("OFFSET_LOG_TOT_LEN", OFFSET_LOG_TOT_LEN); field("OFFSET_LOG_TID", OFFSET_LOG_TID);
    field("OFFSET_LOG_NODE_ID", OFFSET_LOG_NODE_ID); field("OFFSET_PREV_LSN", OFFSET_PREV_LSN); field("OFFSET_LOG_DATA", OFFSET_LOG_DATA);
    field("SEG_SIZE", logfmt::SEG_SIZE); field("SEG_HEADER_SIZE", logfmt::SEG_HEADER_SIZE); field("BLOCK_SIZE", logfmt::BLOCK_SIZE);
    field("BLOCK_HEADER_SIZE", logfmt::BLOCK_HEADER_SIZE); field("BLOCK_PAYLOAD_MAX", logfmt::BLOCK_PAYLOAD_MAX);
    field("LOG_FORMAT_VERSION", logfmt::LOG_FORMAT_VERSION); field("MANIFEST_BODY_SIZE", logfmt::ManifestRecord::kBodyLen);
    field("log_header_size", LOG_HEADER_SIZE); field("batch_size", sizeof(batch_id_t)); field("tx_size", sizeof(tx_id_t));
    field("node_size", sizeof(node_id_t)); field("table_size", sizeof(table_id_t)); field("lsn_size", sizeof(LLSN));
    field("offset_type", OFFSET_LOG_TYPE); field("offset_length", OFFSET_LOG_TOT_LEN); field("offset_tx", OFFSET_LOG_TID);
    field("offset_node", OFFSET_LOG_NODE_ID); field("offset_prev_lsn", OFFSET_PREV_LSN); field("offset_data", OFFSET_LOG_DATA);
    field("segment_header_size", logfmt::SEG_HEADER_SIZE); field("block_size", logfmt::BLOCK_SIZE);
    field("page_size", PAGE_SIZE); field("blink_node_size", sizeof(BLNodeHdr)); field("rid_size", sizeof(Rid));
    field("dataitem_size", sizeof(DataItem)); field("dataitem_lock_offset", offsetof(DataItem, lock));
    field("dataitem_valid_offset", offsetof(DataItem, valid));
    field("FSMUPDATE_FIXED_SIZE", FSMUpdateLogRecord().log_tot_len_);
    field("BLINKINSERT_FIXED_SIZE", BLinkInsertLogRecord().log_tot_len_); field("BLINKDELETE_FIXED_SIZE", BLinkDeleteLogRecord().log_tot_len_);
    RmRecord empty_record(0, size_t(0));
    field("INSERT_FIXED_SIZE", InsertLogRecord(0, 0, 0, empty_record, 0, 0, "").log_tot_len_);
    field("UPDATE_FIXED_SIZE", UpdateLogRecord(0, 0, 0, empty_record, Rid{0, 0}, "").log_tot_len_);
    field("UPDATE_WITH_UNDO_FIXED_SIZE", UpdateLogRecord(0, 0, 0, empty_record, Rid{0, 0}, "", &empty_record).log_tot_len_);
    field("NEWPAGE_FIXED_SIZE", NewPageLogRecord(0, 0, 0, table_id_t(0), "", 0).log_tot_len_);
    DeleteLogRecord deletion(0, 0, 0, table_id_t(0), "", 0, 0);
    field("DELETE_FIXED_SIZE", deletion.log_tot_len_);
    deletion.set_meta(0, 0, RmPageHdr{}, RM_NO_PAGE, 0, RmPageHdr{}, RM_NO_PAGE);
    field("DELETE_WITH_META_FIXED_SIZE", deletion.log_tot_len_);
    for (int kind = int(LogType::UPDATE); kind <= int(LogType::ABORTEND); ++kind) field("LogType." + LogTypeStr[kind], kind);
#undef ABI_OFFSET
#undef ABI_SIZE
    std::cout << "}\n";
}

void print_report(const Options& options, const Stats& stats, const std::vector<std::string>& errors) {
    const bool pass = errors.empty();
    if (!options.json_only) {
        std::cout << "BLink/heap/FSM: " << options.db_dir << '/' << options.table << "\n"
                  << "order=" << kOrder << " height=" << stats.height << " index_keys=" << stats.total_keys
                  << " heap_records=" << stats.heap_records << " pass=" << (pass ? "true" : "false") << '\n';
        for (const auto& error : errors) std::cout << "ERR: " << error << '\n';
    }
    std::cout << "JSON_BEGIN\n{";
    bool first = true;
    auto field = [&](const std::string& name, auto value) { std::cout << (first ? "" : ",") << json_string(name) << ':' << value; first = false; };
    field("table", json_string(options.table)); field("order", kOrder); field("height", stats.height);
    auto array = [&](const char* name, const std::vector<int64_t>& values) {
        std::cout << ',' << json_string(name) << ":[";
        for (size_t i = 0; i < values.size(); ++i) std::cout << (i ? "," : "") << values[i];
        std::cout << ']';
    };
    array("level_pages", stats.level_pages); array("level_keys", stats.level_keys);
    field("leaf_pages", stats.leaf_pages); field("internal_pages", stats.internal_pages);
    field("leaf_min_keys", stats.leaf_min_occ); field("leaf_max_keys", stats.leaf_max_occ);
    field("leaf_avg_occ", stats.leaf_pages ? double(stats.total_keys) / (double(stats.leaf_pages) * kOrder) : 0.0);
    field("index_total_keys", stats.total_keys); field("heap_data_pages", stats.heap_pages);
    field("heap_bitmap_set", stats.heap_bitmap_set); field("heap_distinct_keys", stats.heap_records);
    field("heap_lock_residual", stats.heap_lock_residual); field("idx_only_keys", stats.idx_only_keys); field("heap_only_keys", stats.heap_only_keys);
    field("rid_mismatch", stats.rid_mismatch); field("dup_keys_idx", stats.dup_keys_idx); field("dup_keys_heap", stats.dup_keys_heap);
    field("bl_file_bytes", stats.bl_file_bytes); field("heap_file_bytes", stats.heap_file_bytes); field("fsm_file_bytes", stats.fsm_file_bytes);
    field("fsm_active_pages", stats.fsm_active_pages); field("fsm_retired_pages", stats.fsm_retired_pages); field("fsm_conservative_pages", stats.fsm_conservative_pages);
    field("fsm_capacity_policy", json_string("stored-category<=actual-category;page0=no-space;exact-aggregates"));
    field("model", json_string(options.ranges.empty() ? "none" : options.request_model ? "request_driver.value_for" : "request_driver.load_value_for"));
    field("model_expected_records", stats.model_expected_records); field("model_checked_records", stats.model_checked_records);
    field("model_records_verified", stats.model_checked_records); field("full_value_bytes_verified", stats.full_value_bytes_verified);
    field("content_digest_format", json_string(kDigestFormat));
    field("content_sha256", pass ? json_string(stats.content_sha256) : std::string("null"));
    std::cout << ",\"structure_errors\":[";
    for (size_t i = 0; i < errors.size(); ++i) std::cout << (i ? "," : "") << json_string(errors[i]);
    std::cout << "],\"pass\":" << (pass ? "true" : "false") << "}\nJSON_END\n";
}
} // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--abi-json") {
        try { abi_json(); return std::cout ? 0 : 2; }
        catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 2; }
    }
    Options options;
    Stats stats;
    std::vector<std::string> errors;
    bool parsed = false;
    try {
        options = parse_options(argc, argv);
        parsed = true;
        PageFile index(options.db_dir + "/" + options.table + "_bl");
        PageFile heap(options.db_dir + "/" + options.table);
        PageFile fsm(options.db_dir + "/" + options.table + "_fsm");
        stats.bl_file_bytes = index.bytes(); stats.heap_file_bytes = heap.bytes(); stats.fsm_file_bytes = fsm.bytes();
        std::vector<IndexEntry> entries = check_index(index, stats);
        HeapInfo heap_info = check_heap(heap, entries, options, stats);
        check_fsm(fsm, heap_info, stats);
        stats.content_sha256 = content_digest(entries);
        index.unchanged(); heap.unchanged(); fsm.unchanged();
        if (options.dump_content) dump_content(heap, heap_info, entries);
        index.unchanged(); heap.unchanged(); fsm.unchanged();
    } catch (const std::exception& error) {
        errors.push_back(error.what());
    } catch (...) {
        errors.push_back("unknown checker failure");
    }
    print_report(options, stats, errors);
    if (!std::cout) return 2;
    return errors.empty() ? 0 : parsed ? 1 : 2;
}
