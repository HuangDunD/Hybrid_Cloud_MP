#include "assignment_table.h"

#include <fstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "affinity_config.h"

namespace affinity {

namespace {

std::vector<std::string> SplitCsvLine(const std::string& line) {
    std::vector<std::string> fields;
    std::string cur;
    for (char c : line) {
        if (c == ',') {
            fields.push_back(cur);
            cur.clear();
        } else if (c != '\r') {
            cur.push_back(c);
        }
    }
    fields.push_back(cur);
    return fields;
}

bool ParseUint64(const std::string& raw, uint64_t* out) {
    if (raw.empty() || out == nullptr) return false;
    try {
        size_t idx = 0;
        const uint64_t value = std::stoull(raw, &idx, 10);
        if (idx != raw.size()) return false;
        *out = value;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseInt(const std::string& raw, int* out) {
    if (raw.empty() || out == nullptr) return false;
    try {
        size_t idx = 0;
        const int value = std::stoi(raw, &idx, 10);
        if (idx != raw.size()) return false;
        *out = value;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

AssignmentTable::LoadResult ErrorResult(const std::string& error) {
    AssignmentTable::LoadResult result;
    result.ok = false;
    result.rows_loaded = 0;
    result.error = error;
    return result;
}

}  // namespace

AssignmentTable::LoadResult AssignmentTable::LoadFromCsv(
    const std::string& path, int compute_node_count) {
    if (compute_node_count <= 0) {
        return ErrorResult("compute_node_count must be positive");
    }

    std::ifstream f(path);
    if (!f.is_open()) {
        return ErrorResult("failed to open " + path);
    }

    std::string line;
    if (!std::getline(f, line)) {
        return ErrorResult("empty csv");
    }
    if (line == "tuple_id,table_id,item_key,node_id\r") {
        line.pop_back();
    }
    if (line != "tuple_id,table_id,item_key,node_id") {
        return ErrorResult("unexpected csv header: " + line);
    }

    auto snap = std::make_shared<Snapshot>();
    snap->version = 1;
    size_t row_no = 1;
    while (std::getline(f, line)) {
        ++row_no;
        if (line.empty() || line == "\r") continue;

        const auto fields = SplitCsvLine(line);
        if (fields.size() != 4) {
            return ErrorResult("row " + std::to_string(row_no) +
                               ": expected 4 csv fields");
        }

        uint64_t tuple_id = 0;
        uint64_t table_id_raw = 0;
        uint64_t item_key = 0;
        int node_id = -1;
        if (!ParseUint64(fields[0], &tuple_id) ||
            !ParseUint64(fields[1], &table_id_raw) ||
            !ParseUint64(fields[2], &item_key) ||
            !ParseInt(fields[3], &node_id)) {
            return ErrorResult("row " + std::to_string(row_no) +
                               ": failed to parse numeric field");
        }
        if (table_id_raw > 0xFFFFull) {
            return ErrorResult("row " + std::to_string(row_no) +
                               ": table_id exceeds 16 bits");
        }
        if (node_id < 0 || node_id >= compute_node_count) {
            return ErrorResult("row " + std::to_string(row_no) +
                               ": node_id out of range");
        }
        const uint64_t expected =
            pack_tuple_id(static_cast<uint32_t>(table_id_raw), item_key);
        if (tuple_id != expected) {
            return ErrorResult("row " + std::to_string(row_no) +
                               ": tuple_id does not match table_id/item_key");
        }
        snap->map[tuple_id] = Entry{node_id, snap->version, 0};
    }

    const size_t loaded = snap->map.size();
    Replace(std::shared_ptr<const Snapshot>(std::move(snap)));

    LoadResult result;
    result.ok = true;
    result.rows_loaded = loaded;
    return result;
}

AssignmentTable& GetAssignmentTable() {
    static AssignmentTable g;
    return g;
}

}  // namespace affinity
