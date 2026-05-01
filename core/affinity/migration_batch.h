#pragma once

#include <cstddef>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/page.h"
#include "common.h"
#include "migration_planner.h"

namespace affinity {

struct ResolvedMigrationPlan {
    MigrationPlan plan;
    table_id_t table_id;
    itemkey_t item_key;
    Rid src_rid;
    size_t original_index;
};

struct MigrationGroup {
    table_id_t table_id;
    page_id_t src_page;
    int dst_node;
    std::vector<size_t> member_indices;
};

struct MigrationGroupKey {
    table_id_t table_id;
    page_id_t src_page;
    int dst_node;

    bool operator==(const MigrationGroupKey& other) const {
        return table_id == other.table_id &&
               src_page == other.src_page &&
               dst_node == other.dst_node;
    }
};

struct MigrationGroupKeyHash {
    size_t operator()(const MigrationGroupKey& key) const {
        size_t h = static_cast<size_t>(key.table_id);
        h = (h * 1315423911u) ^ static_cast<size_t>(key.src_page);
        h = (h * 1315423911u) ^ static_cast<size_t>(key.dst_node);
        return h;
    }
};

inline std::vector<MigrationGroup> BuildMigrationGroups(
    const std::vector<ResolvedMigrationPlan>& resolved) {
    std::vector<MigrationGroup> groups;
    std::unordered_map<MigrationGroupKey, size_t, MigrationGroupKeyHash> index;
    groups.reserve(resolved.size());
    index.reserve(resolved.size());

    for (size_t i = 0; i < resolved.size(); ++i) {
        const auto& p = resolved[i];
        const MigrationGroupKey key{p.table_id, p.src_rid.page_no_,
                                    p.plan.dst_node};
        auto it = index.find(key);
        if (it == index.end()) {
            const size_t group_idx = groups.size();
            index.emplace(key, group_idx);
            MigrationGroup group{};
            group.table_id = key.table_id;
            group.src_page = key.src_page;
            group.dst_node = key.dst_node;
            group.member_indices.push_back(i);
            groups.push_back(std::move(group));
        } else {
            groups[it->second].member_indices.push_back(i);
        }
    }
    return groups;
}

}  // namespace affinity
