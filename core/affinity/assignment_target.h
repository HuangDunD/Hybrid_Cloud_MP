// Demand-aware target selection for affinity assignments.
#pragma once

#include <cstdint>
#include <unordered_map>

namespace affinity {

inline bool IsValidAssignmentNode(int node_id, int n_ranks) {
    return node_id >= 0 && (n_ranks <= 0 || node_id < n_ranks);
}

inline uint32_t AssignmentAccessCount(
    const std::unordered_map<int, uint32_t>& by_node,
    int node_id) {
    auto it = by_node.find(node_id);
    return it == by_node.end() ? 0 : it->second;
}

inline int ChooseAssignmentTarget(
    int current_node,
    int partition_node,
    const std::unordered_map<int, uint32_t>& by_node,
    int n_ranks) {
    if (!IsValidAssignmentNode(partition_node, n_ranks)) {
        return current_node;
    }
    if (by_node.empty()) {
        return partition_node;
    }

    uint64_t total = 0;
    int best_node = -1;
    uint32_t best_count = 0;
    uint32_t second_count = 0;
    for (const auto& kv : by_node) {
        const int node_id = kv.first;
        const uint32_t count = kv.second;
        if (!IsValidAssignmentNode(node_id, n_ranks) || count == 0) {
            continue;
        }
        total += count;
        if (count > best_count) {
            second_count = best_count;
            best_count = count;
            best_node = node_id;
        } else if (count > second_count) {
            second_count = count;
        }
    }
    if (best_node < 0 || total == 0) {
        return partition_node;
    }

    const bool clear_demand_winner =
        best_count >= 2 &&
        best_count > second_count &&
        static_cast<uint64_t>(best_count) * 100 >= total * 55;

    int candidate = clear_demand_winner ? best_node : partition_node;
    if (!IsValidAssignmentNode(candidate, n_ranks)) {
        return current_node;
    }
    if (candidate == current_node) {
        return current_node;
    }

    const uint32_t current_count =
        AssignmentAccessCount(by_node, current_node);
    const uint32_t candidate_count =
        AssignmentAccessCount(by_node, candidate);
    if (candidate_count <= current_count) {
        return current_node;
    }
    if (!clear_demand_winner && candidate_count < current_count + 2) {
        return current_node;
    }
    return candidate;
}

}  // namespace affinity
