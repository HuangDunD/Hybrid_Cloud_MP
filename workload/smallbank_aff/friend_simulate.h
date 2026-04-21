#pragma once

#include <cstdint>
#include <utility>
#include <vector>

void generate_friend_simulate_graph(
    std::vector<std::vector<std::pair<uint64_t, float>>>& adj_list,
    int num_users,
    int min_friends = 1,
    int max_friends = 3);

void generate_interleaved_hub_friend_graph(
    std::vector<std::vector<std::pair<uint64_t, float>>>& adj_list,
    int num_users,
    int group_count,
    int min_friends = 2,
    int max_friends = 4,
    int hub_count = 2,
    float hub_weight = 0.85f);
