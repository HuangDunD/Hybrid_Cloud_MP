#include "smallbank_aff/friend_simulate.h"

#include <algorithm>
#include <numeric>
#include <random>
#include <unordered_set>

void generate_friend_simulate_graph(
    std::vector<std::vector<std::pair<uint64_t, float>>>& adj_list,
    int num_users,
    int min_friends,
    int max_friends) {
  adj_list.clear();
  if (num_users <= 1) {
    return;
  }

  min_friends = std::max(1, min_friends);
  max_friends = std::max(min_friends, max_friends);
  max_friends = std::min(max_friends, num_users - 1);
  min_friends = std::min(min_friends, max_friends);

  adj_list.resize(num_users);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> degree_dist(min_friends, max_friends);
  std::uniform_int_distribution<int> user_dist(0, num_users - 1);
  std::uniform_real_distribution<float> weight_dist(0.001f, 1.0f);

  for (int user = 0; user < num_users; ++user) {
    const int degree = degree_dist(gen);
    std::unordered_set<uint64_t> unique_friends;
    unique_friends.reserve(static_cast<size_t>(degree) * 2);

    while (static_cast<int>(unique_friends.size()) < degree) {
      const uint64_t candidate = static_cast<uint64_t>(user_dist(gen));
      if (candidate == static_cast<uint64_t>(user)) {
        continue;
      }
      unique_friends.insert(candidate);
    }

    auto& friends = adj_list[user];
    friends.reserve(unique_friends.size());

    float sum = 0.0f;
    for (uint64_t friend_id : unique_friends) {
      const float weight = weight_dist(gen);
      sum += weight;
      friends.emplace_back(friend_id, weight);
    }

    std::sort(friends.begin(), friends.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    if (sum <= 0.0f) {
      const float uniform = 1.0f / static_cast<float>(friends.size());
      for (auto& entry : friends) {
        entry.second = uniform;
      }
      continue;
    }

    float normalized_sum = 0.0f;
    for (size_t i = 0; i < friends.size(); ++i) {
      friends[i].second /= sum;
      if (i + 1 < friends.size()) {
        normalized_sum += friends[i].second;
      }
    }
    friends.back().second = std::max(0.0f, 1.0f - normalized_sum);
  }
}

void generate_interleaved_hub_friend_graph(
    std::vector<std::vector<std::pair<uint64_t, float>>>& adj_list,
    int num_users,
    int group_count,
    int min_friends,
    int max_friends,
    int hub_count,
    float hub_weight) {
  adj_list.clear();
  if (num_users <= 1) {
    return;
  }

  group_count = std::max(1, std::min(group_count, num_users));
  min_friends = std::max(1, min_friends);
  max_friends = std::max(min_friends, max_friends);
  max_friends = std::min(max_friends, num_users - 1);
  min_friends = std::min(min_friends, max_friends);
  hub_count = std::max(1, hub_count);
  hub_weight = std::max(0.0f, std::min(hub_weight, 1.0f));

  adj_list.resize(num_users);

  std::vector<std::vector<uint64_t>> group_hubs(group_count);
  for (int group = 0; group < group_count; ++group) {
    const int group_size =
        (num_users - 1 - group >= 0) ? ((num_users - 1 - group) / group_count + 1) : 0;
    const int hubs_this_group = std::max(1, std::min(hub_count, group_size));
    auto& hubs = group_hubs[group];
    hubs.reserve(static_cast<size_t>(hubs_this_group));
    for (int i = 0; i < hubs_this_group; ++i) {
      hubs.push_back(static_cast<uint64_t>(group + i * group_count));
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> degree_dist(min_friends, max_friends);

  for (int user = 0; user < num_users; ++user) {
    const int group = user % group_count;
    const int group_size =
        (num_users - 1 - group >= 0) ? ((num_users - 1 - group) / group_count + 1) : 0;
    if (group_size <= 1) {
      continue;
    }

    const int degree = std::min(degree_dist(gen), group_size - 1);
    auto& friends = adj_list[user];
    friends.reserve(static_cast<size_t>(degree));

    std::unordered_set<uint64_t> chosen;
    chosen.reserve(static_cast<size_t>(degree) * 2);

    const auto& hubs = group_hubs[group];
    for (uint64_t hub : hubs) {
      if (hub == static_cast<uint64_t>(user)) {
        continue;
      }
      chosen.insert(hub);
      if (static_cast<int>(chosen.size()) >= degree) {
        break;
      }
    }

    std::uniform_int_distribution<int> member_dist(0, group_size - 1);
    int attempts = 0;
    const int max_attempts = std::max(32, degree * 32);
    while (static_cast<int>(chosen.size()) < degree && attempts < max_attempts) {
      const uint64_t candidate =
          static_cast<uint64_t>(group + member_dist(gen) * group_count);
      ++attempts;
      if (candidate == static_cast<uint64_t>(user)) {
        continue;
      }
      chosen.insert(candidate);
    }

    if (chosen.empty()) {
      continue;
    }

    int hub_edges = 0;
    for (uint64_t friend_id : chosen) {
      const bool is_hub =
          std::find(hubs.begin(), hubs.end(), friend_id) != hubs.end();
      if (is_hub) {
        ++hub_edges;
      }
      friends.emplace_back(friend_id, is_hub ? 0.0f : 0.0f);
    }

    std::sort(friends.begin(), friends.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    const int total_edges = static_cast<int>(friends.size());
    const int non_hub_edges = total_edges - hub_edges;
    if (hub_edges == 0 || non_hub_edges == 0 || hub_weight <= 0.0f ||
        hub_weight >= 1.0f) {
      const float uniform = 1.0f / static_cast<float>(total_edges);
      for (auto& entry : friends) {
        entry.second = uniform;
      }
      continue;
    }

    const float per_hub = hub_weight / static_cast<float>(hub_edges);
    const float per_other =
        (1.0f - hub_weight) / static_cast<float>(non_hub_edges);
    float normalized_sum = 0.0f;
    for (size_t i = 0; i < friends.size(); ++i) {
      const bool is_hub =
          std::find(hubs.begin(), hubs.end(), friends[i].first) != hubs.end();
      friends[i].second = is_hub ? per_hub : per_other;
      if (i + 1 < friends.size()) {
        normalized_sum += friends[i].second;
      }
    }
    friends.back().second = std::max(0.0f, 1.0f - normalized_sum);
  }
}
