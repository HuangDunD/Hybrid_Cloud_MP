#include "graph_dump.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <string>
#include <tuple>
#include <vector>

namespace affinity {

bool DumpLocalGraphCsv(const LocalGraph& graph,
                       const std::string& path,
                       std::string* error) {
    if (path.empty()) {
        if (error != nullptr) *error = "empty graph dump path";
        return false;
    }

    std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> edges;
    edges.reserve(graph.EdgeCount());
    for (const auto& kv_u : graph.edges) {
        const uint64_t u = kv_u.first;
        for (const auto& kv_v : kv_u.second) {
            const uint64_t v = kv_v.first;
            if (u < v) {
                edges.emplace_back(u, v, kv_v.second);
            }
        }
    }
    std::sort(edges.begin(), edges.end());

    std::vector<std::tuple<uint64_t, int, uint32_t>> accesses;
    for (const auto& kv_t : graph.node_access) {
        for (const auto& kv_n : kv_t.second) {
            accesses.emplace_back(kv_t.first, kv_n.first, kv_n.second);
        }
    }
    std::sort(accesses.begin(), accesses.end());

    const std::string tmp_path = path + ".tmp";
    std::ofstream f(tmp_path, std::ios::out | std::ios::trunc);
    if (!f.is_open()) {
        if (error != nullptr) *error = "failed to open " + tmp_path;
        return false;
    }

    f << "record_type,tuple_id_a,tuple_id_b,weight,node_id,access_count,"
      << "epoch,total_samples\n";
    for (const auto& e : edges) {
        f << "edge," << std::get<0>(e) << ',' << std::get<1>(e) << ','
          << std::get<2>(e) << ",,0," << graph.epoch << ','
          << graph.total_samples << '\n';
    }
    for (const auto& a : accesses) {
        f << "access," << std::get<0>(a) << ",,0," << std::get<1>(a)
          << ',' << std::get<2>(a) << ',' << graph.epoch << ','
          << graph.total_samples << '\n';
    }
    f.close();
    if (!f) {
        if (error != nullptr) *error = "failed to write " + tmp_path;
        return false;
    }

    if (std::rename(tmp_path.c_str(), path.c_str()) != 0) {
        if (error != nullptr) *error = "failed to rename " + tmp_path;
        std::remove(tmp_path.c_str());
        return false;
    }
    return true;
}

}  // namespace affinity
