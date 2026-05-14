#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>

namespace {

std::string ReadFile(const std::string& path) {
  std::ifstream in(path);
  if (!in.is_open()) {
    return {};
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

std::string LoadTimeseriesSource() {
  const char* env_root = std::getenv("HYBRID_CLOUD_MP_ROOT");
  if (env_root != nullptr) {
    const std::string text =
        ReadFile(std::string(env_root) + "/core/affinity/affinity_timeseries.cc");
    if (!text.empty()) {
      return text;
    }
  }

  char cwd_buf[4096] = {0};
  if (getcwd(cwd_buf, sizeof(cwd_buf)) == nullptr) {
    return {};
  }
  const std::string cwd(cwd_buf);
  const std::string suffix = "/core/affinity/affinity_timeseries.cc";
  const std::string candidates[] = {
      cwd + suffix,
      cwd + "/.." + suffix,
      cwd + "/../.." + suffix,
      cwd + "/../../.." + suffix,
  };
  for (const auto& candidate : candidates) {
    const std::string text = ReadFile(candidate);
    if (!text.empty()) {
      return text;
    }
  }
  return {};
}

bool Contains(const std::string& haystack, const std::string& needle) {
  return haystack.find(needle) != std::string::npos;
}

}  // namespace

int main() {
  const std::string source = LoadTimeseriesSource();
  assert(!source.empty());

  assert(Contains(source, "wait_log_flush_ns_delta"));
  assert(Contains(source, "total_edge_weight"));
  assert(Contains(source, "last_total_edge_weight.load()"));
  assert(Contains(source, "last_partition_access_total"));
  assert(Contains(source, "last_partition_best_access.load()"));
  assert(Contains(source, "last_partition_assigned_access.load()"));
  assert(Contains(source, "log_flush_storage_rpc_ns_delta"));
  assert(Contains(source, "ownership_transfer_ns_delta"));
  assert(Contains(source, "ownership_transfer_wait_push_page_ns_delta"));
  assert(Contains(source, "global_wait_log_flush_time_ns.load()"));
  assert(Contains(source, "ownership_transfer_time_total.load()"));

  std::cout << "affinity timeseries includes runtime wait breakdown\n";
  return 0;
}
