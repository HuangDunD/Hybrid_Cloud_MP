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

std::string LoadServerSource() {
  const char* env_root = std::getenv("HYBRID_CLOUD_MP_ROOT");
  if (env_root != nullptr) {
    const std::string text =
        ReadFile(std::string(env_root) + "/compute_server/server.cc");
    if (!text.empty()) {
      return text;
    }
  }

  char cwd_buf[4096] = {0};
  if (getcwd(cwd_buf, sizeof(cwd_buf)) == nullptr) {
    return {};
  }
  const std::string cwd(cwd_buf);
  const std::string suffix = "/compute_server/server.cc";
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

std::string SliceFunction(const std::string& source,
                          const std::string& signature,
                          const std::string& next_signature) {
  const size_t start = source.find(signature);
  assert(start != std::string::npos);
  const size_t end = source.find(next_signature, start + signature.size());
  assert(end != std::string::npos);
  return source.substr(start, end - start);
}

}  // namespace

int main() {
  const std::string source = LoadServerSource();
  assert(!source.empty());

  const std::string notify_push_page =
      SliceFunction(source, "void ComputeNodeServiceImpl::NotifyPushPage",
                    "void ComputeNodeServiceImpl::Pending");
  const std::string remote_pending =
      SliceFunction(source, "void ComputeNodeServiceImpl::Pending",
                    "void ComputeNodeServiceImpl::PushPage");

  assert(Contains(notify_push_page,
                  "const int push_type = (table_id < 10000) ? 1 : 0;"));
  assert(Contains(notify_push_page,
                  "server->PushPageToOther(table_id, page_id, dest_node, true, "
                  "false, push_type);"));

  assert(Contains(remote_pending,
                  "const int push_type = (table_id < 10000) ? 1 : 0;"));
  assert(Contains(remote_pending,
                  "server->PushPageToOther(table_id, page_id, dest_node_id, "
                  "true, false, push_type);"));

  std::cout << "remote lazy push uses scheduler for user tables\n";
  return 0;
}
