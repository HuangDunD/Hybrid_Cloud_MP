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

std::string LoadBLinkSource() {
  const char* env_root = std::getenv("HYBRID_CLOUD_MP_ROOT");
  if (env_root != nullptr) {
    const std::string text =
        ReadFile(std::string(env_root) + "/core/index/bp_tree/blink/blink.cc");
    if (!text.empty()) {
      return text;
    }
  }

  char cwd_buf[4096] = {0};
  if (getcwd(cwd_buf, sizeof(cwd_buf)) == nullptr) {
    return {};
  }
  const std::string cwd(cwd_buf);
  const std::string suffix = "/core/index/bp_tree/blink/blink.cc";
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

std::string ExtractFunctionBody(const std::string& source,
                                const std::string& signature) {
  const size_t sig_pos = source.find(signature);
  assert(sig_pos != std::string::npos);
  const size_t open = source.find('{', sig_pos);
  assert(open != std::string::npos);
  int depth = 0;
  for (size_t i = open; i < source.size(); ++i) {
    if (source[i] == '{') {
      ++depth;
    } else if (source[i] == '}') {
      --depth;
      if (depth == 0) {
        return source.substr(open + 1, i - open - 1);
      }
    }
  }
  assert(false);
  return {};
}

bool ContainsAfter(const std::string& source, const std::string& first,
                   const std::string& second) {
  const size_t first_pos = source.find(first);
  if (first_pos == std::string::npos) {
    return false;
  }
  const size_t second_pos = source.find(second, first_pos + first.size());
  return second_pos != std::string::npos;
}

}  // namespace

int main() {
  const std::string source = LoadBLinkSource();
  assert(!source.empty());

  const std::string body = ExtractFunctionBody(
      source, "bool BLinkIndexHandle::checkIfDirectlyGetPage");

  assert(ContainsAfter(body,
                       "release_node(tar_leaf->get_page_no() , "
                       "BPOperation::SEARCH_OPERA);",
                       "delete tar_leaf;"));
  assert(ContainsAfter(body,
                       "key2leaf.erase(*key);",
                       "delete tar_leaf;"));

  std::cout << "BLink cache-hit search releases node handles\n";
  return 0;
}
