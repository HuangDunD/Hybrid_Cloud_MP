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

std::string LoadWorkerSource() {
  const char* env_root = std::getenv("HYBRID_CLOUD_MP_ROOT");
  if (env_root != nullptr) {
    const std::string text =
        ReadFile(std::string(env_root) + "/compute_server/worker/worker.cc");
    if (!text.empty()) {
      return text;
    }
  }

  char cwd_buf[4096] = {0};
  if (getcwd(cwd_buf, sizeof(cwd_buf)) == nullptr) {
    return {};
  }
  const std::string cwd(cwd_buf);
  const std::string suffix = "/compute_server/worker/worker.cc";
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
        return source.substr(open, i - open + 1);
      }
    }
  }
  assert(false && "function body did not close");
  return {};
}

bool Contains(const std::string& haystack, const std::string& needle) {
  return haystack.find(needle) != std::string::npos;
}

}  // namespace

int main() {
  const std::string source = LoadWorkerSource();
  assert(!source.empty());

  const std::string init_thread_body =
      ExtractFunctionBody(source, "void initThread(");

  assert(Contains(init_thread_body,
                  "const bool interactive_session = bench_name == kInteractiveThreadBenchName"));
  assert(Contains(init_thread_body, "if (interactive_session)"));
  assert(Contains(init_thread_body, "compute_server->get_storage_channel()"));
  assert(Contains(init_thread_body, "compute_server->get_pagetable_channel()"));
  assert(Contains(init_thread_body, "return;"));

  const size_t guard_pos = init_thread_body.find("if (interactive_session)");
  const size_t planner_pos = init_thread_body.find("std::make_shared<Planner>");
  const size_t zipfan_pos = init_thread_body.find("zipfan_gens = new");
  assert(guard_pos != std::string::npos);
  assert(planner_pos != std::string::npos);
  assert(zipfan_pos != std::string::npos);
  assert(guard_pos < planner_pos);
  assert(guard_pos < zipfan_pos);

  std::cout << "interactive initThread skips benchmark-only setup\n";
  return 0;
}
