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

std::string LoadSchedulerSource() {
  const char* env_root = std::getenv("HYBRID_CLOUD_MP_ROOT");
  if (env_root != nullptr) {
    const std::string text =
        ReadFile(std::string(env_root) + "/core/fiber/scheduler.cc");
    if (!text.empty()) {
      return text;
    }
  }

  char cwd_buf[4096] = {0};
  if (getcwd(cwd_buf, sizeof(cwd_buf)) == nullptr) {
    return {};
  }
  const std::string cwd(cwd_buf);
  const std::string suffix = "/core/fiber/scheduler.cc";
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

std::string ExtractRegion(const std::string& source,
                          const std::string& begin_marker,
                          const std::string& end_marker) {
  const size_t begin = source.find(begin_marker);
  assert(begin != std::string::npos);
  const size_t end = source.find(end_marker, begin + begin_marker.size());
  assert(end != std::string::npos);
  return source.substr(begin, end - begin);
}

bool Contains(const std::string& haystack, const std::string& needle) {
  return haystack.find(needle) != std::string::npos;
}

}  // namespace

int main() {
  const std::string source = LoadSchedulerSource();
  assert(!source.empty());

  const std::string sql_run_body =
      ExtractRegion(source, "void Scheduler::sql_run()", "void Scheduler::run()");

  assert(!Contains(sql_run_body, "Thread : "));
  assert(!Contains(sql_run_body, "std::cout"));
  assert(Contains(sql_run_body, "cb();"));
  assert(!Contains(sql_run_body, "cb_fiber->swapIn()"));

  const std::string push_page_run_body = ExtractRegion(
      source, "void Scheduler::push_page_run()", "void Scheduler::sql_run()");

  assert(Contains(push_page_run_body, "cb();"));
  assert(!Contains(push_page_run_body, "cb_fiber->swapIn()"));

  std::cout << "sql and push-page schedulers run callbacks directly\n";
  return 0;
}
