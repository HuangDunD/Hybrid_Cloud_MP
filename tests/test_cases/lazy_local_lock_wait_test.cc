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

std::string LoadLocalLockSource() {
  const char* env_root = std::getenv("HYBRID_CLOUD_MP_ROOT");
  if (env_root != nullptr) {
    const std::string text =
        ReadFile(std::string(env_root) + "/core/LPLM/local_LR_page_lock.h");
    if (!text.empty()) {
      return text;
    }
  }

  char cwd_buf[4096] = {0};
  if (getcwd(cwd_buf, sizeof(cwd_buf)) == nullptr) {
    return {};
  }
  const std::string cwd(cwd_buf);
  const std::string suffix = "/core/LPLM/local_LR_page_lock.h";
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
  const std::string source = LoadLocalLockSource();
  assert(!source.empty());

  const std::string lock_shared =
      SliceFunction(source, "bool LockShared()", "bool LockExclusive()");
  const std::string lock_exclusive =
      SliceFunction(source, "bool LockExclusive()", "// 这个函数是在 PushPage");

  assert(Contains(lock_shared,
                  "cv.wait(lock, [this] { return !is_granting && "
                  "!is_pending && !is_evicting; });"));
  assert(Contains(lock_exclusive,
                  "cv.wait(lock, [this] { return !is_granting && "
                  "!is_pending && !is_evicting; });"));
  assert(!Contains(lock_shared,
                   "if(is_granting || is_pending || is_evicting){\n"
                   "                // 当前节点已经有线程正在远程获取这个数据页的锁，其他线程无需再去远程获取锁\n"
                   "                // 其他节点正在远程申请这个数据页的锁, 为了防止饿死, 应阻塞而不授予锁\n"
                   "                mutex.unlock();"));

  assert(Contains(source, "void RemotePushPageSuccess()"));
  assert(Contains(source, "cv.notify_all(); // 通知等待的线程远程页面推送成功"));
  assert(Contains(source, "void RemoteNotifyLockSuccess(bool xlock, bool is_newest)"));
  assert(Contains(source, "cv.notify_all(); // 通知等待的线程远程锁成功"));
  assert(Contains(source, "void LockRemoteOK(node_id_t node_id)"));
  assert(Contains(source, "cv.notify_all();"));

  std::cout << "lazy local lock contention waits on condition variable\n";
  return 0;
}
