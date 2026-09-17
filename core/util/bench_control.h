#pragma once

#include <chrono>
#include <cstdlib>
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include "util/json_config.h"

namespace bench_control {
inline bool enabled() { return std::getenv("HCM_CONTROL_DIR") != nullptr; }
inline std::string run_id() {
    const char* p = std::getenv("HCM_RUN_ID");
    if (!p || !*p) throw std::runtime_error("missing HCM_RUN_ID");
    return p;
}
inline std::filesystem::path directory() {
    const char* p = std::getenv("HCM_CONTROL_DIR");
    if (!p || !std::filesystem::path(p).is_absolute())
        throw std::runtime_error("control directory must be absolute");
    return p;
}
inline void write_all(int fd, const char* p, size_t n) {
    while (n) {
        const ssize_t k = ::write(fd, p, n);
        if (k < 0 && errno == EINTR) continue;
        if (k <= 0) throw std::runtime_error("control write failed");
        p += k;
        n -= static_cast<size_t>(k);
    }
}
inline void publish(const std::string& name, JsonConfig data = JsonConfig::empty_dict("control")) {
    data.insert_string("run_id", run_id());
    data.insert_int64("pid", getpid());
    data.insert_string("state", name);
    data.insert_uint64("time_ns", std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    const auto path = directory() / (name + ".json");
    const auto tmp = directory() / (name + ".tmp");
    if (std::filesystem::exists(path)) throw std::runtime_error("duplicate control state " + name);
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) throw std::runtime_error("cannot create control state " + name);
    try {
        const std::string text = data.dump() + "\n";
        write_all(fd, text.data(), text.size());
        if (::fsync(fd) != 0) throw std::runtime_error("control fsync failed");
    } catch (...) { ::close(fd); throw; }
    ::close(fd);
    if (::rename(tmp.c_str(), path.c_str()) != 0) throw std::runtime_error("control rename failed");
    const int dirfd = ::open(directory().c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0) throw std::runtime_error("control directory open failed");
    const int rc = ::fsync(dirfd);
    ::close(dirfd);
    if (rc != 0) throw std::runtime_error("control directory fsync failed");
}
inline bool requested(const std::string& name) {
    const auto path = directory() / (name + ".request");
    if (!std::filesystem::exists(path)) return false;
    const auto request = JsonConfig::load_file(path.string());
    if (!request.is_dict() || request.get("run_id").get_str("") != run_id())
        throw std::runtime_error("control generation mismatch");
    return true;
}
inline void wait(const std::string& name) {
    const char* budget = std::getenv("HCM_CONTROL_TIMEOUT_SECONDS");
    const int seconds = budget ? std::stoi(budget) : 300;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    while (!requested(name)) {
        if (std::chrono::steady_clock::now() >= deadline)
            throw std::runtime_error("control deadline: " + name);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}
}
