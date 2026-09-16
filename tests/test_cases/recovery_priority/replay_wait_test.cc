#include <chrono>
#include <filesystem>
#include <iostream>
#include "storage/log_manager.h"

int main(int argc, char** argv) {
    if (argc < 2 || !std::filesystem::create_directory(argv[1])) {
        std::cerr << "provide a NEW isolated runtime directory\n";
        return 2;
    }
    std::filesystem::current_path(argv[1]);
    DiskManager disk;
    LogReplay replay(&disk, nullptr, "test");
    LogManager manager(&disk, &replay);
    BatchEndLogRecord commit(1, 0, 1);
    std::string bytes(commit.log_tot_len_, '\0');
    commit.serialize(bytes.data());
    manager.write_batch_log_to_disk(bytes);
    auto start = std::chrono::steady_clock::now();
    bool caught_up = replay.WaitReplayCaughtUp(100);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "{\"v2\":" << replay.IsV2Mode() << ",\"caught_up\":" << caught_up
              << ",\"wait_us\":" << us << ",\"persist_batch\":" << replay.get_persist_batch_id() << "}\n";
    bool expect_timeout = argc > 2 && std::string(argv[2]) == "--expect-timeout";
    return (caught_up != expect_timeout && replay.get_persist_batch_id() == 1) ? 0 : 1;
}
