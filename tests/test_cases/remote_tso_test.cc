#include "remote_page_table/durable_tso.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <butil/logging.h>

int main() {
    char path[] = "/tmp/remote-tso-XXXXXX";
    CHECK(mkdtemp(path));
    const auto child = fork();
    PCHECK(child >= 0);
    if (child == 0) {
        mvcc::DurableTso clock(path);
        CHECK_EQ(clock.Snapshot(0), 0);
        CHECK_EQ(clock.Commit(0), 1);
        CHECK_EQ(clock.Snapshot(0), 1);
        CHECK_EQ(clock.Snapshot(70000), 70000);
        CHECK_EQ(clock.Commit(70000), 70001);
        _exit(0);  // Crash with a partially used allocation block.
    }
    int status = 0;
    PCHECK(waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    {
        mvcc::DurableTso restarted(path);
        const auto after_restart = restarted.Snapshot(0);
        CHECK_GE(after_restart, 70001);
        constexpr int threads = 6, rounds = 2500;
        std::vector<uint64_t> commits(threads * rounds);
        std::vector<std::thread> workers;
        for (int i = 0; i < threads; ++i) {
            workers.emplace_back([&, i] {
                for (int j = 0; j < rounds; ++j) {
                    const auto s = restarted.Snapshot(0);
                    const auto c = restarted.Commit(s);
                    CHECK_GT(c, s);
                    CHECK_GE(restarted.Snapshot(0), c);
                    commits[i * rounds + j] = c;
                }
            });
        }
        for (auto& thread : workers) thread.join();
        std::sort(commits.begin(), commits.end());
        for (size_t i = 0; i < commits.size(); ++i)
            CHECK_EQ(commits[i], after_restart + i + 1);
        CHECK_EQ(restarted.Commit(400000), 400001);
    }
    std::filesystem::remove_all(path);
    return 0;
}
