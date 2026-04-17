#include "sidecar_supervisor.h"

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <mutex>
#include <unordered_map>
#include <sstream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "config.h"

namespace affinity {

namespace {

std::mutex g_mtx;
pid_t g_mpirun_pid = 0;           // 0 == no child; matches kill(0,...) being a no-op
std::once_flag g_register_once;

std::vector<std::string> split_ws(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream iss(s);
    std::string tok;
    while (iss >> tok) out.push_back(std::move(tok));
    return out;
}

bool write_hostfile(const std::string& path,
                    const std::vector<std::string>& ips) {
    std::ofstream f(path, std::ios::out | std::ios::trunc);
    if (!f.is_open()) return false;
    std::vector<std::string> order;
    std::unordered_map<std::string, int> slots_by_host;
    for (const auto& ip : ips) {
        if (slots_by_host.emplace(ip, 0).second) {
            order.push_back(ip);
        }
        slots_by_host[ip] += 1;
    }
    for (const auto& host : order) {
        f << host << " slots=" << slots_by_host[host] << "\n";
    }
    f.flush();
    return f.good();
}

// Async-signal-safe: only kill + raise. Must NOT lock g_mtx (atexit and
// signal-handler paths can interleave); reading g_mpirun_pid as a plain
// pid_t is acceptable on POSIX (sig_atomic_t-equivalent for ILP32/LP64).
extern "C" void sidecar_signal_handler(int signo) {
    pid_t pid = g_mpirun_pid;
    if (pid > 0) ::kill(-pid, SIGTERM);
    ::signal(signo, SIG_DFL);
    ::raise(signo);
}

extern "C" void sidecar_atexit_handler() {
    StopSidecars();
}

void close_inherited_fds() {
    // brpc sockets, leveldb file handles, etc. are NOT marked FD_CLOEXEC by
    // default. Without this, mpirun + ORTE pin them open until they die,
    // blocking port reuse on compute_server restart.
    long maxfd = ::sysconf(_SC_OPEN_MAX);
    if (maxfd < 0 || maxfd > 65536) maxfd = 65536;
    for (int fd = 3; fd < (int)maxfd; ++fd) ::close(fd);
}

}  // namespace

bool SpawnSidecarsIfLeader(const std::vector<std::string>& compute_ips,
                           int machine_id) {
    if (!enable_affinity)              return true;
    if (!affinity_auto_spawn_sidecar)  return true;
    if (ComputeNodeCount < 2)          return true;  // matches PartitionerLoop's no-op
    // remote_compute_nodes are numbered 0..N-1 sequentially in meta_manager.cc,
    // so machine_id == 0 is the smallest node_id (matches the shutdown-coordinator
    // convention at handler.cc::GenThreads).
    if (machine_id != 0)               return true;

    std::lock_guard<std::mutex> lk(g_mtx);
    if (g_mpirun_pid > 0) return true;  // already running

    if (!write_hostfile(affinity_sidecar_hostfile_path, compute_ips)) {
        std::fprintf(stderr,
                     "[affinity-supervisor] failed to write hostfile %s: %s\n",
                     affinity_sidecar_hostfile_path.c_str(),
                     std::strerror(errno));
        return false;
    }

    const std::string n_str = std::to_string(compute_ips.size());

    std::vector<std::string> argv_owned;
    argv_owned.push_back(affinity_sidecar_mpirun_bin);
    argv_owned.push_back("-np");
    argv_owned.push_back(n_str);
    argv_owned.push_back("--hostfile");
    argv_owned.push_back(affinity_sidecar_hostfile_path);
    if (!affinity_sidecar_mpirun_extra_args.empty()) {
        for (auto& s : split_ws(affinity_sidecar_mpirun_extra_args)) {
            argv_owned.push_back(std::move(s));
        }
    }
    argv_owned.push_back(affinity_sidecar_binary_path);
    argv_owned.push_back(affinity_sidecar_uds_path);

    std::vector<char*> argv;
    argv.reserve(argv_owned.size() + 1);
    for (auto& s : argv_owned) argv.push_back(s.data());
    argv.push_back(nullptr);

    std::fprintf(stderr,
                 "[affinity-supervisor] launching mpirun for %zu sidecar(s)"
                 "; uds=%s; bin=%s; log=%s\n",
                 compute_ips.size(),
                 affinity_sidecar_uds_path.c_str(),
                 affinity_sidecar_binary_path.c_str(),
                 affinity_sidecar_log_path.c_str());

    pid_t pid = ::fork();
    if (pid < 0) {
        std::fprintf(stderr,
                     "[affinity-supervisor] fork failed: %s\n",
                     std::strerror(errno));
        return false;
    }
    if (pid == 0) {
        ::setpgid(0, 0);

        // O_TRUNC: each compute_server restart starts a fresh log instead of
        // accumulating gigabytes of mpirun chatter across runs.
        int log_fd = ::open(affinity_sidecar_log_path.c_str(),
                            O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log_fd >= 0) {
            ::dup2(log_fd, STDOUT_FILENO);
            ::dup2(log_fd, STDERR_FILENO);
            if (log_fd > 2) ::close(log_fd);
        }
        close_inherited_fds();

        ::execvp(argv[0], argv.data());
        std::fprintf(stderr,
                     "[affinity-supervisor] execvp(%s) failed: %s\n",
                     argv[0], std::strerror(errno));
        _exit(127);
    }

    // Belt-and-suspenders: the parent also sets the child's pgid so kill(-pid,…)
    // is safe even if StopSidecars races the child's own setpgid call.
    // EACCES (child already exec'd) is benign.
    ::setpgid(pid, pid);

    g_mpirun_pid = pid;
    std::fprintf(stderr,
                 "[affinity-supervisor] mpirun child pid=%d (pgroup leader)\n",
                 (int)pid);

    std::call_once(g_register_once, [] {
        std::atexit(sidecar_atexit_handler);
        struct sigaction old_act{};
        struct sigaction new_act{};
        new_act.sa_handler = sidecar_signal_handler;
        ::sigemptyset(&new_act.sa_mask);
        new_act.sa_flags = 0;
        // Only override SIG_DFL — leave brpc/glog SIGSEGV/SIGABRT handlers alone
        // and don't clobber a user-installed handler.
        for (int signo : {SIGTERM, SIGINT, SIGHUP}) {
            if (::sigaction(signo, nullptr, &old_act) == 0 &&
                old_act.sa_handler == SIG_DFL) {
                ::sigaction(signo, &new_act, nullptr);
            }
        }
    });

    return true;
}

void StopSidecars() {
    pid_t pid;
    {
        std::lock_guard<std::mutex> lk(g_mtx);
        pid = g_mpirun_pid;
        g_mpirun_pid = 0;
    }
    if (pid <= 0) return;

    std::fprintf(stderr,
                 "[affinity-supervisor] SIGTERM mpirun pgroup pid=%d\n",
                 (int)pid);
    if (::kill(-pid, SIGTERM) < 0 && errno != ESRCH) {
        std::fprintf(stderr,
                     "[affinity-supervisor] kill(-%d, SIGTERM) failed: %s\n",
                     (int)pid, std::strerror(errno));
    }

    // Cap at 1.5s so systemd's default 5s SIGKILL escalation still has slack.
    // nanosleep is async-signal-safe; usleep is not.
    const struct timespec tick = {0, 100 * 1000 * 1000};  // 100ms
    for (int i = 0; i < 15; ++i) {
        int status = 0;
        pid_t r = ::waitpid(pid, &status, WNOHANG);
        if (r == pid || (r < 0 && errno == ECHILD)) return;
        ::nanosleep(&tick, nullptr);
    }
    ::kill(-pid, SIGKILL);
    ::waitpid(pid, nullptr, WNOHANG);
}

}  // namespace affinity
