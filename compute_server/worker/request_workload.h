#pragma once

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <limits>
#include <set>
#include <condition_variable>
#include <functional>
#include <mutex>
#include "dtx/dtx.h"
#include "util/bench_control.h"
#include "ycsb/ycsb_db.h"

namespace request_workload {
inline std::mutex identities_mutex;
inline std::set<uint64_t> identities;
inline std::set<std::string> request_identities;
inline std::mutex activity_mutex;
inline std::condition_variable activity_changed;
inline size_t active_transactions = 0;
inline bool quiescent = false;
inline bool snapshot_running = false;
inline bool unresolved_transaction = false;

struct ActiveTransaction {
    bool admitted = false;
    bool rejected_by_recovery = false;
    explicit ActiveTransaction(bool recovery_in_progress) {
        std::lock_guard<std::mutex> lock(activity_mutex);
        // P0 修复：恢复窗口内拒绝新事务 admission——恢复需要全局日志屏障
        //（存储侧 Undo 扫描窗口不应包含存活节点半截事务；见 UndoForFailedNode
        // 的 alive 过滤与 server.h RunIRRecoveryPhase3）。已 admission 的事务
        // 不受影响，其终局由 tainted/abort 机制处理；被拒请求由驱动重试。
        if (recovery_in_progress) {
            rejected_by_recovery = true;
        } else if (!quiescent && !snapshot_running && !unresolved_transaction) {
            ++active_transactions;
            admitted = true;
        }
    }
    ~ActiveTransaction() {
        if (admitted) {
            std::lock_guard<std::mutex> lock(activity_mutex);
            --active_transactions;
            activity_changed.notify_all();
        }
    }
};

struct StableRead {
    bool admitted = false;
    explicit StableRead(bool require_quiescent) {
        std::lock_guard<std::mutex> lock(activity_mutex);
        if (!snapshot_running && !unresolved_transaction && active_transactions == 0 &&
            (!require_quiescent || quiescent)) {
            snapshot_running = true;
            admitted = true;
        }
    }
    ~StableRead() {
        if (admitted) {
            std::lock_guard<std::mutex> lock(activity_mutex);
            snapshot_running = false;
            activity_changed.notify_all();
        }
    }
};

inline uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
inline const std::string& generation() {
    static const std::string value = bench_control::run_id() + ":" + std::to_string(getpid()) +
                                     ":" + std::to_string(now_ns());
    return value;
}
inline bool send_json(int fd, const JsonConfig& value, int timeout_ms = 30000) {
    const std::string text = value.dump() + "\n";
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    size_t offset = 0;
    while (offset < text.size()) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        if (remaining <= 0) return false;
        pollfd event{fd, POLLOUT, 0};
        const int ready = ::poll(&event, 1, static_cast<int>(remaining));
        if (ready < 0 && errno == EINTR) continue;
        if (ready <= 0 || (event.revents & (POLLERR | POLLHUP | POLLNVAL))) return false;
        const ssize_t n = ::send(fd, text.data() + offset, text.size() - offset, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        if (n <= 0) return false;
        offset += static_cast<size_t>(n);
    }
    return true;
}
inline std::string receive_line(int fd, int timeout_ms = 30000) {
    std::string text;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (text.size() <= 8 * 1024 * 1024) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        if (remaining <= 0) return "";
        pollfd event{fd, POLLIN, 0};
        const int ready = ::poll(&event, 1, static_cast<int>(remaining));
        if (ready < 0 && errno == EINTR) continue;
        if (ready <= 0 || (event.revents & (POLLERR | POLLNVAL))) return "";
        char c;
        const ssize_t n = ::recv(fd, &c, 1, MSG_DONTWAIT);
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        if (n <= 0) return "";
        if (c == '\n') return text;
        text.push_back(c);
    }
    throw std::runtime_error("workload frame exceeds budget");
}
inline std::string hex(const uint8_t* data, size_t length) {
    static const char* digits = "0123456789abcdef";
    std::string result(length * 2, '0');
    for (size_t i = 0; i < length; ++i) {
        result[2 * i] = digits[data[i] >> 4];
        result[2 * i + 1] = digits[data[i] & 15];
    }
    return result;
}
inline std::vector<uint8_t> unhex(const std::string& text) {
    if (text.size() != 2 * sizeof(ycsb_user_table_val)) throw std::runtime_error("invalid full value length");
    auto digit = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        throw std::runtime_error("invalid value hex");
    };
    std::vector<uint8_t> result(text.size() / 2);
    for (size_t i = 0; i < result.size(); ++i) result[i] = digit(text[2 * i]) * 16 + digit(text[2 * i + 1]);
    return result;
}
struct Operation {
    std::string kind;
    itemkey_t key;
    DataItemPtr item;
    std::vector<uint8_t> replacement;
    Rid rid = INDEX_NOT_FOUND;
    bool fetched = false;
};
inline JsonConfig response(const JsonConfig& request, const std::string& event) {
    auto result = JsonConfig::empty_dict("workload-response");
    result.insert_string("event", event);
    result.insert_string("run_id", bench_control::run_id());
    result.insert_string("generation", generation());
    result.insert_string("request_id", request.get("request_id").get_str());
    result.insert_uint64("tx_id", request.get("tx_id").get_uint64());
    result.insert_int64("node", request.get("node").get_int64());
    result.insert_uint64("time_ns", now_ns());
    return result;
}
inline void publish_response(const std::string& name, const JsonConfig& value) {
    const auto path = bench_control::directory() / (name + ".json");
    const auto temporary = bench_control::directory() / (name + ".tmp");
    if (std::filesystem::exists(path)) throw std::runtime_error("duplicate workload phase " + name);
    const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) throw std::runtime_error("cannot publish workload phase " + name);
    try {
        const std::string text = value.dump() + "\n";
        bench_control::write_all(fd, text.data(), text.size());
        if (::fsync(fd) != 0) throw std::runtime_error("workload phase fsync failed");
    } catch (...) { ::close(fd); throw; }
    ::close(fd);
    if (::rename(temporary.c_str(), path.c_str()) != 0)
        throw std::runtime_error("workload phase rename failed");
    const int dirfd = ::open(bench_control::directory().c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0) throw std::runtime_error("workload directory open failed");
    const int result = ::fsync(dirfd);
    ::close(dirfd);
    if (result != 0) throw std::runtime_error("workload directory fsync failed");
}
inline bool command_identity_matches(const JsonConfig& command, const JsonConfig& request) {
    if (!command.is_dict()) return false;
    if (request.get("generation").exists() &&
        (!command.get("generation").is_str() || !command.get("request_id").is_str() ||
         !command.get("node").is_int64() || !command.get("tx_id").is_uint64() ||
         !command.get("run_id").is_str())) return false;
    return command.get("run_id").get_str("") == bench_control::run_id() &&
        command.get("tx_id").get_uint64(0) == request.get("tx_id").get_uint64() &&
        (!command.get("generation").exists() || command.get("generation").get_str("") == generation()) &&
        (!command.get("node").exists() || command.get("node").get_int64(-1) == request.get("node").get_int64()) &&
        (!command.get("request_id").exists() ||
         command.get("request_id").get_str("") == request.get("request_id").get_str());
}
inline JsonConfig operation_results(const std::vector<Operation>& operations) {
    auto results = JsonConfig::empty_array("results");
    for (const auto& op : operations) {
        auto result = JsonConfig::empty_dict("operation");
        result.insert_string("op", op.kind);
        result.insert_uint64("key", op.key);
        result.insert_bool("found", op.fetched);
        result.insert_int64("rid_page", op.rid.page_no_);
        result.insert_int64("rid_slot", op.rid.slot_no_);
        if (op.kind == "READ" && op.fetched)
            result.insert_string("value_hex", hex(op.item->value, op.item->value_size));
        results.push_back_dict(result);
    }
    return results;
}
struct ExecutionControl {
    DTX* dtx;
    int fd;
    const JsonConfig& request;
    bool step_mode;
    size_t completed_ops = 0;
    std::string interruption;

    bool stop(const std::string& decision, const std::string& error) {
        interruption = decision;
        if (dtx->workload_error.empty()) dtx->workload_error = error;
        return false;
    }

    bool after_step(size_t completed) {
        completed_ops = completed;
        try {
            if (step_mode) {
                auto step = response(request, "step");
                step.insert_uint64("completed_ops", completed);
                if (!send_json(fd, step, 5000)) return stop("DISCONNECTED", "CLIENT_DISCONNECTED");
            } else {
                char byte;
                const ssize_t available = ::recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
                if (available < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return true;
                if (available <= 0) return stop("DISCONNECTED", "CLIENT_DISCONNECTED");
            }
            const std::string line = receive_line(fd, 5000);
            if (line.empty()) {
                char byte;
                const ssize_t available = ::recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
                return stop("DISCONNECTED", available == 0 || (available < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
                    ? "CLIENT_DISCONNECTED" : "EXECUTION_CONTROL_TIMEOUT");
            }
            const auto command = JsonConfig::load(line, "execution-control");
            if (!command_identity_matches(command, request))
                return stop("REJECTED", "EXECUTION_CONTROL_IDENTITY_MISMATCH");
            const std::string decision = command.get("decision").get_str("");
            if (decision == "CANCEL") return stop("CANCEL", "CANCELLED");
            if (step_mode && decision == "CONTINUE") return true;
            return stop("REJECTED", "INVALID_EXECUTION_CONTROL");
        } catch (const std::exception&) {
            return stop("REJECTED", "INVALID_EXECUTION_CONTROL");
        }
    }
};

inline void run(DTX* dtx, coro_yield_t& yield, int worker, int node) {
    if (!bench_control::enabled() || SYSTEM_MODE != 1 || WORKLOAD_MODE != 2)
        throw std::runtime_error("request workload only supports supervised YCSB lazy");
    const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listener < 0) throw std::runtime_error("workload socket failed");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || ::listen(listener, 16) != 0)
        throw std::runtime_error("workload listener failed");
    socklen_t address_length = sizeof(address);
    if (::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_length) != 0)
        throw std::runtime_error("workload listener address failed");
    auto ready = JsonConfig::empty_dict("worker");
    ready.insert_int64("worker", worker);
    ready.insert_int64("node", node);
    ready.insert_int64("port", ntohs(address.sin_port));
    ready.insert_string("generation", generation());
    bench_control::publish("worker-" + std::to_string(worker), ready);
    while (!bench_control::requested("workload-stop")) {
        pollfd pfd{listener, POLLIN, 0};
        if (::poll(&pfd, 1, 100) <= 0) continue;
        const int fd = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
        if (fd < 0) continue;
        timeval timeout{30, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        const auto text = receive_line(fd);
        if (text.empty()) { ::close(fd); continue; }
        const auto request = JsonConfig::load(text, "workload-request");
        if (!request.is_dict() || request.get("run_id").get_str("") != bench_control::run_id() ||
            !request.get("tx_id").is_uint64() || !request.get("node").is_int64() ||
            request.get("node").get_int64() != node || !request.get("request_id").is_str() ||
            (request.get("generation").exists() && request.get("generation").get_str("") != generation()))
            throw std::runtime_error("invalid workload request identity");
        const uint64_t tx = request.get("tx_id").get_uint64();
        const std::string terminal_name = "txn-" + std::to_string(tx);
        const std::string kind = request.get("kind").get_str("");
        if (kind == "QUIESCE" || kind == "RESUME") {
            auto result = response(request, kind == "QUIESCE" ? "quiescent" : "resumed");
            {
                std::unique_lock<std::mutex> lock(activity_mutex);
                bool ok = !snapshot_running && !unresolved_transaction;
                if (kind == "QUIESCE") {
                    quiescent = true;
                    ok = ok && activity_changed.wait_for(lock, std::chrono::seconds(30), [] {
                        return active_transactions == 0;
                    });
                    ok = ok && !unresolved_transaction && !snapshot_running;
                } else if (ok) {
                    quiescent = false;
                }
                result.insert_bool("ok", ok);
                result.insert_uint64("active_transactions", active_transactions);
                if (!ok) result.insert_string("error", "ACTIVE_OR_UNRESOLVED_TRANSACTION");
            }
            send_json(fd, result);
            ::close(fd);
            continue;
        }
        if (kind == "TREE_STATS") {
            StableRead stable(false);
            if (!stable.admitted) {
                auto result = response(request, "rejected");
                result.insert_string("error", "COMPUTE_VIEW_NOT_QUIESCENT");
                send_json(fd, result);
                ::close(fd);
                continue;
            }
            const auto stats = dtx->compute_server->bl_indexes[0]->StableStats();
            auto result = response(request, "tree-stats");
            result.insert_int64("root_page", stats.root_page);
            result.insert_int64("height", stats.height);
            result.insert_int64("leaf_pages", stats.leaf_pages);
            result.insert_uint64("total_keys", stats.total_keys);
            auto levels = JsonConfig::empty_array("level_pages");
            for (const int pages : stats.level_pages) levels.push_back_int64(pages);
            result.insert_array("level_pages", levels);
            send_json(fd, result);
            ::close(fd);
            continue;
        }
        if (kind == "TREE_SNAPSHOT") {
            StableRead stable(true);
            auto result = response(request, "tree-snapshot");
            if (!stable.admitted) {
                result.insert_bool("ok", false);
                result.insert_string("error", "COMPUTE_VIEW_NOT_QUIESCENT");
            } else {
                const auto destination = bench_control::directory() /
                    ("compute-snapshot-" + std::to_string(node) + "-" + std::to_string(now_ns()));
                result.insert_string("snapshot_dir", destination.string());
                try {
                    dtx->compute_server->bl_indexes[0]->StableSnapshot(destination.string());
                    result.insert_bool("ok", true);
                    result.insert_string("manifest", (destination / "manifest.json").string());
                    result.insert_uint64("active_transactions", 0);
                } catch (const std::exception& error) {
                    result.insert_bool("ok", false);
                    result.insert_string("error", error.what());
                }
            }
            send_json(fd, result);
            ::close(fd);
            continue;
        }
        if (kind == "STATUS") {
            const std::string stage = request.get("stage").get_str("terminal");
            if (stage != "terminal" && stage != "accepted" && stage != "executed") {
                auto rejected = response(request, "rejected");
                rejected.insert_string("error", "INVALID_STATUS_STAGE");
                send_json(fd, rejected);
            } else {
                const auto path = bench_control::directory() /
                    (terminal_name + (stage == "terminal" ? "" : "-" + stage) + ".json");
                if (std::filesystem::exists(path)) {
                    const auto saved = JsonConfig::load_file(path.string());
                    if (!command_identity_matches(saved, request)) {
                        auto rejected = response(request, "rejected");
                        rejected.insert_string("error", "STATUS_IDENTITY_MISMATCH");
                        send_json(fd, rejected);
                    } else send_json(fd, saved);
                } else {
                    auto pending = response(request, "pending");
                    send_json(fd, pending);
                }
            }
            ::close(fd);
            continue;
        }
        ActiveTransaction active(dtx->compute_server->IsRecoveryInProgress());
        if (!active.admitted) {
            auto result = response(request, "rejected");
            result.insert_string("error", active.rejected_by_recovery
                                          ? "RECOVERY_IN_PROGRESS" : "COMPUTE_VIEW_QUIESCENT");
            send_json(fd, result);
            ::close(fd);
            continue;
        }
        if ((tx >> 48) != static_cast<uint64_t>(node + 1) || (tx & ((uint64_t(1) << 48) - 1)) == 0)
            throw std::runtime_error("transaction node prefix mismatch");
        std::string duplicate_error;
        {
            std::lock_guard<std::mutex> lock(identities_mutex);
            const std::string id = request.get("request_id").get_str();
            if (identities.count(tx)) duplicate_error = "DUPLICATE_TRANSACTION_ID";
            else if (request_identities.count(id)) duplicate_error = "DUPLICATE_REQUEST_ID";
            else {
                identities.insert(tx);
                request_identities.insert(id);
            }
        }
        if (!duplicate_error.empty()) {
            auto rejected = response(request, "rejected");
            rejected.insert_string("error", duplicate_error);
            send_json(fd, rejected);
            ::close(fd);
            continue;
        }
        const auto requested_ops = request.get("operations");
        if (!requested_ops.is_array() || requested_ops.size() == 0 || requested_ops.size() > 128)
            throw std::runtime_error("operation count outside workload budget");
        std::vector<Operation> operations;
        for (size_t i = 0; i < requested_ops.size(); ++i) {
            const auto input = requested_ops.get(i);
            Operation op;
            op.kind = input.get("op").get_str();
            op.key = input.get("key").get_uint64();
            if (op.kind != "READ" && op.kind != "UPDATE" && op.kind != "INSERT" && op.kind != "DELETE")
                throw std::runtime_error("unsupported workload operation");
            op.item = std::make_shared<DataItem>(table_id_t(0));
            if (op.kind == "INSERT" || op.kind == "UPDATE") op.replacement = unhex(input.get("value_hex").get_str());
            if (op.kind == "INSERT") {
                op.item = std::make_shared<DataItem>(table_id_t(0), sizeof(ycsb_user_table_val));
                memcpy(op.item->value, op.replacement.data(), op.replacement.size());
            }
            operations.emplace_back(std::move(op));
        }
        auto accepted = response(request, "accepted");
        publish_response(terminal_name + "-accepted", accepted);
        const bool connected = send_json(fd, accepted);
        dtx->TxBegin(tx);
        for (const auto& op : operations) {
            if (op.kind == "READ") dtx->AddToReadOnlySet(op.item, op.key);
            if (op.kind == "UPDATE") dtx->AddToReadWriteSet(op.item, op.key);
            if (op.kind == "INSERT") dtx->AddToInsertSet(op.item, op.key);
            if (op.kind == "DELETE") dtx->AddToDeleteSet(op.item, op.key);
        }
        ExecutionControl execution{dtx, fd, request, request.get("step_mode").get_bool(false)};
        dtx->workload_step = [&](size_t completed) { return execution.after_step(completed); };
        struct StepGuard {
            DTX* dtx;
            ~StepGuard() { dtx->workload_step = {}; }
        } step_guard{dtx};
        bool ok = false;
        // decision 提升到 try 外：异常处理需要区分"提交已确认后发布失败"
        // 与"执行/提交阶段失败"（P0 事务终局契约）
        std::string decision;
        try {
            if (connected) ok = dtx->TxExe(yield, false);
            else execution.stop("DISCONNECTED", "CLIENT_DISCONNECTED");
            for (auto& op : operations) {
                for (const auto* set : {&dtx->read_only_set, &dtx->read_write_set, &dtx->insert_set, &dtx->delete_set}) {
                    for (const auto& item : *set) {
                        if (item.second.item_ptr == op.item) {
                            op.fetched = item.second.is_fetched;
                            if (op.fetched) op.rid = dtx->compute_server->get_rid_from_blink(0, op.key);
                        }
                    }
                }
                if ((op.kind == "UPDATE" || op.kind == "READ" || op.kind == "DELETE") && !op.fetched && ok) {
                    // 串行契约（docs §22）：读/改/删命中不存在键都是服务端确定性 NOT_FOUND
                    // 拒绝，统计为 abort。UPDATE/DELETE 已在 TxExe 内置；READ 走
                    // TX_VAL_NOTFOUND + erase 路径（服务 YCSB 读语义），在适配层转换。
                    ok = false;
                    if (dtx->workload_error.empty()) dtx->workload_error = "NOT_FOUND";
                }
                if (op.kind == "UPDATE" && op.fetched)
                    memcpy(op.item->value, op.replacement.data(), op.replacement.size());
            }
            auto executed = response(request, "executed");
            executed.insert_bool("ok", ok);
            executed.insert_string("error", dtx->workload_error);
            executed.insert_array("results", operation_results(operations));
            executed.insert_uint64("completed_ops", execution.completed_ops);
            publish_response(terminal_name + "-executed", executed);
            const bool replied = send_json(fd, executed);
            decision = execution.interruption;
            if (decision.empty() && connected && replied) {
                const std::string line = receive_line(fd);
                if (!line.empty()) {
                    const auto command = JsonConfig::load(line, "decision");
                    if (!command_identity_matches(command, request)) {
                        decision = "REJECTED";
                        dtx->workload_error = "DECISION_IDENTITY_MISMATCH";
                    } else {
                        decision = command.get("decision").get_str("");
                        if (decision != "COMMIT" && decision != "ROLLBACK") {
                            decision = "REJECTED";
                            dtx->workload_error = "INVALID_DECISION";
                        }
                    }
                }
            }
            bool committed = false;
            if (ok && decision == "COMMIT") committed = dtx->TxCommit(yield);
            else dtx->TxAbortWorkLoad(yield);
            auto terminal = response(request, "terminal");
            terminal.insert_string("outcome", dtx->tx_status == TXStatus::TX_UNKNOWN ? "UNKNOWN" :
                                   committed ? "CONFIRMED_COMMITTED" : "CONFIRMED_ABORTED");
            terminal.insert_string("decision", decision.empty() ? "DISCONNECTED" : decision);
            terminal.insert_uint64("completed_ops", execution.completed_ops);
            terminal.insert_string("error", dtx->workload_error);
            terminal.insert_uint64("end_ticket", dtx->commit_log_ticket);
            terminal.insert_array("results", operation_results(operations));
            terminal.insert_dict("accepted", accepted);
            terminal.insert_dict("executed", executed);
            publish_response(terminal_name, terminal);
            send_json(fd, terminal);
            ::close(fd);
            if (dtx->tx_status == TXStatus::TX_UNKNOWN) throw std::runtime_error("unresolved workload terminal");
        } catch (const std::exception& error) {
            // R2 缺陷修复（r2-fault-small-002 compute_B SIGABRT 的根因）：
            // 原实现此处 throw; 重抛——异常逃逸线程入口导致 std::terminate
            // 杀死整个计算节点。事务执行异常（如恢复窗口内 READ 防御校验
            // 失败"stale RID/uncommitted row"）时事务必然未走到提交，仍在
            // 协程上下文内（catch 未离开协程），应干净回滚释放页锁并归类
            // 为确定性 CONFIRMED_ABORTED：防御检查失败是事务失败，不是
            // 节点失败。仅当事务状态确属 TX_UNKNOWN（如 admission 结果
            // 未知）或回滚自身失败时，才保留 UNKNOWN 语义并停止该 worker
            // 接受新事务（unresolved_transaction），但进程保持存活。
            //
            // P0 修复（事务终局契约）：本 catch 同时覆盖 TxCommit 之后的
            // 响应发布路径（publish_response 可因 IO/duplicate 抛出）。
            // tx_status == TX_COMMIT 表示事务已确认提交——此时回滚已提交
            // 事务或发布 CONFIRMED_ABORTED 都是错误终局（已提交数据丢失 +
            // 虚假台账）。必须按实际状态发布 CONFIRMED_COMMITTED；发布
            // 自身再失败才落入 UNKNOWN 证据路径。
            if (dtx->tx_status == TXStatus::TX_COMMIT) {
                try {
                    auto terminal = response(request, "terminal");
                    terminal.insert_string("outcome", "CONFIRMED_COMMITTED");
                    terminal.insert_string("decision", decision.empty() ? "DISCONNECTED" : decision);
                    terminal.insert_uint64("completed_ops", execution.completed_ops);
                    terminal.insert_string("error", "post-commit publish failure: " +
                                                     std::string(error.what()));
                    terminal.insert_uint64("end_ticket", dtx->commit_log_ticket);
                    terminal.insert_array("results", operation_results(operations));
                    terminal.insert_dict("accepted", accepted);
                    publish_response(terminal_name, terminal);
                    send_json(fd, terminal);
                    ::close(fd);
                    continue;
                } catch (...) {
                    // 发布失败：落入下方 UNKNOWN 证据路径（提交已发生但
                    // 终局未能送达/持久化——结局未知，绝不降级为 ABORTED）
                }
            } else if (dtx->tx_status != TXStatus::TX_UNKNOWN) {
                try {
                    dtx->TxAbortWorkLoad(yield);
                    // 回滚后复查：TxAbortWorkLoad 途中刷新失败可能把状态
                    // 置为 TX_UNKNOWN（数据已写页但持久化未确认）——此时
                    // 不得发布 CONFIRMED_ABORTED
                    if (dtx->tx_status == TXStatus::TX_UNKNOWN) {
                        throw std::runtime_error("rollback persistence unknown");
                    }
                    auto terminal = response(request, "terminal");
                    auto executed = response(request, "executed");
                    executed.insert_bool("ok", false);
                    executed.insert_string("error", error.what());
                    executed.insert_array("results", operation_results(operations));
                    executed.insert_uint64("completed_ops", execution.completed_ops);
                    terminal.insert_string("outcome", "CONFIRMED_ABORTED");
                    terminal.insert_string("decision", "DISCONNECTED");
                    terminal.insert_uint64("completed_ops", execution.completed_ops);
                    terminal.insert_string("error", error.what());
                    terminal.insert_uint64("end_ticket", dtx->commit_log_ticket);
                    terminal.insert_array("results", operation_results(operations));
                    terminal.insert_dict("accepted", accepted);
                    terminal.insert_dict("executed", executed);
                    publish_response(terminal_name, terminal);
                    send_json(fd, terminal);
                    ::close(fd);
                    continue;
                } catch (...) {
                    // 回滚自身失败/回滚后状态未知：落入下方 UNKNOWN 路径
                }
            }
            {
                std::lock_guard<std::mutex> lock(activity_mutex);
                unresolved_transaction = true;
            }
            const auto path = bench_control::directory() / (terminal_name + ".json");
            if (!std::filesystem::exists(path)) {
                auto terminal = response(request, "terminal");
                terminal.insert_string("outcome", "UNKNOWN");
                terminal.insert_string("error", error.what());
                terminal.insert_uint64("end_ticket", dtx->commit_log_ticket);
                terminal.insert_dict("accepted", accepted);
                publish_response(terminal_name, terminal);
                send_json(fd, terminal);
                ::close(fd);
            }
            // 不再重抛：UNKNOWN 证据已持久化，unresolved_transaction 已
            // 阻止该 worker 接受新事务；进程存活以继续提供 STATUS/控制服务
        }
    }
    ::close(listener);
}
}
