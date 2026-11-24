#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <vector>

namespace page_cost {
enum class Kind : uint8_t { Read, Write, Upgrade };

// Node-local observations, independent of the transaction selector. Table arrays
// are initialized with the existing page-lock capacities before use.
class PageCostStats {
public:
    struct Average {
        double ms = 0;
        int64_t updated = 0;
        uint64_t samples = 0;
        void Add(double value, double alpha, int64_t now, double ttl_ms) {
            const bool fresh = samples && now >= updated && (now - updated) / 1e6 <= ttl_ms;
            ms = fresh ? (1 - alpha) * ms + alpha * value : value;
            updated = now;
            ++samples;
        }
    };
    struct Cost : Average {
        uint64_t total_ns = 0;
        void Add(uint64_t ns, double alpha, int64_t now, double ttl_ms) {
            total_ns += ns;
            Average::Add(ns / 1e6, alpha, now, ttl_ms);
        }
    };
    struct LazyCost : Cost {
        uint64_t local_lock_ns = 0;
        uint64_t log_wait_ns = 0;
        uint64_t log_wait_count = 0;
    };
    struct HoldStats {
        uint64_t completed_holds = 0;
        uint64_t completed_accesses = 0;
        uint64_t completed_reused_holds = 0;
        uint64_t max_completed_accesses = 0;
        uint64_t active_accesses = 0;
    };
    struct Page {
        std::array<LazyCost, 3> lazy; // Actual permission acquisition, including upgrades/storage.
        std::array<Cost, 2> local;   // Fetch completed without a permission request; may wait locally.
        std::array<std::array<Cost, 2>, 2> remote; // [read/write][success/conflict]
        HoldStats hold;
        // Preserve the old selector's broader acquisition/wait sampling semantics.
        std::array<Average, 3> acquisition;
    };
    static int64_t NowNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

private:
    struct Table {
        explicit Table(size_t n) : pages(n) {}
        std::vector<Page> pages;
        // SQL may extend a table after its initial lock array has been built.
        std::mutex extra_mutex;
        std::map<int, Page> extra;
    };
    std::array<std::unique_ptr<Table>, 10000> tables_;
    std::array<std::mutex, 64> partitions_;
    double alpha_ = 0.2, ttl_ms_ = 10000;
    // Only the legacy selector uses these node-wide fallbacks, not ordinary sampling.
    std::mutex fallback_mutex_;
    std::array<Average, 3> fallback_;
    std::array<Average, 2> remote_fallback_;

    std::mutex& Mutex(int table, int page) {
        return partitions_[(uint64_t(uint32_t(table)) * 131 + uint32_t(page)) % partitions_.size()];
    }
    Page& At(int table, int page) {
        assert(table >= 0 && table < int(tables_.size()) && tables_[table]);
        assert(page >= 0);
        auto& rows = *tables_[table];
        if (size_t(page) < rows.pages.size()) return rows.pages[page];
        // Callers hold this page's partition mutex. Map insertion keeps other
        // pages' references valid after the index mutex is released.
        std::lock_guard<std::mutex> guard(rows.extra_mutex);
        return rows.extra[page];
    }

    template <typename Fn>
    void ForEachPage(Fn visit) {
        for (int t = 0; t < int(tables_.size()); ++t) {
            if (!tables_[t]) continue;
            auto& rows = *tables_[t];
            for (size_t p = 0; p < rows.pages.size(); ++p)
                visit(t, p, Snapshot(t, p));
            std::vector<int> extra;
            {
                std::lock_guard<std::mutex> guard(rows.extra_mutex);
                for (const auto& entry : rows.extra) extra.push_back(entry.first);
            }
            for (int p : extra) visit(t, p, Snapshot(t, p));
        }
    }

public:
    // Call at a quiescent boundary. Retain table allocation and tuning parameters.
    void ResetPageCostStats() {
        for (int t = 0; t < int(tables_.size()); ++t) {
            if (!tables_[t]) continue;
            tables_[t]->extra.clear();
            for (size_t p = 0; p < TableCapacity(t); ++p) {
                std::lock_guard<std::mutex> guard(Mutex(t, p));
                At(t, p) = Page{};
            }
        }
        std::lock_guard<std::mutex> guard(fallback_mutex_);
        fallback_ = {};
        remote_fallback_ = {};
    }
    void Configure(double alpha, double ttl_ms) { alpha_ = alpha; ttl_ms_ = ttl_ms; }
    void InitTable(int table, size_t pages) {
        assert(table >= 0 && table < int(tables_.size()) && !tables_[table]);
        tables_[table] = std::make_unique<Table>(pages);
    }
    // DDL has excluded all accesses to this table. A later table may reuse its id.
    void DropTable(int table) { tables_[table].reset(); }
    size_t TableCapacity(int table) const {
        return tables_[table] ? tables_[table]->pages.size() : 0;
    }
    Page Snapshot(int table, int page) {
        std::lock_guard<std::mutex> guard(Mutex(table, page));
        return At(table, page);
    }
    // Called while the successful Fetch still holds its page latch. Commit
    // cleanup is excluded by the caller, as in the existing hold statistics.
    void RecordLazy(int table, int page, Kind kind, bool permission_request,
                    uint64_t ns, uint64_t local_ns, uint64_t log_ns) {
        const auto now = NowNs();
        std::lock_guard<std::mutex> guard(Mutex(table, page));
        auto& p = At(table, page);
        ++p.hold.active_accesses;
        if (permission_request) {
            auto& c = p.lazy[static_cast<size_t>(kind)];
            c.Add(ns, alpha_, now, ttl_ms_);
            c.local_lock_ns += local_ns;
            c.log_wait_ns += log_ns;
            c.log_wait_count += log_ns > 0;
        } else {
            p.local[kind == Kind::Read ? 0 : 1].Add(ns, alpha_, now, ttl_ms_);
        }
    }
    void EndHold(int table, int page) {
        std::lock_guard<std::mutex> guard(Mutex(table, page));
        auto& h = At(table, page).hold;
        if (!h.active_accesses) return;
        ++h.completed_holds;
        h.completed_accesses += h.active_accesses;
        h.completed_reused_holds += h.active_accesses > 1;
        h.max_completed_accesses = std::max(h.max_completed_accesses, h.active_accesses);
        h.active_accesses = 0;
    }
    void RecordRemote(int table, int page, bool write, bool conflict, uint64_t ns) {
        const auto now = NowNs();
        std::lock_guard<std::mutex> guard(Mutex(table, page));
        At(table, page).remote[write][conflict].Add(ns, alpha_, now, ttl_ms_);
    }
    void ObserveAcquisition(int table, int page, Kind kind, double ms, int64_t now) {
        if (!std::isfinite(ms) || ms < 0) return;
        {
            std::lock_guard<std::mutex> guard(Mutex(table, page));
            At(table, page).acquisition[static_cast<size_t>(kind)].Add(ms, alpha_, now, ttl_ms_);
        }
        std::lock_guard<std::mutex> guard(fallback_mutex_);
        fallback_[static_cast<size_t>(kind)].Add(ms, alpha_, now, ttl_ms_);
    }
    Average Acquisition(int table, int page, Kind kind) {
        std::lock_guard<std::mutex> guard(Mutex(table, page));
        return At(table, page).acquisition[static_cast<size_t>(kind)];
    }
    Average Fallback(Kind kind) {
        std::lock_guard<std::mutex> guard(fallback_mutex_);
        return fallback_[static_cast<size_t>(kind)];
    }
    void ObserveRemoteFallback(bool write, double ms, int64_t now) {
        if (!std::isfinite(ms) || ms < 0) return;
        std::lock_guard<std::mutex> guard(fallback_mutex_);
        remote_fallback_[write].Add(ms, alpha_, now, ttl_ms_);
    }
    Average RemoteFallback(bool write) {
        std::lock_guard<std::mutex> guard(fallback_mutex_);
        return remote_fallback_[write];
    }
    size_t AcquisitionEntries() {
        size_t n = 0;
        ForEachPage([&](int, int, const Page& row) {
            for (const auto& a : row.acquisition) n += a.samples > 0;
        });
        return n;
    }
    // Snapshot under a partition lock; file I/O happens after releasing it.
    void DumpCosts(std::ostream& out) {
        out << "table_id\tpage_id\toperation\taccess\toutcome\tcount\ttotal_ns\trecent_ms"
            << "\tlast_sample_ns\tlocal_lock_ns\tpage_transfer_log_wait_ns\tpage_transfer_log_wait_count\tscope\n";
        const char* kinds[] = {"read", "write", "upgrade"};
        ForEachPage([&](int t, int p, const Page& row) {
            auto emit = [&](const char* op, const char* access, const char* outcome, const Cost& c,
                            uint64_t local = 0, uint64_t log = 0, uint64_t log_count = 0) {
                if (!c.samples) return;
                out << t << '\t' << p << '\t' << op << '\t' << access << '\t' << outcome
                    << '\t' << c.samples << '\t' << c.total_ns << '\t' << c.ms << '\t' << c.updated
                    << '\t' << local << '\t' << log << '\t' << log_count << "\tmodel_history_including_prewarm\n";
            };
            for (size_t k = 0; k < row.lazy.size(); ++k) {
                const auto& c = row.lazy[k];
                emit("lazy_acquire", kinds[k], "success", c, c.local_lock_ns, c.log_wait_ns, c.log_wait_count);
            }
            for (size_t w = 0; w < 2; ++w) {
                emit("lazy_local", kinds[w], "success", row.local[w]);
                for (size_t conflict = 0; conflict < 2; ++conflict)
                    emit("remote_fetch", kinds[w], conflict ? "conflict" : "success", row.remote[w][conflict]);
            }
        });
    }
};
} // namespace page_cost
