#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>

#if defined(__GLIBC__)
#include <malloc.h>
#endif

namespace affinity {

struct ProcessMemorySnapshot {
    uint64_t vm_size_kb = 0;
    uint64_t vm_rss_kb = 0;
    uint64_t rss_anon_kb = 0;
    uint64_t rss_file_kb = 0;
    uint64_t rss_shmem_kb = 0;
    uint64_t vm_data_kb = 0;

    uint64_t malloc_arena_bytes = 0;
    uint64_t malloc_ordblks = 0;
    uint64_t malloc_hblkhd_bytes = 0;
    uint64_t malloc_uordblks_bytes = 0;
    uint64_t malloc_fordblks_bytes = 0;
    uint64_t malloc_keepcost_bytes = 0;
};

inline bool ParseStatusKbLine(const std::string& line,
                              const char* key,
                              uint64_t* out) {
    const size_t key_len = std::strlen(key);
    if (line.compare(0, key_len, key) != 0) {
        return false;
    }
    const char* p = line.c_str() + key_len;
    while (*p == ' ' || *p == '\t') {
        ++p;
    }
    char* end = nullptr;
    const unsigned long long value = std::strtoull(p, &end, 10);
    if (end == p) {
        return false;
    }
    *out = static_cast<uint64_t>(value);
    return true;
}

inline bool ReadProcStatusMemory(const char* status_path,
                                 ProcessMemorySnapshot* out) {
    if (status_path == nullptr || out == nullptr) {
        return false;
    }
    std::ifstream in(status_path);
    if (!in.is_open()) {
        return false;
    }

    ProcessMemorySnapshot snap;
    std::string line;
    while (std::getline(in, line)) {
        ParseStatusKbLine(line, "VmSize:", &snap.vm_size_kb) ||
            ParseStatusKbLine(line, "VmRSS:", &snap.vm_rss_kb) ||
            ParseStatusKbLine(line, "RssAnon:", &snap.rss_anon_kb) ||
            ParseStatusKbLine(line, "RssFile:", &snap.rss_file_kb) ||
            ParseStatusKbLine(line, "RssShmem:", &snap.rss_shmem_kb) ||
            ParseStatusKbLine(line, "VmData:", &snap.vm_data_kb);
    }
    *out = snap;
    return true;
}

inline void FillMallocInfo(ProcessMemorySnapshot* out) {
    if (out == nullptr) {
        return;
    }
#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 33)
    const struct mallinfo2 info = mallinfo2();
    out->malloc_arena_bytes = static_cast<uint64_t>(info.arena);
    out->malloc_ordblks = static_cast<uint64_t>(info.ordblks);
    out->malloc_hblkhd_bytes = static_cast<uint64_t>(info.hblkhd);
    out->malloc_uordblks_bytes = static_cast<uint64_t>(info.uordblks);
    out->malloc_fordblks_bytes = static_cast<uint64_t>(info.fordblks);
    out->malloc_keepcost_bytes = static_cast<uint64_t>(info.keepcost);
#endif
#endif
}

inline ProcessMemorySnapshot ReadProcessMemorySnapshot() {
    ProcessMemorySnapshot snap;
    ReadProcStatusMemory("/proc/self/status", &snap);
    FillMallocInfo(&snap);
    return snap;
}

}  // namespace affinity
