#include "affinity/process_memory.h"

#include <cassert>
#include <cstdio>
#include <fstream>

int main() {
    const char* path = "/tmp/affinity_process_memory_status_test.txt";
    {
        std::ofstream out(path, std::ios::out | std::ios::trunc);
        out << "Name:\tcompute_server\n"
            << "VmSize:\t  123456 kB\n"
            << "VmRSS:\t   65432 kB\n"
            << "RssAnon:\t   60000 kB\n"
            << "RssFile:\t    5000 kB\n"
            << "RssShmem:\t     432 kB\n"
            << "VmData:\t   77777 kB\n";
    }

    affinity::ProcessMemorySnapshot snap;
    assert(affinity::ReadProcStatusMemory(path, &snap));
    assert(snap.vm_size_kb == 123456);
    assert(snap.vm_rss_kb == 65432);
    assert(snap.rss_anon_kb == 60000);
    assert(snap.rss_file_kb == 5000);
    assert(snap.rss_shmem_kb == 432);
    assert(snap.vm_data_kb == 77777);

    std::remove(path);
    return 0;
}
