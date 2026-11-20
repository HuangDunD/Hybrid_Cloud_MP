#pragma once
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>
#include "undo/redo.h"
#include "storage/disk_manager.h"

namespace undo {
// 负责 undo 日志的回放和获取，因为后续可能调整，为了可拓展性，所以单独搞一个文件
class Store {
public:
    explicit Store(DiskManager& disk) : disk_(disk) {}
    ~Store();
    uint64_t Submit(std::shared_ptr<Redo> redo);  // applied count, including queued predecessors/duplicates
    PageBytes ReadPage(const PageKey& key, uint64_t required_lsn);
    bool Ready(const Ref& ref, const RowId& row, const mvcc::TxnId& creator,
               std::string_view successor); // nonblocking replay dependency, validates complete record
    void Sync();  // existing replay drain, never per append
    uint64_t ReadWaits() const { return waits_.load(); }
    bool WaitForReadWaits(uint64_t count, std::chrono::milliseconds timeout);

private:
    struct Progress {
        bthread::Mutex mutex;
        bthread::ConditionVariable changed;
        bool loaded = false, exists = false;
        uint64_t lsn = 0;
        std::map<uint64_t, std::vector<std::shared_ptr<Redo>>> pending;
    };
    std::shared_ptr<Progress> Get(const PageKey& key);
    int File(const PageKey& key, bool create);
    bool Load(const PageKey& key, PageBytes& bytes, Header* header = nullptr);
    DiskManager& disk_;
    std::mutex map_mutex_, file_mutex_, observer_mutex_;
    std::map<PageKey, std::shared_ptr<Progress>> progress_;
    std::map<std::string, int> files_;
    std::condition_variable observer_;
    std::atomic<uint64_t> waits_{0};
};
}  // namespace undo
