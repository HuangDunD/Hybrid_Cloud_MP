#include "GPLM/global_LR_page_lock.h"

std::atomic<int64_t> global_notify_push_page_time_ns{0};
std::atomic<int64_t> global_notify_push_page_count{0};

ComputeServerInterface* LR_GlobalPageLock::compute_server_instance = nullptr;

bthread::Mutex LR_GlobalPageLock::lock_success_batch_mtx_;
std::unordered_map<node_id_t, LR_GlobalPageLock::LockSuccessBatchState>
    LR_GlobalPageLock::lock_success_batches_;
