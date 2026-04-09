#include "GPLM/global_LR_page_lock.h"

std::atomic<int64_t> global_notify_push_page_time_ns{0};
std::atomic<int64_t> global_notify_push_page_count{0};

ComputeServerInterface* LR_GlobalPageLock::compute_server_instance = nullptr;
