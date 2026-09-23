// author:huangdund

#pragma once
#include "GPLM/global_page_lock_table.h"
#include "GPLM/global_valid_table.h"
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>

#include <map>
#include <mutex>
#include <tuple>
#include "timestamp.pb.h"

namespace timestamp_service{
class TimeStampServiceImpl : public TimeStampService {
  public:
    TimeStampServiceImpl(){
        timestamp_ = 1;
    };

    virtual ~TimeStampServiceImpl(){};

    virtual void GetTimeStamp(::google::protobuf::RpcController* controller,
                    const ::timestamp_service::GetTimeStampRequest* request,
                    ::timestamp_service::GetTimeStampResponse* response,
                    ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        response->set_timestamp(timestamp_.fetch_add(1)); // fetch_add returns the old value
        return;
    }

    void WorkloadLock(::google::protobuf::RpcController* controller,
                      const WorkloadLockRequest* request, WorkloadLockResponse* response,
                      ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        // 第 15 层（R2 live fault）：按节点回收 workload key 锁。victim 被
        // SIGKILL 后其 in-flight 事务持有的 key 锁无人释放（owner 表原本
        // 无死节点回收），幸存节点恢复完成时发 evict_mode 请求清除该节点
        // 全部残留持有，防止后续请求确定性 KEY_CONFLICT。
        if (request->evict_mode()) {
            std::lock_guard<std::mutex> lock(workload_mutex_);
            size_t removed = 0;
            for (auto it = workload_owners_.begin(); it != workload_owners_.end();) {
                if (std::get<0>(it->second) == request->evict_node_id()) {
                    it = workload_owners_.erase(it);
                    ++removed;
                } else {
                    ++it;
                }
            }
            response->set_granted(true);
            LOG(WARNING) << "[IR Recovery] workload key evict for dead node "
                         << request->evict_node_id() << ": removed " << removed << " keys";
            return;
        }
        if (request->node_id() < 0 || request->tx_id() == 0 || request->keys_size() > 4096) {
            controller->SetFailed("invalid workload lock identity or key count");
            return;
        }
        const Owner owner{request->node_id(), request->tx_id(), request->generation()};
        std::lock_guard<std::mutex> lock(workload_mutex_);
        for (const auto& key : request->keys()) {
            const auto it = workload_owners_.find({key.table_id(), key.key()});
            if (it != workload_owners_.end() && it->second != owner) {
                response->set_granted(false);
                return;
            }
        }
        for (const auto& key : request->keys()) {
            const auto identity = std::make_pair(key.table_id(), key.key());
            if (request->release()) workload_owners_.erase(identity);
            else workload_owners_[identity] = owner;
        }
        response->set_granted(true);
    }

  private:
    using Owner = std::tuple<int32_t, uint64_t, std::string>;
    std::mutex workload_mutex_;
    std::map<std::pair<int32_t, uint64_t>, Owner> workload_owners_;
    std::atomic<uint64_t> timestamp_;
};
};