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