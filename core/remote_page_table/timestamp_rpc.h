// author:huangdund

#pragma once
#include "GPLM/global_page_lock_table.h"
#include "GPLM/global_valid_table.h"
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>

#include <map>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>

#include "timestamp.pb.h"

namespace timestamp_service{
class TimeStampServiceImpl : public TimeStampService {
  public:
    TimeStampServiceImpl() : timestamp_(1), epoch_(GenerateEpoch()) {};

    virtual ~TimeStampServiceImpl(){};

    virtual void GetTimeStamp(::google::protobuf::RpcController* controller,
                    const ::timestamp_service::GetTimeStampRequest* request,
                    ::timestamp_service::GetTimeStampResponse* response,
                    ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        uint64_t t = timestamp_.fetch_add(1);
        response->set_timestamp(t);
        return;
    }

    virtual void GetEpoch(::google::protobuf::RpcController* controller,
                    const ::timestamp_service::GetEpochRequest* request,
                    ::timestamp_service::GetEpochResponse* response,
                    ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        response->set_epoch(epoch_);
    }

  private:
    static std::string GenerateEpoch() {
        std::random_device device;
        std::mt19937_64 generator(((uint64_t)device() << 32) ^ device());
        static constexpr char hex[] = "0123456789abcdef";
        std::string epoch(16, '0');
        for (char& c : epoch) c = hex[generator() & 0xfu];
        return epoch;
    }

    std::atomic<uint64_t> timestamp_;
    const std::string epoch_;
};
};
