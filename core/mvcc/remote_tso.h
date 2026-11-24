#pragma once

#include <cstdint>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/logging.h>
#include "remote_page_table/timestamp.pb.h"

namespace mvcc {

inline uint64_t GetSnapshotTs(brpc::Channel& remote, uint64_t floor = 0) {
    timestamp_service::TimeStampService_Stub stub(&remote);
    timestamp_service::MvccTimeRequest req;
    timestamp_service::MvccTimeReply reply;
    brpc::Controller cntl;
    req.set_floor(floor);
    stub.GetSnapshotTs(&cntl, &req, &reply, nullptr);
    CHECK(!cntl.Failed()) << cntl.ErrorText();
    CHECK_GE(reply.timestamp(), floor);
    return reply.timestamp();
}

inline uint64_t GetCommitTs(brpc::Channel& remote, uint64_t floor = 0) {
    timestamp_service::TimeStampService_Stub stub(&remote);
    timestamp_service::MvccTimeRequest req;
    timestamp_service::MvccTimeReply reply;
    brpc::Controller cntl;
    req.set_floor(floor);
    stub.GetCommitTs(&cntl, &req, &reply, nullptr);
    CHECK(!cntl.Failed()) << cntl.ErrorText();
    CHECK_GT(reply.timestamp(), floor);
    return reply.timestamp();
}

} // namespace mvcc
