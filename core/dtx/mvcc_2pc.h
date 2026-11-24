#pragma once
#include <string>
#include <butil/logging.h>
#include "config.h"
#include "compute_node/twoPC.pb.h"
#include "mvcc/protocol.h"

// Wire helpers shared by the coordinator (DTX) and the 2PC participant service.
// They only translate; validation of MVCC invariants stays in the callers.
namespace mvcc_2pc {
inline bool ValidContext(const twopc_service::MvccContext& wire) {
    if (wire.epoch().size() != 16 || wire.epoch() == std::string(16, '\0')) return false;
    if (wire.home() < 0 || wire.home() >= MaxComputeNodeCount) return false;
    return wire.local_id() > 0;
}
inline mvcc::Context DecodeContext(const twopc_service::MvccContext& wire) {
    CHECK(ValidContext(wire));
    return {{wire.epoch(), static_cast<mvcc::NodeId>(wire.home()), wire.local_id()},
            wire.snapshot_ts()};
}
inline void EncodeContext(const mvcc::Context& context, twopc_service::MvccContext* wire) {
    CHECK(context.id.epoch.size() == 16 && context.id.local > 0 &&
          context.id.home < MaxComputeNodeCount);
    wire->set_epoch(context.id.epoch);
    wire->set_home(static_cast<int32_t>(context.id.home));
    wire->set_local_id(context.id.local);
    wire->set_snapshot_ts(context.snapshot);
}
inline bool SameContext(const twopc_service::MvccContext& a, const twopc_service::MvccContext& b) {
    return a.epoch() == b.epoch() && a.home() == b.home() && a.local_id() == b.local_id() &&
           a.snapshot_ts() == b.snapshot_ts();
}
}  // namespace mvcc_2pc
