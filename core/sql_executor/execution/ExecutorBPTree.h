#pragma once

#include "ExecutorSeqScan.h"

// Both access paths consume the same visible-row iterator and residual predicates.
class BPTreeScanExecutor : public SeqScanExecutor {
public:
    BPTreeScanExecutor(DTX* dtx, const std::string& table, itemkey_t key,
                       const std::vector<Condition>& conditions)
        : SeqScanExecutor(dtx, table, conditions, key) {}
};
