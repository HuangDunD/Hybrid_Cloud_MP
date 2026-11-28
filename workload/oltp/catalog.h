#pragma once
#include <string>
#include <vector>

namespace oltp {
constexpr int TatpMode = 6;
constexpr int SysbenchMode = 7;
inline bool Enabled(int mode) { return mode == TatpMode || mode == SysbenchMode; }
inline bool Enabled(const std::string& name) { return name == "tatp" || name == "sysbench"; }
inline const std::vector<std::string>& Tables(const std::string& name) {
    static const std::vector<std::string> tatp{"tatp_subscriber", "tatp_access_info",
        "tatp_special_facility", "tatp_call_forwarding"};
    static const std::vector<std::string> sysbench{"sysbench_sbtest1", "sysbench_k_index"};
    return name == "tatp" ? tatp : sysbench;
}
inline const std::vector<std::string>& Types(const std::string& name) {
    static const std::vector<std::string> tatp{"GetSubscriberData", "GetNewDestination", "GetAccessData",
        "UpdateSubscriberData", "UpdateLocation", "InsertCallForwarding", "DeleteCallForwarding"};
    static const std::vector<std::string> sysbench{"ReadOnly", "ReadWrite"};
    return name == "tatp" ? tatp : sysbench;
}

}
