#pragma once
#include "util/json_config.h"

// Table id
enum class TPCCTableType : uint64_t {
    kWarehouseTable = 0,
    kDistrictTable,
    kCustomerTable,
    kHistoryTable,
    kNewOrderTable,
    kOrderTable,
    kOrderLineTable,
    kItemTable,
    kStockTable,
    kCustomerIndexTable,
    kOrderIndexTable,
};

namespace tpcc_partition {
struct Dimensions {
    uint64_t districts, customers, stocks;

    static Dimensions Read() {
        auto count = [](const std::string& table) {
            return JsonConfig::load_file("../../workload/tpcc/tpcc_tables/" + table + ".json")
                .get("table").get("bkt_num").get_uint64();
        };
        return {count("district"), count("customer"), count("stock")};
    }

    // Inverse of the benchmark's Make*Key functions. Component IDs start at one.
    uint64_t PartitionKey(TPCCTableType table, uint64_t key) const {
        uint64_t district;
        switch (table) {
        case TPCCTableType::kWarehouseTable:
        case TPCCTableType::kItemTable:
            return key - 1;
        case TPCCTableType::kStockTable:
            return (key - 1) / stocks - 1;
        case TPCCTableType::kDistrictTable:
            district = key;
            break;
        case TPCCTableType::kCustomerTable:
        case TPCCTableType::kOrderTable:
        case TPCCTableType::kNewOrderTable:
        case TPCCTableType::kCustomerIndexTable:
            district = key >> 32;
            break;
        case TPCCTableType::kHistoryTable:
            district = key & ((1u << 20) - 1); // warehouse taking the payment
            break;
        case TPCCTableType::kOrderIndexTable:
            district = ((key >> 32) - 1) / customers;
            break;
        case TPCCTableType::kOrderLineTable:
            district = ((key - 1) / 15) / 10000000;
            break;
        default:
            CHECK(false) << "Unknown TPC-C table";
            return 0;
        }
        return (district - 1) / districts - 1;
    }
};
}
