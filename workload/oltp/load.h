#pragma once
#include "catalog.h"
#include "schema.h"
#include "base/data_item.h"
#include "record/rm_manager.h"
#include "storage/blink_tree/blink_tree.h"
#include "storage/fsm_tree/s_fsm_tree.h"
#include "util/json_config.h"
#include <fstream>
#include <numeric>

namespace oltp {
inline void Load(const std::string& workload, RmManager* manager, bool shuffle) {
    auto json = JsonConfig::load_file("../../config/" + workload + "_config.json");
    auto config = json.get(workload);
    const bool tatp = workload == "tatp";
    const int64_t size = config.get(tatp ? "subscribers" : "table_size").get_int64();
    CHECK(size > 0 && size <= INT32_MAX);
    const uint32_t rows = size;
    std::vector<itemkey_t> keys(rows);
    std::iota(keys.begin(), keys.end(), 0);
    // Sysbench's primary-key ranges need contiguous on-disk keys.
    if (tatp && shuffle) {
        std::mt19937 rng(1931);
        std::shuffle(keys.begin(), keys.end(), rng);
    }
    const auto& names = Tables(workload);
    int rows_per_heap_page = 0;
    for (size_t i=0; i<names.size(); ++i) {
        const bool secondary = !tatp && i == 1;
        const int width = sizeof(DataItem) + (tatp ? sizeof(TatpRow) : secondary ? sizeof(SbIndexLeaf) : sizeof(SbRow));
        manager->create_file(names[i], width);
        manager->create_file(names[i] + "_fsm", width);
        auto file = manager->open_file(names[i]);
        S_BLinkIndexHandle index(manager->get_diskmanager(), manager->get_bufferPoolManager(), 10000+i, workload);
        std::ofstream mapping(names[i] + "_index.txt");
        auto insert = [&](itemkey_t key, auto value) {
            DataItem item(i, reinterpret_cast<uint8_t*>(&value), sizeof(value));
            std::vector<char> bytes(item.GetSerializeSize());
            item.Serialize(bytes.data());
            auto rid = secondary ? file->insert_nextpage_record(key,bytes.data(),nullptr)
                                 : file->insert_record(key,bytes.data(),nullptr);
            index.insert_entry(&key, rid);
            mapping << key << ' ' << rid.page_no_ << ' ' << rid.slot_no_ << '\n';
            return rid;
        };
        if (secondary) {
            for (uint32_t first=0; first<rows; first+=rows_per_heap_page) {
                SbIndexLeaf leaf{};
                leaf.count = std::min(uint32_t(rows_per_heap_page), rows-first);
                CHECK(leaf.count <= leaf.entries.size());
                for (uint32_t j=0; j<leaf.count; ++j)
                    leaf.entries[j] = {SbValue((first+j)*2654435761u).k, first+j};
                std::sort(leaf.entries.begin(), leaf.entries.begin()+leaf.count, IndexLess);
                auto page = first/rows_per_heap_page + 1;
                CHECK(insert(page,leaf).page_no_ == page);
            }
        } else {
            rows_per_heap_page = file->get_file_hdr().num_records_per_page_;
            for (auto key : keys) {
                if (tatp) insert(key,TatpInitial(i,key));
                else {
                    const auto rid = insert(key,SbValue(uint32_t(key)*2654435761u));
                    CHECK(rid.page_no_ == key/rows_per_heap_page+1);
                }
            }
        }
        mapping.close();
        CHECK(mapping.good());
        S_SecFSM fsm(manager->get_diskmanager(), manager->get_bufferPoolManager(), 20000+i, workload);
        const auto& header = file->get_file_hdr();
        CHECK(fsm.initialize(20000+i, header.num_pages_));
        const auto free_slots = (header.num_records_per_page_ - rows % header.num_records_per_page_) % header.num_records_per_page_;
        if (!secondary) fsm.update_page_space(header.num_pages_-1, free_slots * (width + sizeof(itemkey_t)));
        manager->close_file(file.get());
    }
}
}
