#pragma once
#include "catalog.h"
#include "schema.h"
#include "partitioned_heap.h"
#include "base/data_item.h"
#include "record/rm_manager.h"
#include "storage/blink_tree/blink_tree.h"
#include "storage/fsm_tree/s_fsm_tree.h"
#include "util/json_config.h"
#include <fstream>
#include <numeric>

namespace oltp {
inline void Load(const std::string& workload, RmManager* manager, bool shuffle, const key_partition::Layout& layout = {}) {
    auto json = JsonConfig::load_file("../../config/" + workload + "_config.json");
    auto config = json.get(workload);
    const bool tatp = workload == "tatp";
    const int64_t size = config.get(tatp ? "subscribers" : "table_size").get_int64();
    CHECK(size > 0 && size <= INT32_MAX);
    const uint32_t rows = size;
    std::vector<itemkey_t> keys(rows);
    std::iota(keys.begin(), keys.end(), 0);
    // Page-routed Sysbench addresses its secondary leaves by heap page.
    if ((tatp || layout.ByKey()) && shuffle) {
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
        key_partition::HeapCursor cursor;
        auto insert = [&](itemkey_t key, auto value) {
            DataItem item(i, reinterpret_cast<uint8_t*>(&value), sizeof(value));
            std::vector<char> bytes(item.GetSerializeSize());
            item.Serialize(bytes.data());
            auto rid = cursor.Insert(*file, key, bytes.data(), layout, key, secondary);
            index.insert_entry(&key, rid);
            mapping << key << ' ' << rid.page_no_ << ' ' << rid.slot_no_ << '\n';
            return rid;
        };
        if (secondary) {
            auto insert_leaf = [&](itemkey_t first, uint32_t stride, uint32_t capacity, itemkey_t leaf_key) {
                SbIndexLeaf leaf{};
                leaf.count = std::min<itemkey_t>(capacity, 1 + (rows - 1 - first) / stride);
                CHECK_LE(leaf.count, leaf.entries.size());
                for (uint32_t j = 0; j < leaf.count; ++j) {
                    const uint32_t id = first + j * uint64_t(stride);
                    leaf.entries[j] = {SbValue(id * 2654435761u).k, id};
                }
                std::sort(leaf.entries.begin(), leaf.entries.begin() + leaf.count, IndexLess);
                return insert(leaf_key, leaf);
            };
            if (layout.ByKey()) {
                for (auto first : keys) {
                    if (SbLeafKey(first, layout.nodes) == first)
                        insert_leaf(first, layout.nodes, SbIndexLeaf::Capacity, first);
                }
            } else {
                for (uint32_t first = 0; first < rows; first += rows_per_heap_page) {
                    const auto page = first / rows_per_heap_page + 1;
                    CHECK_EQ(insert_leaf(first, 1, rows_per_heap_page, page).page_no_, page);
                }
            }
        } else {
            rows_per_heap_page = file->get_file_hdr().num_records_per_page_;
            for (auto key : keys) {
                if (tatp) insert(key,TatpInitial(i,key));
                else {
                    const auto rid = insert(key,SbValue(uint32_t(key)*2654435761u));
                    if (!layout.ByKey()) CHECK(rid.page_no_ == key/rows_per_heap_page+1);
                }
            }
        }
        mapping.close();
        CHECK(mapping.good());
        S_SecFSM fsm(manager->get_diskmanager(), manager->get_bufferPoolManager(), 20000+i, workload);
        const auto& header = file->get_file_hdr();
        if (layout.ByKey()) cursor.Finish(*file, layout, &fsm, 20000+i);
        else {
            CHECK(fsm.initialize(20000+i, header.num_pages_));
            const auto free_slots = (header.num_records_per_page_ - rows % header.num_records_per_page_) % header.num_records_per_page_;
            if (!secondary) fsm.update_page_space(header.num_pages_-1, free_slots * (width + sizeof(itemkey_t)));
        }
        manager->close_file(file.get());
    }
}
}
