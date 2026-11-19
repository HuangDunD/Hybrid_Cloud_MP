#pragma once

#include <atomic>
#include <mutex>
#include "storage/fsm_tree/s_fsm_tree.h"

class ComputeServer;

class SecFSM {
public:
    SecFSM(ComputeServer* server, table_id_t table);
    bool initialize(table_id_t table);
    uint32_t find_free_page(uint32_t min_space_needed);
    void update_page_space(uint32_t page_id, uint32_t free_space);
    uint32_t get_page_space(uint32_t page_id);

private:
    void load_metadata();
    uint32_t leaf_for(uint32_t heap_page) const;
    void publish_root(uint32_t heap, uint32_t child, uint32_t parent, uint8_t value);

    ComputeServer* server_;
    table_id_t table_;
    std::once_flag init_;
    S_FSMMetaData meta_{};
    // A candidate only. The heap bitmap under X remains authoritative.
    std::atomic<uint64_t> candidate_{0};
};
