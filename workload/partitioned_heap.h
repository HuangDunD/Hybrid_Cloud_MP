#pragma once
#include <algorithm>
#include "key_partition.h"
#include "record/rm_file_handle.h"
#include "storage/fsm_tree/s_fsm_tree.h"

namespace key_partition {
// During bulk load each owner appends to its own sequence of heap pages.
class HeapCursor {
public:
    Rid Insert(RmFileHandle& file, itemkey_t key, char* bytes, const Layout& layout,
               uint64_t partition_key, bool next_page = false) {
        if (!layout.ByKey())
            return next_page ? file.insert_nextpage_record(key, bytes, nullptr)
                             : file.insert_record(key, bytes, nullptr);
        if (rows_.empty()) rows_.resize(layout.nodes);
        const int owner = KeyOwner(partition_key, layout.nodes);
        const int capacity = file.file_hdr_.num_records_per_page_;
        const auto ordinal = rows_[owner]++;
        const auto local_page = ordinal / capacity;
        const auto target = 1 + (local_page / layout.pages * layout.nodes + owner) * layout.pages
                              + local_page % layout.pages;
        CHECK_LE(target, INT32_MAX);
        while (target >= file.file_hdr_.num_pages_) {
            auto page = file.create_new_page_handle(nullptr);
            file.buffer_pool_manager_->unpin_page(page.page->get_page_id(), true);
        }
        file.file_hdr_.first_free_page_no_ = target;
        const auto rid = file.insert_record(key, bytes, nullptr);
        CHECK(rid.page_no_ == target && rid.slot_no_ == ordinal % capacity);
        return rid;
    }

    void Finish(RmFileHandle& file, const Layout& layout, S_SecFSM* fsm = nullptr, int fsm_table = 0) {
        if (!layout.ByKey()) return;
        auto& header = file.file_hdr_;
        if (fsm) CHECK(fsm->initialize(fsm_table, header.num_pages_));
        header.first_free_page_no_ = RM_NO_PAGE;
        for (int page = 1; page < header.num_pages_; ++page) {
            const int chunk = (page - 1) / layout.pages;
            const int owner = chunk % layout.nodes;
            const int64_t local_page = int64_t(chunk / layout.nodes) * layout.pages + (page - 1) % layout.pages;
            const int capacity = header.num_records_per_page_;
            const auto used = std::clamp<int64_t>(rows_[owner] - local_page * capacity, 0, capacity);
            if (used == capacity) continue;
            auto handle = file.fetch_page_handle(page);
            CHECK_EQ(handle.page_hdr->num_records_, used);
            handle.page_hdr->next_free_page_no_ = header.first_free_page_no_;
            header.first_free_page_no_ = page;
            file.buffer_pool_manager_->unpin_page(handle.page->get_page_id(), true);
            if (fsm) fsm->update_page_space(page, (capacity - used) * (header.record_size_ + sizeof(itemkey_t)));
        }
    }
private:
    std::vector<int64_t> rows_;
};
}
