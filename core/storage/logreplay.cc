#include <algorithm>
#include <assert.h>
#include <ctime>
#include <iomanip>
#include <limits>
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <fcntl.h>
#include <sys/stat.h>

#include "logreplay.h"
#include "prepared_log_record.h"
#include "fsm_tree/s_fsm_tree.h"
#include "util/bitmap.h"

namespace {

uint8_t FsmSpaceToCategory(uint32_t free_space) {
    if (free_space == 0) return static_cast<uint8_t>(S_SpaceCategory::NO_SPACE);
    if (free_space >= PAGE_SIZE * 9 / 10) return static_cast<uint8_t>(S_SpaceCategory::EMPTY);
    if (free_space >= PAGE_SIZE * 2 / 3) return static_cast<uint8_t>(S_SpaceCategory::ALMOST_EMPTY);
    if (free_space >= PAGE_SIZE / 3) return static_cast<uint8_t>(S_SpaceCategory::HALF_FULL);
    if (free_space >= PAGE_SIZE / 10) return static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL);
    return static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL);
}

uint32_t GetParentIndex(uint32_t child) { return (child - 1) / 2; }
uint32_t GetLeftChild(uint32_t parent) { return 2 * parent + 1; }
uint32_t GetRightChild(uint32_t parent) { return 2 * parent + 2; }
bool IsLeafNode(const S_FSMPageData& page_data, uint32_t index) { return index >= page_data.header.leaf_start; }

void EnsureChildVector(S_FSMPageData& page_data) {
    if (page_data.header.page_type != S_FSMPageType::INTERNAL_PAGE) {
        page_data.child_page_ids.clear();
        return;
    }
    if (!page_data.child_page_ids.empty()) {
        return;
    }
    page_data.child_page_ids.reserve(page_data.header.child_count);
    for (uint32_t i = 0; i < page_data.header.child_count; ++i) {
        page_data.child_page_ids.push_back(page_data.header.first_child_page + i);
    }
}

uint32_t FindChildIndex(const S_FSMPageData& parent_page, uint32_t child_page_id) {
    if (parent_page.header.page_type != S_FSMPageType::INTERNAL_PAGE) {
        return std::numeric_limits<uint32_t>::max();
    }
    for (uint32_t i = 0; i < parent_page.child_page_ids.size(); ++i) {
        if (parent_page.child_page_ids[i] == child_page_id) {
            return i;
        }
    }
    if (parent_page.child_page_ids.empty() && parent_page.header.child_count > 0) {
        if (child_page_id >= parent_page.header.first_child_page &&
            child_page_id < parent_page.header.first_child_page + parent_page.header.child_count) {
            return child_page_id - parent_page.header.first_child_page;
        }
    }
    return std::numeric_limits<uint32_t>::max();
}

bool DeserializeFSMPage(S_FSMPageData& page_data, const char* buffer, uint32_t size) {
    if (size < sizeof(S_FSMPageHeader)) {
        return false;
    }

    S_FSMPageHeader header;
    std::memcpy(&header, buffer, sizeof(S_FSMPageHeader));

    if (header.magic_number != 0x46535047) {
        return false;
    }

    page_data.header = header;

    const size_t payload_bytes = size - sizeof(S_FSMPageHeader);
    const size_t child_bytes_needed = static_cast<size_t>(header.child_count) * sizeof(uint32_t);
    bool has_child_blob = (header.page_type == S_FSMPageType::INTERNAL_PAGE) &&
                         (payload_bytes >= header.node_count + child_bytes_needed);

    size_t node_bytes_limit = has_child_blob ? (payload_bytes - child_bytes_needed) : payload_bytes;
    const char* node_data = buffer + sizeof(S_FSMPageHeader);
    page_data.nodes.resize(std::min(static_cast<size_t>(header.node_count), node_bytes_limit));

    for (size_t i = 0; i < page_data.nodes.size(); ++i) {
        page_data.nodes[i] = S_FSMNode(static_cast<S_SpaceCategory>(static_cast<uint8_t>(node_data[i])));
    }

    const size_t node_bytes = page_data.nodes.size();
    size_t remaining = payload_bytes > node_bytes ? payload_bytes - node_bytes : 0;
    page_data.child_page_ids.clear();
    if (header.page_type == S_FSMPageType::INTERNAL_PAGE && header.child_count > 0) {
        if (has_child_blob && remaining >= child_bytes_needed) {
            page_data.child_page_ids.resize(header.child_count);
            const char* child_ptr = node_data + node_bytes;
            std::memcpy(page_data.child_page_ids.data(), child_ptr, child_bytes_needed);
        } else {
            page_data.child_page_ids.resize(header.child_count);
            for (uint32_t i = 0; i < header.child_count; ++i) {
                page_data.child_page_ids[i] = header.first_child_page + i;
            }
        }
    }

    page_data.is_loaded = true;
    page_data.is_dirty = false;
    return true;
}

bool SerializeFSMPage(S_FSMPageData& page_data, char* buffer, uint32_t size) {
    if (size < PAGE_SIZE) {
        return false;
    }

    if (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE) {
        EnsureChildVector(page_data);
        page_data.header.child_count = static_cast<uint32_t>(page_data.child_page_ids.size());
        page_data.header.first_child_page = page_data.child_page_ids.empty() ? 0 : page_data.child_page_ids.front();
    } else {
        page_data.child_page_ids.clear();
        page_data.header.child_count = 0;
        page_data.header.first_child_page = 0;
    }

    size_t child_bytes = (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE)
                             ? static_cast<size_t>(page_data.header.child_count) * sizeof(uint32_t)
                             : 0;
    size_t node_region_capacity = (size > sizeof(S_FSMPageHeader) + child_bytes)
                                      ? size - sizeof(S_FSMPageHeader) - child_bytes
                                      : 0;
    if (page_data.nodes.size() > node_region_capacity) {
        return false;
    }

    page_data.header.magic_number = 0x46535047;
    page_data.header.timestamp = static_cast<uint64_t>(time(nullptr));
    std::memcpy(buffer, &page_data.header, sizeof(S_FSMPageHeader));

    char* node_data = buffer + sizeof(S_FSMPageHeader);
    size_t node_bytes = std::min(page_data.nodes.size(), node_region_capacity);
    for (size_t i = 0; i < node_bytes; ++i) {
        node_data[i] = static_cast<char>(page_data.nodes[i].get_value());
        page_data.nodes[i].clear_dirty();
    }

    if (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE && page_data.header.child_count > 0) {
        char* child_ptr = node_data + node_bytes;
        std::memcpy(child_ptr, page_data.child_page_ids.data(), child_bytes);
    }

    return true;
}

bool LoadFSMPage(DiskManager* disk_manager, int fd, uint32_t page_id, S_FSMPageData& out) {
    char buffer[PAGE_SIZE];
    disk_manager->read_page(fd, page_id, buffer, PAGE_SIZE);
    return DeserializeFSMPage(out, buffer, PAGE_SIZE);
}

bool StoreFSMPage(DiskManager* disk_manager, int fd, uint32_t page_id, S_FSMPageData& page) {
    char buffer[PAGE_SIZE] = {0};
    if (!SerializeFSMPage(page, buffer, PAGE_SIZE)) {
        return false;
    }
    disk_manager->write_page(fd, page_id, buffer, PAGE_SIZE);
    return true;
}

bool RecomputeWithinPage(S_FSMPageData& page, uint32_t node_index) {
    uint8_t original_root = page.nodes.empty() ? 0 : page.nodes[0].get_value();
    while (node_index > 0) {
        uint32_t parent_index = GetParentIndex(node_index);
        uint32_t left_child = GetLeftChild(parent_index);
        uint32_t right_child = GetRightChild(parent_index);
        uint8_t left_value = (left_child < page.header.node_count) ? page.nodes[left_child].get_value() : 0;
        uint8_t right_value = (right_child < page.header.node_count) ? page.nodes[right_child].get_value() : 0;
        uint8_t new_parent_value = std::max(left_value, right_value);
        if (new_parent_value == page.nodes[parent_index].get_value()) {
            break;
        }
        page.nodes[parent_index].set_value(new_parent_value);
        node_index = parent_index;
    }
    return !page.nodes.empty() && page.nodes[0].get_value() != original_root;
}

using FsmPathEntry = std::pair<uint32_t, S_FSMPageData>;

bool BuildPathToLeaf(DiskManager* disk_manager, int fd, uint32_t page_id, uint32_t heap_page_id, std::vector<FsmPathEntry>& path) {
    S_FSMPageData page;
    if (!LoadFSMPage(disk_manager, fd, page_id, page)) {
        return false;
    }
    path.emplace_back(page_id, std::move(page));
    auto& current = path.back().second;
    if (current.header.page_type == S_FSMPageType::LEAF_PAGE) {
        bool manage = heap_page_id >= current.header.first_heap_page &&
                      heap_page_id < current.header.first_heap_page + current.header.heap_pages_count;
        if (manage) {
            return true;
        }
        path.pop_back();
        return false;
    }

    EnsureChildVector(current);
    for (uint32_t child_page_id : current.child_page_ids) {
        if (BuildPathToLeaf(disk_manager, fd, child_page_id, heap_page_id, path)) {
            return true;
        }
    }
    path.pop_back();
    return false;
}

bool ApplyFsmUpdate(DiskManager* disk_manager, int fd, uint32_t heap_page_id, uint32_t free_space) {
    char meta_buffer[PAGE_SIZE];
    disk_manager->read_page(fd, S_FSM_META_PAGE_ID, meta_buffer, PAGE_SIZE);
    S_FSMMetaData meta{};
    std::memcpy(&meta, meta_buffer, sizeof(S_FSMMetaData));
    if (meta.magic_number != 0x46534D54) {
        return false;
    }

    std::vector<FsmPathEntry> path;
    if (!BuildPathToLeaf(disk_manager, fd, meta.root_page_id, heap_page_id, path)) {
        return false;
    }

    auto& leaf_entry = path.back();
    auto& leaf_page = leaf_entry.second;
    uint32_t heap_offset = heap_page_id - leaf_page.header.first_heap_page;
    uint32_t leaf_index = leaf_page.header.leaf_start + heap_offset;
    if (leaf_index >= leaf_page.header.node_count) {
        return false;
    }

    uint8_t new_category = FsmSpaceToCategory(free_space);
    uint8_t old_category = leaf_page.nodes[leaf_index].get_value();
    if (new_category == old_category) {
        return true;
    }

    leaf_page.nodes[leaf_index].set_value(new_category);
    bool leaf_root_changed = RecomputeWithinPage(leaf_page, leaf_index);
    if (!StoreFSMPage(disk_manager, fd, leaf_entry.first, leaf_page)) {
        return false;
    }

    uint8_t child_root_value = leaf_page.nodes.empty() ? 0 : leaf_page.nodes[0].get_value();
    bool propagate = leaf_root_changed;
    for (int i = static_cast<int>(path.size()) - 2; i >= 0 && propagate; --i) {
        auto& parent_entry = path[static_cast<size_t>(i)];
        auto& parent_page = parent_entry.second;
        EnsureChildVector(parent_page);
        uint32_t child_offset = FindChildIndex(parent_page, path[static_cast<size_t>(i + 1)].first);
        if (child_offset == std::numeric_limits<uint32_t>::max()) {
            break;
        }
        uint32_t parent_node_index = parent_page.header.leaf_start + child_offset;
        if (parent_node_index >= parent_page.header.node_count) {
            break;
        }
        if (parent_page.nodes[parent_node_index].get_value() == child_root_value) {
            propagate = false;
            continue;
        }
        parent_page.nodes[parent_node_index].set_value(child_root_value);
        bool parent_root_changed = RecomputeWithinPage(parent_page, parent_node_index);
        if (!StoreFSMPage(disk_manager, fd, parent_entry.first, parent_page)) {
            return false;
        }
        child_root_value = parent_page.nodes.empty() ? 0 : parent_page.nodes[0].get_value();
        propagate = parent_root_changed;
    }
    return true;
}

}  // namespace

std::string LogReplay::ResolveTableName(table_id_t table_id, const char* table_name_ptr, size_t table_name_size) const {
    auto it = table_name_map_.find(table_id);
    if (it != table_name_map_.end()) {
        return it->second;
    }
    if (table_name_ptr != nullptr && table_name_size > 0) {
        return std::string(table_name_ptr, table_name_ptr + table_name_size);
    }
    LOG(FATAL) << "Cannot resolve table name for table_id " << table_id;
    return {};
}

int LogReplay::ResolveTableFd(table_id_t table_id, const char* table_name_ptr, size_t table_name_size) {
    const std::string table_name = ResolveTableName(table_id, table_name_ptr, table_name_size);
    std::lock_guard<std::mutex> guard(table_fd_mutex_);
    auto it = table_fd_cache_.find(table_id);
    if (it != table_fd_cache_.end()) {
        return it->second;
    }
    int fd = disk_manager_->open_file(table_name);
    table_fd_cache_[table_id] = fd;
    return fd;
}

void LogReplay::apply_single_log(LogRecord* log) {
    switch(log->log_type_) {
        case LogType::INSERT: {
            InsertLogRecord* insert_log = dynamic_cast<InsertLogRecord*>(log);

            std::string table_name(insert_log->table_name_, insert_log->table_name_ + insert_log->table_name_size_);
            if (mode == "SQL" && !sm_manager->db.is_table(table_name)){
                return;
            }
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0){
                break;
            }

            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
            if (insert_log->slot_no_ < 0 || insert_log->slot_no_ >= file_hdr.num_records_per_page_) {
               assert(false);
            }

            char buffer[PAGE_SIZE];
            disk_manager_->read_page(fd, insert_log->page_no_, buffer, PAGE_SIZE);

            auto* page_hdr = reinterpret_cast<RmPageHdr*>(buffer);
            const LLSN log_llsn = static_cast<LLSN>(insert_log->lsn_);
            if (page_hdr->LLSN_ >= log_llsn||log->prev_lsn_!=page_hdr->LLSN_) {
               assert(false);
            }
            
            char* bitmap = buffer + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            const int slot_no = insert_log->slot_no_;
            if (!Bitmap::is_set(bitmap, slot_no)) {
                Bitmap::set(bitmap, slot_no);
                page_hdr->num_records_++;
            }else {
                // BitMap 一定是 false
                assert(false);
            }

            // 要改三个地方，data_item + value + itemkey
            char *slots = bitmap + file_hdr.bitmap_size_;
            char* tuple = slots + slot_no * (file_hdr.record_size_ + sizeof(itemkey_t));

            itemkey_t* item_key = reinterpret_cast<itemkey_t*>(tuple);
            *item_key = insert_log->insert_value_.key_;


            memcpy(tuple + sizeof(itemkey_t) , insert_log->insert_value_.value_ , insert_log->insert_value_.value_size_);

            page_hdr->pre_LLSN_ = page_hdr->LLSN_;
            page_hdr->LLSN_ = log_llsn;

            // 写回到磁盘里
            disk_manager_->write_page(fd , insert_log->page_no_ , buffer , PAGE_SIZE);
            PublishPageApplied(table_name, insert_log->page_no_, log_llsn);
        } break;
        case LogType::DELETE: {
            DeleteLogRecord* delete_log = dynamic_cast<DeleteLogRecord*>(log);

            std::string table_name(delete_log->table_name_, delete_log->table_name_ + delete_log->table_name_size_);
            if (mode == "SQL" && !sm_manager->db.is_table(table_name)){
                return;
            }
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0){
                break;
            }

            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
            if (delete_log->slot_no_ < 0 || delete_log->slot_no_ >= file_hdr.num_records_per_page_) {
                assert(false);
            }

            char buffer[PAGE_SIZE];
            disk_manager_->read_page(fd, delete_log->page_no_, buffer, PAGE_SIZE);

            auto* page_hdr = reinterpret_cast<RmPageHdr*>(buffer);
            char* bitmap = buffer + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            const LLSN log_llsn = static_cast<LLSN>(delete_log->lsn_);

            if (page_hdr->LLSN_ >= log_llsn||log->prev_lsn_!=page_hdr->LLSN_) {
                // TODO
                assert(false);
            }

            // TODO：DeleteLog 的逻辑需要重新考虑下，这里先不搞了
            assert(Bitmap::is_set(bitmap , delete_log->slot_no_));
            Bitmap::reset(bitmap, delete_log->slot_no_);
            page_hdr->num_records_--;

            page_hdr->pre_LLSN_ = page_hdr->LLSN_;
            page_hdr->LLSN_ = log_llsn;
            
            char *slots = bitmap + file_hdr.bitmap_size_;
            char* tuple = slots + delete_log->slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
            DataItem *data_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(data_item->valid == 1);
            assert(data_item->lock == EXCLUSIVE_LOCKED);

            // 写回到存储
            disk_manager_->write_page(fd , delete_log->page_no_ , buffer , PAGE_SIZE);
            PublishPageApplied(table_name, delete_log->page_no_, log_llsn);

        } break;
        case LogType::UPDATE: {
            UpdateLogRecord* update_log = dynamic_cast<UpdateLogRecord*>(log);
            std::string table_name(update_log->table_name_, update_log->table_name_ + update_log->table_name_size_);
            if (mode == "SQL" && !sm_manager->db.is_table(table_name)){
                return;
            }
            
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0){
                break;
            }

            static thread_local std::unordered_map<int, RmFileHdr> file_hdr_cache;
            RmFileHdr file_hdr{};
            auto hdr_it = file_hdr_cache.find(fd);
            if (hdr_it != file_hdr_cache.end()) {
                file_hdr = hdr_it->second;
            } else {
                char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
                disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
                file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
                file_hdr_cache.emplace(fd, file_hdr);
            }
            if (update_log->rid_.slot_no_ < 0 || update_log->rid_.slot_no_ >= file_hdr.num_records_per_page_) {
                assert(false);
            }

            RmPageHdr page_hdr{};
            disk_manager_->read_page(fd, update_log->rid_.page_no_, reinterpret_cast<char*>(&page_hdr), sizeof(RmPageHdr));
            const LLSN log_llsn = static_cast<LLSN>(update_log->lsn_);

            if (page_hdr.LLSN_ >= log_llsn || log->prev_lsn_ != page_hdr.LLSN_) {
                assert(false);
            }

            page_hdr.pre_LLSN_ = page_hdr.LLSN_;
            page_hdr.LLSN_ = log_llsn;

            const int tuple_offset = sizeof(RmPageHdr) + OFFSET_PAGE_HDR +
                                     file_hdr.bitmap_size_ +
                                     update_log->rid_.slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
            const size_t tuple_bytes = sizeof(itemkey_t) + update_log->new_value_.value_size_;
            std::vector<char> tuple_buffer(tuple_bytes);
            std::memcpy(tuple_buffer.data(), &update_log->new_value_.key_, sizeof(itemkey_t));
            std::memcpy(tuple_buffer.data() + sizeof(itemkey_t), update_log->new_value_.value_, update_log->new_value_.value_size_);

            disk_manager_->update_value(fd, update_log->rid_.page_no_, tuple_offset, tuple_buffer.data(), static_cast<int>(tuple_bytes));
            disk_manager_->update_value(fd, update_log->rid_.page_no_, OFFSET_PAGE_HDR, reinterpret_cast<char*>(&page_hdr), sizeof(RmPageHdr));
            PublishPageApplied(table_name, update_log->rid_.page_no_, log_llsn);

            // {
            //     const DataItem* applied_item = reinterpret_cast<const DataItem*>(tuple_buffer.data() + sizeof(itemkey_t));
            //     LOG(INFO) << "Apply Update Log , table_name = " << table_name
            //               << " page_id = " << update_log->rid_.page_no_
            //               << " slot_no = " << update_log->rid_.slot_no_
            //               << " log lsn = " << log_llsn
            //               << " log prev_lsn = " << log->prev_lsn_
            //               << " commit_ts = " << applied_item->commitTimeStamp;
            // }
        } break;
        case LogType::NEWPAGE: {
            // 这里有点问题，需要拿到 tab_name，现在反正没用这个，先不管了
            assert(false);
            NewPageLogRecord* new_page_log = dynamic_cast<NewPageLogRecord*>(log);
            int fd = ResolveTableFd(new_page_log->table_id_, nullptr, 0);
            if (fd < 0) break;

            bool is_file = true;
           
            // 预读现有文件头信息，维护 free list 和 num_pages
            int file_num_pages = disk_manager_->get_fd2pageno(fd);
            int old_first_free = RM_NO_PAGE;
            if (is_file) {
                // 读取旧的 first_free_page_no，作为新链尾的 next
                char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
                disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
                old_first_free = *reinterpret_cast<int*>(page0_buf + OFFSET_FILE_HDR + OFFSET_FIRST_FREE_PAGE_NO);
            }

            page_id_t chain_head = RM_NO_PAGE;
            page_id_t prev_page = RM_NO_PAGE;

            for (int i = 0; i < new_page_log->request_pages_; ++i) {
                page_id_t page_no = disk_manager_->allocate_page(fd);

                char init_value[PAGE_SIZE];
                memset(init_value, 0, PAGE_SIZE);
                disk_manager_->write_page(fd, page_no, (const char*)&init_value, PAGE_SIZE);

                if (chain_head == RM_NO_PAGE) {
                    chain_head = page_no;
                }
                if (is_file) {
                    int next_free = RM_NO_PAGE;
                    disk_manager_->update_value(fd, page_no, OFFSET_NEXT_FREE_PAGE_NO,
                                                reinterpret_cast<char*>(&next_free), sizeof(int));
                    if (prev_page != RM_NO_PAGE) {
                        // 串联上一页
                        disk_manager_->update_value(fd, prev_page, OFFSET_NEXT_FREE_PAGE_NO,
                                                    reinterpret_cast<char*>(&page_no), sizeof(int));
                    }
                    prev_page = page_no;
                }
            }

            if (is_file && chain_head != RM_NO_PAGE) {
                // 将尾部 next 指向旧的 free head
                disk_manager_->update_value(fd, prev_page, OFFSET_NEXT_FREE_PAGE_NO,
                                            reinterpret_cast<char*>(&old_first_free), sizeof(int));

                // 更新文件头 first_free 指向新链头
                disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FILE_HDR + OFFSET_FIRST_FREE_PAGE_NO,
                                            reinterpret_cast<char*>(&chain_head), sizeof(int));

                // 更新 num_pages: 基于原 num_pages 加上申请数量
                int new_num_pages = file_num_pages + new_page_log->request_pages_;
                disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FILE_HDR + OFFSET_NUM_PAGES,
                                            reinterpret_cast<char*>(&new_num_pages), sizeof(int));
            }
        } break;
        case LogType::FSMUPDATE: {
            auto fsm_log = dynamic_cast<FSMUpdateLogRecord*>(log);
            if (fsm_log == nullptr) {
                break;
            }
            int fd = ResolveTableFd(fsm_log->table_id_, fsm_log->table_name_, fsm_log->table_name_size_);
            if (fd >= 0) {
                ApplyFsmUpdate(disk_manager_, fd, fsm_log->page_id_, fsm_log->free_space_);
            }
        } break;
        case LogType::PREPARED:
            // Durable vote metadata only. No page routing, page LSN or business
            // application. Replay does not authorize deleting vote evidence.
            break;
        case LogType::BATCHEND:
            break;
        case LogType::LOCK: {
            LockLogRecord* lock_log = dynamic_cast<LockLogRecord*>(log);

            std::string table_name(lock_log->table_name_, lock_log->table_name_ + lock_log->table_name_size_);
            if (mode == "SQL" && !sm_manager->db.is_table(table_name)){
                return;
            }
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0){
                break;
            }

            static thread_local std::unordered_map<int, RmFileHdr> file_hdr_cache;
            RmFileHdr file_hdr{};
            auto hdr_it = file_hdr_cache.find(fd);
            if (hdr_it != file_hdr_cache.end()) {
                file_hdr = hdr_it->second;
            } else {
                char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
                disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
                file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
                file_hdr_cache.emplace(fd, file_hdr);
            }

            if (lock_log->rid_.slot_no_ < 0 || lock_log->rid_.slot_no_ >= file_hdr.num_records_per_page_) {
                assert(false);
            }

            RmPageHdr page_hdr{};
            disk_manager_->read_page(fd, lock_log->rid_.page_no_, reinterpret_cast<char*>(&page_hdr), sizeof(RmPageHdr));

            const LLSN log_llsn = static_cast<LLSN>(lock_log->lsn_);
            const int slot_no = lock_log->rid_.slot_no_;

            if (page_hdr.LLSN_ >= log_llsn || log->prev_lsn_ != page_hdr.LLSN_) {
                assert(false);
            }

            page_hdr.pre_LLSN_ = page_hdr.LLSN_;
            page_hdr.LLSN_ = log_llsn;

            int bitmap_offset = sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            int tuple_offset = bitmap_offset + file_hdr.bitmap_size_ + slot_no * (file_hdr.record_size_ + sizeof(itemkey_t));
            int item_offset = tuple_offset + sizeof(itemkey_t);
            lock_t lock_type = lock_log->lock_type_;
            tx_id_t time_stamp = lock_log->timeStamp_;
            uint8_t holder_node = lock_log->holder_node_;
            uint8_t lock_owner_node = lock_log->lock_owner_node_;

            // 一次读出整个 DataItem，改完 lock + timeStamp 后整体写回，避免两次 update_value。
            // 注意：DataItem 含有指针成员，且其析构会 delete[] value，因此这里用裸字节缓冲，
            //      避免栈上 DataItem 析构时对从磁盘读出的“伪指针” value 调用 delete[] 而崩溃。
            char item_raw[sizeof(DataItem)];
            ssize_t br = pread(fd, item_raw, sizeof(DataItem), (off_t)lock_log->rid_.page_no_ * PAGE_SIZE + item_offset);
            if (br != sizeof(DataItem)) {
                LOG(ERROR) << "pread DataItem failed in LOCK replay";
                assert(false);
            }
            // 直接按字节偏移修改锁相关字段。holder_node / lock_owner_node 必须一起落盘：
            // 等待者按 lock_owner_node 找解锁节点，丢了这两个字段会把队列挂到错误节点。
            std::memcpy(item_raw + offsetof(DataItem, lock), &lock_type, sizeof(lock_t));
            std::memcpy(item_raw + offsetof(DataItem, timeStamp), &time_stamp, sizeof(tx_id_t));
            std::memcpy(item_raw + offsetof(DataItem, holder_node), &holder_node, sizeof(uint8_t));
            std::memcpy(item_raw + offsetof(DataItem, lock_owner_node), &lock_owner_node, sizeof(uint8_t));

            disk_manager_->update_value(fd, lock_log->rid_.page_no_, item_offset, item_raw, sizeof(DataItem));
            disk_manager_->update_value(fd, lock_log->rid_.page_no_, OFFSET_PAGE_HDR, reinterpret_cast<char*>(&page_hdr), sizeof(RmPageHdr));
            PublishPageApplied(table_name, lock_log->rid_.page_no_, log_llsn);

            // LOG(INFO) << "Apply Lock Log , table_name = " << table_name
            //           << " page_id = " << lock_log->rid_.page_no_
            //           << " slot_no = " << lock_log->rid_.slot_no_
            //           << " log lsn = " << log_llsn
            //           << " log prev_lsn = " << log->prev_lsn_
            //           << " lock = " << (int)lock_type
            //           << " ts = " << time_stamp;

        } break;
        default:
        break;
    }

}

void LogReplay::apply_undo_log(const LogRecord* log_record) {
    if (log_record == nullptr) {
        return;
    }

    switch (log_record->log_type_) {
        case LogType::UPDATE: {
            const UpdateLogRecord* update_log = dynamic_cast<const UpdateLogRecord*>(log_record);
            if (update_log == nullptr || !update_log->HasUndoPayload()) {
                return;
            }
            std::string table_name(update_log->table_name_, update_log->table_name_ + update_log->table_name_size_);
            int fd = disk_manager_->open_file(table_name);
            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
            const RmRecord& undo_image = update_log->old_value();
            const int slot_base = sizeof(RmPageHdr) + file_hdr.bitmap_size_ +
                                   update_log->rid_.slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
            const int value_offset = slot_base + static_cast<int>(sizeof(itemkey_t));
            disk_manager_->update_value(fd,
                                        update_log->rid_.page_no_,
                                        value_offset,
                                        undo_image.value_,
                                        undo_image.value_size_ * sizeof(char));
            break;
        }
        case LogType::DELETE: {
            const DeleteLogRecord* delete_log = dynamic_cast<const DeleteLogRecord*>(log_record);
            if (delete_log == nullptr || !delete_log->has_undo_meta_) {
                return;
            }
            int fd = ResolveTableFd(delete_log->table_id_, delete_log->table_name_, delete_log->table_name_size_);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FILE_HDR + OFFSET_FIRST_FREE_PAGE_NO, (char*)(&delete_log->undo_first_free_page_no_), sizeof(int));
            disk_manager_->update_value(fd, delete_log->page_no_, OFFSET_PAGE_HDR, (char*)&delete_log->undo_page_hdr_, sizeof(RmPageHdr));
            disk_manager_->update_value(fd, delete_log->page_no_, delete_log->bucket_offset_, const_cast<char*>(&delete_log->undo_bucket_value_), sizeof(char));
            PublishPageApplied(disk_manager_->get_file_name(fd), delete_log->page_no_,
                               delete_log->undo_page_hdr_.LLSN_);
            break;
        }
        case LogType::NEWPAGE: {
            // No explicit undo for page allocation requests; replay decides actual allocation.
            return;
        }
        default:
            break;
    }
}


namespace {
[[noreturn]] void ReplayError(const std::string& message) {
    LOG(ERROR) << "WAL replay: " << message;
    std::abort();
}
std::shared_ptr<LogRecord> DecodeRecord(const char* data, uint32_t size) {
    LogType type;
    memcpy(&type, data + OFFSET_LOG_TYPE, sizeof(type));
    std::shared_ptr<LogRecord> record;
    switch (type) {
        case LogType::BEGIN: record.reset(new BeginLogRecord); break;
        case LogType::ABORT: record.reset(new AbortLogRecord); break;
        case LogType::COMMIT: record.reset(new CommitLogRecord); break;
        case LogType::INSERT: record.reset(new InsertLogRecord); break;
        case LogType::UPDATE: record.reset(new UpdateLogRecord); break;
        case LogType::DELETE: record.reset(new DeleteLogRecord); break;
        case LogType::NEWPAGE: record.reset(new NewPageLogRecord); break;
        case LogType::FSMUPDATE: record.reset(new FSMUpdateLogRecord); break;
        case LogType::PREPARED: record.reset(new PreparedLogRecord); break;
        case LogType::BATCHEND: record.reset(new BatchEndLogRecord); break;
        case LogType::LOCK: record.reset(new LockLogRecord); break;
        default: ReplayError("unknown record type");
    }
    if (type == LogType::PREPARED) {
        if (!static_cast<PreparedLogRecord*>(record.get())->Decode(data, size))
            ReplayError("invalid PREPARED record");
    } else {
        record->deserialize(data);
    }
    return record;
}
}

std::string LogReplay::LogFilePath(node_id_t node) {
    return std::string(LOG_FILE_NAME) + "_" + std::to_string(node);
}

LogReplay::LogReplay(DiskManager* disk, SmManager* sm, const std::string m,
                     std::unordered_map<table_id_t, std::string> names)
    : disk_manager_(disk), sm_manager(sm), mode(m), table_name_map_(std::move(names)) {
    // Restart scans every existing node stream from its beginning. Page LLSNs
    // make redo idempotent; an unsynced replay cursor must never skip page redo.
    const std::string prefix = std::string(LOG_FILE_NAME) + "_";
    for (const auto& entry : std::filesystem::directory_iterator(".")) {
        const std::string name = entry.path().filename().string();
        if (!entry.is_regular_file() || name.compare(0, prefix.size(), prefix) != 0) continue;
        const std::string suffix = name.substr(prefix.size());
        if (suffix.empty() || suffix.find_first_not_of("0123456789") != std::string::npos) continue;
        const auto node = static_cast<node_id_t>(std::stoul(suffix));
        const int fd = open(name.c_str(), O_RDWR);
        if (fd < 0) ReplayError("cannot open " + name);
        // A crash may leave a partial final record. Preserve the complete
        // prefix and remove only that incomplete tail before accepting appends.
        const uint64_t length = entry.file_size();
        uint64_t end = 0;
        char header[LOG_HEADER_SIZE];
        while (length - end >= LOG_HEADER_SIZE) {
            if (pread(fd, header, sizeof(header), end) != sizeof(header)) ReplayError("read header failed");
            uint32_t size;
            node_id_t producer;
            memcpy(&size, header + OFFSET_LOG_TOT_LEN, sizeof(size));
            memcpy(&producer, header + OFFSET_LOG_NODE_ID, sizeof(producer));
            if (size < LOG_HEADER_SIZE || size > LOG_REPLAY_BUFFER_SIZE || producer != node)
                ReplayError("invalid record in " + name);
            if (size > length - end) break;
            end += size;
        }
        if (end != length && (ftruncate(fd, end) != 0 || fdatasync(fd) != 0))
            ReplayError("cannot truncate incomplete WAL tail");
        close(fd);
        RegisterStream(node, end);
    }
    // Existing streams form a finite recovery set. Resolve all page chains
    // before accepting requests; a missing predecessor is not successful redo.
    bool progress;
    do {
        progress = false;
        for (auto& entry : streams_) progress |= ReadStream(*entry.second);
    } while (progress);
    if (pending_records_.load() != 0) ReplayError("missing predecessor in existing WAL streams");
    replay_thread_ = std::thread(&LogReplay::replayFun, this);
}

LogReplay::~LogReplay() {
    replay_stop_.store(true);
    cv_.notify_all();
    if (replay_thread_.joinable()) replay_thread_.join();
}

void LogReplay::RegisterStream(node_id_t node, uint64_t end) {
    std::lock_guard<std::mutex> guard(streams_mutex_);
    auto& stream = streams_[node];
    if (!stream) {
        stream.reset(new Stream);
        stream->node = node;
        stream->fd = open(LogFilePath(node).c_str(), O_RDONLY);
        if (stream->fd < 0) ReplayError("cannot open node stream");
        stream->buffer.resize(LOG_REPLAY_BUFFER_SIZE);
        stream->durable_end.store(end);
    }
}

void LogReplay::PublishDurable(node_id_t node, uint64_t end) {
    {
        std::lock_guard<std::mutex> guard(streams_mutex_);
        auto it = streams_.find(node);
        if (it == streams_.end()) ReplayError("unregistered producer");
        it->second->durable_end.store(end, std::memory_order_release);
    }
    cv_.notify_all();
}

bool LogReplay::CaughtUp() const {
    if (pending_records_.load() != 0) return false;
    for (const auto& entry : streams_)
        if (entry.second->read_offset.load() != entry.second->durable_end.load()) return false;
    return true;
}
bool LogReplay::WaitUntilReplayed(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(streams_mutex_);
    return cv_.wait_for(lock, timeout, [&] { return CaughtUp(); });
}
void LogReplay::Drain() {
    std::unique_lock<std::mutex> lock(streams_mutex_);
    cv_.wait(lock, [&] {
        for (const auto& entry : streams_)
            if (entry.second->read_offset.load() != entry.second->durable_end.load()) return false;
        return true;
    });
    if (pending_records_.load() != 0) ReplayError("missing predecessor after WAL drain");
    for (int fd : replayed_files_)
        if (fdatasync(fd) != 0) ReplayError("cannot sync replayed data file");
}
uint64_t LogReplay::DurableBytes() const {
    std::lock_guard<std::mutex> guard(streams_mutex_);
    uint64_t total = 0;
    for (const auto& entry : streams_) total += entry.second->durable_end.load();
    return total;
}
uint64_t LogReplay::ReadBytes() const {
    std::lock_guard<std::mutex> guard(streams_mutex_);
    uint64_t total = 0;
    for (const auto& entry : streams_) total += entry.second->read_offset.load();
    return total;
}

void LogReplay::WaitForPage(const std::string& table, page_id_t page, LLSN target) {
    if (target == 0) return;
    std::shared_ptr<PageProgress> state;
    {
        std::lock_guard<bthread::Mutex> guard(page_progress_mutex_);
        auto& entry = page_progress_[{table, page}];
        if (!entry) entry = std::make_shared<PageProgress>();
        state = entry;
    }
    std::unique_lock<bthread::Mutex> lock(state->mutex);
    if (!state->initialized) {
        RmPageHdr header{};
        const int fd = disk_manager_->open_file(table);
        disk_manager_->read_page(fd, page, reinterpret_cast<char*>(&header), sizeof(header));
        state->applied = header.LLSN_;
        state->initialized = true;
    }
    if (state->applied >= target) return;
    const auto begin = std::chrono::steady_clock::now();
    page_wait_count_.fetch_add(1, std::memory_order_relaxed);
    // Releases the mutex and suspends the bthread, not its RPC worker pthread.
    // Checking under the same mutex as publication prevents lost wakeups.
    while (state->applied < target) state->changed.wait(lock);
    page_wait_time_ns_.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - begin).count(), std::memory_order_relaxed);
}

void LogReplay::PublishPageApplied(const std::string& table, page_id_t page, LLSN lsn) {
    std::shared_ptr<PageProgress> state;
    {
        std::lock_guard<bthread::Mutex> guard(page_progress_mutex_);
        auto it = page_progress_.find({table, page});
        if (it == page_progress_.end()) return;
        state = it->second;
    }
    {
        std::lock_guard<bthread::Mutex> guard(state->mutex);
        state->applied = lsn;
        state->initialized = true;
    }
    state->changed.notify_all();
}

void LogReplay::ForgetTableProgress(const std::string& table) {
    // As with file deletion, the caller must exclude concurrent table access.
    // A recreated file must initialize its progress from its own page headers.
    std::lock_guard<bthread::Mutex> guard(page_progress_mutex_);
    for (auto it = page_progress_.lower_bound({table, 0});
         it != page_progress_.end() && it->first.first == table;) {
        it = page_progress_.erase(it);
    }
}

void LogReplay::set_table_name_map(std::unordered_map<table_id_t, std::string> names) {
    std::lock_guard<std::mutex> guard(table_fd_mutex_);
    table_name_map_ = std::move(names);
    table_fd_cache_.clear();
}

bool LogReplay::PageForLog(const LogRecord& log, PageKey& key) const {
    switch (log.log_type_) {
        case LogType::UPDATE: {
            const auto& r = static_cast<const UpdateLogRecord&>(log);
            key = {std::string(r.table_name_, r.table_name_size_), r.rid_.page_no_}; return true;
        }
        case LogType::LOCK: {
            const auto& r = static_cast<const LockLogRecord&>(log);
            key = {std::string(r.table_name_, r.table_name_size_), r.rid_.page_no_}; return true;
        }
        case LogType::INSERT: {
            const auto& r = static_cast<const InsertLogRecord&>(log);
            key = {std::string(r.table_name_, r.table_name_size_), r.page_no_}; return true;
        }
        case LogType::DELETE: {
            const auto& r = static_cast<const DeleteLogRecord&>(log);
            key = {std::string(r.table_name_, r.table_name_size_), r.page_no_}; return true;
        }
        default: return false;
    }
}

void LogReplay::Dispatch(const std::shared_ptr<LogRecord>& log) {
    PageKey key;
    if (!PageForLog(*log, key)) {
        apply_single_log(log.get());
        records_replayed_.fetch_add(1);
        return;
    }
    const int fd = disk_manager_->open_file(key.first);
    RmPageHdr hdr{};
    disk_manager_->read_page(fd, key.second, reinterpret_cast<char*>(&hdr), sizeof(hdr));
    if (log->lsn_ <= hdr.LLSN_) {
        PublishPageApplied(key.first, key.second, hdr.LLSN_);
        records_replayed_.fetch_add(1); // redo already present (including restart)
        return;
    }
    if (log->prev_lsn_ > hdr.LLSN_) {
        auto& queue = waiting_[key];
        auto inserted = queue.emplace(log->prev_lsn_, log);
        if (!inserted.second) ReplayError("two successors for the same page LLSN");
        pending_records_.fetch_add(1);
        return;
    }
    if (log->prev_lsn_ != hdr.LLSN_ || log->lsn_ <= log->prev_lsn_)
        ReplayError("broken page LLSN chain in " + key.first);
    auto current = log;
    while (current) {
        apply_single_log(current.get());
        replayed_files_.insert(fd);
        records_replayed_.fetch_add(1);
        auto page = waiting_.find(key);
        if (page == waiting_.end()) break;
        auto next = page->second.find(current->lsn_);
        if (next == page->second.end()) break;
        current = next->second;
        page->second.erase(next);
        pending_records_.fetch_sub(1);
        if (page->second.empty()) waiting_.erase(page);
    }
}

bool LogReplay::ReadStream(Stream& stream) {
    const uint64_t offset = stream.read_offset.load();
    const uint64_t end = stream.durable_end.load(std::memory_order_acquire);
    const size_t size = std::min<uint64_t>(end - offset, stream.buffer.size());
    if (size == 0) return false;
    if (pread(stream.fd, stream.buffer.data(), size, offset) != static_cast<ssize_t>(size))
        ReplayError("cannot read durable WAL bytes");
    size_t consumed = 0;
    while (size - consumed >= LOG_HEADER_SIZE) {
        const char* bytes = stream.buffer.data() + consumed;
        uint32_t length;
        memcpy(&length, bytes + OFFSET_LOG_TOT_LEN, sizeof(length));
        if (length < LOG_HEADER_SIZE || length > stream.buffer.size()) ReplayError("invalid record size");
        if (length > size - consumed) break;
        auto log = DecodeRecord(bytes, length);
        if (log->log_node_id_ != stream.node) ReplayError("record in wrong node stream");
        records_read_.fetch_add(1);
        Dispatch(log);
        consumed += length;
    }
    if (consumed == 0) ReplayError("incomplete record within durable prefix");
    {
        std::lock_guard<std::mutex> guard(streams_mutex_);
        stream.read_offset.store(offset + consumed);
    }
    cv_.notify_all();
    return true;
}

void LogReplay::replayFun() {
    while (!replay_stop_.load()) {
        std::vector<Stream*> streams;
        {
            std::lock_guard<std::mutex> guard(streams_mutex_);
            for (const auto& entry : streams_) streams.push_back(entry.second.get());
        }
        bool progress = false;
        for (auto* stream : streams) progress |= ReadStream(*stream);
        if (!progress) {
            std::unique_lock<std::mutex> lock(streams_mutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(1));
        }
    }
}
