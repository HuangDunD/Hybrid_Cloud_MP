#include "ycsb_db.h"
#include <butil/logging.h>
#include <algorithm>
#include <numeric>
#include <random>

void YCSB::LoadTable(const key_partition::Layout& layout) {
    std::string table_name = bench_name + "_user_table";

    // 单个元组的大小 = DataItem 各个数据结构的空间 + 实际数据的大小
    // int tuple_size = sizeof(DataItem) + sizeof(ycsb_user_table_val); 
    rm_manager->create_file(table_name , tuple_size);
    rm_manager->create_file(table_name + "_fsm", tuple_size);

    
    // std::cout << "单个元组大小为: " << tuple_size << "\n";
    std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(table_name);
    std::unique_ptr<RmFileHandle> table_file_fsm = rm_manager->open_file(table_name + "_fsm");

    std::ofstream indexfile;
    indexfile.open(table_name + "_index.txt");

    
    table_file->file_hdr_.record_size_ = tuple_size;
    table_file->file_hdr_.num_records_per_page_ = num_records_per_page;
    table_file->file_hdr_.bitmap_size_ = (num_records_per_page + BITMAP_WIDTH - 1) / BITMAP_WIDTH;

    auto insert_key = [&](itemkey_t key) {
        ycsb_user_table_val val;
        val.magic = ycsb_user_table_magic;
        std::string f0 = ramdom_string(field_len);
        std::string f1 = ramdom_string(field_len);
        std::string f2 = ramdom_string(field_len);
        std::string f3 = ramdom_string(field_len);
        std::string f4 = ramdom_string(field_len);
        std::string f5 = ramdom_string(field_len);
        std::string f6 = ramdom_string(field_len);
        std::string f7 = ramdom_string(field_len);
        std::string f8 = ramdom_string(field_len);
        std::string f9 = ramdom_string(field_len);

        strncpy(val.file_0, f0.c_str(), sizeof(val.file_0));
        strncpy(val.file_1, f1.c_str(), sizeof(val.file_1));
        strncpy(val.file_2, f2.c_str(), sizeof(val.file_2));
        strncpy(val.file_3, f3.c_str(), sizeof(val.file_3));
        strncpy(val.file_4, f4.c_str(), sizeof(val.file_4));
        strncpy(val.file_5, f5.c_str(), sizeof(val.file_5));
        strncpy(val.file_6, f6.c_str(), sizeof(val.file_6));
        strncpy(val.file_7, f7.c_str(), sizeof(val.file_7));
        strncpy(val.file_8, f8.c_str(), sizeof(val.file_8));
        strncpy(val.file_9, f9.c_str(), sizeof(val.file_9));
        
        return LoadRecord(table_file.get(), key, &val, sizeof(val), 0, indexfile);
    };

    std::mt19937_64 rng(0xC0FFEEULL);
    if (layout.ByKey()) {
        CHECK(layout.pages > 0 && record_count > 0);
        std::vector<std::vector<itemkey_t>> groups(layout.nodes);
        for (int key = 0; key < record_count; ++key)
            groups[key_partition::KeyOwner(key, layout.nodes)].push_back(key);
        if (random_generate)
            for (auto& group : groups) std::shuffle(group.begin(), group.end(), rng);

        std::vector<size_t> cursor(layout.nodes);
        std::vector<int> free_slots{0}; // page zero contains the file header
        int inserted = 0;
        while (inserted < record_count) {
            const page_id_t page_no = table_file->file_hdr_.num_pages_;
            const int owner = ((page_no - 1) / layout.pages) % layout.nodes;
            const auto& group = groups[owner];
            const int rows = std::min<size_t>(num_records_per_page, group.size() - cursor[owner]);

            // Start a fresh page even if another owner's tail still has space.
            const auto previous_free = table_file->file_hdr_.first_free_page_no_;
            auto page = table_file->create_new_page_handle(nullptr);
            CHECK_EQ(page.page->get_page_id().page_no, page_no);
            page.page_hdr->next_free_page_no_ = previous_free;
            table_file->buffer_pool_manager_->unpin_page(page.page->get_page_id(), true);
            for (int slot = 0; slot < rows; ++slot) {
                const auto key = group[cursor[owner]++];
                const auto rid = insert_key(key);
                CHECK(rid.page_no_ == page_no && rid.slot_no_ == slot);
            }
            free_slots.push_back(num_records_per_page - rows);
            inserted += rows;
        }
        CHECK(fsm_trees[0]->initialize(20000, free_slots.size()));
        for (size_t page = 1; page < free_slots.size(); ++page)
            if (free_slots[page])
                fsm_trees[0]->update_page_space(page, free_slots[page] * (tuple_size + sizeof(itemkey_t)));
    } else {
        std::vector<itemkey_t> keys(record_count);
        std::iota(keys.begin(), keys.end(), 0);
        if (random_generate) std::shuffle(keys.begin(), keys.end(), rng);
        for (auto key : keys) insert_key(key);
        CHECK(fsm_trees[0]->initialize(20000, num_pages * 3));
        const int last_rows = record_count % num_records_per_page;
        fsm_trees[0]->update_page_space(num_pages, (num_records_per_page - last_rows) * (tuple_size + sizeof(itemkey_t)));
    }
    int fd1 = rm_manager->get_diskmanager()->open_file(table_name + "_fsm");
    rm_manager->get_diskmanager()->update_value(fd1, RM_FILE_HDR_PAGE, sizeof(RmPageHdr),
        (char *)&table_file_fsm->file_hdr_, sizeof(table_file_fsm->file_hdr_));
    CHECK(fsm_trees[0]->flush_all_pages());
    rm_manager->get_diskmanager()->close_file(fd1);

    rm_manager->close_file(table_file.get());
    indexfile.close();
    CHECK(indexfile.good());
}

Rid YCSB::LoadRecord(RmFileHandle *file_handle ,
        itemkey_t item_key , void *val_ptr , 
        size_t val_size , table_id_t table_id ,
        std::ostream &index_file){
    DataItem item_to_be_insert(table_id , (uint8_t*)val_ptr , val_size);
    item_to_be_insert.timeStamp = 0; // 初始化为 0，表示尚未被任何事务写过
    
    // 这里写的有点乱，在计算层看来，存储的数据是 DataItem，但是存储层看来，存储的就是一堆字符
    // 其实应该和计算层对齐一下的，有时间再改改
    char *item_char = (char*)malloc(item_to_be_insert.GetSerializeSize());
    item_to_be_insert.Serialize(item_char);
    Rid rid = file_handle->insert_record(item_key , item_char , nullptr);

    index_file << item_key << " " << rid.page_no_ << " " << rid.slot_no_ << std::endl;
    bl_indexes[table_id]->insert_entry(&item_key , rid);
    
    free(item_char);
    return rid;
}


void YCSB::VerifyData() {
    std::string table_name = bench_name + "_user_table";
    std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(table_name);
    
    int verified_count = 0;
    for (int id = 0; id < record_count; id++) {
        user_table_key_t key;
        key.user_id = (uint64_t)id;
        Rid rid;
        // Search in BLink Tree index
        bool found = bl_indexes[0]->search(&key.item_key, rid);
        
        if (!found) {
            std::cout << "Key " << id << " not found in index";
            assert(false);
        }
        
        RmPageHandle page_handle = table_file->fetch_page_handle(rid.page_no_);
        char* tuple = page_handle.get_slot(rid.slot_no_);
        itemkey_t* disk_key = reinterpret_cast<itemkey_t*>(tuple);
        assert(*disk_key == key.item_key);
        DataItem* data_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            
        // Check size
        if (data_item->value_size == sizeof(ycsb_user_table_val)) {
            assert(data_item->lock == 0);
            data_item->value = (uint8_t*)((char*)data_item + sizeof(DataItem));
            
            ycsb_user_table_val* val = reinterpret_cast<ycsb_user_table_val*>(data_item->value);
            assert(val->magic == ycsb_user_table_magic);
        } else {
            assert(false);
        }
        rm_manager->get_bufferPoolManager()->unpin_page(page_handle.page->get_page_id(), false);
    }
    rm_manager->close_file(table_file.get());
}
