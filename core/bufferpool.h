// 头部 include 区域

#pragma once

#include "base/page.h"
#include "common.h"
#include "config.h"
#include "bufferpool_replacer.h"
#include "LPLM/local_LR_page_lock.h"

#include "memory"
#include "mutex"
#include "array"
#include "iostream"
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>
#include <cstring>
#include <mutex>
#include <string_view>      
#include "condition_variable"
#include "functional"

/*
*   缓冲池里面三类页面：
    1. 正在使用的页面，这个不能淘汰的，在 page_table 中，不在 lru_list 内和 free_lists 中
    2. 空闲的页面：可以被淘汰，不在 page_tale 和 lru_list，在 free_list 内
    3. 本节点持有，但是未使用的页面，这是因为 lazy_release 策略而赖在缓冲区里面的页面，在 buffer_pool 和 lru_list 中，不在 free_list 内
*/
class BufferPool {
    friend class ComputeNode;
public:
    typedef std::shared_ptr<BufferPool> ptr;

    explicit BufferPool(size_t size_ , size_t max_page_num_) 
        : pool_size(size_) , max_page_num(max_page_num_){
        pages.resize(pool_size);
        for (size_t i = 0 ; i < pool_size ; i++) {
            pages[i] = new Page();
            pages[i]->page_id_ = INVALID_PAGE_ID;
        }

        if (std::string_view(REPLACER_TYPE) == "LRU") {  
            replacer = std::make_shared<LRU_Replacer>(pool_size);
        }

        for (size_t i = 0 ; i < pool_size; i++) {
            size_t partition_idx = get_partition_idx(static_cast<page_id_t>(i));
            free_lists[partition_idx].emplace_back(i);
        }
    }

    ~BufferPool(){
        for (size_t i = 0 ; i < pages.size() ; i++){
            delete pages[i];
        }
    }

    // 强要求页面一定在缓冲池里
    Page *fetch_page(page_id_t page_id){
        std::lock_guard<bthread::Mutex> lk(get_partition_mtx(page_id));
        auto &page_table = get_partition_page_table(page_id);
        auto it = page_table.find(page_id);
        assert(it != page_table.end());

        frame_id_t frame_id = it->second;
        Page *page = pages[frame_id];
        // pin_count 的作用不是 RUCBase 那样的，作用只有一个，就是减少 replacer->pin 的次数
        // 它不负责回收 frame，因为页面一定是用完了才 unpin
        if (page->pin_count_++ == 0){
            replacer->pin(frame_id);
        }
        
        return page;
    }

    // 不要求页面在缓冲池里
    Page *try_fetch_page(page_id_t page_id){
        std::lock_guard<bthread::Mutex> lk(get_partition_mtx(page_id));
        auto &page_table = get_partition_page_table(page_id);
        auto it = page_table.find(page_id);
        if (it == page_table.end()){
            return nullptr;
        }

        frame_id_t frame_id = it->second;
        Page *page = pages[frame_id];
        if (page->pin_count_++ == 0){
            replacer->pin(frame_id);
        }
        assert(page->page_id_ == page_id);

        return page;
    }

    

    const std::string try_fetch_page_ret_string(page_id_t page_id){
        std::lock_guard<bthread::Mutex> lk(get_partition_mtx(page_id));
        auto &page_table = get_partition_page_table(page_id);
        auto it = page_table.find(page_id);
        if (it == page_table.end()){
            return "";
        }

        frame_id_t frame_id = it->second;
        std::string ret(pages[frame_id]->get_data() , PAGE_SIZE);

        assert(pages[frame_id]->page_id_ == page_id);
        assert(ret.size() == PAGE_SIZE);
        return ret;
    }

    // 直接从缓冲区里面把这个页面删掉，这个是当节点释放页面所有权的时候调用的
    void release_page(table_id_t table_id , page_id_t page_id){
        if (table_id < 10000){
            // LOG(INFO) << "now release page , table_id = " << table_id << " page_id = " << page_id ; 
        }
        std::lock_guard<bthread::Mutex> lk(get_partition_mtx(page_id));
        auto &page_table = get_partition_page_table(page_id);
        auto it = page_table.find(page_id);
        assert(it != page_table.end());

        frame_id_t frame_id = it->second;
        Page *pg = pages[frame_id];
        pg->pin_count_ = 0;
        pg->reset_memory();
        pg->page_id_ = INVALID_PAGE_ID;

        // 从页表里面删除
        page_table.erase(it);
        // 确保该帧不被 LRU 追踪，然后归还到 free_list
        replacer->pin(frame_id);
        free_lists[get_partition_idx(page_id)].push_back(frame_id);
    }


    // 这个是使用完，而没有 pending(也就是不用立刻释放页面所有权)的时候调用的
    // 把仍然持有所有权，但是没在使用的页面放在 LRU 中
    void unpin_page(page_id_t page_id) {
        std::lock_guard<bthread::Mutex> lk(get_partition_mtx(page_id));
        auto &page_table = get_partition_page_table(page_id);
        auto it = page_table.find(page_id);

        // Debug 用
        // bool should_release = should_release_buffer[page_id];
        // int pending_count = pending_operation_counts[page_id];
        assert(it != page_table.end());

        frame_id_t frame_id = it->second;
        pages[frame_id]->pin_count_ = 0;
        // 不需要 pin_count，因为我这个缓冲区是严格限制的，unpin 一定是用完了缓冲区
        replacer->unpin(frame_id);
    }

    bool is_in_bufferPool(page_id_t page_id){
        std::lock_guard<bthread::Mutex> lock(get_partition_mtx(page_id));
        auto &page_table = get_partition_page_table(page_id);
        return (page_table.find(page_id) != page_table.end());
    }

    void wait_in_bufferPool(page_id_t page_id) {
        size_t idx = get_partition_idx(page_id);
        std::unique_lock<bthread::Mutex> lk(mtx_partitions[idx]);
        cv_partitions[idx].wait(lk, [&]() {
            return page_tables[idx].find(page_id) != page_tables[idx].end();
        });
    }

    bool checkIfDirectlyPutInBuffer(page_id_t page_id , frame_id_t &frame_id){
        std::lock_guard<bthread::Mutex> lk(get_partition_mtx(page_id));
        auto &page_table = get_partition_page_table(page_id);
        assert(page_table.find(page_id) == page_table.end());

        auto &part_free_list = free_lists[get_partition_idx(page_id)];
        if (!part_free_list.empty()){
            frame_id = part_free_list.front();
            part_free_list.pop_front();
            return true;
        }
        return false;
    }

    // 只有 ts 和 eager 会走这个
    bool checkIfDirectlyUpdate(page_id_t page_id , const void *data){
        std::lock_guard<bthread::Mutex>lk(get_partition_mtx(page_id));
        auto &page_table = get_partition_page_table(page_id);
        auto it = page_table.find(page_id);
        if (it == page_table.end()){
            return false;
        }
        frame_id_t frame_id = it->second;
        assert(pages[frame_id]->pin_count_ == 0);
        pages[frame_id]->pin_count_++;
        replacer->pin(frame_id);
        memcpy(pages[frame_id]->get_data() , data , PAGE_SIZE);
        return true;
    }

    // 第一个:选中要淘汰的页面
    // 返回的时候，被选中的这个页面的真实状态
    std::pair<page_id_t , page_id_t> replace_page (page_id_t page_id , 
            frame_id_t &frame_id,
            const std::function<bool(page_id_t)> &try_begin_evict ){
        page_id_t victim_page_id = INVALID_PAGE_ID;
        while (true){
            bool res = replacer->tryVictim(&frame_id);
            assert(res);

            victim_page_id = pages[frame_id]->page_id_;
            if (victim_page_id == INVALID_PAGE_ID){
                replacer->endVictim(false , &frame_id);
                continue;
            }

            /*
                如果别的线程正在用这个页面，升级页面，或者别人正在让节点放弃页面，那就不要淘汰了
                把 is_evicting 设置为 true，防止淘汰过程中别的线程又去申请锁
                唯一无法隔绝的情况是，节点无法预支 Pending 信号什么时候来，如果选中之后，Pending 来了，那就麻烦了，隔绝这个的方法在 RemoteServer 中
            */
            bool ok = try_begin_evict(victim_page_id);

            replacer->endVictim(ok , &frame_id);

            if (ok){
                return std::make_pair(victim_page_id , pages[frame_id]->page_id_);
            }
        }
    }

    Page *insert_or_replace(table_id_t table_id, page_id_t page_id , frame_id_t frame_id , bool need_to_replace , page_id_t replaced_page , const void *src){
        size_t new_idx = get_partition_idx(page_id);
        bthread::Mutex* first_mtx = &mtx_partitions[new_idx];
        bthread::Mutex* second_mtx = nullptr;
        size_t old_idx = new_idx;
        if (need_to_replace){
            old_idx = get_partition_idx(replaced_page);
            if (old_idx != new_idx){
                if (old_idx < new_idx){
                    first_mtx = &mtx_partitions[old_idx];
                    second_mtx = &mtx_partitions[new_idx];
                } else {
                    first_mtx = &mtx_partitions[new_idx];
                    second_mtx = &mtx_partitions[old_idx];
                }
            }
        }
        first_mtx->lock();
        if (second_mtx != nullptr){
            second_mtx->lock();
        }
        if (need_to_replace){
            assert(replaced_page != INVALID_PAGE_ID);
            auto &old_page_table = page_tables[old_idx];
            assert(old_page_table.find(replaced_page) != old_page_table.end());
            old_page_table.erase(replaced_page);
        }                                                                                           

        auto &new_page_table = page_tables[new_idx];
        assert(new_page_table.find(page_id) == new_page_table.end());
        new_page_table[page_id] = frame_id;
        replacer->pin(frame_id);
        Page *page = pages[frame_id];

        assert(src != nullptr);
        std::memcpy(page->get_data() , src , PAGE_SIZE);
        page->page_id_ = page_id;
        page->id_.table_id = table_id;
        page->id_.page_no = page_id;
        page->set_dirty(false);

        if (second_mtx != nullptr){
            second_mtx->unlock();
        }
        first_mtx->unlock();
        cv_partitions[new_idx].notify_all();

        return page;
    }

    void releaseBufferPage(table_id_t table_id , page_id_t page_id) {
        release_page(table_id , page_id);
    }

private:
    ALWAYS_INLINE size_t get_partition_idx(page_id_t page_id) const {
        return static_cast<size_t>(page_id) % NUM_BUFFER_PARTITION;
    }

    ALWAYS_INLINE bthread::Mutex& get_partition_mtx(page_id_t page_id) {
        return mtx_partitions[get_partition_idx(page_id)];
    }

    ALWAYS_INLINE std::unordered_map<page_id_t , frame_id_t>& get_partition_page_table(page_id_t page_id) {
        return page_tables[get_partition_idx(page_id)];
    }

    // 之前的缓冲池性能比较差，因为节点内的所有线程，争夺缓冲区一把锁
    // 改了下，改成了分区的锁，这样性能一下就上去了
    std::array<bthread::Mutex, NUM_BUFFER_PARTITION> mtx_partitions;

    std::vector<Page*> pages;
    size_t pool_size;
    size_t max_page_num;

    ReplacerBase::ptr replacer;
    std::array<std::list<frame_id_t>, NUM_BUFFER_PARTITION> free_lists; // 空闲的帧
    std::array<bthread::ConditionVariable, NUM_BUFFER_PARTITION> cv_partitions;

    // 页表：实现 PageID -> 帧的映射
    std::array<std::unordered_map<page_id_t , frame_id_t>, NUM_BUFFER_PARTITION> page_tables;
};
