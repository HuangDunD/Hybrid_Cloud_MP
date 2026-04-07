#pragma once

#include "memory"
#include "mutex"
#include "list"
#include "unordered_map"
#include "vector"
#include "array"
#include "assert.h"
#include <bthread/mutex.h>

#include "common.h"
#include "config.h"

class ReplacerBase {
public:
    typedef std::shared_ptr<ReplacerBase> ptr;
    ReplacerBase() = default;
    virtual ~ReplacerBase() = default;

    virtual bool tryVictim(frame_id_t *frame_id) = 0;
    virtual void endVictim(bool success , frame_id_t *frame_id) = 0;
    virtual void pin(frame_id_t frame_id) = 0;
    virtual void unpin(frame_id_t frame_id) = 0;
    virtual size_t getSize() const = 0;

private:
};

class LRU_Replacer : public ReplacerBase {
public:
    std::shared_ptr<LRU_Replacer> ptr;
    
    LRU_Replacer(size_t num_pages) : max_size(num_pages) {
        is_evicting = std::vector<uint8_t>(num_pages , 0);
    }
    ~LRU_Replacer() {}

    bool tryVictim(frame_id_t *frame_id) override {
        constexpr size_t max_probe_per_partition = 5;
        size_t start_partition = (FastSeed() >> 32) % NUM_BUFFER_PARTITION;
        for (size_t i = 0; i < NUM_BUFFER_PARTITION; i++) {
            size_t partition_idx = (start_partition + i) % NUM_BUFFER_PARTITION;
            std::lock_guard<bthread::Mutex> lk(mtx_partitions[partition_idx]);
            auto &lru_list = lru_lists[partition_idx];
            if (lru_list.empty()) {
                continue;
            }
            auto it = lru_list.end();
            size_t probes = 0;
            while (it != lru_list.begin() && probes < max_probe_per_partition) {
                --it;
                frame_id_t candidate = *it;
                probes++;
                if (is_evicting[candidate]){
                    continue;
                }
                is_evicting[candidate] = 1;
                *frame_id = candidate;
                return true;
            }
        }
        return false;
    }

    void endVictim(bool success , frame_id_t *frame_id) override {
        size_t partition_idx = get_partition_idx(*frame_id);
        std::lock_guard<bthread::Mutex> lk(mtx_partitions[partition_idx]);
        auto &lru_list = lru_lists[partition_idx];
        auto &lru_hash = lru_hashes[partition_idx];
        if (success){
            auto it = lru_hash.find(*frame_id);
            if (it != lru_hash.end()){
                lru_list.erase(it->second);
                lru_hash.erase(*frame_id);
            }
        }
        is_evicting[*frame_id] = 0;
    }

    void pin(frame_id_t frame_id) override {
        size_t partition_idx = get_partition_idx(frame_id);
        std::lock_guard<bthread::Mutex> lk(mtx_partitions[partition_idx]);
        auto &lru_list = lru_lists[partition_idx];
        auto &lru_hash = lru_hashes[partition_idx];
        auto it = lru_hash.find(frame_id);
        if (it != lru_hash.end()){
            lru_list.erase(it->second);
            lru_hash.erase(it);
        }
    }

    void unpin(frame_id_t frame_id) override {
        size_t partition_idx = get_partition_idx(frame_id);
        std::lock_guard<bthread::Mutex> lk(mtx_partitions[partition_idx]);
        auto &lru_list = lru_lists[partition_idx];
        auto &lru_hash = lru_hashes[partition_idx];
        auto it = lru_hash.find(frame_id);
        if (it != lru_hash.end()){
            lru_list.erase(it->second);
            lru_hash.erase(it);
        }
        if (lru_list.size() >= max_size){
            assert(false);
        }
        std::list<frame_id_t>::iterator iter = 
            lru_list.insert(lru_list.begin() , frame_id);
        lru_hash[frame_id] = iter;
    }

    size_t getSize() const {
        size_t total = 0;
        for (size_t i = 0; i < NUM_BUFFER_PARTITION; i++) {
            total += lru_lists[i].size();
        }
        return total;
    }

private:
    ALWAYS_INLINE uint64_t FastSeed() {
        if (!seed_initialized) {
            local_seed = 1469598103934665603ULL ^
                         static_cast<uint64_t>(reinterpret_cast<uintptr_t>(this)) ^
                         static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&local_seed));
            if (local_seed == 0) {
                local_seed = 1;
            }
            seed_initialized = true;
        }
        local_seed = local_seed * 1103515245ULL + 12345ULL;
        return local_seed;
    }

    ALWAYS_INLINE size_t get_partition_idx(frame_id_t frame_id) const {
        return static_cast<size_t>(frame_id) % NUM_BUFFER_PARTITION;
    }

    std::array<bthread::Mutex, NUM_BUFFER_PARTITION> mtx_partitions;
    std::vector<uint8_t> is_evicting;
    std::array<std::list<frame_id_t>, NUM_BUFFER_PARTITION> lru_lists;
    std::array<std::unordered_map<frame_id_t , std::list<frame_id_t>::iterator>, NUM_BUFFER_PARTITION> lru_hashes;
    size_t max_size;
    inline static thread_local uint64_t local_seed = 0;
    inline static thread_local bool seed_initialized = false;
};
