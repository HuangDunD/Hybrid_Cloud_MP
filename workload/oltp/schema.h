#pragma once
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

namespace oltp {
// One fixed-width record per subscriber in each table keeps page owners aligned.
// Each child array is indexed by facility/access type, forwarding also by start hour / 8.
struct Forward { uint8_t end_hour; char number[16]; };
struct Subscriber {
    char number[16];
    uint32_t location;
    uint8_t bit[10], hex[10];
    uint16_t bytes[10];
};
struct Access { bool present; uint8_t data1, data2; char data3[4], data4[6]; };
struct Facility { bool present, active; uint8_t data; };
union TatpRow {
    Subscriber subscriber;
    Access access[4];
    Facility facility[4];
    Forward forwarding[4][3];
};
inline void Phone(char (&number)[16], uint32_t id) {
    std::snprintf(number, sizeof(number), "%015u", id + 1);
}
inline TatpRow TatpInitial(int table, uint32_t id) {
    std::mt19937 rng(id);
    TatpRow row{};
    // Reproduce the facility mask in both the facility and forwarding tables.
    std::array<int,4> facility{0,1,2,3};
    std::shuffle(facility.begin(), facility.end(), rng);
    const unsigned count = 1 + rng() % 4;
    if (table == 0) {
        Phone(row.subscriber.number, id);
        row.subscriber.location = rng();
        for (int i=0; i<10; ++i) {
            row.subscriber.bit[i] = rng()%2;
            row.subscriber.hex[i] = rng()%16;
            row.subscriber.bytes[i] = rng();
        }
    } else if (table == 1) {
        std::shuffle(facility.begin(), facility.end(), rng);
        const auto access_count = 1 + rng()%4;
        for (unsigned i=0; i<access_count; ++i) {
            auto& a = row.access[facility[i]];
            a.present = true;
            a.data1 = rng(); a.data2 = rng();
            for (auto& c : a.data3) c = 'A' + rng()%26;
            for (auto& c : a.data4) c = 'A' + rng()%26;
        }
    } else {
        for (unsigned i=0; i<count; ++i) {
            const int type = facility[i];
            if (table == 2) {
                row.facility[type] = {true, rng()%100 < 85, static_cast<uint8_t>(rng())};
            } else {
                std::array<int,3> slots{0,1,2};
                std::shuffle(slots.begin(), slots.end(), rng);
                const auto size = rng()%4;
                for (unsigned j=0; j<size; ++j) {
                    auto& f = row.forwarding[type][slots[j]];
                    f.end_hour = slots[j]*8 + 1 + rng()%8;
                    Phone(f.number, rng()%1000000000);
                }
            }
        }
    }
    return row;
}

struct SbRow { uint32_t k; std::array<char,120> c; std::array<char,60> pad; };
// Each ordered secondary-index leaf is a separately versioned row.
struct IndexEntry { uint32_t k, id; };
inline bool IndexLess(const IndexEntry& a, const IndexEntry& b) {
    return a.k < b.k || (a.k == b.k && a.id < b.id);
}
struct SbIndexLeaf {
    uint32_t count = 0;
    static constexpr uint32_t Capacity = 32;
    std::array<IndexEntry, Capacity> entries{};
    bool Replace(uint32_t id, uint32_t before, uint32_t after) {
        if (count > entries.size()) return false;
        auto end = entries.begin() + count;
        auto it = std::lower_bound(entries.begin(), end, IndexEntry{before,id}, IndexLess);
        if (it == end || it->id != id || it->k != before) return false;
        it->k = after;
        std::sort(entries.begin(), end, IndexLess);
        return true;
    }
};
// The first ID of each 32-row group within one owner's sequence is the leaf key.
inline uint64_t SbLeafKey(uint64_t id, uint32_t nodes) {
    return id - (id / nodes % SbIndexLeaf::Capacity) * nodes;
}
inline SbRow SbValue(uint32_t seed) {
    SbRow row{};
    row.k = seed;
    auto fill = [&](auto& field) {
        for (char& c : field) {
            seed = seed * 1664525u + 1013904223u;
            c = '0' + (seed >> 16)%10;
        }
    };
    fill(row.c); fill(row.pad);
    return row;
}
}
