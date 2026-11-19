#pragma once
#include <array>
#include <cstdint>
#include <type_traits>

namespace mvcc {
// Fixed bytes in the main row. Never embed std::string, pointers, or lock owners.
// Row deletion uses DataItem::valid; initial versions have an all-zero creator.
struct RowFields {
    uint32_t format = 0x4d564332;  // MVC2; old datasets must be regenerated
    uint32_t initial = 1;
    uint32_t creator_home = 0;
    uint32_t reserved = 0;
    uint64_t creator_local = 0;
    std::array<char, 16> creator_epoch{};
    std::array<char, 56> history{};
};
static_assert(sizeof(RowFields) == 96 && std::is_trivially_copyable<RowFields>::value);
}  // namespace mvcc
