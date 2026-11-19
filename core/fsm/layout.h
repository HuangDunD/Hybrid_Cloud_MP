#pragma once

#include <cstdint>

namespace fsm {
// Version 2 covers signed 32-bit heap page numbers. The four-level tree is
// numbered in preorder. Ancestors precede their first leaf, so growing a heap
// only appends FSM pages and never moves a published root, child or slot.
constexpr uint32_t kVersion = 2;
constexpr uint32_t kLeafSlots = 1024;
constexpr uint32_t kFanout = 512;
constexpr unsigned kRootLevel = 3;
constexpr uint32_t kHeapCapacity = uint32_t{1} << 31;

constexpr uint32_t LeafSpan(unsigned level) {
    return uint32_t{1} << (9 * level);
}

constexpr uint32_t PageNumber(unsigned level, uint32_t index) {
    const uint32_t first = index * LeafSpan(level);
    return 2 + kRootLevel - level + first + first / kFanout + first / (kFanout * kFanout);
}

constexpr uint32_t ChildSlot(uint32_t heap_page, unsigned parent_level) {
    return heap_page / kLeafSlots / LeafSpan(parent_level - 1) % kFanout;
}
}  // namespace fsm
