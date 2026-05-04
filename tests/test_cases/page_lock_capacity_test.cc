#include <cassert>
#include <cstddef>

#include "config.h"

int main() {
    const size_t compact_capacity = 128;
    assert(NormalizePageLockTableCapacity(compact_capacity) ==
           static_cast<size_t>(ComputeNodeBufferPageSize));

    const size_t larger_capacity = static_cast<size_t>(ComputeNodeBufferPageSize) + 17;
    assert(NormalizePageLockTableCapacity(larger_capacity) == larger_capacity);

    return 0;
}
