#include <cassert>
#include <cstdint>
#include <unordered_map>

#include "affinity/assignment_target.h"

int main() {
    {
        std::unordered_map<int, uint32_t> access{{0, 2}, {1, 9}, {2, 1}};
        assert(affinity::ChooseAssignmentTarget(0, 2, access, 4) == 1);
    }

    {
        std::unordered_map<int, uint32_t> access{{0, 5}, {1, 6}, {2, 5}};
        assert(affinity::ChooseAssignmentTarget(0, 1, access, 4) == 0);
    }

    {
        std::unordered_map<int, uint32_t> access{{0, 7}, {1, 4}};
        assert(affinity::ChooseAssignmentTarget(0, 1, access, 4) == 0);
    }

    {
        std::unordered_map<int, uint32_t> access;
        assert(affinity::ChooseAssignmentTarget(0, 2, access, 4) == 2);
    }

    {
        std::unordered_map<int, uint32_t> access{{0, 1}, {5, 20}};
        assert(affinity::ChooseAssignmentTarget(0, 2, access, 4) == 0);
    }

    return 0;
}
