#include "assignment_table.h"

namespace affinity {

AssignmentTable& GetAssignmentTable() {
    static AssignmentTable g;
    return g;
}

}  // namespace affinity
