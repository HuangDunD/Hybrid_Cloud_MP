#include <cassert>
#include <cstdint>

#include "affinity/migration_policy.h"

int main() {
    assert(affinity::UpdateSameTargetObservations(-1, 0, 2) == 1);
    assert(affinity::UpdateSameTargetObservations(2, 1, 2) == 2);
    assert(affinity::UpdateSameTargetObservations(2, 7, 3) == 1);

    assert(affinity::MigrationTargetObservedOftenEnough(0) == false);
    assert(affinity::MigrationTargetObservedOftenEnough(1) == true);
    assert(affinity::MaxPlansPerSourcePageDestPerSweep() == 2);
    assert(affinity::IsFreshMigrationAssignment(10, 10) == true);
    assert(affinity::IsFreshMigrationAssignment(9, 10) == true);
    assert(affinity::IsFreshMigrationAssignment(8, 10) == false);
    assert(affinity::MigrationSuccessCooldownEpochs() == 3);
    assert(affinity::MigrationFailureCooldownEpochs(0) == 1);
    assert(affinity::MigrationFailureCooldownEpochs(1) == 1);
    assert(affinity::MigrationFailureCooldownEpochs(2) == 2);
    assert(affinity::MigrationFailureCooldownEpochs(3) == 4);
    assert(affinity::MigrationFailureCooldownEpochs(4) == 8);
    assert(affinity::MigrationFailureCooldownEpochs(5) == 16);
    assert(affinity::MigrationFailureCooldownEpochs(20) == 16);
    assert(affinity::MigrationPlannerPriorityWindow(0, 1000) == 0);
    assert(affinity::MigrationPlannerPriorityWindow(10, 100000) == 1024);
    assert(affinity::MigrationPlannerPriorityWindow(100, 1200) == 1200);

    return 0;
}
