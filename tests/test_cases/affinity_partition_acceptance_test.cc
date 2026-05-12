#include <cassert>

#include "affinity/partition_acceptance.h"

int main() {
    affinity::PartitionAcceptanceInput first{};
    first.changed_vertices = 700;
    first.owned_vertices = 1000;
    first.max_changed_vertices_ratio = 0.30;
    assert(affinity::ShouldAcceptPartition(first));

    affinity::PartitionAcceptanceInput disabled{};
    disabled.current_assignment_size = 1000;
    disabled.changed_vertices = 700;
    disabled.owned_vertices = 1000;
    disabled.max_changed_vertices_ratio = 1.0;
    assert(affinity::ShouldAcceptPartition(disabled));

    affinity::PartitionAcceptanceInput excessive{};
    excessive.current_assignment_size = 1000;
    excessive.changed_vertices = 700;
    excessive.owned_vertices = 1000;
    excessive.max_changed_vertices_ratio = 0.30;
    assert(!affinity::ShouldAcceptPartition(excessive));
    const auto clipped =
        affinity::DecidePartitionAcceptance(excessive);
    assert(!clipped.accept_all);
    assert(clipped.clip_changed_vertices);
    assert(clipped.changed_vertices_budget == 300);
    assert(affinity::ShouldAcceptChangedVertex(clipped, 299));
    assert(!affinity::ShouldAcceptChangedVertex(clipped, 300));

    affinity::PartitionAcceptanceInput stable{};
    stable.current_assignment_size = 1000;
    stable.changed_vertices = 250;
    stable.owned_vertices = 1000;
    stable.max_changed_vertices_ratio = 0.30;
    assert(affinity::ShouldAcceptPartition(stable));
    const auto stable_decision =
        affinity::DecidePartitionAcceptance(stable);
    assert(stable_decision.accept_all);
    assert(!stable_decision.clip_changed_vertices);

    return 0;
}
