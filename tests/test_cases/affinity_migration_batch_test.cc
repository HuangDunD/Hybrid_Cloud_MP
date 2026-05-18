#include <cassert>
#include <vector>

#include "affinity/migration_batch.h"

int main() {
    std::vector<affinity::ResolvedMigrationPlan> resolved;

    affinity::MigrationPlan p1{};
    p1.tuple_id = 1;
    p1.dst_node = 2;
    p1.epoch = 7;
    resolved.push_back({p1, 3, 1001, Rid{11, 4}, 0});

    affinity::MigrationPlan p2{};
    p2.tuple_id = 2;
    p2.dst_node = 2;
    p2.epoch = 7;
    resolved.push_back({p2, 3, 1002, Rid{11, 5}, 1});

    affinity::MigrationPlan p3{};
    p3.tuple_id = 3;
    p3.dst_node = 1;
    p3.epoch = 7;
    resolved.push_back({p3, 3, 1003, Rid{11, 6}, 2});

    affinity::MigrationPlan p4{};
    p4.tuple_id = 4;
    p4.dst_node = 2;
    p4.epoch = 7;
    resolved.push_back({p4, 3, 1004, Rid{12, 1}, 3});

    const auto groups = affinity::BuildMigrationGroups(resolved);

    assert(groups.size() == 3);
    assert(groups[0].table_id == 3);
    assert(groups[0].src_page == 11);
    assert(groups[0].dst_node == 2);
    assert(groups[0].member_indices.size() == 2);
    assert(groups[0].member_indices[0] == 0);
    assert(groups[0].member_indices[1] == 1);

    assert(groups[1].src_page == 11);
    assert(groups[1].dst_node == 1);
    assert(groups[1].member_indices.size() == 1);
    assert(groups[1].member_indices[0] == 2);

    assert(groups[2].src_page == 12);
    assert(groups[2].dst_node == 2);
    assert(groups[2].member_indices.size() == 1);
    assert(groups[2].member_indices[0] == 3);

    const auto singleton_groups = affinity::BuildMigrationGroups(resolved, false);
    assert(singleton_groups.size() == resolved.size());
    for (size_t i = 0; i < singleton_groups.size(); ++i) {
        assert(singleton_groups[i].member_indices.size() == 1);
        assert(singleton_groups[i].member_indices[0] == i);
        assert(singleton_groups[i].table_id == resolved[i].table_id);
        assert(singleton_groups[i].src_page == resolved[i].src_rid.page_no_);
        assert(singleton_groups[i].dst_node == resolved[i].plan.dst_node);
    }

    return 0;
}
