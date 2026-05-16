#include <cassert>
#include <cstdint>
#include <fstream>
#include <string>

#include "affinity/affinity_config.h"
#include "affinity/assignment_table.h"

int main() {
    const std::string path = "/tmp/wookong_schism_assignment.csv";
    {
        std::ofstream f(path, std::ios::out | std::ios::trunc);
        f << "tuple_id,table_id,item_key,node_id\n";
        f << affinity::pack_tuple_id(0, 10) << ",0,10,2\n";
        f << affinity::pack_tuple_id(1, 20) << ",1,20,3\n";
        f << affinity::pack_tuple_id(0, 30) << ",0,30,1\n";
    }

    affinity::AssignmentTable table;
    const auto loaded = table.LoadFromCsv(path, 4);
    assert(loaded.ok);
    assert(loaded.rows_loaded == 3);
    assert(table.Size() == 3);
    assert(table.Lookup(affinity::pack_tuple_id(0, 10), 0) == 2);
    assert(table.Lookup(affinity::pack_tuple_id(1, 20), 0) == 3);
    assert(table.Lookup(affinity::pack_tuple_id(0, 999), 0) == 0);

    const std::string bad_path = "/tmp/wookong_schism_assignment_bad.csv";
    {
        std::ofstream f(bad_path, std::ios::out | std::ios::trunc);
        f << "tuple_id,table_id,item_key,node_id\n";
        f << affinity::pack_tuple_id(0, 10) << ",0,10,99\n";
    }
    const auto bad = table.LoadFromCsv(bad_path, 4);
    assert(!bad.ok);
    assert(bad.rows_loaded == 0);
    assert(table.Size() == 3);
    return 0;
}
