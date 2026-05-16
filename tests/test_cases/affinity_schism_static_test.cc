#include <cassert>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>

#include "affinity/affinity_config.h"
#include "affinity/assignment_table.h"
#include "affinity/graph.h"
#include "affinity/graph_dump.h"
#include "affinity/schism_static.h"

namespace {

std::string ReadAll(const std::string& path) {
    std::ifstream f(path);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

void TestAssignmentCsvLoad() {
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
}

void TestGraphDump() {
    affinity::LocalGraph graph;
    graph.epoch = 7;
    graph.total_samples = 2;
    graph.AddEdge(30, 10, 4);
    graph.AddEdge(10, 20, 3);
    graph.AddNodeAccess(10, 0);
    graph.AddNodeAccess(10, 0);
    graph.AddNodeAccess(20, 1);

    std::string error;
    assert(affinity::DumpLocalGraphCsv(
        graph, "/tmp/wookong_schism_graph.csv", &error));
    const std::string text = ReadAll("/tmp/wookong_schism_graph.csv");
    assert(text.find("record_type,tuple_id_a,tuple_id_b,weight,node_id,"
                     "access_count,epoch,total_samples\n") != std::string::npos);
    assert(text.find("edge,10,20,3,,0,7,2\n") != std::string::npos);
    assert(text.find("edge,10,30,4,,0,7,2\n") != std::string::npos);
    assert(text.find("access,10,,0,0,2,7,2\n") != std::string::npos);
    assert(text.find("access,20,,0,1,1,7,2\n") != std::string::npos);
}

void TestSchismApplyConvergence() {
    assert(!affinity::SchismApplyConverged(0, 0, 0));
    assert(affinity::SchismApplyConverged(100, 99, 0));
    assert(!affinity::SchismApplyConverged(100, 98, 0));
    assert(!affinity::SchismApplyConverged(100, 99, 2));
}

}  // namespace

int main() {
    TestAssignmentCsvLoad();
    TestGraphDump();
    TestSchismApplyConvergence();
    return 0;
}
