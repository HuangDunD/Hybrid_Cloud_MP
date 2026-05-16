import tempfile
import unittest
from pathlib import Path

from schism_partition import (
    fallback_partition,
    libmetis_available,
    libmetis_partition,
    load_graph_dump,
    write_assignment_csv,
)


class SchismPartitionTest(unittest.TestCase):
    def test_load_graph_dump_reads_edges_and_accesses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "graph.csv"
            path.write_text(
                "record_type,tuple_id_a,tuple_id_b,weight,node_id,"
                "access_count,epoch,total_samples\n"
                "edge,10,20,3,,0,7,2\n"
                "access,10,,0,0,2,7,2\n"
                "access,20,,0,1,1,7,2\n",
                encoding="utf-8",
            )
            graph = load_graph_dump(path)
            self.assertEqual(graph.vertices, [10, 20])
            self.assertEqual(graph.edges, [(10, 20, 3)])
            self.assertEqual(graph.access[10], 2)

    def test_write_assignment_csv_is_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "assignment.csv"
            write_assignment_csv(out, {20: 1, 10: 0})
            self.assertEqual(
                out.read_text(encoding="utf-8"),
                "tuple_id,table_id,item_key,node_id\n"
                "10,0,10,0\n"
                "20,0,20,1\n",
            )

    def test_fallback_partition_is_deterministic(self) -> None:
        self.assertEqual(
            fallback_partition([10, 20, 30], 2),
            {10: 0, 20: 1, 30: 0},
        )

    @unittest.skipUnless(libmetis_available(), "libmetis is not installed")
    def test_libmetis_partition_assigns_all_vertices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "graph.csv"
            path.write_text(
                "record_type,tuple_id_a,tuple_id_b,weight,node_id,"
                "access_count,epoch,total_samples\n"
                "edge,10,20,100,,0,7,2\n"
                "edge,30,40,100,,0,7,2\n"
                "edge,20,30,1,,0,7,2\n",
                encoding="utf-8",
            )
            graph = load_graph_dump(path)
            assignment = libmetis_partition(graph, 2)
            self.assertEqual(set(assignment), {10, 20, 30, 40})
            self.assertTrue(all(part in {0, 1} for part in assignment.values()))


if __name__ == "__main__":
    unittest.main()
