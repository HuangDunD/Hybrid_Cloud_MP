import unittest
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from multinode_parmetis_smoke import (
    format_progress_line,
    parse_executed_txn_count,
    parse_timeseries_metrics,
)


class MultinodeParmetisSmokeProgressTest(unittest.TestCase):
    def test_parse_executed_txn_count_uses_latest_complete_counter(self) -> None:
        text = "\n".join(
            [
                "Executed Txn Cnt = 1000",
                "noise",
                "Executed Txn Cnt = 42000",
                "Executed Txn Cnt",
            ]
        )

        self.assertEqual(parse_executed_txn_count(text), 42000)

    def test_format_progress_line_shows_bottleneck_fields(self) -> None:
        progress = {
            "10.10.2.31": {
                "running": "1",
                "pid": "123",
                "txn": "42000",
                "result_ready": "0",
                "timeseries_rows": "17",
                "log_age_s": "2",
            },
            "10.10.2.32": {
                "running": "0",
                "pid": "",
                "txn": "39000",
                "result_ready": "1",
                "timeseries_rows": "16",
                "log_age_s": "9",
            },
        }

        line = format_progress_line(progress, elapsed_s=12, timeout_s=60)

        self.assertIn("elapsed=12/60s", line)
        self.assertIn("10.10.2.31:run pid=123 txn=42000 result=0 ts_rows=17 log_age=2s", line)
        self.assertIn("10.10.2.32:done pid=- txn=39000 result=1 ts_rows=16 log_age=9s", line)

    def test_parse_timeseries_metrics_reports_total_edge_weight(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            node_dir = Path(tmp)
            (node_dir / "affinity_timeseries.0.csv").write_text(
                "\n".join(
                    [
                        "wall_ms,elapsed_ms,edgecut,n_vertices,n_edges,total_edge_weight,node_access_vertices,partition_runs",
                        "1,1,8,10,4,40,10,1",
                        "2,2,6,12,5,30,12,2",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            metrics = parse_timeseries_metrics(node_dir)

        self.assertEqual(metrics["affinity_total_edge_weight_final"], "30")
        self.assertEqual(metrics["affinity_weighted_cut_ratio_final"], "0.200000")
        self.assertEqual(metrics["affinity_weighted_cut_ratio_best"], "0.200000")


if __name__ == "__main__":
    unittest.main()
