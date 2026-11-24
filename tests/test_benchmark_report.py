import json
import tempfile
import unittest
from pathlib import Path

from tools.benchmark.report import write_stats


def rows(report, section):
    result = {}
    for line in report.splitlines():
        cells = [cell.strip() for cell in line.split("|")]
        if cells[0] == section:
            result[cells[1]] = cells[2:]
    return result


class MvccReportTest(unittest.TestCase):
    def report(self, nodes, expected, mode="lazy"):
        with tempfile.TemporaryDirectory() as directory:
            case = Path(directory)
            for idx, values in enumerate(nodes):
                node = case / f"node{idx}"
                node.mkdir()
                (node / "result.txt").write_text("".join(f"{k}={v}\n" for k, v in values.items()))
            write_stats(case, {"node_count": expected, "system_name": mode})
            return (case / "stats.log").read_text()

    def test_cluster_values_and_zero_calls(self):
        report = self.report([
            {"tso_snapshot_rpc_count": 1, "tso_snapshot_rpc_ns": 1000, "tso_commit_rpc_count": 0,
             "tso_commit_rpc_ns": 0, "undo_pages": 4, "undo_evictions": 0, "mvcc_history_steps": 11},
            {"tso_snapshot_rpc_count": 3, "tso_snapshot_rpc_ns": 15000, "tso_commit_rpc_count": 0,
             "tso_commit_rpc_ns": 0, "undo_pages": 6, "undo_evictions": 0},
        ], expected=2)
        timings = rows(report, "mvcc_timing")
        self.assertEqual(timings["tso_snapshot_rpc"][:3], ["4", "0.016", "4.000"])
        self.assertEqual(timings["tso_commit_rpc"][:3], ["0", "0.000", "N/A"])
        activity = rows(report, "undo_activity")
        self.assertEqual(set(activity), {"undo_pages", "undo_evictions", "undo_permission_transfers",
                         "undo_remote_fetches", "undo_capacity_rejects", "undo_admitted_reservations",
                         "mvcc_history_steps"})
        self.assertEqual(activity["undo_pages"][:4], ["end gauge", "10", "4", "6"])
        self.assertEqual(activity["undo_evictions"][:4], ["counter", "0", "0", "0"])
        self.assertEqual(activity["mvcc_history_steps"][:4], ["counter", "N/A", "11", "N/A"])

    def test_incomplete_and_non_mvcc_trials_keep_missing_values(self):
        incomplete = self.report([{"undo_pages": 7}], expected=2)
        self.assertEqual(rows(incomplete, "undo_activity")["undo_pages"][:3], ["end gauge", "N/A", "7"])
        legacy = self.report([{}], expected=1, mode="eager")
        self.assertEqual(rows(legacy, "mvcc_timing")["tso_snapshot_rpc"][:3], ["N/A"] * 3)
        self.assertEqual(rows(legacy, "undo_activity")["undo_evictions"][:3], ["counter", "N/A", "N/A"])


class StorageDeviceReportTest(unittest.TestCase):
    def report(self, byte_samples):
        with tempfile.TemporaryDirectory() as directory:
            case = Path(directory)
            for idx, bounds in enumerate(((0, 4), (1, 3))):
                node = case / f"node{idx}"
                node.mkdir()
                (node / "result.txt").touch()
                (node / "compute.out").write_text("".join(
                    f"[2026-11-24 12:00:0{second}.000] [Node {idx}] [Commit TPS] 1\n" for second in bounds))
            storage = case / "storage"
            storage.mkdir()
            with (storage / "disk_io.jsonl").open("w") as output:
                for second, counters in enumerate(byte_samples):
                    sample = dict(device="disk0", time=f"2026-11-24 12:00:0{second}",
                                  monotonic_s=100 + second, reads=10 * second, writes=4 * second, **counters)
                    output.write(json.dumps(sample) + "\n")
            write_stats(case, {"node_count": 2})
            return rows((case / "stats.log").read_text(), "storage_device_bytes")["disk0"]

    def test_bandwidth_uses_common_tps_window(self):
        # Samples at 00 and 04 lie outside the 01..03 overlap of the two nodes.
        mib = 2**20
        samples = [dict(read_bytes=r * mib, write_bytes=w * mib)
                   for r, w in ((1, 1), (50, 75), (52, 75), (53, 76), (90, 80))]
        values = self.report(samples)
        self.assertEqual(values[:5], ["2", str(3 * mib), str(mib), "1.500", "0.500"])
        self.assertIn("drain", values[5])

    def test_legacy_counters_and_measured_zero(self):
        self.assertEqual(self.report([{}] * 5)[:5], ["2", "N/A", "N/A", "N/A", "N/A"])
        self.assertEqual(self.report([{"read_bytes": 17}] * 5)[:5], ["2", "0", "N/A", "0.000", "N/A"])
