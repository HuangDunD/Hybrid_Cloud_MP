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


class CommitPhaseReportTest(unittest.TestCase):
    def test_modern_legacy_and_missing_phase_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            case = Path(directory)
            node = case / "node0"
            node.mkdir()
            result = node / "result.txt"
            raw = ("Test_try_commit=10 9\ntx_write_prepare_log_time=0.003\n"
                   "tx_write_backup_log_time=0\ntx_write_commit_log_time2=0\n"
                   "single_txn_count=0\ndistribute_txn_count=0\n")
            result.write_text(raw)
            metadata = {"node_count": 1, "system_name": "2pc"}
            self.assertAlmostEqual(write_stats(case, metadata)["prepare_ms_per_attempt"], 0.3)
            result.write_text(raw + "stats_commit_multi_writer_prepare_ns=8000000\n"
                              "stats_commit_multi_writer_decision_log_ns=0\n"
                              "stats_commit_multi_writer_count=2\nstats_commit_read_only_count=4\n"
                              "stats_commit_single_local_count=3\nstats_commit_single_remote_count=1\n")
            overview = write_stats(case, metadata)
            self.assertAlmostEqual(overview["prepare_ms_per_attempt"], 0.8)
            self.assertEqual(overview["backup_ms_per_attempt"], 0)
            self.assertIsNone(overview["distributed_commit_ms_per_attempt"])
            paths = rows((case / "stats.log").read_text(), "commit_path_stats")
            self.assertEqual(paths["multi_writer_2pc"][0], "2")
            self.assertEqual(paths["single_writer_fast_path"][0], "8")
            self.assertIsNone(write_stats(case, dict(metadata, node_count=2))["prepare_ms_per_attempt"])


class CornusTimingReportTest(unittest.TestCase):
    def test_signed_lead_and_post_flush_breakdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = Path(tmp)
            (case / "node0").mkdir()
            (case / "node0/result.txt").write_text(
                "cornus_notify_ahead_count=2\ncornus_notify_ahead_ns=3000\n"
                "cornus_notify_behind_count=1\ncornus_notify_behind_ns=6000\n")
            (case / "storage").mkdir()
            source = case / "storage/storage_timing_stats.txt"
            phases = dict(batch=16000, decode=3000, parse=1000, encode=4000,
                          vote_lock_wait=2000, vote_lock_hold=2500, dispatch=1500)
            source.write_text("".join(f"storage_prepare_{p}_count=2\nstorage_prepare_{p}_ns={n}\n"
                                      for p, n in phases.items()))
            write_stats(case, {"node_count": 1})
            text = (case / "stats.log").read_text()
            self.assertEqual(rows(text, "storage_prepare_timing")["scan_other"][2], "1.000")
            self.assertIn(["2", "1", "-1.000"], [v[:3] for v in rows(text, "cornus_notify_timing").values()])
            source.unlink()
            write_stats(case, {"node_count": 2})
            self.assertEqual(rows((case / "stats.log").read_text(), "storage_prepare_timing")["encode"][0], "N/A")


class StorageDeviceReportTest(unittest.TestCase):
    def report(self, byte_samples, section="storage_device_bytes"):
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
            section_rows = rows((case / "stats.log").read_text(), section)
            return next(iter(section_rows.values()))

    def test_cpu_uses_tps_window_and_process_ticks(self):
        samples = [dict(host_ticks=i * 800, busy_ticks=i * 400, steal_ticks=i * 8, logical_cpus=8,
                        storage_pid=123, storage_start="100", storage_cpu_s=t)
                   for i, t in enumerate([0, 4, 6, 9, 15])]
        self.assertEqual(self.report(samples, "storage_cpu"), ["8", "50.000", "1.000", "2.500", "3.000"])
        samples[2]["storage_start"] = "200"
        self.assertEqual(self.report(samples, "storage_cpu")[-2:], ["N/A", "N/A"])

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
