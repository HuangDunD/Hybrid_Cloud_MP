import sys
import tempfile
import unittest
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import multinode_mprouter_smoke as mprouter_smoke
from multinode_mprouter_smoke import run_mprouter
from multinode_parmetis_smoke import make_configs


class MultinodeMPRouterSmokeTest(unittest.TestCase):
    def make_config_args(self, **overrides):
        values = dict(
            parallel_page_fetch=1,
            threads=4,
            disable_wal=False,
            random_generate=False,
            log_flush_interval_ms=3,
            log_flush_batch_trigger=16,
            log_flush_notify_threshold=4,
            push_page_scheduler_threads=12,
            partition_cycle_ms=5000,
            migration_tick_ms=200,
            migration_batch=200,
            migration_workers=1,
            edge_min_weight=1.0,
            edge_decay_factor=0.5,
            repart_itr=5000.0,
            ubvec=1.20,
            max_changed_vertices_ratio=1.0,
            zipf_theta=None,
            num_accounts=500000,
            num_hot_accounts=100000,
            attempted_num=5000,
            use_zipfian=1,
        )
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_make_configs_uses_push_page_scheduler_threads(self) -> None:
        configs = make_configs(
            ["10.10.2.31", "10.10.2.32", "10.10.2.33", "10.10.2.34"],
            "10.10.2.38",
            self.make_config_args(push_page_scheduler_threads=12),
            enable_affinity=True,
        )

        compute_cfg = json.loads(configs["compute_node_config.json"])
        self.assertEqual(compute_cfg["push_page_scheduler_threads"], 12)

    def test_make_configs_sets_storage_random_generate(self) -> None:
        configs = make_configs(
            ["10.10.2.31", "10.10.2.32", "10.10.2.33", "10.10.2.34"],
            "10.10.2.38",
            self.make_config_args(random_generate=True),
            enable_affinity=True,
        )

        storage_cfg = json.loads(configs["storage_node_config.json"])
        self.assertTrue(storage_cfg["local_storage_node"]["random_generate"])

    def test_make_configs_sets_affinity_edge_decay_factor(self) -> None:
        configs = make_configs(
            ["10.10.2.31", "10.10.2.32", "10.10.2.33", "10.10.2.34"],
            "10.10.2.38",
            self.make_config_args(edge_decay_factor=0.8),
            enable_affinity=True,
        )

        compute_cfg = json.loads(configs["compute_node_config.json"])
        self.assertEqual(compute_cfg["affinity"]["edge_decay_factor"], 0.8)

    def test_make_configs_sets_affinity_partition_tuning(self) -> None:
        configs = make_configs(
            ["10.10.2.31", "10.10.2.32", "10.10.2.33", "10.10.2.34"],
            "10.10.2.38",
            self.make_config_args(
                repart_itr=1000.0,
                ubvec=1.10,
                max_changed_vertices_ratio=0.3,
            ),
            enable_affinity=True,
        )

        compute_cfg = json.loads(configs["compute_node_config.json"])
        affinity_cfg = compute_cfg["affinity"]
        self.assertEqual(affinity_cfg["repart_itr"], 1000.0)
        self.assertEqual(affinity_cfg["ubvec"], 1.10)
        self.assertEqual(affinity_cfg["max_changed_vertices_ratio"], 0.3)

    def test_make_configs_sets_hot_account_offset(self) -> None:
        configs = make_configs(
            ["10.10.2.31", "10.10.2.32", "10.10.2.33", "10.10.2.34"],
            "10.10.2.38",
            self.make_config_args(hot_account_offset=250000),
            enable_affinity=True,
        )

        smallbank_cfg = json.loads(configs["smallbank_config.json"])
        smallbank_aff_cfg = json.loads(configs["smallbank_aff_config.json"])
        self.assertEqual(smallbank_cfg["smallbank"]["hot_account_offset"], 250000)
        self.assertEqual(
            smallbank_aff_cfg["smallbank_aff"]["hot_account_offset"], 250000
        )

    def test_run_mprouter_passes_system_mode(self) -> None:
        class FakeProcess:
            pid = 4321

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            args = SimpleNamespace(
                mprouter_dir=tmp_path,
                mprouter_zipfian_theta=0.8,
                num_accounts=500000,
                worker_threads=16,
                try_count=30000,
                mprouter_affinity_txn_ratio=0.5,
                mprouter_system_mode=24,
                batch_size=1000,
                num_bucket=4,
                warmup_rounds=0,
                timeout=900,
            )

            with mock.patch("multinode_mprouter_smoke.subprocess.Popen",
                            return_value=FakeProcess()) as popen:
                run_mprouter(
                    args,
                    tmp_path,
                    tmp_path / "mprouter.log",
                    "10.10.2.31:9115",
                )

            cmd = popen.call_args.args[0]
            mode_idx = cmd.index("--system-mode")
            self.assertEqual(cmd[mode_idx + 1], "24")

    def test_write_schism_compare_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            mprouter_smoke.write_schism_compare_summary(
                out_dir,
                {"mprouter_throughput_tps": "100"},
                {"mprouter_throughput_tps": "110"},
                {"mprouter_throughput_tps": "130"},
            )

            text = (out_dir / "schism_compare_summary.txt").read_text(
                encoding="utf-8"
            )
            self.assertIn("baseline_throughput_tps=100.000000\n", text)
            self.assertIn("schism_static_throughput_tps=110.000000\n", text)
            self.assertIn("affinity_throughput_tps=130.000000\n", text)
            self.assertIn(
                "affinity_vs_schism_throughput_delta_pct=18.18\n",
                text,
            )
            self.assertIn(
                "affinity_vs_baseline_throughput_delta_pct=30.00\n",
                text,
            )

    def test_run_mprouter_cleans_process_group_on_keyboard_interrupt(self) -> None:
        class FakeProcess:
            pid = 4321

            def wait(self, timeout=None):
                raise KeyboardInterrupt

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            args = SimpleNamespace(
                mprouter_dir=tmp_path,
                mprouter_zipfian_theta=0.8,
                num_accounts=500000,
                worker_threads=16,
                try_count=30000,
                mprouter_affinity_txn_ratio=0.5,
                mprouter_system_mode=23,
                batch_size=1000,
                num_bucket=4,
                warmup_rounds=0,
                timeout=900,
            )

            with mock.patch("multinode_mprouter_smoke.subprocess.Popen",
                            return_value=FakeProcess()):
                with mock.patch("multinode_mprouter_smoke.os.killpg") as killpg:
                    with self.assertRaises(KeyboardInterrupt):
                        run_mprouter(
                            args,
                            tmp_path,
                            tmp_path / "mprouter.log",
                            "10.10.2.31:9115",
                        )

            killpg.assert_called_once_with(4321, 15)


if __name__ == "__main__":
    unittest.main()
