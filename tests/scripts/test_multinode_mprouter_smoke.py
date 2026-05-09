import sys
import tempfile
import unittest
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from multinode_mprouter_smoke import run_mprouter
from multinode_parmetis_smoke import make_configs


class MultinodeMPRouterSmokeTest(unittest.TestCase):
    def make_config_args(self, **overrides):
        values = dict(
            parallel_page_fetch=1,
            threads=4,
            disable_wal=False,
            log_flush_interval_ms=3,
            log_flush_batch_trigger=16,
            log_flush_notify_threshold=4,
            push_page_scheduler_threads=12,
            partition_cycle_ms=5000,
            migration_tick_ms=200,
            migration_batch=200,
            migration_workers=1,
            edge_min_weight=1.0,
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
