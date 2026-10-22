import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import benchmark_report as report


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def trial(self, nodes=2):
        trial = report.result_path(self.root, 'ycsb', 'lr0.5_theta_0.95_wr_0.9', 'mix', 0)
        trial.mkdir(parents=True)
        (trial / 'summary_human.txt').write_text(f'node_count={nodes}\nbench_name=ycsb\nsystem_name=mix\n')
        return trial

    def node(self, trial, idx, values):
        p = trial / f'node{idx}'
        p.mkdir()
        (p / 'result.txt').write_text('\n'.join(f'{k}={v}' for k, v in values.items()))

    def test_weighted_counts_and_means_not_node_average(self):
        trial = self.trial()
        for idx, attempts, commits, wait, batches, records in ((0, 10, 5, 1, 1, 10), (1, 90, 90, 3, 9, 18)):
            self.node(trial, idx, dict(ycsb_tx0_try_commit=f'{attempts} {commits}',
                lock_wait_time=wait, log_flush_count=batches, log_flush_total_batch=records,
                lock_wait_us_p99=1000 * (idx + 1), throughput=commits))
        text, summary = report.build_report(trial)
        self.assertEqual(summary['abort_pct'], 5)
        self.assertEqual(summary['lock_wait_ms_per_attempt'], 40)
        self.assertIn('2.800', text)  # 28 records / 10 batches; NOT (10 + 2)/2.
        self.assertIn('end-to-end and per-path timings are unavailable', text)
        cluster_line = next(l for l in text.splitlines() if l.startswith('lock_wait_stats') and 'cluster' in l)
        self.assertGreaterEqual(cluster_line.count('N/A'), 2)

    def test_per_attempt_and_per_path_denominators(self):
        trial = self.trial(1)
        values = dict(ycsb_tx0_try_commit='10 6', stats_txn_completed=10, stats_txn_committed=6,
                      stats_txn_aborted=4, stats_txn_total_ns=100000000,
                      stats_txn_committed_ns=72000000, stats_txn_aborted_ns=28000000)
        for name, value in zip(report.STAGES, (10000000, 40000000, 20000000, 10000000)):
            values[f'stats_txn_{name}_ns'] = value
        values.update(stats_path_mix_lazy_completed=2, stats_path_mix_lazy_total_ns=30000000,
                      stats_path_mix_2pc_completed=8, stats_path_mix_2pc_total_ns=70000000)
        self.node(trial, 0, values)
        text, summary = report.build_report(trial)
        self.assertEqual(summary['avg_completed_attempt_ms'], 10)
        other = next(l for l in text.splitlines() if l.startswith('txn_time_breakdown') and 'other' in l)
        self.assertIn('2.000', other)
        self.assertIn('15.000', next(l for l in text.splitlines() if l.startswith('execution_path_timing') and 'mix_lazy' in l))
        self.assertIn('8.750', next(l for l in text.splitlines() if l.startswith('execution_path_timing') and 'mix_2pc' in l))

    def test_missing_node_and_missing_storage_are_not_zero(self):
        trial = self.trial()
        self.node(trial, 0, dict(ycsb_tx0_try_commit='10 10', throughput=99))
        text, summary = report.build_report(trial)
        self.assertIsNone(summary['attempts'])
        self.assertIsNone(summary['legacy_sum_worker_tps'])
        self.assertIn('Incomplete nodes', text)
        self.assertIn('Storage statistics unavailable', text)

    def test_empty_trial_and_relative_single_trial(self):
        trial = self.trial(1)
        text, summary = report.build_report(trial)
        self.assertIn('(0 rows)', text)
        self.assertIsNone(summary['attempts'])
        self.assertEqual(report.discover(trial), [trial.resolve()])

    def test_storage_snapshot_and_legacy_file_preserved(self):
        trial = self.trial(1)
        self.node(trial, 0, dict(ycsb_tx0_try_commit='10 10'))
        (trial / 'storage').mkdir()
        (trial / 'storage/storage_timing_stats.txt').write_text('storage_logwrite_count=17\nstorage_log_fdatasync_count=4\n')
        old = (trial / 'summary_human.txt').read_bytes()
        report.write_stats(trial)
        self.assertEqual((trial / 'summary_human.txt').read_bytes(), old)
        text = (trial / 'stats.log').read_text()
        self.assertNotIn('Storage statistics unavailable', text)
        self.assertIn('17', next(l for l in text.splitlines() if l.startswith('storage_log_stats')))

    def test_repeat_and_configuration_resume_identity(self):
        trial = self.trial(1)
        metadata = dict(status='complete', run_fingerprint='A', bench_name='ycsb', system_name='mix', case_name='case', round=0)
        (trial / 'metadata.json').write_text(json.dumps(metadata))
        self.assertEqual(report.completed_keys(self.root, 'A'), {('ycsb', 'mix', 'case', 0)})
        self.assertFalse(report.completed_keys(self.root, 'B'))
        self.assertNotIn(('ycsb', 'mix', 'case', 1), report.completed_keys(self.root, 'A'))

    def test_legacy_layout_and_symlink_are_not_double_counted(self):
        trial = self.trial(1)
        legacy = self.root / 'legacy/round_00/ycsb_mix'
        legacy.mkdir(parents=True)
        (legacy / 'lr0.5_theta_0.95_wr_0.9').symlink_to(trial, target_is_directory=True)
        self.assertEqual(report.discover(self.root), [trial.resolve()])
        # Existing plotting interface can still read the compatibility view.
        spec = importlib.util.spec_from_file_location('old_report', ROOT / 'generate_report.py')
        old_report = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(old_report)
        self.assertEqual(old_report.discover_available_modes(str(legacy.parent)), ['ycsb_mix'])
        older = self.root / 'old/round_00/ycsb_lazy/lr0.5'
        older.mkdir(parents=True)
        (older / 'summary_human.txt').write_text('node_count=1\n')
        self.assertEqual(report.discover(self.root / 'old'), [older.resolve()])

    def test_redaction_and_fingerprint(self):
        data = dict(host='test', ssh_passwords=['secret'], nested=dict(token='secret', threads=10))
        cleaned = report.redact(data)
        self.assertNotIn('secret', json.dumps(cleaned))
        self.assertEqual(report.fingerprint(data), report.fingerprint(cleaned))


if __name__ == '__main__':
    unittest.main()
