"""Exercise the real runner/report orchestration with all cluster I/O mocked.

Load function definitions only: importing the legacy runner installs packages,
replaces stdout, and loads deployment credentials at module scope.
"""
import ast
import csv
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import benchmark_report as report


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        tree = ast.parse((ROOT / 'test_multi_node.py').read_text())
        functions = ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)], type_ignores=[])
        self.ns = dict(os=os, Path=Path, json=json, threading=threading, logging=Mock(),
                       time=Mock(strftime=Mock(return_value='test-run')))
        for name in ('redact', 'fingerprint', 'result_path', 'completed_keys', 'write_stats', 'write_comparison'):
            self.ns[name] = getattr(report, name)
        exec(compile(functions, 'test_multi_node.py', 'exec'), self.ns)
        self.ns.update(workspace=str(self.root), result_root_name='result',
            remote_build_dir='/mock/build', remote_server_host='mock-storage', remote_server_port=22,
            remote_server_user='test', remote_server_passwd='', compute_server_hostnames=['node0', 'node1'],
            compute_server_ports=[22, 22], compute_server_usernames=['test', 'test'], compute_server_passwords=['', ''],
            bench_names=['ycsb'], modes=['lazy', '2pc', 'mix'], repeats=2, thread_num=1,
            local_ratios=[0.5], write_txn_ratios=[0.9], use_zipfian=True, zipfian_theta=[0.95],
            tx_hot_list=[90], hybrid_skew_thresholds=[0.2], hot_key_top_ns=[100], lock_modes=[1],
            attempt_num_by_bench={'ycsb': 10}, handshake_stagger_sec=0)
        self.settings = dict(config={'compute_node_config.json': {'local_compute_node': {'generate_log': 1}}},
                             code_revision='test', source_fingerprint='source')
        self.ns['run_settings'] = Mock(return_value=self.settings)
        for name in ('bootstrap_cluster', 'ssh_client', 'distribute_config_to_node',
                     'ensure_compute_killed', 'remove_remote_compute_outputs', 'update_remote_compute_config',
                     'update_access_pattern', 'update_attempt_num', 'fetch_storage_stats',
                     'cleanup_compute_logs_all_nodes', 'collect_compute_debug_snapshot'):
            self.ns[name] = Mock()
        self.ns['start_remote_services_checked'] = Mock(return_value=True)
        self.ns['check_service_running'] = Mock(return_value=True)
        self.ns['start_compute_blocking'] = Mock(return_value=(0, '', ''))
        self.ns['fetch_node_results'] = self.fetch_node
        for context in (patch.dict(os.environ, {'AUTO_BOOTSTRAP': '0'}, clear=True), patch('socket.socket')):
            context.start()
            self.addCleanup(context.stop)

    def fetch_node(self, client, idx, directory, build_dir, header=None):
        node = Path(directory, f'node{idx}')
        node.mkdir(parents=True, exist_ok=True)
        (node / 'result.txt').write_text('ycsb_tx0_try_commit=10 8\nthroughput=8\n'
                                       'tx_begin_time=0.01\ntx_exe_time=0.2\ntx_commit_time=0.1\ntx_abort_time=0.03\n')

    def test_layout_legacy_view_and_resume(self):
        self.ns['main']()
        run = self.root / 'result/test-run'
        trials = report.discover(run)
        self.assertEqual(len(trials), 6)
        for trial in trials:
            self.assertIn(trial.parent.name, ('lazy', '2pc', 'mix'))
            self.assertIn(trial.name, ('repeat_01', 'repeat_02'))
            self.assertTrue((trial / 'stats.log').is_file())
            self.assertEqual(json.loads((trial / 'metadata.json').read_text())['status'], 'complete')
            self.assertIn('ycsb_tx0_try=20.0', (trial / 'summary_human.txt').read_text())
        links = list((run / 'legacy').glob('round_*/*/*'))
        self.assertEqual(len(links), 6)
        self.assertTrue(all(p.is_symlink() for p in links))
        with (run / 'comparison.csv').open() as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 6)
        self.assertEqual(self.ns['start_compute_blocking'].call_count, 12)
        with patch.dict(os.environ, {'RESUME_FROM': str(run)}):
            self.ns['main']()
        self.assertEqual(self.ns['start_compute_blocking'].call_count, 12)

        # A failed forced rerun must invalidate old completion and stale snapshots.
        first = report.result_path(run, 'ycsb', 'lr0.5_theta_0.95_wr_0.9_skew_0.2_topn_100_lock1', 'lazy', 0)
        (first / 'storage').mkdir()
        (first / 'storage/storage_timing_stats.txt').write_text('storage_logwrite_count=123\n')
        self.ns['start_compute_blocking'].return_value = (1, '', 'simulated failure')
        with patch.dict(os.environ, {'RESUME_FROM': str(run), 'SKIP_COMPLETED': '0'}):
            with self.assertRaises(RuntimeError):
                self.ns['main']()
        self.assertEqual(json.loads((first / 'metadata.json').read_text())['status'], 'running')
        self.assertFalse((first / 'storage/storage_timing_stats.txt').exists())
        self.assertEqual(len(report.completed_keys(run, report.fingerprint(self.settings))), 5)
        with (run / 'comparison.csv').open() as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 5)

    def test_bad_resume_rejected_before_cluster_changes(self):
        with patch.dict(os.environ, {'RESUME_FROM': str(self.root), 'AUTO_BOOTSTRAP': '1'}):
            with self.assertRaises(ValueError):
                self.ns['main']()
        self.ns['bootstrap_cluster'].assert_not_called()
        self.ns['ssh_client'].assert_not_called()


if __name__ == '__main__':
    unittest.main()
