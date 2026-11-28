"""Run short three-node workload cases on AliYun1 and verify the drained storage files."""
import argparse
from collections import Counter
from contextlib import ExitStack
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import test_multi_node as bench


def verify(report, storage, binary, case, settings, mode):
    name = case['workload']
    types = (['GetSubscriberData', 'GetNewDestination', 'GetAccessData', 'UpdateSubscriberData',
              'UpdateLocation', 'InsertCallForwarding', 'DeleteCallForwarding'] if name == 'tatp'
             else ['ReadOnly', 'ReadWrite'])
    counts = Counter()
    results = list(report.glob('node*/result.txt'))
    assert len(results) == case['nodes']
    for result in results:
        values = bench.read_kv(result)
        attempts = sum(values[t + '_try_commit'][0] for t in types)
        commits = sum(values[t + '_try_commit'][1] for t in types)
        for t in types:
            tried, committed = values[t + '_try_commit']
            counts[t + '_try'] += tried
            counts[t + '_commit'] += committed
        counts.update({k: v for k, v in values.items() if k.startswith('oltp_')})
        assert attempts == case['threads'] * case['attempts'] and commits > 0
        assert values['oltp_business_misses'] <= attempts - commits
        assert sum(values[f'oltp_{n}_writer_attempts'] for n in ('zero', 'single', 'multi')) == attempts
        if case['local_ratio'] == 1:
            cross_range = (name == 'sysbench' and mode.startswith('2pc') and case['nodes'] > 1 and
                           settings['config']['sysbench_config.json']['sysbench']['range_size'] > 1)
            if cross_range:
                assert values['oltp_all_local_attempts'] < attempts
            else:
                assert values['oltp_all_local_attempts'] == attempts
        if case['write_txn_ratio'] in (0, 1):
            assert values['oltp_zero_writer_attempts'] == attempts * (1 - case['write_txn_ratio'])
    if case['write_txn_ratio'] == 0.2:
        assert all(counts[t + '_commit'] > 0 for t in types)
    if name == 'tatp':
        assert counts['oltp_multi_writer_attempts'] == 0
    else:
        assert counts['oltp_business_misses'] == 0
        if case['local_ratio'] < 1 and case['write_txn_ratio'] > 0:
            assert counts['oltp_multi_writer_attempts'] > 0
    delta = counts['InsertCallForwarding_commit'] - counts['DeleteCallForwarding_commit']
    scale = settings['config'][name + '_config.json'][name]['subscribers' if name == 'tatp' else 'table_size']
    output = subprocess.check_output([binary, name, str(scale), str(delta), str(int(case['write_txn_ratio'] == 0))],
                                     cwd=storage, text=True)
    (report / 'data_verification.txt').write_text(output + json.dumps(counts, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--result-dir', required=True, type=Path)
    args = parser.parse_args()
    if ROOT != Path('/data/workspace/Hybrid_Cloud_MP'):
        parser.error('This test uses the isolated AliYun1 runtime of test_multi_node.py')
    args.result_dir.mkdir(parents=True, exist_ok=False)
    storage_config = json.loads((ROOT / 'config/storage_node_config.json').read_text())
    storage_config['local_storage_node']['wal_preallocate_size'] = '32MB'
    capture, distribute, run = bench.run_settings, bench.distribute_config_to_node, bench.run_case_mode

    def settings():
        result = capture()
        result['config']['storage_node_config.json'] = storage_config
        return result

    def stage_config(client, mode):
        assert bench.isolated_runtime
        distribute(client, mode)
        with client.open_sftp() as sftp:
            with sftp.open(bench.remote_workspace + '/config/storage_node_config.json', 'w') as dest:
                dest.write(json.dumps(storage_config))

    with tempfile.TemporaryDirectory() as tmp, ExitStack() as overrides:
        binary = Path(tmp) / 'verify_native_oltp'
        subprocess.run(['c++', '-std=c++17', '-O2', '-I.', '-Icommon', '-Icore', '-Iworkload',
                        'tests/integration/verify_native_oltp.cc', '-o', binary], cwd=ROOT, check=True)

        def run_and_check(root, case, mode, settings, fingerprint):
            run(root, case, mode, settings, fingerprint)
            # The driver has drained redo and stopped storage. Check before it releases the case directory.
            verify(Path(bench.plan.result_path(root, case, mode)), Path(bench.remote_build_dir) / 'storage_server',
                   binary, case, settings, mode)

        overrides.enter_context(patch.multiple(bench, run_settings=settings, distribute_config_to_node=stage_config,
            run_case_mode=run_and_check, bench_names=['tatp', 'sysbench'], thread_num=2, run_mode='count',
            prewarm_seconds=0, zipfian_theta=[0.6], tx_hot_list=[75]))
        profiles = [
            ('mixed', True, [0.2], [0.5], ['lazy', '2pc', 'mix']),
            ('endpoints', False, [0, 1], [0, 1], ['mix']),
            ('cornus', True, [0.2], [0.5], ['2pc-cornus', '2pc-cornus-opt']),
        ]
        for label, zipf, writes, local, modes in profiles:
            with patch.multiple(bench, use_zipfian=zipf, write_txn_ratios=writes, local_ratios=local, modes=modes,
                                attempt_num_by_bench=dict(bench.attempt_num_by_bench,
                                                          tatp=300 if zipf else 90, sysbench=75 if zipf else 40)):
                bench.main(['--result-dir', str(args.result_dir / label)])


if __name__ == '__main__':
    main()
