#!/usr/bin/env python3
"""Isolated E0 fixtures and L0 pilot; never launches or kills cluster services."""
import argparse
import dataclasses
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys
import tempfile

from priority_scheduler import Options, fixture, simulate, fixture_document, fixture_fingerprint, load_fixture

ROOT = Path(__file__).resolve().parents[2]


def execute(command, directory, env=None, timeout=60):
    directory.mkdir()
    env = dict(os.environ if env is None else env, HCM_TEST_OUTPUT_ROOT=str(directory / 'test-artifacts'))
    (directory / 'command.json').write_text(json.dumps([str(p) for p in command]))
    import resource
    import time
    wall = time.perf_counter_ns()
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        result = subprocess.run([str(p) for p in command], cwd=directory, env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        output = error.stdout or b''
        (directory / 'output.log').write_text(output.decode(errors='replace') if isinstance(output, bytes) else output)
        (directory / 'status.json').write_text(json.dumps({'outcome': 'timeout', 'timeout_seconds': timeout}))
        raise
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    (directory / 'status.json').write_text(json.dumps({'outcome': 'exit', 'exit_code': result.returncode,
        'wall_us': (time.perf_counter_ns() - wall) / 1000,
        'process_cpu_us': (after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime) * 1000000,
        'input_blocks': after.ru_inblock - before.ru_inblock, 'output_blocks': after.ru_oublock - before.ru_oublock}))
    (directory / 'output.log').write_text(result.stdout)
    print(directory.name, 'exit=', result.returncode, flush=True)
    if result.returncode:
        raise RuntimeError(f'{directory}: {result.stdout[-4000:]}')
    return result.stdout


def trace_summary(path, window_us=2000):
    from priority_scheduler import percentile
    events = [json.loads(line) for line in path.read_text().splitlines()]
    if not events or events[0]['event'] != 'trace_config' or events[-1]['event'] != 'trace_summary':
        raise ValueError('trace did not finish cleanly')
    recorder = events[-1]
    if recorder['dropped_events'] or recorder['io_error']:
        raise ValueError('lossy trace is not an experimental ground truth')
    ordered = sorted((e for e in events if 'ts_us' in e), key=lambda e: e['ts_us'])
    stages, waits, begins, ends, first_blocks, active_blocks = {}, {}, {}, {}, {}, {}
    wait_duration, modes, lookups, state = {}, {}, {}, {}
    request_aliases, epoch_origins, lookups_by_request = {}, {}, {}
    for event in ordered:
        kind = event['event']
        request = (event['pid'], event['request'])
        if kind in ('run_begin', 'recovery_begin'):
            epoch_origins[event['epoch']] = event['ts_us']
        elif kind == 'stage':
            row = stages.setdefault(event['reason'], {'wall_us': 0, 'thread_cpu_us': 0, 'count': 0})
            row['wall_us'] += event['a']; row['thread_cpu_us'] += event['b']; row['count'] += 1
        elif kind == 'request_begin':
            if request in begins:
                raise ValueError('duplicate request admission')
            admitted = event.get('c', 0) or event['ts_us']
            if admitted > event['ts_us']:
                raise ValueError('admission time is after execution')
            begins[request] = admitted
            request_aliases[request] = (event['pid'], event.get('logical', event['request']))
        elif kind == 'request_end':
            if request not in begins or request in ends or event['a'] not in (0, 1, 2, 3):
                raise ValueError('orphan, duplicate or invalid request outcome')
            ends[request] = (event['a'], event['ts_us'])
            for lookup_id in lookups_by_request.get(request, ()):
                lookups[lookup_id]['ready'].update(state)
        elif kind == 'block':
            identity = request + (event['lookup'], event['table'], event['page'], event['reason'])
            if identity in active_blocks:
                raise ValueError('polling was counted as another blocked request')
            active_blocks[identity] = event['ts_us']
            waits[event['reason']] = waits.get(event['reason'], 0) + 1
            first_blocks.setdefault(request, event)
        elif kind == 'unblock':
            identity = request + (event['lookup'], event['table'], event['page'], event['reason'])
            if identity not in active_blocks:
                raise ValueError('unmatched unblock')
            start = active_blocks.pop(identity)
            wait_duration[event['reason']] = wait_duration.get(event['reason'], 0) + event['ts_us'] - start
        elif kind == 'key2leaf':
            modes[event['reason']] = modes.get(event['reason'], 0) + 1
        elif kind == 'fixture_not_ready':
            state.pop((event['epoch'], event['table'], event['page']), None)
        elif kind in ('fixture_ready', 'fixture_baseline_ready'):
            state[(event['epoch'], event['table'], event['page'])] = (
                epoch_origins.get(event['epoch'], event['ts_us']) if kind == 'fixture_baseline_ready' else event['ts_us'])
        elif kind == 'lookup_begin':
            lookup_id = (event['pid'], event['lookup'])
            lookups[lookup_id] = {'index': [], 'data': None, 'ready': dict(state), 'start': event['ts_us'], 'epoch': event['epoch']}
            lookups_by_request.setdefault(request, []).append(lookup_id)
        elif kind == 'path_page':
            lookup = lookups.get((event['pid'], event['lookup']))
            if lookup is not None:
                lookup['index'].append((event['epoch'], event['table'], event['page']))
        elif kind == 'lookup_target':
            lookup = lookups.get((event['pid'], event['lookup']))
            if lookup is not None:
                lookup['data'] = (event['epoch'], event['table'], event['page'])
        elif kind == 'lookup_end':
            lookup = lookups.get((event['pid'], event['lookup']))
            if lookup is not None:
                lookup.update(hit=bool(event['a']), sampled=bool(event['c']), end=event['ts_us'])
                lookup['ready'].update(state)
    latencies = [t - begins[r] for r, (s, t) in ends.items() if s == 1]
    run_start = next((e['ts_us'] for e in ordered if e['event'] == 'run_begin'), min(begins.values(), default=0))
    deadline = run_start + window_us
    window_requests = {r: a for r, a in begins.items() if run_start <= a <= deadline}
    window_ends = {r: (s, t) for r, (s, t) in ends.items() if r in window_requests and t <= deadline}
    tails, unknown_tails, not_applicable_tails = [], 0, 0
    for lookup in lookups.values():
        if not lookup.get('sampled') or 'end' not in lookup or not lookup['index']:
            unknown_tails += 1
            continue
        if not lookup['hit'] and lookup['data'] is None:
            not_applicable_tails += 1
            continue
        pages = lookup['index'] + [lookup['data']]
        if any(p not in lookup['ready'] for p in pages):
            unknown_tails += 1
        else:
            origin = epoch_origins.get(lookup['epoch'], run_start)
            index_ready = max(origin, max(lookup['ready'][p] for p in lookup['index']))
            data_ready = max(origin, lookup['ready'][lookup['data']])
            tails.append(max(0, index_ready - data_ready))
    counts = {status: sum(s == status for s, _ in ends.values()) for status in range(4)}
    return {'kind': 'REAL_FIXTURE_OBSERVATIONS_NOT_CLUSTER_RESULTS', 'stages': stages,
            'stage_totals_are_nested_not_additive': True, 'first_block_events': waits,
            'blocked_requests': len(first_blocks), 'wait_duration_us': wait_duration,
            'key2leaf_modes': modes, 'externally_verified_requests': counts[1],
            'unverified_executions': counts[0], 'failed': counts[2], 'cancelled': counts[3],
            'unfinished': len(begins) - len(ends), 'open_waits': len(active_blocks),
            'logical_requests': len(set(request_aliases.values())), 'attempts': len(begins),
            'p95_us_verified_only': percentile(latencies, .95), 'p99_us_verified_only': percentile(latencies, .99),
            'fixed_window': {'origin_us': run_start, 'duration_us': window_us, 'arrived': len(window_requests),
                'verified': sum(s == 1 for s, _ in window_ends.values()),
                'unverified': sum(s == 0 for s, _ in window_ends.values()),
                'failed': sum(s == 2 for s, _ in window_ends.values()),
                'cancelled': sum(s == 3 for s, _ in window_ends.values()),
                'unfinished': len(window_requests) - len(window_ends),
                'censored_wait_us': sum(min(ends.get(r, (0, float('inf')))[1], deadline) - a for r, a in window_requests.items())},
            'fixture_index_tail_sum_us': sum(tails) if tails else None,
            'fixture_index_tail_known': len(tails), 'fixture_index_tail_unknown': unknown_tails,
            'fixture_index_tail_not_applicable': not_applicable_tails, 'fixture_index_tail_cohort': len(lookups),
            'distributed_safe_ready_time': None, 'trace_bytes': path.stat().st_size, 'recorder': recorder,
            'backlogs': [e for e in events if e['event'].startswith('backlog_')]}


def paired_summary(rows, policies=('B0', 'B1', 'B3', 'P1', 'C0')):
    metrics = ('window_arrived', 'model_completed_in_window', 'window_failed', 'window_cancelled',
               'window_unfinished', 'window_p95_us_completed_only', 'window_p99_us_completed_only',
               'window_censored_wait_us', 'window_unfinished_wait_us', 'window_observed_wait_p95_us',
               'window_observed_wait_p99_us', 'completed', 'failed', 'cancelled', 'unfinished',
               'index_tail_sum_us_fixed_cohort', 'index_tail_known_fixed_cohort',
               'index_tail_unknown_fixed_cohort', 'index_tail_not_applicable_fixed_cohort',
               'identification_wall_us', 'identification_cpu_us', 'identification_overrun_us',
               'identification_cpu_overrun_us', 'sampling_wall_us', 'sampling_cpu_us',
               'hint_memory_estimate_bytes', 'hint_count', 'priority_used_us',
               'recognition_priority_budget_used_us', 'recovery_work_us', 'all_recovery_done_us')
    delta_pairs = [('C0', 'B0'), ('P1', 'B0'), ('P1', 'B3')]
    if 'P1F' in policies:
        delta_pairs += [('P1F', 'B0'), ('P1F', 'B3'), ('P1F', 'P1')]

    def distribution(values):
        finite = [v for v in values if v is not None]
        return {'values': values, 'median': statistics.median(finite) if finite else None,
                'min': min(finite) if finite else None, 'max': max(finite) if finite else None,
                'missing': len(values) - len(finite)}

    result = {}
    for scenario in sorted({row['scenario'] for row in rows}):
        group = sorted((r for r in rows if r['scenario'] == scenario), key=lambda r: r['repetition'])
        runs = [{r['policy']: r for r in row['results']} for row in group]
        tables = {p: {m: distribution([r[p][m] for r in runs]) for m in metrics} for p in policies}
        deltas = {}
        for left, right in delta_pairs:
            if left in runs[0] and right in runs[0]:
                deltas[f'{left}_minus_{right}'] = {m: distribution([
                    None if run[left][m] is None or run[right][m] is None else run[left][m] - run[right][m]
                    for run in runs]) for m in metrics}
        result[scenario] = {'timing_repeats': len(runs), 'independent_failure_samples': 1,
                            'fixture_sha256': group[0]['fixture_sha256'], 'policies': tables, 'paired_deltas': deltas}
    return result


def run_saved_l0(run, fixture_dir, repeats, sensitivity=False, policies=None):
    """Run paired timing repeats through --fixture; no E0 binaries or cluster processes."""
    import hashlib
    import platform
    scenarios = ('hotspot', 'index_only', 'data_only', 'switch', 'uniform')
    policies = list(policies) if policies else ['B0', 'B1', 'B3', 'P1', 'C0']
    base = dict(seed=20260915, sample_rate=.01, summary_capacity=512, candidate_limit=128,
                max_hint_pages=64, identify_budget_us=2000, identify_cpu_budget_us=2000,
                priority_budget_us=2000, publish_batch=1, max_group=3, window_us=2000)
    configs = [('primary', base)]
    if sensitivity:
        configs += [('identify_500', dict(base, identify_budget_us=500, identify_cpu_budget_us=500)),
                    ('sample_10pct', dict(base, sample_rate=.1))]
    source = fixture_dir.resolve()
    source_manifest = json.loads((source / 'summary.json').read_text())
    expected = {r['path']: r['sha256'] for r in source_manifest['prepared_fixtures']}
    inputs = run / 'fixtures'
    inputs.mkdir()
    fixtures, facts = {}, {}
    for scenario in scenarios:
        name = f'prepared-{scenario}-20260915.json'
        raw = (source / name).read_bytes()
        document = json.loads(raw)
        tasks, requests, history = load_fixture(document)
        digest = fixture_fingerprint(document)
        if expected.get(name) != digest:
            raise ValueError(f'prepared fixture fingerprint mismatch: {name}')
        if fixture_fingerprint(fixture_document(tasks, requests, history, document['preparation_us'], document['barrier_us'])) != digest:
            raise ValueError('fixture round-trip changed the common boundary')
        (inputs / name).write_bytes(raw)
        fixtures[scenario] = (inputs / name, digest)
        facts[scenario] = {'source': str(source / name), 'fixture_sha256': digest,
                           'file_sha256': hashlib.sha256(raw).hexdigest(), 'tasks': len(tasks),
                           'requests': len(requests), 'history_records': len(history),
                           'nonready_work_us': sum(t.cost_us for t in tasks if not t.ready),
                           'failed_tasks': sum(t.fail for t in tasks),
                           'requests_with_cancellation': sum(r.cancel_us != float('inf') for r in requests),
                           'incomplete_paths': sum(not r.complete for r in requests),
                           'preparation_us': document['preparation_us'], 'barrier_us': document['barrier_us']}
    rng = random.Random(20260915)
    jobs = []
    for name, config in configs:
        for repetition in range(repeats):
            for scenario in scenarios:
                order = list(policies)
                rng.shuffle(order)
                jobs.append({'configuration': name, 'options': config, 'scenario': scenario,
                             'repetition': repetition, 'policy_order': order})
    plan = {'kind': 'L0_MODEL_NOT_DATABASE_SPEEDUP', 'victim': 'A', 'seed': 20260915,
            'parallelism': 1, 'repeats': repeats, 'repetition_unit': 'same fixture, timing only, not independent failures',
            'inputs': facts, 'jobs': jobs, 'python': platform.python_version(), 'platform': platform.platform(),
            'source_sha256': {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest()
                for p in ('scripts/recovery/priority_scheduler.py', 'scripts/recovery/run_priority_pilot.py')},
            'cost_contract': 'measured identification is serially charged; pre-fault summary maintenance and historical E0 diagnostics are separate',
            'not_measured': ['database end-to-end performance', 'production sampling overhead', 'live L1/index physical Redo']}
    (run / 'plan.json').write_text(json.dumps(plan, indent=2, allow_nan=False))
    manifest = {'kind': plan['kind'], 'status': 'running', 'plan': 'plan.json', 'jobs_completed': [], 'summaries': {}}
    clean_env = {k: v for k, v in os.environ.items() if not k.startswith('HCM_TRACE_')}
    clean_env.update(PYTHONDONTWRITEBYTECODE='1', PYTHONHASHSEED='0')
    rows = {name: [] for name, _ in configs}
    try:
        for job in jobs:
            scenario, config_name = job['scenario'], job['configuration']
            fixture_path, fingerprint = fixtures[scenario]
            directory = run / f"{config_name}-{scenario}-r{job['repetition']}"
            output = directory / 'result.json'
            command = [sys.executable, '-B', ROOT / 'scripts/recovery/priority_scheduler.py',
                       '--fixture', fixture_path, '--output', output, '--scenario', scenario,
                       '--policies', ','.join(job['policy_order'])]
            for option, value in job['options'].items():
                command += ['--' + option.replace('_', '-'), str(value)]
            execute(command, directory, clean_env)
            payload = json.loads(output.read_text())
            results = payload['results']
            by_policy = {r['policy']: r for r in results}
            if list(by_policy) != job['policy_order'] or any(r['fixture_sha256'] != fingerprint for r in results):
                raise AssertionError('paired policy inputs/order differ')
            if len({json.dumps({k: v for k, v in r['options'].items() if k != 'policy'}, sort_keys=True) for r in results}) != 1:
                raise AssertionError('paired policy execution options differ')
            if by_policy['B0']['execution_order'] != by_policy['C0']['execution_order']:
                raise AssertionError('C0 changed the B0 execution order')
            if by_policy['C0']['hint_summary_sha256'] != by_policy['P1']['hint_summary_sha256']:
                raise AssertionError('C0 and P1 received different summaries')
            if 'P1F' in by_policy and by_policy['P1F']['hint_summary_sha256'] != by_policy['P1']['hint_summary_sha256']:
                raise AssertionError('P1F and P1 received different summaries')
            if len({r['recovery_work_us'] for r in results}) != 1:
                raise AssertionError('paired recovery work differs')
            for r in results:
                if r['window_arrived'] != sum(r[k] for k in ('model_completed_in_window', 'window_failed', 'window_cancelled', 'window_unfinished')):
                    raise AssertionError('window denominator does not close')
                if r['index_tail_cohort_size'] != sum(r[k] for k in ('index_tail_known_fixed_cohort', 'index_tail_unknown_fixed_cohort', 'index_tail_not_applicable_fixed_cohort')):
                    raise AssertionError('tail cohort denominator does not close')
                if any(len(p['pages']) > job['options']['publish_batch'] for p in r['publications']):
                    raise AssertionError('policy enlarged the publication batch')
                if not 0 <= r['priority_used_us'] <= r['recognition_priority_budget_used_us'] <= job['options']['priority_budget_us']:
                    raise AssertionError('priority budget violated')
            rows[config_name].append(dict(job, results=results, fixture_sha256=fingerprint))
            manifest['jobs_completed'].append(str(output.relative_to(run)))
            (run / 'progress.json').write_text(json.dumps(manifest, indent=2, allow_nan=False))
        manifest['summaries'] = {name: paired_summary(group, policies) for name, group in rows.items()}
        manifest['status'] = 'passed'
    except Exception as error:
        manifest['status'] = 'failed'
        manifest['error'] = repr(error)
        raise
    finally:
        (run / 'summary.json').write_text(json.dumps(manifest, indent=2, allow_nan=False))
    print('PASS; L0 summary:', run / 'summary.json', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binaries-dir', type=Path)
    parser.add_argument('--output-root', type=Path, required=True)
    parser.add_argument('--l0-fixture-dir', type=Path, help='only run L0 from saved prepared fixtures and summary.json')
    parser.add_argument('--policies', default=None,
                        help='comma separated policy list for saved L0 mode; default B0,B1,B3,P1,C0')
    parser.add_argument('--sensitivity', action='store_true', help='two predeclared one-factor checks after the primary run')
    parser.add_argument('--functional-only', action='store_true')
    parser.add_argument('--calibrate-observation', action='store_true',
                        help='measure fixture tracing overhead without requiring a policy performance run')
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    root = args.output_root.resolve()
    if ROOT not in root.parents:
        parser.error('output-root must be inside Hybrid_Cloud_MP')
    if not 1 <= args.repeats <= 10:
        parser.error('pilot repeats must be between 1 and 10')
    if args.l0_fixture_dir:
        if args.functional_only or args.calibrate_observation or args.binaries_dir:
            parser.error('saved L0 mode cannot be combined with E0 execution')
        if ROOT not in args.l0_fixture_dir.resolve().parents:
            parser.error('fixture directory must be inside Hybrid_Cloud_MP')
    elif not args.binaries_dir or args.sensitivity:
        parser.error('E0 requires --binaries-dir; --sensitivity requires saved L0 mode')
    root.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix='pilot-', dir=root))
    print('OUTPUT:', run, flush=True)
    if args.l0_fixture_dir:
        try:
            policies = [p.strip() for p in args.policies.split(',')] if args.policies else None
            run_saved_l0(run, args.l0_fixture_dir, args.repeats, args.sensitivity, policies)
        except Exception as error:
            if not (run / 'summary.json').exists():
                (run / 'summary.json').write_text(json.dumps({'kind': 'L0_MODEL_NOT_DATABASE_SPEEDUP', 'status': 'failed', 'error': repr(error)}))
            raise
        return
    binaries = args.binaries_dir.resolve()
    manifest = {'kind': 'E0_REAL_FIXTURES_PLUS_L0_MODEL', 'victim': 'A', 'nodes': ['A', 'B', 'C'],
                'physical_process_crash': False, 'seed': 20260915, 'run': str(run),
                'scenarios': {}, 'models': [], 'normal_overhead': [], 'prepared_fixtures': []}
    execute([sys.executable, '-B', ROOT / 'tests/test_cases/recovery_priority/test_scheduler.py'], run / 'unit')
    execute([sys.executable, '-B', ROOT / 'tests/test_cases/recovery_priority/test_trace_contract.py'], run / 'trace-unit')
    profile = None
    for name in ('replay_wait', 'blink_path', 'storage', 'lazy_fetch', 'observation'):
        directory = run / name
        env = dict(os.environ, HCM_TRACE_DIR=str(directory), HCM_TRACE_NODE='1',
                   HCM_TRACE_SAMPLE_RATE='1', HCM_TRACE_CAPACITY='131072', HCM_TRACE_SEED='20260915')
        cmd = [binaries / f'recovery_{name}_test']
        if name == 'blink_path':
            cmd.append('200')
        elif name in ('replay_wait', 'storage'):
            cmd.append(directory / 'runtime')
        execute(cmd, directory, env)
        traces = list(directory.glob('trace-*.jsonl'))
        if len(traces) != 1:
            raise RuntimeError(f'expected one trace in {directory}')
        manifest['scenarios'][name] = trace_summary(traces[0])
        if name == 'blink_path':
            profile = traces[0]
    clean_env = {key: value for key, value in os.environ.items() if not key.startswith('HCM_TRACE_')}
    execute([binaries / 'recovery_lazy_fetch_test'], run / 'lazy-disabled', clean_env)
    overflow_dir = run / 'trace-overflow'
    execute([binaries / 'recovery_observation_test', '--overflow'], overflow_dir,
            dict(clean_env, HCM_TRACE_DIR=str(overflow_dir), HCM_TRACE_CAPACITY='32'))
    overflow_trace = next(overflow_dir.glob('trace-*.jsonl'))
    try:
        trace_summary(overflow_trace)
    except ValueError:
        manifest['overflow_rejected'] = True
    else:
        raise RuntimeError('lossy trace accepted as experimental ground truth')
    for scenario in ('hotspot', 'index_only', 'data_only', 'switch', 'uniform'):
        tasks, requests, history = fixture(20260915, scenario, profile)
        document = fixture_document(tasks, requests, history)
        fingerprint = fixture_fingerprint(document)
        second = fixture_document(*fixture(20260915, scenario, profile))
        if fingerprint != fixture_fingerprint(second):
            raise RuntimeError('fixture generation is not deterministic')
        target = run / f'prepared-{scenario}-20260915.json'
        target.write_text(json.dumps(document, indent=2, allow_nan=False))
        manifest['prepared_fixtures'].append({'path': target.name, 'sha256': fingerprint,
            'kind': 'OBSERVED_PATHS_SYNTHETIC_FAILURE_COST_AND_ARRIVAL'})
    if not args.functional_only:
        order_rng = random.Random(20260915)
        for repetition in range(args.repeats):
            seed = 20260915
            for scenario in ('hotspot', 'index_only', 'data_only', 'switch', 'uniform'):
                document = json.loads((run / f'prepared-{scenario}-{seed}.json').read_text())
                tasks, requests, history = load_fixture(document)
                policies = ['B0', 'B1', 'B3', 'P1', 'C0']
                order_rng.shuffle(policies)
                results = []
                for policy in policies:
                    options = Options(policy=policy, seed=seed, sample_rate=.01, summary_capacity=512,
                                      candidate_limit=128, identify_budget_us=2000,
                                      priority_budget_us=2000, publish_batch=1, window_us=2000)
                    results.append(simulate(tasks, requests, history, options,
                                            preparation_us=document['preparation_us'], barrier_us=document['barrier_us']))
                item = {'kind': 'L0_MODEL_NOT_DATABASE_SPEEDUP', 'scenario': scenario, 'seed': seed, 'repetition': repetition,
                        'path_source': str(profile), 'arrivals_and_failure_state': 'synthetic',
                        'cost_source': 'synthetic 200..600 us; no measured local-index-Redo cost',
                        'fixture': fixture_document(tasks, requests, history),
                        'results': results}
                if len({result['fixture_sha256'] for result in results}) != 1:
                    raise RuntimeError('policy baselines differ')
                target = run / f'l0-{scenario}-{seed}-r{repetition}.json'
                target.write_text(json.dumps(item, indent=2, allow_nan=False))
                manifest['models'].append(target.name)
    if args.calibrate_observation or not args.functional_only:
        for repetition in range(args.repeats):
            rates = ['off', '0.01']
            random.Random(20260915 + repetition).shuffle(rates)
            for rate in rates:
                directory = run / f'normal-{rate}-{repetition}'
                env = dict(os.environ)
                for key in list(env):
                    if key.startswith('HCM_TRACE_'):
                        env.pop(key)
                if rate != 'off':
                    env.update(HCM_TRACE_DIR=str(directory), HCM_TRACE_NODE='1', HCM_TRACE_SAMPLE_RATE=rate,
                               HCM_TRACE_CAPACITY='1048576', HCM_TRACE_SEED='20260915')
                output = execute([binaries / 'recovery_blink_path_test', '2000'], directory, env)
                row = next(json.loads(line) for line in output.splitlines() if line.startswith('{'))
                row.update(rate=rate, repetition=repetition,
                           process=json.loads((directory / 'status.json').read_text()))
                if rate != 'off':
                    trace = next(directory.glob('trace-*.jsonl'))
                    row['trace'] = trace_summary(trace)
                manifest['normal_overhead'].append(row)
        off = [r['query_loop_us'] for r in manifest['normal_overhead'] if r['rate'] == 'off']
        on = [r['query_loop_us'] for r in manifest['normal_overhead'] if r['rate'] != 'off']
        manifest['normal_overhead_medians'] = {'disabled_us': statistics.median(off), 'enabled_1pct_us': statistics.median(on),
                                             'scope': 'instrumented fixture, not production TPS or sampling-only cost'}
    overhead = manifest.get('normal_overhead_medians')
    manifest['readiness'] = {
        'isolated_l0_inputs_and_functional_contracts': True,
        'common_fixture_count': len(manifest['prepared_fixtures']),
        'observation_calibration_pairs': len(manifest['normal_overhead']) // 2,
        'fixture_trace_overhead_within_2pct': None if not overhead else overhead['enabled_1pct_us'] <= 1.02 * overhead['disabled_us'],
        'production_overhead_measured': False,
        'live_l1_safe_local_index_redo': False}
    (run / 'summary.json').write_text(json.dumps(manifest, indent=2, allow_nan=False))
    print('PASS; summary:', run / 'summary.json', flush=True)


if __name__ == '__main__':
    main()
