#!/usr/bin/env python3
"""Stage-A attribution diagnostics for the saved-fixture L0 priority-recovery model.

For one saved common fixture this tool produces, per policy:
- an uninstrumented reference run (fresh in-process, not the formal pilot numbers);
- an instrumented decision trace with per-decision phase timings (collection,
  closure, scoring) and candidate snapshots;
- same-visible-state cross-policy decisions (guest policy chooses with a fresh
  budget at each host decision point; never sees future requests or hidden paths);
- two labelled zero-cost counterfactuals: billing-only replay of a recorded
  publication order (identification time not charged, order fixed), and
  re-deciding with identification not charged.

Diagnostic output only. Instrumented timings include the instrumentation itself
and are never formal performance results; zero-cost counterfactuals are
optimistic references, never reported gains. Run it from the repository root.
"""
import argparse
import dataclasses
import hashlib
import json
import statistics
import sys
from collections import OrderedDict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from priority_scheduler import Options, Selector, fixture_fingerprint, load_fixture, simulate  # noqa: E402

METRICS = ('model_completed_in_window', 'window_arrived', 'window_unfinished', 'window_failed',
           'window_cancelled', 'window_restricted_wait_sum_us', 'window_p95_us_completed_only',
           'window_p99_us_completed_only', 'window_unfinished_wait_us', 'window_observed_wait_p95_us',
           'index_tail_sum_us_fixed_cohort', 'identification_wall_us', 'identification_cpu_us',
           'identification_decisions', 'priority_used_us', 'all_recovery_done_us', 'recovery_work_us')


def slim(result):
    row = {m: result[m] for m in METRICS}
    row.update(execution_order=result['execution_order'], suggested_groups=result['suggested_groups'],
               publications=[{'at_us': p['at_us'], 'pages': p['pages']} for p in result['publications']],
               hint_count=result['hint_count'],
               counterfactual_mode=result.get('counterfactual_mode'))
    return row


def load_verified(fixture_path, manifest_path=None):
    raw = Path(fixture_path).read_bytes()
    document = json.loads(raw)
    tasks, requests, history = load_fixture(document)
    digest = fixture_fingerprint(document)
    row = {'source': str(fixture_path), 'fixture_sha256': digest,
           'file_sha256': hashlib.sha256(raw).hexdigest()}
    if manifest_path:
        manifest = json.loads(Path(manifest_path).read_text())
        expected = {r['path']: r['sha256'] for r in manifest['prepared_fixtures']}
        if expected.get(Path(fixture_path).name) != digest:
            raise ValueError(f'fixture fingerprint mismatch against manifest: {Path(fixture_path).name}')
        row['verified_against_manifest'] = str(manifest_path)
    return document, tasks, requests, history, row


def phase_totals(decisions):
    scored = [d for d in decisions if not d.get('budget_guard_exit') and not d.get('early_exit')]
    guards = [d for d in decisions if d.get('budget_guard_exit')]
    early = [d for d in decisions if d.get('early_exit')]
    if not scored:
        return {'decisions': len(decisions), 'budget_guard_exits': len(guards),
                'budget_infeasible_early_exits': len(early)}
    def total(key):
        return sum(d[key] for d in scored if key in d)
    return {'decisions': len(decisions), 'budget_guard_exits': len(guards),
            'budget_infeasible_early_exits': len(early),
            'decisions_with_selection': sum(1 for d in scored if d.get('selected')),
            'collect_wall_us': total('collect_wall_us'), 'closure_wall_us': total('closure_wall_us'),
            'score_wall_us': total('score_wall_us'), 'decision_wall_us': total('decision_wall_us'),
            'median_decision_wall_us': statistics.median(d['decision_wall_us'] for d in scored),
            'waiting_scanned_total': total('waiting_scanned'),
            'full_hint_matches_total': total('full_hint_matches'),
            'subset_checks_total': total('subset_checks'),
            'candidates_total': total('candidates'),
            'phases_note': 'collect+closure+score are timed regions inside choose(); '
                           'their sum plus instrumentation overhead equals decision_wall_us'}


def run_attribution(args):
    trace_policies = [p for p in args.trace_policies.split(',') if p]
    cross_pairs = [p for p in args.cross.split(',') if p]
    replay_policies = [p for p in args.replay_policies.split(',') if p]
    redecide_policies = [p for p in args.redecide_policies.split(',') if p]
    document, tasks, requests, history, fixture_row = load_verified(args.fixture, args.manifest)
    preparation_us, barrier_us = document['preparation_us'], document['barrier_us']
    base_options = Options(seed=args.seed, sample_rate=args.sample_rate,
                           summary_capacity=args.summary_capacity, candidate_limit=args.candidate_limit,
                           max_hint_pages=args.max_hint_pages, identify_budget_us=args.identify_budget_us,
                           identify_cpu_budget_us=args.identify_cpu_budget_us,
                           priority_budget_us=args.priority_budget_us, publish_batch=args.publish_batch,
                           max_group=args.max_group, window_us=args.window_us)
    task_map = OrderedDict((t.name, t) for t in tasks)
    output = {'kind': 'L0_ATTRIBUTION_DIAGNOSTIC_NOT_PERFORMANCE', 'fixture': fixture_row,
              'options': dataclasses.asdict(base_options), 'policies': {}, 'cross_decisions': {},
              'counterfactuals': {},
              'labels': {
                  'instrumented_timing': 'diagnostics=True adds phase timers; identification_wall_us of '
                                         'instrumented runs is NOT formal performance',
                  'zero_cost': 'both counterfactuals remove identification charging; they are diagnostic '
                               'or optimistic references, never formal performance gains',
                  'cross_decisions': 'guest policy chooses with a fresh budget at the host decision '
                                     'state; not a replay of the guest trajectory',
                  'plain_runs': 'uninstrumented in-process reference runs; formal numbers remain the '
                                'paired pilot medians'}}

    plain_runs = {}
    for policy in trace_policies:
        plain = simulate(tasks, requests, history, dataclasses.replace(base_options, policy=policy),
                         preparation_us=preparation_us, barrier_us=barrier_us)
        plain_runs[policy] = plain
        instrumented = simulate(tasks, requests, history,
                                 dataclasses.replace(base_options, policy=policy, diagnostics=True),
                                 preparation_us=preparation_us, barrier_us=barrier_us)
        output['policies'][policy] = {
            'plain': slim(plain),
            'instrumented': {'identification_wall_us': instrumented['identification_wall_us'],
                             'identification_cpu_us': instrumented['identification_cpu_us'],
                             'phase_totals': phase_totals(instrumented['decision_diagnostics']),
                             'decision_diagnostics': instrumented['decision_diagnostics']},
            'diagnostic_overhead_estimate_us': instrumented['identification_wall_us'] - plain['identification_wall_us'],
            'diagnostic_overhead_note': 'single-run difference between instrumented and plain runs; '
                                        'noisy estimate of the instrumentation cost itself'}

    for pair in cross_pairs:
        host, guest = pair.split(':')
        host_options = dataclasses.replace(base_options, policy=host)
        guest_options = dataclasses.replace(base_options, policy=guest)
        guest_selector = Selector(task_map, guest_options, history)
        events = []

        def decider(now, pending, ready, waiting, selected, guest_selector=guest_selector,
                    guest_options=guest_options, events=events):
            guest_selector.reset_for_counterfactual()
            guest_choice, _ = guest_selector.choose(now, pending, ready, waiting)
            events.append({'now_us': now, 'waiting': len(waiting), 'pending': len(pending),
                           'ready': len(ready), 'host_selected': list(selected),
                           'guest_selected': list(guest_choice),
                           'agree': list(selected) == list(guest_choice)})

        simulate(tasks, requests, history, host_options, preparation_us=preparation_us,
                 barrier_us=barrier_us, cross_decider=decider)
        agreeing = sum(1 for e in events if e['agree'])
        output['cross_decisions'][pair] = {
            'host_run_note': 'host run is uninstrumented; guest choose() cost is not charged to the model',
            'decision_points': len(events), 'agreeing_decisions': agreeing,
            'events': events}

    if replay_policies:
        source, source_note = None, None
        if args.replay_from:
            recorded = json.loads(Path(args.replay_from).read_text())
            source_note = f'formal result {args.replay_from}'
        else:
            recorded = None
            source_note = 'in-process uninstrumented plain run of the same policy'
        rows = {}
        for policy in replay_policies:
            if args.replay_from:
                result_row = next(r for r in recorded['results'] if r['policy'] == policy)
                publications = [list(p['pages']) for p in result_row['publications']]
                recorded_order = list(result_row['execution_order'])
            else:
                plain = plain_runs[policy]
                publications = [list(p['pages']) for p in plain['publications']]
                recorded_order = list(plain['execution_order'])
            replayed = simulate(tasks, requests, history,
                                dataclasses.replace(base_options, policy=policy),
                                preparation_us=preparation_us, barrier_us=barrier_us,
                                forced_publications=publications)
            row = slim(replayed)
            row['order_matches_recording'] = replayed['execution_order'] == recorded_order
            row['replayed_publications'] = len(publications)
            rows[policy] = row
        output['counterfactuals']['billing_only_replay'] = {
            'source': source_note, 'note': 'recorded publication order fixed; identification not charged; '
                                           'a diagnostic cost attribution, not a performance claim',
            'policies': rows}

    if redecide_policies:
        rows = {}
        for policy in redecide_policies:
            run = simulate(tasks, requests, history,
                           dataclasses.replace(base_options, policy=policy, charge_identification=False),
                           preparation_us=preparation_us, barrier_us=barrier_us)
            row = slim(run)
            row['note'] = 'identification measured but not charged; decisions retaken as states change'
            rows[policy] = row
        output['counterfactuals']['redecide_without_charging'] = {
            'note': 'optimistic reference only; visible states differ from the charged run',
            'policies': rows}
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, help='prepared-fixtures manifest summary.json for fingerprint verification')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trace-policies', default='B3,P1')
    parser.add_argument('--cross', default='B3:P1,P1:B3', help='host:guest pairs, comma separated')
    parser.add_argument('--replay-policies', default='B3,P1')
    parser.add_argument('--replay-from', type=Path, help='formal result.json providing recorded orders')
    parser.add_argument('--redecide-policies', default='B3,P1')
    parser.add_argument('--seed', type=int, default=20260915)
    parser.add_argument('--sample-rate', type=float, default=.01)
    parser.add_argument('--summary-capacity', type=int, default=512)
    parser.add_argument('--candidate-limit', type=int, default=128)
    parser.add_argument('--max-hint-pages', type=int, default=64)
    parser.add_argument('--identify-budget-us', type=float, default=2000)
    parser.add_argument('--identify-cpu-budget-us', type=float, default=2000)
    parser.add_argument('--priority-budget-us', type=float, default=2000)
    parser.add_argument('--publish-batch', type=int, default=1)
    parser.add_argument('--max-group', type=int, default=3)
    parser.add_argument('--window-us', type=float, default=2000)
    args = parser.parse_args()
    policies = []
    groups = [args.trace_policies, args.replay_policies, args.redecide_policies]
    groups += [part for pair in args.cross.split(',') if pair for part in pair.split(':')]
    for group in groups:
        policies.extend(p for p in group.split(',') if p and p not in policies)
    unknown = [p for p in policies if p not in ('B0', 'B1', 'B3', 'P1', 'P1F', 'C0')]
    if unknown:
        parser.error(f'unknown policies: {unknown}')
    if args.output.exists():
        parser.error('output already exists; choose a new isolated result path')
    output = run_attribution(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as handle:
        handle.write(json.dumps(output, indent=2, allow_nan=False) + '\n')
    for policy, row in output['policies'].items():
        plain = row['plain']
        print(policy, 'plain: window_completed=', plain['model_completed_in_window'],
              'id_wall_us=', round(plain['identification_wall_us'], 1),
              'instrumented_id_wall_us=', round(row['instrumented']['identification_wall_us'], 1), flush=True)
    for name, row in output['counterfactuals'].items():
        if 'policies' in row:
            for policy, result in row['policies'].items():
                print(name, policy, 'window_completed=', result['model_completed_in_window'],
                      'id_wall_us=', round(result['identification_wall_us'], 1), flush=True)


if __name__ == '__main__':
    main()
