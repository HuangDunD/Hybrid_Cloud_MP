import dataclasses
import itertools
import math
import sys
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts' / 'recovery'))
from priority_scheduler import Hint, Options, Request, Task, fixture, simulate, fixture_document, fixture_fingerprint, load_fixture


class SchedulerTests(unittest.TestCase):
    def run_policy(self, tasks, requests, history=(), policy='P1', **kwargs):
        options = Options(policy=policy, sample_rate=1, publish_batch=1,
                          charge_identification=False, identify_budget_us=100000,
                          priority_budget_us=100, window_us=10)
        return simulate(tasks, requests, history, dataclasses.replace(options, **kwargs))

    def test_index_last_blocker(self):
        tasks = [Task('cold', 'data', 50), Task('i', 'index', 2), Task('d', 'data', 2, ready=True)]
        r = self.run_policy(tasks, [Request('B:1', 'k', ('i',), 'd')])
        self.assertEqual(r['execution_order'][0], 'i')
        self.assertEqual(r['model_completed_in_window'], 1)

    def test_data_last_blocker(self):
        tasks = [Task('cold', 'index', 50), Task('i', 'index', 2, ready=True), Task('d', 'data', 2)]
        requests = [Request('B:1', 'k', ('i',), 'd')]
        self.assertEqual(self.run_policy(tasks, requests)['execution_order'][0], 'd')
        self.assertEqual(self.run_policy(tasks, requests, policy='B1')['execution_order'][0], 'cold')

    def test_group_and_shared_page(self):
        tasks = [Task('cold', 'data', 50), Task('i', 'index', 2), Task('d', 'data', 2)]
        requests = [Request(f'B:{n}', 'k', ('i',), 'd') for n in range(10)]
        result = self.run_policy(tasks, requests, [('k', Hint(('i', 'd')), 'C')])
        self.assertEqual(result['suggested_groups'][0], ['i', 'd'])
        self.assertEqual(result['execution_order'][:2], ['i', 'd'])
        self.assertEqual(result['completed'], 10)
        self.assertEqual(len(result['execution_order']), len(set(result['execution_order'])))
        self.assertTrue(all(len(p['pages']) <= 1 for p in result['publications']))

    def test_duplicate_tasks_and_reports(self):
        task = Task('i', 'index', 2)
        request = Request('C:1', 'missing', ('i',))
        result = self.run_policy([task, task], [request, request])
        self.assertEqual(result['completed'], 1)
        self.assertEqual(result['execution_order'], ['i'])
        with self.assertRaises(ValueError):
            self.run_policy([task, dataclasses.replace(task, cost_us=3)], [request])

    def test_cancellation_removes_demand(self):
        tasks = [Task('cold', 'index', 1), Task('hot', 'index', 5)]
        requests = [Request('B:1', 'k', ('hot',), cancel_us=0)]
        result = self.run_policy(tasks, requests)
        self.assertEqual(result['cancelled'], 1)
        self.assertEqual(result['execution_order'][0], 'cold')

    def test_stale_and_incomplete_hints_are_not_groups(self):
        tasks = [Task('cold', 'index', 20), Task('i', 'index', 2), Task('d', 'data', 2)]
        requests = [Request('B:1', 'k', ('i',), 'd')]
        for hint in (Hint(('i', 'd'), generation=2), Hint(('i', 'd'), complete=False), Hint(('i', 'd'), seen_us=-100)):
            result = self.run_policy(tasks, requests, [('k', hint, 'C')], hint_ttl_us=10)
            self.assertEqual(result['suggested_groups'][0], ['i'])

    def test_unknown_dependencies_never_complete(self):
        result = self.run_policy([Task('i', 'index', 2)], [Request('B:1', 'k', ('i',), complete=False)])
        self.assertEqual(result['completed'], 0)
        self.assertEqual(result['unfinished'], 1)

    def test_legal_dependency_failure_never_publishes(self):
        tasks = [Task('i', 'index', 2, dependencies=('meta',)), Task('meta', 'header', 1, fail=True)]
        result = self.run_policy(tasks, [Request('B:1', 'k', ('i',))])
        self.assertEqual(result['completed'], 0)
        self.assertEqual(result['failed'], 1)
        self.assertNotIn('i', result['ready_us'])
        self.assertIsNone(result['all_recovery_done_us'])

    def test_cycle_and_missing_predecessor(self):
        for tasks in ([Task('i', 'index', 2, dependencies=('i',))], [Task('i', 'index', 2, dependencies=('missing',))]):
            result = self.run_policy(tasks, [Request('B:1', 'k', ('i',))])
            self.assertNotIn('i', result['ready_us'])
            self.assertEqual(result['completed'], 0)

    def test_budget_zero_reverts_to_fixed_order(self):
        tasks = [Task('cold', 'data', 20), Task('i', 'index', 2)]
        requests = [Request('B:1', 'k', ('i',))]
        for budgets in ({'priority_budget_us': 0}, {'identify_budget_us': 0}):
            result = self.run_policy(tasks, requests, **budgets)
            self.assertEqual(result['execution_order'], ['cold', 'i'])

    def test_window_is_bounded(self):
        tasks = [Task('cold', 'index', 20), Task('i', 'index', 2), Task('j', 'index', 2)]
        result = self.run_policy(tasks, [Request('B:1', 'k', ('i',)), Request('C:1', 'z', ('j',))], priority_budget_us=2)
        self.assertEqual(result['execution_order'], ['i', 'cold', 'j'])
        self.assertEqual(result['priority_used_us'], 2)

    def test_c0_fixed_order_and_same_initial_recognition(self):
        tasks = [Task('cold', 'data', 20), Task('i', 'index', 2), Task('d', 'data', 2)]
        requests = [Request('B:1', 'k', ('i',), 'd')]
        history = [('k', Hint(('i', 'd')), 'C')]
        b0 = self.run_policy(tasks, requests, history, 'B0')
        c0 = self.run_policy(tasks, requests, history, 'C0')
        p1 = self.run_policy(tasks, requests, history)
        self.assertEqual(c0['execution_order'], b0['execution_order'])
        self.assertEqual(c0['suggested_groups'][0], p1['suggested_groups'][0])
        self.assertGreater(c0['identification_wall_us'], 0)

    def test_future_requests_are_not_visible(self):
        result = self.run_policy([Task('cold', 'index', 2), Task('hot', 'index', 2)],
                                 [Request('C:1', 'k', ('hot',), arrival_us=100)])
        self.assertEqual(result['execution_order'][0], 'cold')
        self.assertEqual(result['completed'], 0)

    def test_failed_a_hints_are_not_free_information(self):
        tasks = [Task('i', 'index', 2), Task('d', 'data', 2)]
        result = self.run_policy(tasks, [Request('B:1', 'k', ('i',), 'd')], [('k', Hint(('i', 'd')), 'A')])
        self.assertEqual(result['hint_count'], 0)
        self.assertEqual(result['suggested_groups'][0], ['i'])

    def test_global_barrier_dominates_all_policies(self):
        tasks = [Task('cold', 'data', 20), Task('i', 'index', 2)]
        requests = [Request('B:1', 'k', ('i',))]
        for policy in ('B0', 'B1', 'B3', 'P1', 'C0'):
            r = simulate(tasks, requests, [], Options(policy=policy, publish_batch=1,
                         window_us=100, charge_identification=False), barrier_us=80)
            self.assertEqual(r['terminal']['B:1'][1], 80)

    def test_uniform_hotspot_change_and_memory_bound(self):
        for scenario in ('uniform', 'switch'):
            tasks, requests, history = fixture(20260915, scenario)
            for policy in ('B0', 'B1', 'B3', 'P1', 'C0'):
                result = self.run_policy(tasks, requests, history, policy, summary_capacity=4, priority_budget_us=1000)
                self.assertEqual(result['completed'], len(requests))
                self.assertEqual(result['failed'], 0)
                self.assertLessEqual(result['hint_count'], 4)

    def test_same_fixture_and_publish_work_for_every_policy(self):
        tasks = [Task('cold', 'data', 10), Task('i', 'index', 2), Task('d', 'data', 3)]
        requests = [Request('B:1', 'k', ('i',), 'd')]
        history = [('k', Hint(('i', 'd')), 'C')]
        results = [self.run_policy(tasks, requests, history, p, publish_batch=2) for p in ('B0', 'B1', 'B3', 'P1', 'C0')]
        self.assertEqual(len({r['fixture_sha256'] for r in results}), 1)
        self.assertEqual({r['recovery_work_us'] for r in results}, {15})
        self.assertEqual(results[0]['execution_order'], results[-1]['execution_order'])
        self.assertEqual(results[-1]['priority_used_us'], 0)
        self.assertTrue(all(len(batch['pages']) <= 2 for r in results for batch in r['publications']))

    def test_window_reports_censored_and_failure_outcomes(self):
        tasks = [Task('slow', 'index', 20), Task('bad', 'index', 2, fail=True)]
        requests = [Request('B:1', 'k', ('slow',)), Request('C:1', 'z', ('bad',)),
                    Request('C:2', 'k', ('slow',), cancel_us=5)]
        r = self.run_policy(tasks, requests, policy='B0')
        self.assertEqual(r['window_arrived'], 3)
        self.assertEqual(r['window_unfinished'], 2)
        self.assertEqual(r['window_cancelled'], 1)
        self.assertEqual(r['window_censored_wait_us'], 25)
        self.assertIsNone(r['window_p99_us_completed_only'])
        self.assertEqual(r['failed'], 1)
        self.assertEqual(r['cancelled'], 1)

    def test_unknown_path_version_is_not_a_complete_group(self):
        tasks = [Task('i', 'index', 2), Task('d', 'data', 2)]
        r = self.run_policy(tasks, [Request('B:1', 'k', ('i',), 'd', generation=None)],
                            [('k', Hint(('i', 'd'), generation=None), 'C')])
        self.assertEqual(r['suggested_groups'][0], ['i'])

    def test_future_hint_is_not_available(self):
        tasks = [Task('i', 'index', 2), Task('d', 'data', 2)]
        r = self.run_policy(tasks, [Request('B:1', 'k', ('i',), 'd')], [('k', Hint(('i', 'd'), seen_us=1), 'C')])
        self.assertEqual(r['hint_count'], 0)

    def test_saved_fixture_round_trip(self):
        import json
        original = fixture_document(*fixture(20260915, 'uniform'))
        loaded = load_fixture(json.loads(json.dumps(original, allow_nan=False)))
        self.assertEqual(fixture_fingerprint(original), fixture_fingerprint(fixture_document(*loaded)))
        a = simulate(*loaded, Options(policy='B0', charge_identification=False))
        b = simulate(*fixture(20260915, 'uniform'), Options(policy='B0', charge_identification=False))
        self.assertEqual(a['execution_order'], b['execution_order'])
        self.assertEqual(a['terminal'], b['terminal'])

    def test_nonfinite_budgets_rejected(self):
        for field in ('identify_budget_us', 'priority_budget_us', 'window_us', 'hint_ttl_us'):
            for value in (math.nan, math.inf, -1):
                with self.assertRaises(ValueError):
                    dataclasses.replace(Options(), **{field: value}).validate()

    def test_identification_deadline_discards_priority(self):
        clock = itertools.count(0, 10000)
        with patch('priority_scheduler.time.perf_counter_ns', side_effect=lambda: next(clock)):
            r = self.run_policy([Task('cold', 'data', 20), Task('hot', 'index', 2)],
                                [Request('B:1', 'k', ('hot',))], identify_budget_us=1)
        self.assertEqual(r['execution_order'][0], 'cold')
        self.assertGreater(r['identification_overrun_us'], 0)

    def test_fixed_cohort_tail_keeps_failed_and_unknown_requests(self):
        tasks = [Task('i', 'index', 20), Task('d', 'data', 2, ready=True)]
        requests = [Request('B:1', 'k', ('i',), 'd'), Request('C:1', 'z', ('missing',), 'd')]
        r = self.run_policy(tasks, requests, policy='B0')
        self.assertEqual(r['index_tail_sum_us_fixed_cohort'], 20)
        self.assertEqual(r['index_tail_unknown_fixed_cohort'], 1)

    def test_no_candidate_round_does_not_disable_later_arrival(self):
        tasks = [Task('first', 'data', 2), Task('cold', 'data', 20), Task('hot', 'index', 2)]
        result = self.run_policy(tasks, [Request('B:1', 'k', ('hot',), arrival_us=1)])
        self.assertEqual(result['execution_order'], ['first', 'hot', 'cold'])

    def test_failed_predecessors_have_common_cost_and_timestamp(self):
        tasks = [Task('meta', 'header', 1, fail=True), Task('i', 'index', 2, dependencies=('meta',))]
        for batch, policy in itertools.product((1, 2), ('B0', 'B1', 'B3', 'P1', 'C0')):
            with self.subTest(batch=batch, policy=policy):
                result = self.run_policy(tasks, [Request('B:1', 'k', ('i',))], policy=policy, publish_batch=batch)
                self.assertEqual(result['recovery_work_us'], 1)
                self.assertEqual(result['terminal']['B:1'], ('failed', 1))
                self.assertNotIn('i', result['execution_order'])

    def test_failure_before_cancellation_is_not_relabelled(self):
        tasks = [Task('bad', 'index', 2, fail=True), Task('slow', 'data', 20)]
        result = self.run_policy(tasks, [Request('B:1', 'k', ('bad',), cancel_us=5)],
                                 policy='B0', publish_batch=2)
        self.assertEqual(result['terminal']['B:1'], ('failed', 2))

    def test_hidden_path_suffix_cannot_select_matching_history(self):
        tasks = [Task('cold', 'data', 20), Task('i', 'index', 2), Task('d1', 'data', 2), Task('d2', 'data', 2)]
        history = [('10000:5:aaaaaaaaaaaa', Hint(('i', 'd1')), 'B'),
                   ('10000:5:bbbbbbbbbbbb', Hint(('i', 'd2')), 'C')]
        results = [self.run_policy(tasks, [Request('B:1', key, ('i',), data)], history)
                   for key, data in (('10000:5:aaaaaaaaaaaa', 'd1'), ('10000:5:bbbbbbbbbbbb', 'd2'))]
        self.assertEqual(results[0]['suggested_groups'][0], ['i'])
        self.assertEqual(results[0]['suggested_groups'][0], results[1]['suggested_groups'][0])
        self.assertEqual(results[0]['hint_count'], 1)
        self.assertEqual(results[0]['ambiguous_hint_count'], 1)

    def test_selector_catalog_excludes_failure_oracle(self):
        from priority_scheduler import Selector
        selector = Selector({'i': Task('i', 'index', 2, fail=True)}, Options(), [])
        self.assertFalse(hasattr(selector.tasks['i'], 'fail'))
        self.assertFalse(hasattr(selector.tasks['i'], 'ready'))

    def test_unknown_empty_paths_are_kept_in_tail_denominator(self):
        requests = [Request('B:1', 'k', (), complete=False),
                    Request('B:2', 'k', ('i',), complete=False),
                    Request('B:3', 'missing', ('i',)),
                    Request('B:4', 'k', ('i',), 'd', cancel_us=0)]
        result = self.run_policy([Task('i', 'index', 2), Task('d', 'data', 2)], requests, policy='B0')
        self.assertEqual(result['index_tail_unknown_fixed_cohort'], 2)
        self.assertEqual(result['index_tail_not_applicable_fixed_cohort'], 1)
        self.assertEqual(result['index_tail_known_fixed_cohort'], 1)
        self.assertEqual(result['window_arrived'], sum(result[k] for k in
            ('model_completed_in_window', 'window_failed', 'window_cancelled', 'window_unfinished')))
        self.assertEqual(result['window_unfinished_wait_us'], 20)

    def test_fixture_rejects_string_bools_and_wrong_epoch(self):
        import copy
        original = fixture_document([Task('i', 'index', 2)], [Request('B:1', 'k', ('i',))], [])
        for field in ('ready', 'legal', 'fail'):
            document = copy.deepcopy(original)
            document['tasks'][0][field] = 'false'
            with self.assertRaises(ValueError):
                load_fixture(document)
        for field, value in (('epoch', 2), ('parallelism', True), ('preparation_us', '0')):
            document = copy.deepcopy(original)
            document[field] = value
            with self.assertRaises(ValueError):
                load_fixture(document)
        document = copy.deepcopy(original)
        document['requests'][0]['complete'] = 'false'
        with self.assertRaises(ValueError):
            load_fixture(document)

    def test_nonzero_common_boundaries_round_trip(self):
        original = fixture_document([Task('i', 'index', 2)], [Request('B:1', 'k', ('i',))], [], 7, 80)
        loaded = load_fixture(original)
        for policy in ('B0', 'B1', 'B3', 'P1', 'C0'):
            result = simulate(*loaded, Options(policy=policy, charge_identification=False),
                              preparation_us=original['preparation_us'], barrier_us=original['barrier_us'])
            self.assertEqual(result['fixture_sha256'], fixture_fingerprint(original))
            self.assertEqual(result['terminal']['B:1'][1], 80)

    def test_cpu_deadline_is_measured_and_discards_selection(self):
        clock = itertools.count(0, 10000)
        with patch('priority_scheduler.time.process_time_ns', side_effect=lambda: next(clock)):
            result = self.run_policy([Task('cold', 'data', 20), Task('hot', 'index', 2)],
                                     [Request('B:1', 'k', ('hot',))], identify_cpu_budget_us=1)
        self.assertEqual(result['execution_order'][0], 'cold')
        self.assertGreater(result['identification_cpu_overrun_us'], 0)

    def test_dependency_walk_is_bounded_and_not_recursive(self):
        from priority_scheduler import Selector
        tasks = {str(i): Task(str(i), 'index', 1, dependencies=(str(i + 1),) if i < 1499 else ())
                 for i in range(1500)}
        selector = Selector(tasks, Options(), [])
        self.assertEqual(selector.closure(('0',), tasks, {}, 3), [])
        self.assertEqual(len(selector.closure(('0',), tasks, {}, 1500)), 1500)

    def test_c0_summary_matches_p1_but_has_no_priority_execution(self):
        tasks, requests, history = fixture(20260915)
        c0 = self.run_policy(tasks, requests, history, 'C0', sample_rate=.01, priority_budget_us=2000)
        p1 = self.run_policy(tasks, requests, history, 'P1', sample_rate=.01, priority_budget_us=2000)
        self.assertEqual(c0['hint_summary_sha256'], p1['hint_summary_sha256'])
        self.assertEqual(c0['sampling_records_seen'], p1['sampling_records_seen'])
        self.assertEqual(c0['priority_used_us'], 0)
        self.assertLessEqual(p1['priority_used_us'], p1['recognition_priority_budget_used_us'])

    def test_measured_identification_is_serially_charged(self):
        tasks = [Task('cold', 'data', 20), Task('i', 'index', 2)]
        result = self.run_policy(tasks, [Request('B:1', 'k', ('i',))], charge_identification=True)
        self.assertAlmostEqual(result['work_end_us'], result['recovery_work_us'] + result['identification_wall_us'])
        self.assertEqual(result['e0_diagnostic_cost_in_model_us'], 0)

    def test_unreached_failed_dependency_does_not_leak_to_blocker_feedback(self):
        tasks = [Task('future', 'data', 1, fail=True), Task('head', 'index', 20)]
        requests = [Request('B:1', 'k', ('head',), 'future', cancel_us=5),
                    Request('B:2', 'k', ('head',), 'future')]
        result = self.run_policy(tasks, requests, policy='B0')
        self.assertEqual(result['terminal']['B:1'], ('cancelled', 5))
        self.assertEqual(result['terminal']['B:2'], ('failed', 21))

    def test_diagnostics_flag_is_off_by_default(self):
        tasks = [Task('cold', 'data', 20), Task('i', 'index', 2)]
        result = self.run_policy(tasks, [Request('B:1', 'k', ('i',))])
        self.assertNotIn('decision_diagnostics', result)
        self.assertNotIn('counterfactual_mode', result)
        self.assertFalse(result['options']['diagnostics'])

    def test_diagnostics_collects_decision_phases(self):
        tasks = [Task('cold', 'data', 20), Task('i', 'index', 2)]
        result = self.run_policy(tasks, [Request('B:1', 'k', ('i',))], [('k', Hint(('i',)), 'C')], diagnostics=True)
        rows = result['decision_diagnostics']
        self.assertGreaterEqual(len(rows), 1)
        selected = [d for d in rows if d.get('selected')]
        self.assertEqual(selected[0]['selected'], ['i'])
        for key in ('collect_wall_us', 'closure_wall_us', 'score_wall_us', 'decision_wall_us',
                    'candidates', 'waiting_scanned', 'top_candidates'):
            self.assertIn(key, selected[0])

    def test_cross_decider_records_host_state_and_selection(self):
        tasks = [Task('cold', 'data', 20), Task('i', 'index', 2)]
        events = []

        def decider(now, pending, ready, waiting, selected):
            events.append((selected, len(waiting), len(pending)))

        result = simulate(tasks, [Request('B:1', 'k', ('i',))], [],
                          Options(policy='B3', publish_batch=1, charge_identification=False,
                                  identify_budget_us=100000, priority_budget_us=100, window_us=10),
                          cross_decider=decider)
        self.assertEqual(result['execution_order'][0], 'i')
        self.assertEqual(events[0][0], ['i'])
        self.assertEqual(events[0][1], 1)
        self.assertEqual(events[0][2], 2)

    def test_forced_publications_replay_charges_no_identification(self):
        tasks = [Task('cold', 'data', 20), Task('i', 'index', 2), Task('d', 'data', 2)]
        requests = [Request('B:1', 'k', ('i',), 'd')]
        history = [('k', Hint(('i', 'd')), 'C')]
        base = self.run_policy(tasks, requests, history)
        publications = [list(p['pages']) for p in base['publications']]
        replayed = simulate(tasks, requests, history,
                            Options(policy='P1', sample_rate=1, publish_batch=1, charge_identification=True,
                                    identify_budget_us=100000, priority_budget_us=100, window_us=10),
                            forced_publications=publications)
        self.assertEqual(replayed['execution_order'], base['execution_order'])
        self.assertEqual(replayed['identification_wall_us'], 0.0)
        self.assertEqual(replayed['counterfactual_mode'], 'zero_cost_publication_replay')
        self.assertEqual(replayed['terminal'], base['terminal'])
        self.assertEqual(replayed['completed'], base['completed'])

    def test_reset_for_counterfactual_restores_budgets(self):
        from priority_scheduler import Selector
        selector = Selector({'a': Task('a', 'index', 2), 'b': Task('b', 'index', 2)},
                            Options(policy='P1', identify_budget_us=1000, priority_budget_us=1000), [])
        selector.closed = True
        selector.identify_wall_us = 999.0
        selector.priority_used_us = 999.0
        selector.reset_for_counterfactual()
        self.assertFalse(selector.closed)
        self.assertEqual(selector.identify_wall_us, 0.0)
        self.assertEqual(selector.priority_used_us, 0.0)
        self.assertEqual(selector.decisions, 0)

    def test_p1f_selects_identical_groups_with_frozen_clock(self):
        for scenario in ('hotspot', 'index_only', 'data_only', 'switch', 'uniform'):
            tasks, requests, history = fixture(20260915, scenario)
            orders, groups = {}, {}
            for policy in ('P1', 'P1F'):
                with patch('priority_scheduler.time.perf_counter_ns', side_effect=lambda: 0), \
                     patch('priority_scheduler.time.process_time_ns', side_effect=lambda: 0):
                    result = self.run_policy(tasks, requests, history, policy, sample_rate=.01,
                                            priority_budget_us=2000)
                orders[policy], groups[policy] = result['execution_order'], result['suggested_groups']
            self.assertEqual(orders['P1F'], orders['P1'], scenario)
            self.assertEqual(groups['P1F'], groups['P1'], scenario)

    def test_p1f_early_exit_after_priority_exhaustion(self):
        tasks, requests, history = fixture(20260915, 'hotspot')
        with patch('priority_scheduler.time.perf_counter_ns', side_effect=lambda: 0), \
             patch('priority_scheduler.time.process_time_ns', side_effect=lambda: 0):
            reference = self.run_policy(tasks, requests, history, 'P1', sample_rate=.01, priority_budget_us=2000)
            improved = self.run_policy(tasks, requests, history, 'P1F', sample_rate=.01, priority_budget_us=2000,
                                       diagnostics=True)
        self.assertEqual(improved['execution_order'], reference['execution_order'])
        self.assertEqual(improved['suggested_groups'], reference['suggested_groups'])
        early = [d for d in improved['decision_diagnostics'] if d.get('early_exit')]
        self.assertGreaterEqual(len(early), 1)
        self.assertTrue(all(d['candidates'] == 0 and d['selected'] == [] for d in early))

    def test_p1f_hint_summary_matches_p1(self):
        tasks, requests, history = fixture(20260915, 'uniform')
        p1 = self.run_policy(tasks, requests, history, 'P1', sample_rate=.01)
        p1f = self.run_policy(tasks, requests, history, 'P1F', sample_rate=.01)
        self.assertEqual(p1f['hint_summary_sha256'], p1['hint_summary_sha256'])
        self.assertEqual(p1f['sampled_records'], p1['sampled_records'])
        self.assertEqual(p1f['hint_count'], p1['hint_count'])
        self.assertLessEqual(p1f['hint_memory_estimate_bytes'], p1['hint_memory_estimate_bytes'] + 1024)

    def test_cli_exact_fixture_boundaries_and_exclusive_output(self):
        import json
        import os
        import subprocess
        import tempfile
        root = Path(__file__).resolve().parents[3]
        parent = Path(os.environ.get('HCM_TEST_OUTPUT_ROOT', root / 'build/recovery-priority/scheduler-contracts')).resolve()
        if root not in parent.parents:
            raise ValueError('test artifacts must remain inside Hybrid_Cloud_MP')
        parent.mkdir(parents=True, exist_ok=True)
        directory = Path(tempfile.mkdtemp(prefix='cli-', dir=parent))
        document = fixture_document([Task('i', 'index', 2)], [Request('B:1', 'k', ('i',))], [], 7, 80)
        source, output = directory / 'fixture.json', directory / 'result.json'
        source.write_text(json.dumps(document))
        command = [sys.executable, '-B', str(root / 'scripts/recovery/priority_scheduler.py'),
                   '--fixture', str(source), '--output', str(output), '--window-us', '100']
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        raw = output.read_bytes()
        for row in json.loads(raw)['results']:
            self.assertEqual(row['fixture_sha256'], fixture_fingerprint(document))
            self.assertGreaterEqual(row['terminal']['B:1'][1], 80)
            self.assertEqual(row['shared_preparation_us'], 7)
        again = subprocess.run(command, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(again.returncode, 0)
        self.assertEqual(output.read_bytes(), raw)
        (directory / 'command.json').write_text(json.dumps(command))
        (directory / 'output.log').write_text(result.stdout + again.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
