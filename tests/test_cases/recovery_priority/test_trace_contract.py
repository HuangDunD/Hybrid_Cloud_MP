import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'scripts/recovery'))
from run_priority_pilot import trace_summary
from priority_scheduler import read_profile


class TraceTests(unittest.TestCase):
    def event(self, name, ts, **kwargs):
        row = dict(event=name, reason='', ts_us=ts, pid=5, node=1, request=1,
                   logical=10, attempt=0, lookup=2, epoch=1, key=42, generation=1,
                   table=10000, page=-1, a=0, b=0, c=0)
        row.update(kwargs)
        return row

    def rows(self):
        return [dict(event='trace_config', schema=2),
                self.event('profile_contract', 1, a=1),
                self.event('request_begin', 100, c=90),
                self.event('lookup_begin', 110, a=1),
                self.event('path_page', 111, page=2),
                self.event('path_status', 112, a=1, b=1),
                self.event('lookup_target', 113, table=0, page=7, a=3),
                self.event('lookup_end', 114, page=7, a=1, b=3, c=1),
                self.event('request_end', 120, a=1),
                dict(event='trace_summary', dropped_events=0, io_error=False)]

    def write(self, rows):
        import os
        directory = Path(os.environ.get('HCM_TEST_OUTPUT_ROOT', ROOT / 'build/recovery-priority/trace-contracts')).resolve()
        if ROOT not in directory.parents:
            raise ValueError('test artifacts must remain inside Hybrid_Cloud_MP')
        directory.mkdir(parents=True, exist_ok=True)
        path = Path(tempfile.mkdtemp(prefix='case-', dir=directory)) / 'trace.jsonl'
        path.write_text('\n'.join(json.dumps(row) for row in rows) + '\n')
        return path

    def test_heap_identity_and_external_validation(self):
        rows = self.rows()
        profile = read_profile(self.write(rows))
        self.assertEqual(profile[0][2], 'd:0:7')
        self.assertTrue(profile[0][4])
        self.assertEqual(trace_summary(self.write(rows))['p99_us_verified_only'], 30)

    def test_local_generation_is_not_authoritative(self):
        rows = [e for e in self.rows() if e['event'] != 'profile_contract']
        self.assertFalse(read_profile(self.write(rows))[0][4])

    def test_not_found_has_no_heap_target(self):
        rows = [e for e in self.rows() if e['event'] != 'lookup_target']
        for row in rows:
            if row['event'] == 'lookup_end':
                row.update(a=0, b=-1, page=-1)
        self.assertIsNone(read_profile(self.write(rows))[0][2])

    def test_missing_footer_and_loss_are_rejected(self):
        for rows in (self.rows()[:-1], self.rows()):
            if rows[-1]['event'] == 'trace_summary':
                rows[-1]['dropped_events'] = 1
            for parser in (read_profile, trace_summary):
                with self.assertRaises(ValueError):
                    parser(self.write(rows))

    def test_stale_or_unverified_path_is_not_ground_truth(self):
        rows = self.rows()
        rows[-2]['a'] = 0
        with self.assertRaises(ValueError):
            read_profile(self.write(rows))
        rows = self.rows()
        next(e for e in rows if e['event'] == 'path_status')['a'] = 2
        with self.assertRaises(ValueError):
            read_profile(self.write(rows))

    def test_polling_duplicates_rejected_and_waits_matched(self):
        rows = self.rows()
        rows[4:4] = [self.event('block', 110, page=2, reason='IR'), self.event('block', 111, page=2, reason='IR')]
        with self.assertRaises(ValueError):
            trace_summary(self.write(rows))
        rows[5] = self.event('unblock', 112, page=2, reason='IR', a=2, b=1)
        result = trace_summary(self.write(rows))
        self.assertEqual(result['blocked_requests'], 1)
        self.assertEqual(result['wait_duration_us']['IR'], 2)

    def test_fixed_window_keeps_unfinished_request(self):
        result = trace_summary(self.write(self.rows()), window_us=15)
        self.assertEqual(result['fixed_window']['arrived'], 1)
        self.assertEqual(result['fixed_window']['unfinished'], 1)
        self.assertEqual(result['fixed_window']['verified'], 0)
        self.assertEqual(result['fixed_window']['censored_wait_us'], 15)

    def test_historical_ready_times_are_clipped_and_heap_can_finish_after_lookup(self):
        rows = self.rows()
        rows[2:2] = [self.event('fixture_ready', 60, table=0, page=7),
                     self.event('fixture_ready', 70, page=2), self.event('run_begin', 80)]
        result = trace_summary(self.write(rows))
        self.assertEqual(result['fixture_index_tail_sum_us'], 0)
        rows = self.rows()
        rows[2:2] = [self.event('run_begin', 80), self.event('fixture_baseline_ready', 81, page=2)]
        rows[-2:-2] = [self.event('fixture_ready', 118, table=0, page=7)]
        result = trace_summary(self.write(rows))
        self.assertEqual(result['fixture_index_tail_known'], 1)
        self.assertEqual(result['fixture_index_tail_unknown'], 0)
        self.assertEqual(result['fixture_index_tail_sum_us'], 0)

    def test_thread_batch_order_is_not_clock_order(self):
        rows = self.rows()
        rows[2:-1] = list(reversed(rows[2:-1]))
        result = trace_summary(self.write(rows))
        self.assertEqual(result['externally_verified_requests'], 1)
        self.assertEqual(result['p95_us_verified_only'], 30)

    def test_unverified_window_outcome_closes_denominator(self):
        rows = self.rows()
        rows[-2]['a'] = 0
        result = trace_summary(self.write(rows))
        window = result['fixed_window']
        self.assertEqual(window['unverified'], 1)
        self.assertEqual(window['arrived'], sum(window[k] for k in ('verified', 'unverified', 'failed', 'cancelled', 'unfinished')))

    def test_incomplete_and_unsampled_lookup_remain_unknown(self):
        for outcome in (None, 2, 3):
            rows = [e for e in self.rows() if e['event'] not in ('lookup_end', 'request_end')]
            if outcome is not None:
                rows.insert(-1, self.event('request_end', 120, a=outcome))
            result = trace_summary(self.write(rows))
            self.assertEqual(result['fixture_index_tail_unknown'], 1)
            self.assertEqual(result['fixture_index_tail_cohort'], 1)
        rows = self.rows()
        next(e for e in rows if e['event'] == 'lookup_end')['c'] = 0
        self.assertEqual(trace_summary(self.write(rows))['fixture_index_tail_unknown'], 1)

    def test_profile_preserves_lost_node_and_future_history_boundary(self):
        from priority_scheduler import Options, Selector, fixture
        for node, failure_at in ((0, None), (1, 105)):
            rows = self.rows()
            for row in rows:
                if 'node' in row:
                    row['node'] = node
            if failure_at is not None:
                rows.insert(2, self.event('recovery_begin', failure_at, epoch=1))
            tasks, requests, history = fixture(20260915, profile=self.write(rows))
            selector = Selector({t.name: t for t in tasks}, Options(policy='P1', sample_rate=1), history)
            self.assertEqual(len(selector.hints), 0)
            self.assertEqual(requests[0].key, '10000:42')
            if node == 0:
                self.assertTrue(all(source == 'A' for _, _, source in history))
            else:
                self.assertTrue(all(h.seen_us > 0 for _, h, _ in history))


if __name__ == '__main__':
    unittest.main(verbosity=2)
