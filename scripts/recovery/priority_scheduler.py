#!/usr/bin/env python3
"""L0 only: online-information scheduling over an offline, explicitly labelled model."""
import argparse
import dataclasses
import hashlib
import json
import math
import random
import sys
import time
from collections import Counter, OrderedDict
from pathlib import Path


@dataclasses.dataclass(frozen=True)
class Task:
    name: str
    role: str
    cost_us: float
    dependencies: tuple = ()
    ready: bool = False
    legal: bool = True
    fail: bool = False
    epoch: int = 1


@dataclasses.dataclass(frozen=True)
class Request:
    identity: str
    key: str
    index: tuple
    data: str = None
    arrival_us: float = 0
    cancel_us: float = math.inf
    generation: int = 1
    complete: bool = True
    txn_ready_us: float = 0

    @property
    def path(self):
        return self.index + (() if self.data is None else (self.data,))


@dataclasses.dataclass(frozen=True)
class Hint:
    path: tuple
    generation: int = 1
    complete: bool = True
    seen_us: float = 0


@dataclasses.dataclass
class Options:
    policy: str = "OFF"
    sample_rate: float = 0.01
    summary_capacity: int = 512
    candidate_limit: int = 128
    max_hint_pages: int = 64
    identify_budget_us: float = 2000
    identify_cpu_budget_us: float = 2000
    priority_budget_us: float = 20000
    publish_batch: int = 16
    max_group: int = 3
    window_us: float = 10000
    hint_ttl_us: float = 10000000
    seed: int = 20260915
    charge_identification: bool = True
    diagnostics: bool = False

    def validate(self):
        if self.policy not in ("OFF", "B0", "B1", "B3", "P1", "P1F", "C0"):
            raise ValueError("unknown policy")
        if not 0 <= self.sample_rate <= 1:
            raise ValueError("sample_rate must be in [0,1]")
        if any(type(v) is not int or v < 1 for v in
               (self.summary_capacity, self.candidate_limit, self.publish_batch, self.max_group, self.max_hint_pages)):
            raise ValueError("capacities and batch/group sizes must be positive integers")
        if any(not math.isfinite(v) or v < 0 for v in
               (self.identify_budget_us, self.identify_cpu_budget_us, self.priority_budget_us, self.window_us, self.hint_ttl_us)):
            raise ValueError("budgets must be finite and non-negative")
        if type(self.diagnostics) is not bool or type(self.charge_identification) is not bool:
            raise ValueError("diagnostics and charge_identification must be booleans")


def online_key(key):
    """Legacy fixture variants belong to model truth, never to the online lookup key."""
    parts = key.split(':')
    if (len(parts) == 3 and all(p.lstrip('-').isdigit() for p in parts[:2])
            and len(parts[2]) == 12 and all(c in '0123456789abcdef' for c in parts[2])):
        return ':'.join(parts[:2])
    return key


@dataclasses.dataclass(frozen=True)
class CatalogTask:
    role: str
    cost_us: float
    dependencies: tuple
    legal: bool


class Selector:
    def __init__(self, tasks, options, history):
        options.validate()
        self.tasks = {name: CatalogTask(t.role, t.cost_us, t.dependencies, t.legal) for name, t in tasks.items()}
        self.options = options
        self.hints = OrderedDict()
        self.identify_wall_us = self.identify_cpu_us = 0.0
        self.sampling_wall_us = self.sampling_cpu_us = 0.0
        self.sampling_records_seen = self.sampled_records = 0
        self.priority_used_us = 0.0
        self.closed = options.policy in ("OFF", "B0")
        self.decisions, self.suggested_groups = 0, []
        self.id_overrun_us = self.cpu_overrun_us = 0.0
        self.diag_events = [] if options.diagnostics else None
        self.identity_hints = {}
        if options.policy in ("P1", "P1F", "C0"):
            wall, cpu = time.perf_counter_ns(), time.process_time_ns()
            rng = random.Random(options.seed)
            for key, hint, node in history:
                self.sampling_records_seen += 1
                if node not in ("B", "C") or hint.seen_us > 0 or rng.random() >= options.sample_rate:
                    continue
                self.sampled_records += 1
                key = online_key(key)
                if len(hint.path) > options.max_hint_pages:
                    hint = dataclasses.replace(hint, path=hint.path[:options.max_hint_pages], complete=False)
                previous = self.hints.pop(key, None)
                if previous and (not previous.complete or previous.path != hint.path or previous.generation != hint.generation):
                    hint = dataclasses.replace(hint, complete=False)
                self.hints[key] = hint
                if len(self.hints) > options.summary_capacity:
                    self.hints.popitem(last=False)
            self.sampling_wall_us = (time.perf_counter_ns() - wall) / 1000
            self.sampling_cpu_us = (time.process_time_ns() - cpu) / 1000

    def reset_for_counterfactual(self):
        """Reset per-run decision/budget state; hints and options stay untouched.

        Diagnostic cross-policy checks only: a fresh-budget choice at an already
        observed state. Never used by formal paired runs."""
        self.identify_wall_us = self.identify_cpu_us = 0.0
        self.priority_used_us = 0.0
        self.closed = self.options.policy in ("OFF", "B0")
        self.decisions, self.suggested_groups = 0, []
        self.id_overrun_us = self.cpu_overrun_us = 0.0

    def closure(self, names, pending, ready, limit, expired=lambda: False):
        result, visiting, included = [], set(), set()
        stack = [(iter(names), None)]
        while stack:
            if expired():
                return []
            children, parent = stack[-1]
            try:
                name = next(children)
            except StopIteration:
                stack.pop()
                if parent is not None:
                    visiting.remove(parent)
                    included.add(parent)
                    result.append(parent)
                continue
            if name in ready or name in included:
                continue
            if (name not in pending or name in visiting or not self.tasks[name].legal
                    or len(visiting) + len(included) >= limit):
                return []
            visiting.add(name)
            stack.append((iter(self.tasks[name].dependencies), name))
        return result

    def choose(self, now, pending, ready, waiting):
        """Only current blocker reports and bounded survivor hints enter identification."""
        o = self.options
        diag = self.diag_events
        if (self.closed or self.identify_wall_us >= o.identify_budget_us
                or self.identify_cpu_us >= o.identify_cpu_budget_us or self.priority_used_us >= o.priority_budget_us):
            self.closed = True
            if diag is not None:
                diag.append({'decision': self.decisions, 'now_us': now, 'pending': len(pending), 'ready': len(ready),
                             'waiting': len(waiting), 'budget_guard_exit': True,
                             'identify_wall_before_us': self.identify_wall_us,
                             'priority_used_before_us': self.priority_used_us})
            return [], 0.0
        wall, cpu = time.perf_counter_ns(), time.process_time_ns()
        deadline = wall + int((o.identify_budget_us - self.identify_wall_us) * 1000)
        cpu_deadline = cpu + int((o.identify_cpu_budget_us - self.identify_cpu_us) * 1000)

        def expired():
            return time.perf_counter_ns() >= deadline or time.process_time_ns() >= cpu_deadline

        row = {'decision': self.decisions, 'now_us': now, 'pending': len(pending), 'ready': len(ready),
               'waiting': len(waiting), 'budget_guard_exit': False,
               'identify_wall_before_us': self.identify_wall_us, 'priority_used_before_us': self.priority_used_us}
        phase = time.perf_counter_ns() if diag is not None else 0
        candidates, counts, full_missing = OrderedDict(), Counter(), {}
        effective = "P1" if o.policy == "C0" else o.policy
        fast = effective == "P1F"
        if fast:
            # A pending group's cost is the sum of its member task costs, so once the
            # remaining priority budget is below the cheapest pending task no group can
            # fit and the full scan below would return the same empty decision.
            remaining = o.priority_budget_us - self.priority_used_us
            cheapest = min((self.tasks[p].cost_us for p in pending), default=0.0)
            if remaining < cheapest:
                elapsed = (time.perf_counter_ns() - wall) / 1000
                self.identify_wall_us += elapsed
                self.identify_cpu_us += (time.process_time_ns() - cpu) / 1000
                self.id_overrun_us = max(0.0, self.identify_wall_us - o.identify_budget_us)
                self.cpu_overrun_us = max(0.0, self.identify_cpu_us - o.identify_cpu_budget_us)
                if self.identify_wall_us >= o.identify_budget_us or self.identify_cpu_us >= o.identify_cpu_budget_us:
                    self.closed = True
                self.decisions += 1
                if diag is not None:
                    row.update(early_exit=True, waiting_scanned=0, candidates=0, full_hint_matches=0,
                               collect_wall_us=0.0, closure_wall_us=0.0, score_wall_us=0.0, subset_checks=0,
                               closures_attempted=0, top_candidates=[], selected=[], decision_wall_us=elapsed,
                               identify_wall_after_us=self.identify_wall_us,
                               priority_used_after_us=self.priority_used_us, selector_closed=self.closed)
                    diag.append(row)
                return [], elapsed
        visible = []
        for identity, key, generation, blocker in (() if effective == 'B1' else waiting):
            if expired():
                break
            visible.append((identity, blocker))
            counts[blocker] += 1
            if blocker in pending and len(candidates) < o.candidate_limit:
                candidates.setdefault((blocker,), None)
            if effective == 'P1':
                hint = self.hints.get(online_key(key))
            elif fast:
                # Requests keep their key and generation for the whole run, so the
                # key resolution is cacheable per identity; this is a repeated-scan
                # reduction, not new information.
                entry = self.identity_hints.get(identity)
                if entry is None:
                    hint = self.hints.get(online_key(key))
                    entry = (hint, frozenset(hint.path)) if hint is not None else (None, None)
                    self.identity_hints[identity] = entry
                hint, path_pages = entry
            else:
                hint = None
            if (hint and hint.complete and generation is not None and hint.generation == generation
                    and 0 <= now - hint.seen_us <= o.hint_ttl_us
                    and blocker in (path_pages if fast else hint.path)):
                missing = tuple(p for p in hint.path if p not in ready)
                full_missing[identity] = frozenset(missing)
                if missing and len(missing) <= o.max_group and len(candidates) < o.candidate_limit:
                    candidates.setdefault(missing, None)
        if effective == "B1":
            for name in pending:
                if len(candidates) >= o.candidate_limit or expired():
                    break
                candidates[(name,)] = None
        if diag is not None:
            row.update(waiting_scanned=len(visible), candidates=len(candidates),
                       full_hint_matches=len(full_missing),
                       collect_wall_us=(time.perf_counter_ns() - phase) / 1000)
            phase = time.perf_counter_ns()
        best, best_score = [], None
        scored, subset_checks, closure_wall_ns = [], 0, 0
        by_page = {}
        if fast:
            for identity, missing in full_missing.items():
                for page in missing:
                    by_page.setdefault(page, set()).add(identity)
        for names in candidates:
            if expired():
                break
            closure_start = time.perf_counter_ns() if diag is not None else 0
            group = self.closure(names, pending, ready, o.max_group, expired)
            if diag is not None:
                closure_wall_ns += time.perf_counter_ns() - closure_start
            cost = sum(self.tasks[p].cost_us for p in group)
            if not group or self.priority_used_us + cost > o.priority_budget_us:
                continue
            if effective == "B1":
                rank = min({"header": 0, "internal": 1, "index": 2, "data": 3}.get(self.tasks[p].role, 4) for p in group)
                score = (4 - rank, 0)
            elif effective == "B3":
                score = (sum(counts[p] for p in group) / max(cost, 0.001), 0)
            elif fast:
                # gain == 0.1*blocked + 0.9*full == the reference P1 gain: every request
                # whose full missing set is covered contributes 1.0 instead of its 0.1
                # blocked-on-group share. Same scores, same tie-breaking, fewer scans.
                group_set = set(group)
                blocked = sum(counts[p] for p in group)
                full, seen_identities = 0, set()
                for page in group:
                    for identity in by_page.get(page, ()):
                        if identity not in seen_identities:
                            seen_identities.add(identity)
                            subset_checks += 1 if diag is not None else 0
                            if full_missing[identity] <= group_set:
                                full += 1
                gain = 0.1 * blocked + 0.9 * full
                score = (gain / max(cost, 0.001), -cost)
            else:
                group_set, gain = set(group), 0.0
                for identity, blocker in visible:
                    if expired():
                        break
                    subset_checks += 1 if diag is not None else 0
                    if identity in full_missing and full_missing[identity] <= group_set:
                        gain += 1.0
                    elif blocker in group_set:
                        gain += 0.1
                score = (gain / max(cost, 0.001), -cost)
            if diag is not None:
                scored.append([list(group), [score[0], score[1]]])
            if best_score is None or score > best_score:
                best, best_score = group, score
        self.decisions += 1
        elapsed = (time.perf_counter_ns() - wall) / 1000
        self.identify_wall_us += elapsed
        self.identify_cpu_us += (time.process_time_ns() - cpu) / 1000
        self.id_overrun_us = max(0.0, self.identify_wall_us - o.identify_budget_us)
        self.cpu_overrun_us = max(0.0, self.identify_cpu_us - o.identify_cpu_budget_us)
        if self.identify_wall_us >= o.identify_budget_us or self.identify_cpu_us >= o.identify_cpu_budget_us:
            self.closed = True
            best = []
        if best:
            self.suggested_groups.append(best)
            self.priority_used_us += sum(self.tasks[p].cost_us for p in best)
        if diag is not None:
            row.update(score_wall_us=(time.perf_counter_ns() - phase) / 1000,
                       closure_wall_us=closure_wall_ns / 1000, subset_checks=subset_checks,
                       closures_attempted=len(candidates),
                       top_candidates=sorted(scored, key=lambda s: s[1], reverse=True)[:5],
                       selected=list(best), decision_wall_us=elapsed,
                       identify_wall_after_us=self.identify_wall_us,
                       priority_used_after_us=self.priority_used_us, selector_closed=self.closed)
            diag.append(row)
        if o.policy == "C0":
            return [], elapsed
        return best, elapsed


def percentile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * fraction) - 1)]


def fixture_document(task_list, request_list, history, preparation_us=0, barrier_us=0):
    request_rows = []
    for request in request_list:
        row = dataclasses.asdict(request)
        if row['cancel_us'] == math.inf:
            row['cancel_us'] = None
        request_rows.append(row)
    return {'schema': 1, 'victim': 'A', 'epoch': 1, 'parallelism': 1,
            'tasks': [dataclasses.asdict(t) for t in task_list], 'requests': request_rows,
            'history': [[k, dataclasses.asdict(h), n] for k, h, n in history],
            'preparation_us': preparation_us, 'barrier_us': barrier_us}


def fixture_fingerprint(document):
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def load_fixture(document):
    def number(value, nonnegative=True):
        return type(value) in (int, float) and math.isfinite(value) and (not nonnegative or value >= 0)

    def names(value):
        return isinstance(value, (list, tuple)) and all(isinstance(p, str) and p for p in value)

    def version(value):
        return value is None or (type(value) is int and value >= 0)

    try:
        if (set(document) != {'schema', 'victim', 'epoch', 'parallelism', 'tasks', 'requests', 'history', 'preparation_us', 'barrier_us'}
                or any(type(document[k]) is not int or document[k] != 1 for k in ('schema', 'epoch', 'parallelism'))
                or document['victim'] != 'A' or not all(number(document[k]) for k in ('preparation_us', 'barrier_us'))):
            raise ValueError('unsupported common recovery fixture')
        for row in document['tasks']:
            if (not isinstance(row['name'], str) or not row['name'] or not isinstance(row['role'], str)
                    or not number(row['cost_us']) or not names(row['dependencies'])
                    or any(type(row[k]) is not bool for k in ('ready', 'legal', 'fail'))
                    or type(row['epoch']) is not int or row['epoch'] != 1
                    or (row['ready'] and (row['fail'] or not row['legal']))):
                raise ValueError('invalid task schema')
        for row in document['requests']:
            if (any(not isinstance(row[k], str) or not row[k] for k in ('identity', 'key'))
                    or not names(row['index']) or (row['data'] is not None and (not isinstance(row['data'], str) or not row['data']))
                    or type(row['complete']) is not bool or not version(row['generation'])
                    or not all(number(row[k]) for k in ('arrival_us', 'txn_ready_us'))
                    or (row['cancel_us'] is not None and (not number(row['cancel_us']) or row['cancel_us'] < row['arrival_us']))):
                raise ValueError('invalid request schema')
        for key, row, node in document['history']:
            if (not isinstance(key, str) or not key or not isinstance(node, str) or not node
                    or not names(row['path']) or type(row['complete']) is not bool
                    or not version(row['generation']) or not number(row['seen_us'], nonnegative=False)):
                raise ValueError('invalid history schema')
        tasks = [Task(**dict(row, dependencies=tuple(row['dependencies']))) for row in document['tasks']]
        requests = [Request(**dict(row, index=tuple(row['index']), cancel_us=math.inf if row['cancel_us'] is None else row['cancel_us'])) for row in document['requests']]
        history = [(k, Hint(**dict(row, path=tuple(row['path']))), n) for k, row, n in document['history']]
    except (KeyError, TypeError) as error:
        raise ValueError('malformed common recovery fixture') from error
    return tasks, requests, history


def simulate(task_list, request_list, history, options, preparation_us=0, barrier_us=0,
             cross_decider=None, forced_publications=None):
    """cross_decider(now, pending, ready, waiting, selected) is a diagnostic hook invoked
    after each online decision, before its identification time is charged; its own cost is
    never charged to the model. forced_publications replays a recorded publication order
    without any identification charging: a zero-cost counterfactual, never a formal result."""
    options.validate()
    if any(not math.isfinite(t) or t < 0 for t in (preparation_us, barrier_us)):
        raise ValueError('invalid shared preparation/barrier cost')
    tasks = OrderedDict()
    for task in task_list:
        if task.epoch != 1:
            raise ValueError("this L0 fixture requires recovery epoch 1")
        if not math.isfinite(task.cost_us) or task.cost_us < 0:
            raise ValueError("invalid task cost")
        if task.name in tasks and tasks[task.name] != task:
            raise ValueError("conflicting duplicate task")
        tasks[task.name] = task
    requests = OrderedDict()
    for request in request_list:
        if any(not math.isfinite(t) or t < 0 for t in (request.arrival_us, request.txn_ready_us)) or math.isnan(request.cancel_us) or request.cancel_us < request.arrival_us:
            raise ValueError('invalid request timing')
        if request.identity in requests and requests[request.identity] != request:
            raise ValueError("conflicting duplicate request")
        requests[request.identity] = request
    selector = Selector(tasks, options, history)
    pending = OrderedDict((name, None) for name, task in tasks.items() if not task.ready)
    ready = {name: 0.0 for name, task in tasks.items() if task.ready}
    terminal, errors, order, publications = {}, {}, [], []
    now = float(preparation_us)
    planned, planned_priority, executed_priority_us = [], False, 0.0

    def invalidate(at):
        changed = True
        while changed:
            changed = False
            for name in list(pending):
                task = tasks[name]
                if not task.legal or any(d in errors or d not in tasks for d in task.dependencies):
                    errors[name] = at
                    pending.pop(name)
                    changed = True

    def refresh(at):
        waiting = []
        for identity, request in requests.items():
            if identity in terminal or request.arrival_us > at:
                continue
            complete_at = max([request.arrival_us, request.txn_ready_us, barrier_us]
                              + [ready.get(p, math.inf) for p in request.path])
            prefix_ready, failure_at = request.arrival_us, math.inf
            for page in request.path:
                if page in errors:
                    failure_at = max(prefix_ready, errors[page])
                    break
                if page not in ready:
                    break
                prefix_ready = max(prefix_ready, ready[page])
            outcomes = [(request.cancel_us, 2, 'cancelled'), (failure_at, 1, 'failed')]
            if request.complete and request.path:
                outcomes.append((complete_at, 0, 'completed'))
            end, _, status = min(outcomes)
            if end <= at:
                terminal[identity] = (status, end)
            else:
                blocker = next((p for p in request.path if p not in ready), None)
                if blocker:
                    waiting.append((identity, request.key, request.generation, blocker))
        return waiting

    invalidate(now)
    replay = forced_publications is not None
    replay_index = 0
    while pending:
        waiting = refresh(now)
        planned = [p for p in planned if p in pending]
        if replay:
            identification_us = 0.0
            priority = False
            if replay_index < len(forced_publications):
                selected, replay_index = list(forced_publications[replay_index]), replay_index + 1
            else:
                selected = []
        elif planned:
            selected, planned, identification_us = planned, [], 0.0
            priority = planned_priority
        else:
            selected, identification_us = selector.choose(now, pending, ready, waiting)
            priority = bool(selected)
            if cross_decider is not None:
                cross_decider(now, pending, ready, waiting, selected)
        if options.charge_identification:
            now += identification_us
        if not selected:
            for name in pending:
                selected = selector.closure((name,), pending, ready, len(tasks))
                if selected:
                    break
        if not selected:
            errors.update((name, now) for name in pending)
            pending.clear()
            break
        # One worker and one publisher; a priority group never enlarges a batch.
        batch = list(selected[:options.publish_batch])
        planned, planned_priority = list(selected[options.publish_batch:]), priority
        priority_names = set(selected) if priority else set()
        for name in pending:
            if len(batch) >= options.publish_batch:
                break
            if name not in batch and tasks[name].legal and all(d in ready or d in batch for d in tasks[name].dependencies):
                batch.append(name)
        succeeded = []
        for name in batch:
            if name not in pending:
                continue
            if not tasks[name].legal or not all(d in ready or d in succeeded for d in tasks[name].dependencies):
                raise AssertionError('executor received an illegal dependency order')
            now += tasks[name].cost_us
            executed_priority_us += tasks[name].cost_us if name in priority_names else 0
            order.append(name)
            pending.pop(name)
            if tasks[name].fail:
                errors[name] = now
                invalidate(now)
            else:
                succeeded.append(name)
        for name in succeeded:
            ready[name] = max(now, barrier_us)
        publications.append({"at_us": max(now, barrier_us), "materialized_at_us": now, "pages": succeeded})
        refresh(now)
    work_done = now
    horizon = max(now, options.window_us)
    refresh(horizon)
    completed = [(requests[i], at) for i, (status, at) in terminal.items() if status == "completed"]
    latencies = [at - r.arrival_us for r, at in completed]
    tails = [max(0, max(ready[p] for p in r.index) - ready[r.data])
             for r, _ in completed if r.data is not None and r.index]
    arrived = [r for r in requests.values() if r.arrival_us <= horizon]
    hint_bytes = sys.getsizeof(selector.hints) + sum(sys.getsizeof(k) + sys.getsizeof(h) + sys.getsizeof(vars(h))
                     + sum(sys.getsizeof(v) for v in vars(h).values())
                     + sum(sys.getsizeof(p) for p in h.path) for k, h in selector.hints.items())
    window = options.window_us
    window_requests = [r for r in requests.values() if r.arrival_us <= window]
    window_terminal = {i: (s, t) for i, (s, t) in terminal.items() if t <= window}
    window_latencies = [at - r.arrival_us for r, at in completed if at <= window]
    observed_waits = [min(terminal.get(r.identity, ('unfinished', math.inf))[1], window) - r.arrival_us for r in window_requests]
    cohort_tails, unknown_tails, not_applicable_tails = [], 0, 0
    for request in window_requests:
        if not request.complete:
            unknown_tails += 1
        elif request.data is None or not request.index:
            not_applicable_tails += 1
        elif any(p not in ready for p in request.path):
            unknown_tails += 1
        else:
            cohort_tails.append(max(0, max(ready[p] for p in request.index) - ready[request.data]))
    document = fixture_document(task_list, request_list, history, preparation_us, barrier_us)
    return {
        'fixture_sha256': fixture_fingerprint(document),
        'observation_end_us': horizon,
        'window_arrived': len(window_requests),
        'window_unfinished': sum(r.identity not in window_terminal for r in window_requests),
        'window_failed': sum(s == 'failed' for s, _ in window_terminal.values()),
        'window_cancelled': sum(s == 'cancelled' for s, _ in window_terminal.values()),
        'window_p95_us_completed_only': percentile(window_latencies, .95),
        'window_p99_us_completed_only': percentile(window_latencies, .99),
        'window_censored_wait_us': sum(observed_waits),
        'window_restricted_wait_sum_us': sum(observed_waits),
        'window_unfinished_wait_us': sum(window - r.arrival_us for r in window_requests if r.identity not in window_terminal),
        'window_observed_wait_p95_us': percentile(observed_waits, .95),
        'window_observed_wait_p99_us': percentile(observed_waits, .99),
        'index_tail_sum_us_fixed_cohort': sum(cohort_tails),
        'index_tail_known_fixed_cohort': len(cohort_tails),
        'index_tail_unknown_fixed_cohort': unknown_tails,
        'index_tail_not_applicable_fixed_cohort': not_applicable_tails,
        'index_tail_cohort_size': len(window_requests),
        'recovery_parallelism': 1, 'shared_preparation_us': preparation_us,
        'recovery_work_us': sum(tasks[n].cost_us for n in order),
        "result_kind": "L0_MODEL_NOT_DATABASE_SPEEDUP", "policy": options.policy,
        "fixed_window_us": options.window_us,
        "model_completed_in_window": sum(at <= options.window_us for _, at in completed),
        "completed": len(completed), "p95_us_completed_only": percentile(latencies, .95),
        "p99_us_completed_only": percentile(latencies, .99),
        "unfinished": sum(r.identity not in terminal for r in arrived),
        "failed": sum(s == "failed" for s, _ in terminal.values()),
        "cancelled": sum(s == "cancelled" for s, _ in terminal.values()),
        "index_tail_sum_us_completed_only": sum(tails),
        "index_tail_p95_us_completed_only": percentile(tails, .95),
        "all_recovery_done_us": None if errors else max(work_done, barrier_us),
        "work_end_us": work_done, "error_tasks": sorted(errors), "error_times_us": errors,
        "identification_wall_us": selector.identify_wall_us,
        "identification_cpu_us": selector.identify_cpu_us,
        "identification_overrun_us": selector.id_overrun_us,
        "identification_cpu_overrun_us": selector.cpu_overrun_us,
        "identification_decisions": selector.decisions,
        "sampling_wall_us": selector.sampling_wall_us, "sampling_cpu_us": selector.sampling_cpu_us,
        "sampling_records_seen": selector.sampling_records_seen, "sampled_records": selector.sampled_records,
        "hint_memory_estimate_bytes": hint_bytes, "hint_count": len(selector.hints),
        "ambiguous_hint_count": sum(not h.complete for h in selector.hints.values()),
        "hint_summary_sha256": fixture_fingerprint([[k, dataclasses.asdict(h)] for k, h in selector.hints.items()]),
        "cost_contract": {
            "recovery": "synthetic task costs plus serially charged measured identification wall time",
            "sampling": "offline replay of assumed pre-fault summary maintenance; measured separately, not production overhead",
            "diagnostics": "E0 acquisition excluded and not claimed free; no detailed E0 recorder in L0",
            "budget": "cooperative wall/CPU deadlines, actual overrun reported and charged; not a hard real-time guarantee",
            "memory": "persistent Python hint object estimate including per-object dicts; may double-count shared values, excludes runtime/transients"},
        "e0_diagnostic_cost_in_model_us": 0,
        "extra_probe_io_bytes": 0,
        "priority_used_us": executed_priority_us,
        "recognition_priority_budget_used_us": selector.priority_used_us,
        "execution_order": order, "publications": publications,
        "suggested_groups": selector.suggested_groups, "ready_us": ready,
        "terminal": terminal, "options": dataclasses.asdict(options),
        **({"decision_diagnostics": selector.diag_events} if selector.diag_events is not None else {}),
        **({"counterfactual_mode": "zero_cost_publication_replay",
            "counterfactual_note": "identification is not charged; priority accounting is not meaningful here"}
           if replay else {}),
    }


def read_profile(path):
    events = [json.loads(line) for line in Path(path).read_text().splitlines()]
    if not events or events[0].get('event') != 'trace_config' or events[0].get('schema') != 2 or events[-1].get('event') != 'trace_summary':
        raise ValueError('profile requires a complete schema-2 trace')
    if events[-1]['dropped_events'] or events[-1]['io_error']:
        raise ValueError('incomplete trace: dropped events or output error')
    static_fixture = any(e['event'] == 'profile_contract' and e.get('a') == 1 for e in events)
    validated = {(e['pid'], e['request']) for e in events if e['event'] == 'request_end' and e['a'] == 1}
    failure_us = min((e['ts_us'] for e in events if e['event'] == 'recovery_begin' and e.get('epoch', 0) > 0),
                     default=max(e.get('ts_us', 0) for e in events) + 1)
    lookups, profiles = {}, []
    for event in sorted((e for e in events if 'ts_us' in e), key=lambda e: e['ts_us']):
        identity = (event['pid'], event['lookup'])
        if event['event'] == 'lookup_begin':
            lookups[identity] = {'begin': event, 'path': [], 'target': None, 'status': None}
        elif event['event'] in ('path_page', 'lookup_target', 'path_status', 'lookup_end'):
            if identity not in lookups:
                raise ValueError('orphan lookup event')
            row = lookups[identity]
            if event['event'] == 'path_page':
                row['path'].append(f"i:{event['table']}:{event['page']}")
            elif event['event'] == 'lookup_target':
                if event['table'] < 0 or event['page'] < 0:
                    raise ValueError('unknown heap target identity')
                row['target'] = f"d:{event['table']}:{event['page']}"
            elif event['event'] == 'path_status':
                row['status'] = event
            else:
                begin = row['begin']
                if not event['c'] or (event['pid'], event['request']) not in validated:
                    continue
                index = tuple(dict.fromkeys(row['path']))
                if not index or not row['status'] or (bool(event['a']) != (row['target'] is not None)):
                    raise ValueError('incomplete sampled lookup or invalid NOT_FOUND target')
                if row['status']['a'] != begin['generation'] or not row['status']['b']:
                    continue
                variant = hashlib.sha256(json.dumps([index, row['target']]).encode()).hexdigest()[:12]
                key = f"{event['table']}:{event['key']}:{variant}"
                source_node = {0: 'A', 1: 'B', 2: 'C'}.get(begin.get('node'), 'UNKNOWN')
                profiles.append((key, index, row['target'], begin['generation'], static_fixture,
                                 source_node, event['ts_us'] - failure_us))
    if not profiles:
        raise ValueError('no complete externally validated paths')
    return profiles


def fixture(seed, scenario="hotspot", profile=None):
    rng = random.Random(seed)
    if profile:
        profiles = read_profile(profile)
    else:
        profiles = [(str(k), ("i:root", f"i:leaf:{k // 4}"), f"d:{k}", 1, True, None, 0) for k in range(32)]
        profiles += [("missing", ("i:root", "i:leaf:7"), None, 1, True, None, 0)]
    unique = OrderedDict((key, (index, data, generation, known, node, seen))
                         for key, index, data, generation, known, node, seen in profiles)
    names = sorted({p for index, data, *_ in unique.values() for p in index + (() if data is None else (data,))})
    tasks = []
    for name in sorted(names, key=lambda p: (p.startswith("i:"), p)):
        is_index = name.startswith("i:")
        is_ready = (scenario == "index_only" and not is_index) or (scenario == "data_only" and is_index)
        tasks.append(Task(name, "index" if is_index else "data", rng.randint(200, 600), ready=is_ready))
    keys = list(unique)
    history = []
    for i in range(2000):
        key = rng.choice(keys[:max(1, len(keys) // 4)]) if rng.random() < .8 else rng.choice(keys)
        index, data, generation, known, node, seen = unique[key]
        history.append((online_key(key), Hint(index + (() if data is None else (data,)), generation=generation,
                        complete=known, seen_us=seen), node or ("B" if i % 2 else "C")))
    requests = []
    for i in range(200):
        hot = keys[-max(1, len(keys) // 4):] if scenario == "switch" and i >= 50 else keys[:max(1, len(keys) // 4)]
        key = rng.choice(keys) if scenario == "uniform" or rng.random() >= .8 else rng.choice(hot)
        index, data, generation, known, node, seen = unique[key]
        requests.append(Request(f"{'B' if i % 2 else 'C'}:{i}", online_key(key), index, data, i * 20.0, generation=generation))
    return tasks, requests, history


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--profile", type=Path)
    inputs.add_argument("--fixture", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenario", choices=["hotspot", "switch", "uniform", "index_only", "data_only"], default="hotspot")
    parser.add_argument("--policies", default="B0,B1,B3,P1,C0")
    parser.add_argument("--seed", type=int, default=20260915)
    parser.add_argument("--sample-rate", type=float, default=.01)
    parser.add_argument("--summary-capacity", type=int, default=512)
    parser.add_argument("--candidate-limit", type=int, default=128)
    parser.add_argument("--max-hint-pages", type=int, default=64)
    parser.add_argument("--identify-budget-us", type=float, default=2000)
    parser.add_argument("--identify-cpu-budget-us", type=float, default=2000)
    parser.add_argument("--priority-budget-us", type=float, default=2000)
    parser.add_argument("--publish-batch", type=int, default=1)
    parser.add_argument("--max-group", type=int, default=3)
    parser.add_argument("--window-us", type=float, default=4000)
    parser.add_argument("--barrier-us", type=float, default=0)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists; choose a new isolated result path")
    if args.fixture:
        document = json.loads(args.fixture.read_text())
        tasks, requests, history = load_fixture(document)
        preparation_us, barrier_us = document['preparation_us'], document['barrier_us']
    else:
        tasks, requests, history = fixture(args.seed, args.scenario, args.profile)
        preparation_us, barrier_us = 0, args.barrier_us
        document = fixture_document(tasks, requests, history, preparation_us, barrier_us)
    options = Options(**{f.name: getattr(args, f.name) for f in dataclasses.fields(Options) if hasattr(args, f.name)})
    results = [simulate(tasks, requests, history, dataclasses.replace(options, policy=p), preparation_us=preparation_us, barrier_us=barrier_us)
               for p in args.policies.split(",")]
    if any(r['fixture_sha256'] != fixture_fingerprint(document) for r in results):
        raise ValueError('policy did not use the identical recovery fixture')
    payload = {"kind": "L0_MODEL_NOT_DATABASE_SPEEDUP", "victim": "A", "survivors": ["B", "C"],
               "seed": args.seed, "scenario": args.scenario, "recovery_parallelism": 1,
               "path_source": "saved_common_fixture" if args.fixture else "observed_BLink" if args.profile else "synthetic",
               "fixture": document, "fixture_sha256": fixture_fingerprint(document),
               "profile_sha256": hashlib.sha256(args.profile.read_bytes()).hexdigest() if args.profile else None,
               "arrival_and_failure_state": "synthetic; no real process killed",
               "replay_cost_source": "synthetic uniform integer 200..600 us, not measured BLink redo",
               "tasks": [dataclasses.asdict(t) for t in tasks], "results": results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as output:
        output.write(json.dumps(payload, indent=2, allow_nan=False) + "\n")
    for result in results:
        print(result["policy"], "model_window_completed=", result["model_completed_in_window"],
              "recovery_us=", result["all_recovery_done_us"], "id_wall_us=", round(result["identification_wall_us"], 2))


if __name__ == "__main__":
    main()
