#!/usr/bin/env python3
"""Small independent counterexamples for the trace audit, without running pools."""
from copy import deepcopy
import random
import unittest

from audit import audit, interval_oracle, Span


def event(ns, kind, phase, ident, parent=0, first=0, last=0, tid=0, worker=0):
    return dict(ns=ns, kind=kind, ph=phase, id=ident, parent=parent,
                a=first, b=last, tid=tid, worker=worker)


def fixture(mode='RAPID_AUTO', callbacks=True):
    events = [event(0, 'wave', 'B', 1, first=0, last=2),
              event(1, 'range', 'B', 2, 1, 0, 2)]
    parent = 2
    if mode not in ('RAPID_START', 'RAPID_GROUP'):
        events.append(event(2, 'chunk', 'B', 3, 2, 0, 2))
        parent = 3
    if callbacks:
        for index in range(2):
            ident, ns = 4 + index, 3 + index * 2
            events += [event(ns, 'callback', 'B', ident, parent, index, index + 1),
                       event(ns + 1, 'callback', 'E', ident, parent)]
    if parent == 3:
        events.append(event(8, 'chunk', 'E', 3, 2))
    events += [event(9, 'range', 'E', 2, 1), event(10, 'wave', 'E', 1)]
    return dict(schema='oox-scheduler-trace-v1', mode=mode, workload='synthetic',
                workers=2, tasks=2, iterations=1, oracle_ok=True,
                instrumented=True, dropped_events=0, events=events)


class AuditTests(unittest.TestCase):
    def test_recorded_chunks_and_fixed_ranges_pass(self):
        for mode in ('RAPID_AUTO', 'RAPID_START', 'RAPID_GROUP', 'EIGEN_AUTO'):
            with self.subTest(mode=mode):
                result = audit(fixture(mode))
                self.assertEqual(result['status'], 'pass', result)
                self.assertEqual(result['waves'][0]['last_callback_to_wave_end_ns'], 4)
                self.assertEqual(result['waves'][0]['useful_busy_by_native_thread'][0]['useful_busy_ns'], 2)

    def test_missing_callbacks_are_unverified_not_passed(self):
        result = audit(fixture(callbacks=False))
        self.assertEqual(result['status'], 'unverified')
        self.assertEqual(result['waves'][0]['callback_coverage']['status'], 'unverified')
        self.assertEqual(result['waves'][0]['executed_range_coverage']['status'], 'pass')

    def test_unknown_mode_is_unverified(self):
        result = audit(fixture('LEGACY_UNKNOWN'))
        self.assertEqual(result['status'], 'unverified')
        self.assertIn('mode', result['unverified']['by_check'])

    def test_scope_lifo_and_completeness_are_checked(self):
        for change in ('missing_end', 'wrong_end'):
            data = fixture()
            if change == 'missing_end':
                data['events'].pop()
            else:
                data['events'][4]['id'] = 999
            result = audit(data)
            self.assertEqual(result['status'], 'fail')
            self.assertIn('incomplete_scope', result['errors']['by_check'])

    def test_drop_fails_even_with_complete_coverage(self):
        data = fixture()
        data['dropped_events'] = 1
        self.assertEqual(audit(data)['status'], 'fail')

    def test_per_wave_oracle_catches_balanced_aggregate_duplicates(self):
        data = fixture()
        second = deepcopy(data['events'])
        for e in second:
            e['ns'] += 20
            e['id'] += 100
            if e['parent']:
                e['parent'] += 100
            if e['kind'] == 'wave' and e['ph'] == 'B':
                e['a'] = 1
        data['iterations'] = 2
        for e in data['events']:
            if e['kind'] == 'callback' and e['ph'] == 'B':
                e['a'], e['b'] = 0, 1
        for e in second:
            if e['kind'] == 'callback' and e['ph'] == 'B':
                e['a'], e['b'] = 1, 2
        data['events'] += second
        result = audit(data)
        self.assertEqual(result['status'], 'fail')
        self.assertEqual(result['errors']['by_check']['callback_coverage'], 2)
        for wave in result['waves']:
            self.assertEqual(wave['callback_coverage']['missing_indices'], 1)
            self.assertEqual(wave['callback_coverage']['duplicate_indices'], 1)

    def test_integer_interval_oracle_rejects_overlap_gap_and_bounds(self):
        for intervals in ([(0, 2), (1, 3)], [(0, 1), (2, 3)], [(0, 4)]):
            spans = [Span(event(0, 'chunk', 'B', index, first=a, last=b), 1)
                     for index, (a, b) in enumerate(intervals)]
            self.assertEqual(interval_oracle(spans, 3)['status'], 'fail')

    def test_generated_intervals_match_plain_integer_visit_reference(self):
        seed = 0xA0D1710
        rng = random.Random(seed)
        for case in range(256):
            size = 1 + case % 32
            intervals = [(rng.randrange(size + 1), rng.randrange(size + 1))
                         for _ in range(case % 11)]
            expected = [0] * size
            invalid = 0
            for first, last in intervals:
                if not 0 <= first < last <= size:
                    invalid += 1
                else:
                    for index in range(first, last):
                        expected[index] += 1
            spans = [Span(event(0, 'chunk', 'B', index, first=a, last=b), 1)
                     for index, (a, b) in enumerate(intervals)]
            with self.subTest(seed=seed, case=case):
                actual = interval_oracle(spans, size)
                self.assertEqual(actual['missing_indices'], expected.count(0))
                self.assertEqual(actual['duplicate_indices'], sum(count > 1 for count in expected))
                self.assertEqual(actual['excess_visits'], sum(max(0, count - 1) for count in expected))
                self.assertEqual(actual['invalid_intervals'], invalid)

    def test_callback_parent_must_contain_index(self):
        data = fixture()
        data['events'][3]['a'], data['events'][3]['b'] = 2, 3
        result = audit(data)
        self.assertIn('callback_containment', result['errors']['by_check'])

    def test_task_cleanup_envelope_may_finish_after_wave(self):
        data = fixture()
        data['events'] += [event(1, 'task', 'B', 9, tid=1, worker=1),
                           event(11, 'task', 'E', 9, tid=1, worker=1)]
        result = audit(data)
        self.assertEqual(result['status'], 'pass', result)
        self.assertEqual(result['waves'][0]['envelopes_ending_after_wave'], 1)

    def test_callback_may_not_finish_after_wave(self):
        data = fixture()
        data['events'] += [event(1, 'range', 'B', 9, first=0, last=1, tid=1, worker=1),
                           event(2, 'callback', 'B', 10, 9, 0, 1, tid=1, worker=1),
                           event(11, 'callback', 'E', 10, 9, tid=1, worker=1),
                           event(12, 'range', 'E', 9, tid=1, worker=1)]
        result = audit(data)
        self.assertIn('work_after_wave', result['errors']['by_check'])

    def test_split_donor_identity_and_specification(self):
        data = fixture()
        data['events'] += [event(3, 'range_spec', 'i', 20, first=1, last=2, tid=1, worker=1),
                           event(4, 'split', 'i', 20, 2, 1, 2, tid=1, worker=1),
                           event(5, 'steal_range', 'i', 20, first=0, last=1, tid=1, worker=1),
                           event(6, 'discard', 'i', 20, tid=1, worker=1)]
        self.assertEqual(audit(data)['status'], 'pass')
        for field, value, finding in (('a', 1, 'range_theft_donor'), ('b', 0, 'range_theft_donor')):
            changed = deepcopy(data)
            changed['events'][-2][field] = value
            self.assertIn(finding, audit(changed)['errors']['by_check'])
        changed = deepcopy(data)
        changed['events'][-3]['b'] = 3
        self.assertIn('split_bounds', audit(changed)['errors']['by_check'])
        changed = deepcopy(data)
        changed['events'][-4]['a'] = 0
        self.assertIn('split_specification', audit(changed)['errors']['by_check'])

    def test_counters_are_recording_totals_without_wave_inference(self):
        data = fixture()
        data['thread_counters'] = [dict(tid=0, range_lock_acquired=4, owner_wait_spins=3),
                                   dict(tid=1, range_lock_acquired=5, owner_wait_spins=6)]
        result = audit(data)
        self.assertEqual(result['aggregate_counters']['range_lock_acquired'], 9)
        self.assertEqual(result['aggregate_counters']['owner_wait_spins'], 9)
        self.assertEqual(result['counter_scope'], 'entire_recording')
        self.assertNotIn('aggregate_counters', result['waves'][0])

    def test_scope_drain_metadata_is_echoed_and_checked(self):
        for key, value, expected in (('pending_scopes', 1, 'fail'),
                                     ('truncated_scopes', 1, 'fail'),
                                     ('scope_drain_timed_out', True, 'fail'),
                                     ('closing_rejected_scopes', 1, 'unverified')):
            data = fixture()
            data[key] = value
            with self.subTest(key=key):
                result = audit(data)
                self.assertEqual(result['status'], expected)
                self.assertEqual(result['scope_drain'][key], value)
        data = fixture()
        data.update(pending_scopes=0, truncated_scopes=0, scope_drain_timed_out=False,
                    closing_rejected_scopes=0, scope_drain_timeout_ms=250, scope_drain_ns=42)
        self.assertEqual(audit(data)['status'], 'pass')
        self.assertEqual(audit(fixture())['scope_drain'], {})

    def test_invalid_scope_drain_metadata_is_rejected(self):
        for key, value in (('pending_scopes', -1), ('truncated_scopes', '0'),
                           ('scope_drain_timed_out', 1), ('closing_rejected_scopes', False)):
            data = fixture()
            data[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                audit(data)

    def test_optional_phases_keep_recorded_arguments_and_offsets(self):
        data = fixture()
        data['events'].insert(1, event(0, 'claimed', 'i', 21, 1, 1, 0))
        phases = audit(data)['waves'][0]['phases']
        self.assertEqual(phases[0]['kind'], 'claimed')
        self.assertEqual(phases[0]['offset_ns'], 0)
        self.assertEqual(phases[0]['a'], 1)
        data['events'][1]['kind'] = 'rapid_claimed'
        self.assertEqual(audit(data)['waves'][0]['phases'][0]['stage'], 'claimed')


if __name__ == '__main__':
    unittest.main()
