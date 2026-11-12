#!/usr/bin/env python3
"""Recorder, overflow, format, and actual scheduling-oracle integration tests."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from record import validate, render

BUILD = Path(sys.argv.pop(1)).resolve() / 'benchmarks/scheduler_eval'


def run_recorder(mode):
    result = subprocess.run([str(BUILD / 'test_scheduler_trace_recorder'), mode],
                            capture_output=True, text=True, check=True, timeout=20)
    data = json.loads(result.stdout)
    validate(data)
    return data


class TraceTests(unittest.TestCase):
    def test_thread_local_recording_and_scope_crossing_start(self):
        data = run_recorder('normal')
        events = data['events']
        self.assertEqual(len(events), 900)
        self.assertEqual(data['dropped_events'], 0)
        self.assertEqual(data['pending_scopes'], 0)
        self.assertEqual(data['truncated_scopes'], 0)
        self.assertFalse(data['scope_drain_timed_out'])
        self.assertEqual(sum(t['range_lock_acquired'] for t in data['thread_counters']), 300)
        self.assertEqual({e['worker'] for e in events}, {0, 1, 2})
        self.assertFalse(any(e['kind'] == 'before_session' for e in events))
        begins = {e['id']: e for e in events if e['ph'] == 'B'}
        ends = {e['id']: e for e in events if e['ph'] == 'E'}
        self.assertEqual(len(begins), 300)
        self.assertEqual(begins.keys(), ends.keys())
        for id, begin in begins.items():
            self.assertEqual(begin['tid'], ends[id]['tid'])
            self.assertGreaterEqual(ends[id]['ns'], begin['ns'])

    def test_bounded_overflow_is_explicit(self):
        data = run_recorder('overflow')
        self.assertEqual(len(data['events']), 6)
        # Once a begin is dropped its end is suppressed, not falsely paired.
        self.assertEqual(data['dropped_events'], 597)
        self.assertEqual(data['truncated_scopes'], 3)
        self.assertEqual(data['pending_scopes'], 0)

    def test_accepted_scope_ends_after_admission_closes(self):
        data = run_recorder('closing')
        accepted = [e for e in data['events'] if e['kind'] == 'accepted_before_close']
        self.assertEqual([e['ph'] for e in accepted], ['B', 'E'])
        self.assertEqual(accepted[0]['id'], accepted[1]['id'])
        self.assertFalse(any(e['kind'] == 'rejected_after_close' for e in data['events']))
        self.assertTrue(any(e['kind'] == 'after_close_marker' for e in data['events']))
        self.assertEqual(sum(t['range_lock_acquired'] for t in data['thread_counters']), 17)
        self.assertEqual(data['closing_rejected_scopes'], 1)
        self.assertEqual(data['pending_scopes'], 0)
        self.assertEqual(data['truncated_scopes'], 0)
        self.assertFalse(data['scope_drain_timed_out'])

    def test_scope_drain_is_bounded_and_keeps_truncation_explicit(self):
        started = time.monotonic()
        data = run_recorder('timeout')
        self.assertLess(time.monotonic() - started, 3)
        held = [e for e in data['events'] if e['kind'] == 'held_past_timeout']
        self.assertEqual([e['ph'] for e in held], ['B'])
        self.assertEqual(data['scope_drain_timeout_ms'], 250)
        self.assertTrue(data['scope_drain_timed_out'])
        self.assertEqual(data['pending_scopes'], 1)
        self.assertEqual(data['truncated_scopes'], 1)

    def test_stop_can_overlap_writers(self):
        for case in range(20):
            with self.subTest(case=case):
                data = run_recorder('racing')
                self.assertTrue(data['events'])
                self.assertEqual(data['pending_scopes'], 0)
                self.assertFalse(data['scope_drain_timed_out'])
                if not data['dropped_events']:
                    self.assertEqual(data['truncated_scopes'], 0)
                    begins = {e['id'] for e in data['events'] if e['ph'] == 'B'}
                    ends = {e['id'] for e in data['events'] if e['ph'] == 'E'}
                    self.assertEqual(begins, ends)

    def test_render_does_not_allow_script_injection(self):
        data = run_recorder('normal')
        data['mode'] = '</script><script>alert(1)</script>'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'viewer.html'
            render([data], path)
            text = path.read_text()
            self.assertNotIn(data['mode'], text)
            self.assertNotIn('src="model.js"', text)

    def test_real_modes_against_visitation_oracle(self):
        with tempfile.TemporaryDirectory() as directory:
            case = 0
            for threads in (1, 2, 4):
                for size in (1, 2, 7, 64, 257):
                    for mode in ('RAPID_AUTO', 'RAPID_START', 'EIGEN_AUTO', 'EIGEN_STATIC',
                                 'RAPID_NATIVE_AUTO', 'RAPID_NATIVE_SIMPLE', 'RAPID_NATIVE_STATIC', 'RAPID_NATIVE_AFFINITY'):
                        case += 1
                        with self.subTest(case=case, threads=threads, size=size, mode=mode):
                            path = Path(directory) / f'{case}.json'
                            env = dict(os.environ, BENCH_NUM_THREADS=str(threads),
                                       OOX_RAPID_RESIDENT_LIMIT=str(threads), OOX_RAPID_AUTOCALIBRATE='0')
                            subprocess.run([str(BUILD / f'trace_scheduler_{mode}'), '--output', str(path),
                                            '--tasks', str(size), '--iterations', '2', '--work', '2',
                                            '--shape', 'tail', '--callbacks'], env=env, check=True,
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
                            data = json.loads(path.read_text())
                            self.assertEqual(validate(data), 0)
                            calls = [e for e in data['events'] if e['kind'] == 'callback' and e['ph'] == 'B']
                            # Independent, deliberately simple oracle: each integer
                            # occurs twice, irrespective of scheduler structure.
                            self.assertEqual(sorted(e['a'] for e in calls), sorted(list(range(size)) * 2))
                            if mode != 'RAPID_START':
                                chunks = [e for e in data['events'] if e['kind'] == 'chunk' and e['ph'] == 'B']
                                self.assertEqual(sorted(i for e in chunks for i in range(e['a'], e['b'])),
                                                 sorted(list(range(size)) * 2))
                            if mode == 'RAPID_START':
                                self.assertFalse(any(e['kind'].startswith('steal') for e in data['events']))


if __name__ == '__main__':
    unittest.main()
