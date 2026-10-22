#!/usr/bin/env python3
"""Audit recorded scheduler events; timings describe instrumentation, not speedups."""
import argparse
from array import array
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
import json
from pathlib import Path
import sys

MAX_TASKS = 1048576
MAX_EVENTS = 4194304
KNOWN_MODES = {'RAPID_AUTO', 'RAPID_START', 'RAPID_GROUP', 'EIGEN_AUTO',
               'EIGEN_STATIC', 'EIGEN_AUTO_RESIDENT', 'EIGEN_SIMPLE', 'EIGEN_AFFINITY',
               'EIGEN_STEALING', 'EIGEN_SHARING', 'TBB_AUTO',
               'RAPID_NATIVE_AUTO', 'RAPID_NATIVE_SIMPLE', 'RAPID_NATIVE_STATIC', 'RAPID_NATIVE_AFFINITY'}
FIXED_MODES = {'RAPID_START', 'RAPID_GROUP'}
PHASES = ('rapid_begin', 'claimed', 'prepared', 'published', 'caller_finished',
          'commands_joined', 'tree_joined', 'closed')
PHASE_ALIASES = {name: name for name in PHASES}
PHASE_ALIASES.update({'rapid_' + name: name for name in PHASES if name != 'rapid_begin'})
DRAIN_COUNTS = ('scope_drain_timeout_ms', 'scope_drain_ns', 'pending_scopes',
                'truncated_scopes', 'closing_rejected_scopes')


@dataclass(slots=True)
class Span:
    event: dict
    end: int | None = None


class Findings:
    def __init__(self):
        self.counts = Counter()
        self.samples = []

    def add(self, check, detail):
        self.counts[check] += 1
        if len(self.samples) < 64:
            self.samples.append({'check': check, 'detail': detail})

    def result(self):
        return {'count': sum(self.counts.values()), 'by_check': dict(self.counts),
                'samples': self.samples}


def _integer(value, maximum=(1 << 64) - 1):
    return type(value) is int and 0 <= value <= maximum


def validate_format(data):
    if not isinstance(data, dict):
        raise ValueError('trace must be a JSON object')
    if data.get('schema') != 'oox-scheduler-trace-v1':
        raise ValueError('unsupported schema; correctness unverified')
    events = data.get('events')
    if not isinstance(events, list) or len(events) > MAX_EVENTS:
        raise ValueError('events must be an array of at most 4M entries')
    if not _integer(data.get('tasks'), MAX_TASKS) or not data['tasks']:
        raise ValueError('tasks must be 1..1M')
    if not _integer(data.get('iterations'), 1000) or not data['iterations']:
        raise ValueError('iterations must be 1..1000')
    if data['tasks'] * data['iterations'] > MAX_EVENTS:
        raise ValueError('expected callback count exceeds the 4M audit bound')
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError(f'event {index} is not an object')
        for key in ('ns', 'tid', 'id', 'parent', 'a', 'b'):
            if not _integer(event.get(key)):
                raise ValueError(f'event {index}: invalid {key}')
        if (type(event.get('worker')) is not int or event['worker'] < -1 or
                event.get('ph') not in ('B', 'E', 'i') or
                not isinstance(event.get('kind'), str)):
            raise ValueError(f'event {index}: invalid worker, phase, or kind')
    if not _integer(data.get('dropped_events', 0)):
        raise ValueError('invalid dropped_events')
    for key in DRAIN_COUNTS:
        if key in data and not _integer(data[key]):
            raise ValueError('invalid ' + key)
    if 'scope_drain_timed_out' in data and type(data['scope_drain_timed_out']) is not bool:
        raise ValueError('invalid scope_drain_timed_out')
    counters = data.get('thread_counters', [])
    if not isinstance(counters, list) or len(counters) > 128:
        raise ValueError('thread_counters must contain at most 128 lanes')
    seen = set()
    for lane in counters:
        if not isinstance(lane, dict) or not _integer(lane.get('tid')) or lane['tid'] in seen:
            raise ValueError('invalid or duplicate counter lane')
        seen.add(lane['tid'])
        if any(not _integer(value) for value in lane.values()):
            raise ValueError('invalid counter value')


def interval_oracle(spans, tasks):
    """Plain integer coverage, independent of any runtime splitting algorithm."""
    difference = array('i', [0]) * (tasks + 1)
    invalid, invalid_count = [], 0
    for span in spans:
        event = span.event
        first, last = event['a'], event['b']
        if not 0 <= first < last <= tasks:
            invalid_count += 1
            if len(invalid) < 16:
                invalid.append({'id': event['id'], 'range': [first, last]})
            continue
        difference[first] += 1
        difference[last] -= 1
    missing = duplicate = excess = count = 0
    missing_sample, duplicate_sample = [], []
    for index in range(tasks):
        count += difference[index]
        if count == 0:
            missing += 1
            if len(missing_sample) < 16:
                missing_sample.append(index)
        elif count > 1:
            duplicate += 1
            excess += count - 1
            if len(duplicate_sample) < 16:
                duplicate_sample.append({'index': index, 'count': count})
    return {'status': 'pass' if not (invalid or missing or duplicate) else 'fail',
            'intervals': len(spans), 'missing_indices': missing,
            'duplicate_indices': duplicate, 'excess_visits': excess,
            'invalid_intervals': invalid_count,
            'invalid_interval_samples': invalid, 'missing_samples': missing_sample,
            'duplicate_samples': duplicate_sample}


def union_time(spans):
    """Useful wall time per lane: overlapping/nested spans are counted once."""
    intervals = sorted((span.event['ns'], span.end) for span in spans if span.end is not None)
    total = 0
    start = end = None
    for left, right in intervals:
        if start is None:
            start, end = left, right
        elif left <= end:
            end = max(end, right)
        else:
            total += end - start
            start, end = left, right
    return total + (end - start if start is not None else 0)


def audit(data, source='<memory>'):
    validate_format(data)
    errors, unverified = Findings(), Findings()
    if data.get('mode') not in KNOWN_MODES:
        unverified.add('mode', f"unknown mode {data.get('mode')!r}")
    if 'oracle_ok' not in data:
        unverified.add('runtime_oracle', 'oracle_ok metadata is absent')
    elif data['oracle_ok'] is not True:
        errors.add('runtime_oracle', 'runtime did not report successful visitation')
    if 'dropped_events' not in data:
        unverified.add('recording_completeness', 'dropped_events metadata is absent')
    elif data['dropped_events']:
        errors.add('dropped_events', data['dropped_events'])
    for key in ('pending_scopes', 'truncated_scopes', 'scope_drain_timed_out'):
        if data.get(key):
            errors.add('recording_' + key, data[key])
    if data.get('closing_rejected_scopes'):
        unverified.add('closing_rejected_scopes', data['closing_rejected_scopes'])
    stacks, last_ns, by_id = defaultdict(list), {}, {}
    spans, instants = [], []
    specs, splits = {}, {}
    event_counts = Counter()
    for event in data['events']:
        tid, ident, kind = event['tid'], event['id'], event['kind']
        event_counts[kind] += 1
        if event['ns'] < last_ns.get(tid, 0):
            errors.add('lane_timestamp_order', {'tid': tid, 'id': ident})
        last_ns[tid] = event['ns']
        if event['ph'] == 'B':
            span = Span(event)
            if ident in by_id:
                errors.add('duplicate_scope_id', ident)
            else:
                by_id[ident] = span
            if stacks[tid] and event['parent'] != stacks[tid][-1].event['id']:
                errors.add('scope_parent', {'id': ident, 'recorded': event['parent'],
                                           'stack_parent': stacks[tid][-1].event['id']})
            stacks[tid].append(span)
            spans.append(span)
        elif event['ph'] == 'E':
            top = stacks[tid][-1] if stacks[tid] else None
            if top is None or (top.event['id'], top.event['kind']) != (ident, kind):
                errors.add('scope_end_mismatch', {'tid': tid, 'id': ident, 'kind': kind})
                continue
            if event['parent'] != top.event['parent']:
                errors.add('scope_end_parent', ident)
            top.end = event['ns']
            stacks[tid].pop()
        else:
            instants.append(event)
            if kind in ('range_spec', 'split'):
                target = specs if kind == 'range_spec' else splits
                if ident in target:
                    errors.add('duplicate_' + kind, ident)
                target[ident] = event
    for stack in stacks.values():
        for span in stack:
            errors.add('incomplete_scope', {'id': span.event['id'], 'kind': span.event['kind']})

    waves = sorted((span for span in spans if span.event['kind'] == 'wave'),
                   key=lambda span: span.event['ns'])
    if len(waves) != data['iterations']:
        errors.add('wave_count', {'expected': data['iterations'], 'recorded': len(waves)})
    if [wave.event['a'] for wave in waves] != list(range(data['iterations'])):
        errors.add('wave_indices', [wave.event['a'] for wave in waves])
    for index, wave in enumerate(waves):
        if wave.event['b'] != data['tasks']:
            errors.add('wave_task_count', {'wave': wave.event['a'], 'tasks': wave.event['b']})
        if index and waves[index - 1].end is not None and wave.event['ns'] < waves[index - 1].end:
            errors.add('overlapping_waves', wave.event['a'])
    starts = [wave.event['ns'] for wave in waves]
    wave_spans, wave_events = [[] for _ in waves], [[] for _ in waves]

    def wave_index(ns, end=None):
        index = bisect_right(starts, ns) - 1
        if index < 0:
            return None
        wave = waves[index]
        if wave.end is None or ns > wave.end or (end is not None and end > wave.end):
            return None
        return index

    for span in spans:
        if span.event['kind'] == 'wave':
            continue
        index = wave_index(span.event['ns'])
        if index is None:
            if span.event['kind'] in ('callback', 'chunk'):
                errors.add('work_outside_wave', {'id': span.event['id'], 'kind': span.event['kind']})
            else:
                unverified.add('scope_outside_wave', {'id': span.event['id'], 'kind': span.event['kind']})
        else:
            wave_spans[index].append(span)
            if (span.event['kind'] in ('callback', 'chunk') and span.end is not None and
                    span.end > waves[index].end):
                errors.add('work_after_wave', {'id': span.event['id'], 'kind': span.event['kind']})
    for event in instants:
        index = wave_index(event['ns'])
        if index is not None:
            wave_events[index].append(event)
        elif event['kind'] in PHASE_ALIASES or event['kind'] in ('split', 'activate', 'activate_mask'):
            errors.add('event_outside_wave', {'id': event['id'], 'kind': event['kind']})

    # Every callback must name the actual recorded chunk/range on its native lane.
    for span in spans:
        event = span.event
        if event['kind'] != 'callback':
            continue
        parent = by_id.get(event['parent'])
        if parent is None:
            unverified.add('callback_parent', {'id': event['id'], 'parent': event['parent']})
        elif (parent.event['kind'] not in ('chunk', 'range') or
              parent.event['tid'] != event['tid'] or
              not parent.event['a'] <= event['a'] < event['b'] <= parent.event['b'] or
              parent.event['ns'] > event['ns'] or
              (parent.end is not None and span.end is not None and span.end > parent.end)):
            errors.add('callback_containment', {'id': event['id'], 'parent': event['parent']})
        if event['b'] != event['a'] + 1:
            errors.add('callback_width', {'id': event['id'], 'range': [event['a'], event['b']]})

    discarded_ids = {event['id'] for event in instants if event['kind'] == 'discard'}
    for ident, split in splits.items():
        donor = by_id.get(split['parent'])
        if donor is None or donor.event['kind'] != 'range':
            unverified.add('split_donor', {'id': ident, 'parent': split['parent']})
        elif (not donor.event['a'] <= split['a'] < split['b'] <= donor.event['b'] or
              split['ns'] < donor.event['ns'] or
              (donor.end is not None and split['ns'] > donor.end)):
            errors.add('split_bounds', {'id': ident, 'donor': split['parent']})
        spec = specs.get(ident)
        if spec is None:
            unverified.add('split_specification', ident)
        elif (split['a'], split['b']) != (spec['a'], spec['b']):
            errors.add('split_specification', ident)
        if ident not in by_id and ident not in discarded_ids:
            unverified.add('split_delivery', ident)
    for event in instants:
        if event['kind'] != 'steal_range':
            continue
        split = splits.get(event['id'])
        donor = by_id.get(split['parent']) if split else None
        if split is None or donor is None:
            unverified.add('range_theft_donor', event['id'])
        elif donor.event['worker'] != event['a'] or event['worker'] != event['b']:
            errors.add('range_theft_donor', {'id': event['id'], 'victim': event['a'],
                                            'donor_worker': donor.event['worker'],
                                            'recipient': event['b'], 'worker': event['worker']})

    execution_cache = {}

    def execution(span):
        ident = span.event['id']
        if ident in execution_cache:
            return execution_cache[ident]
        chain, seen = [], set()
        current = span
        while current.event['kind'] not in ('task', 'resident', 'mask_task', 'wave'):
            if current.event['id'] in seen:
                errors.add('scope_parent_cycle', ident)
                break
            seen.add(current.event['id'])
            chain.append(current.event['id'])
            parent = by_id.get(current.event['parent'])
            if parent is None:
                break
            current = parent
        for child in chain:
            execution_cache[child] = current
        execution_cache[ident] = current
        return current

    results = []
    for index, wave in enumerate(waves):
        groups = defaultdict(list)
        for span in wave_spans[index]:
            groups[span.event['kind']].append(span)
        calls, chunks, ranges = groups['callback'], groups['chunk'], groups['range']
        events = wave_events[index]
        counts = Counter(event['kind'] for event in events)
        callbacks = interval_oracle(calls, data['tasks']) if calls else {'status': 'unverified', 'reason': 'callbacks were not recorded'}
        if not calls:
            unverified.add('callback_coverage', {'wave': wave.event['a'], 'reason': 'callbacks absent'})
        elif callbacks['status'] == 'fail':
            errors.add('callback_coverage', wave.event['a'])
        fixed = data.get('mode') in FIXED_MODES
        blocks = chunks or (ranges if fixed else [])
        coverage = interval_oracle(blocks, data['tasks']) if blocks else {'status': 'unverified', 'reason': 'no executed chunks or known fixed ranges'}
        if not blocks:
            unverified.add('executed_range_coverage', wave.event['a'])
        elif coverage['status'] == 'fail':
            errors.add('executed_range_coverage', wave.event['a'])
        initial = []
        for span in ranges:
            envelope = execution(span)
            if envelope.event['id'] not in splits:
                initial.append({'id': span.event['id'], 'tid': span.event['tid'],
                                'worker': span.event['worker'], 'range': [span.event['a'], span.event['b']],
                                'start_from_wave_ns': span.event['ns'] - wave.event['ns']})
            else:
                split = splits[envelope.event['id']]
                if (span.event['a'], span.event['b']) != (split['a'], split['b']):
                    errors.add('split_execution_range', span.event['id'])
        busy_source = calls or blocks
        lanes = defaultdict(list)
        for span in busy_source:
            lanes[span.event['tid']].append(span)
        busy = []
        for tid, lane_spans in sorted(lanes.items()):
            busy.append({'tid': tid, 'workers': sorted({span.event['worker'] for span in lane_spans}),
                         'useful_busy_ns': union_time(lane_spans),
                         'callbacks': sum(span.event['kind'] == 'callback' for span in lane_spans)})
        deliveries = groups['resident'] + groups['mask_task']
        delivery_times = [span.event['ns'] for span in deliveries]
        last_callback = max((span.end for span in calls if span.end is not None), default=None)
        phase_events = [event for event in events if event['kind'] in PHASE_ALIASES]
        phase_events.sort(key=lambda event: event['ns'])
        split_callbacks = Counter(execution(span).event['id'] for span in calls)
        wave_splits = [event for event in events if event['kind'] == 'split']
        split_details = [{'id': event['id'], 'donor_range_id': event['parent'],
                          'range': [event['a'], event['b']], 'offset_ns': event['ns'] - wave.event['ns'],
                          'direct_callbacks': split_callbacks[event['id']]} for event in wave_splits[:64]]
        publication_times = [event['ns'] for event in events if event['kind'] in ('activate', 'activate_mask')]
        owner_times = [item['start_from_wave_ns'] for item in initial]
        results.append({'wave': wave.event['a'], 'duration_ns': None if wave.end is None else wave.end - wave.event['ns'],
                        'callback_coverage': callbacks, 'executed_range_coverage': coverage,
                        'coverage_source': 'chunk' if chunks else 'fixed_range' if fixed and ranges else None,
                        'initial_ranges': sorted(initial, key=lambda item: item['start_from_wave_ns']),
                        'event_counts': dict(counts), 'thefts': counts['steal_range'] + counts['steal_queue'],
                        'defers': counts['defer'], 'activations': counts['activate'] + counts['activate_mask'],
                        'activation_spread_ns': max(delivery_times) - min(delivery_times) if delivery_times else None,
                        'activation_spread_source': 'resident_or_mask_task_begin',
                        'activation_publication_spread_ns': max(publication_times) - min(publication_times) if publication_times else None,
                        'initial_owner_start_spread_ns': max(owner_times) - min(owner_times) if owner_times else None,
                        'useful_splits': sum(split_callbacks[event['id']] > 0 for event in wave_splits),
                        'useful_splits_definition': 'split child task directly recorded at least one callback',
                        'split_details_first_64': split_details,
                        'last_callback_to_wave_end_ns': wave.end - last_callback if wave.end is not None and last_callback is not None else None,
                        'envelopes_ending_after_wave': sum(span.end is not None and wave.end is not None and span.end > wave.end
                                                           for span in wave_spans[index] if span.event['kind'] in ('task', 'resident', 'range', 'mask_task')),
                        'useful_busy_source': 'callback' if calls else 'chunk_or_fixed_range' if blocks else None,
                        'useful_busy_by_native_thread': busy,
                        'phases': [{'kind': event['kind'], 'stage': PHASE_ALIASES[event['kind']], 'offset_ns': event['ns'] - wave.event['ns'],
                                    'tid': event['tid'], 'worker': event['worker'], 'id': event['id'],
                                    'parent': event['parent'], 'a': event['a'], 'b': event['b']} for event in phase_events],
                        'phase_gaps': [{'from': left['kind'], 'to': right['kind'], 'duration_ns': right['ns'] - left['ns']}
                                       for left, right in zip(phase_events, phase_events[1:])]})
    totals = Counter()
    for lane in data.get('thread_counters', []):
        totals.update({key: value for key, value in lane.items() if key != 'tid'})
    return {'source': str(source), 'status': 'fail' if errors.counts else 'unverified' if unverified.counts else 'pass',
            'mode': data.get('mode'), 'workload': data.get('workload'), 'tasks': data['tasks'],
            'workers': data.get('workers'), 'iterations': data['iterations'], 'events': len(data['events']),
            'instrumented': data.get('instrumented'), 'comparable_speedup': False,
            'dropped_events': data.get('dropped_events'), 'errors': errors.result(),
            'scope_drain': {key: data[key] for key in (*DRAIN_COUNTS, 'scope_drain_timed_out') if key in data},
            'counter_scope': 'entire_recording', 'aggregate_counters': dict(totals),
            'thread_counters': data.get('thread_counters', []),
            'unverified': unverified.result(), 'scope_count': len(spans),
            'event_counts': dict(event_counts), 'waves': results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths', nargs='+', type=Path, help='raw recorder JSON files')
    args = parser.parse_args(argv)
    reports = []
    for path in args.paths:
        try:
            with path.open() as stream:
                data = json.load(stream)
            reports.append(audit(data, path))
        except (OSError, ValueError, TypeError, KeyError) as error:
            reports.append({'source': str(path), 'status': 'unverified', 'error': str(error)})
    json.dump({'schema': 'oox-scheduler-audit-v1', 'reports': reports}, sys.stdout, indent=2)
    sys.stdout.write('\n')
    return 1 if any(report['status'] == 'fail' for report in reports) else 2 if any(report['status'] == 'unverified' for report in reports) else 0


if __name__ == '__main__':
    raise SystemExit(main())
