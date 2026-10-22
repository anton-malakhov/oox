#!/usr/bin/env python3
"""Capture matched workloads serially and produce a standalone offline viewer."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
MODES = ('RAPID_AUTO', 'RAPID_START', 'RAPID_GROUP', 'EIGEN_AUTO', 'EIGEN_STATIC',
         'EIGEN_AUTO_RESIDENT', 'EIGEN_SIMPLE', 'EIGEN_AFFINITY',
         'EIGEN_STEALING', 'EIGEN_SHARING', 'TBB_AUTO',
         'RAPID_NATIVE_AUTO', 'RAPID_NATIVE_SIMPLE', 'RAPID_NATIVE_STATIC', 'RAPID_NATIVE_AFFINITY')


def validate(data):
    if data.get('schema') != 'oox-scheduler-trace-v1' or not isinstance(data.get('events'), list):
        raise ValueError('unsupported trace format')
    if not data.get('oracle_ok'):
        raise ValueError('callback visitation oracle failed')
    if len(data['events']) > 4194304:
        raise ValueError('too many events')
    for event in data['events']:
        for key in ('ns', 'tid', 'id', 'parent', 'a', 'b'):
            if type(event.get(key)) is not int or not 0 <= event[key] < 2**53:
                raise ValueError(f'invalid {key}')
        if event.get('ph') not in ('B', 'E', 'i') or not isinstance(event.get('kind'), str):
            raise ValueError('invalid event type')
    return data.get('dropped_events', 0)


def render(records, destination):
    text = (HERE / 'viewer.html').read_text()
    payload = json.dumps(records, ensure_ascii=True).replace('&', '\\u0026').replace('<', '\\u003c').replace('>', '\\u003e')
    text = text.replace('<script id="trace-data" type="application/json">[]</script>',
                        '<script id="trace-data" type="application/json">' + payload + '</script>')
    for name in ('model.js', 'viewer.js'):
        text = text.replace(f'<script src="{name}"></script>', '<script>\n' + (HERE / name).read_text() + '\n</script>')
    destination.write_text(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--output', type=Path, help='new directory; never overwritten')
    parser.add_argument('--modes', choices=MODES, nargs='+', default=list(MODES[:5]))
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--tasks', type=int, default=256)
    parser.add_argument('--iterations', type=int, default=3)
    parser.add_argument('--work', type=int, default=200)
    parser.add_argument('--grain', type=int, default=1)
    parser.add_argument('--shape', choices=('balanced', 'hyperbolic', 'tail'), default='balanced')
    parser.add_argument('--events-per-thread', type=int, default=32768)
    parser.add_argument('--callbacks', action='store_true', help='record each callback (substantial extra overhead)')
    parser.add_argument('--timeout', type=float, default=60)
    args = parser.parse_args()
    if not 1 <= args.threads <= 64 or args.timeout <= 0:
        parser.error('threads must be 1..64 and timeout positive')
    modes = list(dict.fromkeys(args.modes))
    binaries = {mode: args.build.resolve() / 'benchmarks/scheduler_eval' / f'trace_scheduler_{mode}' for mode in modes}
    for binary in binaries.values():
        if not binary.is_file():
            parser.error(f'build the trace target first: {binary}')
    out = args.output or Path('results') / ('scheduler-trace-' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
    out = out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('OOX_RAPID_', 'OOX_BENCH_'))}
    env.update(BENCH_NUM_THREADS=str(args.threads), OOX_RAPID_RESIDENT_LIMIT=str(args.threads), OOX_RAPID_AUTOCALIBRATE='0')
    records, runs = [], []
    for mode, binary in binaries.items():
        dest = out / f'{mode}.json'
        command = [str(binary), '--output', str(dest), '--shape', args.shape]
        for key in ('tasks', 'iterations', 'work', 'grain', 'events_per_thread'):
            command += ['--' + key.replace('_', '-'), str(getattr(args, key))]
        if args.callbacks:
            command += ['--callbacks']
        print(f'Recording {mode} ({args.threads} threads)…', flush=True)
        with (out / f'{mode}.log').open('w') as log:
            subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=args.timeout)
        data = json.loads(dest.read_text())
        dropped = validate(data)
        records.append(data)
        runs.append(dict(mode=mode, command=command, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), dropped=dropped))
        if dropped:
            print(f'WARNING: {mode} dropped {dropped} events; increase --events-per-thread', file=sys.stderr)
    manifest = dict(instrumented=True, comparable_speedup=False, resident_limit=args.threads,
                    autocalibration=False, callbacks=args.callbacks, runs=runs)
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    render(records, out / 'comparison.html')
    print(out / 'comparison.html')


if __name__ == '__main__':
    main()
