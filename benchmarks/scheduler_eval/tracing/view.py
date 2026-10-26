#!/usr/bin/env python3
"""Build an offline comparison from existing trace files without rerunning work."""
import argparse
import json
from pathlib import Path
from record import render, validate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('traces', type=Path, nargs='+')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--replace', action='store_true', help='refresh an existing generated HTML report')
    args = parser.parse_args()
    if args.output.resolve() in {path.resolve() for path in args.traces}:
        parser.error('output must differ from input traces')
    if args.output.exists() and not args.replace:
        parser.error('output already exists')
    records = []
    for path in args.traces:
        data = json.loads(path.read_text())
        validate(data)
        records.append(data)
    render(records, args.output)
    print(args.output.resolve())


if __name__ == '__main__':
    main()
