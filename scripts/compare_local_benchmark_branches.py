#!/usr/bin/env python3
"""Aggregate Apple M4 local benchmark runs across OOX branches and baseline runners."""

import argparse
import csv
import json
from math import exp, log
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

BRANCHES = [
    "add-twist-testing-benchmarks",
    "better_exceptions-benchmarks",
    "fully-pay-as-you-go-benchmarks",
]

OOX_LABEL = {
    "add-twist-testing-benchmarks": "twist",
    "better_exceptions-benchmarks": "better-exc",
    "fully-pay-as-you-go-benchmarks": "pay-as-you-go",
}

BASELINE_RUNNERS = ["tbb-flow", "taskflow", "openmp"]

# Custom result folder names (under --results-root) when not using <branch><suffix>.
BRANCH_RESULT_DIRS = {
    "m1max": {
        "add-twist-testing-benchmarks": "baseline-m1max-3s",
        "better_exceptions-benchmarks": "better_exceptions-m1max-3s",
        "fully-pay-as-you-go-benchmarks": "fully-pay-as-you-go-m1max-3s",
    },
}

JSON_FILES = [
    "big-graph-smoke/exceptions_256.json",
    "big-graph-smoke/cancellation_1024.json",
    "big-graph-smoke/taskbench_failures_widths_up_to_128k.json",
    "big-graph-smoke/branching_cancel_depth22.json",
]


def geomean(values):
    values = [v for v in values if v > 0]
    if not values:
        return float("nan")
    return exp(sum(log(v) for v in values) / len(values))


def fmt_ratio_value(ratio):
    if ratio != ratio:
        return "—"
    delta = (ratio - 1.0) * 100.0
    sign = "+" if delta >= 0 else ""
    return f"{ratio:.3f}x ({sign}{delta:.1f}%)"


def fmt_ratio(value, baseline):
    if value != value or baseline != baseline or not baseline:
        return "—"
    return fmt_ratio_value(value / baseline)


def short_name(name):
    for token in (
        "/min_time:1.000/real_time",
        "/min_time:0.005/real_time",
        "/min_time:3.000/real_time",
        "/real_time",
    ):
        name = name.replace(token, "")
    return name


def to_microseconds(real_time, time_unit):
    unit = (time_unit or "us").lower()
    if unit == "us":
        return real_time
    if unit == "ns":
        return real_time / 1e3
    if unit == "ms":
        return real_time * 1e3
    if unit == "s":
        return real_time * 1e6
    return real_time


def fmt_us(value):
    if value != value:
        return "—"
    if value >= 1000:
        return f"{value:.0f} µs"
    if value >= 100:
        return f"{value:.0f} µs"
    if value >= 10:
        return f"{value:.1f} µs"
    return f"{value:.2f} µs"


def load_json(path):
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    out = {}
    for bench in data.get("benchmarks", []):
        if bench.get("run_type") != "iteration":
            continue
        us = to_microseconds(bench["real_time"], bench.get("time_unit"))
        out[short_name(bench["name"])] = us
    return out


def load_taskbench(path):
    if not path.exists():
        return {}
    groups = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            key = (row["pattern"], row["width"])
            ns = float(row["wall_s"]) / int(row["tasks"]) * 1e9
            groups.setdefault(key, []).append(ns)
    return {key: geomean(vals) for key, vals in groups.items()}


def branch_result_dir(results_root, branch, suffix):
    return results_root / f"{branch}{suffix}"


def parse_branch_dir_args(values):
    mapping = {}
    for item in values:
        if "=" not in item:
            raise argparse.ArgumentTypeError(
                f"--branch-dir expects BRANCH=DIR, got {item!r}"
            )
        branch, dirname = item.split("=", 1)
        if branch not in OOX_LABEL:
            known = ", ".join(OOX_LABEL)
            raise argparse.ArgumentTypeError(f"unknown branch {branch!r} (known: {known})")
        mapping[branch] = dirname
    return mapping


def resolve_branch_dirs(results_root, suffix, branch_dir_overrides, layout):
    if branch_dir_overrides:
        return {
            branch: results_root / dirname
            for branch, dirname in branch_dir_overrides.items()
        }

    if layout == "m1max":
        preset = BRANCH_RESULT_DIRS["m1max"]
        return {branch: results_root / preset[branch] for branch in BRANCHES}

    if suffix == "-m1max-3s" and (results_root / "baseline-m1max-3s").is_dir():
        preset = BRANCH_RESULT_DIRS["m1max"]
        return {branch: results_root / preset[branch] for branch in BRANCHES}

    return {branch: branch_result_dir(results_root, branch, suffix) for branch in BRANCHES}


def find_oox_csv(taskbench_dir, branch):
    label = OOX_LABEL[branch]
    candidates = [
        taskbench_dir / f"oox-{label}.csv",
        taskbench_dir / "oox.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def collect(results_root, branch_dirs):
    benches = {}
    taskbench = {}

    for branch in BRANCHES:
        branch_dir = branch_dirs.get(branch)
        if branch_dir is None or not branch_dir.is_dir():
            continue
        impl = f"oox-{OOX_LABEL[branch]}"
        for rel in JSON_FILES:
            for name, value in load_json(branch_dir / rel).items():
                benches.setdefault(name, {})[impl] = value
        oox_csv = find_oox_csv(branch_dir / "taskbench", branch)
        if oox_csv:
            for key, value in load_taskbench(oox_csv).items():
                taskbench.setdefault(key, {})[impl] = value

    ref_branch_dir = branch_dirs.get(BRANCHES[0])
    if ref_branch_dir is not None and ref_branch_dir.is_dir():
        baseline_ref = ref_branch_dir / "taskbench"
        for runner in BASELINE_RUNNERS:
            path = baseline_ref / f"{runner}.csv"
            if not path.exists():
                continue
            for key, value in load_taskbench(path).items():
                taskbench.setdefault(key, {})[runner] = value

    return benches, taskbench


def implementations(taskbench):
    oox = [f"oox-{OOX_LABEL[b]}" for b in BRANCHES]
    baselines = [r for r in BASELINE_RUNNERS if any(r in v for v in taskbench.values())]
    return oox + baselines


def write_full_markdown(benches, taskbench, out_path, baseline_impl, title):
    impls = implementations(taskbench)
    lines = []
    lines.append(title)
    lines.append("")
    lines.append("OOX variants: twist, better-exc, pay-as-you-go (one per git branch).")
    lines.append("Baselines: tbb-flow, taskflow, openmp (from add-twist worktree unless duplicated per branch).")
    lines.append(f"TaskBench ratios vs `{baseline_impl}`. Lower is better.")
    lines.append("")

    lines.append("## TaskBench Overall (geometric mean ratio vs baseline)")
    lines.append("")
    lines.append("| Implementation | Geo ratio |")
    lines.append("|---|---:|")
    for impl in impls:
        ratios = []
        for key, values in taskbench.items():
            base = values.get(baseline_impl)
            if impl in values and base:
                ratios.append(values[impl] / base)
        lines.append(f"| {impl} | {fmt_ratio_value(geomean(ratios))} |")
    lines.append("")

    lines.append("## TaskBench by pattern × width")
    lines.append("")
    header = "| Pattern | Width | " + " | ".join(impls) + " |"
    lines.append(header)
    lines.append("|---" + "|---:" * (len(impls) + 1) + "|")
    for pattern, width in sorted(taskbench.keys(), key=lambda k: (k[0], int(k[1]))):
        values = taskbench[(pattern, width)]
        base = values.get(baseline_impl)
        cells = [fmt_ratio(values.get(impl, float("nan")), base) if base else "—" for impl in impls]
        lines.append(f"| {pattern} | {width} | " + " | ".join(cells) + " |")
    lines.append("")

    oox_prefixes = ("OOX_", "BM_OOX_", "BM_TaskBench_")
    bg_impls = [i for i in impls if i.startswith("oox-")]
    lines.append("## Big-graph smoke (OOX, µs from Google Benchmark JSON)")
    lines.append("")
    lines.append(
        f"Wall time (`real_time`) per run; ratios vs `{baseline_impl}` when present, else vs first column with data."
    )
    lines.append("Lower is better.")
    lines.append("")
    if bg_impls:
        lines.append("| Benchmark | " + " | ".join(bg_impls) + " |")
        lines.append("|---" + "|---:" * len(bg_impls) + "|")
        for name in sorted(benches.keys()):
            if not any(name.startswith(p) for p in oox_prefixes):
                continue
            values = benches[name]
            base = values.get(baseline_impl)
            if base is None:
                for impl in bg_impls:
                    if impl in values:
                        base = values[impl]
                        break
            cells = []
            for impl in bg_impls:
                v = values.get(impl)
                if v is None:
                    cells.append("—")
                else:
                    cells.append(f"{fmt_us(v)} ({fmt_ratio(v, base)})")
            lines.append(f"| `{name}` | " + " | ".join(cells) + " |")
        lines.append("")

    out_path.write_text("\n".join(lines))


def write_full_csv(taskbench, out_path, baseline_impl):
    impls = implementations(taskbench)
    with out_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["pattern", "width", "implementation", "ns_per_task", "ratio_vs_baseline", "baseline"]
        )
        for (pattern, width), values in sorted(taskbench.items()):
            base = values.get(baseline_impl)
            for impl in impls:
                if impl not in values:
                    continue
                ratio = values[impl] / base if base else ""
                writer.writerow([pattern, width, impl, values[impl], ratio, baseline_impl])


def main():
    parser = argparse.ArgumentParser(description="Compare local Apple M4 benchmark results.")
    parser.add_argument(
        "--results-root",
        default=str(ROOT / "results" / "local-benchmarks"),
        help="Directory containing <branch><suffix> result folders.",
    )
    parser.add_argument(
        "--suffix",
        default="-3s",
        help="Suffix appended to branch directory names (e.g. add-twist-testing-benchmarks-3s).",
    )
    parser.add_argument(
        "--layout",
        choices=["auto", "branch-suffix", "m1max"],
        default="auto",
        help="How to locate result dirs: auto (detect m1max folders), branch-suffix, or m1max preset.",
    )
    parser.add_argument(
        "--branch-dir",
        action="append",
        default=[],
        metavar="BRANCH=DIR",
        help="Explicit result subdir under --results-root (repeatable). Overrides --layout/--suffix.",
    )
    parser.add_argument(
        "--out-stem",
        default="full_comparison",
        help="Output basename under --results-root (e.g. full_comparison_m1max).",
    )
    parser.add_argument(
        "--title",
        default="",
        help="Markdown report title (default depends on layout).",
    )
    parser.add_argument(
        "--baseline",
        default="oox-twist",
        help="TaskBench implementation used as ratio baseline.",
    )
    args = parser.parse_args()

    results_root = Path(args.results_root)
    branch_dir_overrides = parse_branch_dir_args(args.branch_dir)
    layout = args.layout
    if layout == "auto":
        layout = "branch-suffix"
    if layout == "branch-suffix" and not branch_dir_overrides:
        if args.suffix == "-m1max-3s" and (results_root / "baseline-m1max-3s").is_dir():
            layout = "m1max"

    branch_dirs = resolve_branch_dirs(results_root, args.suffix, branch_dir_overrides, layout)
    benches, taskbench = collect(results_root, branch_dirs)
    if not taskbench:
        dirs = ", ".join(str(branch_dirs[b]) for b in BRANCHES)
        raise SystemExit(
            f"No TaskBench CSVs found. Looked in:\n  {dirs.replace(', ', chr(10) + '  ')}"
        )

    title = args.title or (
        "# M1 Max Full Benchmark Comparison"
        if layout == "m1max"
        else "# Apple Silicon Full Benchmark Comparison"
    )
    out_md = results_root / f"{args.out_stem}.md"
    out_csv = results_root / f"{args.out_stem}.csv"
    write_full_markdown(benches, taskbench, out_md, args.baseline, title)
    write_full_csv(taskbench, out_csv, args.baseline)
    print(out_md)
    print(out_csv)


if __name__ == "__main__":
    main()
