#!/usr/bin/env python3
"""Create CSV, Markdown, and dependency-free SVG scheduler summaries."""

import csv
import html
import json
import math
from pathlib import Path
import statistics
import sys


UNIT_SECONDS = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0}
COLORS = ["#3973ac", "#d95f02", "#2a9d50", "#8e5bb7", "#c33d58",
          "#6b6b6b", "#c49a00", "#008b8b"]


def write_csv(path, fieldnames, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def bar_chart(path, title, values, unit):
    if not values:
        return
    width, row_height = 900, 42
    height = 70 + row_height * len(values)
    maximum = max(value for _, value in values) or 1
    bars = []
    for index, (label, value) in enumerate(values):
        y = 45 + index * row_height
        bar_width = 560 * value / maximum
        bars.append(f'<text x="10" y="{y + 18}" font-size="14">'
                    f'{html.escape(label)}</text>')
        bars.append(f'<rect x="275" y="{y}" width="{bar_width}" height="24" '
                    'fill="#3973ac"/>')
        bars.append(f'<text x="{285 + bar_width}" y="{y + 18}" font-size="14">'
                    f'{value:.3f} {unit}</text>')
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
           f'height="{height}"><text x="10" y="24" font-size="18">'
           f'{html.escape(title)}</text>{"".join(bars)}</svg>\n')
    path.write_text(svg)


def compact_number(value):
    if value >= 1_000_000 and value % 1_000_000 == 0:
        return f"{value / 1_000_000:g}M"
    if value >= 1_000 and value % 1_000 == 0:
        return f"{value / 1_000:g}k"
    return f"{value:g}"


def line_chart(path, title, series, x_label):
    """Write an absolute-time parameter sweep with logarithmic axes."""
    points = [(x, y) for values in series.values() for x, y in values
              if x > 0 and y > 0]
    if not points:
        return
    width, height = 1180, 640
    left, right, top, bottom = 95, 280, 55, 80
    plot_width = width - left - right
    plot_height = height - top - bottom
    x_values = sorted({x for x, _ in points})
    log_x = [math.log10(x) for x in x_values]
    log_y_values = [math.log10(y) for _, y in points]
    x_low, x_high = min(log_x), max(log_x)
    y_low, y_high = min(log_y_values), max(log_y_values)
    if x_low == x_high:
        x_low, x_high = x_low - 0.5, x_high + 0.5
    if y_low == y_high:
        y_low, y_high = y_low - 0.5, y_high + 0.5
    y_padding = max(0.05, (y_high - y_low) * 0.08)
    y_low -= y_padding
    y_high += y_padding

    def sx(value):
        return left + (math.log10(value) - x_low) * plot_width / (x_high - x_low)

    def sy(value):
        return top + (y_high - math.log10(value)) * plot_height / (y_high - y_low)

    elements = [
        f'<rect width="{width}" height="{height}" fill="white"/>',
        f'<text x="{left}" y="28" font-size="20" font-weight="600">'
        f'{html.escape(title)}</text>',
    ]
    for index in range(6):
        exponent = y_low + index * (y_high - y_low) / 5
        value = 10 ** exponent
        y = sy(value)
        elements.append(f'<line x1="{left}" y1="{y:.2f}" '
                        f'x2="{left + plot_width}" y2="{y:.2f}" '
                        'stroke="#dddddd"/>')
        elements.append(f'<text x="{left - 10}" y="{y + 5:.2f}" '
                        f'text-anchor="end" font-size="12">{value:.3g}</text>')
    for value in x_values:
        x = sx(value)
        elements.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" '
                        f'y2="{top + plot_height}" stroke="#eeeeee"/>')
        elements.append(f'<text x="{x:.2f}" y="{top + plot_height + 24}" '
                        f'text-anchor="middle" font-size="12">'
                        f'{compact_number(value)}</text>')
    elements.extend([
        f'<line x1="{left}" y1="{top}" x2="{left}" '
        f'y2="{top + plot_height}" stroke="#333"/>',
        f'<line x1="{left}" y1="{top + plot_height}" '
        f'x2="{left + plot_width}" y2="{top + plot_height}" stroke="#333"/>',
        f'<text x="{left + plot_width / 2}" y="{height - 20}" '
        f'text-anchor="middle" font-size="14">{html.escape(x_label)} '
        '(log scale)</text>',
        f'<text x="22" y="{top + plot_height / 2}" text-anchor="middle" '
        f'font-size="14" transform="rotate(-90 22 {top + plot_height / 2})">'
        'Median real time, us (log scale)</text>',
    ])
    for index, (mode, values) in enumerate(sorted(series.items())):
        color = COLORS[index % len(COLORS)]
        ordered = sorted((x, y) for x, y in values if x > 0 and y > 0)
        coordinates = " ".join(f"{sx(x):.2f},{sy(y):.2f}" for x, y in ordered)
        elements.append(f'<polyline points="{coordinates}" fill="none" '
                        f'stroke="{color}" stroke-width="2"/>')
        for x, y in ordered:
            elements.append(f'<circle cx="{sx(x):.2f}" cy="{sy(y):.2f}" '
                            f'r="4" fill="{color}"/>')
        legend_y = top + 22 + index * 27
        elements.append(f'<line x1="{left + plot_width + 25}" y1="{legend_y}" '
                        f'x2="{left + plot_width + 55}" y2="{legend_y}" '
                        f'stroke="{color}" stroke-width="3"/>')
        elements.append(f'<text x="{left + plot_width + 65}" y="{legend_y + 5}" '
                        f'font-size="13">{html.escape(mode)}</text>')
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
                    f'height="{height}" viewBox="0 0 {width} {height}">'
                    f'{"".join(elements)}</svg>\n')


def parameter_plot_spec(benchmark, threads):
    """Map Google Benchmark names to the effective parameter shown in plots."""
    case = benchmark.removesuffix("/real_time")
    parts = case.rsplit("/", 1)
    if len(parts) != 2 or not parts[1].isdigit():
        return None
    family, raw_parameter = parts[0], int(parts[1])
    if family == "Launch":
        return "launch_tasks", "Parallel-for launch", "Tasks", raw_parameter
    if family == "Reduce":
        effective = raw_parameter + threads + 3
        return "reduce_block_size", "Reduction", "Block size", effective
    if family == "Scan":
        return "scan_elements", "Exclusive scan", "Elements", 1 << raw_parameter
    spmv_kinds = {
        "SpmvBenchmark<SparseKind::Balanced>": ("balanced", "Balanced SpMV"),
        "SpmvBenchmark<SparseKind::Hyperbolic>": ("hyperbolic", "Hyperbolic SpMV"),
        "SpmvBenchmark<SparseKind::Triangle>": ("triangle", "Triangular SpMV"),
    }
    if family in spmv_kinds:
        slug, title = spmv_kinds[family]
        effective = raw_parameter + (threads << 2) + 3
        return f"spmv_{slug}_width", title, "Matrix width (columns)", effective
    return None


def main():
    result = Path(sys.argv[1]).resolve()
    metadata = json.loads((result / "metadata.json").read_text())
    selected_modes = set(metadata["modes"])
    raw = result / "raw"
    summary = result / "summaries"
    plots = result / "plots"
    summary.mkdir(exist_ok=True)
    plots.mkdir(exist_ok=True)

    rows = []
    samples = {}
    for path in sorted(raw.glob("bench_scheduler_eval_*.json")):
        data = json.loads(path.read_text())
        mode = path.stem.removeprefix("bench_scheduler_eval_")
        if mode not in selected_modes:
            continue
        for benchmark in data.get("benchmarks", []):
            if benchmark.get("run_type", "iteration") != "iteration":
                continue
            unit = benchmark.get("time_unit", "ns")
            value = float(benchmark["real_time"])
            rows.append({"benchmark": benchmark["name"], "mode": mode,
                         "real_time": value, "unit": unit})
            samples.setdefault((benchmark["name"], mode), []).append(
                value * UNIT_SECONDS[unit])
    write_csv(summary / "benchmarks.csv",
              ["benchmark", "mode", "real_time", "unit"], rows)

    medians = {key: statistics.median(values) for key, values in samples.items()}
    benchmarks_by_mode = {
        mode: {benchmark for benchmark, sample_mode in medians
               if sample_mode == mode}
        for mode in selected_modes
    }
    common_benchmarks = set.intersection(
        *(benchmarks for benchmarks in benchmarks_by_mode.values()))
    best = {}
    for (benchmark, mode), value in medians.items():
        if benchmark not in common_benchmarks:
            continue
        best[benchmark] = min(best.get(benchmark, math.inf), value)
    scores = {}
    for (benchmark, mode), value in medians.items():
        if benchmark not in common_benchmarks:
            continue
        scores.setdefault(mode, []).append(value / best[benchmark])
    normalized = {
        mode: math.exp(sum(map(math.log, values)) / len(values))
        for mode, values in scores.items() if values
    }
    ordered_scores = sorted(normalized.items(), key=lambda item: item[1])
    bar_chart(plots / "normalized.svg",
              "Normalized median scheduler time (lower is better)",
              ordered_scores, "x")

    parameter_plots = {}
    parameter_rows = []
    threads = int(metadata["threads"])
    for (benchmark, mode), seconds in medians.items():
        spec = parameter_plot_spec(benchmark, threads)
        if spec is None:
            continue
        slug, title, x_label, parameter = spec
        microseconds = seconds * 1e6
        plot = parameter_plots.setdefault(
            slug, {"title": title, "x_label": x_label, "series": {}})
        plot["series"].setdefault(mode, []).append((parameter, microseconds))
        parameter_rows.append({"family": slug, "benchmark": benchmark,
                               "mode": mode, "parameter": parameter,
                               "median_real_time_us": microseconds})
    write_csv(summary / "absolute_time_sweeps.csv",
              ["family", "benchmark", "mode", "parameter",
               "median_real_time_us"], parameter_rows)
    for slug, plot in sorted(parameter_plots.items()):
        line_chart(plots / f"time_vs_{slug}.svg",
                   f'{plot["title"]}: absolute execution time',
                   plot["series"], plot["x_label"])

    latency_rows = []
    for path in sorted(raw.glob("scheduling_dist_*.json")):
        data = json.loads(path.read_text())
        if data["mode"] not in selected_modes:
            continue
        latency_rows.append({
            "file": path.name,
            "mode": data["mode"],
            "scenario": data["scenario"],
            "initialization_ns": data["initialization_ns"],
            "median_spread_ns": data["spread_summary_ns"]["median"],
            "p99_spread_ns": data["spread_summary_ns"]["p99"],
        })
    write_csv(summary / "startup_latency.csv",
              ["file", "mode", "scenario", "initialization_ns",
               "median_spread_ns", "p99_spread_ns"], latency_rows)
    startup = [(row["mode"], float(row["initialization_ns"]) / 1e6)
               for row in latency_rows if row["scenario"] == "spin"]
    publication = [(row["mode"], float(row["p99_spread_ns"]) / 1e3)
                   for row in latency_rows if row["scenario"] == "spin"]
    bar_chart(plots / "worker_initialization.svg",
              "Worker initialization including creation (lower is better)",
              sorted(startup, key=lambda item: item[1]), "ms")
    bar_chart(plots / "publication_spread.svg",
              "P99 task publication spread (lower is better)",
              sorted(publication, key=lambda item: item[1]), "us")

    tuner_rows = []
    for path in sorted(raw.glob("timespan_tuner_*.json")):
        data = json.loads(path.read_text())
        if data["mode"] not in selected_modes:
            continue
        tuner_rows.append({
            "mode": data["mode"],
            "workers": data["workers"],
            "iterations": data["iterations"],
            "recommended_init_time_ns": data["recommended_init_time_ns"],
            "p99_iteration_maximum_ns": data["iteration_maximum_ns"]["p99"],
        })
    write_csv(summary / "timespan_tuner.csv",
              ["mode", "workers", "iterations", "recommended_init_time_ns",
               "p99_iteration_maximum_ns"], tuner_rows)

    with (summary / "README.md").open("w") as stream:
        stream.write("# Scheduler evaluation summary\n\n")
        if metadata.get("smoke"):
            stream.write("**Pipeline smoke run: these values are not research evidence.**\n\n")
        stream.write("Each benchmark/mode value is the median repetition; the overall "
                     f"score is the geometric mean over {len(common_benchmarks)} "
                     "benchmark case(s) available in every selected mode.\n\n")
        stream.write("| Mode | Geometric mean normalized time |\n| --- | ---: |\n")
        for mode, score in ordered_scores:
            stream.write(f"| {mode} | {score:.4f} |\n")
        stream.write("\n| Mode | Worker initialization (ms) |\n| --- | ---: |\n")
        for mode, value in sorted(startup, key=lambda item: item[1]):
            stream.write(f"| {mode} | {value:.3f} |\n")
        stream.write("\n| Mode | P99 task publication spread (us) |\n| --- | ---: |\n")
        for mode, value in sorted(publication, key=lambda item: item[1]):
            stream.write(f"| {mode} | {value:.3f} |\n")
        stream.write("\n## Absolute-time parameter sweeps\n\n")
        stream.write("Each curve is the median real time for one scheduler. Both axes "
                     "are logarithmic; SpMV width and reduction block size are the "
                     "effective runtime values after thread-dependent adjustments.\n\n")
        for slug, plot in sorted(parameter_plots.items()):
            stream.write(f'- `{plot["title"]}`: `plots/time_vs_{slug}.svg`\n')


if __name__ == "__main__":
    main()
