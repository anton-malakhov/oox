#!/usr/bin/env python3
"""Fit an explainable scheduler model to one scheduler_eval result directory."""

import argparse
import csv
import html
import json
import math
from pathlib import Path
import re
import statistics


TIME_TO_US = {"ns": 1e-3, "us": 1.0, "ms": 1e3, "s": 1e6}
SPMV_RE = re.compile(
    r"^SpmvBenchmark<SparseKind::(Balanced|Hyperbolic|Triangle)>/(\d+)/real_time$")
SCAN_RE = re.compile(r"^Scan/(\d+)/real_time$")
LAUNCH_RE = re.compile(r"^Launch/(\d+)/real_time$")
COLORS = {
    "RAPID_START": "#3973ac",
    "RAPID_MAILBOX": "#1b9e77",
    "RAPID_LAZY_STEALING": "#7570b3",
    "RAPID_TIMESPAN_LAZY_STEALING": "#e7298a",
    "EIGEN_STEALING": "#d95f02",
    "EIGEN_SHARING": "#2a9d50",
    "EIGEN_STEALING_GRAINSIZE": "#e6ab02",
    "EIGEN_SHARING_STEALING": "#8e5bb7",
}
POLICY_MODELS = {
    "EIGEN_STEALING": {
        "name": "fixed-grain work stealing",
        "events": "N/g-1 binary splits, exposed through the local deque",
    },
    "EIGEN_SHARING": {
        "name": "work sharing plus fixed-grain stealing",
        "events": "min(P,N)-1 targeted tree publications, then fixed-grain splits",
    },
    "EIGEN_STEALING_GRAINSIZE": {
        "name": "timespan-grain work stealing",
        "events": "a serial g_hat prefix, then splits of the residual range",
    },
    "EIGEN_SHARING_STEALING": {
        "name": "work sharing plus timespan-grain stealing",
        "events": "targeted tree publications, then locally measured effective grains",
    },
    "RAPID_START": {
        "name": "static Rapid Start",
        "events": "min(P,N)-1 rapid activations and one contiguous range per slot",
    },
    "RAPID_MAILBOX": {
        "name": "Rapid Start to stealable mailboxes",
        "events": "rapid activations followed by B(N,P) ordinary range tasks",
    },
    "RAPID_LAZY_STEALING": {
        "name": "lazy Rapid Start stealing",
        "events": "rapid activations, P first reservations, then B(N,P)-P claims",
    },
    "RAPID_TIMESPAN_LAZY_STEALING": {
        "name": "timespan-adaptive lazy Rapid stealing",
        "events": "rapid activation, timed owner blocks, then lazy peer claims",
    },
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path,
                        help="a complete results/scheduler_eval directory")
    return parser.parse_args()


def write_csv(path, fieldnames, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def benchmark_medians(result, modes):
    samples = {}
    for path in sorted((result / "raw").glob("bench_scheduler_eval_*.json")):
        mode = path.stem.removeprefix("bench_scheduler_eval_")
        if mode not in modes:
            continue
        for row in json.loads(path.read_text()).get("benchmarks", []):
            if row.get("run_type", "iteration") != "iteration":
                continue
            value = float(row["real_time"]) * TIME_TO_US[row.get("time_unit", "ns")]
            samples.setdefault((row["name"], mode), []).append(value)
    return {key: statistics.median(values) for key, values in samples.items()}


def nonnegative_line_fit(xs, ys):
    """Least squares y=a+b*x with a,b >= 0."""
    candidates = []
    count = len(xs)
    sx, sy = sum(xs), sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    denominator = count * sxx - sx * sx
    if denominator:
        b = (count * sxy - sx * sy) / denominator
        a = (sy - b * sx) / count
        if a >= 0 and b >= 0:
            candidates.append((a, b))
    candidates.append((max(0.0, sy / count), 0.0))
    candidates.append((0.0, max(0.0, sxy / sxx) if sxx else 0.0))
    return min(candidates,
               key=lambda ab: sum((y - ab[0] - ab[1] * x) ** 2
                                   for x, y in zip(xs, ys)))


def holdout_values(values):
    """Deterministic interpolation plus largest-size holdout."""
    ordered = sorted(set(values))
    held_out = {value for index, value in enumerate(ordered) if index % 3 == 2}
    held_out.add(ordered[-1])
    return held_out


def fit_launch(points):
    """Fit H(N)=a+b*(N/scale)^gamma by a bounded grid search."""
    points = sorted(points)
    held_out = holdout_values(x for x, _ in points)
    training = [(x, y) for x, y in points if x not in held_out]
    scale = float(max(x for x, _ in training))
    best = None
    for step in range(20, 161):
        gamma = step / 100.0
        xs = [(x / scale) ** gamma for x, _ in training]
        ys = [y for _, y in training]
        a, b = nonnegative_line_fit(xs, ys)
        error = sum(((y - a - b * x) / y) ** 2
                    for x, y in zip(xs, ys))
        candidate = (error, a, b, gamma)
        if best is None or candidate < best:
            best = candidate
    _, a, b, gamma = best
    predictions = {x: a + b * (x / scale) ** gamma for x, _ in points}
    return {
        "intercept_us": a,
        "scale_us": b,
        "task_scale": scale,
        "exponent": gamma,
        "training_mape_percent": mean_absolute_percentage(
            [y for x, y in points if x not in held_out],
            [predictions[x] for x, _ in points if x not in held_out]),
        "holdout_mape_percent": mean_absolute_percentage(
            [y for x, y in points if x in held_out],
            [predictions[x] for x, _ in points if x in held_out]),
        "mape_percent": mean_absolute_percentage(
            [y for _, y in points], [predictions[x] for x, _ in points]),
    }


def launch_time(fit, tasks):
    return (fit["intercept_us"] + fit["scale_us"] *
            (max(1, tasks) / fit["task_scale"]) ** fit["exponent"])


def mean_absolute_percentage(observed, predicted):
    values = [abs(actual - estimate) / actual
              for actual, estimate in zip(observed, predicted) if actual]
    return 100.0 * sum(values) / len(values) if values else math.nan


def through_origin(xs, ys):
    denominator = sum(x * x for x in xs)
    return max(0.0, sum(x * y for x, y in zip(xs, ys)) / denominator)


def solve_linear(matrix, vector):
    size = len(vector)
    rows = [list(matrix[index]) + [vector[index]] for index in range(size)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(rows[row][column]))
        if abs(rows[pivot][column]) < 1e-12:
            return None
        rows[column], rows[pivot] = rows[pivot], rows[column]
        divisor = rows[column][column]
        rows[column] = [value / divisor for value in rows[column]]
        for row in range(size):
            if row == column:
                continue
            factor = rows[row][column]
            rows[row] = [left - factor * right
                         for left, right in zip(rows[row], rows[column])]
    return [rows[index][-1] for index in range(size)]


def nonnegative_least_squares(columns, targets):
    """Small exhaustive active-set NNLS."""
    width = len(columns[0])
    candidates = [tuple(0.0 for _ in range(width))]
    for mask in range(1, 1 << width):
        active = [index for index in range(width) if mask & (1 << index)]
        gram = [[sum(row[left] * row[right] for row in columns)
                 for right in active] for left in active]
        rhs = [sum(row[index] * target
                   for row, target in zip(columns, targets)) for index in active]
        solution = solve_linear(gram, rhs)
        if solution is None or any(value < 0 for value in solution):
            continue
        candidate = [0.0] * width
        for index, value in zip(active, solution):
            candidate[index] = value
        candidates.append(tuple(candidate))
    return min(candidates,
               key=lambda values: sum((target - sum(value * x for value, x
                                                     in zip(values, row))) ** 2
                                      for row, target in zip(columns, targets)))


def hybrid_block_size(work, workers, divisor=1):
    work_per_worker = (work + workers - 1) // workers
    if work_per_worker <= 8:
        blocks_per_worker = 2
    elif work_per_worker <= 64:
        blocks_per_worker = 8
    elif work_per_worker <= 4096:
        blocks_per_worker = 32
    else:
        blocks_per_worker = 64
    blocks = workers * max(blocks_per_worker // divisor, 1)
    return max((work + blocks - 1) // blocks, 1)


def partitioned_block_count(work, slots, block):
    quotient, remainder = divmod(work, slots)
    return sum((quotient + (slot < remainder) + block - 1) // block
               for slot in range(slots))


def policy_events(mode, tasks, threads, effective_grain=1):
    slots = min(tasks, threads)
    depth = math.ceil(math.log2(slots)) if slots > 1 else 0
    callbacks = (tasks + slots - 1) // slots
    fixed_indicator = 1
    rapid, targeted, ordinary, reservations, claims, block = 0, 0, 0, 0, 0, 0
    if mode.startswith("RAPID"):
        rapid = max(0, slots - 1)
    if mode == "RAPID_START":
        critical = depth
    elif mode in ("RAPID_MAILBOX", "RAPID_LAZY_STEALING",
                  "RAPID_TIMESPAN_LAZY_STEALING"):
        density_workers = threads if mode == "RAPID_MAILBOX" else slots
        work_per_worker = (tasks + density_workers - 1) // density_workers
        divisor = (2 if mode == "RAPID_MAILBOX" and
                   work_per_worker <= 512 else 1)
        block = hybrid_block_size(tasks, density_workers, divisor)
        blocks = partitioned_block_count(tasks, slots, block)
        if mode == "RAPID_MAILBOX":
            ordinary = blocks
        else:
            reservations = slots
            claims = max(0, blocks - slots)
        critical = depth + (blocks + slots - 1) // slots
    else:
        if mode == "EIGEN_STEALING_GRAINSIZE":
            prefix = min(tasks, effective_grain)
            remainder = tasks - prefix
            callbacks = prefix + (remainder + slots - 1) // slots
            fixed_indicator = int(remainder != 0)
            leaves = (remainder + effective_grain - 1) // effective_grain
            ordinary = max(0, leaves - 1)
        elif mode == "EIGEN_SHARING_STEALING":
            quotient, remainder = divmod(tasks, slots)
            targeted = max(0, slots - 1)
            for slot in range(slots):
                length = quotient + (slot < remainder)
                rest = max(0, length - effective_grain)
                ordinary += max(0, (rest + effective_grain - 1) //
                                effective_grain - 1)
        else:
            leaves = (tasks + effective_grain - 1) // effective_grain
            generated = max(0, leaves - 1)
            if "SHARING" in mode:
                targeted = min(generated, max(0, slots - 1))
            ordinary = generated - targeted
        critical = depth + (ordinary + slots - 1) // slots
    return {
        "slots": slots, "fixed_indicator": fixed_indicator,
        "callbacks_on_critical_worker": callbacks,
        "critical_scheduler_events": critical, "rapid_activations": rapid,
        "targeted_publications": targeted, "ordinary_tasks": ordinary,
        "first_reservations": reservations, "later_block_claims": claims,
        "block_size": block,
    }


def fit_structural_launch(points, mode, threads, shared_effective_grain=None):
    held_out = holdout_values(x for x, _ in points)
    training = [(x, y) for x, y in points if x not in held_out]
    maximum = max(x for x, _ in points)
    grains = [1]
    if mode in ("EIGEN_STEALING_GRAINSIZE", "EIGEN_SHARING_STEALING"):
        grains = ([shared_effective_grain] if shared_effective_grain else
                  [1 << power for power in range(int(math.log2(maximum)) + 1)])
    best = None
    for grain in grains:
        features = []
        for tasks, observed in training:
            events = policy_events(mode, tasks, threads, grain)
            features.append((events["fixed_indicator"] / observed,
                             events["callbacks_on_critical_worker"] / observed,
                             events["critical_scheduler_events"] / observed))
        coefficients = nonnegative_least_squares(features,
                                                  [1.0] * len(training))
        error = sum((1.0 - sum(value * feature for value, feature in
                               zip(coefficients, row))) ** 2
                    for row in features)
        candidate = (error, grain, coefficients)
        if best is None or candidate < best:
            best = candidate
    _, grain, coefficients = best
    rows = []
    for tasks, observed in sorted(points):
        events = policy_events(mode, tasks, threads, grain)
        predicted = (coefficients[0] * events["fixed_indicator"] +
                     coefficients[1] * events["callbacks_on_critical_worker"] +
                     coefficients[2] * events["critical_scheduler_events"])
        rows.append({
            "family": "launch", "case": f"launch/{tasks}",
            "parameter": tasks, "mode": mode, "observed_us": observed,
            "predicted_us": predicted,
            "error_percent": 100.0 * (predicted - observed) / observed,
            "split": "holdout" if tasks in held_out else "training", **events,
        })
    parameter = {
        "effective_grain": grain, "fixed_us": coefficients[0],
        "callback_us": coefficients[1],
        "critical_scheduler_event_us": coefficients[2],
    }
    for split in ("training", "holdout"):
        selected = [row for row in rows if row["split"] == split]
        parameter[f"{split}_mape_percent"] = mean_absolute_percentage(
            [row["observed_us"] for row in selected],
            [row["predicted_us"] for row in selected])
    return parameter, rows


def sparse_weights(rows, columns, kind):
    harmonic = sum(1.0 / index for index in range(1, rows + 1))
    average = max(1, (columns + 8) // 9)
    weights = []
    for row in range(rows):
        count = average
        if kind == "Hyperbolic":
            count = max(1, int(average * rows / harmonic / (row + 1)))
        elif kind == "Triangle":
            count = max(1, 2 * average * (rows - row) // (rows + 1))
        width = (max(1, columns * (row + 1) // rows)
                 if kind == "Triangle" else columns)
        weights.append(min(count, width))
    return weights


def static_maximum(weights, parts):
    step, remainder = divmod(len(weights), parts)
    cursor = 0
    loads = []
    for part in range(parts):
        length = step + (part < remainder)
        loads.append(sum(weights[cursor:cursor + length]))
        cursor += length
    return max(loads)


def spmv_cases(medians, threads):
    rows = (threads << 9) + (threads << 4) + 7
    cases = []
    structures = {}
    for (name, mode), observed in medians.items():
        match = SPMV_RE.match(name)
        if not match:
            continue
        kind, raw_columns = match.group(1), int(match.group(2))
        columns = raw_columns + (threads << 2) + 3
        key = (kind, columns)
        if key not in structures:
            weights = sparse_weights(rows, columns, kind)
            total = sum(weights)
            maximum = static_maximum(weights, threads)
            structures[key] = (total, maximum)
        total, maximum = structures[key]
        cases.append({
            "family": kind.lower(), "parameter": columns, "mode": mode,
            "observed_us": observed, "tasks": rows, "total_work": total,
            "ideal_work": total / threads, "static_max_work": maximum,
            "static_excess": maximum - total / threads,
        })
    for family in {case["family"] for case in cases}:
        held_out = holdout_values(case["parameter"] for case in cases
                                  if case["family"] == family)
        for case in cases:
            if case["family"] == family:
                case["split"] = ("holdout" if case["parameter"] in held_out
                                 else "training")
    return cases


def fit_spmv(cases, launch_fits, modes):
    rapid = "RAPID_START"
    work_unit = {}
    for family in sorted({case["family"] for case in cases}):
        selected = [case for case in cases
                    if case["family"] == family and case["mode"] == rapid and
                    case["split"] == "training"]
        xs = [case["static_max_work"] for case in selected]
        ys = [max(0.0, case["observed_us"] -
                  launch_time(launch_fits[rapid], case["tasks"]))
              for case in selected]
        work_unit[family] = through_origin(xs, ys)

    coefficients = {rapid: {"body_interaction_us_per_iteration": 0.0,
                            "work_inflation": 1.0,
                            "residual_static_imbalance": 1.0}}
    for mode in sorted(modes - {rapid}):
        selected = [case for case in cases if case["mode"] == mode and
                    case["split"] == "training"]
        columns, targets = [], []
        for case in selected:
            rate = work_unit[case["family"]]
            columns.append((case["tasks"], rate * case["ideal_work"],
                            rate * case["static_excess"]))
            targets.append(max(0.0, case["observed_us"] -
                               launch_time(launch_fits[mode], case["tasks"])))
        body_interaction, work_inflation, residual = nonnegative_least_squares(
            columns, targets)
        coefficients[mode] = {
            "body_interaction_us_per_iteration": body_interaction,
            "work_inflation": work_inflation,
            "residual_static_imbalance": residual,
        }

    predictions = []
    for case in cases:
        rate = work_unit[case["family"]]
        coefficient = coefficients[case["mode"]]
        scheduler = launch_time(launch_fits[case["mode"]], case["tasks"])
        body_interaction = (coefficient["body_interaction_us_per_iteration"] *
                            case["tasks"])
        ideal = rate * coefficient["work_inflation"] * case["ideal_work"]
        imbalance = (rate * coefficient["residual_static_imbalance"] *
                     case["static_excess"])
        predicted = scheduler + body_interaction + ideal + imbalance
        predictions.append({
            **case, "case": f'{case["family"]}/{case["parameter"]}',
            "predicted_us": predicted, "scheduler_us": scheduler,
            "body_interaction_us": body_interaction,
            "ideal_work_us": ideal, "imbalance_us": imbalance,
            "error_percent": 100.0 * (predicted - case["observed_us"]) /
                             case["observed_us"],
            "static_imbalance_ratio": (case["static_max_work"] /
                                       case["ideal_work"]),
        })
    for mode, coefficient in coefficients.items():
        for split in ("training", "holdout"):
            selected = [row for row in predictions if row["mode"] == mode and
                        row["split"] == split]
            coefficient[f"{split}_mape_percent"] = mean_absolute_percentage(
                [row["observed_us"] for row in selected],
                [row["predicted_us"] for row in selected])
        selected = [row for row in predictions if row["mode"] == mode]
        coefficient["mape_percent"] = mean_absolute_percentage(
            [row["observed_us"] for row in selected],
            [row["predicted_us"] for row in selected])
    return work_unit, coefficients, predictions


def fit_scan(medians, launch_fits, modes, threads):
    cases = []
    for (name, mode), observed in medians.items():
        match = SCAN_RE.match(name)
        if not match:
            continue
        size = 1 << int(match.group(1))
        stages = []
        tasks = size // 2
        while tasks:
            stages.append(tasks)
            tasks //= 2
        within_launch_range = max(stages) <= min(
            fit["task_scale"] for fit in launch_fits.values())
        launch_sum = 2.0 * sum(launch_time(launch_fits[mode], n) for n in stages)
        calls = 2 * len(stages)
        waves = 2 * sum((n + threads - 1) // threads for n in stages)
        cases.append({"family": "scan", "case": f"scan/{size}",
                      "parameter": size, "mode": mode,
                      "observed_us": observed, "launch_sum_us": launch_sum,
                      "calls": calls, "waves": waves,
                      "total_work": 2 * size - 2,
                      "within_launch_range": within_launch_range})
    if not cases:
        return {}, []
    held_out = holdout_values(case["parameter"] for case in cases)
    for case in cases:
        case["split"] = ("holdout" if case["parameter"] in held_out
                         else "training")
    coefficients = {}
    for mode in sorted(modes):
        selected = [case for case in cases if case["mode"] == mode and
                    case["split"] == "training"]
        call_us, wave_us = nonnegative_least_squares(
            [(case["calls"] / case["observed_us"],
              case["waves"] / case["observed_us"]) for case in selected],
            [1.0 for _ in selected])
        coefficients[mode] = {"effective_call_us": call_us,
                              "effective_wave_us": wave_us}
    predictions = []
    for case in cases:
        coefficient = coefficients[case["mode"]]
        scheduler = coefficient["effective_call_us"] * case["calls"]
        ideal = coefficient["effective_wave_us"] * case["waves"]
        predicted = scheduler + ideal
        predictions.append({
            **case, "predicted_us": predicted, "scheduler_us": scheduler,
            "ideal_work_us": ideal,
            "body_interaction_us": 0.0, "imbalance_us": 0.0, "error_percent":
                100.0 * (predicted - case["observed_us"]) / case["observed_us"],
            "tasks": "", "ideal_work": case["waves"],
            "static_max_work": "", "static_excess": "",
            "static_imbalance_ratio": "",
        })
    for mode, coefficient in coefficients.items():
        for split in ("training", "holdout"):
            selected = [row for row in predictions if row["mode"] == mode and
                        row["split"] == split]
            coefficient[f"{split}_mape_percent"] = mean_absolute_percentage(
                [row["observed_us"] for row in selected],
                [row["predicted_us"] for row in selected])
        selected = [row for row in predictions if row["mode"] == mode]
        coefficient["mape_percent"] = mean_absolute_percentage(
            [row["observed_us"] for row in selected],
            [row["predicted_us"] for row in selected])
        valid = [row for row in selected if row["within_launch_range"]]
        coefficient["launch_sum_mape_in_measured_range_percent"] = (
            mean_absolute_percentage(
                [row["observed_us"] for row in valid],
                [row["launch_sum_us"] for row in valid]))
    return coefficients, predictions


def startup_parameters(result, modes):
    values = {}
    for path in sorted((result / "raw").glob("scheduling_dist_spin_*.json")):
        data = json.loads(path.read_text())
        mode = data.get("mode")
        if mode not in modes:
            continue
        distinct = [len(set(row["worker"])) for row in data.get("iterations", [])]
        maximum = []
        for row in data.get("iterations", []):
            counts = {}
            for worker in row["worker"]:
                counts[worker] = counts.get(worker, 0) + 1
            maximum.append(max(counts.values(), default=0))
        values[mode] = {
            "initialization_us": float(data["initialization_ns"]) / 1000.0,
            "p99_publication_spread_us":
                float(data["spread_summary_ns"]["p99"]) / 1000.0,
            "median_distinct_workers": statistics.median(distinct) if distinct else 0,
            "median_max_tasks_per_worker": statistics.median(maximum) if maximum else 0,
        }
    return values


def policy_selection(predictions):
    grouped = {}
    for row in predictions:
        grouped.setdefault((row["family"], row["case"], row["split"]), []).append(row)
    selections = []
    for (family, case, split), rows in sorted(grouped.items()):
        observed_best = min(rows, key=lambda row: row["observed_us"])
        predicted_best = min(rows, key=lambda row: row["predicted_us"])
        regret = (100.0 * (predicted_best["observed_us"] -
                           observed_best["observed_us"]) /
                  observed_best["observed_us"])
        selections.append({
            "family": family, "case": case, "split": split,
            "observed_best": observed_best["mode"],
            "predicted_best": predicted_best["mode"],
            "observed_best_us": observed_best["observed_us"],
            "selected_observed_us": predicted_best["observed_us"],
            "regret_percent": regret,
            "correct": observed_best["mode"] == predicted_best["mode"],
        })
    summary = {}
    for split in ("training", "holdout"):
        selected = [row for row in selections if row["split"] == split]
        summary[split] = {
            "cases": len(selected),
            "accuracy_percent": (100.0 * sum(row["correct"] for row in selected) /
                                 len(selected) if selected else math.nan),
            "mean_regret_percent": (sum(row["regret_percent"] for row in selected) /
                                    len(selected) if selected else math.nan),
            "max_regret_percent": (max((row["regret_percent"] for row in selected),
                                       default=math.nan)),
        }
    return selections, summary


def write_spmv_chart(path, predictions):
    families = ["balanced", "hyperbolic", "triangle"]
    modes = sorted({row["mode"] for row in predictions})
    width, height = 1280, 500
    left, right, top, bottom, gap = 95, 25, 50, 115, 38
    panel_width = (width - left - right - gap * 2) / 3
    panel_height = height - top - bottom
    all_x = [float(row["parameter"]) for row in predictions]
    all_y = [float(row[key]) for row in predictions
             for key in ("observed_us", "predicted_us")]
    x0, x1 = math.log10(min(all_x)), math.log10(max(all_x))
    y0, y1 = math.log10(min(all_y)), math.log10(max(all_y))
    ypad = max(0.05, (y1 - y0) * 0.08)
    y0, y1 = y0 - ypad, y1 + ypad
    elements = [f'<rect width="{width}" height="{height}" fill="white"/>',
                '<text x="18" y="25" font-size="18" font-weight="600">'
                'SpMV model: measured (solid) and predicted (dashed)</text>',
                f'<text x="24" y="{top + panel_height / 2:.1f}" '
                f'text-anchor="middle" font-size="13" transform="rotate(-90 24 '
                f'{top + panel_height / 2:.1f})">Time, us (log scale)</text>',
                f'<text x="{width / 2:.1f}" y="{height - 48}" '
                'text-anchor="middle" font-size="13">Matrix width (columns), '
                'log scale</text>']
    for panel, family in enumerate(families):
        rows = [row for row in predictions if row["family"] == family]
        xs = [float(row["parameter"]) for row in rows]
        px = left + panel * (panel_width + gap)
        sx = lambda value: px + (math.log10(value) - x0) * panel_width / (x1 - x0)
        sy = lambda value: top + (y1 - math.log10(value)) * panel_height / (y1 - y0)
        elements.extend([
            f'<text x="{px + panel_width / 2:.1f}" y="43" text-anchor="middle" '
            f'font-size="14">{html.escape(family.title())}</text>',
            f'<line x1="{px:.1f}" y1="{top}" x2="{px:.1f}" '
            f'y2="{top + panel_height}" stroke="#444"/>',
            f'<line x1="{px:.1f}" y1="{top + panel_height}" '
            f'x2="{px + panel_width:.1f}" y2="{top + panel_height}" stroke="#444"/>',
        ])
        for tick in range(5):
            value = 10 ** (y0 + tick * (y1 - y0) / 4)
            y = sy(value)
            elements.append(f'<line x1="{px:.1f}" y1="{y:.1f}" '
                            f'x2="{px + panel_width:.1f}" y2="{y:.1f}" '
                            'stroke="#e3e3e3"/>')
            if panel == 0:
                elements.append(f'<text x="{px - 8:.1f}" y="{y + 4:.1f}" '
                                f'text-anchor="end" font-size="11">{value:.3g}</text>')
        for mode in modes:
            selected = sorted((row for row in rows if row["mode"] == mode),
                              key=lambda row: row["parameter"])
            color = COLORS.get(mode, "#666666")
            for key, dash in (("observed_us", ""), ("predicted_us", "6 4")):
                points = " ".join(f'{sx(row["parameter"]):.1f},'
                                  f'{sy(row[key]):.1f}' for row in selected)
                dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
                elements.append(f'<polyline points="{points}" fill="none" '
                                f'stroke="{color}" stroke-width="2"{dash_attr}/>')
        for value in sorted(set(xs)):
            if value in (min(xs), max(xs)):
                elements.append(f'<text x="{sx(value):.1f}" '
                                f'y="{top + panel_height + 20}" text-anchor="middle" '
                                f'font-size="11">{value:g}</text>')
    legend_x = left
    for index, mode in enumerate(modes):
        x = legend_x + (index % 4) * 290
        y = height - 34 + (index // 4) * 18
        color = COLORS.get(mode, "#666666")
        elements.append(f'<line x1="{x}" y1="{y}" x2="{x + 26}" '
                        f'y2="{y}" stroke="{color}" stroke-width="3"/>')
        elements.append(f'<text x="{x + 33}" y="{y + 5}" '
                        f'font-size="12">{html.escape(mode)}</text>')
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
                    f'height="{height}" viewBox="0 0 {width} {height}">'
                    f'{"".join(elements)}</svg>\n')


def main():
    result = parse_args().result.resolve()
    metadata = json.loads((result / "metadata.json").read_text())
    if not metadata.get("complete") or metadata.get("smoke"):
        raise RuntimeError("The model requires a complete, non-smoke result")
    modes = set(metadata["modes"])
    if "RAPID_START" not in modes:
        raise RuntimeError("RAPID_START is required as the static calibration anchor")
    medians = benchmark_medians(result, modes)
    launch_points = {mode: [] for mode in modes}
    for (name, mode), value in medians.items():
        match = LAUNCH_RE.match(name)
        if match:
            launch_points[mode].append((int(match.group(1)), value))
    missing = [mode for mode, points in launch_points.items() if len(points) < 3]
    if missing:
        raise RuntimeError(f"Need at least three Launch cases for: {', '.join(missing)}")
    launch_fits = {mode: fit_launch(points)
                   for mode, points in launch_points.items()}

    threads = int(metadata["threads"])
    structural_launch, launch_predictions = {}, []
    ordered_modes = sorted(modes, key=lambda mode:
                           mode != "EIGEN_STEALING_GRAINSIZE")
    shared_effective_grain = None
    for mode in ordered_modes:
        parameter, rows = fit_structural_launch(
            launch_points[mode], mode, threads,
            shared_effective_grain if mode == "EIGEN_SHARING_STEALING" else None)
        structural_launch[mode] = parameter
        launch_predictions.extend(rows)
        if mode == "EIGEN_STEALING_GRAINSIZE":
            shared_effective_grain = parameter["effective_grain"]
    work_unit, spmv_parameters, spmv_predictions = fit_spmv(
        spmv_cases(medians, threads), launch_fits, modes)
    scan_parameters, scan_predictions = fit_scan(
        medians, launch_fits, modes, threads)
    startup = startup_parameters(result, modes)
    selections, selection_summary = policy_selection(
        launch_predictions + spmv_predictions + scan_predictions)

    summaries, plots = result / "summaries", result / "plots"
    summaries.mkdir(exist_ok=True)
    plots.mkdir(exist_ok=True)
    parameters = {
        "schema": 2, "result": str(result), "threads": threads,
        "model": ("T = I_m/q + A_m + u_m*C(N,P) + v_m*E_m(N,P) + "
                  "kappa_k*(phi_m*W/P + theta_m*Delta_static)"),
        "amortized_model": "T_amortized(q) = I_m/q + T_warm",
        "policy_models": POLICY_MODELS,
        "structural_launch": structural_launch,
        "launch": launch_fits,
        "spmv_work_unit_us": work_unit,
        "spmv_mode_parameters": spmv_parameters,
        "scan_mode_parameters": scan_parameters,
        "startup": startup,
        "policy_selection": selection_summary,
    }
    (summaries / "model_parameters.json").write_text(
        json.dumps(parameters, indent=2, sort_keys=True) + "\n")

    fields = ["family", "case", "parameter", "mode", "split", "observed_us",
              "predicted_us", "scheduler_us", "body_interaction_us",
              "ideal_work_us", "imbalance_us", "error_percent", "tasks",
              "total_work", "ideal_work", "within_launch_range",
              "static_max_work", "static_excess", "static_imbalance_ratio"]
    write_csv(summaries / "model_predictions.csv", fields,
              [{field: row.get(field, "") for field in fields}
               for row in spmv_predictions + scan_predictions])
    launch_fields = ["family", "case", "parameter", "mode", "split",
                     "observed_us", "predicted_us", "error_percent", "slots",
                     "fixed_indicator", "callbacks_on_critical_worker",
                     "critical_scheduler_events",
                     "rapid_activations", "targeted_publications",
                     "ordinary_tasks", "first_reservations",
                     "later_block_claims", "block_size"]
    write_csv(summaries / "model_launch_predictions.csv", launch_fields,
              [{field: row.get(field, "") for field in launch_fields}
               for row in launch_predictions])
    selection_fields = ["family", "case", "split", "observed_best",
                        "predicted_best", "observed_best_us",
                        "selected_observed_us", "regret_percent", "correct"]
    write_csv(summaries / "model_policy_selection.csv", selection_fields,
              selections)
    amortization = []
    for mode, values in sorted(startup.items()):
        for calls in (1, 10, 100, 1000, 10000):
            amortization.append({
                "mode": mode, "calls": calls,
                "initialization_us": values["initialization_us"],
                "initialization_us_per_call": values["initialization_us"] / calls,
            })
    write_csv(summaries / "model_initialization_amortization.csv",
              ["mode", "calls", "initialization_us",
               "initialization_us_per_call"], amortization)
    write_spmv_chart(plots / "model_spmv_observed_vs_predicted.svg",
                     spmv_predictions)

    with (summaries / "model.md").open("w") as stream:
        stream.write("# Explainable scheduler model\n\n")
        stream.write(f"Fit to `{result.name}` at P={threads}. Times are microseconds.\n\n")
        stream.write("Every third ordered problem size and the largest sampled "
                     "size are holdouts and are not used to tune coefficients.\n\n")
        stream.write("## Unified model\n\n")
        stream.write("`T = I/q + A + u C(N,P) + v E_m(N,P) + useful work + "
                     "residual imbalance`. Here `C=ceil(N/min(P,N))` and `E_m` "
                     "is the policy-specific critical-path event proxy below. "
                     "For root timespan stealing, `C=min(N,g_hat) + "
                     "ceil(max(N-g_hat,0)/P)` and `A` applies only when residual work "
                     "is published.\n\n")
        stream.write("| Mode | Policy | Counted events |\n| --- | --- | --- |\n")
        for mode in sorted(modes):
            policy = POLICY_MODELS.get(mode, {"name": mode, "events": "generic"})
            stream.write(f'| {mode} | {policy["name"]} | {policy["events"]} |\n')
        stream.write("\n## Structural empty-loop calibration\n\n")
        stream.write("`H = A + u C + v E_m`. The empty-body effective grain is "
                     "fitted on timespan stealing and shared with the "
                     "sharing-timespan policy because both use the same gate and "
                     "body. Other event counts follow the implementation.\n\n")
        stream.write("| Mode | Effective grain | A (us) | u/callback (us) | "
                     "v/event (us) | Train MAPE | Holdout MAPE |\n")
        stream.write("| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
        for mode, values in sorted(structural_launch.items()):
            stream.write(f'| {mode} | {values["effective_grain"]} | '
                         f'{values["fixed_us"]:.3f} | {values["callback_us"]:.6f} | '
                         f'{values["critical_scheduler_event_us"]:.6f} | '
                         f'{values["training_mape_percent"]:.1f}% | '
                         f'{values["holdout_mape_percent"]:.1f}% |\n')
        stream.write("\n## SpMV coefficients\n\n")
        stream.write("| Mode | Body interaction/row (us) | Work inflation phi | "
                     "Residual imbalance theta | Train MAPE | Holdout MAPE |\n")
        stream.write("| --- | ---: | ---: | ---: | ---: | ---: |\n")
        for mode, values in sorted(spmv_parameters.items()):
            stream.write(f'| {mode} | '
                         f'{values["body_interaction_us_per_iteration"]:.3f} | '
                         f'{values["work_inflation"]:.3f} | '
                         f'{values["residual_static_imbalance"]:.3f} | '
                         f'{values["training_mape_percent"]:.1f}% | '
                         f'{values["holdout_mape_percent"]:.1f}% |\n')
        stream.write("\nThe body-interaction term captures per-row costs that an "
                     "empty `Launch` cannot see (task/body overlap, migration, and "
                     "cache effects). `phi` is extra effective work relative to Rapid Start's "
                     "placement; `theta=0` removes the static tail and `theta=1` "
                     "retains it. They are fitted diagnostic coefficients, not "
                     "universal scheduler constants.\n\n")
        stream.write("## Launch model\n\n")
        stream.write("`H(N) = a + b (N/Nmax)^gamma`. The exponent describes this "
                     "measurement range; it is not an asymptotic complexity claim.\n\n")
        stream.write("| Mode | a (us) | b (us) | gamma | Train MAPE | "
                     "Holdout MAPE |\n")
        stream.write("| --- | ---: | ---: | ---: | ---: | ---: |\n")
        for mode, values in sorted(launch_fits.items()):
            stream.write(f'| {mode} | {values["intercept_us"]:.2f} | '
                         f'{values["scale_us"]:.2f} | {values["exponent"]:.2f} | '
                         f'{values["training_mape_percent"]:.1f}% | '
                         f'{values["holdout_mape_percent"]:.1f}% |\n')
        stream.write("\n## Cold worker cost\n\n")
        stream.write("Initialization is kept separate: for q calls, add `I/q` to "
                     "each predicted warm call.\n\n")
        stream.write("| Mode | I (us) | P99 publication spread (us) | "
                     "Median workers observed |\n| --- | ---: | ---: | ---: |\n")
        for mode, values in sorted(startup.items()):
            stream.write(f'| {mode} | {values["initialization_us"]:.1f} | '
                         f'{values["p99_publication_spread_us"]:.1f} | '
                         f'{values["median_distinct_workers"]:.0f} |\n')
        if scan_parameters:
            stream.write("\n## Scan model\n\n")
            stream.write("Scan uses `T = alpha * (2 log2 N) + beta * waves`, where "
                         "`waves = 2 sum ceil((N/2^j)/P)`. `alpha` is the effective "
                         "hot parallel-call/barrier cost and `beta` combines one wave of "
                         "scan work with body-dependent scheduling.\n\n")
            stream.write("| Mode | alpha/call (us) | beta/wave (us) | "
                         "Train MAPE | Holdout MAPE |\n")
            stream.write("| --- | ---: | ---: | ---: | ---: |\n")
            for mode, values in sorted(scan_parameters.items()):
                stream.write(f'| {mode} | {values["effective_call_us"]:.3f} | '
                             f'{values["effective_wave_us"]:.6f} | '
                             f'{values["training_mape_percent"]:.1f}% | '
                             f'{values["holdout_mape_percent"]:.1f}% |\n')
        stream.write("\n## Policy selection validation\n\n")
        stream.write("A selector chooses the mode with the smallest prediction. "
                     "Regret is measured using the observed time of that choice.\n\n")
        stream.write("| Split | Cases | Exact winner | Mean regret | Max regret |\n")
        stream.write("| --- | ---: | ---: | ---: | ---: |\n")
        for split, values in selection_summary.items():
            stream.write(f'| {split} | {values["cases"]} | '
                         f'{values["accuracy_percent"]:.1f}% | '
                         f'{values["mean_regret_percent"]:.1f}% | '
                         f'{values["max_regret_percent"]:.1f}% |\n')
        stream.write("\nSee `model_predictions.csv` for every fitted case, "
                     "`model_launch_predictions.csv` for structural event counts, "
                     "`model_policy_selection.csv` for choices and regret, and "
                     "`../plots/model_spmv_observed_vs_predicted.svg` for residuals.\n")

    print(summaries / "model.md")


if __name__ == "__main__":
    main()
