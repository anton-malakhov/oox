# SPDX-License-Identifier: Apache-2.0
"""Explicit whole-command counter collection; never invent per-kernel counts."""
import platform
import re
import shutil


def validate_options(args, check_tools=True):
    papi = getattr(args, "papi_events", None)
    if papi is not None and not papi:
        raise ValueError("PAPI event list must not be empty")
    if sum(bool(x) for x in (args.perf, args.likwid_group, papi)) > 1:
        raise ValueError("select one of perf, LIKWID or PAPI for one run")
    if papi:
        events = papi.split(",")
        if len(set(events)) != len(events) or any(not e or any(c.isspace() for c in e) for e in events):
            raise ValueError("PAPI events must be nonempty, distinct and comma-separated")
    if bool(args.likwid_group) != bool(args.likwid_cpus):
        raise ValueError("LIKWID requires both --likwid-group and --likwid-cpus")
    if args.likwid_group:
        if args.cpu_node is not None:
            raise ValueError("LIKWID CPU pinning cannot be combined with --cpu-node")
        if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*", args.likwid_cpus):
            raise ValueError("likwid-cpus must be a numeric CPU list, such as 0,2-5")
        seen = set()
        for part in args.likwid_cpus.split(","):
            limits = part.split("-")
            first, last = int(limits[0]), int(limits[-1])
            if first > last or last > 65535:
                raise ValueError("invalid LIKWID CPU range")
            selected = set(range(first, last + 1))
            if selected & seen:
                raise ValueError("duplicate LIKWID CPUs")
            seen.update(selected)
    tool = "perf" if args.perf else "likwid-perfctr" if args.likwid_group else None
    if check_tools and tool and (platform.system() != "Linux" or not shutil.which(tool)):
        raise RuntimeError(f"{tool} collection requires the tool on Linux")


def counter_prefix(args, output):
    if args.perf:
        return ["perf", "stat", "-x", ";", "-e", args.perf_events,
                "-o", str(output), "--"]
    if args.likwid_group:
        return ["likwid-perfctr", "-C", args.likwid_cpus,
                "-g", args.likwid_group, "-O", "-o", str(output)]
    return []


def metadata(args):
    if getattr(args, "papi_events", None):
        return dict(tool="PAPI", events=args.papi_events.split(","),
                    scope="outermost callback regions on each execution thread while benchmark metrics scope is active; includes paused/setup callbacks")
    if args.likwid_group:
        return dict(tool="likwid-perfctr", group=args.likwid_group,
                    cpus=args.likwid_cpus,
                    scope="selected CPUs during the whole command, including initialization")
    if args.perf:
        return dict(tool="perf", events=args.perf_events.split(","),
                    scope="whole command, including initialization")
    return None
