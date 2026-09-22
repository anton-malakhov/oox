# SPDX-License-Identifier: Apache-2.0
"""Independent fixtures for historical command plans and metric ingestion."""
import unittest
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

from baselines import parse_metrics
from paper_graphs import parameters
from input_graphs import recipe, validate_graph
import hardware
import provenance
import model
import rapid_model


class HistoricalToolsTest(unittest.TestCase):
    def test_rapid_model_reuses_validated_helpers(self):
        self.assertIs(rapid_model.through_origin, model.through_origin)
        self.assertIs(rapid_model.launch_time, model.launch_time)
        for points in ([], [(0, 1)], [(1, float("nan"))], [(1, -1)]):
            with self.subTest(points=points), self.assertRaises(ValueError):
                rapid_model.fit_launch(points)
        fit = rapid_model.fit_launch([(1, 4), (2, 4), (4, 4)])
        self.assertAlmostEqual(rapid_model.launch_time(fit, 8), 4)
        self.assertAlmostEqual(fit["holdout_mape_percent"], 0)
        fit = rapid_model.fit_launch([(1, 4)])
        self.assertAlmostEqual(rapid_model.launch_time(fit, 1), 4)
        self.assertIsNone(fit["holdout_mape_percent"])

    def test_model_rejects_degenerate_samples(self):
        for points in ([], [(0, 1)], [(-1, 2)], [(float("nan"), 1)], [(1, float("inf"))]):
            with self.subTest(points=points), self.assertRaises(ValueError):
                model.fit_launch(points)
        for xs, ys in (([], []), ([0, 0], [1, 2]), ([1], []), ([float("inf")], [1])):
            with self.subTest(xs=xs, ys=ys), self.assertRaises(ValueError):
                model.through_origin(xs, ys)
        with self.assertRaises(ValueError):
            model.launch_time(dict(task_scale=0), 1)
        self.assertEqual(model.through_origin([0, 1, 2], [0, 3, 6]), 3)
        fit = model.fit_launch([(1, 4), (2, 4), (4, 4)])
        self.assertAlmostEqual(model.launch_time(fit, 2), 4)

    def test_execution_environment_disables_backend_specific_pinning(self):
        original = dict(OMP_PROC_BIND="close", KMP_AFFINITY="compact",
                        GOMP_CPU_AFFINITY="0", KMP_HW_SUBSET="1c", KEEP="yes")
        result = provenance.execution_environment(original)
        self.assertEqual(result, dict(OMP_PROC_BIND="false",
                                     KMP_AFFINITY="disabled", KEEP="yes"))
        self.assertEqual(original["OMP_PROC_BIND"], "close")

    def test_artifact_identity_changes_with_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "benchmark"
            binary.write_bytes(b"abc")
            before = provenance.artifacts([binary])
            self.assertEqual(before[str(binary.resolve())]["sha256"],
                             "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
            binary.write_bytes(b"abd")
            self.assertNotEqual(before, provenance.artifacts([binary]))
            with self.assertRaises(FileNotFoundError):
                provenance.artifacts([Path(directory) / "missing"])

    def test_checkout_distinguishes_dirty_state_from_revision(self):
        for status in (b"", b" M source.cpp\n", b"?? new.cpp\n"):
            with self.subTest(status=status), patch(
                    "provenance.subprocess.check_output",
                    side_effect=[status, b"diff", b"revision\n"]):
                record = provenance.checkout(Path("repo"))
                self.assertEqual(record["commit"], "revision")
                self.assertEqual(record["dirty"], bool(status))
                self.assertEqual(record["status"], status.decode())

    def test_papi_options_and_scope(self):
        args = SimpleNamespace(perf=False, perf_events="cycles", cpu_node=None,
                               likwid_group=None, likwid_cpus=None,
                               papi_events="PAPI_TOT_CYC,PAPI_TOT_INS")
        hardware.validate_options(args, check_tools=False)
        self.assertEqual(hardware.counter_prefix(args, Path("unused.csv")), [])
        self.assertEqual(hardware.metadata(args)["tool"], "PAPI")
        for events in ("", "A,A", "A,", "A, B"):
            args.papi_events = events
            with self.subTest(events=events), self.assertRaises(ValueError):
                hardware.validate_options(args, check_tools=False)
        args.papi_events = "PAPI_TOT_CYC"
        args.perf = True
        with self.assertRaises(ValueError):
            hardware.validate_options(args, check_tools=False)

    def test_rmat_profiles_and_small_graph_reader(self):
        program, args = recipe("rmat24")
        self.assertEqual(program, "rMatGraph")
        self.assertEqual(args[-1], "16777216")
        self.assertEqual(args[args.index("-m") + 1], "201326592")
        self.assertEqual(recipe("rmat27")[1][-1], "134217728")
        self.assertEqual(recipe("rmat27", smoke=True)[1][-1], "1024")
        self.assertEqual(validate_graph(Path(__file__).parent / "fixtures" / "graph_smoke.adj"), (4, 4))

    def test_likwid_wrapper_and_conflicts(self):
        args = SimpleNamespace(perf=False, perf_events="cycles", cpu_node=None,
                               likwid_group="MEM", likwid_cpus="0,2-3")
        hardware.validate_options(args, check_tools=False)
        self.assertEqual(hardware.counter_prefix(args, Path("counts.csv")),
                         ["likwid-perfctr", "-C", "0,2-3", "-g", "MEM", "-O", "-o", "counts.csv"])
        for cpus in ("3-1", "0,0", "-C 1", "0-65536"):
            args.likwid_cpus = cpus
            with self.subTest(cpus=cpus), self.assertRaises(ValueError):
                hardware.validate_options(args, check_tools=False)
        args.likwid_cpus = "0-3"
        args.perf = True
        with self.assertRaises(ValueError):
            hardware.validate_options(args, check_tools=False)

    def test_pasl_published_harness_parameters(self):
        # Expected values evaluated independently from graph.ml's load table.
        expected = {
            ("square-grid", "small"): ("width", 707),
            ("square-grid", "large"): ("width", 7071),
            ("cube-grid", "small"): ("nb_on_side", 69),
            ("cube-grid", "large"): ("nb_on_side", 321),
            ("par-chains-100", "large"): ("nb_edges_per_path", 500000),
            ("phases-10-d-2", "large"): ("nb_vertices_per_phase", 3333333),
            ("phases-50-d-5", "large"): ("nb_vertices_per_phase", 800000),
            ("trees-524k", "small"): ("nb_phases", 3),
            ("trees-524k", "medium"): ("nb_phases", 38),
            ("trees-524k", "large"): ("nb_phases", 381),
            ("rand-arity-100", "large"): ("num_rows", 1000000),
            ("trunk-first", "large"): ("depth_of_branches", 10000000),
            ("rmat24", "large"): ("tgt_nb_vertices", 13333333),
            ("rmat27", "large"): ("nb_edges", 119999997),
        }
        for (kind, size), (name, value) in expected.items():
            with self.subTest(kind=kind, size=size):
                self.assertEqual(parameters(kind, size)[name], value)
        self.assertEqual(parameters("rand-arity-100", "large")["bits"], 32)
        self.assertEqual(parameters("trees-524k", "large")["bits"], 64)
        self.assertEqual(parameters("rmat27", "small")["a"], 0.57)
        self.assertEqual(parameters("rmat24", "small")["a"], 0.5)

    def test_heartbeat_example_keeps_repeated_measurements(self):
        result = parse_metrics("""diagnostic text
exectime 0.076
nb_promotions 6144
nb_steals 1220
utilization 0.8211
exectime 0.078
""")
        self.assertEqual(result["exectime"], [0.076, 0.078])
        self.assertEqual(result["nb_steals"], [1220])
        self.assertNotIn("nb_stacklet_allocations", result)

    def test_invalid_or_missing_timing_is_rejected(self):
        for output in ("nb_steals 0", "exectime nan", "exectime -1", "exectime inf"):
            with self.subTest(output=output), self.assertRaises(ValueError):
                parse_metrics(output)


if __name__ == "__main__":
    unittest.main()
