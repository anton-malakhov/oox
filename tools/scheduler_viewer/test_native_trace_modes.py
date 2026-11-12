import json
from pathlib import Path
import tempfile
import unittest

from capture import MODES
from run import load_traces


class NativeModesTest(unittest.TestCase):
    def test_native_modes_are_captured_and_loaded(self):
        native = {"RAPID_NATIVE_" + name for name in ("AUTO", "SIMPLE", "STATIC", "AFFINITY")}
        self.assertTrue(native.issubset(MODES))
        with tempfile.TemporaryDirectory() as folder:
            for mode in native:
                (Path(folder) / (mode + ".json")).write_text(json.dumps({
                    "mode": mode, "schema": "oox-scheduler-trace-v1", "oracle_ok": True,
                    "dropped_events": 0, "events": []}))
            self.assertEqual({r["mode"] for r in load_traces(Path(folder))}, native)

    def test_native_trace_with_failed_oracle_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "RAPID_NATIVE_AUTO.json"
            path.write_text(json.dumps({"mode": "RAPID_NATIVE_AUTO", "schema": "oox-scheduler-trace-v1",
                                        "oracle_ok": False, "dropped_events": 0}))
            with self.assertRaisesRegex(ValueError, "failed oracle"):
                load_traces(Path(folder))


if __name__ == "__main__":
    unittest.main()
