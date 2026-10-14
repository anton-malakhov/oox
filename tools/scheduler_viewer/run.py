#!/usr/bin/env python3
"""Serve only the viewer's assets on loopback; Python standard library only."""

import argparse
import json
import math
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
import webbrowser


ROOT = Path(__file__).resolve().parent
ASSETS = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/style.css": ("style.css", "text/css; charset=utf-8"),
    "/model.js": ("model.js", "text/javascript; charset=utf-8"),
    "/native-model.js": ("native-model.js", "text/javascript; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/trace.html": ("trace.html", "text/html; charset=utf-8"),
    "/trace.js": ("trace.js", "text/javascript; charset=utf-8"),
    "/trace-model.js": ("../../benchmarks/scheduler_eval/tracing/model.js", "text/javascript; charset=utf-8"),
    "/compare.html": ("compare.html", "text/html; charset=utf-8"),
    "/compare.js": ("compare.js", "text/javascript; charset=utf-8"),
    "/lab.html": ("lab.html", "text/html; charset=utf-8"),
    "/lab.css": ("lab.css", "text/css; charset=utf-8"),
    "/lab.js": ("lab.js", "text/javascript; charset=utf-8"),
    "/lab-engine.js": ("lab-engine.js", "text/javascript; charset=utf-8"),
    "/flowchart.js": ("flowchart.js", "text/javascript; charset=utf-8"),
    "/lab-evidence.js": ("lab-evidence.js", "text/javascript; charset=utf-8"),
    "/local-measurements.js": ("local-measurements.js", "text/javascript; charset=utf-8"),
}


def load_traces(directory):
    records = []
    if directory is None:
        return records
    for mode in ("EIGEN_STEALING", "EIGEN_SHARING", "TBB_AUTO", "RAPID_START", "RAPID_AUTO",
                 "RAPID_NATIVE_AUTO", "RAPID_NATIVE_SIMPLE", "RAPID_NATIVE_STATIC", "RAPID_NATIVE_AFFINITY"):
        path = directory / (mode + ".json")
        if not path.exists():
            continue
        if path.stat().st_size > 32 * 1024 * 1024:
            raise ValueError("Trace exceeds 32 MiB: " + str(path))
        data = json.loads(path.read_text())
        if data.get("schema") != "oox-scheduler-trace-v1" or data.get("mode") != mode:
            raise ValueError("Unexpected trace schema or mode: " + str(path))
        if data.get("oracle_ok") is not True or data.get("dropped_events") != 0:
            raise ValueError("Trace has failed oracle or dropped events: " + str(path))
        records.append(data)
    if not records:
        raise ValueError("No supported trace JSON files in " + str(directory))
    return records


def load_profile(path):
    if path is None:
        return None
    if path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("Profile exceeds 32 MiB")
    data = json.loads(path.read_text())
    if data.get("schema") not in ("scheduler-lab-measured-report-v1", "scheduler-calibration-profile-v2"):
        raise ValueError("Unsupported calibration profile schema")
    for key in ("hardware", "protocol", "body_ns", "primitives_ns"):
        if not isinstance(data.get(key), dict):
            raise ValueError("Profile missing object: " + key)
    for key in ("measurements", "paired"):
        if not isinstance(data.get(key), list):
            raise ValueError("Profile missing list: " + key)
    for work in ("100", "1000"):
        value = data["body_ns"].get(work)
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError("Invalid calibrated body time")
    for key in ("push_front_hot", "pop_front_hot", "pop_back_uncontended_same_thread", "empty_pop_back", "sc_store_acquire_load"):
        metric = data["primitives_ns"].get(key)
        if not isinstance(metric, dict) or any(type(metric.get(q)) not in (int, float) or not math.isfinite(metric[q]) or metric[q] < 0 for q in ("median", "q25", "q75")):
            raise ValueError("Invalid primitive summary: " + key)
    if data["primitives_ns"]["sc_store_acquire_load"]["median"] <= .001:
        raise ValueError("Atomic probe too small; profile rejected")
    if data["schema"] == "scheduler-calibration-profile-v2":
        primitive = data["primitives_ns"]
        data = dict(data, schema="scheduler-lab-measured-report-v1", source=str(path),
                    proxy_costs={"publish": primitive["push_front_hot"]["median"],
                                 "take": primitive["pop_front_hot"]["median"],
                                 "steal": primitive["pop_back_uncontended_same_thread"]["median"],
                                 "reserve": primitive["sc_store_acquire_load"]["median"]},
                    holdout={}, replication_same_binaries=False,
                    paired=[dict(row, previous_ratio=None) for row in data["paired"]])
    return data


class ViewerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.respond(send_body=True)

    def do_HEAD(self):
        self.respond(send_body=False)

    def respond(self, send_body):
        asset = ASSETS.get(urlsplit(self.path).path)
        if asset is None:
            self.send_error(404)
            return
        filename, mime = asset
        try:
            payload = (ROOT / filename).read_bytes()
            if filename == "local-measurements.js" and getattr(self.server, "measurement_profile", None) is not None:
                encoded = json.dumps(self.server.measurement_profile, ensure_ascii=True).replace("<", "\\u003c")
                payload = ("'use strict';\nglobalThis.SchedulerMeasurements = " + encoded + ";\n").encode()
            if filename in ("trace.html", "compare.html"):
                records = getattr(self.server, "trace_records", [])
                encoded = json.dumps(records, ensure_ascii=True).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
                payload = payload.replace(b'<script id="trace-data" type="application/json">[]</script>',
                                          ('<script id="trace-data" type="application/json">' + encoded + '</script>').encode())
        except OSError:
            self.send_error(500, "Viewer asset unavailable")
            return
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'none'; object-src 'none'; frame-ancestors 'self'; base-uri 'none'")
        self.end_headers()
        if send_body:
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def log_message(self, format_string, *args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0, help="Port (default: select an available port)")
    parser.add_argument("--no-browser", action="store_true", help="Print the URL without opening a browser")
    parser.add_argument("--traces", type=Path, help="Directory containing supported scheduler trace JSON files")
    parser.add_argument("--profile", type=Path, help="Load a specific machine profile.json or analyzed report.json without overwriting saved files")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    try:
        records = load_traces(args.traces)
        profile = load_profile(args.profile)
        server = ThreadingHTTPServer(("127.0.0.1", args.port), ViewerHandler)
        server.trace_records = records
        server.measurement_profile = profile
    except (OSError, ValueError) as error:
        parser.exit(1, "Could not start viewer: " + str(error) + "\n")
    with server:
        url = "http://127.0.0.1:{}/".format(server.server_port)
        if profile:
            url += "lab.html"
        elif records:
            url += "trace.html"
        print("Scheduler Viewer: " + url, flush=True)
        print("Instrumented timings are not benchmark results. Stop with Ctrl+C.", flush=True)
        if not args.no_browser:
            try:
                opened = webbrowser.open(url)
            except webbrowser.Error:
                opened = False
            if not opened:
                print("Open the URL above in your browser.", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nViewer stopped.")


if __name__ == "__main__":
    main()
