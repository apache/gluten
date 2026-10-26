#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reproduce S3 retry timing and Parquet read failures using local HTTP faults.

Requires an existing S3-enabled native test build in cpp/build. Compiles the
checkout's Gluten filesystem/token/config sources into an isolated executable;
uses the build's Velox and AWS libraries. No real S3 endpoint is contacted.
The 20-retry schedule is calculated by the actual AWS SDK without sleeping.
Network cases use two retries and a one-second socket timeout. The direct-input
cases compare the current Velox guard against pre-fix behavior reproduced by
--without-direct-load-guard. Only a temporary source copy is changed. The default
fixture is a small repository Parquet file, not the unavailable incident object.
"""

import argparse
import concurrent.futures
import hashlib
import http.server
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import threading
import time


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def build(work, build_dir, without_direct_load_guard):
    commands = json.loads((build_dir / "compile_commands.json").read_text())
    template = next(x for x in commands if x["file"].endswith("/GlutenS3FileSystemTest.cc"))
    sources = [
        HERE / "Reproduce.cc",
        ROOT / "cpp/velox/filesystem/GlutenS3FileSystem.cc",
        ROOT / "cpp/velox/utils/GlutenS3TokenProvider.cc",
        ROOT / "cpp/velox/utils/ConfigExtractor.cc",
    ]
    velox = ROOT / "ep/build-velox/build/velox_ep"
    direct_source = velox / "velox/dwio/common/DirectBufferedInput.cpp"
    direct_entry = next(
        x for x in json.loads((velox / "_build/release/compile_commands.json").read_text())
        if Path(x["file"]) == direct_source
    )
    direct_copy = work / "DirectBufferedInput.cpp"
    direct_text = direct_source.read_text()
    guard = """  if (state() != CoalescedLoad::State::kLoaded) {
    return {};
  }
"""
    if without_direct_load_guard:
        assert direct_text.count(guard) == 1, "Expected the upstream cancelled-load guard"
        direct_text = direct_text.replace(guard, "")
    direct_copy.write_text(direct_text)
    sources.append(direct_copy)

    def compile_one(source):
        entry = direct_entry if source == direct_copy else next(
            (x for x in commands if Path(x["file"]) == source), template
        )
        args = shlex.split(entry["command"])
        obj = work / (source.stem + ".o")
        args[args.index("-o") + 1] = str(obj)
        args[args.index("-c") + 1] = str(source)
        args = ["-O0" if x == "-O3" else x for x in args]
        with (work / (source.stem + ".build.log")).open("w") as log:
            subprocess.run(args, cwd=entry["directory"], stdout=log, stderr=subprocess.STDOUT, check=True)
        return str(obj)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        objects = list(pool.map(compile_one, sources))
    commands = subprocess.check_output(
        ["ninja", "-C", str(build_dir), "-t", "commands", "gluten_s3_file_system_test"], text=True
    )
    line = next(
        x for x in reversed(commands.splitlines())
        if " -o velox/tests/gluten_s3_file_system_test " in x
    )
    args = shlex.split(line)
    start = next(i for i, x in enumerate(args) if x.endswith("/c++") or x.endswith("/g++"))
    args = args[start:args.index("&&", start)]
    args = [x for x in args if not x.endswith(".o") and not x.startswith("-Wl,--dependency-file=")]
    args[args.index("-o") + 1] = str(work / "reproduce")
    args[1:1] = objects
    # Some local Arrow builds bundle Snappy. Resolve Velox's Snappy symbols
    # first so the duplicate bundled archive members are not pulled in.
    snappy = next((x for x in args if x.endswith("/libsnappy.a")), None)
    arrow = next((x for x in args if x.endswith("/libarrow.a")), None)
    if snappy and arrow:
        args.remove(snappy)
        args.insert(args.index(arrow), snappy)
    with (work / "link.log").open("w") as log:
        subprocess.run(args, cwd=build_dir, stdout=log, stderr=subprocess.STDOUT, check=True)


def run_case(binary, data, name, mode, file_size, behaviors, attempts=3):
    requests = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            index = len(requests)
            behavior = behaviors[min(index, len(behaviors) - 1)]
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
            start, end = map(int, match.groups())
            payload = data[start:min(end + 1, len(data))]
            if behavior == "short":
                payload = payload[:-4]
            sent_size = len(payload)
            if behavior == "timeout":
                sent_size = 0
            elif behavior == "partial-timeout":
                sent_size = 1
            requests.append({
                "behavior": behavior,
                "range": [start, end],
                "response_bytes": sent_size,
            })
            if behavior == "timeout":
                time.sleep(5)
                self.close_connection = True
                return
            if behavior == "partial-timeout":
                self.send_response(206)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload[:1])
                self.wfile.flush()
                time.sleep(5)
                self.close_connection = True
                return
            self.send_response(206)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Content-Range", f"bytes {start}-{start + len(payload) - 1}/{len(data)}")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            [str(binary), f"http://127.0.0.1:{server.server_port}", str(file_size), mode, str(attempts)],
            capture_output=True, text=True, timeout=60, check=True,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    return {"name": name, "requests": requests, "stdout": result.stdout, "stderr": result.stderr}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, default=ROOT / "cpp-ch/local-engine/tests/data/sample.parquet")
    parser.add_argument("--work", type=Path, default=Path("/tmp/gluten-s3-read-failures"))
    parser.add_argument("--build-dir", type=Path, default=ROOT / "cpp/build")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--without-direct-load-guard", action="store_true",
                        help="Remove the cancelled-load guard in a temporary Velox source copy")
    parser.add_argument("--case", action="append", help="Run only the named case (repeatable)")
    args = parser.parse_args()
    args.work = args.work.resolve()
    args.build_dir = args.build_dir.resolve()
    args.work.mkdir(parents=True, exist_ok=True)
    # Ensure the extracted Spark configuration is deterministic and never use
    # real credential/profile overrides for the localhost-only test.
    for key in list(os.environ):
        if key.startswith("AWS_"):
            os.environ.pop(key)
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    if not args.skip_build:
        print("Compiling current checkout sources; build logs:", args.work, flush=True)
        build(args.work, args.build_dir, args.without_direct_load_guard)
        (args.work / "build-variant.json").write_text(json.dumps({
            "without_direct_load_guard": args.without_direct_load_guard,
        }))
    else:
        variant = json.loads((args.work / "build-variant.json").read_text())
        assert variant["without_direct_load_guard"] == args.without_direct_load_guard
    data = args.file.read_bytes()
    if data[:4] != b"PAR1" or data[-4:] != b"PAR1":
        raise ValueError(f"{args.file}: expected a Parquet file with PAR1 header and footer")
    binary = args.work / "reproduce"
    schedule = subprocess.check_output([str(binary), "--schedule"], text=True, timeout=10)
    assert "mode=legacy requests=21 backoff_ms=26214350 connect_timeout_ms=30000 total_ms=26844350" in schedule
    print(schedule, flush=True)
    cases = [
        ("healthy-vended", "vended", len(data), ["ok"], 1),
        ("timeout-then-recovered", "vended", len(data), ["timeout"] * 3 + ["ok"], 3),
        ("partial-timeout-then-recovered", "vended", len(data), ["partial-timeout"] * 3 + ["ok"], 3),
        ("short-success-vended", "vended", len(data), ["short"], 1),
        ("short-success-ordinary", "ordinary", len(data), ["short"], 1),
        ("short-success-raw", "raw", len(data), ["short"], 1),
        ("stale-oversized-file-length", "vended", len(data) + 4, ["ok"], 1),
        (
            "reported-error-sequence", "vended", len(data),
            ["timeout"] * 3 + ["timeout", "timeout", "short"] * 2, 3,
        ),
        ("cached-timeout-then-recovered", "cached", len(data), ["timeout"] * 6 + ["ok"], 3),
        ("cached-short-response", "cached", len(data), ["short", "ok"], 3),
        ("direct-healthy", "direct", len(data), ["ok"], 3),
        ("direct-timeouts-only", "direct", len(data), ["timeout"], 3),
        ("direct-prefetch-fails-then-recovers", "direct", len(data),
         ["ok"] + ["timeout"] * 3 + ["ok"], 3),
    ]
    report = {
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "file": str(args.file),
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "schedule": schedule,
        "without_direct_load_guard": args.without_direct_load_guard,
        "cases": [],
    }
    for name, mode, size, behaviors, attempts in cases:
        if args.case and name not in args.case:
            continue
        print("Running", name, flush=True)
        result = run_case(binary, data, name, mode, size, behaviors, attempts)
        report["cases"].append(result)
        (args.work / "results.json").write_text(json.dumps(report, indent=2))
        print(result["stdout"], flush=True)
        output = result["stdout"]
        if name in ("healthy-vended", "direct-healthy"):
            assert "attempt=0 rows=" in output and "error=" not in output
        elif name == "direct-timeouts-only":
            assert all(x["behavior"] == "timeout" for x in result["requests"])
            assert "attempt=0 elapsed_ms=" in output and "curlCode: 28" in output
            if args.without_direct_load_guard:
                assert output.count("Reason: No magic bytes") == 2
                assert len(result["requests"]) == 9
            else:
                assert "No magic bytes" not in output
                assert output.count("attempt=") == 3
                assert len(result["requests"]) == 15
        elif name == "direct-prefetch-fails-then-recovers":
            assert "prefetch_error=" in result["stderr"]
            assert "curlCode: 28" in result["stderr"]
            assert "attempt=0 rows=" in output and "attempt=2 rows=" in output
            if args.without_direct_load_guard:
                assert output.count("Reason: No magic bytes") == 1
                assert len(result["requests"]) == 5
            else:
                assert "attempt=1 rows=" in output and "error=" not in output
                assert len(result["requests"]) == 6
        elif name in (
            "timeout-then-recovered", "partial-timeout-then-recovered",
            "cached-timeout-then-recovered",
        ):
            assert "curlCode: 28" in output
            assert "attempt=1 rows=" in output and "attempt=2 rows=" in output
            assert "No magic bytes" not in output
        elif name == "short-success-raw":
            assert f"returned_bytes={len(data)} footer_magic=0" in output
            assert result["requests"][0]["response_bytes"] == len(data) - 4
        elif name == "reported-error-sequence":
            assert "curlCode: 28" in output
            assert output.count("Reason: No magic bytes") == 2
        else:
            assert "Reason: No magic bytes found at end of the Parquet file" in output
        result["checks_passed"] = True
        (args.work / "results.json").write_text(json.dumps(report, indent=2))
    print("Results:", args.work / "results.json")


if __name__ == "__main__":
    main()
