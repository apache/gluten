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

"""Join execution evidence with JUnit outcomes; missing evidence is never a native pass."""

import collections
import csv
import json
import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

STATUSES = (
    "PASSED_NATIVE",
    "PASSED_FALLBACK",
    "FAILED",
    "NO_ICEBERG_EXECUTION",
    "PASSED_UNVERIFIED",
    "SKIPPED",
    "COVERAGE_ERROR",
)
REPORT_VERSION = 2


def native_proven(row):
    return (
        row.get("body_started") is True
        and bool(row.get("native"))
        and not row.get("fallback")
        and not row.get("unverified")
        and not row.get("failed_queries")
        and not row.get("expected_exception")
        and all(
            any(value > 0 for value in entry.get("metrics", {}).values())
            for entry in row.get("native_metrics", [])
        )
        and set(row["native"])
        <= {entry.get("node") for entry in row.get("native_metrics", [])}
    )


def classify(directory):
    evidence = {}
    for path in sorted(directory.glob("coverage-*.jsonl")):
        for line in path.read_text().splitlines():
            row = json.loads(line)
            # Surefire drops empty parentheses for ordinary @Test methods, but retains the
            # signature and invocation index for parameterized/template tests.
            name = row["name"]
            key = (row["class"], name[:-2] if name.endswith("()") else name)
            if key in evidence:
                raise ValueError(f"Duplicate execution coverage for {key}")
            evidence[key] = row

    results, seen = [], set()
    for path in sorted(directory.glob("TEST-*.xml")):
        for test in ET.parse(path).getroot().iter("testcase"):
            key = (test.get("classname", ""), test.get("name", ""))
            if not key[0].startswith("org.apache.iceberg."):
                continue
            if key in seen:
                raise ValueError(f"Duplicate JUnit result: {key}")
            seen.add(key)
            row = dict(evidence.get(key, {}), **{"class": key[0], "name": key[1]})
            failure = test.find("failure")
            if failure is None:
                failure = test.find("error")
            if failure is not None:
                row.update(
                    status="FAILED",
                    failure=failure.get("message", ""),
                    failure_type=failure.get("type", ""),
                )
                if key not in evidence:
                    row["coverage_error"] = (
                        "Failure before per-test observation; check test setup or fork logs"
                    )
            elif test.find("skipped") is not None:
                row["status"] = "SKIPPED"
            elif row.get("coverage_error") or row.get("status") not in (
                "PASSED_NATIVE",
                "PASSED_FALLBACK",
                "NO_ICEBERG_EXECUTION",
                "PASSED_UNVERIFIED",
            ):
                row["coverage_error"] = (
                    row.get("coverage_error")
                    or "Missing or inconsistent execution evidence"
                )
            elif row["status"] == "PASSED_NATIVE" and not native_proven(row):
                row["coverage_error"] = (
                    "Native label without successful test-body executor evidence"
                )
            if row.get("coverage_error"):
                row["status"] = "COVERAGE_ERROR"
            results.append(row)
    if not results:
        raise ValueError("No upstream Iceberg JUnit results found")
    if evidence.keys() - seen:
        raise ValueError(
            f"Execution evidence without JUnit result: {sorted(evidence.keys() - seen)}"
        )
    return results


def write_reports(directory, results, label="Iceberg execution coverage"):
    counts = collections.Counter(row["status"] for row in results)
    summary = {status: counts[status] for status in STATUSES}
    (directory / "execution-coverage.json").write_text(
        json.dumps(
            {
                "version": REPORT_VERSION,
                "scope": "test-body Iceberg I/O",
                "counts": summary,
                "tests": results,
            },
            indent=2,
        )
        + "\n"
    )
    columns = (
        "status",
        "class",
        "test",
        "parameters",
        "parameter_values",
        "test_configuration",
        "native_nodes",
        "fallback_nodes",
        "unverified_nodes",
        "native_metrics",
        "body_started",
        "failed_queries",
        "expected_exception",
        "failure_origin",
        "failure_phase",
        "failure_type",
        "vanilla_status",
        "vanilla_error",
        "error",
    )
    exports = {
        "all-tests.csv": None,
        "native.csv": {"PASSED_NATIVE"},
        "fallback.csv": {"PASSED_FALLBACK"},
        "failed.csv": {"FAILED", "COVERAGE_ERROR"},
        "unverified.csv": {"PASSED_UNVERIFIED", "NO_ICEBERG_EXECUTION"},
        "skipped.csv": {"SKIPPED"},
        "execution-coverage.tsv": None,
    }
    for filename, statuses in exports.items():
        with (directory / filename).open("w", newline="") as output:
            writer = csv.writer(
                output, delimiter="\t" if filename.endswith(".tsv") else ","
            )
            writer.writerow(columns)
            for row in results:
                if statuses is not None and row["status"] not in statuses:
                    continue
                writer.writerow(
                    (
                        row["status"],
                        row["class"],
                        row["name"],
                        row.get("display_name", ""),
                        json.dumps(row.get("parameters", {}), sort_keys=True),
                        json.dumps(row.get("test_configuration", {}), sort_keys=True),
                        ", ".join(row.get("native", [])),
                        ", ".join(row.get("fallback", [])),
                        ", ".join(row.get("unverified", [])),
                        json.dumps(row.get("native_metrics", []), sort_keys=True),
                        row.get("body_started", ""),
                        row.get("failed_queries", ""),
                        row.get("expected_exception", ""),
                        row.get(
                            "failure_origin",
                            (
                                "UNVERIFIED"
                                if row["status"] in {"FAILED", "COVERAGE_ERROR"}
                                else ""
                            ),
                        ),
                        (
                            row.get("failure_phase", "")
                            if row["status"] == "FAILED"
                            else ""
                        ),
                        row.get("failure_type", ""),
                        row.get("vanilla_status", ""),
                        row.get("vanilla_failure", ""),
                        row.get("coverage_error") or row.get("failure", ""),
                    )
                )
    table = f"### {label}\n\n| Iceberg execution result | Tests |\n| --- | ---: |\n"
    table += "".join(f"| {status} | {count} |\n" for status, count in summary.items())
    print(table)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(table)
    # Control runs and the comparison gate handle upstream failures. Broken observation fails here.
    return int(bool(counts["COVERAGE_ERROR"]))


def main(directory):
    return write_reports(directory, classify(directory))


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
