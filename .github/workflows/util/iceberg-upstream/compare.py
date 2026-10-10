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

"""Require each upstream Iceberg test to preserve or improve its base-commit result."""

import argparse
import csv
import json
import os
import re
from pathlib import Path

from summarize import REPORT_VERSION, native_proven

PASSING = {
    "PASSED_NATIVE",
    "PASSED_FALLBACK",
    "NO_ICEBERG_EXECUTION",
    "PASSED_UNVERIFIED",
}
ALLOWED = {
    "PASSED_NATIVE": {"PASSED_NATIVE"},
    "PASSED_FALLBACK": {"PASSED_FALLBACK", "PASSED_NATIVE"},
    "FAILED": PASSING | {"FAILED"},
    "NO_ICEBERG_EXECUTION": PASSING,
    "PASSED_UNVERIFIED": PASSING,
    "SKIPPED": PASSING | {"SKIPPED"},
}


def parameters(row):
    display = row.get("display_name", "")
    # Iceberg's UUID default-value argument is a fresh random v4 UUID in every JVM.
    # Retain its type and invocation index, without treating random test data as a mode.
    display = re.sub(
        r"^(\[\d+\] uuid, )[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}"
        r"-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        r"\1<random-uuid>",
        display,
    )
    value = json.dumps(
        {
            "parameters": row.get("parameters") or display,
            "configuration": row.get("test_configuration", {}),
        },
        sort_keys=True,
    )
    # The REST fixture binds a fresh local port in each fork; this is not a test option.
    return re.sub(
        r"http://(?:localhost|127\.0\.0\.1):\d+/?", "http://localhost:<port>/", value
    )


def index(report, validate=True):
    if validate and report.get("version") != REPORT_VERSION:
        raise ValueError(
            "Incompatible coverage schema; a baseline from the current harness is required"
        )
    tests = {}
    for row in report["tests"]:
        key = (row["class"], row["name"])
        if key in tests:
            raise ValueError(f"Duplicate test result: {key}")
        if validate and (row["status"] not in ALLOWED or row.get("coverage_error")):
            raise ValueError(f"Invalid execution coverage: {key}: {row['status']}")
        if validate and row["status"] == "PASSED_NATIVE" and not native_proven(row):
            raise ValueError(f"Unproven native coverage: {key}")
        if (
            validate
            and row["status"] == "FAILED"
            and row.get("failure_origin") != "GLUTEN"
        ):
            raise ValueError(
                f"Unverified failure attribution: {key}: {row.get('failure_origin', 'MISSING')}"
            )
        tests[key] = row
    if not tests:
        raise ValueError("Cannot gate an empty test report")
    return tests


def compare(baseline, current, validate=True):
    before, after = index(baseline, validate), index(current, validate)
    regressions, improvements, changes = [], [], []
    for key in sorted(before.keys() | after.keys()):
        old = before.get(key, {}).get("status", "NEW")
        new = after.get(key, {}).get("status", "MISSING")
        previous, current_row = before.get(key, {}), after.get(key, {})
        change = {
            "class": key[0],
            "name": key[1],
            "before": old,
            "after": new,
            "base": previous,
            "current": current_row,
        }
        # Newly discovered upstream tests must pass or be disabled upstream. Missing or
        # newly skipped tests cannot conceal a previous pass (or an unresolved failure).
        allowed = PASSING | {"SKIPPED"} if old == "NEW" else ALLOWED.get(old, set())
        mismatch = bool(
            previous
            and current_row
            and old != "SKIPPED"
            and new != "SKIPPED"
            and parameters(previous) != parameters(current_row)
        )
        if mismatch:
            change["reason"] = "PARAMETER_MISMATCH"
            regressions.append(change)
        elif new not in allowed:
            change["reason"] = "STATUS_REGRESSION"
            regressions.append(change)
        elif old != new and new != "SKIPPED":
            improvements.append(change)
        # Metrics/timings vary between successful runs. Compare their existence and node types,
        # while retaining the actual values in each changed row for verification.
        evidence_changed = any(
            sorted(previous.get(field, [])) != sorted(current_row.get(field, []))
            for field in ("native", "fallback", "unverified")
        )
        failure_changed = any(
            previous.get(field) != current_row.get(field)
            for field in (
                "failure_type",
                "failure_phase",
                "failure_origin",
                "coverage_error",
                "expected_exception",
            )
        )
        failure_changed |= bool(previous.get("failed_queries")) != bool(
            current_row.get("failed_queries")
        )
        if old != new or mismatch or evidence_changed or failure_changed:
            change.setdefault(
                "reason", "STATUS_CHANGE" if old != new else "EVIDENCE_CHANGE"
            )
            changes.append(change)
    return {
        "regressions": regressions,
        "improvements": improvements,
        "changes": changes,
        "base_tests": len(before),
        "current_tests": len(after),
    }


def write_changes(path, changes):
    fields = (
        "display_name",
        "native",
        "fallback",
        "unverified",
        "native_metrics",
        "failed_queries",
        "expected_exception",
        "failure_phase",
        "failure_type",
        "failure_origin",
        "vanilla_status",
        "coverage_error",
        "failure",
    )
    columns = [
        "reason",
        "class",
        "test",
        "before",
        "after",
        "parameters_before",
        "parameters_after",
        *[
            f"{field}_{revision}"
            for field in fields
            for revision in ("before", "after")
        ],
    ]
    with path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in changes:
            old, new = row["base"], row["current"]
            values = [
                row["reason"],
                row["class"],
                row["name"],
                row["before"],
                row["after"],
                parameters(old),
                parameters(new),
            ]
            for field in fields:
                for revision in (old, new):
                    value = revision.get(field, "")
                    values.append(
                        json.dumps(value, sort_keys=True)
                        if isinstance(value, (list, dict))
                        else value
                    )
            writer.writerow(values)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("current", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Validate initial head evidence when the target tree has no Iceberg harness yet",
    )
    args = parser.parse_args()
    baseline, current, errors = None, None, []
    for label, path in (("Baseline", args.baseline), ("Current report", args.current)):
        if label == "Baseline" and args.bootstrap and not path.exists():
            continue
        try:
            report = json.loads(path.read_text())
            if not isinstance(report, dict) or not isinstance(
                report.get("tests"), list
            ):
                raise ValueError("Expected a test report object with a tests array")
            if label == "Baseline":
                baseline = report
            else:
                current = report
        except (ValueError, OSError) as error:
            errors.append(f"{label} unavailable: {error}")
    diagnostic = args.baseline.parent / "baseline-unavailable.txt"
    if baseline is None and diagnostic.exists() and not args.bootstrap:
        errors.append(diagnostic.read_text().strip())
    result = {
        "comparison_performed": False,
        "changes": [],
        "regressions": [],
        "improvements": [],
        "base_tests": len((baseline or {}).get("tests", [])),
        "current_tests": len((current or {}).get("tests", [])),
    }
    try:
        if errors:
            if current is not None:
                index(current)
        elif baseline is None and args.bootstrap:
            index(current)
            result["baseline_bootstrap"] = True
        else:
            result = compare(baseline, current)
            result["comparison_performed"] = True
    except (ValueError, KeyError, TypeError) as error:
        # Still export diagnostic differences when complete reports exist, but invalid
        # evidence must fail the gate even if no statuses changed.
        if baseline is not None and current is not None:
            try:
                result = compare(baseline, current, validate=False)
            except (ValueError, KeyError, TypeError):
                pass
        result["comparison_performed"] = False
        errors.append(str(error))
    if errors:
        result["validation_errors"] = errors
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    write_changes(args.output.with_suffix(".csv"), result["changes"])
    lines = (
        [
            f"Iceberg initial baseline: {result['current_tests']} current tests validated. "
            "The target branch has no Iceberg harness yet; no regression comparison was performed. "
            "This head result becomes eligible for reuse only after its tested tree is merged."
        ]
        if result.get("baseline_bootstrap")
        else (
            [
                f"Iceberg: {len(result['regressions'])} regressions, "
                f"{len(result['improvements'])} improvements; "
                f"{result['current_tests']} current tests."
            ]
            if result["comparison_performed"]
            else [
                f"Iceberg comparison unavailable; {result['current_tests']} current tests. "
                "The check fails, but available head results remain downloadable for manual review."
            ]
        )
    )
    if result.get("validation_errors"):
        lines.append(
            "Report validation failed; the CSV is diagnostic only: "
            + "; ".join(result["validation_errors"])
        )
    for row in result["regressions"][:20]:
        lines.append(
            f"- `{row['class']}#{row['name']}`: {row['before']} → {row['after']}"
        )
    if len(result["regressions"]) > 20:
        lines.append("See the comparison artifact for the remaining regressions.")
    text = "\n".join(lines) + "\n"
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
    return int(bool(result["regressions"] or result.get("validation_errors")))


if __name__ == "__main__":
    raise SystemExit(main())
