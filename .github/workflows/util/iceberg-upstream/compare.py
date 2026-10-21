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

"""Gate native-status regressions and new test failures; highlight other changes."""

import argparse
import csv
import json
import os
import re
from pathlib import Path

from flaky import FLAKY_TESTS, is_flaky, load_flaky_tests
from summarize import REPORT_VERSION, native_data_proven, native_proven

PASSING = {
    "PASSED_NATIVE",
    "PASSED_FALLBACK",
    "NO_ICEBERG_EXECUTION",
    "PASSED_UNVERIFIED",
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


def index(report, validate=True, flaky_tests=None):
    if flaky_tests is None:
        flaky_tests = load_flaky_tests()
    if validate and report.get("version") != REPORT_VERSION:
        raise ValueError(
            "Incompatible coverage schema; a baseline from the current harness is required"
        )
    tests = {}
    for row in report["tests"]:
        key = (row["class"], row["name"])
        if key in tests:
            raise ValueError(f"Duplicate test result: {key}")
        tests[key] = row
        if not validate or is_flaky(row, flaky_tests):
            continue
        if row["status"] not in PASSING | {"FAILED", "SKIPPED"} or row.get(
            "coverage_error"
        ):
            raise ValueError(f"Invalid execution coverage: {key}: {row['status']}")
        if row["status"] == "PASSED_NATIVE" and not native_proven(row):
            raise ValueError(f"Unproven native coverage: {key}")
        if row["status"] == "FAILED" and row.get("failure_origin") != "GLUTEN":
            raise ValueError(
                f"Unverified failure attribution: {key}: {row.get('failure_origin', 'MISSING')}"
            )
    if not tests:
        raise ValueError("Cannot gate an empty test report")
    return tests


def proven_native_nodes(row):
    # Mixed execution still proves individual native operations. Planned nodes and
    # work associated with expected/failed queries do not establish that coverage.
    if (
        row.get("body_started") is not True
        or row.get("failed_queries")
        or row.get("expected_exception")
    ):
        return set()
    return set(row.get("native", [])) & {
        entry.get("node")
        for entry in row.get("native_metrics", [])
        if native_data_proven(entry)
    }


def compare(baseline, current, validate=True, flaky_tests=None):
    if flaky_tests is None:
        flaky_tests = load_flaky_tests()
    before = index(baseline, validate, flaky_tests)
    after = index(current, validate, flaky_tests)
    regressions, improvements, changes, highlights, ignored_changes = [], [], [], [], []
    ignored_tests = 0
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
            "ignored": is_flaky(previous or current_row, flaky_tests),
        }
        ignored_tests += change["ignored"]
        mismatch = bool(
            previous
            and current_row
            and old != "SKIPPED"
            and new != "SKIPPED"
            and parameters(previous) != parameters(current_row)
        )
        lost_native = proven_native_nodes(previous) - proven_native_nodes(current_row)
        if lost_native:
            change["lost_native"] = sorted(lost_native)
        if mismatch:
            change["parameter_mismatch"] = True
        # Only a native test becoming non-native or a newly failed test gates.
        # Node-level evidence changes remain visible even when the status is unchanged.
        regression = (old == "PASSED_NATIVE" and new != old) or (
            new == "FAILED" and old != new
        )
        improvement = (
            not regression
            and not mismatch
            and (
                (new == "PASSED_NATIVE" and old != new)
                or (old == "FAILED" and new in PASSING)
            )
        )
        if regression:
            change["reason"] = "STATUS_REGRESSION"
        elif mismatch:
            change["reason"] = "PARAMETER_MISMATCH"
        elif lost_native:
            change["reason"] = "NATIVE_COVERAGE_CHANGE"
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
        if old != new or mismatch or lost_native or evidence_changed or failure_changed:
            change.setdefault(
                "reason", "STATUS_CHANGE" if old != new else "EVIDENCE_CHANGE"
            )
            changes.append(change)
            if change["ignored"]:
                ignored_changes.append(change)
            elif regression:
                regressions.append(change)
            elif improvement:
                improvements.append(change)
                if lost_native:
                    highlights.append(change)
            else:
                highlights.append(change)
    return {
        "regressions": regressions,
        "improvements": improvements,
        "changes": changes,
        "highlights": highlights,
        "ignored_changes": ignored_changes,
        "ignored_tests": ignored_tests,
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
        "ignored",
        "lost_native",
        "parameter_mismatch",
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
                row["ignored"],
                json.dumps(row.get("lost_native", [])),
                row.get("parameter_mismatch", False),
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
        "--flaky-tests",
        type=Path,
        default=FLAKY_TESTS,
        help="class,method CSV whitelist (defaults to the adjacent flaky-tests.csv)",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Validate initial head evidence when the target tree has no Iceberg harness yet",
    )
    args = parser.parse_args()
    baseline, current, errors = None, None, []
    flaky_tests = set()
    try:
        flaky_tests = load_flaky_tests(args.flaky_tests)
    except (ValueError, OSError, csv.Error) as error:
        errors.append(f"Flaky test whitelist unavailable: {error}")
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
        "highlights": [],
        "ignored_changes": [],
        "ignored_tests": 0,
        "base_tests": len((baseline or {}).get("tests", [])),
        "current_tests": len((current or {}).get("tests", [])),
    }
    try:
        if errors:
            if current is not None:
                index(current, flaky_tests=flaky_tests)
        elif baseline is None and args.bootstrap:
            index(current, flaky_tests=flaky_tests)
            result["baseline_bootstrap"] = True
        else:
            result = compare(baseline, current, flaky_tests=flaky_tests)
            result["comparison_performed"] = True
    except (ValueError, KeyError, TypeError) as error:
        # Still export diagnostic differences when complete reports exist, but invalid
        # evidence must fail the gate even if no statuses changed.
        if baseline is not None and current is not None:
            try:
                result = compare(
                    baseline, current, validate=False, flaky_tests=flaky_tests
                )
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
    if result["ignored_tests"]:
        lines.append(
            f"{result['ignored_tests']} whitelisted test invocations excluded from gating."
        )
    for title, rows in (
        ("Regressions", result["regressions"]),
        ("Improvements", result["improvements"]),
        ("Other changes (informational)", result["highlights"]),
        ("Whitelisted changes (ignored)", result["ignored_changes"]),
    ):
        if not rows:
            continue
        lines.extend(["", f"### {title}: {len(rows)}", ""])
        for row in rows:
            detail = row["reason"]
            if row.get("lost_native"):
                detail += "; native nodes lost: " + ", ".join(row["lost_native"])
            if row.get("parameter_mismatch"):
                detail += "; parameters changed"
            lines.append(
                f"- `{row['class']}#{row['name']}`: {row['before']} → {row['after']} ({detail})"
            )
    text = "\n".join(lines) + "\n"
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
    return int(bool(result["regressions"] or result.get("validation_errors")))


if __name__ == "__main__":
    raise SystemExit(main())
