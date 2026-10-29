#!/usr/bin/env python3
#
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

"""Fail closed on missing/failed Gluten activation checks, outside the failure baseline."""

import argparse
from collections import Counter
import importlib.util
import json
import os
from pathlib import Path
import re
import sys

SPEC = importlib.util.spec_from_file_location(
    "delta_test_results", Path(__file__).with_name("compare-test-results.py")
)
results = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(results)

INFRASTRUCTURE = "org.apache.spark.sql.gluten."
CANARY = INFRASTRUCTURE + "GlutenActivationSuite"
CANARY_TEST = "native execution with inherited Gluten defaults"
SBT_SUITE_SELECTOR = "(It is not a test it is a sbt.testing.SuiteSelector)"


def read_exemptions(path):
    entries = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t", 1)
        if (
            len(parts) != 2
            or not parts[1].strip()
            or not re.fullmatch(r"[\w.$]+#.+", parts[0])
        ):
            raise ValueError(
                "Exemptions require <exact suite>#<test><TAB><reason>: " + line
            )
        if parts[0].startswith(INFRASTRUCTURE) or parts[0] in entries:
            raise ValueError("Duplicate or infrastructure exemption: " + parts[0])
        entries[parts[0]] = parts[1].strip()
    return entries


def read_inventory(directory):
    inventory = {}
    for line in (
        (directory / "expected-suites.tsv").read_text(encoding="utf-8").splitlines()
    ):
        parts = line.split("\t")
        if len(parts) != 2 or parts[0] not in {
            "required",
            "uncovered",
            "canary",
            "self-test",
        }:
            raise ValueError("Malformed compiled-suite inventory: " + line)
        role, suite = parts
        if not re.fullmatch(r"[\w.$]+", suite) or suite in inventory:
            raise ValueError("Invalid or duplicate inventory suite: " + suite)
        inventory[suite] = role
    if inventory.get(CANARY) != "canary":
        raise ValueError("Compiled-suite inventory is missing the native canary")
    return inventory


def read_evidence(directory):
    paths = sorted(directory.glob("jvm-*.jsonl"))
    if not paths:
        raise ValueError(
            "No JVM validation evidence; the validation hooks may not have run"
        )
    rows = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        if not text or not text.endswith("\n"):
            raise ValueError("Empty or truncated validation evidence: " + str(path))
        for number, line in enumerate(text.splitlines(), 1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError("{}:{}: {}".format(path, number, error)) from error
            if (
                not isinstance(row, dict)
                or type(row.get("version")) is not int
                or row["version"] != 1
                or not isinstance(row.get("pid"), str)
                or not re.fullmatch(r"[1-9][0-9]*", row["pid"])
                or row.get("kind") not in {"test", "canary", "violation"}
                or not isinstance(row.get("suite"), str)
                or not row["suite"]
                or not isinstance(row.get("test"), str)
                or not row["test"]
            ):
                raise ValueError("{}:{}: invalid evidence schema".format(path, number))
            rows.append(row)
    return rows


def validate(reports, evidence, exemptions_path, expected_forks):
    if expected_forks < 1:
        raise ValueError("Expected fork count must be positive")
    inventory = read_inventory(evidence)
    planned_forks = int(
        (evidence / "expected-forks.txt").read_text(encoding="utf-8").strip()
    )
    if planned_forks != expected_forks:
        raise ValueError(
            "Fork plan {} disagrees with requested parallelism {}".format(
                planned_forks, expected_forks
            )
        )
    exemptions = read_exemptions(exemptions_path)
    passed, failed, skipped, _ = results.parse_reports(str(reports))
    executed = passed | failed
    if not any(
        not suite.startswith(INFRASTRUCTURE)
        and test not in {results.SUITE_ABORTED, SBT_SUITE_SELECTOR}
        for suite, test in executed
    ):
        raise ValueError(
            "No Delta tests executed; validation canaries cannot replace the upstream suite"
        )
    reported = executed | skipped
    observed_suites = {suite for suite, _ in reported}
    unknown = observed_suites - inventory.keys()
    if unknown:
        raise ValueError(
            "JUnit reports contain suites absent from the compiled inventory: "
            + ", ".join(sorted(unknown))
        )
    errors = []
    native = set()
    checked = {}
    for row in read_evidence(evidence):
        key = results.normalize_key(row["suite"], row["test"])
        if row["kind"] == "violation":
            errors.append(
                "{}#{} [{}]: {}".format(
                    *key,
                    row.get("phase", "unknown"),
                    row.get("message", "validation failed")
                )
            )
        elif row["kind"] == "canary":
            if key != (CANARY, CANARY_TEST) or row.get("status") != "passed":
                errors.append("Invalid native canary record: " + repr(row))
            elif row["pid"] in native:
                errors.append("Duplicate native canary for JVM " + row["pid"])
            else:
                native.add(row["pid"])
        else:
            if key in checked:
                errors.append("Duplicate validation for " + results.format_entry(*key))
            if key not in reported:
                errors.append(
                    "Validation has no matching JUnit result: "
                    + results.format_entry(*key)
                )
            expected_reason = exemptions.get(
                results.format_entry(*key), exemptions.get(key[0] + "#*")
            )
            if row.get("status") == "exempt":
                if (
                    not expected_reason
                    or row.get("reason") != expected_reason
                    or type(row.get("checks")) is not int
                    or row["checks"] != 0
                ):
                    errors.append(
                        "Undeclared validation exemption: " + results.format_entry(*key)
                    )
            elif (
                row.get("status") != "checked"
                or type(row.get("checks")) is not int
                or row["checks"] < 1
                or expected_reason is not None
            ):
                errors.append(
                    "Invalid completed validation: " + results.format_entry(*key)
                )
            checked[key] = row

    if len(native) != expected_forks:
        errors.append(
            "Expected native proof from {} JVMs, received {}".format(
                expected_forks, len(native)
            )
        )
    if (CANARY, CANARY_TEST) not in passed:
        errors.append("Native canary has no successful JUnit result")
    for key in sorted(failed):
        if key[0].startswith(INFRASTRUCTURE):
            errors.append(
                "Validation harness test failed: " + results.format_entry(*key)
            )
    required = {key for key in executed if inventory[key[0]] == "required"}
    missing = required - checked.keys()
    if missing:
        errors.append(
            "{} tests are missing required validation:\n{}".format(
                len(missing),
                "\n".join(results.format_entry(*key) for key in sorted(missing)[:20]),
            )
        )
    for key, row in checked.items():
        if row["pid"] not in native:
            errors.append(
                "No native proof for JVM {} running {}".format(
                    row["pid"], results.format_entry(*key)
                )
            )
    if errors:
        raise ValueError("\n".join(errors[:30]))
    uncovered = Counter(
        suite
        for suite, test in executed
        if inventory[suite] == "uncovered" and (suite, test) not in checked
    )
    return {
        "forks": len(native),
        "checked": sum(
            key in executed and row["status"] == "checked"
            for key, row in checked.items()
        ),
        "exempt": sum(
            key in executed and row["status"] == "exempt"
            for key, row in checked.items()
        ),
        "uncovered": dict(sorted(uncovered.items())),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports-dir", required=True, type=Path)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--exemptions", required=True, type=Path)
    parser.add_argument("--expected-forks", required=True, type=int)
    args = parser.parse_args()
    try:
        summary = validate(
            args.reports_dir, args.evidence_dir, args.exemptions, args.expected_forks
        )
    except (OSError, ValueError, results.NoReportsError) as error:
        print(
            "::error::Gluten activation validation failed:\n" + str(error),
            file=sys.stderr,
        )
        return 2
    lines = [
        "## Gluten activation validation",
        "",
        "{} native-validated JVMs; {} checked tests; {} explicit exemptions.".format(
            summary["forks"], summary["checked"], summary["exempt"]
        ),
        "",
        "### Uncovered suites (not automatically classified as Spark-free)",
        "",
    ]
    lines.extend(
        "- `{}`: {} executed tests".format(suite, count)
        for suite, count in summary["uncovered"].items()
    )
    if not summary["uncovered"]:
        lines.append("None.")
    text = "\n".join(lines) + "\n"
    print(text)
    (args.evidence_dir / "summary.txt").write_text(text, encoding="utf-8")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
            output.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
