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
import base64
import binascii
from collections import Counter
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET

SPEC = importlib.util.spec_from_file_location(
    "delta_test_results", Path(__file__).with_name("compare-test-results.py")
)
results = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(results)

INFRASTRUCTURE = "org.apache.spark.sql.gluten."
CANARY = INFRASTRUCTURE + "GlutenActivationSuite"
CANARY_TEST = "native execution with inherited Gluten defaults"
SBT_SUITE_SELECTOR = "(It is not a test it is a sbt.testing.SuiteSelector)"
EXECUTED = {"Success", "Failure", "Error"}
STATUSES = EXECUTED | {"Skipped", "Ignored", "Canceled", "Pending"}


def display_test(key):
    return "{}#{}".format(key[0], json.dumps(key[1], ensure_ascii=True))


def decode_identity(value):
    try:
        return base64.b64decode(value, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as error:
        raise ValueError("Invalid execution identity encoding") from error


def execution_name(suite, name, fingerprint):
    # junit-interface 0.11 prefixes method selectors. ScalaTest names must stay untouched.
    if fingerprint in {
        "annotation:org.junit.Test",
        "subclass:junit.framework.TestCase",
    }:
        for prefix in (suite + ".", suite.rsplit(".", 1)[-1] + "."):
            if name.startswith(prefix):
                return name[len(prefix) :]
    return name


def read_executions(directory):
    failures = sorted(directory.glob("execution-error-*.txt"))
    if failures:
        raise ValueError(
            "Execution result listener failed:\n"
            + "\n".join(path.read_text(encoding="utf-8") for path in failures[:3])
        )
    paths = sorted(directory.glob("execution-*.tsv"))
    if not paths:
        raise ValueError(
            "Missing lossless execution results; the sbt listener did not run"
        )
    outcomes = {}
    groups = Counter()
    aborted = set()
    canaries = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        if not text.endswith("\n"):
            raise ValueError("Truncated execution results: " + str(path))
        lines = text[:-1].split("\n")
        if len(lines) < 2:
            raise ValueError("Incomplete execution results: " + str(path))
        header, footer = lines[0].split("\t"), lines[-1].split("\t")
        if (
            len(header) != 3
            or header[:2] != ["version", "1"]
            or len(footer) != 3
            or footer[0] != "complete"
            or footer[1] not in {"Passed", "Failed", "Error"}
            or not re.fullmatch(r"0|[1-9][0-9]*", footer[2])
            or int(footer[2]) != len(lines) - 2
        ):
            raise ValueError("Invalid execution result framing: " + str(path))
        group = decode_identity(header[2])
        if not group:
            raise ValueError("Empty execution result group: " + str(path))
        groups[group] += 1
        if groups[group] > 1 and group != CANARY:
            raise ValueError("Duplicate execution result group: " + group)
        failed_event = False
        for line in lines[1:-1]:
            parts = line.split("\t")
            if (
                len(parts) != 5
                or parts[0] not in {"test", "suite"}
                or parts[1] not in STATUSES
            ):
                raise ValueError("Invalid execution event: " + str(path))
            kind, status = parts[:2]
            suite, name, fingerprint = map(decode_identity, parts[2:])
            if (
                not suite
                or (kind == "test" and not name)
                or (kind == "suite" and name)
                or (
                    not re.fullmatch(r"(annotation|subclass):[\w.$]+", fingerprint)
                    and not (kind == "suite" and fingerprint == "")
                )
            ):
                raise ValueError("Invalid execution event identity: " + str(path))
            failed_event |= status in {"Failure", "Error"}
            if kind == "suite":
                if status in {"Failure", "Error"}:
                    aborted.add(suite)
                continue
            key = (suite, execution_name(suite, name, fingerprint))
            if key == (CANARY, CANARY_TEST):
                canaries.append(status)
            elif key in outcomes:
                raise ValueError("Duplicate execution result: " + display_test(key))
            outcomes[key] = status
        if footer[1] != "Passed" and not failed_event:
            aborted.add(group)
    return outcomes, groups, aborted, canaries


def junit_counts(reports):
    """Cross-check cardinality, never identities, across duplicate JUnit reporters."""
    counts = Counter()
    for path in reports.rglob("TEST-*.xml"):
        root = ET.parse(path).getroot()
        per_file = Counter()
        for suite in results._iter_testsuites(root):
            name = suite.get("name") or ""
            per_file[name] += 0
            for case in suite:
                if case.tag.split("}")[-1] != "testcase":
                    continue
                if case.get("name") == SBT_SUITE_SELECTOR:
                    continue
                per_file[case.get("classname") or name] += 1
        for suite, count in per_file.items():
            counts[suite] = max(counts[suite], count)
    return counts


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
        for number, line in enumerate(text[:-1].split("\n"), 1):
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
    xml_passed, xml_failed, xml_skipped, _ = results.parse_reports(str(reports))
    outcomes, groups, aborted, canaries = read_executions(evidence)
    executed = {key for key, status in outcomes.items() if status in EXECUTED}
    if not any(not suite.startswith(INFRASTRUCTURE) for suite, _ in executed):
        raise ValueError(
            "No Delta tests executed; validation canaries cannot replace the upstream suite"
        )
    reported = set(outcomes)
    observed_suites = set(groups) | {suite for suite, _ in reported} | aborted
    unknown = observed_suites - inventory.keys()
    if unknown:
        raise ValueError(
            "Execution results contain suites absent from the compiled inventory: "
            + ", ".join(sorted(unknown))
        )
    errors = []
    xml_suites = {suite for suite, _ in xml_passed | xml_failed | xml_skipped}
    missing_results = xml_suites - observed_suites
    if missing_results:
        errors.append(
            "JUnit suites have no lossless execution results: "
            + ", ".join(sorted(missing_results))
        )
    raw_counts = Counter(suite for suite, _ in outcomes)
    for suite, count in junit_counts(reports).items():
        if suite != CANARY and raw_counts[suite] != count:
            errors.append(
                "Execution/JUnit count mismatch for {}: {} raw results, {} JUnit tests".format(
                    suite, raw_counts[suite], count
                )
            )
    required_groups = {
        suite
        for suite, role in inventory.items()
        if role in {"required", "self-test", "canary"}
    }
    if required_groups - groups.keys():
        errors.append(
            "Missing completed execution groups: "
            + ", ".join(sorted(required_groups - groups.keys()))
        )
    for suite in sorted(aborted):
        if inventory[suite] != "uncovered":
            errors.append("Required execution suite aborted: " + suite)
    exemption_identities = {}
    for key in reported:
        normalized = results.format_entry(*results.normalize_key(*key))
        exemption_identities.setdefault(normalized, set()).add(key)
    for entry in exemptions:
        if not entry.endswith("#*") and len(exemption_identities.get(entry, ())) > 1:
            errors.append(
                "Ambiguous exemption matches distinct raw test names: " + entry
            )
    native = set()
    checked = {}
    for row in read_evidence(evidence):
        key = (row["suite"], row["test"])
        if row["kind"] == "violation":
            errors.append(
                "{} [{}]: {}".format(
                    display_test(key),
                    row.get("phase", "unknown"),
                    row.get("message", "validation failed"),
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
                errors.append("Duplicate validation for " + display_test(key))
            if key not in reported:
                errors.append(
                    "Validation has no matching execution result: " + display_test(key)
                )
            expected_reason = exemptions.get(
                results.format_entry(*results.normalize_key(*key)),
                exemptions.get(key[0] + "#*"),
            )
            if row.get("status") == "exempt":
                if (
                    not expected_reason
                    or row.get("reason") != expected_reason
                    or type(row.get("checks")) is not int
                    or row["checks"] != 0
                ):
                    errors.append(
                        "Undeclared validation exemption: " + display_test(key)
                    )
            elif (
                row.get("status") != "checked"
                or type(row.get("checks")) is not int
                or row["checks"] < 1
                or expected_reason is not None
            ):
                errors.append("Invalid completed validation: " + display_test(key))
            checked[key] = row

    if len(native) != expected_forks:
        errors.append(
            "Expected native proof from {} JVMs, received {}".format(
                expected_forks, len(native)
            )
        )
    if (
        groups[CANARY] != expected_forks
        or len(canaries) != expected_forks
        or any(status != "Success" for status in canaries)
    ):
        errors.append(
            "Native canary execution results do not cover every requested fork"
        )
    if (CANARY, CANARY_TEST) not in xml_passed:
        errors.append("Native canary has no successful JUnit result")
    for key, status in outcomes.items():
        if key[0].startswith(INFRASTRUCTURE) and status in {"Failure", "Error"}:
            errors.append("Validation harness test failed: " + display_test(key))
    if any(suite.startswith(INFRASTRUCTURE) for suite, _ in xml_failed):
        errors.append("Validation harness has a failed JUnit report")
    required = {key for key in executed if inventory[key[0]] == "required"}
    missing = required - checked.keys()
    if missing:
        errors.append(
            "{} tests are missing required validation:\n{}".format(
                len(missing),
                "\n".join(display_test(key) for key in sorted(missing)[:20]),
            )
        )
    for key, row in checked.items():
        if row["pid"] not in native:
            errors.append(
                "No native proof for JVM {} running {}".format(
                    row["pid"], display_test(key)
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
