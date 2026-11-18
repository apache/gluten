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

import importlib.util
import copy
import csv
import sys
import json
import os
import subprocess
import tempfile
from pathlib import Path
import unittest

SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/iceberg-upstream/compare.py"
)
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("comparison", SCRIPT)
comparison = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(comparison)


def report(status, name="read()[1]"):
    row = {"class": "org.apache.iceberg.TestScan", "name": name, "status": status}
    if status == "PASSED_NATIVE":
        row.update(
            body_started=True,
            native=["IcebergScanTransformer"],
            native_metrics=[
                {"node": "IcebergScanTransformer", "metrics": {"rawInputRows": 1}}
            ],
        )
    if status == "FAILED":
        row["failure_origin"] = "GLUTEN"
    return {"version": 2, "tests": [row]}


class ComparisonTest(unittest.TestCase):
    def test_initial_baseline_validates_head_without_claiming_a_comparison(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            head, base = root / "head.json", root / "base.json"
            command = [
                sys.executable,
                str(SCRIPT),
                str(base),
                str(head),
                "--output",
                str(root / "diff.json"),
                "--bootstrap",
            ]
            for current in (report("PASSED_NATIVE"), report("FAILED")):
                head.write_text(json.dumps(current))
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertEqual(0, result.returncode, result.stderr)
                diagnostic = json.loads((root / "diff.json").read_text())
                self.assertTrue(diagnostic["baseline_bootstrap"])
                self.assertFalse(diagnostic["comparison_performed"])
                self.assertNotIn("0 regressions", result.stdout)
                with (root / "diff.csv").open(newline="") as output:
                    self.assertEqual([], list(csv.DictReader(output)))
            for invalid in (report("COVERAGE_ERROR"), {"version": 2, "tests": []}):
                head.write_text(json.dumps(invalid))
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertEqual(1, result.returncode)
                diagnostic = json.loads((root / "diff.json").read_text())
                self.assertTrue(diagnostic["validation_errors"])
                self.assertFalse(diagnostic.get("baseline_bootstrap", False))
            head.write_text(json.dumps(report("FAILED")))
            base.write_text(json.dumps(report("PASSED_NATIVE")))
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(1, result.returncode)
            diagnostic = json.loads((root / "diff.json").read_text())
            self.assertTrue(diagnostic["comparison_performed"])
            self.assertEqual(1, len(diagnostic["regressions"]))

    def test_only_native_status_loss_and_new_failures_gate(self):
        statuses = sorted(comparison.PASSING | {"FAILED", "SKIPPED"})
        for old in statuses:
            for new in statuses:
                with self.subTest(before=old, after=new):
                    result = comparison.compare(report(old), report(new))
                    fails = (old == "PASSED_NATIVE" and new != old) or (
                        new == "FAILED" and old != new
                    )
                    self.assertEqual(fails, bool(result["regressions"]))

    def test_missing_non_native_tests_are_highlighted(self):
        for old in sorted(comparison.PASSING | {"FAILED", "SKIPPED"}):
            with self.subTest(before=old):
                result = comparison.compare(
                    report(old), report("PASSED_FALLBACK", "different")
                )
                self.assertEqual(old == "PASSED_NATIVE", bool(result["regressions"]))
                changes = [
                    row for row in result["changes"] if row["after"] == "MISSING"
                ]
                self.assertEqual(1, len(changes))
                if old != "PASSED_NATIVE":
                    self.assertIn(changes[0], result["highlights"])

    def test_passing_coverage_changes_are_informational_without_special_exceptions(
        self,
    ):
        statuses = ("PASSED_FALLBACK", "PASSED_UNVERIFIED", "NO_ICEBERG_EXECUTION")
        for expected_exception in (False, True):
            for old in statuses:
                for new in statuses:
                    with self.subTest(
                        expected=expected_exception, before=old, after=new
                    ):
                        base, current = report(old), report(new)
                        for source in (base, current):
                            source["tests"][0].update(
                                body_started=True, expected_exception=expected_exception
                            )
                        result = comparison.compare(base, current)
                        self.assertFalse(result["regressions"])
                        self.assertFalse(result["improvements"])
                        self.assertEqual(old != new, bool(result["highlights"]))
                        if old != new:
                            self.assertEqual(
                                base["tests"][0], result["highlights"][0]["base"]
                            )
                            self.assertEqual(
                                current["tests"][0], result["highlights"][0]["current"]
                            )

    def test_mixed_execution_coverage_loss_is_highlighted(self):
        base = report("PASSED_NATIVE")
        base["tests"][0].update(status="PASSED_FALLBACK", fallback=["AppendDataExec"])
        for loss in (
            "fallback",
            "unverified",
            "metrics",
            "timing",
            "body",
            "expected_error",
        ):
            current = copy.deepcopy(base)
            after = current["tests"][0]
            if loss in ("fallback", "unverified"):
                after.update(native=[], native_metrics=[])
                after.setdefault(loss, []).append("IcebergScanTransformer")
            elif loss == "metrics":
                after["native_metrics"][0]["metrics"]["rawInputRows"] = 0
            elif loss == "timing":
                after["native_metrics"][0]["metrics"] = {"wallNanos": 100}
            elif loss == "body":
                after["body_started"] = False
            else:
                after["expected_exception"] = True
            with self.subTest(loss=loss):
                result = comparison.compare(base, current)
                self.assertFalse(result["regressions"])
                self.assertFalse(result["improvements"])
                self.assertEqual(1, len(result["highlights"]))
                self.assertEqual(
                    "NATIVE_COVERAGE_CHANGE", result["highlights"][0]["reason"]
                )
                self.assertEqual(
                    ["IcebergScanTransformer"], result["highlights"][0]["lost_native"]
                )

    def test_native_to_native_node_changes_are_highlighted(self):
        base = report("PASSED_NATIVE")
        base["tests"][0]["native"].append("IcebergWriteExec")
        base["tests"][0]["native_metrics"].append(
            {"node": "IcebergWriteExec", "metrics": {"numWrittenBytes": 10}}
        )
        result = comparison.compare(base, report("PASSED_NATIVE"))
        self.assertFalse(result["regressions"])
        self.assertFalse(result["improvements"])
        self.assertEqual(["IcebergWriteExec"], result["highlights"][0]["lost_native"])

    def test_counter_variation_and_unproven_nodes_do_not_gate_mixed_execution(self):
        base = report("PASSED_NATIVE")
        row = base["tests"][0]
        row.update(status="PASSED_FALLBACK", fallback=["AppendDataExec"])
        current = copy.deepcopy(base)
        current["tests"][0]["native_metrics"][0]["metrics"]["rawInputRows"] = 20
        self.assertFalse(comparison.compare(base, current)["changes"])
        row["native"].append("UnprovenPlanNode")
        self.assertFalse(comparison.compare(base, current)["regressions"])

    def test_whitelist_excludes_all_invocations_from_every_gate_outcome(self):
        whitelist = {("org.apache.iceberg.TestScan", "read")}
        statuses = sorted(comparison.PASSING | {"FAILED", "SKIPPED", "COVERAGE_ERROR"})
        for old in statuses:
            for new in statuses:
                with self.subTest(before=old, after=new):
                    base, current = report(old), report(new)
                    current["tests"][0].update(
                        parameters={"formatVersion": "3"},
                        failure_origin="CONTROL_MISSING",
                    )
                    result = comparison.compare(base, current, flaky_tests=whitelist)
                    self.assertFalse(result["regressions"])
                    self.assertFalse(result["improvements"])
                    self.assertFalse(result["highlights"])
                    self.assertEqual(1, result["ignored_tests"])
                    self.assertTrue(result["changes"][0]["ignored"])
        for name in ("read", "read()", "read()[12]", "read(int)[3]"):
            base, current = report("PASSED_NATIVE", name), report("FAILED", name)
            current["tests"][0].pop("failure_origin")
            result = comparison.compare(base, current, flaky_tests=whitelist)
            self.assertFalse(result["regressions"])
            self.assertEqual(1, len(result["ignored_changes"]))
        removed = comparison.compare(
            report("PASSED_NATIVE"),
            report("PASSED_FALLBACK", "other"),
            flaky_tests=whitelist,
        )
        self.assertFalse(removed["regressions"])
        self.assertEqual("MISSING", removed["ignored_changes"][0]["after"])
        for fields in (
            {"class": "org.apache.iceberg.OtherTest"},
            {"name": "readOther()[1]"},
        ):
            base, current = report("PASSED_NATIVE"), report("FAILED")
            base["tests"][0].update(fields)
            current["tests"][0].update(fields)
            self.assertTrue(
                comparison.compare(base, current, flaky_tests=whitelist)["regressions"]
            )

    def test_whitelist_does_not_hide_invalid_report_structure(self):
        whitelist = {("org.apache.iceberg.TestScan", "read")}
        for invalid in (
            {"version": 1, "tests": report("FAILED")["tests"]},
            {"version": 2, "tests": []},
            {"version": 2, "tests": report("FAILED")["tests"] * 2},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                comparison.compare(invalid, report("FAILED"), flaky_tests=whitelist)

    def test_csv_whitelist_and_informational_changes_are_visible_in_gate_summary(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            base, current = report("PASSED_NATIVE"), report("FAILED")
            current["tests"][0].pop("failure_origin")
            base["tests"].extend(report("PASSED_FALLBACK", "anotherMethod")["tests"])
            current["tests"].extend(
                report("PASSED_UNVERIFIED", "anotherMethod")["tests"]
            )
            (root / "base.json").write_text(json.dumps(base))
            (root / "head.json").write_text(json.dumps(current))
            config = root / "flaky.csv"
            config.write_text(
                "# Known flaky test\nclass,method\norg.apache.iceberg.TestScan,read\n"
            )
            command = [
                sys.executable,
                str(SCRIPT),
                str(root / "base.json"),
                str(root / "head.json"),
                "--output",
                str(root / "diff.json"),
                "--flaky-tests",
                str(config),
            ]
            summary = root / "summary.md"
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                env=dict(os.environ, GITHUB_STEP_SUMMARY=str(summary)),
            )
            self.assertEqual(0, result.returncode, result.stderr + result.stdout)
            diagnostic = json.loads((root / "diff.json").read_text())
            self.assertFalse(diagnostic["regressions"])
            self.assertFalse(diagnostic["improvements"])
            self.assertEqual(1, len(diagnostic["ignored_changes"]))
            self.assertEqual(1, len(diagnostic["highlights"]))
            self.assertIn("Other changes (informational)", summary.read_text())
            self.assertIn("PASSED_FALLBACK → PASSED_UNVERIFIED", summary.read_text())
            self.assertIn("Whitelisted changes (ignored)", summary.read_text())
            self.assertIn("PASSED_NATIVE → FAILED", summary.read_text())
            with (root / "diff.csv").open(newline="") as output:
                rows = {row["test"]: row for row in csv.DictReader(output)}
            self.assertEqual("True", rows["read()[1]"]["ignored"])
            self.assertEqual("False", rows["anotherMethod"]["ignored"])
            for contents in ("class,method,reason\n", None):
                with self.subTest(contents=contents):
                    if contents is None:
                        config.unlink()
                    else:
                        config.write_text(contents)
                    result = subprocess.run(command, capture_output=True, text=True)
                    self.assertEqual(1, result.returncode, result.stderr)
                    diagnostic = json.loads((root / "diff.json").read_text())
                    self.assertTrue(diagnostic["validation_errors"])

    def test_gate_prints_every_change_in_logs_and_summary(self):
        base, current = report("PASSED_NATIVE"), report("PASSED_NATIVE")
        expected = []
        for method, before, after, reason in (
            ("regression", "PASSED_FALLBACK", "FAILED", "STATUS_REGRESSION"),
            ("improvement", "PASSED_FALLBACK", "PASSED_NATIVE", "STATUS_CHANGE"),
            ("informational", "PASSED_FALLBACK", "PASSED_FALLBACK", "EVIDENCE_CHANGE"),
            ("ignored", "PASSED_FALLBACK", "FAILED", "STATUS_REGRESSION"),
        ):
            for invocation in range(1, 22):
                name = f"{method}()[{invocation}]"
                base["tests"].extend(report(before, name)["tests"])
                current["tests"].extend(report(after, name)["tests"])
                if method == "informational":
                    current["tests"][-1]["fallback"] = ["IcebergScanTransformer"]
                expected.append(
                    f"- `org.apache.iceberg.TestScan#{name}`: {before} → {after} ({reason})"
                )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "base.json").write_text(json.dumps(base))
            (root / "head.json").write_text(json.dumps(current))
            config = root / "flaky.csv"
            config.write_text("class,method\norg.apache.iceberg.TestScan,ignored\n")
            summary = root / "summary.md"
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(root / "base.json"),
                    str(root / "head.json"),
                    "--output",
                    str(root / "diff.json"),
                    "--flaky-tests",
                    str(config),
                ],
                capture_output=True,
                text=True,
                env=dict(os.environ, GITHUB_STEP_SUMMARY=str(summary)),
            )
            self.assertEqual(1, result.returncode, result.stderr + result.stdout)
            for output in (result.stdout, summary.read_text()):
                self.assertIn("21 regressions, 21 improvements", output)
                self.assertIn("### Improvements: 21", output)
                self.assertEqual(
                    sorted(expected),
                    sorted(
                        line for line in output.splitlines() if line.startswith("- ")
                    ),
                )

    def test_new_tests_cannot_add_failures(self):
        current = report("PASSED_NATIVE")
        current["tests"].extend(report("FAILED", "new")["tests"])
        result = comparison.compare(report("PASSED_NATIVE"), current)
        self.assertEqual("NEW", result["regressions"][0]["before"])

    def test_empty_duplicate_and_broken_reports_fail_closed(self):
        duplicate = report("PASSED_NATIVE")
        duplicate["tests"] *= 2
        for invalid in (
            {"tests": []},
            duplicate,
            report("COVERAGE_ERROR"),
            report("UNKNOWN"),
        ):
            with self.assertRaises(ValueError):
                comparison.compare(invalid, report("PASSED_NATIVE"))
            with self.assertRaises(ValueError):
                comparison.compare(report("PASSED_NATIVE"), invalid)

    def test_parameter_changes_are_highlighted_except_fixture_ports(self):
        old, new = report("PASSED_NATIVE"), report("PASSED_NATIVE")
        old["tests"][0]["parameters"] = {
            "config": "uri=http://localhost:1234/",
            "vectorized": "true",
        }
        new["tests"][0]["parameters"] = {
            "config": "uri=http://localhost:5678/",
            "vectorized": "true",
        }
        self.assertFalse(comparison.compare(old, new)["changes"])
        new["tests"][0]["parameters"]["vectorized"] = "false"
        self.assertEqual(
            "PARAMETER_MISMATCH",
            comparison.compare(old, new)["highlights"][0]["reason"],
        )

    def test_random_uuid_literals_are_ignored_but_other_arguments_are_highlighted(self):
        old, new = report("PASSED_FALLBACK"), report("PASSED_FALLBACK")
        old["tests"][0][
            "display_name"
        ] = "[10] uuid, 6b311e8f-a06a-4faa-9d10-7de90a4c0d0a"
        new["tests"][0][
            "display_name"
        ] = "[10] uuid, f41c0f33-5800-453f-b4d5-0d77ca0d1915"
        self.assertFalse(comparison.compare(old, new)["changes"])
        new["tests"][0][
            "display_name"
        ] = "[10] string, f41c0f33-5800-453f-b4d5-0d77ca0d1915"
        self.assertFalse(comparison.compare(old, new)["regressions"])
        self.assertTrue(comparison.compare(old, new)["highlights"])

    def test_gate_rejects_unproven_native_and_unattributed_failures(self):
        invalid_native, invalid_failure = report("PASSED_NATIVE"), report("FAILED")
        invalid_native["tests"][0]["native_metrics"] = []
        invalid_failure["tests"][0]["failure_origin"] = "VANILLA_ALSO_FAILS"
        for invalid in (invalid_native, invalid_failure):
            with self.assertRaises(ValueError):
                comparison.compare(report("PASSED_NATIVE"), invalid)

    def test_change_csv_contains_only_changed_rows_and_preserves_messages(self):
        base, current = report("PASSED_NATIVE"), report("PASSED_NATIVE")
        base["tests"].extend(report("FAILED", "fixed")["tests"])
        current["tests"].extend(report("PASSED_FALLBACK", "fixed")["tests"])
        base["tests"][1][
            "failure"
        ] = 'message with a comma, a "quote"\nand another line'
        base["tests"][1]["failure_phase"] = "test"
        current["tests"][1]["unverified"] = ["IcebergScanTransformer"]
        result = comparison.compare(base, current)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "changes.csv"
            comparison.write_changes(path, result["changes"])
            with path.open(newline="") as output:
                rows = list(csv.DictReader(output))
        self.assertEqual(1, len(rows))
        self.assertEqual("fixed", rows[0]["test"])
        self.assertEqual(base["tests"][1]["failure"], rows[0]["failure_before"])
        self.assertEqual("test", rows[0]["failure_phase_before"])
        self.assertEqual("GLUTEN", rows[0]["failure_origin_before"])
        self.assertEqual(
            ["IcebergScanTransformer"], json.loads(rows[0]["unverified_after"])
        )

    def test_invalid_evidence_still_exports_diagnostics_and_cannot_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            before, after = report("PASSED_NATIVE"), report("COVERAGE_ERROR")
            (root / "base.json").write_text(json.dumps(before))
            (root / "head.json").write_text(json.dumps(after))
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(root / "base.json"),
                    str(root / "head.json"),
                    "--output",
                    str(root / "diff.json"),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(1, result.returncode)
            self.assertTrue(
                json.loads((root / "diff.json").read_text())["validation_errors"]
            )
            with (root / "diff.csv").open(newline="") as output:
                rows = list(csv.DictReader(output))
            self.assertEqual("PASSED_NATIVE", rows[0]["before"])
            self.assertEqual("COVERAGE_ERROR", rows[0]["after"])

    def test_missing_baseline_fails_but_preserves_head_and_exports_explicit_diagnostics(
        self,
    ):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            head = root / "head.json"
            head.write_text(json.dumps(report("PASSED_NATIVE")))
            original = head.read_bytes()
            (root / "baseline-unavailable.txt").write_text("Saved baseline expired")
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(root / "missing.json"),
                    str(head),
                    "--output",
                    str(root / "diff.json"),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(1, result.returncode)
            self.assertIn("comparison unavailable", result.stdout)
            self.assertNotIn("0 regressions", result.stdout)
            diagnostic = json.loads((root / "diff.json").read_text())
            self.assertFalse(diagnostic["comparison_performed"])
            self.assertEqual(1, diagnostic["current_tests"])
            self.assertIn("Saved baseline expired", diagnostic["validation_errors"])
            self.assertEqual(original, head.read_bytes())
            with (root / "diff.csv").open(newline="") as output:
                self.assertEqual([], list(csv.DictReader(output)))


if __name__ == "__main__":
    unittest.main()
