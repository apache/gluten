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
import csv
import sys
import json
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

    def test_native_fallback_failure_transition_rules(self):
        allowed = {
            ("FAILED", "FAILED"),
            ("FAILED", "PASSED_FALLBACK"),
            ("FAILED", "PASSED_NATIVE"),
            ("PASSED_FALLBACK", "PASSED_FALLBACK"),
            ("PASSED_FALLBACK", "PASSED_NATIVE"),
            ("PASSED_NATIVE", "PASSED_NATIVE"),
        }
        for old in ("FAILED", "PASSED_FALLBACK", "PASSED_NATIVE"):
            for new in ("FAILED", "PASSED_FALLBACK", "PASSED_NATIVE"):
                with self.subTest(before=old, after=new):
                    result = comparison.compare(report(old), report(new))
                    self.assertEqual(
                        (old, new) not in allowed, bool(result["regressions"])
                    )

    def test_disabling_or_removing_tests_does_not_hide_regressions(self):
        for old in (
            "FAILED",
            "PASSED_FALLBACK",
            "PASSED_NATIVE",
            "NO_ICEBERG_EXECUTION",
        ):
            with self.subTest(before=old):
                self.assertTrue(
                    comparison.compare(report(old), report("SKIPPED"))["regressions"]
                )
                result = comparison.compare(
                    report(old), report("PASSED_NATIVE", "different")
                )
                self.assertEqual("MISSING", result["regressions"][0]["after"])

    def test_losing_execution_evidence_is_not_an_improvement(self):
        for old in ("PASSED_FALLBACK", "PASSED_NATIVE"):
            for new in ("NO_ICEBERG_EXECUTION", "PASSED_UNVERIFIED"):
                result = comparison.compare(report(old), report(new))
                self.assertTrue(result["regressions"])

    def test_new_tests_and_previously_skipped_tests_cannot_add_failures(self):
        current = report("PASSED_NATIVE")
        current["tests"].extend(report("FAILED", "new")["tests"])
        result = comparison.compare(report("PASSED_NATIVE"), current)
        self.assertEqual("NEW", result["regressions"][0]["before"])
        self.assertTrue(
            comparison.compare(report("SKIPPED"), report("FAILED"))["regressions"]
        )
        self.assertFalse(
            comparison.compare(report("SKIPPED"), report("PASSED_NATIVE"))[
                "regressions"
            ]
        )

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

    def test_parameters_must_match_except_fixture_ports(self):
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
            comparison.compare(old, new)["regressions"][0]["reason"],
        )

    def test_random_uuid_literal_is_data_but_other_arguments_must_match(self):
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
        self.assertTrue(comparison.compare(old, new)["regressions"])

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
