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
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/iceberg-upstream/summarize.py"
)
SPEC = importlib.util.spec_from_file_location("summary", SCRIPT)
summary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(summary)


class SummaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def reports(self, cases, evidence=()):
        suite = ET.Element("testsuite")
        for name, outcome in cases:
            case = ET.SubElement(
                suite, "testcase", classname="org.apache.iceberg.TestScan", name=name
            )
            if outcome:
                ET.SubElement(case, outcome, message="test result")
        ET.ElementTree(suite).write(self.directory / "TEST-scan.xml")
        rows = [
            dict(row, **{"class": "org.apache.iceberg.TestScan"}) for row in evidence
        ]
        (self.directory / "coverage-scan.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows)
        )
        return summary.classify(self.directory)

    def native(self, name):
        return {
            "name": name,
            "status": "PASSED_NATIVE",
            "body_started": True,
            "native": ["IcebergScanTransformer"],
            "native_metrics": [
                {"node": "IcebergScanTransformer", "metrics": {"rawInputRows": 2}}
            ],
        }

    def test_join_preserves_each_parameterized_invocation(self):
        rows = self.reports(
            [("scan()[1]", None), ("scan()[2]", None), ("ordinary", None)],
            [
                self.native("scan()[1]"),
                {"name": "scan()[2]", "status": "PASSED_FALLBACK"},
                {"name": "ordinary()", "status": "NO_ICEBERG_EXECUTION"},
            ],
        )
        self.assertEqual(
            ["PASSED_NATIVE", "PASSED_FALLBACK", "NO_ICEBERG_EXECUTION"],
            [r["status"] for r in rows],
        )

    def test_junit_failure_overrides_execution_and_missing_observation_is_broken_coverage(
        self,
    ):
        rows = self.reports(
            [("scan", "failure"), ("setup", "error"), ("skip", "skipped")],
            [
                {"name": "scan", "status": "PASSED_NATIVE"},
            ],
        )
        self.assertEqual(
            ["FAILED", "COVERAGE_ERROR", "SKIPPED"], [r["status"] for r in rows]
        )

    def test_missing_or_broken_observer_cannot_become_native_coverage(self):
        rows = self.reports(
            [("missing", None), ("broken", None), ("parser", None)],
            [
                {
                    "name": "broken",
                    "status": "PASSED_NATIVE",
                    "coverage_error": "listener failed",
                },
                {"name": "parser", "status": "NO_ICEBERG_EXECUTION"},
            ],
        )
        self.assertEqual(
            ["COVERAGE_ERROR", "COVERAGE_ERROR", "NO_ICEBERG_EXECUTION"],
            [r["status"] for r in rows],
        )

    def test_duplicate_evidence_and_empty_run_are_errors(self):
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.reports([("scan", None)], [{"name": "scan"}, {"name": "scan"}])
        with self.assertRaisesRegex(ValueError, "No upstream"):
            self.reports([])

    def test_native_label_needs_positive_executor_metrics_and_test_body(self):
        for change in (
            {"native_metrics": []},
            {"body_started": False},
            {"fallback": ["BatchScanExec"]},
            {"failed_queries": 1},
            {"expected_exception": True},
            {
                "native_metrics": [
                    {"node": "IcebergScanTransformer", "metrics": {"rawInputRows": 0}}
                ]
            },
        ):
            row = dict(self.native("scan"), **change)
            self.assertEqual(
                "COVERAGE_ERROR", self.reports([("scan", None)], [row])[0]["status"]
            )

    def test_csv_exports_match_json_and_preserve_diagnostic_text(self):
        rows = self.reports(
            [("native", None), ("fallback", None), ("failed", "failure")],
            [
                self.native("native"),
                {"name": "fallback", "status": "PASSED_FALLBACK"},
                {"name": "failed", "status": "FAILED"},
            ],
        )
        rows[-1]["failure"] = 'a comma, a "quote"\nand a newline'
        summary.write_reports(self.directory, rows)
        for filename, status in (
            ("native.csv", "PASSED_NATIVE"),
            ("fallback.csv", "PASSED_FALLBACK"),
            ("failed.csv", "FAILED"),
        ):
            with (self.directory / filename).open(newline="") as output:
                exported = list(csv.DictReader(output))
            self.assertEqual([status], [r["status"] for r in exported])
            if status == "FAILED":
                self.assertEqual(rows[-1]["failure"], exported[0]["error"])


if __name__ == "__main__":
    unittest.main()
