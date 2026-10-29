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
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

SUPPORT = Path(__file__).resolve().parents[4] / ".github/workflows/util/delta-spark-ut"
SPEC = importlib.util.spec_from_file_location(
    "validation", SUPPORT / "check-gluten-validation.py"
)
validation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validation)


class ValidationTest(unittest.TestCase):
    suite = "org.example.SparkSuite"
    helper = "org.example.HelperSuite"
    name = "test with # and\nnewlines "

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.reports = self.root / "reports"
        self.reports.mkdir()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()
        self.exemptions = self.root / "exemptions.txt"
        self.exemptions.write_text("# none\n", encoding="utf-8")
        (self.evidence / "expected-suites.tsv").write_text(
            "required\t{}\nuncovered\t{}\ncanary\t{}\n".format(
                self.suite, self.helper, validation.CANARY
            ),
            encoding="utf-8",
        )
        (self.evidence / "expected-forks.txt").write_text("1\n", encoding="utf-8")
        self.rows = [
            self.row(
                "canary", validation.CANARY, validation.CANARY_TEST, status="passed"
            ),
            self.row(
                "test", self.suite, self.name, status="checked", checks=1, reason=""
            ),
        ]
        self.write_reports()
        self.write_evidence()

    def row(self, kind, suite, name, **values):
        return dict(version=1, pid="100", kind=kind, suite=suite, test=name, **values)

    def write_reports(self, failure=False, skipped=False, canary_failure=False):
        root = ET.Element("testsuites")
        for suite, name, failed, skip in (
            (self.suite, self.name, failure, skipped),
            (self.helper, "pure helper", False, False),
            (validation.CANARY, validation.CANARY_TEST, canary_failure, False),
        ):
            element = ET.SubElement(root, "testsuite", name=suite)
            test = ET.SubElement(element, "testcase", classname=suite, name=name)
            if failed:
                ET.SubElement(test, "failure", message="known failure")
            elif skip:
                ET.SubElement(test, "skipped")
        ET.ElementTree(root).write(self.reports / "TEST-results.xml", encoding="utf-8")

    def write_evidence(self):
        (self.evidence / "jvm-100.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in self.rows), encoding="utf-8"
        )

    def check(self, forks=1):
        return validation.validate(self.reports, self.evidence, self.exemptions, forks)

    def test_checked_tests_and_uncovered_suites_are_counted_separately(self):
        summary = self.check()
        self.assertEqual(1, summary["forks"])
        self.assertEqual(1, summary["checked"])
        self.assertEqual(0, summary["exempt"])
        self.assertEqual({self.helper: 1}, summary["uncovered"])

    def test_explicit_checks_can_cover_a_suite_outside_the_known_fixtures(self):
        self.rows.append(
            self.row(
                "test",
                self.helper,
                "pure helper",
                status="checked",
                checks=1,
                reason="",
            )
        )
        self.write_evidence()
        self.assertEqual({}, self.check()["uncovered"])

    def test_canaries_alone_cannot_make_an_empty_delta_run_pass(self):
        root = ET.Element("testsuite", name=validation.CANARY)
        ET.SubElement(
            root, "testcase", classname=validation.CANARY, name=validation.CANARY_TEST
        )
        ET.ElementTree(root).write(self.reports / "TEST-results.xml", encoding="utf-8")
        self.rows.pop()
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "No Delta tests executed"):
            self.check()

    def test_guard_success_does_not_change_ordinary_test_failures(self):
        self.write_reports(failure=True)
        self.assertEqual(1, self.check()["checked"])

    def test_missing_check_fails_even_for_an_already_failing_test(self):
        self.write_reports(failure=True)
        self.rows.pop()
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "missing required validation"):
            self.check()

    def test_missing_instrumentation_and_inventory_fail_closed(self):
        (self.evidence / "jvm-100.jsonl").unlink()
        with self.assertRaisesRegex(ValueError, "No JVM validation evidence"):
            self.check()
        self.write_evidence()
        (self.evidence / "expected-suites.tsv").unlink()
        with self.assertRaises(OSError):
            self.check()

    def test_every_requested_fork_requires_native_execution(self):
        (self.evidence / "expected-forks.txt").write_text("2\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Expected native proof from 2 JVMs"):
            self.check(2)
        self.rows.append(dict(self.rows[0], pid="101"))
        self.write_evidence()
        self.assertEqual(2, self.check(2)["forks"])

    def test_proof_from_a_different_jvm_cannot_cover_a_test(self):
        self.rows[1]["pid"] = "101"
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "No native proof for JVM 101"):
            self.check()

    def test_duplicate_native_records_cannot_replace_a_missing_fork(self):
        self.rows.append(dict(self.rows[0]))
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "Duplicate native canary"):
            self.check()

    def test_failed_canary_is_never_an_ordinary_baseline_failure(self):
        self.write_reports(canary_failure=True)
        with self.assertRaisesRegex(ValueError, "Validation harness test failed"):
            self.check()

    def test_caught_violation_is_still_fatal(self):
        self.rows.append(
            self.row(
                "violation",
                self.suite,
                self.name,
                phase="before",
                message="plugin missing",
            )
        )
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "plugin missing"):
            self.check()

    def test_exemption_requires_a_matching_reason_and_a_runtime_record(self):
        key = validation.results.format_entry(
            *validation.results.normalize_key(self.suite, self.name)
        )
        self.exemptions.write_text(key + "\tintentionally disabled\n", encoding="utf-8")
        self.rows[1].update(status="exempt", checks=0, reason="intentionally disabled")
        self.write_evidence()
        self.assertEqual(1, self.check()["exempt"])
        self.rows[1]["reason"] = "unreviewed reason"
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "Undeclared validation exemption"):
            self.check()
        self.rows.pop()
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "missing required validation"):
            self.check()

    def test_suite_exemptions_are_explicit_and_cannot_exempt_the_canary(self):
        self.exemptions.write_text(
            self.suite + "#*\tdeliberately vanilla\n", encoding="utf-8"
        )
        self.rows[1].update(status="exempt", checks=0, reason="deliberately vanilla")
        self.write_evidence()
        self.assertEqual(1, self.check()["exempt"])
        self.exemptions.write_text(validation.CANARY + "#*\tignore\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "infrastructure exemption"):
            self.check()

    def test_missing_or_malformed_exemption_reason_is_rejected(self):
        for text in (
            "Suite#test",
            "Suite#test\t",
            "*#test\treason",
            "Suite#test\tone\nSuite#test\ttwo",
        ):
            with self.subTest(text=text):
                self.exemptions.write_text(text, encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.check()

    def test_skipped_tests_do_not_require_a_guard_but_can_have_one(self):
        self.write_reports(skipped=True)
        self.assertEqual(0, self.check()["checked"])
        self.rows.pop()
        self.write_evidence()
        self.assertEqual(0, self.check()["checked"])

    def test_corrupt_or_truncated_evidence_is_rejected(self):
        for text in ('{"version":', json.dumps(self.rows[0]), "\n"):
            with self.subTest(text=text):
                (self.evidence / "jvm-100.jsonl").write_text(text, encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.check()

    def test_malformed_records_are_not_success_shaped_defaults(self):
        for field, value in (
            ("version", True),
            ("pid", ""),
            ("kind", "unknown"),
            ("test", None),
            ("status", "passed"),
            ("checks", 0),
        ):
            with self.subTest(field=field):
                original = self.rows[1][field]
                self.rows[1][field] = value
                self.write_evidence()
                with self.assertRaises(ValueError):
                    self.check()
                self.rows[1][field] = original

    def test_partial_inventory_is_rejected(self):
        (self.evidence / "expected-suites.tsv").write_text(
            "canary\t" + validation.CANARY + "\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "absent from the compiled inventory"):
            self.check()

    def test_incorrect_fork_plan_is_rejected(self):
        (self.evidence / "expected-forks.txt").write_text("2\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "disagrees with requested parallelism"):
            self.check()

    def test_aborted_required_suite_cannot_hide_missing_validation(self):
        root = ET.Element("testsuites")
        ET.SubElement(root, "testsuite", name=self.suite, errors="1")
        helper = ET.SubElement(root, "testsuite", name=self.helper)
        ET.SubElement(helper, "testcase", classname=self.helper, name="pure helper")
        canary = ET.SubElement(root, "testsuite", name=validation.CANARY)
        ET.SubElement(
            canary, "testcase", classname=validation.CANARY, name=validation.CANARY_TEST
        )
        ET.ElementTree(root).write(self.reports / "TEST-results.xml", encoding="utf-8")
        self.rows.pop()
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "missing required validation"):
            self.check()

    def test_duplicate_test_evidence_is_rejected(self):
        self.rows.append(dict(self.rows[1]))
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "Duplicate validation"):
            self.check()


if __name__ == "__main__":
    unittest.main()
