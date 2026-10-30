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

import base64
from collections import defaultdict
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
        self.events = []
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
            self.events.append(
                self.event(
                    suite,
                    name,
                    "Failure" if failed else "Skipped" if skip else "Success",
                )
            )
        ET.ElementTree(root).write(self.reports / "TEST-results.xml", encoding="utf-8")
        self.write_executions()

    def event(
        self,
        suite,
        name,
        status="Success",
        kind="test",
        fingerprint="subclass:org.scalatest.Suite",
    ):
        return (kind, status, suite, name, fingerprint)

    @staticmethod
    def encoded(value):
        return base64.b64encode(value.encode("utf-8")).decode("ascii")

    def write_group(self, path, suite, events):
        failed = any(event[1] in {"Failure", "Error"} for event in events)
        lines = ["version\t1\t" + self.encoded(suite)]
        lines.extend(
            "\t".join(
                (
                    kind,
                    status,
                    self.encoded(owner),
                    self.encoded(name),
                    self.encoded(fingerprint),
                )
            )
            for kind, status, owner, name, fingerprint in events
        )
        lines.append(
            "complete\t{}\t{}".format("Failed" if failed else "Passed", len(events))
        )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def write_executions(self, forks=1):
        for path in self.evidence.glob("execution-*.tsv"):
            path.unlink()
        groups = defaultdict(list)
        for event in self.events:
            groups[event[2]].append(event)
        for index, (suite, events) in enumerate(groups.items()):
            copies = forks if suite == validation.CANARY else 1
            for copy in range(copies):
                self.write_group(
                    self.evidence / "execution-{}-{}.tsv".format(index, copy),
                    suite,
                    events,
                )

    def write_evidence(self, ensure_ascii=True):
        (self.evidence / "jvm-100.jsonl").write_text(
            "".join(
                json.dumps(row, ensure_ascii=ensure_ascii) + "\n" for row in self.rows
            ),
            encoding="utf-8",
        )

    def named_cases(self, names):
        self.events = [self.event(self.suite, name) for name in names] + [
            self.event(self.helper, "pure helper"),
            self.event(validation.CANARY, validation.CANARY_TEST),
        ]
        self.rows = [self.rows[0]] + [
            self.row("test", self.suite, name, status="checked", checks=1, reason="")
            for name in names
        ]
        (self.reports / "TEST-results.xml").unlink()
        for reporter in ("scalatest", "sbt"):
            root = ET.Element("testsuites")
            for _, status, suite, name, _ in self.events:
                # Both XML readers collapse literal attribute whitespace; sbt also strips prefixes.
                rendered = name.replace("\r", " ").replace("\n", " ").replace("\t", " ")
                if reporter == "sbt":
                    for prefix in (suite, suite.rsplit(".", 1)[-1]):
                        if rendered.startswith(prefix):
                            rendered = rendered[len(prefix) :]
                            if rendered.startswith("."):
                                rendered = rendered[1:]
                            break
                owner = ET.SubElement(root, "testsuite", name=suite)
                case = ET.SubElement(owner, "testcase", classname=suite, name=rendered)
                if status not in validation.EXECUTED:
                    ET.SubElement(case, "skipped")
            ET.ElementTree(root).write(
                self.reports / ("TEST-" + reporter + ".xml"), encoding="utf-8"
            )
        self.write_executions()
        self.write_evidence(ensure_ascii=False)

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
        self.events = [self.event(validation.CANARY, validation.CANARY_TEST)]
        self.write_executions()
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
        self.write_executions(forks=2)
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
        self.events = [
            self.event(self.suite, "", status="Error", kind="suite"),
            self.event(self.helper, "pure helper"),
            self.event(validation.CANARY, validation.CANARY_TEST),
        ]
        self.write_executions()
        with self.assertRaisesRegex(ValueError, "Required execution suite aborted"):
            self.check()

    def test_duplicate_test_evidence_is_rejected(self):
        self.rows.append(dict(self.rows[1]))
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "Duplicate validation"):
            self.check()

    def test_lossless_names_survive_both_junit_reporter_transformations(self):
        names = [
            "partition column names and values contain '{}'".format(char)
            for char in (" ", "\n", "\r", "\t")
        ] + [
            "SparkSuite with nested struct and mixed types",
            "org.example.SparkSuite.case",
            "case",
            " leading and trailing ",
            "Unicode\u2028separator",
        ]
        self.named_cases(names)
        self.assertEqual(len(names), self.check()["checked"])

    def test_one_whitespace_variant_cannot_cover_another_missing_check(self):
        self.named_cases(["column ' '", "column '\n'", "column '\t'"])
        self.rows.pop(2)
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "missing required validation") as error:
            self.check()
        self.assertIn(r"\n", str(error.exception))

    def test_duplicate_raw_execution_results_are_rejected(self):
        self.events.insert(0, self.events[0])
        self.write_executions()
        with self.assertRaisesRegex(ValueError, "Duplicate execution result"):
            self.check()

    def test_missing_execution_listener_is_fatal(self):
        for path in self.evidence.glob("execution-*.tsv"):
            path.unlink()
        with self.assertRaisesRegex(ValueError, "Missing lossless execution results"):
            self.check()

    def test_listener_errors_cannot_be_silently_discarded(self):
        (self.evidence / "execution-error-test.txt").write_text(
            "unsupported selector", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "listener failed"):
            self.check()

    def test_corrupt_execution_streams_fail_closed(self):
        path = self.evidence / "execution-0-0.tsv"
        original = path.read_text(encoding="utf-8")
        variants = [
            original.rstrip("\n"),
            original.replace("version\t1", "version\t2"),
            original.replace("complete\tPassed\t1", "complete\tPassed\t2"),
            original.replace("\tSuccess\t", "\tUnknown\t"),
            original.replace(self.encoded(self.name), "$invalid-base64"),
            original.replace(
                self.encoded("subclass:org.scalatest.Suite"), self.encoded("unknown")
            ),
            original.replace(self.encoded("subclass:org.scalatest.Suite"), ""),
        ]
        for text in variants:
            with self.subTest(text=text):
                path.write_text(text, encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.check()
        path.write_text(original, encoding="utf-8")

    def test_partial_raw_results_cannot_hide_missing_guard_evidence(self):
        self.named_cases(["first", "second"])
        self.events = [event for event in self.events if event[3] != "first"]
        self.rows = [row for row in self.rows if row["test"] != "first"]
        self.write_executions()
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "Execution/JUnit count mismatch"):
            self.check()

    def test_legacy_junit_method_prefix_is_not_applied_to_scalatest_names(self):
        self.name = "javaMethod"
        self.write_reports()
        self.rows[1]["test"] = self.name
        self.write_evidence()
        self.events[0] = self.event(
            self.suite,
            self.suite + "." + self.name,
            fingerprint="annotation:org.junit.Test",
        )
        self.write_executions()
        self.assertEqual(1, self.check()["checked"])
        self.events[0] = self.event(self.suite, self.suite + "." + self.name)
        self.write_executions()
        with self.assertRaisesRegex(ValueError, "missing required validation"):
            self.check()

    def test_ambiguous_exemption_does_not_exempt_distinct_raw_names(self):
        self.named_cases(["column ' '", "column '\n'"])
        self.exemptions.write_text(
            self.suite + "#column ' '\tintentionally disabled\n", encoding="utf-8"
        )
        for row in self.rows[1:]:
            row.update(status="exempt", checks=0, reason="intentionally disabled")
        self.write_evidence()
        with self.assertRaisesRegex(ValueError, "Ambiguous exemption"):
            self.check()


if __name__ == "__main__":
    unittest.main()
