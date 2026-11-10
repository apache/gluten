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

import csv
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

SCRIPTS = (
    Path(__file__).resolve().parents[4] / ".github/workflows/util/iceberg-upstream"
)
sys.path.insert(0, str(SCRIPTS))
from compare import index
from control import attribute, selection
from flaky import is_flaky, load_flaky_tests


class FlakyTest(unittest.TestCase):
    def test_two_column_whitelist_matches_exact_methods_across_invocations(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "flaky.csv"
            path.write_text("# Confirmed flake\nclass,method\nexample.Test, read\n\n")
            tests = load_flaky_tests(path)
            self.assertEqual({("example.Test", "read")}, tests)
            for name in ("read", "read()", "read()[12]", "read(int)[1]"):
                self.assertTrue(
                    is_flaky({"class": "example.Test", "name": name}, tests)
                )
            for row in (
                {"class": "example.Other", "name": "read"},
                {"class": "example.Test", "name": "reader()"},
            ):
                self.assertFalse(is_flaky(row, tests))

    def test_invalid_whitelist_fails_configuration_validation(self):
        header, row = "class,method\n", "example.Test,read\n"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "flaky.csv"
            for contents in (
                "",
                "method,class\n",
                header + row + row,
                header + "example.Test, \n",
                header + "example.Test\n",
                header + "example.Test,read,extra\n",
                header + 'example.Test,"unclosed',
            ):
                with self.subTest(contents=contents):
                    path.write_text(contents)
                    with self.assertRaises((ValueError, csv.Error)):
                        load_flaky_tests(path)

    def test_controls_are_only_required_for_unlisted_failures(self):
        tests = {("example.Test", "flakyMethod")}
        ignored = {
            "class": "example.Test",
            "name": "flakyMethod()[1]",
            "status": "FAILED",
        }
        active = {
            "class": "example.Test",
            "name": "otherMethod()[1]",
            "status": "FAILED",
        }
        self.assertEqual(
            "example.Test#otherMethod", selection([ignored, active], tests)
        )
        attribute([ignored, active], [], tests)
        self.assertNotIn("failure_origin", ignored)
        self.assertEqual("CONTROL_MISSING", active["failure_origin"])

    def test_whitelisted_failures_survive_the_complete_reporting_pipeline(self):
        class_name, method = "org.apache.iceberg.TestScan", "flakyMethod"
        for status in ("FAILED", "COVERAGE_ERROR"):
            with self.subTest(
                status=status
            ), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                scripts = root / "harness"
                scripts.mkdir()
                for script in (
                    "compare.py",
                    "control.py",
                    "flaky.py",
                    "merge.py",
                    "summarize.py",
                ):
                    shutil.copy2(SCRIPTS / script, scripts / script)
                (scripts / "flaky-tests.csv").write_text(
                    f"class,method\n{class_name},{method}\n"
                )
                shard = root / "shard0"
                shard.mkdir()
                name = method + "()[2]"
                ignored = {
                    "class": class_name,
                    "name": name,
                    "status": "PASSED_FALLBACK",
                }
                active = {
                    "class": class_name,
                    "name": "nonFlakyMethod",
                    "status": "PASSED_FALLBACK",
                }
                suite = ET.Element("testsuite")
                case = ET.SubElement(suite, "testcase", classname=class_name, name=name)
                ET.SubElement(case, "failure", message="flaky assertion")
                ET.SubElement(
                    suite, "testcase", classname=class_name, name=active["name"]
                )
                ET.ElementTree(suite).write(shard / "TEST-queries.xml")
                # Missing per-test observation must also remain non-gating for this method.
                observed = [active] + ([ignored] if status == "FAILED" else [])
                (shard / "coverage-queries.jsonl").write_text(
                    "".join(json.dumps(row) + "\n" for row in observed)
                )
                (shard / "shard.json").write_text(json.dumps({"index": 0, "count": 1}))

                commands = [
                    ["summarize.py", str(shard)],
                    [
                        "control.py",
                        str(shard),
                        "--select",
                        str(shard / "selection.txt"),
                    ],
                    [
                        "control.py",
                        str(shard),
                        "--vanilla",
                        str(shard / "absent-vanilla"),
                    ],
                    ["merge.py", str(root), "--shards", "1"],
                ]
                for script, *arguments in commands:
                    result = subprocess.run(
                        [sys.executable, str(scripts / script), *arguments],
                        capture_output=True,
                        text=True,
                    )
                    self.assertEqual(
                        0, result.returncode, result.stderr + result.stdout
                    )
                self.assertEqual("", (shard / "selection.txt").read_text())
                report = json.loads((root / "execution-coverage.json").read_text())
                # Baseline record/restore uses the same validator.
                indexed = index(report, flaky_tests={(class_name, method)})
                outcome = indexed[class_name, name]
                self.assertEqual(status, outcome["status"])
                self.assertTrue(outcome["ignored"])
                self.assertEqual("flaky assertion", outcome["failure"])
                self.assertFalse(indexed[class_name, active["name"]]["ignored"])
                with (root / "failed.csv").open(newline="") as source:
                    rows = list(csv.DictReader(source))
                self.assertEqual("True", rows[0]["ignored"])

                base = dict(
                    ignored,
                    status="PASSED_NATIVE",
                    body_started=True,
                    native=["IcebergScanTransformer"],
                    native_metrics=[
                        {
                            "node": "IcebergScanTransformer",
                            "metrics": {"rawInputRows": 1},
                        }
                    ],
                )
                (root / "base.json").write_text(
                    json.dumps({"version": 2, "tests": [base, active]})
                )
                command = [
                    sys.executable,
                    str(scripts / "compare.py"),
                    str(root / "base.json"),
                    str(root / "execution-coverage.json"),
                    "--output",
                    str(root / "diff.json"),
                ]
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertEqual(0, result.returncode, result.stderr + result.stdout)
                diff = json.loads((root / "diff.json").read_text())
                self.assertFalse(diff["regressions"])
                self.assertFalse(diff["improvements"])
                self.assertEqual(status, diff["ignored_changes"][0]["after"])

                # The identical failure in an unlisted method must still fail validation.
                outcome["name"] = "notWhitelisted"
                (root / "execution-coverage.json").write_text(json.dumps(report))
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertEqual(1, result.returncode, result.stderr)
                self.assertTrue(
                    json.loads((root / "diff.json").read_text())["validation_errors"]
                )


if __name__ == "__main__":
    unittest.main()
