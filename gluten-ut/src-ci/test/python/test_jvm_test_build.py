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
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

SCRIPT = (
    Path(__file__).resolve().parents[4] / ".github/workflows/util/jvm-test-build.py"
)
SPEC = importlib.util.spec_from_file_location("jvm_test_build", SCRIPT)
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


class JvmTestBuildTest(unittest.TestCase):
    def test_shards_cover_new_classes_once_and_keep_inner_classes_together(self):
        classes = ["example.SlowSuite", "example.FastSuite", "example.NewTest"]
        timings = {classes[0]: 90, classes[1]: 10, "example.RemovedSuite": 300}
        groups = build.partition(classes, 2, timings)
        self.assertEqual(groups, build.partition(reversed(classes), 2, timings))
        self.assertEqual([[classes[0]], sorted(classes[1:])], groups)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "build.zip"
            with zipfile.ZipFile(archive, "w") as output:
                output.writestr("manifest.json", json.dumps({"classes": classes}))
            selected = []
            for shard in range(2):
                filters = build.shard_filters(
                    archive, "unknown", shard, 2, root / str(shard)
                )
                pattern = re.compile(filters[0].split("=", 1)[1])
                excluded = Path(filters[1].split("=", 1)[1]).read_text().splitlines()
                current = [name for name in classes if pattern.fullmatch(name)]
                selected.extend(current)
                for name in classes:
                    self.assertEqual(
                        name in current, bool(pattern.fullmatch(name + "$Nested"))
                    )
                    self.assertEqual(
                        name not in current,
                        name.replace(".", "/") + ".class" in excluded,
                    )
                self.assertIn("**/*$*", excluded)
            self.assertCountEqual(classes, selected)
        with self.assertRaisesRegex(ValueError, "positive"):
            build.partition(classes, 0, timings)

    def test_failed_selection_keeps_reports_and_runs_remaining_selection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkout, reports = root / "checkout", root / "reports"
            target = checkout / "module/target"
            arguments = [
                "-P" + build.CONFIGURATIONS["spark35"],
                "-DargLine=-Dspark.test.home=/spark",
            ]
            calls = []

            def restore(*args):
                if target.exists():
                    build.shutil.rmtree(target)
                (target / "surefire-reports").mkdir(parents=True)

            def run(command, env):
                calls.append(command)
                self.assertIn("-Pci-reuse-test-build", command)
                self.assertIn(arguments[-1], command)
                report = target / "surefire-reports/TEST-suite.xml"
                self.assertFalse(
                    report.exists(), "A previous selection polluted the next one"
                )
                report.write_text("failed" if len(calls) == 1 else "passed")
                (target / "test.log").write_text("selection " + str(len(calls)))
                return 1 if len(calls) == 1 else 0

            with patch.object(build, "restore", side_effect=restore), patch.object(
                build.subprocess, "call", side_effect=run
            ):
                self.assertEqual(
                    1,
                    build.run_selections(
                        checkout, root / "build.zip", "spark35", arguments, reports
                    ),
                )
            self.assertEqual(2, len(calls))
            self.assertNotIn("-Dtest=__no_repeated_junit_tests__", calls[0])
            self.assertIn("-Dtest=__no_repeated_junit_tests__", calls[1])
            self.assertEqual(
                "failed",
                (
                    reports / "standard/module/target/surefire-reports/TEST-suite.xml"
                ).read_text(),
            )
            self.assertEqual(
                "passed",
                (
                    reports / "slow/module/target/surefire-reports/TEST-suite.xml"
                ).read_text(),
            )
            self.assertEqual(
                "selection 1", (reports / "standard/module/target/test.log").read_text()
            )

    def test_revision_check_accepts_only_the_requested_container_checkout(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(
            os.environ,
            {
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_CONFIG_COUNT": "0",
            },
        ):
            checkout = Path(temporary).resolve()
            subprocess.run(["git", "init", "-q", str(checkout)], check=True)
            git = ["git", "-C", str(checkout)]
            subprocess.run(
                git
                + [
                    "-c",
                    "user.name=CI test",
                    "-c",
                    "user.email=ci@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "--allow-empty",
                    "-qm",
                    "Test checkout",
                ],
                check=True,
            )
            revision = subprocess.check_output(
                git + ["rev-parse", "HEAD"], universal_newlines=True
            ).strip()
            with patch.dict(os.environ, {"GIT_TEST_ASSUME_DIFFERENT_OWNER": "1"}):
                for _ in range(2):
                    untrusted = subprocess.run(
                        git + ["rev-parse", "HEAD"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                    )
                    self.assertNotEqual(0, untrusted.returncode)
                    self.assertIn("dubious ownership", untrusted.stderr)
                    self.assertEqual(
                        revision, build.identity(checkout, "spark35")["revision"]
                    )

    def test_restore_preserves_executables_and_resets_mutated_fixtures_and_reports(
        self,
    ):
        with tempfile.TemporaryDirectory() as temporary, patch.object(
            build,
            "identity",
            return_value={
                "revision": "head",
                "configuration": "spark35",
                "architecture": "x86_64",
            },
        ):
            root = Path(temporary)
            producer, consumer = root / "producer", root / "consumer"
            target = producer / "module/target"
            target.mkdir(parents=True)
            (target / "test.class").write_bytes(b"compiled")
            classes = target / "test-classes/example"
            classes.mkdir(parents=True)
            (classes / "NewTest.class").write_bytes(b"test")
            (classes / "NewTest$Nested.class").write_bytes(b"nested")
            (target / "fixture").write_bytes(b"original")
            (target / "protoc").write_bytes(b"executable")
            (target / "protoc").chmod(0o755)
            pom = root / "effective.xml"
            pom.write_text(
                f'<projects xmlns="{build.NS["m"]}"><project><build><directory>{target}</directory>'
                f'<testOutputDirectory>{target / "test-classes"}</testOutputDirectory>'
                "</build></project></projects>"
            )
            archive = root / "build.zip"
            args = ["-Pspark-3.5,backends-velox", "-Dmaven.compiler.release=17"]
            build.pack(producer, archive, pom, "spark35", args)
            with zipfile.ZipFile(archive) as source:
                self.assertEqual(
                    ["example.NewTest"],
                    json.loads(source.read("manifest.json"))["classes"],
                )
            test_args = [
                "-Pbackends-velox",
                "-Pspark-3.5",
                "-Dmaven.compiler.release=17",
                "-DtagsToInclude=slow",
            ]
            build.restore(consumer, archive, "spark35", test_args)
            destination = consumer / "module/target"
            (destination / "fixture").write_bytes(b"mutated by first suite")
            outside = root / "outside-fixture"
            outside.write_text("must survive")
            (destination / "fixture").unlink()
            (destination / "fixture").symlink_to(outside)
            (destination / "stale-report.xml").touch()
            build.restore(consumer, archive, "spark35", test_args)
            self.assertEqual(b"original", (destination / "fixture").read_bytes())
            self.assertEqual(b"compiled", (destination / "test.class").read_bytes())
            self.assertFalse((destination / "stale-report.xml").exists())
            self.assertEqual("must survive", outside.read_text())
            self.assertEqual(0o755, (destination / "protoc").stat().st_mode & 0o777)
            for wrong in (
                args + ["-Pdelta"],
                ["-Pspark-3.5,backends-velox", "-Dmaven.compiler.release=11"],
            ):
                with self.assertRaisesRegex(ValueError, "profiles or compiler"):
                    build.restore(consumer, archive, "spark35", wrong)
            with patch.object(build, "identity", return_value={"revision": "other"}):
                with self.assertRaisesRegex(ValueError, "mismatch"):
                    build.restore(consumer, archive, "spark35", args)

    def test_invalid_archive_cannot_delete_sources(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(
            build, "identity", return_value={}
        ):
            root = Path(temporary)
            source = root / "src"
            source.mkdir()
            (source / "keep.java").touch()
            archive = root / "build.zip"
            with zipfile.ZipFile(archive, "w") as output:
                output.writestr(
                    "manifest.json",
                    json.dumps({"targets": ["src"], "build": build.signature([])}),
                )
            with self.assertRaisesRegex(ValueError, "Unexpected build directory"):
                build.restore(root, archive, "spark35", [])
            self.assertTrue((source / "keep.java").exists())

    def test_compiler_only_profile_keeps_both_test_runners_enabled(self):
        import xml.etree.ElementTree as ET

        pom = ET.parse(SCRIPT.parents[3] / "pom.xml").getroot()
        profile = pom.find("m:profiles/m:profile[m:id='ci-reuse-test-build']", build.NS)
        self.assertIsNotNone(profile)
        plugins = profile.findall(
            "m:build/m:pluginManagement/m:plugins/m:plugin", build.NS
        )
        self.assertEqual(
            {"maven-compiler-plugin", "scala-maven-plugin"},
            {p.findtext("m:artifactId", namespaces=build.NS) for p in plugins},
        )
        self.assertIsNone(profile.find("m:properties/m:maven.test.skip", build.NS))
        self.assertIsNone(profile.find("m:properties/m:skipTests", build.NS))


if __name__ == "__main__":
    unittest.main()
