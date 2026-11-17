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

import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET

import yaml

ROOT = Path(__file__).resolve().parents[4]
ACTION = yaml.safe_load((ROOT / ".github/actions/cache-maven/action.yml").read_text())
VERSION_STEP = next(
    step for step in ACTION["runs"]["steps"] if step.get("id") == "maven"
)
CACHE_STEP = next(
    step
    for step in ACTION["runs"]["steps"]
    if step.get("uses", "").startswith("actions/cache@")
)


def read_version(pom):
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        (root / "pom.xml").write_text(pom)
        binary = root / "mvn"
        binary.write_text("#!/usr/bin/env bash\nexit 99\n")
        binary.chmod(0o755)
        output = root / "output"
        result = subprocess.run(
            [
                "bash",
                "--noprofile",
                "--norc",
                "-eo",
                "pipefail",
                "-c",
                VERSION_STEP["run"],
            ],
            cwd=root,
            env={
                **os.environ,
                "GITHUB_OUTPUT": str(output),
                "PATH": f"{root}{os.pathsep}{os.environ['PATH']}",
            },
            capture_output=True,
            text=True,
        )
        return result, output.read_text() if output.exists() else ""


def cache_key(version, os_name="Linux", arch="X64"):
    values = {
        "runner.os": os_name,
        "runner.arch": arch,
        "steps.maven.outputs.version": version,
    }
    return re.sub(
        r"\$\{\{\s*(.*?)\s*\}\}",
        lambda match: values[match.group(1)],
        CACHE_STEP["with"]["key"],
    )


class MavenDistributionCacheTest(unittest.TestCase):
    def test_reads_pinned_version_without_running_maven(self):
        pom = (ROOT / "pom.xml").read_text()
        version = ET.fromstring(pom).findtext("{*}properties/{*}maven.version")
        for content in (
            pom,
            pom.replace("<name>Gluten Parent Pom</name>", "<name>Other</name>"),
        ):
            with self.subTest(pom=content[:40]):
                result, output = read_version(content)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertEqual(f"version={version}\n", output)

        updated = "4.0.0-rc-3"
        result, output = read_version(
            pom.replace(
                f"<maven.version>{version}</maven.version>",
                f"<maven.version>{updated}</maven.version>",
            )
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(f"version={updated}\n", output)
        self.assertEqual(
            4,
            len(
                {
                    cache_key(version),
                    cache_key(updated),
                    cache_key(version, os_name="macOS"),
                    cache_key(version, arch="ARM64"),
                }
            ),
        )
        self.assertNotIn("restore-keys", CACHE_STEP["with"])
        self.assertEqual("build/.mvn", CACHE_STEP["with"]["path"])

    def test_missing_or_empty_version_fails(self):
        for pom in ("<project/>", "<project><maven.version></maven.version></project>"):
            with self.subTest(pom=pom):
                result, output = read_version(pom)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("::error::", result.stdout)
                self.assertEqual("", output)

    def test_every_direct_maven_job_restores_before_building(self):
        workflow = yaml.safe_load(
            (ROOT / ".github/workflows/velox_backend_x86.yml").read_text()
        )
        for name, job in workflow["jobs"].items():
            steps = job.get("steps", [])
            builds = [
                index
                for index, step in enumerate(steps)
                if re.search(r"\$MVN_CMD\b|\bbuild/mvn\s", step.get("run", ""))
            ]
            caches = [
                index
                for index, step in enumerate(steps)
                if step.get("uses") == "./.github/actions/cache-maven"
            ]
            with self.subTest(job=name):
                if not builds:
                    self.assertEqual([], caches)
                    continue
                self.assertEqual(1, len(caches))
                cache = caches[0]
                self.assertTrue(
                    steps[cache - 1]["uses"].startswith("actions/checkout@")
                )
                self.assertLess(cache, min(builds))
                self.assertNotIn("if", steps[cache])
                for index in builds:
                    self.assertNotIn("cache-hit", steps[index].get("if", ""))


if __name__ == "__main__":
    unittest.main()
