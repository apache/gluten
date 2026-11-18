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

import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

WRAPPER = Path(__file__).resolve().parents[5] / "build/mvn"
MOCK = r"""
import getopt
import os
from pathlib import Path
import shutil
import sys

root = Path(os.environ["MVN_TEST_ROOT"])
tool = Path(sys.argv[0]).name
with (root / "calls").open("a") as log:
    log.write(("sleep " + sys.argv[1] if tool == "sleep" else tool) + "\n")
if tool == "sleep":
    sys.exit(0)
if tool == "mvn":
    sys.exit(int(os.environ["MVN_TEST_EXIT"]))
if tool == "curl":
    options, urls = getopt.gnu_getopt(
        sys.argv[1:], "fLo:",
        ["retry=", "retry-delay=", "connect-timeout=", "max-time="])
    output = Path(dict(options)["-o"])
else:
    options, urls = getopt.gnu_getopt(
        sys.argv[1:], "O:",
        ["tries=", "waitretry=", "connect-timeout=", "read-timeout="])
    output = Path(dict(options)["-O"])
if len(urls) != 1 or not urls[0].endswith("apache-maven-0.0.1-bin.tar.gz"):
    sys.exit("Unexpected Maven distribution URL")
if output.exists():
    sys.exit("Partial file was not removed between attempts")
counter = root / "attempts"
attempt = int(counter.read_text()) + 1 if counter.exists() else 1
counter.write_text(str(attempt))
if attempt == int(os.environ["MVN_TEST_SUCCESS_AT"]):
    shutil.copyfile(root / "distribution.tar.gz", output)
    sys.exit(0)
output.write_text("partial download")
# Legacy clients return HTTP 429 immediately despite their native retry options.
print("HTTP 429: Too Many Requests", file=sys.stderr)
sys.exit((22 if "-f" in dict(options) else 0) if tool == "curl" else 8)
"""


class MavenBootstrapTest(unittest.TestCase):
    def run_wrapper(self, tool, success_at, maven_exit=0):
        temporary = tempfile.TemporaryDirectory(prefix="gluten-mvn-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        build = self.root / "build"
        build.mkdir()
        (build / "mvn").write_text(WRAPPER.read_text())
        (self.root / "pom.xml").write_text("<maven.version>0.0.1</maven.version>\n")
        executable = f"#!{sys.executable}\n{MOCK}".encode()
        mock = self.root / "mock"
        mock.write_bytes(executable)
        mock.chmod(0o755)
        tools = self.root / "tools"
        tools.mkdir()
        for name in (
            "awk",
            "bash",
            "dirname",
            "grep",
            "gzip",
            "head",
            "mkdir",
            "mv",
            "rm",
            "tar",
        ):
            (tools / name).symlink_to(shutil.which(name))
        for name in ("sleep", tool):
            (tools / name).symlink_to(mock)
        with tarfile.open(self.root / "distribution.tar.gz", "w:gz") as archive:
            entry = tarfile.TarInfo("apache-maven-0.0.1/bin/mvn")
            entry.mode = 0o755
            entry.size = len(executable)
            archive.addfile(entry, io.BytesIO(executable))
        env = dict(
            os.environ,
            PATH=str(tools),
            MVN_TEST_ROOT=str(self.root),
            MVN_TEST_SUCCESS_AT=str(success_at),
            MVN_TEST_EXIT=str(maven_exit),
        )
        return subprocess.run(
            [shutil.which("bash"), str(build / "mvn"), "verify"],
            env=env,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
        )

    def calls(self):
        return (self.root / "calls").read_text().splitlines()

    def test_legacy_rate_limit_retries_then_runs_maven(self):
        for tool in ("curl", "wget"):
            with self.subTest(tool=tool):
                result = self.run_wrapper(tool, success_at=2)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertEqual([tool, "sleep 20", tool, "mvn"], self.calls())
                self.assertEqual([], list((self.root / "build/.mvn").glob("*.tar.gz*")))

    def test_exhausted_retries_fail_and_remove_partial_downloads(self):
        for tool, status in (("curl", 22), ("wget", 8)):
            with self.subTest(tool=tool):
                result = self.run_wrapper(tool, success_at=0)
                self.assertNotEqual(0, result.returncode)
                self.assertEqual(
                    [tool, "sleep 20", tool, "sleep 40", tool], self.calls()
                )
                self.assertIn("ERROR: Failed to download Maven from ", result.stderr)
                self.assertIn(f"{tool} exit {status}", result.stderr)
                self.assertEqual([], list((self.root / "build/.mvn").glob("*.tar.gz*")))
                self.assertFalse((self.root / "build/.mvn/apache-maven-0.0.1").exists())

    def test_maven_failure_is_not_retried(self):
        for tool in ("curl", "wget"):
            with self.subTest(tool=tool):
                result = self.run_wrapper(tool, success_at=1, maven_exit=17)
                self.assertEqual(17, result.returncode, result.stderr)
                self.assertEqual([tool, "mvn"], self.calls())


if __name__ == "__main__":
    unittest.main()
