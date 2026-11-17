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
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

WRAPPER = Path(__file__).resolve().parents[5] / "build/mvn"
VERSION = "0.0.1"
URL = (
    "https://repo1.maven.org/maven2/org/apache/maven/apache-maven/"
    f"{VERSION}/apache-maven-{VERSION}-bin.tar.gz"
)
MOCK = r"""
import getopt
import json
import os
from pathlib import Path
import shutil
import sys

root = Path(os.environ["MVN_TEST_ROOT"])
config = json.loads((root / "config.json").read_text())
tool = Path(sys.argv[0]).name
with (root / "calls.jsonl").open("a") as calls:
    calls.write(json.dumps({"tool": tool, "args": sys.argv[1:],
                            "path": sys.argv[0],
                            "maven_opts": os.environ.get("MAVEN_OPTS")}) + "\n")
if tool == "sleep":
    sys.exit(0)
if tool == "tar":
    os.execv(config["tar"], [config["tar"]] + sys.argv[1:])
if tool == "mvn":
    if sys.argv[1:] == ["--version"]:
        print("Apache Maven " + config["system_version"])
        sys.exit(0)
    print("Mock Maven invocation", file=sys.stderr)
    sys.exit(config["maven_exit"])

# Accept only options supported by old curl/wget, including the original wrapper's options.
if tool == "curl":
    options, urls = getopt.gnu_getopt(
        sys.argv[1:], "fLo:w:",
        ["retry=", "retry-delay=", "connect-timeout=", "max-time="])
    output = dict(options)["-o"]
else:
    options, urls = getopt.gnu_getopt(
        sys.argv[1:], "O:",
        ["tries=", "waitretry=", "connect-timeout=", "read-timeout=",
         "no-verbose", "server-response"])
    output = dict(options)["-O"]
if urls != [config["url"]]:
    sys.exit("Unexpected Maven download URL: " + repr(urls))
counter = root / "attempts"
attempt = int(counter.read_text()) + 1 if counter.exists() else 1
counter.write_text(str(attempt))
http, status = config["responses"][min(attempt - 1, len(config["responses"]) - 1)]
if status == 0:
    shutil.copyfile(root / "distribution.tar.gz", output)
else:
    Path(output).write_text("partial download, not a valid Maven archive")
    print(f"Mock {tool} failure: HTTP {http}, exit {status}", file=sys.stderr)
if tool == "curl":
    sys.stdout.write(http)
elif http != "000":
    print(f"  HTTP/1.1 302 Found\n  HTTP/1.1 {http} Response", file=sys.stderr)
sys.exit(status)
"""


class MavenBootstrapTest(unittest.TestCase):
    def run_wrapper(
        self,
        tool="curl",
        responses=(("200", 0),),
        system_version=None,
        local_install=False,
        cached_archive=False,
        maven_exit=0,
        maven_opts=None,
    ):
        temporary = tempfile.TemporaryDirectory(prefix="gluten-mvn-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        build = self.root / "build"
        build.mkdir()
        (build / "mvn").write_text(WRAPPER.read_text())
        (self.root / "pom.xml").write_text(
            f"<maven.version>{VERSION}</maven.version>\n"
        )
        self.downloads = build / ".mvn"
        self.archive = self.downloads / f"apache-maven-{VERSION}-bin.tar.gz"
        self.local_mvn = self.downloads / f"apache-maven-{VERSION}/bin/mvn"

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
        ):
            (tools / name).symlink_to(shutil.which(name))
        for name in ("sleep", "tar") + ((tool,) if tool else ()):
            (tools / name).symlink_to(mock)
        if system_version is not None:
            (tools / "mvn").symlink_to(mock)
        if local_install:
            self.local_mvn.parent.mkdir(parents=True)
            self.local_mvn.write_bytes(executable)
            self.local_mvn.chmod(0o755)

        with tarfile.open(self.root / "distribution.tar.gz", "w:gz") as archive:
            entry = tarfile.TarInfo(f"apache-maven-{VERSION}/bin/mvn")
            entry.mode = 0o755
            entry.size = len(executable)
            archive.addfile(entry, io.BytesIO(executable))
        if cached_archive:
            self.downloads.mkdir(exist_ok=True)
            shutil.copyfile(self.root / "distribution.tar.gz", self.archive)
        (self.root / "config.json").write_text(
            json.dumps(
                {
                    "responses": responses,
                    "system_version": system_version,
                    "maven_exit": maven_exit,
                    "tar": shutil.which("tar"),
                    "url": URL,
                }
            )
        )
        env = dict(os.environ, PATH=str(tools), MVN_TEST_ROOT=str(self.root))
        env.pop("MAVEN_OPTS", None)
        if maven_opts is not None:
            env["MAVEN_OPTS"] = maven_opts
        self.result = subprocess.run(
            [
                shutil.which("bash"),
                str(build / "mvn"),
                "verify",
                "-Dmessage=hello world",
            ],
            env=env,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
        )
        return self.result

    def calls(self, tool):
        log = self.root / "calls.jsonl"
        rows = (
            [json.loads(row) for row in log.read_text().splitlines()]
            if log.exists()
            else []
        )
        return [row for row in rows if row["tool"] == tool]

    def assert_no_distribution(self):
        self.assertFalse(self.archive.exists())
        self.assertFalse(Path(str(self.archive) + ".part").exists())
        self.assertFalse(self.local_mvn.exists())
        self.assertEqual([], self.calls("tar"))
        self.assertEqual([], self.calls("mvn"))

    def test_rate_limit_is_retried(self):
        for tool, status in (("curl", 22), ("wget", 8)):
            with self.subTest(tool=tool):
                result = self.run_wrapper(tool, (("429", status), ("200", 0)))
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertEqual(2, len(self.calls(tool)))
                self.assertEqual(
                    [["3"]], [call["args"] for call in self.calls("sleep")]
                )
                self.assertEqual(1, len(self.calls("tar")))
                self.assertTrue(self.local_mvn.is_file())
                self.assertFalse(self.archive.exists())
                self.assertFalse(Path(str(self.archive) + ".part").exists())

    def test_other_transient_http_errors_are_retried(self):
        for tool, status in (("curl", 22), ("wget", 8)):
            for http in ("408", "500", "502", "503", "504"):
                with self.subTest(tool=tool, http=http):
                    result = self.run_wrapper(tool, ((http, status), ("200", 0)))
                    self.assertEqual(0, result.returncode, result.stderr)
                    self.assertEqual(2, len(self.calls(tool)))

    def test_network_and_partial_download_failures_are_retried(self):
        for tool, errors in (
            ("curl", (5, 6, 7, 16, 18, 28, 35, 52, 55, 56, 92)),
            ("wget", (4,)),
        ):
            for status in errors:
                with self.subTest(tool=tool, status=status):
                    http = "200" if status == 18 else "000"
                    result = self.run_wrapper(tool, ((http, status), ("200", 0)))
                    self.assertEqual(0, result.returncode, result.stderr)
                    self.assertEqual(2, len(self.calls(tool)))
                    self.assertEqual(1, len(self.calls("tar")))

    def test_exhausted_retries_are_bounded_and_do_not_extract_partial_files(self):
        for tool, errors in (
            ("curl", (("429", 22), ("000", 28))),
            ("wget", (("429", 8), ("000", 4))),
        ):
            for response in errors:
                with self.subTest(tool=tool, response=response):
                    result = self.run_wrapper(tool, (response,))
                    self.assertNotEqual(0, result.returncode)
                    self.assertEqual(4, len(self.calls(tool)))
                    self.assertEqual(
                        [["3"], ["6"], ["12"]],
                        [call["args"] for call in self.calls("sleep")],
                    )
                    self.assertIn(
                        "ERROR: Failed to download Maven from " + URL, result.stderr
                    )
                    self.assertIn(f"{tool} exit {response[1]}", result.stderr)
                    self.assert_no_distribution()

    def test_permanent_http_tls_and_local_io_failures_are_not_retried(self):
        for tool, responses in (
            ("curl", (("404", 22), ("501", 22), ("000", 60), ("503", 23))),
            ("wget", (("404", 8), ("501", 8), ("000", 5), ("503", 3))),
        ):
            for response in responses:
                with self.subTest(tool=tool, response=response):
                    result = self.run_wrapper(tool, (response,))
                    self.assertNotEqual(0, result.returncode)
                    self.assertEqual(1, len(self.calls(tool)))
                    self.assertEqual([], self.calls("sleep"))
                    self.assertIn(f"Mock {tool} failure", result.stderr)
                    self.assert_no_distribution()

    def test_matching_system_maven_takes_precedence_over_local_install(self):
        result = self.run_wrapper(tool=None, system_version=VERSION, local_install=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(str(self.root / "tools/mvn"), self.calls("mvn")[-1]["path"])
        self.assertEqual([], self.calls("tar"))
        self.assertFalse(self.archive.exists())

    def test_local_install_is_used_when_system_maven_does_not_match(self):
        result = self.run_wrapper(tool=None, system_version="9.9.9", local_install=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(str(self.local_mvn), self.calls("mvn")[-1]["path"])
        self.assertEqual([], self.calls("tar"))

    def test_existing_archive_is_used_without_downloading(self):
        result = self.run_wrapper(tool=None, cached_archive=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(1, len(self.calls("tar")))
        self.assertFalse(self.archive.exists())

    def test_pinned_maven_and_arguments_opts_and_exit_status_are_preserved(self):
        result = self.run_wrapper(
            system_version="9.9.9", maven_exit=17, maven_opts="-Xmx512m"
        )
        self.assertEqual(17, result.returncode, result.stderr)
        self.assertEqual(1, len(self.calls("curl")))
        self.assertEqual(1, len(self.calls("tar")))
        invocation = self.calls("mvn")[-1]
        self.assertEqual(str(self.local_mvn), invocation["path"])
        self.assertEqual(["verify", "-Dmessage=hello world"], invocation["args"])
        self.assertEqual("-Xmx512m", invocation["maven_opts"])

    def test_missing_download_tools_fail_explicitly(self):
        result = self.run_wrapper(tool=None)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("ERROR: Neither curl nor wget found.", result.stderr)
        self.assert_no_distribution()


if __name__ == "__main__":
    unittest.main()
