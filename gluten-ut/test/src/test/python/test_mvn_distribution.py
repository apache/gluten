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

import hashlib
import io
from itertools import groupby
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

WRAPPER = Path(__file__).resolve().parents[5] / "build" / "mvn"
VERSION = "3.9.123"
DISTRIBUTION = f"apache-maven-{VERSION}-bin.tar.gz"
RELEASE_PATH = f"maven/maven-3/{VERSION}/binaries/{DISTRIBUTION}"
CDN = f"https://dlcdn.apache.org/{RELEASE_PATH}"
CHECKSUM = f"https://downloads.apache.org/{RELEASE_PATH}.sha512"
ARCHIVE = f"https://archive.apache.org/dist/{RELEASE_PATH}"
ARCHIVE_CHECKSUM = ARCHIVE + ".sha512"
HASHERS = tuple(name for name in ("sha512sum", "shasum") if shutil.which(name))
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
if tool in ("tar", "sha512sum", "shasum"):
    os.execv(config[tool], [config[tool]] + sys.argv[1:])
if tool == "sleep":
    sys.exit(0)
if tool == "mvn":
    if sys.argv[1:] == ["--version"]:
        print("Apache Maven " + config["system_version"])
        sys.exit(0)
    sys.exit(config["maven_exit"])

# Limit flags to those supported by the older curl/wget used in CI.
if tool == "curl":
    options, urls = getopt.gnu_getopt(
        sys.argv[1:], "fLD:o:w:",
        ["retry=", "retry-delay=", "connect-timeout=", "max-time="])
    output = dict(options)["-o"]
else:
    options, urls = getopt.gnu_getopt(
        sys.argv[1:], "O:",
        ["tries=", "waitretry=", "connect-timeout=", "read-timeout=",
         "no-verbose", "server-response"])
    output = dict(options)["-O"]
if len(urls) != 1 or urls[0] not in config["urls"]:
    sys.exit("Unexpected Maven download URL: " + repr(urls))
url = urls[0]
http, status = config["failures"].get(url, ("200", 0))
if status:
    Path(output).write_text("partial download")
elif url.endswith(".sha512"):
    shutil.copyfile(root / "checksum", output)
elif config["corrupt_archive"]:
    Path(output).write_bytes((root / "distribution.tar.gz").read_bytes()[:64])
else:
    shutil.copyfile(root / "distribution.tar.gz", output)
headers = f"HTTP/1.1 {http} Response\r\n\r\n"
if tool == "curl":
    if "-D" in dict(options):
        sys.stdout.write(headers)
    if "-w" in dict(options):
        sys.stdout.write(dict(options)["-w"].replace("%{http_code}", http).replace("\\n", "\n"))
    if http.startswith(("4", "5")) and "-f" not in dict(options) and status == 22:
        status = 0
else:
    print(headers, file=sys.stderr)
sys.exit(status)
"""


@unittest.skipUnless(HASHERS, "sha512sum or shasum is required")
class MavenDistributionTest(unittest.TestCase):
    def run_wrapper(
        self,
        tool="curl",
        hasher=HASHERS[0] if HASHERS else None,
        failures=None,
        checksum_contents=None,
        corrupt_archive=False,
        cached_archive=None,
        system_version=None,
        local_install=False,
        maven_exit=0,
        maven_opts=None,
    ):
        temporary = tempfile.TemporaryDirectory(prefix="gluten maven distribution ")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        build = self.root / "build"
        build.mkdir()
        (build / "mvn").write_text(WRAPPER.read_text())
        (self.root / "pom.xml").write_text(
            f"<maven.version>{VERSION}</maven.version>\n"
        )
        self.downloads = build / ".mvn"
        self.archive = self.downloads / DISTRIBUTION
        self.checksum = self.downloads / (DISTRIBUTION + ".sha512")
        self.local_mvn = self.downloads / f"apache-maven-{VERSION}" / "bin" / "mvn"

        executable = f"#!{sys.executable}\n{MOCK}".encode()
        mock = self.root / "mock"
        mock.write_bytes(executable)
        mock.chmod(0o755)
        tools = self.root / "tools"
        tools.mkdir()
        for name in (
            "awk",
            "bash",
            "date",
            "dirname",
            "grep",
            "gzip",
            "head",
            "mkdir",
            "mv",
            "rm",
            "tr",
        ):
            (tools / name).symlink_to(shutil.which(name))
        for name in ("tar", "sleep", tool, hasher):
            if name:
                (tools / name).symlink_to(mock)
        if system_version is not None:
            (tools / "mvn").symlink_to(mock)
        if local_install:
            self.local_mvn.parent.mkdir(parents=True)
            self.local_mvn.write_bytes(executable)
            self.local_mvn.chmod(0o755)

        distribution = self.root / "distribution.tar.gz"
        with tarfile.open(distribution, "w:gz") as archive:
            entry = tarfile.TarInfo(f"apache-maven-{VERSION}/bin/mvn")
            entry.mode = 0o755
            entry.size = len(executable)
            archive.addfile(entry, io.BytesIO(executable))
        digest = hashlib.sha512(distribution.read_bytes()).hexdigest()
        (self.root / "checksum").write_text(
            digest if checksum_contents is None else checksum_contents
        )
        if cached_archive:
            self.downloads.mkdir(exist_ok=True)
            if cached_archive == "corrupt":
                self.archive.write_bytes(distribution.read_bytes()[:64])
            else:
                shutil.copyfile(distribution, self.archive)
            # A cached sidecar must not bypass fetching the official checksum.
            self.checksum.write_text(
                hashlib.sha512(self.archive.read_bytes()).hexdigest()
            )
        (self.root / "config.json").write_text(
            json.dumps(
                {
                    "urls": [CDN, CHECKSUM, ARCHIVE, ARCHIVE_CHECKSUM],
                    "failures": failures or {},
                    "corrupt_archive": corrupt_archive,
                    "system_version": system_version,
                    "maven_exit": maven_exit,
                    "tar": shutil.which("tar"),
                    "sha512sum": shutil.which("sha512sum"),
                    "shasum": shutil.which("shasum"),
                }
            )
        )
        env = dict(os.environ, PATH=str(tools), MVN_TEST_ROOT=str(self.root))
        env.pop("MAVEN_OPTS", None)
        if maven_opts is not None:
            env["MAVEN_OPTS"] = maven_opts
        result = subprocess.run(
            [shutil.which("bash"), str(build / "mvn"), "verify", "-Dmessage=hello world"],
            env=env,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
        )
        log = self.root / "calls.jsonl"
        self.log = (
            [json.loads(row) for row in log.read_text().splitlines()]
            if log.exists()
            else []
        )
        return result

    def calls(self, tool):
        return [call for call in self.log if call["tool"] == tool]

    def urls(self, tool):
        # Source ordering is independent of retries within a download.
        return [
            url for url, _ in groupby(call["args"][-1] for call in self.calls(tool))
        ]

    def assert_downloads_cleaned(self):
        for path in (self.archive, self.checksum):
            self.assertFalse(path.exists())
            self.assertFalse(Path(str(path) + ".part").exists())

    def assert_not_installed(self):
        self.assert_downloads_cleaned()
        self.assertFalse(self.local_mvn.exists())
        self.assertEqual([], self.calls("tar"))
        self.assertEqual([], self.calls("mvn"))

    def test_official_cdn_is_verified_before_extraction_with_either_hash_tool(self):
        for tool in ("curl", "wget"):
            for hasher in HASHERS:
                with self.subTest(tool=tool, hasher=hasher):
                    result = self.run_wrapper(tool, hasher)
                    self.assertEqual(0, result.returncode, result.stderr)
                    self.assertEqual([CDN, CHECKSUM], self.urls(tool))
                    tools = [call["tool"] for call in self.log]
                    self.assertLess(tools.index(hasher), tools.index("tar"))
                    self.assertTrue(self.local_mvn.is_file())
                    self.assert_downloads_cleaned()

    def test_missing_cdn_distribution_or_checksum_falls_back_to_archive(self):
        for tool, status in (("curl", 22), ("wget", 8)):
            for missing in (CDN, CHECKSUM):
                with self.subTest(tool=tool, missing=missing):
                    result = self.run_wrapper(
                        tool, failures={missing: ("404", status)}
                    )
                    self.assertEqual(0, result.returncode, result.stderr)
                    expected = [CDN] + ([CHECKSUM] if missing == CHECKSUM else [])
                    self.assertEqual(
                        expected + [ARCHIVE, ARCHIVE_CHECKSUM], self.urls(tool)
                    )
                    self.assertIn("trying the Apache archive", result.stderr)
                    self.assertEqual(1, len(self.calls("tar")))
                    self.assert_downloads_cleaned()

    def test_both_sources_unavailable_fail_without_extracting_partial_files(self):
        for tool, status in (("curl", 22), ("wget", 8)):
            for missing in ((CDN, ARCHIVE), (CHECKSUM, ARCHIVE_CHECKSUM)):
                with self.subTest(tool=tool, missing=missing):
                    result = self.run_wrapper(
                        tool, failures={url: ("404", status) for url in missing}
                    )
                    self.assertNotEqual(0, result.returncode)
                    expected = (
                        [CDN, ARCHIVE]
                        if CDN in missing
                        else [CDN, CHECKSUM, ARCHIVE, ARCHIVE_CHECKSUM]
                    )
                    self.assertEqual(expected, self.urls(tool))
                    self.assertIn("Failed to download Maven", result.stderr)
                    self.assert_not_installed()

    def test_incomplete_downloads_are_not_used(self):
        for tool, status in (("curl", 18), ("wget", 4)):
            with self.subTest(tool=tool):
                result = self.run_wrapper(
                    tool, failures={url: ("200", status) for url in (CDN, ARCHIVE)}
                )
                self.assertNotEqual(0, result.returncode)
                self.assert_not_installed()

    def test_truncated_archive_is_rejected_even_when_download_reports_success(self):
        for tool in ("curl", "wget"):
            with self.subTest(tool=tool):
                result = self.run_wrapper(tool, corrupt_archive=True)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("SHA-512 checksum verification failed", result.stderr)
                self.assertEqual([CDN, CHECKSUM], self.urls(tool))
                self.assert_not_installed()

    def test_bad_checksums_fail_closed_without_fallback(self):
        for hasher in HASHERS:
            for checksum in ("", "not a checksum\n", "a" * 127, "0" * 128):
                with self.subTest(hasher=hasher, checksum=checksum):
                    result = self.run_wrapper(
                        hasher=hasher, checksum_contents=checksum
                    )
                    self.assertNotEqual(0, result.returncode)
                    self.assertIn("ERROR:", result.stderr)
                    self.assertIn("SHA-512 checksum", result.stderr)
                    self.assertEqual([CDN, CHECKSUM], self.urls("curl"))
                    self.assert_not_installed()

    def test_archive_fallback_also_requires_a_matching_checksum(self):
        result = self.run_wrapper(
            failures={CDN: ("404", 22)}, checksum_contents="0" * 128
        )
        self.assertNotEqual(0, result.returncode)
        self.assertEqual([CDN, ARCHIVE, ARCHIVE_CHECKSUM], self.urls("curl"))
        self.assertIn("SHA-512 checksum verification failed", result.stderr)
        self.assert_not_installed()

    def test_cached_archives_are_verified_against_a_fresh_official_checksum(self):
        for cached in ("valid", "corrupt"):
            with self.subTest(cached=cached):
                result = self.run_wrapper(cached_archive=cached)
                self.assertEqual([CHECKSUM], self.urls("curl"))
                if cached == "valid":
                    self.assertEqual(0, result.returncode, result.stderr)
                    self.assertTrue(self.local_mvn.is_file())
                    self.assert_downloads_cleaned()
                else:
                    self.assertNotEqual(0, result.returncode)
                    self.assertIn("SHA-512 checksum verification failed", result.stderr)
                    self.assert_not_installed()

    def test_existing_installations_need_no_download_or_checksum_tools(self):
        for system_version in (VERSION, "9.9.9", None):
            with self.subTest(system_version=system_version):
                result = self.run_wrapper(
                    tool=None,
                    hasher=None,
                    system_version=system_version,
                    local_install=True,
                )
                self.assertEqual(0, result.returncode, result.stderr)
                expected = (
                    self.root / "tools" / "mvn"
                    if system_version == VERSION
                    else self.local_mvn
                )
                self.assertEqual(str(expected), self.calls("mvn")[-1]["path"])
                self.assertEqual([], self.calls("tar"))
                self.assert_downloads_cleaned()

    def test_pinned_version_arguments_options_and_exit_status_are_preserved(self):
        result = self.run_wrapper(
            system_version="9.9.9", maven_exit=17, maven_opts="-Xmx512m"
        )
        self.assertEqual(17, result.returncode, result.stderr)
        invocation = self.calls("mvn")[-1]
        self.assertEqual(str(self.local_mvn), invocation["path"])
        self.assertEqual(["verify", "-Dmessage=hello world"], invocation["args"])
        self.assertEqual("-Xmx512m", invocation["maven_opts"])

    def test_missing_download_or_checksum_tools_fail_explicitly(self):
        for tool, hasher, message in (
            (None, HASHERS[0], "Neither curl nor wget found"),
            ("curl", None, "Neither sha512sum nor shasum found"),
        ):
            with self.subTest(tool=tool, hasher=hasher):
                result = self.run_wrapper(tool, hasher)
                self.assertNotEqual(0, result.returncode)
                self.assertIn(message, result.stderr)
                self.assert_not_installed()


if __name__ == "__main__":
    unittest.main()
