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

from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / "cpp/CMake/AcquireVelox.cmake"


class VeloxSourceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.origin = self.root / "origin"
        self.origin.mkdir()
        (self.origin / "CMakeLists.txt").write_text("project(velox)\n")
        (self.origin / "scripts").mkdir()
        (self.origin / "scripts/setup-helper-functions.sh").write_text("# fixture\n")
        subprocess.run(["git", "init", "-b", "main", str(self.origin)],
                       check=True, stdout=subprocess.DEVNULL)
        self.git("add", ".")
        self.git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "commit", "-m", "fixture")

    def git(self, *args, repository=None):
        return subprocess.check_output(
            ["git", "-C", str(repository or self.origin), *args], text=True
        )

    def acquire(self, home=None):
        args = ["cmake", f"-DGLUTEN_HOME={self.root}",
                f"-DVELOX_REPO={self.origin}", "-DVELOX_BRANCH=main"]
        if home is not None:
            args.append(f"-DVELOX_HOME={home}")
        return subprocess.run([*args, "-P", str(MODULE)], text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    def test_default_missing_and_empty_directories(self):
        for home in (None, self.root / "missing", self.root / "empty"):
            with self.subTest(home=home):
                if home is not None and home.name == "empty":
                    home.mkdir()
                result = self.acquire(home)
                self.assertEqual(result.returncode, 0, result.stdout)
                target = home or self.root / "ep/build-velox/build/velox_ep"
                self.assertEqual(self.git("rev-parse", "HEAD", repository=target),
                                 self.git("rev-parse", "HEAD"))

    def test_existing_checkout_is_not_modified(self):
        home = self.root / "existing"
        subprocess.run(["git", "clone", str(self.origin), str(home)],
                       check=True, stdout=subprocess.DEVNULL)
        (home / "CMakeLists.txt").write_text("local changes\n")
        (home / "notes").write_text("untracked\n")
        before = self.git("show-ref", repository=home), self.git(
            "status", "--porcelain", repository=home)
        self.origin.rename(self.root / "offline-origin")
        result = self.acquire(home)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(before, (self.git("show-ref", repository=home),
                                  self.git("status", "--porcelain", repository=home)))
        self.assertEqual((home / "CMakeLists.txt").read_text(), "local changes\n")

    def test_invalid_nonempty_directory_and_file_are_preserved(self):
        for home in (self.root / "invalid", self.root / "hidden", self.root / "file"):
            with self.subTest(home=home):
                if home.name == "file":
                    home.write_text("keep\n")
                    sentinel = home
                else:
                    home.mkdir()
                    sentinel = home / (".keep" if home.name == "hidden" else "keep")
                    sentinel.write_text("keep\n")
                result = self.acquire(home)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("VELOX_HOME", result.stdout)
                self.assertEqual(sentinel.read_text(), "keep\n")

    def test_relative_directory_is_resolved_from_gluten_home(self):
        result = self.acquire("relative")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertTrue((self.root / "relative/CMakeLists.txt").is_file())

    def test_clone_failure_is_reported(self):
        self.origin.rename(self.root / "offline-origin")
        result = self.acquire(self.root / "missing")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Velox clone failed", result.stdout)


if __name__ == "__main__":
    unittest.main()
