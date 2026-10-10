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
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

SCRIPT = Path(__file__).resolve().parents[4] / ".github/workflows/util/tpc-build.py"
SPEC = importlib.util.spec_from_file_location("tpc_build", SCRIPT)
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


class TpcBuildTest(unittest.TestCase):
    def test_restore_in_a_rest_checkout_without_git_still_verifies_revision(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            producer, consumer = root / "producer", root / "consumer"
            library = producer / build.LIBRARY
            library.mkdir(parents=True)
            consumer.mkdir()
            for name in ("gluten-package-spark3.5.jar", "gluten-it-common-1.jar"):
                (library / name).write_bytes(name.encode())
            archive = root / "runtime.zip"
            revision = "a" * 40
            build.pack(
                producer,
                archive,
                dict(
                    revision=revision, spark="spark-3.5", java="java-8", shuffle="plain"
                ),
            )
            command = [
                sys.executable,
                str(SCRIPT),
                "restore",
                "--archive",
                str(archive),
                "--spark",
                "spark-3.5",
                "--java",
                "java-8",
            ]

            def run(arguments):
                return subprocess.run(
                    arguments,
                    cwd=consumer,
                    env=dict(os.environ, PATH=""),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )

            # actions/checkout uses a source archive in minimal containers with
            # no Git executable or .git directory. The default path reproduces CI.
            self.assertNotEqual(0, run(command).returncode)
            restored = run(command + ["--revision", revision])
            self.assertEqual(0, restored.returncode, restored.stderr)
            target = consumer / build.LIBRARY / "gluten-package-spark3.5.jar"
            self.assertEqual(target.read_bytes(), b"gluten-package-spark3.5.jar")
            for invalid in ("b" * 40, "head", ""):
                result = run(command + ["--revision", invalid])
                self.assertNotEqual(0, result.returncode)
                self.assertEqual(target.read_bytes(), b"gluten-package-spark3.5.jar")
            command[2] = "pack"
            result = run(command + ["--revision", revision])
            self.assertEqual(2, result.returncode)
            self.assertIn("pack verifies Git HEAD", result.stderr)

    def test_runtime_restores_without_stale_jars_and_rejects_wrong_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            producer, consumer = root / "producer", root / "consumer"
            library = producer / build.LIBRARY
            library.mkdir(parents=True)
            names = [
                "gluten-package-spark3.5.jar",
                "gluten-it-common-1.jar",
                "dependency.jar",
            ]
            for name in names:
                (library / name).write_bytes(name.encode())
            archive = root / "runtime.zip"
            identity = dict(
                revision="head", spark="spark-3.5", java="java-8", shuffle="plain"
            )
            build.pack(producer, archive, identity)
            build.restore(consumer, archive, identity)
            target = consumer / build.LIBRARY
            (target / "stale.jar").touch()
            build.restore(consumer, archive, identity)
            self.assertEqual(set(names), {path.name for path in target.iterdir()})
            for name in names:
                self.assertEqual(name.encode(), (target / name).read_bytes())
            for field, value in (
                ("revision", "base"),
                ("spark", "spark-4.1"),
                ("java", "java-17"),
                ("shuffle", "celeborn"),
            ):
                with self.assertRaisesRegex(ValueError, "mismatch"):
                    build.restore(consumer, archive, dict(identity, **{field: value}))
                self.assertTrue((target / names[0]).exists())

    def test_invalid_archive_cannot_replace_existing_runtime(self):
        import json

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            library = root / build.LIBRARY
            library.mkdir(parents=True)
            sentinel = library / "keep.jar"
            sentinel.write_text("current runtime")
            for name in (
                "../outside.jar",
                "/tmp/absolute.jar",
                "nested/file.jar",
                "file.class",
            ):
                archive = root / "bad.zip"
                with zipfile.ZipFile(archive, "w") as output:
                    output.writestr("manifest.json", json.dumps({}))
                    output.writestr(name, "bad")
                with self.assertRaisesRegex(ValueError, "Unexpected"):
                    build.restore(root, archive, {})
                self.assertEqual("current runtime", sentinel.read_text())
            with self.assertRaisesRegex(ValueError, "Missing current Gluten bundle"):
                build.pack(root, root / "incomplete.zip", {})


if __name__ == "__main__":
    unittest.main()
