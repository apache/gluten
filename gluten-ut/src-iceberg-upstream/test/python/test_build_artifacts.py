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
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/iceberg-upstream/build-artifacts.py"
)
SPEC = importlib.util.spec_from_file_location("build_artifacts", SCRIPT)
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


class BuildArtifactsTest(unittest.TestCase):
    def test_restores_native_classes_fixtures_and_only_freshly_installed_dependencies(
        self,
    ):
        with tempfile.TemporaryDirectory() as temporary, patch.object(
            build, "revision", return_value="base-sha"
        ):
            root = Path(temporary)
            checkout, repository = root / "build", root / "m2"
            for directory in build.CLASSES:
                target = checkout / directory
                target.mkdir(parents=True)
                (target / "native-or-fixture.bin").write_bytes(b"\x00native\xff")
            dependency = repository / "org/apache/gluten/gluten-core/1-SNAPSHOT"
            dependency.mkdir(parents=True)
            installed = []
            for name in ("core.jar", "core.pom", "core-tests.jar", "core-sources.jar"):
                path = dependency / name
                path.write_text(name)
                installed.append(f"[INFO] Installing /build/{name} to {path}")
            (dependency / "stale-other-profile.jar").write_text("wrong build")
            (dependency / "maven-metadata-local.xml").write_text("local snapshot")
            (dependency / "_remote.repositories").write_text("installed locally")
            log = root / "install.log"
            log.write_text("\n".join(installed))
            archive = root / "build.zip"
            build.pack(checkout, repository, archive, log)
            restored, restored_repo = root / "shard", root / "shard-m2"
            build.restore(restored, restored_repo, archive)
            for directory in build.CLASSES:
                self.assertEqual(
                    b"\x00native\xff",
                    (restored / directory / "native-or-fixture.bin").read_bytes(),
                )
            self.assertEqual(
                {
                    "core.jar",
                    "core.pom",
                    "core-tests.jar",
                    "maven-metadata-local.xml",
                    "_remote.repositories",
                },
                {path.name for path in restored_repo.rglob("*") if path.is_file()},
            )
            with patch.object(build, "revision", return_value="head-sha"):
                with self.assertRaisesRegex(ValueError, "checkout revision"):
                    build.restore(root / "other-shard", root / "other-m2", archive)
            self.assertFalse((root / "other-shard").exists())
            self.assertFalse((root / "other-m2").exists())

    def test_rejects_incomplete_builds(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            log = root / "install.log"
            log.write_text("[INFO] BUILD FAILURE")
            with self.assertRaisesRegex(ValueError, "Missing compiled classes"):
                build.pack(root, root, root / "build.zip", log)
            for directory in build.CLASSES:
                target = root / directory
                target.mkdir(parents=True)
                (target / "test.class").touch()
            with self.assertRaisesRegex(ValueError, "No installed Gluten"):
                build.pack(root, root, root / "build.zip", log)

    def test_rejects_paths_outside_the_restore_directory(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(
            build, "revision", return_value="head-sha"
        ):
            root = Path(temporary)
            archive = root / "build.zip"
            with zipfile.ZipFile(archive, "w") as output:
                output.writestr("revision", "head-sha")
                output.writestr("checkout/../outside", "unexpected")
            with self.assertRaises(ValueError):
                build.restore(root / "checkout", root / "m2", archive)
            self.assertFalse((root / "outside").exists())


if __name__ == "__main__":
    unittest.main()
