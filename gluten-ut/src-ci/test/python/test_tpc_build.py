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
import zipfile

SCRIPT = Path(__file__).resolve().parents[4] / ".github/workflows/util/tpc-build.py"
SPEC = importlib.util.spec_from_file_location("tpc_build", SCRIPT)
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


class TpcBuildTest(unittest.TestCase):
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
