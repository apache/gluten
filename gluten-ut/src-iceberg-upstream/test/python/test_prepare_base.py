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
import xml.etree.ElementTree as ET

SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/iceberg-upstream/prepare-base.py"
)
SPEC = importlib.util.spec_from_file_location("prepare_base", SCRIPT)
prepare_base = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(prepare_base)


class PrepareBaseTest(unittest.TestCase):
    def test_bootstrap_copies_only_test_harness_and_preserves_base_production_code(
        self,
    ):
        with tempfile.TemporaryDirectory() as temporary:
            harness, base = Path(temporary) / "head", Path(temporary) / "base"
            for root in (harness, base):
                (root / "backends-velox").mkdir(parents=True)
            harness_pom = f"""<project xmlns="{prepare_base.NS}"><dependencies>
              <dependency><artifactId>head-production</artifactId></dependency></dependencies>
              <profiles><profile><id>iceberg-upstream-test</id><properties><observer>new</observer>
              </properties></profile><profile><id>spark-4.1</id><build><plugins><plugin>
              <artifactId>maven-surefire-plugin</artifactId><executions><execution>
              <id>iceberg-upstream</id><configuration><testRuntime>new</testRuntime></configuration>
              </execution></executions></plugin></plugins></build></profile></profiles></project>"""
            base_pom = f"""<project xmlns="{prepare_base.NS}"><dependencies>
              <dependency><artifactId>base-production</artifactId></dependency></dependencies>
              <profiles><profile><id>spark-4.1</id><properties><production>base</production></properties>
              <build><plugins><plugin><artifactId>maven-surefire-plugin</artifactId><executions>
              <execution><id>existing-test</id></execution>
              <execution><id>iceberg-upstream</id><obsolete>old runtime</obsolete></execution>
              </executions></plugin></plugins></build>
              </profile></profiles></project>"""
            (harness / "backends-velox/pom.xml").write_text(harness_pom)
            (base / "backends-velox/pom.xml").write_text(base_pom)
            (base / "pom.xml").write_text("unchanged base root pom")
            (base / "backends-velox/production.java").write_text("base implementation")
            for name in (
                "backends-velox/src-iceberg-upstream",
                "gluten-ut/src-iceberg-upstream",
            ):
                directory = harness / name
                directory.mkdir(parents=True)
                (directory / "observer.java").write_text("new observer")

            prepare_base.prepare(harness, base)
            # Re-applying the overlay must not accumulate duplicate profiles/executions.
            (base / "gluten-ut/src-iceberg-upstream/obsolete.java").write_text(
                "old observer"
            )
            prepare_base.prepare(harness, base)
            result = (base / "backends-velox/pom.xml").read_text()
            self.assertIn("base-production", result)
            self.assertNotIn("head-production", result)
            self.assertNotIn("old runtime", result)
            self.assertIn("<production>base</production>", result)
            self.assertEqual(1, result.count("<id>existing-test</id>"))
            self.assertEqual(1, result.count("<id>iceberg-upstream</id>"))
            self.assertEqual(1, result.count("<id>iceberg-upstream-test</id>"))
            self.assertEqual("unchanged base root pom", (base / "pom.xml").read_text())
            self.assertEqual(
                "base implementation",
                (base / "backends-velox/production.java").read_text(),
            )
            self.assertEqual(
                "new observer",
                (base / "gluten-ut/src-iceberg-upstream/observer.java").read_text(),
            )
            self.assertFalse(
                (base / "gluten-ut/src-iceberg-upstream/obsolete.java").exists()
            )
            ET.parse(base / "backends-velox/pom.xml")

    def test_refuses_to_overwrite_the_harness_checkout(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "different directories"):
                prepare_base.prepare(Path(temporary), Path(temporary))


if __name__ == "__main__":
    unittest.main()
