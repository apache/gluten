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

SUPPORT = Path(__file__).resolve().parents[4] / ".github/workflows/util/delta-spark-ut"
SPEC = importlib.util.spec_from_file_location(
    "installation", SUPPORT / "install-validation.py"
)
installation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(installation)


class InstallationTest(unittest.TestCase):
    def test_import_alias_preserves_other_imports_and_test_strings(self):
        source = (
            "package example\n"
            "import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}\n"
            'class Example extends SharedSparkSession { val name = "SharedSparkSession" }\n'
        )
        result = installation.shared_imports(source)
        self.assertIn("import org.apache.spark.sql.test.{SQLTestUtils}", result)
        self.assertIn(installation.SHARED_IMPORT, result)
        self.assertIn('val name = "SharedSparkSession"', result)
        self.assertEqual(result, installation.shared_imports(result))

    def test_plain_import_is_aliased(self):
        source = "import org.apache.spark.sql.test.SharedSparkSession\nclass A\n"
        self.assertEqual(
            installation.SHARED_IMPORT + "\nclass A\n",
            installation.shared_imports(source),
        )

    def test_hook_preserves_self_type_and_is_idempotent(self):
        source = "trait A extends B { self: C =>\n  def method = 1\n}\n"
        result = installation.add_hook(source, "A")
        self.assertIn("with " + installation.PACKAGE + ".GlutenCheckedTest", result)
        self.assertIn("self: C =>", result)
        self.assertEqual(result, installation.add_hook(result, "A"))
        with self.assertRaises(ValueError):
            installation.add_hook(source, "Missing")

    def test_unexpected_source_context_fails_instead_of_skipping(self):
        with self.assertRaises(ValueError):
            installation.replace_once(
                "changed upstream", "expected", "patched", "fixture"
            )
        with self.assertRaises(ValueError):
            installation.replace_once(
                "expected expected", "expected", "patched", "fixture"
            )

    def test_complete_installation_covers_fixtures_factories_and_java(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)

            def write(relative, text):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")

            fixtures = "spark/src/test/scala/org/apache/spark/sql/delta/test/"
            write(
                fixtures + "DeltaSQLCommandTest.scala",
                "package example\nimport org.apache.spark.sql.test.SharedSparkSession\n"
                "trait DeltaSQLCommandTest extends SharedSparkSession {}\n",
            )
            write(
                fixtures + "DeltaHiveTest.scala",
                "package example\ntrait DeltaHiveTest extends SparkFunSuite { self: Other =>\n"
                "  override protected def spark: SparkSession = session\n}\n",
            )
            write(
                fixtures + "DeltaSessionQueryTest.scala",
                "package example\ntrait DeltaSessionQueryTest extends SessionQueryTest {\n"
                "  override protected def sparkConf: SparkConf = conf\n}\n",
            )
            factories = (SUPPORT / "validation-factories.txt").read_text(
                encoding="utf-8"
            )
            for name in factories.splitlines():
                if not name or name.startswith("#"):
                    continue
                package, simple = name.rsplit(".", 1)
                if simple == "SparkThreadLocalForwardingSuite":
                    body = "\n".join(
                        'val sc{} = SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local"))'.format(
                            index
                        )
                        for index in range(4)
                    )
                else:
                    body = "val spark = SparkSession.builder().getOrCreate()"
                write(
                    "spark/src/test/scala/" + name.replace(".", "/") + ".scala",
                    "package {}\nclass {} extends SparkFunSuite {{\n{}\n}}\n".format(
                        package, simple, body
                    ),
                )
            java = "spark/src/test/java/org/apache/spark/sql/delta/"
            write(
                java + "DeltaSQLCommandJavaTest.java",
                "public interface DeltaSQLCommandJavaTest {\n"
                "  default SparkSession buildSparkSession() {\n"
                "    return SparkSession.builder().getOrCreate();\n  }\n}\n",
            )
            write(
                java + "Example.java",
                "public class Example implements Serializable, DeltaSQLCommandJavaTest {}\n",
            )

            installation.install(root)
            first = {
                path.relative_to(root).as_posix(): path.read_bytes()
                for path in root.rglob("*")
                if path.is_file()
            }
            installation.install(root)
            second = {
                path.relative_to(root).as_posix(): path.read_bytes()
                for path in root.rglob("*")
                if path.is_file()
            }
            self.assertEqual(first, second)
            self.assertIn(b"GlutenValidationRule", first[java + "Example.java"])
            self.assertIn(
                b"public final transient org.junit.rules.TestRule",
                first[java + "Example.java"],
            )
            self.assertIn(
                b"checkedSession", first[java + "DeltaSQLCommandJavaTest.java"]
            )
            self.assertFalse((root / "gluten-validation.sbt").exists())
            self.assertTrue((root / "project/GlutenTestGrouping.scala").is_file())
            copied = root / "spark-unified/src/test/scala/org/apache/spark/sql/gluten"
            self.assertEqual(3, len(list(copied.glob("*.scala"))))
            self.assertIn(
                "import org.apache.spark.sql.test.SharedSparkSession",
                (copied / "GlutenActivationSuite.scala").read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
