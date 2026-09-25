#!/usr/bin/env python3
#
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
import unittest

REPO_ROOT = Path(__file__).resolve().parents[5]
JAVA_TEST_ARGS = REPO_ROOT / ".github/workflows/util/delta-spark-ut/java-test-args.sh"


class DeltaSparkTestArgsSuite(unittest.TestCase):
    def java_properties(self, existing_options=""):
        env = dict(os.environ, JAVA_TOOL_OPTIONS=existing_options)
        result = subprocess.run(
            [
                "bash",
                "-c",
                'set -euo pipefail; source "$1"; exec java -XshowSettings:properties -version',
                "bash",
                str(JAVA_TEST_ARGS),
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return dict(re.findall(r"^\s+(\S+) = (.*)$", result.stderr, re.MULTILINE))

    def test_gluten_defaults_reach_child_jvm_without_a_test_fixture(self):
        properties = self.java_properties()
        expected = {
            "spark.plugins": "org.apache.gluten.GlutenPlugin",
            "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
            "spark.memory.offHeap.enabled": "true",
            "spark.memory.offHeap.size": "2g",
            "spark.default.parallelism": "1",
            "spark.sql.shuffle.partitions": "5",
            "spark.unsafe.exceptionOnMemoryLeak": "true",
            "spark.sql.ansi.enabled": "false",
            "spark.gluten.sql.ansiFallback.enabled": "false",
            "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite": "true",
            "spark.databricks.delta.snapshotPartitions": "2",
            "spark.gluten.sql.fallbackUnexpectedMetadataParquet": "true",
        }
        for key, value in expected.items():
            with self.subTest(property=key):
                self.assertEqual(properties.get(key), value)

    def test_existing_jvm_options_and_arrow_settings_are_preserved(self):
        properties = self.java_properties("-Ddelta.test.marker=preserved")
        self.assertEqual(properties.get("delta.test.marker"), "preserved")
        self.assertEqual(properties.get("io.netty.tryReflectionSetAccessible"), "true")
        self.assertEqual(properties.get("file.encoding"), "UTF-8")

    def test_delta_extensions_and_catalog_remain_suite_specific(self):
        properties = self.java_properties()
        self.assertNotIn("spark.sql.extensions", properties)
        self.assertNotIn("spark.sql.catalog.spark_catalog", properties)


if __name__ == "__main__":
    unittest.main()
