#!/usr/bin/env bash

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

# Run the upstream Spark 3.5 streaming sink suites with native writes and stats.
# Usage: run-delta-streaming-tests.sh <gluten_bundle_jar> <new_delta_directory>
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <gluten_bundle_jar> <new_delta_directory>" >&2
  exit 1
fi
if [ ! -f "$1" ]; then
  echo "Gluten bundle jar not found: $1" >&2
  exit 1
fi
if [ -e "$2" ]; then
  echo "Delta checkout destination already exists: $2" >&2
  exit 1
fi

UTIL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export GLUTEN_DELTA_STREAMING_BUNDLE="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
git clone --depth 1 --branch v3.3.2 https://github.com/delta-io/delta.git "$2"
DELTA_DIR="$(cd "$2" && pwd)"

# The general Delta runner patches v4.4.1 and uses spark-unified/. Delta 3.3.2
# has one spark/ project. Add startup assertions proving that both suites use
# Gluten's streaming sink, and unwrap that sink in the catalog metadata check.
python3 - "$DELTA_DIR" <<'PY'
from pathlib import Path
import sys

source = Path(sys.argv[1]) / "spark/src/test/scala/org/apache/spark/sql/delta/DeltaSinkSuite.scala"
text = source.read_text()
anchor = """      query.stop()
    }
  }

  protected def withTempDirs"""
replacement = """      try {
        // Verify the CI configuration without changing upstream test cases.
        assert(spark.version == "3.5.5")
        Seq(
          "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite",
          "spark.gluten.sql.columnar.backend.velox.delta.enableNativeStreamingWrite",
          "spark.databricks.delta.stats.collect"
        ).foreach { key => assert(spark.conf.get(key).toBoolean, s"$key must be enabled") }
        val sink = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sink
        assert(sink.getClass.getName == "org.apache.spark.sql.delta.sources.GlutenDeltaSink",
          s"Native streaming sink was not installed: ${sink.getClass.getName}")
      } finally {
        query.stop()
      }
    }
  }

  protected def withTempDirs"""
if text.count(anchor) != 1:
    raise SystemExit("DeltaSinkTest startup assertion anchor changed")
text = text.replace(anchor, replacement)
catalog_check = """    val WriteToMicroBatchDataSourceV1(catalogTable, sink: DeltaSink, _, _, _, _, _) = plan
    assert(catalogTable === sink.catalogTable)"""
wrapped_catalog_check = """    val WriteToMicroBatchDataSourceV1(catalogTable, sink, _, _, _, _, _) = plan
    val deltaSink = sink match {
      case wrapped: org.apache.spark.sql.delta.sources.GlutenDeltaSink => wrapped.delegate
      case original: DeltaSink => original
      case other => fail(s"Unexpected streaming sink: ${other.getClass.getName}")
    }
    assert(catalogTable === deltaSink.catalogTable)"""
if text.count(catalog_check) != 1:
    raise SystemExit("DeltaSinkSuite catalog metadata assertion changed")
source.write_text(text.replace(catalog_check, wrapped_catalog_check))
PY

# Keep Delta's extensions and catalog untouched. SparkConf reads these JVM
# defaults before each upstream suite creates its SparkSession. The implicit
# cast suite enables ANSI; disable ANSI fallback so it still tests native writes.
# shellcheck source=./java-test-args.sh
source "$UTIL_DIR/java-test-args.sh"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} \
-Dspark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true \
-Dspark.gluten.sql.columnar.backend.velox.delta.enableNativeStreamingWrite=true \
-Dspark.databricks.delta.stats.collect=true \
-Dspark.gluten.sql.ansiFallback.enabled=false"
export DELTA_TESTING=1
unset NUM_SHARDS SHARD_ID TEST_PARALLELISM_COUNT
cd "$DELTA_DIR"

# -DsparkVersion=3.5 selects Delta's 3.5 sources. Its version parser only accepts
# 3.5.3, so override the project setting afterward to resolve the same Spark
# 3.5.5 dependencies and test jars as Gluten. Restrict the bundle to Test scope
# to avoid exposing Gluten's Spark symbols while compiling Delta main sources.
./build/sbt -DsparkVersion=3.5 -J-Xmx4G \
  '++ 2.12.18' \
  'set spark / sparkVersion := "3.5.5"' \
  'set spark / Test / unmanagedJars += Attributed.blank(file(sys.env("GLUTEN_DELTA_STREAMING_BUNDLE")))' \
  'set spark / Test / javaOptions += "-Xmx2G"' \
  'set spark / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports")' \
  'spark/testOnly org.apache.spark.sql.delta.DeltaSinkSuite org.apache.spark.sql.delta.DeltaSinkImplicitCastSuite' \
  2>&1 | tee delta-streaming-tests.log

# testOnly can succeed when no suite matches. Require nonempty reports for both
# requested suites; no baseline or quarantine applies to this focused gate.
python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

expected = {
    "org.apache.spark.sql.delta.DeltaSinkSuite",
    "org.apache.spark.sql.delta.DeltaSinkImplicitCastSuite",
}
seen = set()
for report in Path("spark/target/test-reports").glob("*.xml"):
    root = ET.parse(report).getroot()
    for suite in root.iter("testsuite"):
        name = suite.get("name")
        if name not in expected:
            continue
        cases = list(suite.iter("testcase"))
        if not cases or not any(case.find("skipped") is None for case in cases):
            raise SystemExit(f"Suite did not execute tests: {name}")
        if any(case.find("failure") is not None or case.find("error") is not None
               for case in cases):
            raise SystemExit(f"Suite failed: {name}")
        seen.add(name)
if seen != expected:
    raise SystemExit(f"Missing streaming suite reports: {sorted(expected - seen)}")
print("Both upstream Delta streaming sink suites passed with native writes and stats enabled.")
PY
