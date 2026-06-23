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

#
# Prepares a delta-io/delta clone for running its `spark` module tests with the
# Gluten (Velox) bundle jar on the classpath.
#
# Usage:
#   setup-delta.sh <delta_ref> <delta_dir> <gluten_bundle_jar> <gluten_repo_root>
#
# Arguments:
#   delta_ref           - git ref (tag/branch/sha) to check out (e.g. v4.3.0)
#   delta_dir           - destination directory for the Delta clone
#   gluten_bundle_jar   - path to the gluten-velox-bundle fat jar
#   gluten_repo_root    - path to the Gluten repository root (used to locate
#                         backends-velox/src-delta40/.../DeltaSQLCommandTest.scala)
#

set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <delta_ref> <delta_dir> <gluten_bundle_jar> <gluten_repo_root>" >&2
  exit 1
fi

DELTA_REF="$1"
DELTA_DIR="$2"
GLUTEN_BUNDLE_JAR="$3"
GLUTEN_ROOT="$4"

if [ ! -f "$GLUTEN_BUNDLE_JAR" ]; then
  echo "Gluten bundle jar not found: $GLUTEN_BUNDLE_JAR" >&2
  exit 1
fi

# Reuse the existing DeltaSQLCommandTest from Gluten's backends-velox module
# rather than maintaining a separate copy. This file is compiled as part of the
# unified `spark` project's Test scope, which has the Gluten bundle on its
# classpath (via spark-unified/lib/), so the typed GlutenConfig / VeloxDeltaConfig
# imports resolve correctly.
PATCH_SOURCE="$GLUTEN_ROOT/backends-velox/src-delta40/test/scala/org/apache/spark/sql/delta/test/DeltaSQLCommandTest.scala"
if [ ! -f "$PATCH_SOURCE" ]; then
  echo "Gluten DeltaSQLCommandTest not found: $PATCH_SOURCE" >&2
  exit 1
fi

echo "::group::Cloning delta-io/delta @ ${DELTA_REF}"
# Shallow clone the requested tag/branch. Fall back to full clone when the ref is a SHA.
if ! git clone --depth 1 --branch "$DELTA_REF" https://github.com/delta-io/delta.git "$DELTA_DIR"; then
  echo "Shallow clone of ref '${DELTA_REF}' failed, falling back to full clone."
  rm -rf "$DELTA_DIR"
  git clone https://github.com/delta-io/delta.git "$DELTA_DIR"
  git -C "$DELTA_DIR" checkout "$DELTA_REF"
fi
git -C "$DELTA_DIR" --no-pager log -1 --oneline
echo "::endgroup::"

echo "::group::Injecting Gluten bundle jar onto the spark project's TEST classpath"
# The Gluten bundle jar must be on the spark project's TEST runtime classpath
# (so DeltaSQLCommandTest can load org.apache.gluten.GlutenPlugin by name) but
# NOT on the COMPILE classpath of `sparkV1`, which is the project that holds
# Delta's main sources. The bundle's transitive contents include extra symbols
# under `org.apache.spark.sql` that collide with Delta's main sources -- e.g.
# MergeOutputGeneration.scala imports both `org.apache.spark.sql._` and
# `org.apache.spark.sql.delta.ClassicColumnConversions._`, and would then fail
# with `reference to expression is ambiguous`.
#
# sbt auto-scans `<baseDirectory>/lib` via `unmanagedBase`. Two relevant
# projects in Delta v4.3.0 have a `lib/` baseDirectory:
#   - sparkV1: `project in file("spark")`     -> spark/lib
#   - spark  : `project in file("spark-unified")` -> spark-unified/lib
# unmanagedJars are project-scoped (NOT inherited by dependents), so dropping
# the bundle into spark-unified/lib/ adds it to the unified `spark` project's
# Compile *and* Test classpaths -- but NOT to sparkV1's. That's exactly what
# we want:
#   * sparkV1/Compile sees ONLY Delta's regular deps -> Delta main compiles.
#   * spark/Test/fullClasspath sees the bundle -> tests load GlutenPlugin.
# (Verified empirically: with bundle only in spark-unified/lib/, sbt's
#  `show sparkV1/Compile/dependencyClasspath` excludes the bundle and
#  `show spark/Test/fullClasspath` includes it.)
#
# We deliberately do NOT also drop the bundle into spark/lib/, which is what
# caused the previous compile failure: spark/lib/ is sparkV1's unmanagedBase,
# and putting the bundle there would re-introduce the ambiguity errors.
SPARK_UNIFIED_LIB="$DELTA_DIR/spark-unified/lib"
mkdir -p "$SPARK_UNIFIED_LIB"
cp "$GLUTEN_BUNDLE_JAR" "$SPARK_UNIFIED_LIB/gluten-velox-bundle.jar"
ls -lh "$SPARK_UNIFIED_LIB"
echo "::endgroup::"

echo "::group::Patching DeltaSQLCommandTest to enable Gluten plugin"
TARGET="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/test/DeltaSQLCommandTest.scala"
if [ ! -f "$TARGET" ]; then
  echo "Expected file not found in Delta clone: $TARGET" >&2
  echo "The Delta directory layout for ref '${DELTA_REF}' may have changed."
  exit 1
fi
cp "$PATCH_SOURCE" "$TARGET"
echo "Patched $TARGET"
echo "--- diff vs. upstream ---"
git -C "$DELTA_DIR" --no-pager diff -- "spark/src/test/scala/org/apache/spark/sql/delta/test/DeltaSQLCommandTest.scala" || true
echo "::endgroup::"

echo "::group::Disabling Delta scalastyle HeaderMatchesChecker"
# Our reused DeltaSQLCommandTest carries Gluten's ASF-only license header, which
# does not match Delta's HeaderMatchesChecker regex (the regex expects either a
# Delta copyright block, or the ASF header followed by a Spark-modifications
# block and the Delta copyright block). HeaderMatchesChecker is a file-level
# checker that does NOT honor `// scalastyle:off` directives, so we instead
# disable it globally in Delta's shared scalastyle-config.xml. The config is
# applied via `ThisBuild / scalastyleConfig` in project/Checkstyle.scala, so a
# single edit covers every sbt sub-project.
SCALASTYLE_CONFIG="$DELTA_DIR/scalastyle-config.xml"
if [ ! -f "$SCALASTYLE_CONFIG" ]; then
  echo "Expected scalastyle config not found: $SCALASTYLE_CONFIG" >&2
  exit 1
fi
sed -i \
  's|<check level="error" class="org.scalastyle.file.HeaderMatchesChecker" enabled="true">|<check level="error" class="org.scalastyle.file.HeaderMatchesChecker" enabled="false">|' \
  "$SCALASTYLE_CONFIG"
if ! grep -q '<check level="error" class="org.scalastyle.file.HeaderMatchesChecker" enabled="false">' "$SCALASTYLE_CONFIG"; then
  echo "Failed to disable HeaderMatchesChecker in $SCALASTYLE_CONFIG" >&2
  grep -n 'HeaderMatchesChecker' "$SCALASTYLE_CONFIG" >&2 || true
  exit 1
fi
echo "Disabled HeaderMatchesChecker in $SCALASTYLE_CONFIG"
echo "::endgroup::"
