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
#   delta_ref           - git ref (tag/branch/sha) to check out (e.g. v4.2.0)
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
# init + shallow fetch resolves a tag, branch OR commit SHA in a single path
# (`git clone --branch` rejects SHAs). Avoids a full-clone fallback and the
# destructive `rm -rf "$DELTA_DIR"` it required. `--` terminates options so a
# DELTA_REF starting with `-` can't be misread as a git flag (this script is
# workflow_dispatch-runnable with a user-supplied ref).
git init -q "$DELTA_DIR"
git -C "$DELTA_DIR" remote add origin https://github.com/delta-io/delta.git
git -C "$DELTA_DIR" fetch -q --depth 1 origin -- "$DELTA_REF"
git -C "$DELTA_DIR" checkout -q FETCH_HEAD
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
# projects in Delta v4.2.0 have a `lib/` baseDirectory:
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

# Delta's tests collect file-source scans by matching the concrete
# `FileSourceScanExec` case class; Gluten offloads the scan to
# DeltaScanTransformer, a `FileSourceScanLike` sibling, so those matches miss
# (`scala.MatchError: List()`, empty partition filters, broken column-pruning /
# scan-metric checks across many suites). delta-io/delta#7104 and #7105 widen the
# matches to the shared `FileSourceScanLike` interface that both the vanilla and
# Gluten scans implement (behavior-preserving for vanilla). Both are merged
# upstream but land after the pinned DELTA_REF (v4.2.0), so apply them here; once
# DELTA_REF includes a commit its cherry-pick is a clean no-op and the call can go.
#
# Depth-2 fetch brings each fix commit and its parent, which cherry-pick needs to
# diff against (a depth-1 fetch grafts the parent away); `-n` stages the change
# without requiring a committer identity.
cherry_pick_delta_fix() {
  local sha="$1" pr="$2"
  echo "Cherry-picking delta-io/delta${pr}"
  git -C "$DELTA_DIR" fetch --quiet --depth 2 origin "$sha"
  git -C "$DELTA_DIR" cherry-pick -n "$sha"
}

echo "::group::Cherry-picking upstream Delta FileSourceScanLike test fixes"
cherry_pick_delta_fix 46bd45d57eadd7e528002a0ae7bd36ce5a456eca "#7104 (ScanReportHelper.collectScans)"
cherry_pick_delta_fix 959e00e15f41f56afc1c9bb95d160c55c6dc7068 "#7105 (9 more test suites)"
echo "::endgroup::"

echo "::group::Force-failing memory-hog DeletionVectorsSuite 2B-row tests"
# Two DeletionVectorsSuite tests read from / delete from a 2-billion-row table.
# Under the Gluten Velox bundle they balloon the forked test JVM to ~13G of
# NATIVE memory (row-index materialization) and the kernel/cgroup OOM-kills it.
# The dead fork then wedges sbt, hanging the whole shard until the workflow's
# hang-watchdog dumps threads and kills it (~16 min wasted, and every suite
# QUEUED AFTER it in that fork is skipped) -- see delta_spark_ut.yml.
#
# Rather than silently `ignore` these (easy to forget), we make them FAIL FAST
# with a clear message: the gap stays visible in the test reports / baseline
# until the native memory blow-up is fixed, at which point this patch should be
# removed. NOTE: making the suite complete also un-skips the rest of the shard's
# suite queue, so the known-failures baseline must be refreshed after this.
#
# ORDER MATTERS: keep this sed AFTER the cherry-picks above. #7105 also edits
# DeletionVectorsSuite.scala, and git cherry-pick aborts (exit 128) when the work
# tree has uncommitted edits to a file it touches.
DVS="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala"
if [ ! -f "$DVS" ]; then
  echo "Expected file not found in Delta clone: $DVS" >&2
  echo "The Delta directory layout for ref '${DELTA_REF}' may have changed." >&2
  exit 1
fi
# Inject `fail(...)` as the first statement of each test body (the line ending
# in `) {`). Delta sets no -Xfatal-warnings / dead-code warning, so the now-
# unreachable original body compiles fine. Keep each injected line <100 chars:
# Delta's scalastyle enforces a 100-char line length on test sources. The full
# rationale lives in this comment, so the in-test message stays terse.
sed -i 's#huge table: read from tables of 2B rows with existing DV of many zeros") {#&\n    fail("[Gluten CI] Force-failed: 2B-row DV read OOMs the test JVM; see setup-delta.sh")#' "$DVS"
sed -i 's#number of rows from tables of 2B rows with DVs") {#&\n      fail("[Gluten CI] Force-failed: 2B-row DV delete OOMs the test JVM; see setup-delta.sh")#' "$DVS"
INJECTED=$(grep -c "Gluten CI] Force-failed" "$DVS" || true)
if [ "$INJECTED" -ne 2 ]; then
  echo "ERROR: expected to force-fail 2 DeletionVectorsSuite tests but injected ${INJECTED}." >&2
  echo "Their test names likely changed in Delta ref '${DELTA_REF}'; update setup-delta.sh." >&2
  exit 1
fi
echo "Force-failed 2 DeletionVectorsSuite 2B-row tests (read + delete)."
git -C "$DELTA_DIR" --no-pager diff -- "spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala" || true
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
