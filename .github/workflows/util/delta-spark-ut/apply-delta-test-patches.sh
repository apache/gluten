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
# Applies temporary Gluten compatibility patches to a delta-io/delta checkout.
#
# Usage:
#   apply-delta-test-patches.sh <delta_ref> <delta_dir>
#
# Remove each patch group when DELTA_REF contains the corresponding upstream
# fix or Gluten no longer needs the workaround.
#

set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <delta_ref> <delta_dir>" >&2
  exit 1
fi

DELTA_REF="$1"
DELTA_DIR="$2"

# Delta's tests collect file-source scans by matching the concrete
# `FileSourceScanExec` case class; Gluten offloads the scan to
# DeltaScanTransformer, a `FileSourceScanLike` sibling, so those matches miss
# (`scala.MatchError: List()`, empty partition filters, broken column-pruning /
# scan-metric checks across many suites). delta-io/delta#7104 and #7105 widen the
# matches to the shared `FileSourceScanLike` interface that both the vanilla and
# Gluten scans implement (behavior-preserving for vanilla). Both are merged
# upstream but land after the pinned DELTA_REF (v4.2.0), so apply them here; once
# DELTA_REF includes a fix, cherry_pick_delta_fix detects it and skips (see below).
#
# Depth-2 fetch brings each fix commit and its parent, which cherry-pick needs to
# diff against (a depth-1 fetch grafts the parent away); `-n` stages the change
# without requiring a committer identity.
cherry_pick_delta_fix() {
  local sha="$1" pr="$2"
  git -C "$DELTA_DIR" fetch --quiet --depth 2 origin "$sha"
  echo "Cherry-picking delta-io/delta${pr}"
  if git -C "$DELTA_DIR" cherry-pick -n "$sha"; then
    return 0
  fi
  # The cherry-pick did not apply. The usual cause is that the pinned DELTA_REF
  # already contains this fix (e.g. after a version bump), which makes the patch
  # empty/conflicting and would -- under `set -e` -- abort the whole setup. We
  # can't use ancestry to tell "already contained" from a genuine conflict here
  # (the clone is shallow, so `merge-base --is-ancestor` can't see past the graft),
  # so recover the exact paths this fix touches -- leaving other setup such as the
  # DeltaSQLCommandTest patch intact -- and continue. This is self-correcting: if
  # the fix is genuinely still needed, the FileSourceScanLike failures it prevents
  # resurface as gate regressions rather than being hidden by a hard abort here.
  echo "Cherry-pick of delta-io/delta${pr} did not apply cleanly" \
    "(most likely already contained in ${DELTA_REF}); skipping it."
  local f
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    git -C "$DELTA_DIR" reset -q -- "$f" 2>/dev/null || true
    git -C "$DELTA_DIR" checkout -q -- "$f" 2>/dev/null || true
  done < <(git -C "$DELTA_DIR" diff-tree --no-commit-id --name-only -r "$sha")
  # Clear any leftover sequencer state (harmless if none exists).
  git -C "$DELTA_DIR" cherry-pick --quit 2>/dev/null || true
  return 0
}

echo "::group::Cherry-picking upstream Delta FileSourceScanLike test fixes"
cherry_pick_delta_fix 46bd45d57eadd7e528002a0ae7bd36ce5a456eca "#7104 (ScanReportHelper.collectScans)"
cherry_pick_delta_fix 959e00e15f41f56afc1c9bb95d160c55c6dc7068 "#7105 (9 more test suites)"
echo "::endgroup::"

echo "::group::Capping DeltaParquetFileFormat fixture row groups by row count"
# DeltaParquetFileFormatSuite generates one 20,000-row Parquet file and sets a
# 50 KiB block size to ensure that it contains multiple row groups. Velox sizes
# row groups by buffered bytes after writing each input batch. Because this
# fixture arrives in one batch, lowering the byte threshold cannot split it.
# Scope Gluten's native row-count limit around the fixture write so Arrow splits
# the 20,000 rows deterministically while keeping the native write path enabled.
DPFFS="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/DeltaParquetFileFormatSuite.scala"
if [ ! -f "$DPFFS" ]; then
  echo "Expected file not found in Delta clone: $DPFFS" >&2
  echo "The Delta directory layout for ref '${DELTA_REF}' may have changed." >&2
  exit 1
fi
if ! sed 's/^__BLANK_CONTEXT__$/ /' <<'PATCH' | git -C "$DELTA_DIR" apply -
diff --git a/spark/src/test/scala/org/apache/spark/sql/delta/DeltaParquetFileFormatSuite.scala b/spark/src/test/scala/org/apache/spark/sql/delta/DeltaParquetFileFormatSuite.scala
--- a/spark/src/test/scala/org/apache/spark/sql/delta/DeltaParquetFileFormatSuite.scala
+++ b/spark/src/test/scala/org/apache/spark/sql/delta/DeltaParquetFileFormatSuite.scala
@@ -68,9 +68,11 @@ trait DeltaParquetFileFormatSuiteBase
   protected def generateData(tablePath: String): Unit = {
     // This is to generate a Parquet file with two row groups
     hadoopConf().set("parquet.block.size", (1024 * 50).toString)
__BLANK_CONTEXT__
     // Keep the number of partitions to 1 to generate a single Parquet data file
     val df = Seq.range(0, 20000).toDF().repartition(1)
-    df.write.format("delta").mode("append").save(tablePath)
+    withSQLConf("spark.gluten.sql.native.parquet.write.blockRows" -> "10000") {
+      df.write.format("delta").mode("append").save(tablePath)
+    }
__BLANK_CONTEXT__
     // Set DFS block size to be less than Parquet rowgroup size, to allow
PATCH
then
  echo "ERROR: DeltaParquetFileFormat fixture patch did not apply." >&2
  echo "The patch expects the Delta v4.2.0 generateData fixture shape;" \
    "ref '${DELTA_REF}' must remain source-compatible." >&2
  exit 1
fi
ROW_CAP_SCOPES=$(
  grep -Fxc \
    '    withSQLConf("spark.gluten.sql.native.parquet.write.blockRows" -> "10000") {' \
    "$DPFFS" || true
)
if [ "$ROW_CAP_SCOPES" -ne 1 ]; then
  echo "ERROR: expected exactly one native Parquet row-count scope;" \
    "found ${ROW_CAP_SCOPES}." >&2
  echo "DeltaParquetFileFormatSuite may have changed in Delta ref '${DELTA_REF}'." >&2
  exit 1
fi
echo "Capped DeltaParquetFileFormat fixture row groups at 10,000 rows."
git -C "$DELTA_DIR" --no-pager diff -- \
  "spark/src/test/scala/org/apache/spark/sql/delta/DeltaParquetFileFormatSuite.scala" || true
echo "::endgroup::"

echo "::group::Capping predicate-pushdown DV fixture row groups by row count"
# DeletionVectorsWithPredicatePushdownSuite writes one 1,000,000-row Parquet
# file and expects its 2 MiB block size to produce two row groups. The rows can
# arrive at Velox in one Arrow batch, so the native writer cannot evaluate its
# buffered-byte flush threshold until the whole batch is already in one group.
# Cap this fixture at 500,000 rows per group so Arrow splits the batch while
# retaining the native write path and the existing Hadoop block-size setting.
DV_SUITE="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala"
if [ ! -f "$DV_SUITE" ]; then
  echo "Expected file not found in Delta clone: $DV_SUITE" >&2
  echo "The Delta directory layout for ref '${DELTA_REF}' may have changed." >&2
  exit 1
fi
if ! sed 's/^__BLANK_CONTEXT__$/ /' <<'PATCH' | git -C "$DELTA_DIR" apply -
diff --git a/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala b/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala
--- a/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala
+++ b/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala
@@ -913,12 +913,14 @@ class DeletionVectorsWithPredicatePushdownSuite extends DeletionVectorsSuite {
     super.beforeAll()
__BLANK_CONTEXT__
     // 2MB rowgroups.
     hadoopConf().set("parquet.block.size", (2 * 1024 * 1024).toString)
__BLANK_CONTEXT__
-    spark.range(0, multiRowgroupTableRowsNum, 1, 1).toDF("id")
-      .write
-      .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, true.toString)
-      .format("delta")
-      .saveAsTable(multiRowgroupTable)
+    withSQLConf("spark.gluten.sql.native.parquet.write.blockRows" -> "500000") {
+      spark.range(0, multiRowgroupTableRowsNum, 1, 1).toDF("id")
+        .write
+        .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, true.toString)
+        .format("delta")
+        .saveAsTable(multiRowgroupTable)
+    }
__BLANK_CONTEXT__
     val deltaLog = DeltaLog.forTable(spark, TableIdentifier(multiRowgroupTable))
     val files = deltaLog.update().allFiles.collect()
PATCH
then
  echo "ERROR: predicate-pushdown DV fixture patch did not apply." >&2
  echo "The patch expects the Delta v4.2.0 beforeAll fixture shape;" \
    "ref '${DELTA_REF}' must remain source-compatible." >&2
  exit 1
fi
DV_ROW_CAP_SCOPES=$(
  grep -Fxc \
    '    withSQLConf("spark.gluten.sql.native.parquet.write.blockRows" -> "500000") {' \
    "$DV_SUITE" || true
)
if [ "$DV_ROW_CAP_SCOPES" -ne 1 ]; then
  echo "ERROR: expected exactly one predicate-pushdown DV row-count scope;" \
    "found ${DV_ROW_CAP_SCOPES}." >&2
  echo "DeletionVectorsSuite may have changed in Delta ref '${DELTA_REF}'." >&2
  exit 1
fi
echo "Capped predicate-pushdown DV fixture row groups at 500,000 rows."
git -C "$DELTA_DIR" --no-pager diff -- \
  "spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala" || true
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
# tree has uncommitted edits to a file it touches. It also relies on the clone
# step's `checkout -f`: the sed appends after the declaration line, so without
# that per-run reset a re-run injects duplicate `fail` lines and trips the
# INJECTED != 2 check below.
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
  echo "Their test names likely changed in Delta ref '${DELTA_REF}'; update apply-delta-test-patches.sh." >&2
  exit 1
fi
echo "Force-failed 2 DeletionVectorsSuite 2B-row tests (read + delete)."
git -C "$DELTA_DIR" --no-pager diff -- "spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala" || true
echo "::endgroup::"

echo "::group::Resulting temporary Delta test source diff"
git -C "$DELTA_DIR" --no-pager diff HEAD -- "spark/src/test"
echo "::endgroup::"
