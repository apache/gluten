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
#   apply-delta-test-patches.sh <delta_dir> <delta_ref>
#
# Remove each patch group when DELTA_REF contains the corresponding upstream
# fix or Gluten no longer needs the workaround.
#

set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <delta_dir> <delta_ref>" >&2
  exit 1
fi

DELTA_DIR="$1"
DELTA_REF="$2"

if [ -z "$DELTA_REF" ]; then
  echo "Delta ref must not be empty." >&2
  exit 1
fi
if ! git -C "$DELTA_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Delta checkout is not a Git work tree: $DELTA_DIR" >&2
  exit 1
fi

EXPECTED_FILES=(
  "spark/src/test/scala/org/apache/spark/sql/delta/DeleteSuiteBase.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/DeltaParquetFileFormatSuite.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/DeltaSuite.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/DeltaTestUtils.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/MergeIntoSQLSuite.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/UpdateSuiteBase.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/perf/OptimizeGeneratedColumnSuite.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/rowid/RowIdSuite.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/test/ScanReportHelper.scala"
  "spark/src/test/scala/org/apache/spark/sql/delta/test/TestsStatistics.scala"
)
for file in "${EXPECTED_FILES[@]}"; do
  if [ ! -f "$DELTA_DIR/$file" ]; then
    echo "Expected file not found in Delta clone: $DELTA_DIR/$file" >&2
    echo "The Delta directory layout for ref '${DELTA_REF}' may have changed." >&2
    exit 1
  fi
done

# Delta's tests collect file-source scans by matching the concrete
# `FileSourceScanExec` case class; Gluten offloads the scan to
# DeltaScanTransformer, a `FileSourceScanLike` sibling, so those matches miss.
# delta-io/delta#7104 and #7105 widen the matches to the shared interface. Both
# fixes land after v4.2.0 and can be removed once the pinned ref contains them.
#
# Depth-2 fetch brings each fix commit and its parent, which cherry-pick needs
# to diff against. `-n` applies without requiring a committer identity.
cherry_pick_delta_fix() {
  local sha="$1" pr="$2"
  git -C "$DELTA_DIR" fetch --quiet --depth 2 origin "$sha"
  echo "Cherry-picking delta-io/delta${pr}"
  if git -C "$DELTA_DIR" cherry-pick -n "$sha"; then
    return 0
  fi

  local file
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    git -C "$DELTA_DIR" reset -q -- "$file" 2>/dev/null || true
    git -C "$DELTA_DIR" checkout -q -- "$file" 2>/dev/null || true
  done < <(git -C "$DELTA_DIR" diff-tree --no-commit-id --name-only -r "$sha")
  git -C "$DELTA_DIR" cherry-pick --quit 2>/dev/null || true

  if git -C "$DELTA_DIR" diff "${sha}^" "$sha" | \
      git -C "$DELTA_DIR" apply --reverse --check -; then
    echo "delta-io/delta${pr} is already present in ${DELTA_REF}; skipping it."
    return 0
  fi

  echo "ERROR: delta-io/delta${pr} did not apply cleanly and is not already present." >&2
  echo "Delta ref '${DELTA_REF}' has drifted from the expected patch context." >&2
  exit 1
}

echo "::group::Cherry-picking upstream Delta FileSourceScanLike test fixes"
cherry_pick_delta_fix 46bd45d57eadd7e528002a0ae7bd36ce5a456eca "#7104 (ScanReportHelper.collectScans)"
cherry_pick_delta_fix 959e00e15f41f56afc1c9bb95d160c55c6dc7068 "#7105 (9 more test suites)"
echo "::endgroup::"

echo "::group::Capping DeltaParquetFileFormat fixture row groups by row count"
# Remove after the Delta fixture no longer relies on a byte threshold to split
# one 20,000-row input batch. Gluten's row-count limit makes the split
# deterministic while preserving the native write path.
DPFFS="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/DeltaParquetFileFormatSuite.scala"
ORIGINAL_WRITES=$(
  grep -Fxc '    df.write.format("delta").mode("append").save(tablePath)' "$DPFFS" || true
)
EXISTING_ROW_CAPS=$(
  grep -Fxc \
    '    withSQLConf("spark.gluten.sql.native.parquet.write.blockRows" -> "10000") {' \
    "$DPFFS" || true
)
if [ "$ORIGINAL_WRITES" -ne 1 ] || [ "$EXISTING_ROW_CAPS" -ne 0 ]; then
  echo "ERROR: expected exactly one unpatched DeltaParquetFileFormat fixture write;" \
    "found ${ORIGINAL_WRITES} writes and ${EXISTING_ROW_CAPS} row-count scopes." >&2
  echo "DeltaParquetFileFormatSuite has drifted in Delta ref '${DELTA_REF}'." >&2
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
  echo "Delta ref '${DELTA_REF}' must remain source-compatible with the patch." >&2
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
  exit 1
fi
echo "Capped DeltaParquetFileFormat fixture row groups at 10,000 rows."
echo "::endgroup::"

echo "::group::Force-failing memory-hog DeletionVectorsSuite 2B-row tests"
# Remove once Gluten can process both 2-billion-row tests without exhausting
# native memory. Keep this after #7105, which edits the same source file.
DVS="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala"
READ_TEST='huge table: read from tables of 2B rows with existing DV of many zeros") {'
DELETE_TEST='number of rows from tables of 2B rows with DVs") {'
READ_MATCHES=$(grep -Fc "$READ_TEST" "$DVS" || true)
DELETE_MATCHES=$(grep -Fc "$DELETE_TEST" "$DVS" || true)
EXISTING_FAILURES=$(grep -c "Gluten CI] Force-failed" "$DVS" || true)
if [ "$READ_MATCHES" -ne 1 ] || [ "$DELETE_MATCHES" -ne 1 ] || \
    [ "$EXISTING_FAILURES" -ne 0 ]; then
  echo "ERROR: expected one unpatched declaration for each 2B-row test;" \
    "found read=${READ_MATCHES}, delete=${DELETE_MATCHES}, injected=${EXISTING_FAILURES}." >&2
  echo "DeletionVectorsSuite has drifted in Delta ref '${DELTA_REF}'." >&2
  exit 1
fi
sed -i 's#huge table: read from tables of 2B rows with existing DV of many zeros") {#&\n    fail("[Gluten CI] Force-failed: 2B-row DV read OOMs; see apply-delta-test-patches.sh")#' "$DVS"
sed -i 's#number of rows from tables of 2B rows with DVs") {#&\n      fail("[Gluten CI] Force-failed: 2B-row DV delete OOMs; see apply-delta-test-patches.sh")#' "$DVS"
INJECTED=$(grep -c "Gluten CI] Force-failed" "$DVS" || true)
if [ "$INJECTED" -ne 2 ]; then
  echo "ERROR: expected to force-fail 2 DeletionVectorsSuite tests but injected ${INJECTED}." >&2
  exit 1
fi
echo "Force-failed 2 DeletionVectorsSuite 2B-row tests (read + delete)."
echo "::endgroup::"

echo "::group::Resulting temporary Delta test source diff"
git -C "$DELTA_DIR" --no-pager diff HEAD -- "spark/src/test"
echo "::endgroup::"
