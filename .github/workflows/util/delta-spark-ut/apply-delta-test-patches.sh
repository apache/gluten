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
PATCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/patches"

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

echo "::group::Applying temporary Delta test patches"
# Keep local patches after the cherry-picks, which require an unmodified worktree
# for the files they touch. Patch paths are relative to this script, not Delta.
for patch in \
  cdf-pushed-filter-assertions.patch \
  parquet-fixture-row-groups.patch \
  dv-predicate-pushdown-row-groups.patch \
  dv-2b-row-fail-fast.patch; do
  echo "Applying $patch"
  if ! git -C "$DELTA_DIR" apply "$PATCH_DIR/$patch"; then
    echo "ERROR: Delta test patch '$patch' did not apply." >&2
    echo "The patch expects Delta v4.2.0 test sources;" \
      "ref '${DELTA_REF}' must remain source-compatible." >&2
    exit 1
  fi
done
echo "::endgroup::"

echo "::group::Checking Delta CDF pushed-filter assertions"
CDF_TEST_DIR="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta"
for suite in DeltaCDCSuite DeltaCDCSQLSuite DeltaCDCColumnMappingSuite; do
  if [ ! -f "$CDF_TEST_DIR/$suite.scala" ]; then
    echo "Expected file not found in Delta clone: $CDF_TEST_DIR/$suite.scala" >&2
    echo "The Delta directory layout for ref '${DELTA_REF}' may have changed." >&2
    exit 1
  fi
  EXPECTED='        .contains("PushedFilters: [IsNotNull(id), LessThan(id,5)]"))'
  if [ "$suite" = DeltaCDCColumnMappingSuite ]; then
    EXPECTED='        .contains("PushedFilters: [IsNotNull(`id with space`), LessThan(`id with space`,5)]"))'
  fi
  MATCHED=$(grep -Fxc "$EXPECTED" "$CDF_TEST_DIR/$suite.scala" || true)
  ASSERTIONS=$(grep -Fc '.contains("PushedFilters:' "$CDF_TEST_DIR/$suite.scala" || true)
  if [ "$MATCHED" != "1" ] || [ "$ASSERTIONS" != "1" ]; then
    echo "ERROR: expected exactly one Gluten pushed-filter assertion in $suite.scala;" \
      "found ${MATCHED} matching out of ${ASSERTIONS} assertions." >&2
    echo "The CDF assertions may have changed in Delta ref '${DELTA_REF}'." >&2
    exit 1
  fi
done
echo "Adapted 3 Delta CDF pushed-filter assertions for 13 test cases."
echo "::endgroup::"

echo "::group::Checking DeltaParquetFileFormat fixture row groups"
DPFFS="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/DeltaParquetFileFormatSuite.scala"
if [ ! -f "$DPFFS" ]; then
  echo "Expected file not found in Delta clone: $DPFFS" >&2
  echo "The Delta directory layout for ref '${DELTA_REF}' may have changed." >&2
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

echo "::group::Checking predicate-pushdown DV fixture row groups"
DV_SUITE="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala"
if [ ! -f "$DV_SUITE" ]; then
  echo "Expected file not found in Delta clone: $DV_SUITE" >&2
  echo "The Delta directory layout for ref '${DELTA_REF}' may have changed." >&2
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

echo "::group::Checking DeletionVectorsSuite 2B-row fail-fast guards"
DVS="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala"
if [ ! -f "$DVS" ]; then
  echo "Expected file not found in Delta clone: $DVS" >&2
  echo "The Delta directory layout for ref '${DELTA_REF}' may have changed." >&2
  exit 1
fi
INJECTED=$(grep -c "Gluten CI] Force-failed" "$DVS" || true)
if [ "$INJECTED" -ne 2 ]; then
  echo "ERROR: expected to force-fail 2 DeletionVectorsSuite tests but injected ${INJECTED}." >&2
  echo "Their test names likely changed in Delta ref '${DELTA_REF}'; update dv-2b-row-fail-fast.patch." >&2
  exit 1
fi
echo "Force-failed 2 DeletionVectorsSuite 2B-row tests (read + delete)."
git -C "$DELTA_DIR" --no-pager diff -- "spark/src/test/scala/org/apache/spark/sql/delta/deletionvectors/DeletionVectorsSuite.scala" || true
echo "::endgroup::"

echo "::group::Resulting temporary Delta test source diff"
git -C "$DELTA_DIR" --no-pager diff HEAD -- "spark/src/test"
echo "::endgroup::"
