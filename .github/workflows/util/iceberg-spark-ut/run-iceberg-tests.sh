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
# Runs one shard of Apache Iceberg's own Spark unit tests against Gluten/Velox
# and gates the result against the committed known-failures baseline. Extracted
# from iceberg_spark_ut.yml so the workflow step stays readable; it is also the
# supported way to reproduce a CI shard locally (see README.md).
#
# Driven by environment (set by the workflow step / job):
#   SPARK_VERSION     - Gluten Spark profile version, e.g. 3.5 (also selects the
#                       Iceberg module: iceberg-spark-<SPARK_VERSION>_<SCALA_VERSION>)
#   SCALA_VERSION     - 2.12 or 2.13
#   SHARD_ID          - this shard's id (matrix.shard)
#   NUM_SHARDS        - total shards (env.ICEBERG_NUM_SHARDS)
#   FORK_COUNT        - surefire forkCount (env.ICEBERG_FORK_COUNT)
#   UPDATE_BASELINE   - 'true' -> gate seed mode; else enforce
#   FAIL_ON_FIXED     - passed through to the gate
#   GITHUB_WORKSPACE  - repo root
#
# Optional:
#   JAVA_HOME         - defaults to the container's java-17 install
#   CLASS_TIMEOUT     - per-test-class hang guard in seconds (default 2400)

set -euo pipefail

: "${SPARK_VERSION:?SPARK_VERSION is required}"
: "${SCALA_VERSION:?SCALA_VERSION is required}"
: "${SHARD_ID:?SHARD_ID is required}"
: "${NUM_SHARDS:?NUM_SHARDS is required}"
: "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is required}"
FORK_COUNT="${FORK_COUNT:-2}"
UPDATE_BASELINE="${UPDATE_BASELINE:-false}"
FAIL_ON_FIXED="${FAIL_ON_FIXED:-true}"
CLASS_TIMEOUT="${CLASS_TIMEOUT:-2400}"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}"
export PATH="$JAVA_HOME/bin:$PATH"
java -version

cd "$GITHUB_WORKSPACE"

UTIL_DIR="$GITHUB_WORKSPACE/.github/workflows/util/iceberg-spark-ut"
# The known-failures gate is shared with the Delta pipeline: it is engine
# agnostic (JUnit XML in, suite#test lists out), so it lives in one place rather
# than being duplicated per pipeline.
GATE_SCRIPT="$GITHUB_WORKSPACE/.github/workflows/util/delta-spark-ut/compare-test-results.py"
KNOWN_FAILURES="$UTIL_DIR/known-failures-spark-${SPARK_VERSION}.txt"
REPORTS_DIR="$GITHUB_WORKSPACE/backends-velox/target/surefire-reports"
MVN_CMD=("$GITHUB_WORKSPACE/build/mvn" -ntp)

ICEBERG_MODULE_SUFFIX="${SPARK_VERSION}_${SCALA_VERSION}"
# `-Piceberg-test` is NOT optional: Iceberg's TestBaseWithCatalog -- the base of
# nearly every Spark test class -- references RESTCatalogServer /
# RESTServerExtension from the iceberg-open-api test fixtures, which only that
# profile declares. Without it virtually every suite aborts on
# NoClassDefFoundError.
#
# For the Spark 3.4 target that profile also adds
# backends-velox/src-iceberg-spark34/test as a source root. What is left there
# is Gluten's OWN TestTPCHStoragePartitionedJoins plus the two base classes it
# needs; those get compiled but never run here (`-Dtest` only ever names classes
# enumerated from the Iceberg test jars), and their names --
# SparkTestBase / SparkTestBaseWithCatalog -- no longer exist in Iceberg, so
# they cannot shadow anything in the test jars either.
PROFILES=(
  "-Pspark-${SPARK_VERSION}"
  "-Pscala-${SCALA_VERSION}"
  -Pjava-17
  -Pbackends-velox
  -Piceberg
  -Piceberg-test
)

echo "::group::Building backends-velox (Spark ${SPARK_VERSION} / Scala ${SCALA_VERSION})"
# `-pl backends-velox -am` builds only backends-velox and the modules it depends
# on (including gluten-iceberg, added by -Piceberg), skipping gluten-ut/package
# and the other backends. `-DskipTests` skips test EXECUTION but still COMPILES
# the test sources, which is what the standalone `surefire:test` invocation below
# needs. `install` (not `package`) puts the reactor's artifacts -- including the
# test-jars backends-velox depends on -- in the local repository, so the
# single-module `surefire:test` run can resolve them.
"${MVN_CMD[@]}" clean install -DskipTests -pl backends-velox -am "${PROFILES[@]}"
echo "::endgroup::"

echo "::group::Resolving the Iceberg version and this shard's test classes"
# Ask Maven for both values rather than hardcoding or guessing them. `build/mvn`
# writes its own chatter to stderr, so `-q -DforceStdout` leaves only the value
# on stdout; `tail -n 1` guards against that changing.
mvn_eval() {
  "${MVN_CMD[@]}" -q -pl backends-velox "${PROFILES[@]}" \
    help:evaluate -Dexpression="$1" -DforceStdout \
    | tail -n 1 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

# The Iceberg version is a property of the selected Spark profile (pom.xml), so
# reading it here cannot drift when the pinned version is bumped.
ICEBERG_VERSION="$(mvn_eval iceberg.version)"
if ! printf '%s' "$ICEBERG_VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+'; then
  echo "ERROR: could not resolve iceberg.version (got '${ICEBERG_VERSION}')" >&2
  exit 1
fi
echo "Iceberg version: ${ICEBERG_VERSION}"

# Where the Iceberg test jars actually landed. Deliberately NOT `$HOME/.m2`: in
# a container job GitHub sets HOME=/github/home, but the JVM takes `user.home`
# from the passwd entry (root -> /root), so Maven's local repository is
# /root/.m2/repository. Asking Maven keeps this correct whichever user, image or
# settings.xml the job runs with.
M2_REPO="$(mvn_eval settings.localRepository)"
if [ -z "$M2_REPO" ] || [ ! -d "$M2_REPO" ]; then
  echo "ERROR: could not resolve Maven's local repository (got '${M2_REPO}')" >&2
  exit 1
fi
echo "Maven local repository: ${M2_REPO}"

SHARD_CLASS_LIST="$GITHUB_WORKSPACE/shard-classes.txt"
TEST_PATTERNS="$(python3 "$UTIL_DIR/shard-test-classes.py" \
  --spark-version "$SPARK_VERSION" \
  --scala-version "$SCALA_VERSION" \
  --iceberg-version "$ICEBERG_VERSION" \
  --m2-repo "$M2_REPO" \
  --num-shards "$NUM_SHARDS" \
  --shard-id "$SHARD_ID" \
  --out "$SHARD_CLASS_LIST")"
if [ -z "$TEST_PATTERNS" ]; then
  echo "ERROR: shard ${SHARD_ID} resolved to no test classes." >&2
  exit 1
fi
EXPECTED_CLASSES="$(wc -l < "$SHARD_CLASS_LIST" | tr -d '[:space:]')"
echo "::endgroup::"

echo "::group::Running upstream Iceberg tests (shard ${SHARD_ID}/${NUM_SHARDS})"
# How Gluten gets enabled, with NO patch to any upstream Iceberg source: none of
# Iceberg's test classes set `spark.plugins`, and Spark's `SparkConf` loads every
# `spark.*` JVM system property, so these `-D`s in the forked test JVM's command
# line apply to every SparkSession the tests build -- including the ~25 classes
# that build their own session instead of extending TestBase. The set mirrors
# org.apache.gluten.TestConfUtil (used by the vendored Spark-3.4 Iceberg tests)
# so both paths enable Gluten identically.
#
# Memory per fork: 2G heap + 2G Velox off-heap ~= 4G, so FORK_COUNT=2 stays
# inside the ~16G runner. Keep heap-dump-on-OOM so a real heap OOM is
# analyzable.
ARG_LINE=(
  -Xmx2g
  -XX:+HeapDumpOnOutOfMemoryError
  -XX:HeapDumpPath=/tmp
  -Dspark.plugins=org.apache.gluten.GlutenPlugin
  -Dspark.memory.offHeap.enabled=true
  -Dspark.memory.offHeap.size=2g
  -Dspark.ui.enabled=false
  -Dspark.gluten.ui.enabled=false
)

# `surefire:test` is invoked as a standalone goal (not via `mvn test`) so ONLY
# surefire runs: the `test` lifecycle phase would also run the scalatest plugin,
# i.e. Gluten's own Scala suites, which this pipeline is not about.
#
# -DdependenciesToScan makes surefire discover test classes inside the Iceberg
# `-tests.jar` dependencies, not just in src/test; -Dtest narrows that discovery
# to this shard's classes (the filter applies to the scanned dependencies too,
# and it also excludes backends-velox's own test classes -- this pipeline is
# only about the upstream Iceberg suite).
#
# -DreuseForks=false gives every test class a fresh JVM. Each class already
# starts its own Hive metastore + Spark session, so the extra JVM start costs
# little, and it (a) stops Velox native allocations from accumulating across a
# fork's ~30 classes and (b) makes -Dsurefire.timeout a PER-CLASS hang guard
# rather than a cap on the whole shard.
#
# -Dmaven.test.failure.ignore=true: test failures are EXPECTED here (Gluten does
# not yet support every Iceberg code path) and must not fail this step directly
# -- the baseline gate below decides pass/fail. Maven still exits non-zero for
# real build/plugin errors (e.g. a fork that died without reporting), which is
# handled after the gate so a regression is never masked by it.
set +e
"${MVN_CMD[@]}" surefire:test -pl backends-velox "${PROFILES[@]}" \
  -DdependenciesToScan="org.apache.iceberg:iceberg-spark-${ICEBERG_MODULE_SUFFIX},org.apache.iceberg:iceberg-spark-extensions-${ICEBERG_MODULE_SUFFIX}" \
  -Dtest="$TEST_PATTERNS" \
  -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DforkCount="$FORK_COUNT" \
  -DreuseForks=false \
  -Dsurefire.timeout="$CLASS_TIMEOUT" \
  -Dmaven.test.failure.ignore=true \
  -DargLine="${ARG_LINE[*]}"
MVN_RC=$?
set -e
echo "Maven exit code: ${MVN_RC}"
echo "::endgroup::"

echo "::group::Checking the test selection took effect"
REPORT_COUNT=0
if [ -d "$REPORTS_DIR" ]; then
  REPORT_COUNT="$(find "$REPORTS_DIR" -name 'TEST-*.xml' | wc -l | tr -d '[:space:]')"
fi
echo "shard classes selected: ${EXPECTED_CLASSES}; suite reports written: ${REPORT_COUNT}"
if [ "$REPORT_COUNT" -eq 0 ]; then
  echo "ERROR: no surefire reports under ${REPORTS_DIR}. The upstream test" >&2
  echo "classes were not discovered at all -- check that -DdependenciesToScan" >&2
  echo "matches the resolved Iceberg artifacts for Spark ${SPARK_VERSION}." >&2
  exit 1
fi
# A shard selects each class by its fully qualified name, so it should write at
# most one report per selected class (abstract bases report none). Many times
# more means -Dtest did not filter the scanned dependencies and EVERY shard just
# ran the whole suite -- a silent NUM_SHARDS-fold waste that would still produce
# a green gate. Fail loudly instead. The slack keeps a class that legitimately
# reports more than once from tripping the check.
MAX_REPORTS=$((EXPECTED_CLASSES * 2 + 10))
if [ "$REPORT_COUNT" -gt "$MAX_REPORTS" ]; then
  echo "ERROR: ${REPORT_COUNT} suite reports for ${EXPECTED_CLASSES} selected" >&2
  echo "classes (limit ${MAX_REPORTS}). The -Dtest filter did not apply to the" >&2
  echo "-DdependenciesToScan classes, so this shard ran far more than its" >&2
  echo "slice. Fix the sharding before trusting this run." >&2
  exit 1
fi
echo "::endgroup::"

echo "::group::Gating against the known-failures baseline"
# Classify this shard's results against the baseline: seed mode when
# UPDATE_BASELINE=true (record failures, never fail) so the baseline can be
# (re)generated; otherwise enforce against it. Writes this shard's
# gate-out/*.txt for the aggregate job.
GATE_MODE=enforce
if [ "${UPDATE_BASELINE}" = "true" ]; then
  GATE_MODE=seed
fi
mkdir -p "$GITHUB_WORKSPACE/gate-out"
set +e
python3 "$GATE_SCRIPT" \
  --mode "$GATE_MODE" \
  --reports-dir "$REPORTS_DIR" \
  --known-failures "$KNOWN_FAILURES" \
  --flaky-tests "$UTIL_DIR/flaky-tests.txt" \
  --flaky-error-patterns "$UTIL_DIR/flaky-error-patterns.txt" \
  --failures-out "$GITHUB_WORKSPACE/gate-out/failures-shard-${SHARD_ID}.txt" \
  --ran-out "$GITHUB_WORKSPACE/gate-out/ran-shard-${SHARD_ID}.txt" \
  --skipped-out "$GITHUB_WORKSPACE/gate-out/skipped-shard-${SHARD_ID}.txt" \
  --fail-on-fixed "${FAIL_ON_FIXED}"
GATE_RC=$?
set -e
echo "::endgroup::"

if [ "$GATE_RC" -ne 0 ]; then
  exit "$GATE_RC"
fi
if [ "$MVN_RC" -ne 0 ]; then
  echo "ERROR: the baseline gate passed but Maven exited ${MVN_RC}, i.e. it hit" >&2
  echo "a build/plugin error rather than a test failure (a crashed or timed-out" >&2
  echo "test fork typically reports this way). Some classes in this shard may" >&2
  echo "never have run, so the green gate is not trustworthy -- see the log." >&2
  exit "$MVN_RC"
fi
