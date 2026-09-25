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
# Gluten (Velox) bundle jar on the classpath. Source java-test-args.sh before
# running sbt to configure Gluten without replacing Delta's test fixtures.
#
# Usage:
#   setup-delta.sh <delta_ref> <delta_dir> <gluten_bundle_jar>
#
# Arguments:
#   delta_ref           - git ref (tag/branch/sha) to check out (e.g. v4.2.0)
#   delta_dir           - destination directory for the Delta clone
#   gluten_bundle_jar   - path to the gluten-velox-bundle fat jar
#

set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <delta_ref> <delta_dir> <gluten_bundle_jar>" >&2
  exit 1
fi

DELTA_REF="$1"
DELTA_DIR="$2"
GLUTEN_BUNDLE_JAR="$3"

if [ ! -f "$GLUTEN_BUNDLE_JAR" ]; then
  echo "Gluten bundle jar not found: $GLUTEN_BUNDLE_JAR" >&2
  exit 1
fi

echo "::group::Cloning delta-io/delta @ ${DELTA_REF}"
# init + shallow fetch resolves a tag, branch OR commit SHA in a single path
# (`git clone --branch` rejects SHAs). Avoids a full-clone fallback and the
# destructive `rm -rf "$DELTA_DIR"` it required. `--` terminates options so a
# DELTA_REF starting with `-` can't be misread as a git flag (this script is
# workflow_dispatch-runnable with a user-supplied ref).
#
# Every step here is idempotent so a local re-run (or a CI re-run on a runner
# that kept the workspace) resumes instead of dying: `git init` re-initializes
# an existing repo harmlessly, but `remote add` errors out when `origin` already
# exists, so drop it first; and `checkout -f` discards leftovers from a previous
# partial run. Nothing worth keeping exists here yet -- the bundle jar and the
# source patches below are applied *after* this block.
git init -q "$DELTA_DIR"
git -C "$DELTA_DIR" remote remove origin 2>/dev/null || true
git -C "$DELTA_DIR" remote add origin https://github.com/delta-io/delta.git
git -C "$DELTA_DIR" fetch -q --depth 1 origin -- "$DELTA_REF"
git -C "$DELTA_DIR" checkout -qf FETCH_HEAD
git -C "$DELTA_DIR" --no-pager log -1 --oneline
echo "::endgroup::"

echo "::group::Injecting Gluten bundle jar onto the spark project's TEST classpath"
# The Gluten bundle jar must be on the spark project's TEST runtime classpath
# (so Spark can load org.apache.gluten.GlutenPlugin by name) but
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

PATCH_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/apply-delta-test-patches.sh"
bash "$PATCH_SCRIPT" "$DELTA_REF" "$DELTA_DIR"
