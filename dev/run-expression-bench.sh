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

set -euo pipefail

usage() {
  printf '%s\n' \
    'Usage: dev/run-expression-bench.sh [--skip-build] functionsCSV [options]' \
    '       dev/run-expression-bench.sh [--skip-build] --cases function/case,glob [options]' \
    '       dev/run-expression-bench.sh [--skip-build] --list [functionsCSV] | --config file [options]' \
    'Options: --input-partitions (alias --maps) --aggregate-mode complete|partial-final --reducers' \
    '         --shuffle-dir --rows --batch-size --seed --key-cardinality' \
    '         --warmup-seconds N --measurement-seconds N' \
    '         --async-profiler dir --profile-event cpu|alloc --profile-output dir' \
    'Default: 10s warmup / 60s measurement per engine; at least 2 iterations.' \
    'Always measures JVM first, then Native.' \
    'Requires JAVA_HOME (Java 17 HotSpot) and native libraries built in this checkout.' \
    'Compiles Spark 4.1 / Scala 2.13 sources before each run; does not rebuild native libraries.' \
    '--skip-build skips Maven compilation and classpath export; requires existing build outputs.' \
    'With --skip-build, the caller must ensure those outputs match the current sources.' \
    'Example: dev/run-expression-bench.sh --cases rtrim/l13-half-even --rows 4000000 --batch-size 10240'
}

SKIP_BUILD=false
if [[ "${1:-}" == "--skip-build" ]]; then
  SKIP_BUILD=true
  shift
fi

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi
if [[ $# -eq 0 ]]; then
  usage >&2
  exit 1
fi
MAIN='org.apache.spark.sql.execution.benchmark.expression.RegisteredExpressionBenchmark'
CALLER_CWD="$PWD"

if [[ -z "${JAVA_HOME:-}" || ! -x "$JAVA_HOME/bin/java" ]]; then
  printf 'Set JAVA_HOME to a Java 17 HotSpot JDK.\n' >&2
  exit 1
fi

GLUTEN_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$GLUTEN_HOME"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
PROFILES=java-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta
TARGET="$GLUTEN_HOME/backends-velox/target"
CLASSPATH_FILE="$TARGET/expression-benchmark.classpath"
JVM_ARGS_FILE="$TARGET/expression-benchmark.jvmargs"

if [[ "$SKIP_BUILD" == false ]]; then
  mkdir -p "$TARGET"
  # Install this reactor's dependencies rather than using older local SNAPSHOT jars.
  ./build/mvn -P"$PROFILES" -pl backends-velox -am install -DskipTests
  ./build/mvn -P"$PROFILES" -pl gluten-arrow,backends-velox dependency:build-classpath help:evaluate \
    -DincludeScope=test -Dmdep.outputFile="$CLASSPATH_FILE" \
    -Dexpression=extraJavaTestArgs -Doutput="$JVM_ARGS_FILE"
fi

for file in "$CLASSPATH_FILE" "$JVM_ARGS_FILE" "$TARGET/scala-2.13/test-classes/${MAIN//.//}.class"; do
  if [[ ! -s "$file" ]]; then
    printf 'Missing benchmark artifact: %s. Run without --skip-build to prepare it.\n' "$file" >&2
    exit 1
  fi
done
if [[ ! -d "$TARGET/scala-2.13/classes" ]]; then
  printf 'Missing benchmark classes directory. Run without --skip-build to prepare it.\n' >&2
  exit 1
fi

IFS=: read -r -a DEPENDENCIES <<< "$(< "$CLASSPATH_FILE")"
for entry in "${DEPENDENCIES[@]}"; do
  if [[ ! -e "$entry" ]]; then
    printf 'Missing benchmark classpath entry: %s. Run without --skip-build to prepare it.\n' "$entry" >&2
    exit 1
  fi
done

exec "$JAVA_HOME/bin/java" @"$JVM_ARGS_FILE" \
  -Dspark.testing=true -Xmx16g \
  "-Dgluten.expressionBenchmark.cwd=$CALLER_CWD" \
  -XX:+UnlockExperimentalVMOptions \
  '-XX:CompileCommand=blackhole,org.apache.spark.sql.execution.benchmark.expression.BenchmarkBlackhole::consume' \
  -cp "$TARGET/scala-2.13/test-classes:$TARGET/scala-2.13/classes:$(< "$CLASSPATH_FILE")" \
  "$MAIN" "$@"
