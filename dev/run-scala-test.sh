#!/bin/bash
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

# =============================================================================
# run-scala-test.sh - Run ScalaTest like IntelliJ IDEA
# =============================================================================
#
# This script simulates IntelliJ IDEA's ScalaTest execution to allow running
# individual test methods, which is not possible with standard Maven commands
# due to the conflict between -Dsuites and -am parameters.
#
# Usage:
#   ./dev/run-scala-test.sh [options] -P<profiles> -pl <module> -s <suite> [-t "test name"]
#
# Examples:
#   # Run entire suite
#   ./dev/run-scala-test.sh \
#     -Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,spark-ut \
#     -pl gluten-ut/spark40 \
#     -s org.apache.spark.sql.GlutenDeprecatedDatasetAggregatorSuite
#
#   # Run single test method
#   ./dev/run-scala-test.sh \
#     -Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,spark-ut \
#     -pl gluten-ut/spark40 \
#     -s org.apache.spark.sql.GlutenDeprecatedDatasetAggregatorSuite \
#     -t "typed aggregation: class input with reordering"
#
#   # With Maven Daemon (mvnd) for faster builds
#   ./dev/run-scala-test.sh \
#     -Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,spark-ut \
#     -pl gluten-ut/spark40 \
#     -s org.apache.spark.sql.GlutenDeprecatedDatasetAggregatorSuite \
#     --mvnd
#
#   # With Maven profiler enabled
#   ./dev/run-scala-test.sh \
#     -Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,spark-ut \
#     -pl gluten-ut/spark40 \
#     -s org.apache.spark.sql.GlutenDeprecatedDatasetAggregatorSuite \
#     --profile
#
#   # Export classpath only (no test execution)
#   ./dev/run-scala-test.sh \
#     -Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,spark-ut \
#     -pl gluten-ut/spark40 \
#     -s org.apache.spark.sql.GlutenDeprecatedDatasetAggregatorSuite \
#     --export-only
#
# =============================================================================

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLUTEN_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'

# =============================================================================
# Timing helpers
# =============================================================================

timer_now() {
  date +%s%N
}

# Returns elapsed time in seconds (with milliseconds)
timer_elapsed() {
  local start=$1 end=$2
  echo "scale=1; ($end - $start) / 1000000000" | bc
}

format_duration() {
  local secs=$1
  local mins=$(echo "$secs / 60" | bc)
  local remaining=$(echo "scale=1; $secs - $mins * 60" | bc)
  if [[ "$mins" -gt 0 ]]; then
    printf "%dm %.1fs" "$mins" "$remaining"
  else
    printf "%.1fs" "$remaining"
  fi
}

log_timing() {
  [[ "$ENABLE_PROFILER" != "true" ]] && return
  local label=$1 elapsed=$2
  local formatted=$(format_duration "$elapsed")
  echo -e "${CYAN}[TIME]${NC} ${label}: ${MAGENTA}${formatted}${NC}"
}

# Print timing summary table. Args: include_step4 (true/false)
print_timing_summary() {
  [[ "$ENABLE_PROFILER" != "true" ]] && return
  local include_step4=${1:-false}
  local mvn_total=$(echo "$TIMING_STEP1 + $TIMING_STEP2" | bc)
  local overall=$(echo "$mvn_total + $TIMING_STEP3" | bc)
  local cached_tag=""
  [[ "$CACHE_HIT" == "true" ]] && cached_tag=" (cached)"

  echo ""
  echo -e "${CYAN}==========================================${NC}"
  echo -e "${CYAN}  ⏱  Timing Summary${NC}"
  echo -e "${CYAN}==========================================${NC}"
  printf "  %-40s %s\n" "Step 1 - Compile + Classpath (mvn):" "$(format_duration $TIMING_STEP1)${cached_tag}"
  printf "  %-40s %s\n" "Step 2 - Evaluate JVM args (mvn):" "$(format_duration $TIMING_STEP2)${cached_tag}"
  printf "  %-40s %s\n" "Step 3 - Resolve classpath:" "$(format_duration $TIMING_STEP3)"
  if [[ "$include_step4" == "true" ]]; then
    printf "  %-40s %s\n" "Step 4 - ScalaTest execution:" "$(format_duration $TIMING_STEP4)"
    overall=$(echo "$overall + $TIMING_STEP4" | bc)
  fi
  echo "  ------------------------------------------"
  printf "  %-40s %s\n" "Maven total:" "$(format_duration $mvn_total)"
  if [[ "$include_step4" == "true" ]]; then
    printf "  %-40s %s\n" "Test execution:" "$(format_duration $TIMING_STEP4)"
  fi
  printf "  %-40s %s\n" "Overall:" "$(format_duration $overall)"
  echo -e "${CYAN}==========================================${NC}"
}

# =============================================================================
# Module Mapping: artifactId -> directory path
# =============================================================================
# Format: "artifactId:directory:type"
# type: "scala" for target/scala-X.XX/classes, "java" for target/classes
declare -A MODULE_MAP=(
  # Core modules
  ["gluten-core"]="gluten-core:scala"
  ["gluten-substrait"]="gluten-substrait:scala"
  ["gluten-ui"]="gluten-ui:scala"
  ["gluten-arrow"]="gluten-arrow:scala"

  # Backend modules
  ["backends-velox"]="backends-velox:scala"
  ["backends-clickhouse"]="backends-clickhouse:scala"

  # RAS modules
  ["gluten-ras-common"]="gluten-ras/common:scala"
  ["gluten-ras-planner"]="gluten-ras/planner:scala"

  # Shims modules (Java only, no scala subdirectory)
  ["spark-sql-columnar-shims-common"]="shims/common:java"
  ["spark-sql-columnar-shims-spark32"]="shims/spark32:java"
  ["spark-sql-columnar-shims-spark33"]="shims/spark33:java"
  ["spark-sql-columnar-shims-spark34"]="shims/spark34:java"
  ["spark-sql-columnar-shims-spark35"]="shims/spark35:java"
  ["spark-sql-columnar-shims-spark40"]="shims/spark40:java"
  ["spark-sql-columnar-shims-spark41"]="shims/spark41:java"

  # Unit test modules
  ["gluten-ut-common"]="gluten-ut/common:scala"
  ["gluten-ut-test"]="gluten-ut/test:scala"
  ["gluten-ut-spark32"]="gluten-ut/spark32:scala"
  ["gluten-ut-spark33"]="gluten-ut/spark33:scala"
  ["gluten-ut-spark34"]="gluten-ut/spark34:scala"
  ["gluten-ut-spark35"]="gluten-ut/spark35:scala"
  ["gluten-ut-spark40"]="gluten-ut/spark40:scala"
  ["gluten-ut-spark41"]="gluten-ut/spark41:scala"

  # Data lake modules
  ["gluten-delta"]="gluten-delta:scala"
  ["gluten-iceberg"]="gluten-iceberg:scala"
  ["gluten-hudi"]="gluten-hudi:scala"
  ["gluten-paimon"]="gluten-paimon:scala"

  # Shuffle modules
  ["gluten-celeborn"]="gluten-celeborn:scala"
  ["gluten-uniffle"]="gluten-uniffle:scala"

  # Other modules
  ["gluten-kafka"]="gluten-kafka:scala"
)

# =============================================================================
# Functions
# =============================================================================

print_usage() {
  cat << EOF
Usage: $0 [options] -P<profiles> -pl <module> -s <suite> [-t "test name"]

Required:
  -P<profiles>      Maven profiles (e.g., -Pjava-17,spark-4.0,scala-2.13,backends-velox)
  -pl <module>      Target module (e.g., gluten-ut/spark40)
  -s <suite>        Full suite class name

Optional:
  -t "test name"    Specific test method name to run
  --mvnd            Use Maven Daemon (mvnd) instead of ./build/mvn
  --clean           Run 'mvn clean' before compiling
  --force           Force Maven rebuild, bypass build cache
  --profile         Enable Maven profiler (reports in .profiler/)
  --export-only     Export classpath and exit (no test execution)
  --help            Show this help message

Examples:
  # Run entire suite
  $0 -Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,spark-ut \\
     -pl gluten-ut/spark40 \\
     -s org.apache.spark.sql.GlutenDeprecatedDatasetAggregatorSuite

  # Run single test method
  $0 -Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,spark-ut \\
     -pl gluten-ut/spark40 \\
     -s org.apache.spark.sql.GlutenDeprecatedDatasetAggregatorSuite \\
     -t "typed aggregation: class input with reordering"
EOF
}

log_info() {
  echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
  echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
  echo -e "${BLUE}[STEP]${NC} $1"
}

# Detect Scala version from profiles
detect_scala_version() {
  local profiles="$1"
  if [[ "$profiles" == *"scala-2.13"* ]]; then
    echo "2.13"
  else
    echo "2.12"
  fi
}

# Get target classes directory for a module
get_target_dir() {
  local artifact_id="$1"
  local scala_version="$2"
  local class_type="$3"

  local mapping="${MODULE_MAP[$artifact_id]}"
  if [[ -z "$mapping" ]]; then
    return 1
  fi

  local dir_path="${mapping%%:*}"
  local module_type="${mapping##*:}"

  local target_path="${GLUTEN_HOME}/${dir_path}/target"

  if [[ "$module_type" == "scala" ]]; then
    echo "${target_path}/scala-${scala_version}/${class_type}"
  else
    echo "${target_path}/${class_type}"
  fi
}

# Replace gluten jar paths with target/classes directories
replace_gluten_paths() {
  local classpath="$1"
  local scala_version="$2"
  local result=""
  local added_paths=""

  IFS=':' read -ra CP_ENTRIES <<< "$classpath"
  for entry in "${CP_ENTRIES[@]}"; do
    if [[ "$entry" != *"/org/apache/gluten/"* && "$entry" != *"/gluten-"*"/target/"* ]]; then
      result="${result}:${entry}"
      continue
    fi

    # Local target directories are already in the desired form from reactor builds
    if [[ -d "$entry" && "$entry" == *"/target/"* ]]; then
      result="${result}:${entry}"
      continue
    fi

    local filename=$(basename "$entry")
    local artifact_id=""
    for known_artifact in "${!MODULE_MAP[@]}"; do
      if [[ "$filename" == "${known_artifact}-"* ]]; then
        artifact_id="$known_artifact"
        break
      fi
    done

    if [[ -z "$artifact_id" ]]; then
      log_error "Unknown gluten module in classpath: $entry" >&2
      log_error "Please add it to MODULE_MAP in this script." >&2
      exit 1
    fi

    local class_type="classes"
    [[ "$filename" == *"-tests.jar" ]] && class_type="test-classes"

    local target_dir=$(get_target_dir "$artifact_id" "$scala_version" "$class_type")
    if [[ -d "$target_dir" && "$added_paths" != *"$target_dir"* ]]; then
      result="${result}:${target_dir}"
      added_paths="${added_paths}:${target_dir}"
    fi
  done

  echo "${result#:}"
}

# =============================================================================
# Parse Arguments
# =============================================================================

PROFILES=""
MODULE=""
SUITE=""
TEST_METHOD=""
EXTRA_MVN_ARGS=""
ENABLE_PROFILER=false
EXPORT_ONLY=false
ENABLE_CLEAN=false
FORCE_BUILD=false
USE_MVND=false

while [[ $# -gt 0 ]]; do
  case $1 in
    -P*)
      PROFILES="${1#-P}"
      shift
      ;;
    -pl)
      MODULE="$2"
      shift 2
      ;;
    -s)
      SUITE="$2"
      shift 2
      ;;
    -t)
      TEST_METHOD="$2"
      shift 2
      ;;
    --profile)
      ENABLE_PROFILER=true
      shift
      ;;
    --clean)
      ENABLE_CLEAN=true
      shift
      ;;
    --force)
      FORCE_BUILD=true
      shift
      ;;
    --mvnd)
      USE_MVND=true
      shift
      ;;
    --export-only)
      EXPORT_ONLY=true
      shift
      ;;
    --help)
      print_usage
      exit 0
      ;;
    *)
      # Collect other arguments for Maven
      EXTRA_MVN_ARGS="${EXTRA_MVN_ARGS} $1"
      shift
      ;;
  esac
done

# Validate required arguments
if [[ -z "$PROFILES" ]]; then
  log_error "Missing required argument: -P<profiles>"
  print_usage
  exit 1
fi

if [[ -z "$MODULE" ]]; then
  log_error "Missing required argument: -pl <module>"
  print_usage
  exit 1
fi

if [[ -z "$SUITE" ]]; then
  log_error "Missing required argument: -s <suite>"
  print_usage
  exit 1
fi

# Detect Scala version
SCALA_VERSION=$(detect_scala_version "$PROFILES")
log_info "Detected Scala version: ${SCALA_VERSION}"

# =============================================================================
# Build Cache - Skip Maven when source files haven't changed
# =============================================================================
#
# After a successful Maven build, a sentinel file is touched. On subsequent
# runs, if no .scala/.java/pom.xml files are newer than the sentinel, Maven
# is skipped entirely and cached classpath/JVM args are reused. This saves
# ~52s of Zinc analysis loading overhead per run.
#
# Use --force to bypass the cache, or --clean which implicitly bypasses it.
# =============================================================================

CACHE_DIR="${GLUTEN_HOME}/.run-scala-test-cache"
mkdir -p "$CACHE_DIR"
CACHE_KEY=$(echo "${PROFILES}__${MODULE}" | md5sum | cut -d' ' -f1)
BUILD_SENTINEL="${CACHE_DIR}/sentinel_${CACHE_KEY}"
CLASSPATH_CACHE="${CACHE_DIR}/classpath_${CACHE_KEY}.txt"
JVM_ARGS_CACHE="${CACHE_DIR}/jvm_args_${CACHE_KEY}.txt"

can_skip_maven() {
  [[ "$ENABLE_CLEAN" == "true" ]] && return 1
  [[ "$FORCE_BUILD" == "true" ]] && return 1
  [[ ! -f "$BUILD_SENTINEL" || ! -f "$CLASSPATH_CACHE" || ! -f "$JVM_ARGS_CACHE" ]] && return 1

  # Check if any source file or pom.xml changed since last successful build
  local changed
  changed=$(find "${GLUTEN_HOME}" \
    \( -path "*/target" -o -path "*/.git" -o -path "*/.run-scala-test-cache" \
       -o -path "*/.profiler" -o -path "*/.mvn" \) -prune -o \
    -newer "$BUILD_SENTINEL" \( \
      -name "pom.xml" -o \
      \( -path "*/src/*" \( -name "*.scala" -o -name "*.java" \) \) \
    \) -print 2>/dev/null | head -1)

  [[ -z "$changed" ]]
}

save_build_cache() {
  echo "$RAW_CLASSPATH" > "$CLASSPATH_CACHE"
  echo "$JVM_ARGS" > "$JVM_ARGS_CACHE"
  touch "$BUILD_SENTINEL"
  log_info "Build cache saved"
}

if can_skip_maven; then
  log_step "Steps 1-2: Using cached build (no source changes detected)"
  RAW_CLASSPATH=$(cat "$CLASSPATH_CACHE")
  JVM_ARGS=$(cat "$JVM_ARGS_CACHE")
  TIMING_STEP1=0
  TIMING_STEP2=0
  CACHE_HIT=true
  log_info "Cached classpath (${#RAW_CLASSPATH} chars)"
  log_info "Cached JVM args: ${JVM_ARGS:0:100}..."
  log_info "Maven compilation skipped — use --force to rebuild"
else

# =============================================================================
# Step 0: Ensure maven-profiler extension is installed (optional)
# =============================================================================

MVN_EXTENSIONS_FILE="${GLUTEN_HOME}/.mvn/extensions.xml"
if [[ ! -f "${MVN_EXTENSIONS_FILE}" ]]; then
  log_info "Creating .mvn/extensions.xml for maven-profiler..."
  mkdir -p "${GLUTEN_HOME}/.mvn"
  cat > "${MVN_EXTENSIONS_FILE}" << 'EXTENSIONS_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<extensions>
  <extension>
    <groupId>fr.jcgay.maven</groupId>
    <artifactId>maven-profiler</artifactId>
    <version>3.3</version>
  </extension>
</extensions>
EXTENSIONS_EOF
fi

# =============================================================================
# Step 1: Compile and get classpath
# =============================================================================

log_step "Step 1: Compiling and getting classpath..."

TIMER_STEP1_START=$(timer_now)

CLASSPATH_FILE="/tmp/gluten-test-classpath-$$.txt"

cd "${GLUTEN_HOME}"

log_info "Running: ./build/mvn test-compile dependency:build-classpath -pl ${MODULE} -am -P${PROFILES} ..."

# Enable Maven profiler if requested
if [[ "$ENABLE_PROFILER" == "true" ]]; then
  EXTRA_MVN_ARGS="${EXTRA_MVN_ARGS} -Dprofile -DprofileFormat=JSON"
  log_info "Profiler enabled - timing summary will be printed; JSON reports in .profiler/"
fi

# Build Maven goals
MVN_GOALS="test-compile dependency:build-classpath"
if [[ "$ENABLE_CLEAN" == "true" ]]; then
  MVN_GOALS="clean ${MVN_GOALS}"
  log_info "Clean build requested"
fi

# Select Maven command: --mvnd uses Maven Daemon (persistent JVM that
# keeps Zinc's JIT-optimized code and classloader caches across builds).
if [[ "$USE_MVND" == "true" ]]; then
  MVN_CMD="./build/mvnd"
  log_info "Using Maven Daemon (mvnd) for faster builds"
else
  MVN_CMD="./build/mvn"
fi

${MVN_CMD} ${MVN_GOALS} \
  -T 1C -q \
  -pl "${MODULE}" -am \
  -P"${PROFILES},fast-build" \
  -DincludeScope=test \
  -Dmdep.outputFile="${CLASSPATH_FILE}" \
  ${EXTRA_MVN_ARGS}

if [[ ! -f "${CLASSPATH_FILE}" ]]; then
  log_error "Failed to generate classpath file"
  exit 1
fi

RAW_CLASSPATH=$(cat "${CLASSPATH_FILE}")
log_info "Got raw classpath (${#RAW_CLASSPATH} chars)"

TIMER_STEP1_END=$(timer_now)
TIMING_STEP1=$(timer_elapsed $TIMER_STEP1_START $TIMER_STEP1_END)
log_timing "Step 1 - Compile + Classpath (mvn)" "$TIMING_STEP1"

# =============================================================================
# Step 2: Get JVM arguments
# =============================================================================

log_step "Step 2: Getting JVM arguments from pom.xml..."

TIMER_STEP2_START=$(timer_now)

# Always use build/mvn for help:evaluate (mvnd returns empty output for this goal)
JVM_ARGS_RAW=$(./build/mvn help:evaluate \
  -Dexpression=extraJavaTestArgs \
  -q -DforceStdout \
  -P"${PROFILES}" 2>/dev/null || echo "")

# Clean up JVM args: remove comments, trim whitespace, convert to single line
JVM_ARGS=$(echo "$JVM_ARGS_RAW" | \
  grep -v '<!--' | \
  grep -v '^\s*$' | \
  tr '\n' ' ' | \
  tr -s ' ')

log_info "JVM args: ${JVM_ARGS:0:100}..."

TIMER_STEP2_END=$(timer_now)
TIMING_STEP2=$(timer_elapsed $TIMER_STEP2_START $TIMER_STEP2_END)
log_timing "Step 2 - Evaluate JVM args (mvn)" "$TIMING_STEP2"
# Save to cache for next run
save_build_cache

fi  # end of can_skip_maven else branch
# =============================================================================
# Step 3: Replace gluten paths with target/classes
# =============================================================================

log_step "Step 3: Replacing gluten jar paths with target/classes..."

TIMER_STEP3_START=$(timer_now)

RESOLVED_CLASSPATH=$(replace_gluten_paths "$RAW_CLASSPATH" "$SCALA_VERSION")

# Also add the target module's test-classes at the beginning (highest priority)
MODULE_TEST_CLASSES="${GLUTEN_HOME}/${MODULE}/target/scala-${SCALA_VERSION}/test-classes"
MODULE_CLASSES="${GLUTEN_HOME}/${MODULE}/target/scala-${SCALA_VERSION}/classes"

if [[ -d "$MODULE_TEST_CLASSES" ]]; then
  RESOLVED_CLASSPATH="${MODULE_TEST_CLASSES}:${RESOLVED_CLASSPATH}"
fi
if [[ -d "$MODULE_CLASSES" ]]; then
  RESOLVED_CLASSPATH="${MODULE_CLASSES}:${RESOLVED_CLASSPATH}"
fi

log_info "Resolved classpath ready"

# Sanity check: no non-.m2 jar files should remain in the classpath
IFS=':' read -ra FINAL_ENTRIES <<< "$RESOLVED_CLASSPATH"
for entry in "${FINAL_ENTRIES[@]}"; do
  if [[ "$entry" == *.jar && "$entry" != *"/.m2/"* ]]; then
    log_error "Non-.m2 jar found in resolved classpath: $entry"
    log_error "All gluten jars should be replaced with target/classes or target/test-classes directories."
    exit 1
  fi
done

TIMER_STEP3_END=$(timer_now)
TIMING_STEP3=$(timer_elapsed $TIMER_STEP3_START $TIMER_STEP3_END)
log_timing "Step 3 - Resolve classpath" "$TIMING_STEP3"

# =============================================================================
# Step 3.5: Create pathing JAR
# =============================================================================
# A "pathing JAR" is a thin JAR whose MANIFEST.MF Class-Path header lists all
# the real classpath entries as file: URIs. This avoids exceeding OS command-line
# length limits. Works on Java 8+ (unlike @argfile which requires Java 9+).
# Also includes META-INF/classpath.txt for human-readable review.
# =============================================================================

PATHING_JAR="/tmp/gluten-test-pathing-$$.jar"
rm -f "${PATHING_JAR}"

PATHING_MANIFEST_DIR="/tmp/gluten-test-manifest-$$"
mkdir -p "${PATHING_MANIFEST_DIR}/META-INF"

# Convert colon-separated classpath to space-separated file: URIs for manifest.
# Also write a human-readable classpath listing (one entry per line, same order).
CP_URIS=""
IFS=':' read -ra _CP_ITEMS <<< "${RESOLVED_CLASSPATH}"
: > "${PATHING_MANIFEST_DIR}/META-INF/classpath.txt"

for _item in "${_CP_ITEMS[@]}"; do
  [[ -z "$_item" ]] && continue
  echo "$_item" >> "${PATHING_MANIFEST_DIR}/META-INF/classpath.txt"
  # Ensure directories end with / (required by Class-Path spec for directories)
  [[ -d "$_item" && "$_item" != */ ]] && _item="${_item}/"
  CP_URIS="${CP_URIS} file://${_item}"
done
CP_URIS="${CP_URIS# }"

# Write manifest with proper 72-byte line wrapping.
# MANIFEST.MF spec: max 72 bytes per line; continuation lines start with
# a single space character.
{
  echo "Manifest-Version: 1.0"
  _mf_header="Class-Path: ${CP_URIS}"
  echo "${_mf_header:0:72}"
  _mf_rest="${_mf_header:72}"
  while [[ -n "$_mf_rest" ]]; do
    echo " ${_mf_rest:0:71}"
    _mf_rest="${_mf_rest:71}"
  done
  echo ""
} > "${PATHING_MANIFEST_DIR}/META-INF/MANIFEST.MF"

# Build the pathing JAR (manifest + human-readable classpath listing)
(cd "${PATHING_MANIFEST_DIR}" && jar cfm "${PATHING_JAR}" META-INF/MANIFEST.MF META-INF/classpath.txt)
rm -rf "${PATHING_MANIFEST_DIR}"
log_info "Created pathing JAR: ${PATHING_JAR} ($(du -h "${PATHING_JAR}" | cut -f1))"
log_info "  Review classpath: unzip -p ${PATHING_JAR} META-INF/classpath.txt"

# =============================================================================
# Build java command (shared by export-only and run modes)
# =============================================================================

JAVA_CMD="${JAVA_HOME}/bin/java"
[[ ! -x "$JAVA_CMD" ]] && JAVA_CMD="java"

JAVA_ARGS=(
  ${JVM_ARGS}
  "-Dlog4j.configurationFile=file:${GLUTEN_HOME}/${MODULE}/src/test/resources/log4j2.properties"
  -cp "${PATHING_JAR}"
  org.scalatest.tools.Runner
  -oDF
  -s "${SUITE}"
)
[[ -n "$TEST_METHOD" ]] && JAVA_ARGS+=(-t "${TEST_METHOD}")

# =============================================================================
# Step 3.6: Export-only mode (if requested)
# =============================================================================

if [[ "$EXPORT_ONLY" == "true" ]]; then
  echo ""
  echo -e "${YELLOW}# Run the test with:${NC}"
  echo "${JAVA_CMD} ${JAVA_ARGS[*]}"
  echo ""
  print_timing_summary false
  exit 0
fi

# =============================================================================
# Step 4: Run ScalaTest
# =============================================================================

log_step "Step 4: Running ScalaTest..."

log_info "Suite: ${SUITE}"
[[ -n "$TEST_METHOD" ]] && log_info "Test method: ${TEST_METHOD}"

echo ""
echo "=========================================="
echo "Running ScalaTest"
echo "=========================================="
echo ""

TIMER_STEP4_START=$(timer_now)

TEST_EXIT_CODE=0
${JAVA_CMD} "${JAVA_ARGS[@]}" || TEST_EXIT_CODE=$?

rm -f "${PATHING_JAR}"

TIMER_STEP4_END=$(timer_now)
TIMING_STEP4=$(timer_elapsed $TIMER_STEP4_START $TIMER_STEP4_END)
log_timing "Step 4 - ScalaTest execution" "$TIMING_STEP4"

print_timing_summary true
exit $TEST_EXIT_CODE
