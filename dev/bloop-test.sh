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
# bloop-test.sh - Run Scala Tests via Bloop
# =============================================================================
#
# A convenience wrapper matching the run-scala-test.sh interface for running
# tests via Bloop instead of Maven. Requires bloop configuration to be
# generated first via bloop-setup.sh.
#
# Usage:
#   ./dev/bloop-test.sh -pl <module> -s <suite> [-t "test name"]
#
# Examples:
#   # Run entire suite
#   ./dev/bloop-test.sh -pl gluten-ut/spark35 -s GlutenSQLQuerySuite
#
#   # Run specific test method
#   ./dev/bloop-test.sh -pl gluten-ut/spark35 -s GlutenSQLQuerySuite -t "test method"
#
#   # Run with wildcard pattern
#   ./dev/bloop-test.sh -pl gluten-ut/spark40 -s '*Aggregate*'
#
# Prerequisites:
#   1. Install bloop CLI: cs install bloop
#   2. Generate config: ./dev/bloop-setup.sh -P<profiles>
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

# =============================================================================
# Module Mapping: Maven module path -> Bloop project name
# =============================================================================
# Bloop uses Maven artifactIds as project names.
# This maps the -pl module path format to the corresponding bloop project name.

declare -A MODULE_MAP=(
  # Core modules
  ["gluten-core"]="gluten-core"
  ["gluten-substrait"]="gluten-substrait"
  ["gluten-ui"]="gluten-ui"
  ["gluten-arrow"]="gluten-arrow"

  # Backend modules
  ["backends-velox"]="backends-velox"
  ["backends-clickhouse"]="backends-clickhouse"

  # RAS modules
  ["gluten-ras/common"]="gluten-ras-common"
  ["gluten-ras/planner"]="gluten-ras-planner"

  # Shims modules
  ["shims/common"]="spark-sql-columnar-shims-common"
  ["shims/spark32"]="spark-sql-columnar-shims-spark32"
  ["shims/spark33"]="spark-sql-columnar-shims-spark33"
  ["shims/spark34"]="spark-sql-columnar-shims-spark34"
  ["shims/spark35"]="spark-sql-columnar-shims-spark35"
  ["shims/spark40"]="spark-sql-columnar-shims-spark40"
  ["shims/spark41"]="spark-sql-columnar-shims-spark41"

  # Unit test modules
  ["gluten-ut/common"]="gluten-ut-common"
  ["gluten-ut/test"]="gluten-ut-test"
  ["gluten-ut/spark32"]="gluten-ut-spark32"
  ["gluten-ut/spark33"]="gluten-ut-spark33"
  ["gluten-ut/spark34"]="gluten-ut-spark34"
  ["gluten-ut/spark35"]="gluten-ut-spark35"
  ["gluten-ut/spark40"]="gluten-ut-spark40"
  ["gluten-ut/spark41"]="gluten-ut-spark41"

  # Data lake modules
  ["gluten-delta"]="gluten-delta"
  ["gluten-iceberg"]="gluten-iceberg"
  ["gluten-hudi"]="gluten-hudi"
  ["gluten-paimon"]="gluten-paimon"

  # Shuffle modules
  ["gluten-celeborn"]="gluten-celeborn"
  ["gluten-uniffle"]="gluten-uniffle"

  # Other modules
  ["gluten-kafka"]="gluten-kafka"
)

# =============================================================================
# Functions
# =============================================================================

print_usage() {
  cat << EOF
Usage: $0 -pl <module> -s <suite> [-t "test name"]

Run Scala tests via Bloop (faster than Maven).

Required:
  -pl <module>      Target module (e.g., gluten-ut/spark40)
  -s <suite>        Suite class name (can be short name or fully qualified)

Optional:
  -t "test name"    Specific test method name to run
  --help            Show this help message

Examples:
  # Run entire suite
  $0 -pl gluten-ut/spark35 -s GlutenSQLQuerySuite

  # Run specific test method
  $0 -pl gluten-ut/spark35 -s GlutenSQLQuerySuite -t "test method name"

  # Run suites matching pattern
  $0 -pl gluten-ut/spark40 -s '*Aggregate*'

Prerequisites:
  1. Install bloop CLI: cs install bloop
  2. Generate config: ./dev/bloop-setup.sh -P<your-profiles>
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

# Get bloop project name from module path
get_bloop_project() {
  local module="$1"
  local project="${MODULE_MAP[$module]}"

  if [[ -z "$project" ]]; then
    # Try using the module path as-is (may work for simple cases)
    # Replace slashes with dashes for bloop naming convention
    project=$(echo "$module" | tr '/' '-')
  fi

  echo "$project"
}

# =============================================================================
# Parse Arguments
# =============================================================================

MODULE=""
SUITE=""
TEST_METHOD=""

while [[ $# -gt 0 ]]; do
  case $1 in
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
    --help)
      print_usage
      exit 0
      ;;
    *)
      log_error "Unknown argument: $1"
      print_usage
      exit 1
      ;;
  esac
done

# Validate required arguments
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

# =============================================================================
# Check Prerequisites
# =============================================================================

cd "${GLUTEN_HOME}"

# Check for bloop CLI
if ! command -v bloop &> /dev/null; then
  log_error "Bloop CLI not found. Install with: cs install bloop"
  exit 1
fi

# Check for .bloop directory
if [[ ! -d ".bloop" ]]; then
  log_error "Bloop configuration not found (.bloop directory missing)"
  echo ""
  echo "Generate configuration first:"
  echo "  ./dev/bloop-setup.sh -P<your-profiles>"
  echo ""
  echo "Example:"
  echo "  ./dev/bloop-setup.sh -Pspark-3.5,scala-2.12,backends-velox"
  exit 1
fi

# =============================================================================
# Run Tests
# =============================================================================

PROJECT=$(get_bloop_project "$MODULE")

# Verify project exists
if [[ ! -f ".bloop/${PROJECT}.json" ]]; then
  log_error "Bloop project '${PROJECT}' not found"
  echo ""
  echo "Available projects:"
  bloop projects 2>/dev/null | head -20
  echo ""
  echo "If your project is not listed, regenerate config with the correct profiles:"
  echo "  ./dev/bloop-setup.sh -P<profiles>"
  exit 1
fi

log_step "Running tests via Bloop"
log_info "Project: ${PROJECT}"
log_info "Suite: ${SUITE}"
[[ -n "$TEST_METHOD" ]] && log_info "Test method: ${TEST_METHOD}"

echo ""
echo "=========================================="
echo "Running Tests via Bloop"
echo "=========================================="
echo ""

# Build test command
BLOOP_ARGS=("test" "$PROJECT" "-o" "$SUITE")

# Add test method filter if specified
# Note: Bloop uses -t for test name filtering (same as ScalaTest)
if [[ -n "$TEST_METHOD" ]]; then
  BLOOP_ARGS+=("--" "-t" "$TEST_METHOD")
fi

# Set SPARK_ANSI_SQL_MODE=false by default for Gluten tests
# Spark 4.x enables ANSI mode by default, which is incompatible with some Gluten features
export SPARK_ANSI_SQL_MODE="${SPARK_ANSI_SQL_MODE:-false}"

# Execute tests
bloop "${BLOOP_ARGS[@]}"
