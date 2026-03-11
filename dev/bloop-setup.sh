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
# bloop-setup.sh - Generate Bloop Configuration for Gluten
# =============================================================================
#
# This script generates Bloop configuration files for fast incremental Scala
# compilation. Bloop maintains a persistent build server that:
#   - Keeps the JVM warm (no startup overhead)
#   - Maintains Zinc incremental compiler state
#   - Enables watch mode for automatic recompilation
#
# Usage:
#   ./dev/bloop-setup.sh -P<profiles>
#
# Examples:
#   # Velox backend with Spark 3.5
#   ./dev/bloop-setup.sh -Pspark-3.5,scala-2.12,backends-velox
#
#   # Velox backend with Spark 4.0 and unit tests
#   ./dev/bloop-setup.sh -Pjava-17,spark-4.0,scala-2.13,backends-velox,spark-ut
#
#   # ClickHouse backend
#   ./dev/bloop-setup.sh -Pspark-3.5,scala-2.12,backends-clickhouse
#
# After running this script:
#   - Use 'bloop projects' to list available projects
#   - Use 'bloop compile <project>' to compile
#   - Use 'bloop compile <project> -w' for watch mode
#   - Use 'bloop test <project>' to run tests
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
# Functions
# =============================================================================

print_usage() {
  cat << EOF
Usage: $0 -P<profiles>

Generate Bloop configuration for the specified Maven profiles.

Required:
  -P<profiles>      Maven profiles (e.g., -Pspark-3.5,scala-2.12,backends-velox)

Optional:
  --help            Show this help message

Examples:
  # Velox backend with Spark 3.5
  $0 -Pspark-3.5,scala-2.12,backends-velox

  # Velox backend with Spark 4.0 (requires JDK 17)
  $0 -Pjava-17,spark-4.0,scala-2.13,backends-velox,spark-ut

  # ClickHouse backend
  $0 -Pspark-3.5,scala-2.12,backends-clickhouse

Common Profile Combinations:
  Spark 3.5 + Velox:  -Pspark-3.5,scala-2.12,backends-velox
  Spark 4.0 + Velox:  -Pjava-17,spark-4.0,scala-2.13,backends-velox
  Spark 4.1 + Velox:  -Pjava-17,spark-4.1,scala-2.13,backends-velox
  With unit tests:    Add ,spark-ut to any profile
  With Delta:         Add ,delta to any profile
  With Iceberg:       Add ,iceberg to any profile
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

# =============================================================================
# Parse Arguments
# =============================================================================

PROFILES=""

while [[ $# -gt 0 ]]; do
  case $1 in
    -P*)
      PROFILES="${1#-P}"
      shift
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
if [[ -z "$PROFILES" ]]; then
  log_error "Missing required argument: -P<profiles>"
  print_usage
  exit 1
fi

# =============================================================================
# Check Prerequisites
# =============================================================================

log_step "Checking prerequisites..."

# Check if bloop CLI is installed
if ! command -v bloop &> /dev/null; then
  log_warn "Bloop CLI not found in PATH"
  echo ""
  echo "Install bloop CLI using one of these methods:"
  echo ""
  echo "  # Using coursier (recommended)"
  echo "  cs install bloop"
  echo ""
  echo "  # Using Homebrew (macOS)"
  echo "  brew install scalacenter/bloop/bloop"
  echo ""
  echo "  # Using apt (Debian/Ubuntu)"
  echo "  echo 'deb https://dl.bintray.com/scalacenter/releases /' | sudo tee /etc/apt/sources.list.d/sbt.list"
  echo "  sudo apt-get update && sudo apt-get install bloop"
  echo ""
  echo "See https://scalacenter.github.io/bloop/setup for more options."
  echo ""
  log_info "Continuing with bloopInstall (you'll need bloop CLI to use the generated config)..."
fi

# =============================================================================
# Generate Bloop Configuration
# =============================================================================

log_step "Generating Bloop configuration for profiles: ${PROFILES}"
log_info "This may take several minutes on first run (full Maven dependency resolution)..."

cd "${GLUTEN_HOME}"

# Remove existing bloop config to ensure clean generation
if [[ -d ".bloop" ]]; then
  log_info "Removing existing .bloop directory..."
  rm -rf .bloop
fi

# Run generate-sources + bloopInstall in single Maven invocation
# This ensures:
# 1. Protobuf and other code generators run first (generate-sources phase)
# 2. bloop-maven-plugin picks up the generated source directories
# Skip style checks for faster generation
log_step "Running generate-sources + bloopInstall..."
./build/mvn generate-sources bloop:bloopInstall \
  -P"${PROFILES}",fast-build \
  -DskipTests

if [[ ! -d ".bloop" ]]; then
  log_error "Bloop configuration generation failed - .bloop directory not created"
  exit 1
fi

# Count generated projects
PROJECT_COUNT=$(ls -1 .bloop/*.json 2>/dev/null | wc -l)

log_info "Successfully generated ${PROJECT_COUNT} project configurations in .bloop/"

# =============================================================================
# Inject JVM Options for Tests
# =============================================================================
# Bloop-maven-plugin doesn't automatically pick up Maven's argLine/extraJavaTestArgs.
# We need to inject the --add-opens flags required for Spark tests on Java 17+.

log_step "Injecting JVM test options into bloop configurations..."

# JVM options required for Spark tests (matches extraJavaTestArgs in pom.xml)
JVM_OPTIONS='[
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "-Dio.netty.tryReflectionSetAccessible=true"
]'

# Inject JVM options into each bloop config file
INJECTED_COUNT=0
for config_file in .bloop/*.json; do
  if [[ -f "$config_file" ]]; then
    # Use python to update the JSON (available on most systems)
    python3 -c "
import json
import sys

with open('$config_file', 'r') as f:
    config = json.load(f)

if 'project' in config:
    project = config['project']

    # Set JVM options in platform.config.options
    if 'platform' in project:
        platform = project['platform']
        if platform.get('name') == 'jvm' and 'config' in platform:
            platform['config']['options'] = $JVM_OPTIONS

    # Fix scala options: remove unsupported -release 21 (separate args)
    if 'scala' in project and 'options' in project['scala']:
        opts = project['scala']['options']
        new_opts = []
        skip_next = False
        for i, opt in enumerate(opts):
            if skip_next:
                skip_next = False
                continue
            if opt == '-release' and i+1 < len(opts) and opts[i+1] == '21':
                skip_next = True
                continue
            # Also remove combined form
            if opt == '-release:21':
                continue
            new_opts.append(opt)
        project['scala']['options'] = new_opts

    # Fix java options: remove --release 21 (separate args)
    if 'java' in project and 'options' in project['java']:
        opts = project['java']['options']
        new_opts = []
        skip_next = False
        for i, opt in enumerate(opts):
            if skip_next:
                skip_next = False
                continue
            if opt == '--release' and i+1 < len(opts) and opts[i+1] == '21':
                skip_next = True
                continue
            new_opts.append(opt)
        project['java']['options'] = new_opts

with open('$config_file', 'w') as f:
    json.dump(config, f, indent=2)
" 2>/dev/null && ((INJECTED_COUNT++))
  fi
done

log_info "Injected JVM options into ${INJECTED_COUNT} configurations"

# =============================================================================
# Print Usage Instructions
# =============================================================================

echo ""
echo -e "${GREEN}=========================================="
echo "  Bloop Configuration Complete"
echo -e "==========================================${NC}"
echo ""
echo "Quick Start:"
echo "  bloop projects                    # List all projects"
echo "  bloop compile gluten-core         # Compile a project"
echo "  bloop compile gluten-core -w      # Watch mode (auto-recompile)"
echo "  bloop test gluten-core            # Run tests"
echo ""
echo "Tips:"
echo "  - Use 'bloop compile -w <project>' for instant feedback during development"
echo "  - Bloop keeps a warm JVM, so subsequent compiles are much faster"
echo "  - Project names match Maven artifactIds (e.g., gluten-core, backends-velox)"
echo ""
echo "Environment Variables:"
echo "  - SPARK_ANSI_SQL_MODE=false  Required for Spark 4.x tests (set by bloop-test.sh)"
echo "  - JAVA_HOME                  Set to JDK 21 path if bloop uses wrong JDK version"
echo ""
echo "Unit Test Examples:"
echo "  bloop test gluten-ut-spark35 -o '*GlutenSuite*'   # Run matching suites"
echo "  bloop test gluten-ut-spark40 -o SomeSuite         # Run specific suite"
echo ""
echo -e "${YELLOW}Note:${NC} Re-run this script when changing Maven profiles."
echo ""
