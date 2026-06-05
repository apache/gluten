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

# Incremental C++ build for Gluten + Velox (no source reset, no Maven, no OS setup)
# Designed for vibe coding: lets AI agents iterate on C++ code without full rebuilds.
# Only applicable when vcpkg is enabled for static linking.
# Usage: ./dev/builddep-veloxbe-inc.sh [--build_type=Debug|Release] [--update_vcpkg]

set -eu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."

# Helper functions
num_threads() {
  [[ -n "${NUM_THREADS:-}" ]] && echo "$NUM_THREADS" && return
  if [[ "$(uname)" == "Darwin" ]]; then
    local cores=$(sysctl -n hw.physicalcpu)
    echo $(( cores > 2 ? cores - 2 : cores ))
  else
    echo $(nproc --ignore=2)
  fi
}

platform() {
  [[ "$(uname -s)" == "Linux" ]] && echo "linux" || echo "darwin"
}

arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "amd64" ;;
    *) echo "aarch64" ;;
  esac
}

step() { echo -e "\n[Step $1/4] $2..."; }

extract_opt() {
  grep "^$1:" "$GLUTEN_BUILD_DIR/CMakeCache.txt" 2>/dev/null | \
    cut -d= -f2 || echo "${2:-ON}"
}

# Defaults
VELOX_HOME="$GLUTEN_DIR/ep/build-velox/build/velox_ep"
UPDATE_VCPKG=OFF
NUM_THREADS=${NUM_THREADS:-$(num_threads)}

# Parse arguments
for arg in "$@"; do
    case $arg in
        --build_type=*)    BUILD_TYPE="${arg#*=}" ;;
        --scala_version=*) SCALA_VERSION="${arg#*=}" ;;
        --velox_home=*)    VELOX_HOME="${arg#*=}" ;;
        --num_threads=*)   NUM_THREADS="${arg#*=}" ;;
        --update_vcpkg)    UPDATE_VCPKG=ON ;;
        *)
            echo "Unknown: $arg"
            echo "Usage: $0 [--build_type=Debug|Release] [--update_vcpkg]"
            exit 1
            ;;
    esac
done

# Auto-detect BUILD_TYPE
if [ -z "${BUILD_TYPE:-}" ]; then
    [ -d "$VELOX_HOME/_build/debug" ] && BUILD_TYPE=Debug || BUILD_TYPE=Release
fi

# Auto-detect SCALA_VERSION
if [ -z "${SCALA_VERSION:-}" ]; then
    if [ -d "$GLUTEN_DIR/backends-velox/target/scala-2.13" ]; then
        SCALA_VERSION=2.13
    else
        SCALA_VERSION=2.12
    fi
fi

# Paths
VELOX_BUILD_DIR_NAME=$([[ "$BUILD_TYPE" =~ ^[Dd]ebug$ ]] && echo 'debug' || echo 'release')
VELOX_BUILD_DIR="$VELOX_HOME/_build/$VELOX_BUILD_DIR_NAME"
GLUTEN_BUILD_DIR="$GLUTEN_DIR/cpp/build"

# Validate directories
for dir in "$VELOX_HOME" "$VELOX_BUILD_DIR" "$GLUTEN_BUILD_DIR"; do
    if [ ! -d "$dir" ]; then
        echo "ERROR: Missing $dir"
        echo "Run: ./dev/package-vcpkg.sh --build_type=$BUILD_TYPE"
        exit 1
    fi
done

# Validate CMake cache
for cache in "$VELOX_BUILD_DIR/CMakeCache.txt" "$GLUTEN_BUILD_DIR/CMakeCache.txt"; do
    if [ ! -f "$cache" ]; then
        echo "ERROR: Missing $cache. Run full build first"
        exit 1
    fi
done

echo "============================================"
echo "Gluten C++ Incremental Build"
echo "BUILD_TYPE: $BUILD_TYPE | THREADS: $NUM_THREADS"
echo "============================================"

cd "$GLUTEN_DIR"

# Step 1: vcpkg
BUILD_OPTIONS="--build_tests=$(extract_opt BUILD_TESTS)"
BUILD_OPTIONS="$BUILD_OPTIONS --enable_s3=$(extract_opt ENABLE_S3)"
BUILD_OPTIONS="$BUILD_OPTIONS --enable_gcs=$(extract_opt ENABLE_GCS)"
BUILD_OPTIONS="$BUILD_OPTIONS --enable_abfs=$(extract_opt ENABLE_ABFS OFF)"

if [ "$UPDATE_VCPKG" = "ON" ]; then
    step 1 "Updating vcpkg dependencies"
    source "$GLUTEN_DIR/dev/vcpkg/env.sh" $BUILD_OPTIONS
    echo "[Step 1/4] vcpkg dependencies updated."
else
    step 1 "Skipping vcpkg check (use --update_vcpkg to update)"
    VCPKG_ROOT="$GLUTEN_DIR/dev/vcpkg/.vcpkg"
    export VCPKG_ROOT

    if [ "${CPU_TARGET:-}" = "aarch64" ]; then
        export VCPKG_TRIPLET="arm64-linux-neon"
    else
        export VCPKG_TRIPLET="x64-linux-avx"
    fi

    export VCPKG_TRIPLET_INSTALL_DIR="$GLUTEN_DIR/dev/vcpkg/vcpkg_installed/${VCPKG_TRIPLET}"
    export CMAKE_TOOLCHAIN_FILE="$GLUTEN_DIR/dev/vcpkg/toolchain.cmake"
    export PKG_CONFIG_PATH="${VCPKG_TRIPLET_INSTALL_DIR}/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
    export GLUTEN_VCPKG_ENABLED="${VCPKG_ROOT}"
fi

# Step 2: Build Velox
step 2 "Building Velox (incremental)"
cmake --build "$VELOX_BUILD_DIR" --target velox -j $NUM_THREADS
echo "[Step 2/4] Velox build complete."

# Step 3: Build Gluten C++
step 3 "Building Gluten C++ (incremental)"
cmake --build "$GLUTEN_BUILD_DIR" --target gluten velox -j $NUM_THREADS
echo "[Step 3/4] Gluten C++ build complete."

# Step 4: Copy libraries
step 4 "Copying shared libraries to backends-velox target"
CPP_RELEASES="$GLUTEN_DIR/cpp/build/releases"
if [ ! -d "$CPP_RELEASES" ]; then
    echo "ERROR: CPP releases directory not found. Build may have failed."
    exit 1
fi

TARGET_DIR="$GLUTEN_DIR/backends-velox/target/scala-${SCALA_VERSION}"
TARGET_DIR="$TARGET_DIR/classes/$(platform)/$(arch)"
mkdir -p "$TARGET_DIR"
cp -u -v "$CPP_RELEASES"/lib*.so "$TARGET_DIR/"

echo -e "\n============================================"
echo "Build complete! Libraries copied to:"
echo "$TARGET_DIR"
ls -lh "$TARGET_DIR"/lib*.so
echo "============================================"
