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

GLUTEN_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BOLT_ROOT=$(cd "${1:?Usage: ci-bolt-build.sh BOLT_CHECKOUT}" && pwd -P)
export PATH="${HOME}/miniconda3/bin:${PATH}"
export CC=${CC:-clang-22}
export CXX=${CXX:-clang++-22}
BUILD_THREADS=${CI_NUM_THREADS:-$(nproc)}
REPORTS="$GLUTEN_ROOT/bolt-ci-reports/native"
mkdir -p "$REPORTS"

for tool in git make cmake ninja conan javac; do
  command -v "$tool" > /dev/null
done
if [[ -n "${CCACHE_DIR:-}" ]]; then
  command -v ccache > /dev/null
  ccache --show-config > "$REPORTS/ccache-config.txt"
fi

# Inspect the generated commands, since a persistent directory alone does not
# enable ccache. Bolt's compiler launcher and Gluten's compile rule enable it.
record_ccache_commands() {
  local build_dir="$1" report_name="$2"
  shift 2
  if [[ -n "${CCACHE_DIR:-}" ]]; then
    ninja -C "$build_dir" -t commands "$@" > "$REPORTS/$report_name"
    if ! grep -Eq '(^|[[:space:]])([^[:space:]]*/)?ccache[[:space:]].* -c ' "$REPORTS/$report_name"; then
      echo "No ccache compiler command found in $build_dir" >&2
      exit 1
    fi
  fi
}

# Use Bolt's CI toolchain profile for both Bolt and Gluten.
python "$BOLT_ROOT/scripts/configure-conan-profile.py" \
  --compiler "$CC" --libcxx libstdc++11 --linker mold --lto none
git -C "$BOLT_ROOT" diff --exit-code HEAD --
BOLT_SHA=$(git -C "$BOLT_ROOT" rev-parse HEAD)
export BOLT_BUILD_VERSION="ci-$BOLT_SHA"
printf '%s\n' "$BOLT_SHA" > "$REPORTS/bolt-sha.txt"
git -C "$GLUTEN_ROOT" rev-parse HEAD > "$REPORTS/gluten-sha.txt"
git -C "$GLUTEN_ROOT" diff HEAD -- cpp > "$REPORTS/gluten-native.patch"

BOLT_OPTIONS=(-o 'bolt/*:spark_compatible=True' -o 'bolt/*:enable_testutil=True'
  -o 'bolt/*:shared=False' -o 'bolt/*:enable_hdfs=True'
  -o 'bolt/*:enable_s3=True' -o 'bolt/*:enable_asan=False')
HOST_PROFILE=(-pr:h default)
if [[ -f "$BOLT_ROOT/scripts/conan/bolt.profile" ]]; then
  HOST_PROFILE+=(-pr:h "$BOLT_ROOT/scripts/conan/bolt.profile")
fi
printf -v HOST_PROFILE_ARGS '%q ' "${HOST_PROFILE[@]}"

# Use Bolt's default setup-dev-env.sh before this entry point. Configure its
# test-support dependencies, but build only the engine and test-support library.
cd "$BOLT_ROOT"
make release_spark_with_test BOLT_CONAN_CONFIGURE_ONLY=1 \
  "BUILD_VERSION=$BOLT_BUILD_VERSION" "CONAN_OVERRIDE=${BOLT_OPTIONS[*]}" \
  "CONAN_HOST_PROFILE_ARGS=$HOST_PROFILE_ARGS"
source "$BOLT_ROOT/_build/Release/generators/conanbuild.sh"
record_ccache_commands "$BOLT_ROOT/_build/Release" bolt-build-commands.txt \
  bolt_engine bolt_testutils_static
cmake --build --preset conan-release --target bolt_engine bolt_testutils_static \
  --parallel "$BUILD_THREADS"
conan export-pkg . --name=bolt "--version=$BOLT_BUILD_VERSION" \
  "${HOST_PROFILE[@]}" -s build_type=Release "${BOLT_OPTIONS[@]}" \
  -c 'bolt/*:tools.build:skip_test=False' --test-folder="" \
  --format=json > "$REPORTS/bolt-export.json"

cd "$GLUTEN_ROOT/cpp"
conan install . --name=gluten --version=ci-build \
  "${HOST_PROFILE[@]}" -s build_type=Release "${BOLT_OPTIONS[@]}" \
  -o 'gluten/*:shared=True' -o 'gluten/*:build_tests=True' \
  -o 'gluten/*:build_examples=True' -o 'gluten/*:build_benchmarks=True' \
  -o 'gluten/*:enable_hdfs=True' -o 'gluten/*:enable_s3=True' \
  -o 'gluten/*:enable_asan=False' -c tools.build:skip_test=True \
  -c 'bolt/*:tools.build:skip_test=False' \
  -c 'arrow/*:tools.build:download_source=True' \
  --build=missing --build='~bolt/*' --lockfile= --format=json \
  > "$REPORTS/gluten-install.json"
source "$GLUTEN_ROOT/cpp/build/Release/generators/conanbuild.sh"
{
  git --version
  "$CC" --version
  "$CXX" --version
  javac -version
  conan --version
  cmake --version
} > "$REPORTS/tool-versions.txt" 2>&1
# POST_BUILD discovery would execute test binaries even without ctest.
cmake --preset conan-release -DCMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE=PRE_TEST
record_ccache_commands "$GLUTEN_ROOT/cpp/build/Release" gluten-build-commands.txt
cmake --build --preset conan-release --parallel "$BUILD_THREADS"
if [[ -n "${CCACHE_DIR:-}" ]]; then
  ccache --show-stats > "$REPORTS/ccache-stats.txt"
fi

python3 - "$REPORTS" <<'PY'
import json
import sys
from pathlib import Path

reports = Path(sys.argv[1])
def identity(name):
    nodes = json.loads((reports / name).read_text())["graph"]["nodes"].values()
    bolt = [n for n in nodes if n.get("name") == "bolt" and n.get("context") == "host"]
    if len(bolt) != 1:
        raise RuntimeError("Expected one locally built Bolt package")
    return {key: bolt[0].get(key) for key in ("ref", "package_id", "prev")}
exported, consumed = identity("bolt-export.json"), identity("gluten-install.json")
if any(not value for value in exported.values()) or exported != consumed:
    raise RuntimeError(f"Bolt package mismatch: {exported} / {consumed}")
(reports / "bolt-package.json").write_text(json.dumps(consumed, indent=2) + "\n")
PY

cd "$GLUTEN_ROOT"
for library in libgluten.so libbolt_backend.so libglutenlibloader.so; do
  test -s "cpp/build/package/bolt/linux/amd64/$library"
done
