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
# Verify the bundled gluten-velox jar's Arrow C-Data classes have method
# signatures referencing the *unshaded* org.apache.arrow.memory.BufferAllocator
# and org.apache.arrow.vector.* types — not the gluten-shaded copies.
#
# Background: org.apache.arrow.c.* must NOT be relocated (its native JNI binds
# to the original class names), but its public API methods accept/return
# org.apache.arrow.memory.* and org.apache.arrow.vector.* types. Those types
# must therefore also stay unshaded in the bundle, otherwise the bundled
# ArrowArrayStream/ArrowSchema get re-bound to the shaded BufferAllocator at
# compile time and any caller passing a vanilla Apache Arrow allocator hits
# `NoSuchMethodError`. See gluten#12225.
#
# Usage:
#   dev/check-arrow-c-shading.sh <path-to-gluten-velox-bundle.jar>
#
# Exit codes:
#   0 — bundle is well-shaded (Arrow C-Data API uses public Apache Arrow types)
#   1 — bundle is broken (Arrow C-Data API references gluten-shaded types)
#   2 — usage / setup error

set -euo pipefail

JAR="${1:?usage: $0 <path-to-gluten-velox-bundle.jar>}"
if [[ ! -f "$JAR" ]]; then
  echo "error: jar not found: $JAR" >&2
  exit 2
fi

if ! command -v javap >/dev/null; then
  echo "error: javap not found on PATH" >&2
  exit 2
fi

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

# Classes whose public API touches the unshaded boundary.
CLASSES=(
  "org/apache/arrow/c/ArrowArrayStream"
  "org/apache/arrow/c/ArrowSchema"
  "org/apache/arrow/c/ArrowArray"
  "org/apache/arrow/c/Data"
)

failures=0
for cls in "${CLASSES[@]}"; do
  if ! unzip -p "$JAR" "${cls}.class" > "$WORKDIR/$(basename "$cls").class" 2>/dev/null; then
    echo "  SKIP $cls (not in bundle)"
    continue
  fi
  signatures=$(javap -p "$WORKDIR/$(basename "$cls").class" 2>/dev/null || true)
  # Any method signature mentioning the gluten-shaded Arrow path is the bug.
  bad=$(echo "$signatures" | grep -E "org\.apache\.gluten\.shaded\.org\.apache\.arrow\.(memory|vector)\." || true)
  if [[ -n "$bad" ]]; then
    echo "  FAIL $cls — public API references gluten-shaded Arrow types:"
    echo "$bad" | sed 's/^/    /'
    failures=$((failures + 1))
  else
    echo "  OK   $cls"
  fi
done

if (( failures > 0 )); then
  echo
  echo "Bundle has $failures Arrow C-Data class(es) with shaded API types."
  echo "See gluten#12225 for context. Update package/pom.xml's"
  echo "<relocation org.apache.arrow> excludes to also exclude"
  echo "org.apache.arrow.memory.** and org.apache.arrow.vector.**."
  exit 1
fi

echo
echo "All Arrow C-Data classes use unshaded public Apache Arrow API. ✓"
