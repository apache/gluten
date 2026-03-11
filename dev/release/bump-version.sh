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

set -euo pipefail

usage() {
  echo "Usage: $0 <new-version>  e.g., 1.7.0-SNAPSHOT"
  exit 1
}

NEW_VERSION="${1:-}"; [[ -n "$NEW_VERSION" ]] || usage

# Resolve script dir
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
GLUTEN_HOME="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
MVN_BIN="${MVN:-${GLUTEN_HOME}/build/mvn}"

bump() {
  local dir="$1"
  echo "Bumping version in: $dir"
  pushd "$dir" >/dev/null
  # Avoid backup POMs; keep logs quiet; filter out warnings: "xxx no value".
  "$MVN_BIN" -q -DgenerateBackupPoms=false versions:set -DnewVersion="$NEW_VERSION" | grep -v \
    "no value" || true
  popd >/dev/null
}

bump "$GLUTEN_HOME"
bump "$GLUTEN_HOME/tools/gluten-it"
bump "$GLUTEN_HOME/tools/qualification-tool"
bump "$GLUTEN_HOME/gluten-flink"

echo "Set version to $NEW_VERSION in all modules."

