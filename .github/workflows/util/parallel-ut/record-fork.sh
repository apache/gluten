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

# Stand-in for the java executable of scalatest-maven-plugin (-Dscalatest.jvm=<this file>).
# Instead of running the suites, it saves the fork's working directory, environment and
# arguments under $GLUTEN_UT_FORK_DIR so run.py can replay them across parallel JVMs.
set -euo pipefail
mkdir -p "${GLUTEN_UT_FORK_DIR:?GLUTEN_UT_FORK_DIR must be set}"
out=$(mktemp -d "$GLUTEN_UT_FORK_DIR/fork-XXXXXX")
pwd > "$out/cwd"
env -0 > "$out/env"
printf '%s\0' "$@" > "$out/args"
