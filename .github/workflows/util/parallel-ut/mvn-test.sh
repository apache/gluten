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

# Usage: mvn-test.sh <maven test command...>
#
# Runs the Maven command with every ScalaTest fork recorded instead of run (Maven still
# compiles and runs the JUnit tests), then runs the recorded suites in ${UT_PARALLELISM:-3}
# concurrent JVMs with run.py. Extra run.py options can be passed in $PARALLEL_UT_OPTS.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
GLUTEN_UT_FORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/gluten-ut-forks-XXXXXX")
export GLUTEN_UT_FORK_DIR
"$@" -Dscalatest.jvm="$here/record-fork.sh"
# CentOS 8 images without a python3 module still ship platform-python.
python=$(command -v python3 || echo /usr/libexec/platform-python)
# shellcheck disable=SC2086
"$python" "$here/run.py" --jobs "${UT_PARALLELISM:-3}" ${PARALLEL_UT_OPTS:-}
