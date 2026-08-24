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

# Dev Container postCreateCommand.
#
# This only prepares the environment; it never builds Velox. A Velox build takes
# tens of minutes to several hours, and a postCreateCommand that long stalls
# container creation and leaves a half-built tree behind when the editor
# disconnects or Codespaces times out. Run the build yourself once the container
# is up -- the command is printed at the end of this script.

set -uo pipefail

NUM_THREADS_MARKER='# >>> gluten dev container num_threads >>>'

warn() { echo "WARNING: $*" >&2; }

echo "Preparing the Gluten dev container..."

# Spark 4.0/4.1 and the UDF tests need JDK 17, which this JDK 8 image lacks.
# Both JDKs can coexist: JAVA_HOME still points at JDK 8 for the default build.
if [ ! -d /usr/lib/jvm/java-17-openjdk ]; then
    echo "Installing JDK 17 alongside JDK 8 (needed for Spark 4.x)..."
    dnf install -y --setopt=install_weak_deps=False java-17-openjdk-devel >/dev/null ||
        warn "could not install JDK 17; Spark 4.x builds will not work until it is installed."
fi

# The clang-format and regex installs below need pip3, which the image gets
# transitively rather than by an explicit install.
if ! command -v pip3 >/dev/null 2>&1; then
    echo "Installing python3-pip..."
    dnf install -y --setopt=install_weak_deps=False python3-pip >/dev/null ||
        warn "could not install python3-pip; clang-format 15 and regex will be missing."
fi

# dev/format-cpp-code.sh requires a binary literally named clang-format-15, and
# tries to install it with apt, which does not exist on CentOS.
if ! command -v clang-format-15 >/dev/null 2>&1; then
    echo "Installing clang-format 15..."
    if pip3 install --quiet --retries 1 clang-format==15.0.7; then
        CLANG_FORMAT=$(command -v clang-format)
        if [ -n "$CLANG_FORMAT" ]; then
            ln -sf "$CLANG_FORMAT" /usr/local/bin/clang-format-15
        fi
    else
        warn "could not install clang-format 15; ./dev/format-cpp-code.sh will not run."
    fi
fi

# dev/check.py and .github/workflows/util/license-header.py import regex.
if ! python3 -c "import regex" >/dev/null 2>&1; then
    echo "Installing the regex module..."
    pip3 install --quiet --retries 1 regex ||
        warn "could not install the regex module; ./dev/check.py will not run."
fi

# Cap build parallelism by memory, not just by core count.
# dev/builddeps-veloxbe.sh defaults NUM_THREADS to "nproc --ignore=2", which
# ignores memory entirely. Velox's heavier translation units peak at roughly
# 3.5 GB of resident memory each, so on a machine with many cores relative to
# its RAM the default oversubscribes memory badly: on 32 cores / 62 GB it asks
# for 30 jobs, about 100 GB, and the OOM killer takes down the build or the
# whole container. Reserve a few GB for the editor, Maven and the OS, allow
# about 4 GB per job, and never exceed the CPU-based default.
#
# Installed as a command and re-run per shell below, so the value follows the
# machine: a Codespace can be resized without postCreateCommand running again.
cat >/usr/local/bin/gluten-num-threads <<'HELPER'
#!/usr/bin/env bash
# Build parallelism for Velox: ~4 GB per compile job, capped by core count.
cpu=$(nproc --ignore=2)
mem=$(awk '/^MemTotal:/ {print int(($2 / 1048576 - 8) / 4)}' /proc/meminfo)
[ "${cpu:-0}" -lt 1 ] && cpu=1
[ "${mem:-0}" -lt 1 ] && mem=1
[ "$mem" -lt "$cpu" ] && echo "$mem" || echo "$cpu"
HELPER
chmod +x /usr/local/bin/gluten-num-threads

# Export it so a plain "./dev/buildbundle-veloxbe.sh", as documented in
# docs/get-started/Velox.md, is memory-safe too and not just the command printed
# below.
if ! grep -qF "$NUM_THREADS_MARKER" "$HOME/.bashrc" 2>/dev/null; then
    cat >>"$HOME/.bashrc" <<EOF

$NUM_THREADS_MARKER
# Velox compiles need ~4 GB per job; the build scripts size NUM_THREADS from the
# core count alone, which the OOM killer punishes on core-rich machines.
export NUM_THREADS=\${NUM_THREADS:-\$(gluten-num-threads)}
# <<< gluten dev container num_threads <<<
EOF
fi

NUM_THREADS=$(/usr/local/bin/gluten-num-threads)
CPU_THREADS=$(nproc --ignore=2)
MEM_GB=$(awk '/^MemTotal:/ {printf "%d", $2 / 1048576}' /proc/meminfo 2>/dev/null)

cat <<EOF

============================================================================
Gluten dev container is ready. The native build has NOT been run.

Build the Velox backend and install the Gluten jars:

    ./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \\
                                 --build_tests=ON --spark_version=3.5

  --run_setup_script=OFF  dependencies are already installed in this image
  --build_arrow=OFF       Arrow is already installed under /usr/local
  --build_tests=ON        also build the C++ unit tests (drop it to build faster)
  --spark_version=3.5     build one Spark version instead of all five

NUM_THREADS=${NUM_THREADS} is exported for you, sized from this machine's ${MEM_GB:-?} GB at
~4 GB per compile job. The build scripts would take $CPU_THREADS from the core count
alone, which invites the OOM killer. VS Code tasks do not read ~/.bashrc, so pass
--num_threads=${NUM_THREADS} there.

After changing C++ code, rebuild just the native side (drop build_velox when only
Gluten's own C++ under cpp/ changed):

    ./dev/builddeps-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \\
                               --build_tests=ON build_velox build_gluten_cpp

Run a Spark unit test suite (Spark distributions are pre-installed in this image;
CI runs these on JDK 17, and without -DwildcardSuites the whole suite runs for
hours):

    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
    ./build/mvn test -Pspark-ut -Pbackends-velox -Pspark-3.5 -Pjava-17 \\
        -DargLine="-Dspark.test.home=/opt/shims/spark35/spark_home/" \\
        -DwildcardSuites=org.apache.spark.sql.GlutenSQLQuerySuite

See docs/developers/dev-container.md for details.
============================================================================
EOF
