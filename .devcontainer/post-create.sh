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
STATIC_ARM_MARKER='# >>> gluten static dev container arm64 >>>'
DEV_CONTAINER_VARIANT=${GLUTEN_DEV_CONTAINER_VARIANT:-velox-dynamic}

WARNINGS=()
warn() {
    echo "WARNING: $*" >&2
    WARNINGS+=("$*")
}

echo "Preparing the Gluten dev container..."

# The container runs as root while the bind-mounted workspace retains the host
# user's ownership. Register only this repository so Git accepts that mismatch.
WORKSPACE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
if ! git config --global --get-all safe.directory 2>/dev/null |
    grep -Fqx "$WORKSPACE_DIR"; then
    git config --global --add safe.directory "$WORKSPACE_DIR" ||
        warn "could not mark $WORKSPACE_DIR as a safe Git directory."
fi

# /workspaces persists across "Reopen in Container"/"Rebuild Container" and
# across switching between the velox-dynamic and velox-static configs, but
# ep/build-velox/build/velox_ep/_build and cpp/build bake in the vcpkg
# toolchain choice (or its absence) at first CMake configure and CMake never
# re-evaluates it. Reusing a build tree from the other variant does not error
# clearly -- it silently resolves dependencies like zlib/zstd from the wrong
# place and fails much later, e.g. "could not find SnappyConfig.cmake", deep
# into a build that can take hours. Catch the mismatch up front instead.
check_stale_build_tree() {
    local cache="$1"
    [ -f "$cache" ] || return 0
    local has_toolchain=false
    grep -q '^CMAKE_TOOLCHAIN_FILE:' "$cache" 2>/dev/null && has_toolchain=true

    if [ "$DEV_CONTAINER_VARIANT" = "velox-static" ] && [ "$has_toolchain" = false ]; then
        warn "$cache was configured without the vcpkg toolchain (looks like it came from the velox-dynamic container, or a build before --enable_vcpkg=ON). Remove stale build trees before building here: rm -rf ep/build-velox/build/velox_ep/_build cpp/build"
    elif [ "$DEV_CONTAINER_VARIANT" = "velox-dynamic" ] && [ "$has_toolchain" = true ]; then
        warn "$cache was configured with the vcpkg toolchain (looks like it came from the velox-static container). Remove stale build trees before building here: rm -rf ep/build-velox/build/velox_ep/_build cpp/build"
    fi
}

for cache in ep/build-velox/build/velox_ep/_build/*/CMakeCache.txt cpp/build/CMakeCache.txt; do
    check_stale_build_tree "$cache"
done

# vcpkg otherwise defaults to the x64 triplet on arm64.
if [ "$DEV_CONTAINER_VARIANT" = "velox-static" ] &&
    [ "$(uname -m)" = "aarch64" ] &&
    ! grep -qF "$STATIC_ARM_MARKER" "$HOME/.bashrc" 2>/dev/null; then
    cat >>"$HOME/.bashrc" <<EOF

$STATIC_ARM_MARKER
export CPU_TARGET=aarch64
export VCPKG_FORCE_SYSTEM_BINARIES=1
# <<< gluten static dev container arm64 <<<
EOF
fi

# The dynamic image defaults to JDK 8; the static image already has JDK 17.
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

print_parallelism() {
    cat <<EOF

NUM_THREADS=${NUM_THREADS} is exported for you, sized from this machine's ${MEM_GB:-?} GB at
~4 GB per compile job. The build scripts would take $CPU_THREADS from the core count
alone, which invites the OOM killer. VS Code tasks do not read ~/.bashrc, so pass
--num_threads=${NUM_THREADS} there.

EOF
}

case "$DEV_CONTAINER_VARIANT" in
velox-dynamic)
    cat <<EOF
============================================================================
Gluten Velox dynamic-link container is ready. The native build has NOT been run.

Build the Velox backend and install the Gluten jars:

    ./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \\
                                 --build_tests=ON --spark_version=3.5

  --run_setup_script=OFF  dependencies are already installed in this image
  --build_arrow=OFF       Arrow is already installed under /usr/local
  --build_tests=ON        also build the C++ unit tests (drop it to build faster)
  --spark_version=3.5     build one Spark version instead of all five
EOF

    print_parallelism

    cat <<EOF
After changing C++ code, rebuild just the native side (drop build_velox when only
Gluten's own C++ under cpp/ changed):

    ./dev/builddeps-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \\
                               --build_tests=ON build_velox build_gluten_cpp

Build for Spark 4.1 with JDK 17 (the Maven profile does not switch the running JDK):

    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
    export PATH="\$JAVA_HOME/bin:\$PATH"
    java -version  # must report 17
    ./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \\
                                 --build_tests=ON --spark_version=4.1

Run a Spark unit test suite (Spark distributions are pre-installed in this image;
CI runs these on JDK 17, and without -DwildcardSuites the whole suite runs for
hours):

    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
    export PATH="\$JAVA_HOME/bin:\$PATH"
    ./build/mvn test -Pspark-ut -Pbackends-velox -Pspark-3.5 -Pjava-17 \\
        -DargLine="-Dspark.test.home=/opt/shims/spark35/spark_home/" \\
        -DwildcardSuites=org.apache.spark.sql.GlutenSQLQuerySuite

See docs/developers/dev-container.md for details.
============================================================================
EOF
    ;;
velox-static)
    cat <<EOF
============================================================================
Gluten Velox static-link container is ready. The native build has NOT been run.

Build a portable static-link jar:

    ./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF \\
                                 --spark_version=3.5

  --enable_vcpkg=ON       statically link third-party dependencies
  --build_arrow=OFF       Arrow is already installed by the image
  --spark_version=3.5     build one Spark version instead of all five

S3, GCS, HDFS and ABFS are disabled by default. Enable only what you need with
--enable_s3=ON, --enable_gcs=ON, --enable_hdfs=ON or --enable_abfs=ON.
EOF

    print_parallelism

    cat <<EOF
After changing C++ code, rebuild just the native side:

    ./dev/builddeps-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF \\
                               build_velox build_gluten_cpp

JDK 17 is already active. To build for Spark 4.1:

    java -version  # must report 17
    ./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF \\
                                 --spark_version=4.1

This image does not include /opt/shims, so it is intended for static packaging
and reproduction rather than Spark unit tests.

See docs/developers/dev-container.md for details.
============================================================================
EOF
    ;;
*)
    echo "ERROR: unknown GLUTEN_DEV_CONTAINER_VARIANT: $DEV_CONTAINER_VARIANT" >&2
    exit 1
    ;;
esac

# Warnings logged with warn() above can easily scroll past unnoticed among all
# the setup output, so repeat them here, in yellow, after everything else.
if [ "${#WARNINGS[@]}" -gt 0 ]; then
    YELLOW='\033[1;33m'
    NO_COLOR='\033[0m'
    echo -e "${YELLOW}============================================================================${NO_COLOR}" >&2
    echo -e "${YELLOW}WARNINGS:${NO_COLOR}" >&2
    for w in "${WARNINGS[@]}"; do
        echo -e "${YELLOW}  - $w${NO_COLOR}" >&2
    done
    echo -e "${YELLOW}============================================================================${NO_COLOR}" >&2
fi
