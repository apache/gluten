#! /bin/bash

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

set -e

if [ -z "${BASH_SOURCE[0]}" ] || [ "$0" == "${BASH_SOURCE[0]}" ]; then
    echo "env.sh should only be sourced in bash" >&2
    exit 1
fi

SCRIPT_ROOT="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"

export VCPKG_ROOT="$SCRIPT_ROOT/.vcpkg"
export VCPKG="$SCRIPT_ROOT/.vcpkg/vcpkg"
export VCPKG_TRIPLET=$([ "${CPU_TARGET:-}" = "aarch64" ] && echo "arm64-linux-neon" || echo "x64-linux-avx")
export VCPKG_TRIPLET_INSTALL_DIR=${SCRIPT_ROOT}/vcpkg_installed/${VCPKG_TRIPLET}

${SCRIPT_ROOT}/init.sh "$@"

unset PKG_CONFIG_PATH
export PKG_CONFIG_LIBDIR=${VCPKG_TRIPLET_INSTALL_DIR}/lib/pkgconfig:${VCPKG_TRIPLET_INSTALL_DIR}/share/pkgconfig

if [ "${GLUTEN_VCPKG_ENABLED:-}" != "${VCPKG_ROOT}" ]; then
    EXPORT_TOOLS_PATH="${VCPKG_TRIPLET_INSTALL_DIR}/tools/protobuf"
    # The scripts depends on environment $CMAKE_TOOLCHAIN_FILE, which requires
    # cmake >= 3.21. If system cmake < 3.25, vcpkg will download latest cmake. We
    # can use vcpkg's internal cmake if we find it.
    VCPKG_CMAKE_BIN_DIR=$(echo "${VCPKG_ROOT}"/downloads/tools/cmake-*/cmake-*/bin)
    if [ -f "$VCPKG_CMAKE_BIN_DIR/cmake" ]; then
        EXPORT_TOOLS_PATH="${VCPKG_CMAKE_BIN_DIR}:${EXPORT_TOOLS_PATH}"
    fi
    EXPORT_TOOLS_PATH=${EXPORT_TOOLS_PATH/%:/}

    export VCPKG_ROOT=${VCPKG_ROOT}
    export VCPKG_MANIFEST_DIR=${SCRIPT_ROOT}
    export VCPKG_TRIPLET=${VCPKG_TRIPLET}
    export CMAKE_TOOLCHAIN_FILE=${SCRIPT_ROOT}/toolchain.cmake
    export PATH="${EXPORT_TOOLS_PATH}:$PATH"

    export GLUTEN_VCPKG_ENABLED=${VCPKG_ROOT}
else
    echo "Gluten's vcpkg environment is already enabled" >&2
fi
