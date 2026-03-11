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

function get_cxx_flags {
  local CPU_ARCH=$1

  local OS
  OS=$(uname)
  local MACHINE
  MACHINE=$(uname -m)
  ADDITIONAL_FLAGS=""

  if [[ -z "$CPU_ARCH" ]] || [[ $CPU_ARCH == "unknown" ]]; then
    if [ "$OS" = "Darwin" ]; then

      if [ "$MACHINE" = "x86_64" ]; then
        local CPU_CAPABILITIES
        CPU_CAPABILITIES=$(sysctl -a | grep machdep.cpu.features | awk '{print tolower($0)}')

        if [[ $CPU_CAPABILITIES =~ "avx" ]]; then
          CPU_ARCH="avx"
        else
          CPU_ARCH="sse"
        fi

      elif [[ $(sysctl -a | grep machdep.cpu.brand_string) =~ "Apple" ]]; then
        # Apple silicon.
        CPU_ARCH="arm64"
      fi

    # On MacOs prevent the flood of translation visibility settings warnings.
    ADDITIONAL_FLAGS="-fvisibility=hidden -fvisibility-inlines-hidden"
    else [ "$OS" = "Linux" ];

      local CPU_CAPABILITIES
      CPU_CAPABILITIES=$(cat /proc/cpuinfo | grep flags | head -n 1| awk '{print tolower($0)}')

      if [[ "$CPU_CAPABILITIES" =~ "avx" ]]; then
            CPU_ARCH="avx"
      elif [[ "$CPU_CAPABILITIES" =~ "sse" ]]; then
            CPU_ARCH="sse"
      elif [ "$MACHINE" = "aarch64" ]; then
            CPU_ARCH="aarch64"
      fi
    fi
  fi

  case $CPU_ARCH in

    "arm64")
      echo -n "-mcpu=apple-m1+crc -std=c++20 -fvisibility=hidden $ADDITIONAL_FLAGS"
    ;;

    "avx")
      echo -n "-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++20 -mbmi2 $ADDITIONAL_FLAGS"
    ;;

    "sse")
      echo -n "-msse4.2 -std=c++20 $ADDITIONAL_FLAGS"
    ;;

    "aarch64")
      # Follow Velox's ARM CPU detection logic to ensure consistent compiler flags
      # between Gluten and Velox, preventing xsimd initialization issues, see GLUTEN-11390.
      # Reference: function get_cxx_flags in Velox's setup-helper-functions.sh.

      # Read Arm MIDR_EL1 register to detect Arm cpu.
      # https://developer.arm.com/documentation/100616/0301/register-descriptions/aarch64-system-registers/midr-el1--main-id-register--el1
      ARM_CPU_FILE="/sys/devices/system/cpu/cpu0/regs/identification/midr_el1"
      ARM_BUILD_TARGET="${ARM_BUILD_TARGET:-local}"

      # https://gitlab.arm.com/telemetry-solution/telemetry-solution/-/blob/main/data/pmu/cpu/neoverse/neoverse-n1.json#L13
      # N1:d0c; N2:d49; V1:d40; V2:d4f
      Neoverse_N1="d0c"
      Neoverse_N2="d49"
      Neoverse_V1="d40"
      Neoverse_V2="d4f"

      if [ -f "$ARM_CPU_FILE" ] && [ "$ARM_BUILD_TARGET" = "local" ]; then
        hex_ARM_CPU_DETECT=$(cat $ARM_CPU_FILE)
        # PartNum, [15:4]: The primary part number such as Neoverse N1/N2 core.
        ARM_CPU_PRODUCT=${hex_ARM_CPU_DETECT: -4:3}

        if [ "$ARM_CPU_PRODUCT" = "$Neoverse_N1" ]; then
          echo -n "-mcpu=neoverse-n1 -std=c++20 $ADDITIONAL_FLAGS"
        elif [ "$ARM_CPU_PRODUCT" = "$Neoverse_N2" ]; then
          echo -n "-mcpu=neoverse-n2 -std=c++20 $ADDITIONAL_FLAGS"
        elif [ "$ARM_CPU_PRODUCT" = "$Neoverse_V1" ]; then
          echo -n "-mcpu=neoverse-v1 -std=c++20 $ADDITIONAL_FLAGS"
        elif [ "$ARM_CPU_PRODUCT" = "$Neoverse_V2" ]; then
          # Read the JEDEC JEP-106 manufacturer ID to distinguish different Neoverse V2 cores
          # https://developer.arm.com/documentation/ka001301/latest/
          SOC_ID_FILE="/sys/devices/soc0/soc_id"
          GRACE_SOC_ID="jep106:036b:0241"
          # Check for NVIDIA Grace which has various extensions
          if [ -f "$SOC_ID_FILE" ] && [ "$(cat $SOC_ID_FILE)" = "$GRACE_SOC_ID" ]; then
            echo -n "-mcpu=neoverse-v2+crypto+sha3+sm4+sve2-aes+sve2-sha3+sve2-sm4 -std=c++20 $ADDITIONAL_FLAGS"
          else
            echo -n "-mcpu=neoverse-v2 -std=c++20 $ADDITIONAL_FLAGS"
          fi
        else
          # Fallback to generic ARMv8-A for compatibility with unknown ARM CPUs
          echo -n "-march=armv8-a+crc+crypto -std=c++20 $ADDITIONAL_FLAGS"
        fi
      else
        # Fallback to generic ARMv8-A for compatibility when CPU detection is not available
        echo -n "-march=armv8-a+crc+crypto -std=c++20 $ADDITIONAL_FLAGS"
      fi
    ;;
  *)
    echo -n "Architecture not supported!"
  esac

}

function github_checkout {
  local REPO=$1
  shift
  local VERSION=$1
  shift
  local GIT_CLONE_PARAMS=$@
  local DIRNAME=$(basename $REPO)
  SUDO="${SUDO:-""}"
  cd "${DEPENDENCY_DIR}"
  if [ -z "${DIRNAME}" ]; then
    echo "Failed to get repo name from ${REPO}"
    exit 1
  fi
  if [ -d "${DIRNAME}" ] && prompt "${DIRNAME} already exists. Delete?"; then
    ${SUDO} rm -rf "${DIRNAME}"
  fi
  if [ ! -d "${DIRNAME}" ]; then
    git clone -q -b $VERSION $GIT_CLONE_PARAMS "https://github.com/${REPO}.git"
  fi
  cd "${DIRNAME}"
}

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DIR}"
  pushd "${DIR}"
  curl -L "${URL}" > $2.tar.gz
  tar -xz --strip-components=1 -f $2.tar.gz
  popd
}

function cmake_install {
  local NAME=$(basename "$(pwd)")
  local BINARY_DIR=_build
  SUDO="${SUDO:-""}"
  if [ -d "${BINARY_DIR}" ] && prompt "Do you want to rebuild ${NAME}?"; then
    ${SUDO} rm -rf "${BINARY_DIR}"
  fi
  mkdir -p "${BINARY_DIR}"
  CPU_TARGET="${CPU_TARGET:-unknown}"
  COMPILER_FLAGS=$(get_cxx_flags $CPU_TARGET)

  # CMAKE_POSITION_INDEPENDENT_CODE is required so that Velox can be built into dynamic libraries \
  cmake -Wno-dev -B"${BINARY_DIR}" \
    -GNinja \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=20 \
    "${INSTALL_PREFIX+-DCMAKE_PREFIX_PATH=}${INSTALL_PREFIX-}" \
    "${INSTALL_PREFIX+-DCMAKE_INSTALL_PREFIX=}${INSTALL_PREFIX-}" \
    -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" \
    -DBUILD_TESTING=OFF \
    "$@"

  cmake --build "${BINARY_DIR}"
  ${SUDO} cmake --install "${BINARY_DIR}"
}

function setup_macos {
  sed -i '' '/run_and_time install_arrow/d' scripts/setup-macos.sh
  if [ $ARCH == 'x86_64' ]; then
    ./scripts/setup-macos.sh
  elif [ $ARCH == 'arm64' ]; then
    CPU_TARGET="arm64" ./scripts/setup-macos.sh
  else
    echo "Unknown arch: $ARCH"
  fi
}

function setup_linux {
  local LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
  local LINUX_VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})
  CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
  GLUTEN_VELOX_SCRIPT_HOME=$CURRENT_DIR/../ep/build-velox/src
  # Skip UTF-8 validation in JSON parsing. Required for compatibility with Spark.
  export SIMDJSON_SKIPUTF8VALIDATION=ON

  if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" || "$LINUX_DISTRIBUTION" == "pop" ]]; then
    scripts/setup-ubuntu.sh
  elif [[ "$LINUX_DISTRIBUTION" == "centos" ]]; then
    case "$LINUX_VERSION_ID" in
    9) scripts/setup-centos9.sh ;;
    8) $GLUTEN_VELOX_SCRIPT_HOME/setup-centos8.sh ;;
    7)
      $GLUTEN_VELOX_SCRIPT_HOME/setup-centos7.sh
      set +u
      export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
      source /opt/rh/devtoolset-11/enable
      set -u
      ;;
    *)
      echo "Unsupported centos version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "openEuler" ]]; then
    case "$LINUX_VERSION_ID" in
      24.03)
        $GLUTEN_VELOX_SCRIPT_HOME/setup-openeuler24.sh ;;
      *)
        echo "Unsupported openEuler version: $LINUX_VERSION_ID"
        exit 1
        ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "alinux" ]]; then
    case "${LINUX_VERSION_ID:0:1}" in
    2)
      $GLUTEN_VELOX_SCRIPT_HOME/setup-centos7.sh
      set +u
      export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
      source /opt/rh/devtoolset-11/enable
      set -u
      ;;
    3) $GLUTEN_VELOX_SCRIPT_HOME/setup-centos8.sh ;;
    *)
      echo "Unsupported alinux version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "tencentos" ]]; then
    case "$LINUX_VERSION_ID" in
    2.4)
        $GLUTEN_VELOX_SCRIPT_HOME/setup-centos7.sh
        set +u
        export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
        source /opt/rh/devtoolset-11/enable
        set -u
        ;;
    3.2) $GLUTEN_VELOX_SCRIPT_HOME/setup-centos8.sh ;;
    *)
      echo "Unsupported tencentos version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "rhel" ]]; then
    case "$LINUX_VERSION_ID" in
    9.6)
       $GLUTEN_VELOX_SCRIPT_HOME/setup-rhel.sh ;;
    9.7)
       $GLUTEN_VELOX_SCRIPT_HOME/setup-rhel.sh ;;
    *)
      echo "Unsupported rhel version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  else
    echo "Unsupported linux distribution: $LINUX_DISTRIBUTION"
    exit 1
  fi
}
