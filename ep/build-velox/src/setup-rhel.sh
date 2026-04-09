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

# This script documents setting up a RHEL host for Velox
# development.  Running it should make you ready to compile.
#
# Environment variables:
# * INSTALL_PREREQUISITES="N": Skip installation of packages for build.
# * PROMPT_ALWAYS_RESPOND="n": Automatically respond to interactive prompts.
#     Use "n" to never wipe directories.
#
# You can also run individual functions below by specifying them as arguments:
# $ scripts/setup-rhel.sh install_adapters install_gflags
#

set -efx -o pipefail
# Some of the packages must be build with the same compiler flags
# so that some low level types are the same size. Also, disable warnings.
SCRIPTDIR=./scripts
source $SCRIPTDIR/setup-common.sh
export CXXFLAGS=$(get_cxx_flags) # Used by boost.
export CFLAGS=${CXXFLAGS//"-std=c++17"/} # Used by LZO.
SUDO="${SUDO:-""}"
EXTRA_ARROW_OPTIONS=${EXTRA_ARROW_OPTIONS:-""}
USE_CLANG="${USE_CLANG:-false}"
export INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)/deps-download}

FB_ZSTD_VERSION="1.5.6"
DBL_CONVERSION_VERSION="v3.3.0"
SODIUM_VERSION="libsodium-1.0.20-stable"
FLEX_VERSION="2.6.4"
DWARF_VERSION="0.11.1"
BISON_VERSION="bison-3.8.2"
RAPIDJSON_VERSION="v1.1.0"
RE2_VERSION="2023-03-01"
XXHASH_VERSION="0.8.2"
GOOGLETEST_VERSION="1.11.0"
C_ARES_VERSION="v1.34.5"

function dnf_install {
  dnf install -y -q --setopt=install_weak_deps=False "$@"
}

function install_clang15 {
  dnf_install clang15 gcc-toolset-13-libatomic-devel
}

# Install packages required for build.
function install_build_prerequisites {
  dnf update -y
  dnf_install dnf-plugins-core
  dnf_install ninja-build cmake gcc-toolset-12 git wget which bzip2
  dnf_install autoconf automake python3-devel pip libtool
  pip install cmake==3.28.3
  if [[ ${USE_CLANG} != "false" ]]; then
    install_clang15
  fi
}

# Install dependencies from the package managers.
function install_velox_deps_from_dnf {
  dnf_install libevent-devel \
    openssl-devel lz4-devel curl-devel libicu-devel zlib-devel
  # install sphinx for doc gen
  pip install sphinx sphinx-tabs breathe sphinx_rtd_theme
}

function install_gflags {
  # Remove an older version if present.
  dnf remove -y gflags
  wget_and_untar https://github.com/gflags/gflags/archive/${GFLAGS_VERSION}.tar.gz gflags
  cmake_install_dir gflags -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON -DLIB_SUFFIX=64
}

function install_cuda {
  # See https://developer.nvidia.com/cuda-downloads
  dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64/cuda-rhel9.repo
  local dashed="$(echo $1 | tr '.' '-')"
  dnf install -y \
    cuda-compat-$dashed \
    cuda-driver-devel-$dashed \
    cuda-minimal-build-$dashed \
    cuda-nvrtc-devel-$dashed
}

function install_re2 {
  wget_and_untar https://github.com/google/re2/archive/refs/tags/${RE2_VERSION}.tar.gz re2
  cmake_install_dir re2
}

function install_zstd {
  wget_and_untar https://github.com/facebook/zstd/releases/download/v${FB_ZSTD_VERSION}/zstd-${FB_ZSTD_VERSION}.tar.gz zstd
  (
    cd ${DEPENDENCY_DIR}/zstd
    make "-j${NPROC}"
    make install PREFIX=${INSTALL_PREFIX}
  )
}

function install_elfutils-libelf {
  DIR="elfutils-libelf"
  TARFILE="elfutils-latest.tar.bz2"
  pushd "${DEPENDENCY_DIR}"
  if [ -d "${DIR}" ]; then
    rm -rf "${DIR}"
  fi
  mkdir -p "${DIR}"
  pushd "${DIR}"
  curl -L "https://sourceware.org/elfutils/ftp/elfutils-latest.tar.bz2" > "${TARFILE}"
  tar -xj --strip-components=1 -f "${TARFILE}"
  ./configure --disable-demangler --disable-test --disable-doc --prefix=${INSTALL_PREFIX}
  make "-j${NPROC}"
  make install
  popd
  popd
}

function install_xxHash {
  wget_and_untar https://github.com/Cyan4973/xxHash/archive/refs/tags/v${XXHASH_VERSION}.tar.gz xxHash
  cd ${DEPENDENCY_DIR}/xxHash
  make "-j${NPROC}"
  make install PREFIX=${INSTALL_PREFIX}
}

function install_googletest {
  wget_and_untar https://github.com/google/googletest/archive/refs/tags/release-${GOOGLETEST_VERSION}.tar.gz googletest
  cmake_install_dir googletest -DBUILD_TESTING=OFF
}

function install_double_conversion {
  wget_and_untar https://github.com/google/double-conversion/archive/refs/tags/${DBL_CONVERSION_VERSION}.tar.gz double-conversion
  cmake_install_dir double-conversion -DBUILD_TESTING=OFF
}

function install_libsodium {
  wget_and_untar https://download.libsodium.org/libsodium/releases/${SODIUM_VERSION}.tar.gz libsodium
  (
    cd ${DEPENDENCY_DIR}/libsodium
    ./configure --prefix=${INSTALL_PREFIX}
    make "-j${NPROC}"
    make install
  )
}

function install_flex {
  wget_and_untar https://github.com/westes/flex/releases/download/v${FLEX_VERSION}/flex-${FLEX_VERSION}.tar.gz flex
  (
    cd ${DEPENDENCY_DIR}/flex
    ./configure --prefix=${INSTALL_PREFIX}
    make "-j${NPROC}"
    make install
  )
}

function install_libdwarf {
  wget_and_untar https://github.com/davea42/libdwarf-code/archive/refs/tags/v${DWARF_VERSION}.tar.gz libdwarf
  (
    cmake_install_dir libdwarf
  )
}

function install_bison {
  wget_and_untar https://ftp.gnu.org/gnu/bison/${BISON_VERSION}.tar.gz bison
  (
    cd ${DEPENDENCY_DIR}/bison
    ./configure --prefix=${INSTALL_PREFIX}
    make "-j${NPROC}"
    make install
  )
}

function install_rapidjson {
  wget_and_untar https://github.com/Tencent/rapidjson/archive/refs/tags/${RAPIDJSON_VERSION}.tar.gz rapidjson
  (
    cmake_install_dir rapidjson \
      -DRAPIDJSON_BUILD_DOC=OFF \
      -DRAPIDJSON_BUILD_EXAMPLES=OFF \
      -DRAPIDJSON_BUILD_TESTS=OFF
  )
}

function install_c-ares {
  wget_and_untar https://github.com/c-ares/c-ares/archive/refs/tags/${C_ARES_VERSION}.tar.gz c-ares
  cmake_install_dir c-ares -DCMAKE_BUILD_TYPE=Release
}

function install_s3 {
  install_aws_deps

  local MINIO_OS="linux"
  install_minio ${MINIO_OS}
}

function install_gcs {
  # Dependencies of GCS, probably a workaround until the docker image is rebuilt
  dnf -y install npm curl-devel
  install_c-ares
  install_gcs_sdk_cpp
}

function install_abfs {
  # Dependencies of Azure Storage Blob cpp
  dnf -y install perl-IPC-Cmd openssl libxml2-devel
  install_azure_storage_sdk_cpp
}

function install_adapters {
  run_and_time install_s3
  run_and_time install_gcs
  run_and_time install_abfs
}

function install_velox_deps {
  run_and_time install_velox_deps_from_dnf
  run_and_time install_xxHash
  run_and_time install_googletest
  run_and_time install_re2
  run_and_time install_double_conversion
  run_and_time install_libdwarf
  run_and_time install_flex
  run_and_time install_libsodium
  run_and_time install_elfutils-libelf
  run_and_time install_zstd
  run_and_time install_bison
  run_and_time install_rapidjson
  run_and_time install_c-ares
  run_and_time install_gflags
  run_and_time install_glog
  run_and_time install_snappy
  run_and_time install_boost
  run_and_time install_protobuf
  run_and_time install_fmt
  run_and_time install_fast_float
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
  run_and_time install_mvfst
  run_and_time install_fbthrift
  run_and_time install_duckdb
  run_and_time install_stemmer
  run_and_time install_thrift
  run_and_time install_simdjson
  run_and_time install_geos
}

(return 2> /dev/null) && return # If script was sourced, don't run commands.

(
  if [[ $# -ne 0 ]]; then
    if [[ ${USE_CLANG} != "false" ]]; then
      export CC=/usr/bin/clang-15
      export CXX=/usr/bin/clang++-15
    else
      # Activate gcc12; enable errors on unset variables afterwards.
      source /opt/rh/gcc-toolset-12/enable || exit 1
      set -u
    fi

    for cmd in "$@"; do
      run_and_time "${cmd}"
    done
    echo "All specified dependencies installed!"
  else
    if [ "${INSTALL_PREREQUISITES:-Y}" == "Y" ]; then
      echo "Installing build dependencies"
      run_and_time install_build_prerequisites
    else
      echo "Skipping installation of build dependencies since INSTALL_PREREQUISITES is not set"
    fi
    if [[ ${USE_CLANG} != "false" ]]; then
      export CC=/usr/bin/clang-15
      export CXX=/usr/bin/clang++-15
    else
      # Activate gcc12; enable errors on unset variables afterwards.
      source /opt/rh/gcc-toolset-12/enable || exit 1
      set -u
    fi
    install_velox_deps
    echo "All dependencies for Velox installed!"
    if [[ ${USE_CLANG} != "false" ]]; then
      echo "To use clang for the Velox build set the CC and CXX environment variables in your session."
      echo "  export CC=/usr/bin/clang-15"
      echo "  export CXX=/usr/bin/clang++-15"
    fi
    dnf clean all
  fi
)

