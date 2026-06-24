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

LIBRDKAFKA_VERSION="v2.10.0"
CPPKAFKA_VERSION="v0.4.1"
FROCKSDB_VERSION="FRocksDB-6.20.3"
FROCKSDB_REPO="ververica/frocksdb"

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DEPENDENCY_DIR}"
  pushd "${DEPENDENCY_DIR}"
  SUDO="${SUDO:-""}"
  if [ -d "${DIR}" ]; then
    if prompt "${DIR} already exists. Delete?"; then
      ${SUDO} rm -rf "${DIR}"
    else
      popd
      return
    fi
  fi
  mkdir -p "${DIR}"
  pushd "${DIR}"
  curl ${CURL_OPTIONS} -L "${URL}" > $2.tar.gz
  tar -xz --strip-components=1 -f $2.tar.gz
  popd
  popd
}

function github_checkout {
  local REPO=$1
  shift
  local VERSION=$1
  shift
  local GIT_CLONE_PARAMS=("$@")
  local DIRNAME
  DIRNAME=$(basename "$REPO")
  SUDO="${SUDO:-""}"
  cd "${DEPENDENCY_DIR}" || exit
  if [ -z "${DIRNAME}" ]; then
    echo "Failed to get repo name from ${REPO}"
    exit 1
  fi
  if [ -d "${DIRNAME}" ] && prompt "${DIRNAME} already exists. Delete?"; then
    ${SUDO} rm -rf "${DIRNAME}"
  fi
  if [ ! -d "${DIRNAME}" ]; then
    git clone -q -b "$VERSION" "${GIT_CLONE_PARAMS[@]}" "https://github.com/${REPO}.git"
  fi
}

function cmake_install_dir {
  pushd "./${DEPENDENCY_DIR}/$1"
  # remove the directory argument
  shift
  cmake_install $@
  popd
}

function cmake_install {
  local NAME=$(basename "$(pwd)")
  local BINARY_DIR=_build
  SUDO="${SUDO:-""}"
  if [ -d "${BINARY_DIR}" ]; then
    if prompt "Do you want to rebuild ${NAME}?"; then
      ${SUDO} rm -rf "${BINARY_DIR}"
    else
      return 0
    fi
  fi

  mkdir -p "${BINARY_DIR}"
  COMPILER_FLAGS="-g -gdwarf-2"
  # Add platform specific CXX flags if any
  COMPILER_FLAGS+=${OS_CXXFLAGS}

  # CMAKE_POSITION_INDEPENDENT_CODE is required so that Velox can be built into dynamic libraries \
  cmake -Wno-dev ${CMAKE_OPTIONS} -B"${BINARY_DIR}" \
    -GNinja \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    "${INSTALL_PREFIX+-DCMAKE_PREFIX_PATH=}${INSTALL_PREFIX-}" \
    "${INSTALL_PREFIX+-DCMAKE_INSTALL_PREFIX=}${INSTALL_PREFIX-}" \
    -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" \
    -DBUILD_TESTING=OFF \
    "$@"
  # Exit if the build fails.
  cmake --build "${BINARY_DIR}" "-j ${NPROC}" || { echo 'build failed' ; exit 1; }
  ${SUDO} cmake --install "${BINARY_DIR}"
}

function run_and_time {
  time "$@" || (echo "Failed to run $* ." ; exit 1 )
  { echo "+ Finished running $*"; } 2> /dev/null
}

function install_librdkafka {
  wget_and_untar https://github.com/confluentinc/librdkafka/archive/refs/tags/${LIBRDKAFKA_VERSION}.tar.gz librdkafka
  cmake_install_dir librdkafka -DBUILD_TESTS=OFF
}

function install_cppkafka {
  wget_and_untar https://github.com/mfontanini/cppkafka/archive/refs/tags/${CPPKAFKA_VERSION}.tar.gz cppkafka
  cmake_install_dir cppkafka -DBUILD_TESTS=OFF
}

function install_rocksdb {
  github_checkout ${FROCKSDB_REPO} ${FROCKSDB_VERSION}
  cmake_install_dir frocksdb -DWITH_GFLAGS=OFF -DWITH_TESTS=OFF -DFAIL_ON_WARNINGS=OFF
}

function install_velox_deps {
  run_and_time install_librdkafka
  run_and_time install_cppkafka
  run_and_time install_rocksdb
}

install_velox_deps
