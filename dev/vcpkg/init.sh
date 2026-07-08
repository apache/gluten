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

BUILD_TESTS=OFF
ENABLE_S3=OFF
ENABLE_GCS=OFF
ENABLE_HDFS=OFF
ENABLE_ABFS=OFF

for arg in "$@"; do
  case $arg in
  --build_tests=*)
    BUILD_TESTS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_gcs=*)
    ENABLE_GCS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_abfs=*)
    ENABLE_ABFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    echo "Unrecognized argument: $arg"
    exit 1
    ;;
  esac
done

require_set() {
  if [ -z "${!1}" ]; then
    echo "Required variable $1 not found!"
    exit 1
  fi
}

require_set "VCPKG_ROOT"
require_set "VCPKG"
require_set "VCPKG_TRIPLET"
require_set "VCPKG_TRIPLET_INSTALL_DIR"

SCRIPT_ROOT="$(realpath "$(dirname "$0")")"
cd "$SCRIPT_ROOT"

if [ ! -d "$VCPKG_ROOT" ] || [ -z "$(ls "$VCPKG_ROOT")" ]; then
    # The builtin-baseline (commit hash) specified in vcpkg.json should exist in this branch.
    # Therefore, upgrading the builtin-baseline may require updating the branch.
    git clone https://github.com/microsoft/vcpkg.git --branch 2026.03.18 "$VCPKG_ROOT"
fi
[ -f "$VCPKG" ] || "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics

EXTRA_FEATURES=""
if [ "$BUILD_TESTS" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=duckdb "
fi
if [ "$ENABLE_S3" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=velox-s3 "
fi
if [ "$ENABLE_GCS" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=velox-gcs "
fi
if [ "$ENABLE_ABFS" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=velox-abfs "
fi
if [ "${VCPKG_DYNAMIC_OPENSSL:-OFF}" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=dynamic-openssl "
fi


$VCPKG install --no-print-usage --allow-unsupported \
    --triplet="${VCPKG_TRIPLET}" --host-triplet="${VCPKG_TRIPLET}" ${EXTRA_FEATURES}

# For fixing a build error like below when gluten's build type is Debug:
# No rule to make target '/root/gluten/dev/vcpkg/vcpkg_installed/x64-linux-avx/debug/lib/libz.a',
# needed by 'releases/libvelox.so'
mkdir -p $VCPKG_TRIPLET_INSTALL_DIR/debug/lib/
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libz.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
if [ "${VCPKG_DYNAMIC_OPENSSL:-OFF}" = "OFF" ]; then
  cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libssl.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
  cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libcrypto.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
fi
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/liblzma.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libdwarf.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
