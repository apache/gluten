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

####################################################################################################
#  The main function of this script is to allow developers to build the environment with one click #
#  Recommended commands for first-time installation:                                               #
#  ./dev/buildbundle-veloxbe.sh                                                                    #
####################################################################################################
set -exu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."
BUILD_TYPE=Release
BUILD_TESTS=OFF
BUILD_EXAMPLES=OFF
BUILD_BENCHMARKS=OFF
ENABLE_JEMALLOC_STATS=OFF
BUILD_VELOX_TESTS=OFF
BUILD_VELOX_BENCHMARKS=OFF
ENABLE_QAT=OFF
ENABLE_GCS=OFF
ENABLE_S3=OFF
ENABLE_HDFS=OFF
ENABLE_ABFS=OFF
ENABLE_VCPKG=OFF
ENABLE_GPU=OFF
ENABLE_ENHANCED_FEATURES=OFF
RUN_SETUP_SCRIPT=ON
VELOX_REPO=""
VELOX_BRANCH=""
VELOX_HOME="$GLUTEN_DIR/ep/build-velox/build/velox_ep"
VELOX_PARAMETER=""
BUILD_ARROW=ON
SPARK_VERSION=ALL

# set default number of threads as cpu cores minus 2
if [[ "$(uname)" == "Darwin" ]]; then
    physical_cpu_cores=$(sysctl -n hw.physicalcpu)
    ignore_cores=2
    if [ "$physical_cpu_cores" -gt "$ignore_cores" ]; then
        NUM_THREADS=${NUM_THREADS:-$(($physical_cpu_cores - $ignore_cores))}
    else
        NUM_THREADS=${NUM_THREADS:-$physical_cpu_cores}
    fi
else
    NUM_THREADS=${NUM_THREADS:-$(nproc --ignore=2)}
fi

for arg in "$@"
do
    case $arg in
        --build_type=*)
        BUILD_TYPE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_tests=*)
        BUILD_TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_examples=*)
        BUILD_EXAMPLES=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_benchmarks=*)
        BUILD_BENCHMARKS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_jemalloc_stats=*)
        ENABLE_JEMALLOC_STATS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_qat=*)
        ENABLE_QAT=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_gcs=*)
        ENABLE_GCS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_s3=*)
        ENABLE_S3=("${arg#*=}")
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
        --enable_vcpkg=*)
        ENABLE_VCPKG=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_gpu=*)
        ENABLE_GPU=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_enhanced_features=*)
        ENABLE_ENHANCED_FEATURES=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --run_setup_script=*)
        RUN_SETUP_SCRIPT=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_repo=*)
        VELOX_REPO=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_branch=*)
        VELOX_BRANCH=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_home=*)
        VELOX_HOME=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_velox_tests=*)
        BUILD_VELOX_TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_velox_benchmarks=*)
        BUILD_VELOX_BENCHMARKS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_arrow=*)
        BUILD_ARROW="${arg#*=}"
        shift # Remove argument name from processing
        ;;
        --num_threads=*)
        NUM_THREADS="${arg#*=}"
        shift # Remove argument name from processing
        ;;
        --spark_version=*)
        SPARK_VERSION="${arg#*=}"
        shift # Remove argument name from processing
        ;;
	      *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

if [[ "$(uname)" == "Darwin" ]]; then
    export INSTALL_PREFIX=${INSTALL_PREFIX:-${VELOX_HOME}/deps-install}
fi

function concat_velox_param {
    # check velox repo
    if [[ -n $VELOX_REPO ]]; then
        VELOX_PARAMETER+="--velox_repo=$VELOX_REPO "
    fi

    # check velox branch
    if [[ -n $VELOX_BRANCH ]]; then
        VELOX_PARAMETER+="--velox_branch=$VELOX_BRANCH "
    fi

    # check velox home
    if [[ -n $VELOX_HOME ]]; then
        VELOX_PARAMETER+="--velox_home=$VELOX_HOME "
    fi

    if [ "$ENABLE_ENHANCED_FEATURES" = "ON" ]; then
        VELOX_PARAMETER+="--enable_enhanced_features=$ENABLE_ENHANCED_FEATURES "
    fi

    VELOX_PARAMETER+="--run_setup_script=$RUN_SETUP_SCRIPT "
}


if [ "$ENABLE_VCPKG" = "ON" ]; then
    # vcpkg will install static depends and init build environment
    BUILD_OPTIONS="--build_tests=$BUILD_TESTS --enable_s3=$ENABLE_S3 --enable_gcs=$ENABLE_GCS \
                   --enable_hdfs=$ENABLE_HDFS --enable_abfs=$ENABLE_ABFS"
    source ./dev/vcpkg/env.sh ${BUILD_OPTIONS}
fi

# Supported Spark versions
SUPPORTED_SPARK_VERSIONS=("3.3" "3.4" "3.5" "4.0" "4.1" "ALL")

# Check if SPARK_VERSION is in the supported list
pattern=" $SPARK_VERSION "
if [[ " ${SUPPORTED_SPARK_VERSIONS[*]} " =~ $pattern ]]; then
  echo "Building for Spark $SPARK_VERSION"
else
  echo "Invalid Spark version: $SPARK_VERSION"
  echo "Supported versions: 3.3 3.4 3.5 4.0 4.1 ALL"
  exit 1
fi

concat_velox_param

function build_arrow {
  if [ ! -d "$VELOX_HOME" ]; then
    get_velox
    if [ -z "${GLUTEN_VCPKG_ENABLED:-}" ] && [ $RUN_SETUP_SCRIPT == "ON" ]; then
      setup_dependencies
    else
      setup_dependencies_arrow
    fi
  fi
  cd $GLUTEN_DIR/dev
  source ./build-arrow.sh
}

function build_velox {
  echo "Start to build Velox"
  cd $GLUTEN_DIR/ep/build-velox/src
  # When BUILD_TESTS is on for gluten cpp, we need turn on VELOX_BUILD_TEST_UTILS via build_test_utils.
  ./build-velox.sh --enable_s3=$ENABLE_S3 --enable_gcs=$ENABLE_GCS --build_type=$BUILD_TYPE --enable_hdfs=$ENABLE_HDFS \
                   --enable_abfs=$ENABLE_ABFS --enable_gpu=$ENABLE_GPU --build_test_utils=$BUILD_TESTS \
                   --build_tests=$BUILD_VELOX_TESTS --build_benchmarks=$BUILD_VELOX_BENCHMARKS --num_threads=$NUM_THREADS \
                   --velox_home=$VELOX_HOME
}

function build_gluten_cpp {
  echo "Start to build Gluten CPP"
  cd $GLUTEN_DIR/cpp
  rm -rf build
  mkdir build
  cd build

  GLUTEN_CMAKE_OPTIONS="-DBUILD_VELOX_BACKEND=ON \
    -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
    -DVELOX_HOME=$VELOX_HOME \
    -DBUILD_TESTS=$BUILD_TESTS \
    -DBUILD_EXAMPLES=$BUILD_EXAMPLES \
    -DBUILD_BENCHMARKS=$BUILD_BENCHMARKS \
    -DENABLE_JEMALLOC_STATS=$ENABLE_JEMALLOC_STATS \
    -DENABLE_QAT=$ENABLE_QAT \
    -DENABLE_GCS=$ENABLE_GCS \
    -DENABLE_S3=$ENABLE_S3 \
    -DENABLE_HDFS=$ENABLE_HDFS \
    -DENABLE_ABFS=$ENABLE_ABFS \
    -DENABLE_GPU=$ENABLE_GPU \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DENABLE_ENHANCED_FEATURES=$ENABLE_ENHANCED_FEATURES"

  if [ $OS == 'Darwin' ]; then
    GLUTEN_CMAKE_OPTIONS+=" -DCMAKE_PREFIX_PATH=$INSTALL_PREFIX"
  fi

  cmake -G Ninja $GLUTEN_CMAKE_OPTIONS ..
  ninja -j $NUM_THREADS
}

function build_velox_backend {
  if [ $BUILD_ARROW == "ON" ]; then
    build_arrow
  fi
  build_velox
  build_gluten_cpp
}

function get_velox {
  cd $GLUTEN_DIR/ep/build-velox/src
  ./get-velox.sh $VELOX_PARAMETER
}

function setup_dependencies {
  DEPENDENCY_DIR=${DEPENDENCY_DIR:-$CURRENT_DIR/../ep/_ep}
  mkdir -p ${DEPENDENCY_DIR}

  source $GLUTEN_DIR/dev/build-helper-functions.sh
  source ${VELOX_HOME}/scripts/setup-common.sh

  echo "Start to install dependencies"
  pushd $VELOX_HOME
  if [ $OS == 'Linux' ]; then
    setup_linux
  elif [ $OS == 'Darwin' ]; then
    setup_macos
  else
    echo "Unsupported kernel: $OS"
    exit 1
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    install_aws_deps
  fi
  if [ $ENABLE_GCS == "ON" ]; then
    install_gcs_sdk_cpp
  fi
  if [ $ENABLE_ABFS == "ON" ]; then
    export AZURE_SDK_DISABLE_AUTO_VCPKG=ON
    install_azure_storage_sdk_cpp
  fi
  popd
}

function setup_dependencies_arrow {
  DEPENDENCY_DIR=${DEPENDENCY_DIR:-$CURRENT_DIR/../ep/_ep}
  mkdir -p ${DEPENDENCY_DIR}

  source ${VELOX_HOME}/scripts/setup-common.sh
}

OS=`uname -s`
ARCH=`uname -m`
commands_to_run=(${OTHER_ARGUMENTS[@]:-})
(
  if [[ ${#commands_to_run[@]} -eq 0 ]]; then
    get_velox
    if [ -z "${GLUTEN_VCPKG_ENABLED:-}" ] && [ $RUN_SETUP_SCRIPT == "ON" ]; then
      setup_dependencies
    fi
    build_velox_backend
  else
    echo "Commands to run: ${commands_to_run[@]}"
    for cmd in "${commands_to_run[@]}"; do
       "${cmd}"
    done
  fi
)
