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

set -exu
# New build option may need to be included in get_build_summary to ensure EP build cache workable.
# Path to the Velox source code.
VELOX_HOME=""
# Enable S3 connector.
ENABLE_S3=OFF
# Enable GCS connector.
ENABLE_GCS=OFF
# Enable HDFS connector.
ENABLE_HDFS=OFF
# Enable ABFS connector.
ENABLE_ABFS=OFF
# Enable GPU support
ENABLE_GPU=OFF
# CMake build type for Velox.
BUILD_TYPE=release
# May be deprecated in Gluten build.
ENABLE_BENCHMARK=OFF
# May be deprecated in Gluten build.
ENABLE_TESTS=OFF
# Set to ON for gluten cpp test build.
BUILD_TEST_UTILS=OFF
# Number of threads to use for building.
NUM_THREADS=""

OTHER_ARGUMENTS=""

OS=`uname -s`
ARCH=`uname -m`

for arg in "$@"; do
  case $arg in
  --velox_home=*)
    VELOX_HOME=("${arg#*=}")
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
  --enable_gpu=*)
    ENABLE_GPU=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_type=*)
    BUILD_TYPE=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_test_utils=*)
    BUILD_TEST_UTILS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_tests=*)
    ENABLE_TESTS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_benchmarks=*)
    ENABLE_BENCHMARK=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --num_threads=*)
    NUM_THREADS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function compile {
  # -Wno-unknown-warning-option is a Clang-originated flag. GCC ignores unrecognized -Wno- flags to
  # maintain compatibility, but it prints a diagnostic note about the unknown flag if a true warning
  # or error occurs.
  CXX_FLAGS='-Wno-error=stringop-overflow -Wno-error=cpp -Wno-missing-field-initializers \
    -Wno-error=uninitialized -Wno-unknown-warning-option -Wno-deprecated-declarations'

  COMPILE_OPTION="-DCMAKE_CXX_FLAGS=\"$CXX_FLAGS\" -DVELOX_ENABLE_PARQUET=ON -DVELOX_BUILD_TESTING=OFF \
      -DVELOX_MONO_LIBRARY=ON -DVELOX_BUILD_RUNNER=OFF -DVELOX_SIMDJSON_SKIPUTF8VALIDATION=ON \
      -DVELOX_ENABLE_GEO=ON"
  if [ $BUILD_TEST_UTILS == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_BUILD_TEST_UTILS=ON"
  fi
  if [ $ENABLE_HDFS == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_HDFS=ON"
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_S3=ON"
  fi
  # If ENABLE_BENCHMARK == ON, Velox disables tests and connectors
  if [ $ENABLE_BENCHMARK == "OFF" ]; then
    if [ $ENABLE_TESTS == "ON" ]; then
      COMPILE_OPTION="$COMPILE_OPTION -DVELOX_BUILD_TESTING=ON "
    fi
    if [ $ENABLE_ABFS == "ON" ]; then
      COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_ABFS=ON"
    fi
    if [ $ENABLE_GCS == "ON" ]; then
      COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_GCS=ON"
    fi
  else
    echo "ENABLE_BENCHMARK is ON. Disabling Tests, GCS and ABFS connectors if enabled."
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_BENCHMARKS=ON"
  fi
  if [ $ENABLE_GPU == "ON" ]; then
    # the cuda default options are for Centos9 image from Meta
    echo "enable GPU support."
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_CUDF=ON -DCMAKE_CUDA_ARCHITECTURES=75 \
        -DCMAKE_CUDA_COMPILER=/usr/local/cuda/bin/nvcc"
  fi
  if [ -n "${GLUTEN_VCPKG_ENABLED:-}" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_GFLAGS_TYPE=static"
  fi

  COMPILE_OPTION="$COMPILE_OPTION -DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
  COMPILE_TYPE=$(if [[ "$BUILD_TYPE" == "debug" ]] || [[ "$BUILD_TYPE" == "Debug" ]]; then echo 'debug'; else echo 'release'; fi)
  echo "COMPILE_OPTION: "$COMPILE_OPTION

  NUM_THREADS_OPTS=""
  if [ -n "${NUM_THREADS:-}" ]; then
    NUM_THREADS_OPTS="NUM_THREADS=$NUM_THREADS MAX_HIGH_MEM_JOBS=$NUM_THREADS MAX_LINK_JOBS=$NUM_THREADS"
  fi
  echo "NUM_THREADS_OPTS: $NUM_THREADS_OPTS"

  export simdjson_SOURCE=AUTO
  export Arrow_SOURCE=AUTO
  if [ $ARCH == 'x86_64' ]; then
    make $COMPILE_TYPE $NUM_THREADS_OPTS EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
  elif [[ "$ARCH" == 'arm64' || "$ARCH" == 'aarch64' ]]; then
    CPU_TARGET=$ARCH make $COMPILE_TYPE $NUM_THREADS_OPTS EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
  else
    echo "Unsupported arch: $ARCH"
    exit 1
  fi

  # Install deps to system as needed
  if [ -d "_build/$COMPILE_TYPE/_deps" ]; then
    cd _build/$COMPILE_TYPE/_deps
    if [ -d xsimd-build ]; then
      echo "INSTALL xsimd."
      if [ $OS == 'Linux' ]; then
        sudo cmake --install xsimd-build/
      elif [ $OS == 'Darwin' ]; then
        sudo cmake --install xsimd-build/
      fi
    fi
    if [ -d googletest-build ]; then
      echo "INSTALL gtest."
      if [ $OS == 'Linux' ]; then
        cd googletest-src; cmake . ; sudo make install -j
        #sudo cmake --install googletest-build/
      elif [ $OS == 'Darwin' ]; then
        sudo cmake --install googletest-build/
      fi
    fi
  fi
}

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../build/velox_ep"
fi

echo "Start building Velox..."
echo "CMAKE Arguments:"
echo "VELOX_HOME=${VELOX_HOME}"
echo "ENABLE_S3=${ENABLE_S3}"
echo "ENABLE_GCS=${ENABLE_GCS}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "ENABLE_ABFS=${ENABLE_ABFS}"
echo "ENABLE_GPU=${ENABLE_GPU}"
echo "BUILD_TYPE=${BUILD_TYPE}"

cd ${VELOX_HOME}
compile

echo "Successfully built Velox from Source."
