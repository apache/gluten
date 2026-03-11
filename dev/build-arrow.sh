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

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
SUDO="${SUDO:-""}"
source ${CURRENT_DIR}/build-helper-functions.sh
VELOX_ARROW_BUILD_VERSION=15.0.0
ARROW_PREFIX=$CURRENT_DIR/../ep/_ep/arrow_ep
BUILD_TYPE=Release
INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}

function prepare_arrow_build() {
  mkdir -p ${ARROW_PREFIX}/../ && pushd ${ARROW_PREFIX}/../ && ${SUDO} rm -rf arrow_ep/
  wget_and_untar https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${VELOX_ARROW_BUILD_VERSION}.tar.gz arrow_ep
  #wget_and_untar https://archive.apache.org/dist/arrow/arrow-${VELOX_ARROW_BUILD_VERSION}/apache-arrow-${VELOX_ARROW_BUILD_VERSION}.tar.gz arrow_ep
  cd arrow_ep
  patch -p1 < $CURRENT_DIR/../ep/build-velox/src/modify_arrow.patch
  patch -p1 < $CURRENT_DIR/../ep/build-velox/src/modify_arrow_dataset_scan_option.patch
  patch -p1 < $CURRENT_DIR/../ep/build-velox/src/cmake-compatibility.patch
  patch -p1 < $CURRENT_DIR/../ep/build-velox/src/support_ibm_power.patch
  popd
}

function build_arrow_cpp() {
  pushd $ARROW_PREFIX/cpp
  ARROW_WITH_ZLIB=ON
  # The zlib version bundled with arrow is not compatible with clang 17.
  # It can be removed after upgrading the arrow version.
  if [[ "$(uname)" == "Darwin" ]]; then
    clang_major_version=$(echo | clang -dM -E - | grep __clang_major__ | awk '{print $3}')
    if [ "${clang_major_version}" -ge 17 ]; then
      ARROW_WITH_ZLIB=OFF
    fi
  fi
  cmake_install \
       -DARROW_PARQUET=OFF \
       -DARROW_FILESYSTEM=ON \
       -DARROW_PROTOBUF_USE_SHARED=OFF \
       -DARROW_DEPENDENCY_USE_SHARED=OFF \
       -DARROW_DEPENDENCY_SOURCE=BUNDLED \
       -DARROW_WITH_THRIFT=ON \
       -DARROW_WITH_LZ4=ON \
       -DARROW_WITH_SNAPPY=ON \
       -DARROW_WITH_ZLIB=${ARROW_WITH_ZLIB} \
       -DARROW_WITH_ZSTD=ON \
       -DARROW_JEMALLOC=OFF \
       -DARROW_SIMD_LEVEL=NONE \
       -DARROW_RUNTIME_SIMD_LEVEL=NONE \
       -DARROW_WITH_UTF8PROC=OFF \
       -DARROW_TESTING=ON \
       -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
       -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
       -DARROW_BUILD_SHARED=OFF \
       -DARROW_BUILD_STATIC=ON

 # Install thrift.
 cd _build/thrift_ep-prefix/src/thrift_ep-build
 ${SUDO} cmake --install ./ --prefix "${INSTALL_PREFIX}"/
 popd
}

function build_arrow_java() {
    ARROW_INSTALL_DIR="${ARROW_PREFIX}/install"

    # Use Gluten's Maven wrapper
    MVN_CMD="${CURRENT_DIR}/../build/mvn"

    # set default number of threads as cpu cores minus 2
    if [[ "$(uname)" == "Darwin" ]]; then
        physical_cpu_cores=$(sysctl -n hw.physicalcpu)
        ignore_cores=2
        if [ "$physical_cpu_cores" -gt "$ignore_cores" ]; then
            NPROC=${NPROC:-$(($physical_cpu_cores - $ignore_cores))}
        else
            NPROC=${NPROC:-$physical_cpu_cores}
        fi
    else
        NPROC=${NPROC:-$(nproc --ignore=2)}
    fi
    echo "set cmake build level to ${NPROC}"
    export CMAKE_BUILD_PARALLEL_LEVEL=$NPROC

    pushd $ARROW_PREFIX/java
    # Because arrow-bom module need the -DprocessAllModules
    ${MVN_CMD} versions:set -DnewVersion=15.0.0-gluten -DprocessAllModules

    ${MVN_CMD} clean install -pl bom,maven/module-info-compiler-maven-plugin,vector -am \
          -DskipTests -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -Dassembly.skipAssembly

    # Arrow C Data Interface CPP libraries
    ${MVN_CMD} generate-resources -P generate-libs-cdata-all-os -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -N

    # Arrow JNI Date Interface CPP libraries
    export PKG_CONFIG_PATH="${INSTALL_PREFIX}"/lib64/pkgconfig:"${INSTALL_PREFIX}"/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}
    ${MVN_CMD} generate-resources -Pgenerate-libs-jni-macos-linux -N -Darrow.dataset.jni.dist.dir=$ARROW_INSTALL_DIR \
      -DARROW_GANDIVA=OFF -DARROW_JAVA_JNI_ENABLE_GANDIVA=OFF -DARROW_ORC=OFF -DARROW_JAVA_JNI_ENABLE_ORC=OFF \
	    -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -N

    # Arrow Java libraries
    ${MVN_CMD} install -Parrow-jni -P arrow-c-data -pl c,dataset -am \
      -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Darrow.dataset.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Darrow.cpp.build.dir=$ARROW_INSTALL_DIR/lib \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -Dassembly.skipAssembly
    popd
}

echo "Start to build Arrow"
prepare_arrow_build
build_arrow_cpp
echo "Finished building arrow CPP"
build_arrow_java
echo "Finished building arrow Java"
