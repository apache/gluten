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
  patch -p1 < $CURRENT_DIR/../ep/build-velox/src/cmake-compatibility.patch
  patch -p1 < $CURRENT_DIR/../ep/build-velox/src/support_ibm_power.patch
  popd
}

function build_arrow_cpp() {
  pushd $ARROW_PREFIX/cpp
  ARROW_WITH_ZLIB=ON
  # Major version of the compiler CMake will use, when it is clang. Non-clang
  # compilers (e.g. gcc) do not define __clang_major__, leaving this empty.
  clang_major_version=$(echo | ${CXX:-c++} -dM -E -x c++ - 2>/dev/null | awk '/__clang_major__/ {print $3}')
  # The zlib version bundled with arrow is not compatible with clang 17.
  # It can be removed after upgrading the arrow version.
  if [[ "$(uname)" == "Darwin" ]]; then
    if [ -n "${clang_major_version}" ] && [ "${clang_major_version}" -ge 17 ]; then
      ARROW_WITH_ZLIB=OFF
    fi
  fi
  # Clang 21 added -Wcharacter-conversion (enabled by default), which Arrow's
  # bundled googletest trips under its own -Werror. Downgrade it to a warning on
  # clang 21+. Not OS-specific: any clang 21+ (macOS or Linux) is affected. Can be
  # removed once an Arrow upgrade ships a newer googletest.
  EXTRA_CMAKE_CXX_FLAGS=""
  if [ -n "${clang_major_version}" ] && [ "${clang_major_version}" -ge 21 ]; then
    EXTRA_CMAKE_CXX_FLAGS="-Wno-error=character-conversion"
  fi
  cmake_install \
       -DARROW_PARQUET=OFF \
       -DARROW_FILESYSTEM=ON \
       -DARROW_PROTOBUF_USE_SHARED=OFF \
       -DARROW_DEPENDENCY_USE_SHARED=OFF \
       -DARROW_DEPENDENCY_SOURCE=BUNDLED \
       -DARROW_WITH_LZ4=ON \
       -DARROW_WITH_SNAPPY=ON \
       -DARROW_WITH_ZLIB=${ARROW_WITH_ZLIB} \
       -DARROW_WITH_ZSTD=ON \
       -DBoost_NO_BOOST_CMAKE=TRUE \
       -DARROW_JEMALLOC=OFF \
       -DARROW_SIMD_LEVEL=NONE \
       -DARROW_RUNTIME_SIMD_LEVEL=NONE \
       -DARROW_WITH_UTF8PROC=OFF \
       -DARROW_TESTING=ON \
       -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
       -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
       -DARROW_BUILD_SHARED=OFF \
       -DARROW_BUILD_STATIC=ON

}

function build_arrow_java() {
    # Maven Central's arrow-c-data / arrow-dataset jars at ${VELOX_ARROW_BUILD_VERSION}
    # already ship libarrow_cdata_jni / libarrow_dataset_jni for x86_64 (Linux/macOS/Windows)
    # and aarch_64 (Linux/macOS), so contributors on those archs do not need a locally-built
    # jar — gluten-arrow resolves the same artifact transitively.
    #
    # ppc64le has no native in the Central jar; support_ibm_power.patch (applied above) adds
    # the ppc64le -> ppcle_64 arch case to JniLoader.java and the local mvn install step bakes
    # a locally-built libarrow_cdata_jni.so for ppc64le into the resulting arrow-c-data jar in
    # ~/.m2, overriding Central. Skip the Java build on every other arch.
    local ARCH=$(uname -m)
    if [[ "${ARCH}" != "ppc64le" ]]; then
        echo "Skipping local Arrow Java build on ${ARCH} — gluten resolves arrow-c-data:${VELOX_ARROW_BUILD_VERSION} from Maven Central. Local build is only required on ppc64le for the patched JniLoader."
        return 0
    fi

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
if [[ $(uname -m) == "ppc64le" && $SPARK_VERSION == "4.0" ]]; then
    echo "Building Spark 4.0 on ppc64le";
    source ${CURRENT_DIR}/build-arrow-18.sh;
    exit 0;
fi
prepare_arrow_build
build_arrow_cpp
echo "Finished building arrow CPP"
build_arrow_java
echo "Finished building arrow Java"
