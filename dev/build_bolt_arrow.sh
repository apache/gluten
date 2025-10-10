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
BOLT_ARROW_BUILD_VERSION=15.0.1-oss
ARROW_PREFIX=$CURRENT_DIR/../ep/_ep/arrow_ep
BUILD_TYPE=Release
INSTALL_PREFIX=${INSTALL_PREFIX:-"/tmp"}

function prepare_arrow_build() {
    rm -rf ${ARROW_PREFIX} && mkdir -p ${ARROW_PREFIX}
    # get arrow source and patches from conan cache
    ARROW_CACHE_DIR=$(conan cache path arrow/${BOLT_ARROW_BUILD_VERSION})
    ARROW_SOURCE_DIR=${ARROW_CACHE_DIR}/../s/src
    cp -a ${ARROW_SOURCE_DIR}/. ${ARROW_PREFIX}
}

function build_arrow_java() {
    ARROW_INSTALL_DIR="${ARROW_PREFIX}/install"

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

    export ARROW_THRIFT_URL="https://archive.apache.org/dist//thrift/0.16.0/thrift-0.16.0.tar.gz"
    ARROW_THRIFT_DST=${ARROW_PREFIX}/cpp-jni/thrift_ep-prefix/src/
    mkdir -p ${ARROW_THRIFT_DST}
    curl --noproxy '*' -o ${ARROW_THRIFT_DST}/thrift-0.16.0.tar.gz ${ARROW_THRIFT_URL}
    pushd $ARROW_PREFIX/java
    # Because arrow-bom module need the -DprocessAllModules
    mvn versions:set -DnewVersion=15.0.0-gluten -DprocessAllModules

    mvn clean install -pl bom,maven/module-info-compiler-maven-plugin,vector -am \
          -DskipTests -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -Dassembly.skipAssembly

    # Arrow C Data Interface CPP libraries
    mvn generate-resources -P generate-libs-cdata-all-os -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -N

    # Arrow JNI Date Interface CPP libraries
    export PKG_CONFIG_PATH="${INSTALL_PREFIX}"/lib64/pkgconfig:"${INSTALL_PREFIX}"/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}
    mvn generate-resources -Pgenerate-libs-jni-macos-linux -N -Darrow.dataset.jni.dist.dir=$ARROW_INSTALL_DIR \
      -DARROW_GANDIVA=OFF -DARROW_JAVA_JNI_ENABLE_GANDIVA=OFF -DARROW_ORC=OFF -DARROW_JAVA_JNI_ENABLE_ORC=OFF \
	    -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -N

    # Arrow Java libraries
    mvn install -Parrow-jni -P arrow-c-data -pl c,dataset -am \
      -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Darrow.dataset.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Darrow.cpp.build.dir=$ARROW_INSTALL_DIR/lib \
      -Dmaven.test.skip -Drat.skip -Dmaven.gitcommitid.skip -Dcheckstyle.skip -Dassembly.skipAssembly
    popd
}

echo "Start to build Arrow"
prepare_arrow_build
build_arrow_java
echo "Finished building arrow Java"
