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
export SUDO=sudo
source ${CURRENT_DIR}/build-helper-functions.sh
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$CURRENT_DIR/../ep/_ep}

function build_libhdfs3 {
  cd "${DEPENDENCY_DIR}"
  github_checkout apache/hawq master
  cd depends/libhdfs3
  sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt
  sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake
  sed -i "s/dfs.domain.socket.path\", \"\"/dfs.domain.socket.path\", \"\/var\/lib\/hadoop-hdfs\/dn_socket\"/g" src/common/SessionConfig.cpp
  sed -i "s/pos < endOfCurBlock/pos \< endOfCurBlock \&\& pos \- cursor \<\= 128 \* 1024/g" src/client/InputStreamImpl.cpp
  cmake_install
}

echo "Start to build Libhdfs3"
build_libhdfs3
echo "Finished building Libhdfs3. You can find the libhdfs3.so in /usr/local/lib/libhdfs3.so.1"
