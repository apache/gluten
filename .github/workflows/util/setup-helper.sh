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

set -e

function install_maven {
  (
    local maven_version="3.9.12"
    local local_binary="apache-maven-${maven_version}-bin.tar.gz"
    local mirror_host="https://www.apache.org/dyn/closer.lua"
    local url="${mirror_host}/maven/maven-3/${maven_version}/binaries/${local_binary}?action=download"
    cd /opt/
    wget -nv -O ${local_binary} ${url}
    tar -xvf ${local_binary} && mv apache-maven-${maven_version} /usr/lib/maven
  )
  export PATH=/usr/lib/maven/bin:$PATH
  if [ -n "$GITHUB_ENV" ]; then
    echo "PATH=/usr/lib/maven/bin:$PATH" >> $GITHUB_ENV
  else
    echo "Warning: GITHUB_ENV is not set. Skipping environment variable export."
  fi
}

function install_iwyu {
  yum install -y llvm llvm-devel clang clang-devel llvm-toolset
  CLANG_VERSION=`clang --version | awk '/clang version/ {print $3}' | cut -d. -f1`
  echo $CLANG_VERSION
  git clone https://github.com/include-what-you-use/include-what-you-use.git
  cd include-what-you-use
  git checkout clang_$CLANG_VERSION
  mkdir build && cd build
  cmake -G "Unix Makefiles" -DCMAKE_PREFIX_PATH=/usr/include/llvm ../
  make -j$(nproc)
  ln -s `pwd`/bin/include-what-you-use /usr/bin/include-what-you-use
}

for cmd in "$@"
do
    echo "Running: $cmd"
    "$cmd"
done
