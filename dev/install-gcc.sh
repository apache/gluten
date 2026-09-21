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

CUR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

set -exu

if [ "$(id -u)" -ne 0 ]; then
  echo -e "\033[31m  Run this script as root user! \033[0m"
  echo " sudo $0  or su root  "
  exit 1
fi


if [ $# -lt 1 ]; then
  GCC_VERSION=12.5.0
  echo "by default GCC_VERSION=12.5.0"
else
  GCC_VERSION=$1
fi

GCC_URL=https://github.com/gcc-mirror/gcc/archive/refs/tags/releases/gcc-${GCC_VERSION}.tar.gz
. /etc/os-release

if [[ "$ID" = "centos" || "$ID" = "fedora" ]]; then
  dnf updateinfo -y && dnf --refresh -y install gcc gcc-c++ make bzip2 autoconf automake
fi
if [[ "$ID" = "debian" || "$ID" = "ubuntu" ]]; then
  apt update && apt install -y gcc g++ make bzip2 autoconf automake
fi

rm -rf /tmp/gcc-${GCC_VERSION}*
wget  https://mirrors.aliyun.com/gnu/gcc/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.gz -P /tmp
tar -xzvf /tmp/gcc-${GCC_VERSION}.tar.gz -C /tmp && cd $(realpath /tmp/gcc-${GCC_VERSION})
./contrib/download_prerequisites
./configure --prefix=/usr/ --enable-checking=release --enable-languages=c,c++ --disable-multilib
make -j $(nproc) && make install-strip
ldconfig
rm -rf  /tmp/gcc*
gcc --version
