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

# ref: https://clickhouse.com/docs/en/development/build

echo "Install Prerequisites"
sudo apt-get install git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg

echo "Install and Use the Clang compiler"
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"

echo "add CC and CXX to .bashrc"
echo "export CC=clang-18" >> ~/.bashrc
echo "export CXX=clang++-18" >> ~/.bashrc
source ~/.bashrc