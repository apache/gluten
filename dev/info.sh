#!/bin/bash
# Copyright (c) Intel, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e

version='1.7.0-SNAPSHOT'
cb='```'

if [ ! -x "$(command -v cmake)" ]; then
  echo "Please install cmake to use this script."
  exit 1
fi

info=$(cmake --system-information)

ext() {
  grep -oE '".+"$' $1 | tr -d '"'
}

print_info() {
echo "$info" | grep -e "$1" | ext
}

result="
Gluten Version: ${version}
Commit: $(git rev-parse HEAD 2> /dev/null || echo "Not in a git repo.")
CMake Version: $(cmake --version | grep -oE '[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+')
System: $(print_info 'CMAKE_SYSTEM "')
Arch: $(print_info 'CMAKE_SYSTEM_PROCESSOR')
CPU Name: $(lscpu | grep 'Model name')
C++ Compiler: $(print_info 'CMAKE_CXX_COMPILER ==')
C++ Compiler Version: $(print_info 'CMAKE_CXX_COMPILER_VERSION')
C Compiler: $(print_info 'CMAKE_C_COMPILER ==')
C Compiler Version: $(print_info 'CMAKE_C_COMPILER_VERSION')
CMake Prefix Path: $(print_info '_PREFIX_PATH ')
"
echo "$result"
